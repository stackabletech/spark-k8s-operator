//! Roles and configuration for SparkApplications.
//!
//! Spark applications have three roles described by the [`SparkApplicationRole`].
//!
//! Unlike others, the Spark application controller doesn't create objects
//! like Pods, Services, etc. for these roles directly, but instead it delegates
//! this responsibility to the Submit job.
//!
//! The submit job only supports one group per role. For this reason, the
//! [`v1alpha1::SparkApplication`] spec doesn't declare Role objects directly. Instead it
//! only declares [`stackable_operator::role_utils::CommonConfiguration`] objects for job,
//!  driver and executor and constructs the Roles dynamically when needed. The only group under
//! each role is named "default". These roles are transparent to the user.
//!
//! The history server has its own role completely unrelated to this module.
use std::{borrow::Cow, fmt::Display, ops::Deref, slice, str::FromStr};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::Fragment,
        merge::{Atomic, Merge},
    },
    constant,
    crd::{openlineage, s3},
    k8s_openapi::{api::core::v1::VolumeMount, apimachinery::pkg::api::resource::Quantity},
    product_logging::{self, spec::Logging},
    schemars::{self, JsonSchema},
    shared::time::{Duration, DurationParseError},
    utils::crds::raw_object_list_schema,
    v2::types::{kubernetes::ContainerName, operator::RoleName},
};
use strum::{Display, EnumIter};

use crate::crd::{ResolvedLogDir, constants::DEFAULT_SUBMIT_JOB_RETRY_ON_FAILURE_COUNT, v1alpha1};

constant!(SUBMIT_ROLE_NAME: RoleName = "submit");
constant!(DRIVER_ROLE_NAME: RoleName = "driver");
constant!(EXECUTOR_ROLE_NAME: RoleName = "executor");

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SparkApplicationRole {
    Submit,
    Driver,
    Executor,
}

impl Deref for SparkApplicationRole {
    type Target = RoleName;

    fn deref(&self) -> &Self::Target {
        match self {
            SparkApplicationRole::Submit => &SUBMIT_ROLE_NAME,
            SparkApplicationRole::Driver => &DRIVER_ROLE_NAME,
            SparkApplicationRole::Executor => &EXECUTOR_ROLE_NAME,
        }
    }
}

impl SparkApplicationRole {
    /// The type-safe name of this role, e.g. to build
    /// [`ResourceNames`](stackable_operator::v2::role_group_utils::ResourceNames).
    pub fn role_name(&self) -> RoleName {
        RoleName::clone(self)
    }
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize,
    ),
    allow(clippy::derive_partial_eq_without_eq),
    serde(rename_all = "camelCase")
)]
pub struct SparkStorageConfig {}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    Eq,
    EnumIter,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum SparkContainer {
    SparkSubmit,
    Job,
    Requirements,
    Spark,
    Vector,
    Tls,
}

// Typed container names. They must match the strum `Display` (kebab-case) of the variants above,
// which is pinned by a unit test.
constant!(SPARK_SUBMIT_CONTAINER_NAME: ContainerName = "spark-submit");
constant!(JOB_CONTAINER_NAME: ContainerName = "job");
constant!(REQUIREMENTS_CONTAINER_NAME: ContainerName = "requirements");
constant!(SPARK_CONTAINER_NAME: ContainerName = "spark");
constant!(VECTOR_CONTAINER_NAME: ContainerName = "vector");
constant!(TLS_CONTAINER_NAME: ContainerName = "tls");

impl SparkContainer {
    /// The typed container name of this variant.
    pub fn name(&self) -> &'static ContainerName {
        match self {
            SparkContainer::SparkSubmit => &SPARK_SUBMIT_CONTAINER_NAME,
            SparkContainer::Job => &JOB_CONTAINER_NAME,
            SparkContainer::Requirements => &REQUIREMENTS_CONTAINER_NAME,
            SparkContainer::Spark => &SPARK_CONTAINER_NAME,
            SparkContainer::Vector => &VECTOR_CONTAINER_NAME,
            SparkContainer::Tls => &TLS_CONTAINER_NAME,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Display, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum SparkMode {
    Cluster,
    Client,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct RoleConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<SparkStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub logging: Logging<SparkContainer>,

    #[fragment_attrs(serde(default, flatten))]
    pub volume_mounts: VolumeMounts,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

impl RoleConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config() -> RoleConfigFragment {
        RoleConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1Gi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: SparkStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            volume_mounts: Some(VolumeMounts::default()),
            affinity: Default::default(),
            requested_secret_lifetime: Some(Self::DEFAULT_SECRET_LIFETIME),
        }
    }

    pub fn volume_mounts(
        &self,
        spark_application: &v1alpha1::SparkApplication,
        s3conn: &Option<s3::v1alpha1::ConnectionSpec>,
        logdir: &Option<ResolvedLogDir>,
        open_lineage_conn: Option<&openlineage::ResolvedOpenLineageConnection>,
    ) -> Vec<VolumeMount> {
        let volume_mounts = self.volume_mounts.clone().into();
        spark_application.add_common_volume_mounts(
            volume_mounts,
            s3conn,
            logdir,
            true,
            open_lineage_conn,
        )
    }
}

#[derive(Debug, Snafu)]
#[snafu(display(
    "invalid ttlAfterFinished {input:?}, expected \"never\" or a duration such as \"10m\""
))]
pub struct ParseTtlAfterFinishedError {
    input: String,
    source: DurationParseError,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub enum TtlAfterFinished {
    Never,
    After(Duration),
}

impl TtlAfterFinished {
    const NEVER: &'static str = "never";

    pub fn job_ttl_seconds(&self) -> Option<i32> {
        match self {
            Self::Never => None,
            Self::After(duration) => Some(i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)),
        }
    }

    pub fn is_immediate(&self) -> bool {
        self.job_ttl_seconds() == Some(0)
    }
}

impl FromStr for TtlAfterFinished {
    type Err = ParseTtlAfterFinishedError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == Self::NEVER {
            return Ok(Self::Never);
        }
        Duration::from_str(input)
            .map(Self::After)
            .context(ParseTtlAfterFinishedSnafu { input })
    }
}

impl Display for TtlAfterFinished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Never => f.write_str(Self::NEVER),
            Self::After(duration) => duration.fmt(f),
        }
    }
}

impl TryFrom<String> for TtlAfterFinished {
    type Error = ParseTtlAfterFinishedError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TtlAfterFinished> for String {
    fn from(value: TtlAfterFinished) -> Self {
        value.to_string()
    }
}

impl JsonSchema for TtlAfterFinished {
    fn schema_name() -> Cow<'static, str> {
        "TtlAfterFinished".into()
    }

    fn json_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

impl Default for TtlAfterFinished {
    fn default() -> Self {
        Self::After(Duration::from_minutes_unchecked(10))
    }
}

impl Atomic for TtlAfterFinished {}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct SubmitConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<SparkStorageConfig, NoRuntimeLimits>,

    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    #[fragment_attrs(serde(default, flatten))]
    pub volume_mounts: Option<VolumeMounts>,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,

    /// Number of times to retry the submit job on failure. Default is `0` (no retries).
    #[fragment_attrs(serde(default))]
    pub retry_on_failure_count: u16,

    /// How long the spark-submit Job, the driver Pod and the objects the operator created for
    /// the application are kept after the Job finished. A duration such as `10m` (default),
    /// `0s` to delete them right away, or `never` to keep them until the SparkApplication is
    /// deleted.
    #[fragment_attrs(serde(default))]
    pub ttl_after_finished: TtlAfterFinished,
}

impl SubmitConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config() -> SubmitConfigFragment {
        SubmitConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("100m".to_owned())),
                    max: Some(Quantity("400m".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: SparkStorageConfigFragment {},
            },
            affinity: Default::default(),
            volume_mounts: Some(VolumeMounts::default()),
            requested_secret_lifetime: Some(Self::DEFAULT_SECRET_LIFETIME),
            retry_on_failure_count: Some(DEFAULT_SUBMIT_JOB_RETRY_ON_FAILURE_COUNT),
            ttl_after_finished: Some(TtlAfterFinished::default()),
        }
    }
}

// TODO: remove this when switch to pod overrides ???
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMounts {
    /// Volume mounts for the spark-submit, driver and executor pods.
    #[schemars(schema_with = "raw_object_list_schema")]
    pub volume_mounts: Vec<VolumeMount>,
}

impl Atomic for VolumeMounts {}

impl<'a> IntoIterator for &'a VolumeMounts {
    type IntoIter = slice::Iter<'a, VolumeMount>;
    type Item = &'a VolumeMount;

    fn into_iter(self) -> Self::IntoIter {
        self.volume_mounts.iter()
    }
}

impl From<VolumeMounts> for Vec<VolumeMount> {
    fn from(value: VolumeMounts) -> Self {
        value.volume_mounts
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *DRIVER_ROLE_NAME;
        let _ = *EXECUTOR_ROLE_NAME;
        let _ = *SUBMIT_ROLE_NAME;
        let _ = *SPARK_SUBMIT_CONTAINER_NAME;
        let _ = *JOB_CONTAINER_NAME;
        let _ = *REQUIREMENTS_CONTAINER_NAME;
        let _ = *SPARK_CONTAINER_NAME;
        let _ = *VECTOR_CONTAINER_NAME;
        let _ = *TLS_CONTAINER_NAME;
    }

    /// The typed container names returned by `name` must agree with the strum `Display` of
    /// `SparkContainer`, which `build_log4j2` still uses for the log directory path.
    #[test]
    fn container_names_match_display() {
        for container in SparkContainer::iter() {
            assert_eq!(container.name().to_string(), container.to_string());
        }
    }
    #[rstest]
    #[case::never("never", TtlAfterFinished::Never)]
    #[case::zero("0s", TtlAfterFinished::After(Duration::from_secs(0)))]
    #[case::ten_minutes("10m", TtlAfterFinished::After(Duration::from_minutes_unchecked(10)))]
    fn ttl_after_finished_round_trips(#[case] input: &str, #[case] expected: TtlAfterFinished) {
        let parsed: TtlAfterFinished = input.parse().expect("the input is valid");
        assert_eq!(parsed, expected);
        assert_eq!(parsed.to_string(), input);
    }

    #[rstest]
    #[case::empty("")]
    #[case::typo("nevr")]
    fn ttl_after_finished_rejects_invalid_input(#[case] input: &str) {
        assert!(input.parse::<TtlAfterFinished>().is_err());
    }

    #[rstest]
    #[case::never(TtlAfterFinished::Never, None)]
    #[case::zero(TtlAfterFinished::After(Duration::from_secs(0)), Some(0))]
    fn ttl_after_finished_job_seconds(
        #[case] ttl: TtlAfterFinished,
        #[case] expected: Option<i32>,
    ) {
        assert_eq!(ttl.job_ttl_seconds(), expected);
    }
}
