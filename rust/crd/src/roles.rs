//! Roles and configuration for SparkApplications.
//!
//! Spark applications have three roles described by the [`SparkApplicationRole`].
//!
//! Unlike others, the Spark application controller doesn't create objects
//! like Pods, Services, etc. for these roles directly, but instead it delegates
//! this responsibility to the Submit job.
//!
//! The submit job only supports one group per role. For this reason, the
//! [`SparkApplication`] spec doesn't declare Role objects directly. Instead it
//! only declares [`stackable_operator::role_utils::CommonConfiguration`] objects for job,
//!  driver and executor and constructs the Roles dynamically when needed. The only group under
//! each role is named "default". These roles are transparent to the user.
//!
//! The history server has its own role completely unrelated to this module.
use std::{collections::BTreeMap, slice};

use serde::{Deserialize, Serialize};

use crate::s3logdir::S3LogDir;
use crate::SparkApplication;
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
        s3::S3ConnectionSpec,
    },
    config::{
        fragment::Fragment,
        merge::{Atomic, Merge},
    },
    k8s_openapi::{api::core::v1::VolumeMount, apimachinery::pkg::api::resource::Quantity},
    product_config_utils::Configuration,
    product_logging::{self, spec::Logging},
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter};

#[derive(Clone, Debug, Deserialize, Display, Eq, PartialEq, Serialize, JsonSchema)]
#[strum(serialize_all = "kebab-case")]
pub enum SparkApplicationRole {
    Submit,
    Driver,
    Executor,
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
    pub volume_mounts: Option<VolumeMounts>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl RoleConfig {
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
        }
    }
    pub fn volume_mounts(
        &self,
        spark_application: &SparkApplication,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        let volume_mounts = self.volume_mounts.clone().unwrap_or_default().into();
        spark_application.add_common_volume_mounts(volume_mounts, s3conn, s3logdir, true)
    }
}

impl Configuration for RoleConfigFragment {
    type Configurable = SparkApplication;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }
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
pub struct SubmitConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<SparkStorageConfig, NoRuntimeLimits>,
}

impl SubmitConfig {
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
        }
    }
}

impl Configuration for SubmitConfigFragment {
    type Configurable = SparkApplication;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }
}

// TODO: remove this when switch to pod overrides ???
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VolumeMounts {
    pub volume_mounts: Option<Vec<VolumeMount>>,
}

impl Atomic for VolumeMounts {}

impl<'a> IntoIterator for &'a VolumeMounts {
    type Item = &'a VolumeMount;
    type IntoIter = slice::Iter<'a, VolumeMount>;

    fn into_iter(self) -> Self::IntoIter {
        self.volume_mounts.as_deref().unwrap_or_default().iter()
    }
}

impl From<VolumeMounts> for Vec<VolumeMount> {
    fn from(value: VolumeMounts) -> Self {
        value.volume_mounts.unwrap_or_default()
    }
}
