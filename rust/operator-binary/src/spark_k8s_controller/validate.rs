//! The validate step in the SparkApplication controller.
//!
//! Synchronously validates the [`super::dereference::DereferencedSparkApplication`] and
//! resolves the product image. Does not touch the Kubernetes API.

use std::{borrow::Cow, ops::Deref, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    attributed_string_type,
    cli::OperatorEnvironmentOptions,
    commons::{
        product_image_selection::{self, ResolvedProductImage},
        tls_verification::TlsVerification,
    },
    constant,
    crd::{
        containerdebug::{self, ValidatedContainerdebugConfig, v1alpha1::ContainerdebugConfig},
        openlineage, s3,
    },
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        controller_utils::{get_namespace, get_uid},
        types::{
            kubernetes::{NamespaceName, Uid},
            operator::{ControllerName, OperatorName, ProductName, ProductVersion, RoleName},
        },
    },
};

use crate::{
    crd::{
        cleanup::{Cleanup, DEFAULT_AFTER_FAILED, DEFAULT_AFTER_SUCCEEDED, ResolvedCleanup},
        constants::{
            APP_NAME, CONTAINER_IMAGE_BASE_NAME, SPARK_CONTROLLER_NAME, SPARK_DRIVER_POD_NAME,
            SPARK_OPERATOR_NAME,
        },
        logdir::ResolvedLogDir,
        roles::SparkApplicationRole,
        v1alpha1,
    },
    spark_k8s_controller::dereference::DereferencedSparkApplication,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to get the spark application name"))]
    GetSparkApplicationName {},

    #[snafu(display("failed to parse the spark application name"))]
    ParseSparkApplicationName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve uid"))]
    ResolveUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("S3 TLS with verification disabled is not supported ({context})"))]
    S3TlsNoVerificationNotSupported { context: String },

    #[snafu(display(
        "a fixed driver Pod name ({SPARK_DRIVER_POD_NAME}) requires the driver Pod to be deleted \
         immediately, so cleanup (after merging templates) must be unset or set both TTLs to 0s, \
         got {cleanup:?}"
    ))]
    FixedDriverPodNameWithDelayedCleanup { cleanup: Cleanup },

    #[snafu(display("failed to resolve the config of role {role}"))]
    ResolveRoleConfig {
        source: crate::crd::Error,
        role: RoleName,
    },

    #[snafu(display("invalid containerdebug configuration for role {role}"))]
    InvalidContainerdebugConfig {
        source: containerdebug::Error,
        role: RoleName,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

// The product name (`spark-k8s`) as a type-safe label value.
constant!(pub(crate) PRODUCT_NAME: ProductName = APP_NAME);
// The operator name as a type-safe label value.
constant!(pub(crate) OPERATOR_NAME: OperatorName = SPARK_OPERATOR_NAME);
// The controller name as a type-safe label value.
constant!(pub(crate) CONTROLLER_NAME: ControllerName = SPARK_CONTROLLER_NAME);

attributed_string_type! {
    SparkApplicationName,
    "The name of a SparkApplication",
    "my-spark-application",
    // SparkApplication names were limited to 63 characters before the switch to `ClusterName`
    // (40 characters), see https://github.com/stackabletech/spark-k8s-operator/issues/775.
    (max_length = 63),
    is_rfc_1035_label_name,
    is_valid_label_value
}

/// Inputs the rest of `reconcile` needs after dereferencing.
pub struct ValidatedSparkApplication {
    /// Metadata mirroring the source [`v1alpha1::SparkApplication`] (name, namespace and UID), so
    /// this struct can be used as the owner of generated objects via its [`Resource`] impl.
    metadata: ObjectMeta,
    pub name: SparkApplicationName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label. Derived from the resolved image's app version label
    /// value.
    pub product_version: ProductVersion,
    /// The full source spec.
    ///
    /// Unlike the other operators' validated types, a `SparkApplication` cannot be reduced to
    /// resolved fields: it has no Stackable role/role-group model to decompose into. The submit,
    /// driver and executor pods are built directly from the whole spec, and the per-role helpers
    /// (`merged_env`, `pod_overrides`, `requirements`, …) read it at build time. Resolving all of
    /// that up front would duplicate the spec rather than replace it, so the raw CR is retained
    /// here as the deliberate "too interwoven to refactor" exception.
    pub spark_application: v1alpha1::SparkApplication,
    pub resolved_product_image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    /// The driver Pod TTLs, resolved from the merged application.
    pub cleanup: ResolvedCleanup,
    /// The `containerdebug` configuration of each role, resolved from the merged application.
    pub containerdebug: ValidatedContainerdebug,
}

/// The validated `containerdebug` configuration of each role.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedContainerdebug {
    pub submit: ValidatedContainerdebugConfig,
    pub driver: ValidatedContainerdebugConfig,
    pub executor: ValidatedContainerdebugConfig,
}

impl ValidatedContainerdebug {
    pub fn for_role(&self, role: &SparkApplicationRole) -> &ValidatedContainerdebugConfig {
        match role {
            SparkApplicationRole::Submit => &self.submit,
            SparkApplicationRole::Driver => &self.driver,
            SparkApplicationRole::Executor => &self.executor,
        }
    }
}

/// Cluster-wide settings resolved during validation and dereferencing, so the resource builders
/// can use the resolved values rather than re-resolving from the raw spec.
pub struct ValidatedClusterConfig {
    pub resolved_template_refs: Vec<v1alpha1::ResolvedSparkApplicationTemplate>,
    pub s3_connection: Option<s3::v1alpha1::ConnectionSpec>,
    pub open_lineage_connection: Option<openlineage::ResolvedOpenLineageConnection>,
    pub log_dir: Option<ResolvedLogDir>,
}

impl NameIsValidLabelValue for ValidatedSparkApplication {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl HasName for ValidatedSparkApplication {
    fn to_name(&self) -> String {
        String::from(&self.name)
    }
}

impl HasUid for ValidatedSparkApplication {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl Resource for ValidatedSparkApplication {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkApplication as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkApplication as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    dereferenced: DereferencedSparkApplication,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkApplication> {
    if let Some(conn) = &dereferenced.s3_connection {
        reject_tls_no_verification(conn, "S3 connection")?;
    }
    if let Some(ResolvedLogDir::S3(s3_log_dir)) = &dereferenced.log_dir {
        reject_tls_no_verification(&s3_log_dir.bucket.connection, "S3 log directory")?;
    }

    let resolved_product_image = dereferenced
        .spark_application
        .spec
        .spark_image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            &crate::built_info::PKG_VERSION_SEMVER,
        )
        .context(ResolveProductImageSnafu)?;

    // `app_version_label_value` is constructed to be a valid label value, so it is also a valid
    // `ProductVersion`.
    let product_version = ProductVersion::from_str(&resolved_product_image.app_version_label_value)
        .expect("the app version label value is a valid product version");

    let raw_name = dereferenced
        .spark_application
        .metadata
        .name
        .as_deref()
        .context(GetSparkApplicationNameSnafu)?;
    let name = SparkApplicationName::from_str(raw_name).context(ParseSparkApplicationNameSnafu)?;
    let namespace =
        get_namespace(&dereferenced.spark_application).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(&dereferenced.spark_application).context(ResolveUidSnafu)?;
    let metadata = dereferenced.spark_application.meta().clone();

    let cleanup = validate_cleanup(&dereferenced.spark_application)?;
    let containerdebug = validate_containerdebug(&dereferenced.spark_application)?;

    Ok(ValidatedSparkApplication {
        metadata,
        name,
        namespace,
        uid,
        product_version,
        spark_application: dereferenced.spark_application,
        resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            resolved_template_refs: dereferenced.resolved_template_refs,
            s3_connection: dereferenced.s3_connection,
            open_lineage_connection: dereferenced.open_lineage_connection,
            log_dir: dereferenced.log_dir,
        },
        cleanup,
        containerdebug,
    })
}

fn reject_tls_no_verification(conn: &s3::v1alpha1::ConnectionSpec, context: &str) -> Result<()> {
    if let Some(tls) = &conn.tls.tls
        && matches!(&tls.verification, TlsVerification::None {})
    {
        return S3TlsNoVerificationNotSupportedSnafu {
            context: context.to_owned(),
        }
        .fail();
    }
    Ok(())
}

/// Resolves the driver Pod TTLs of the given application, which must already have its templates
/// merged (see [`DereferencedSparkApplication`]).
///
/// A driver Pod with a fixed name must be deleted as soon as it finished, otherwise the next
/// submission or retry fails because a Pod with that name still exists.
/// Reject an explicit `cleanup` that asks for something else instead of silently ignoring it.
fn validate_cleanup(merged_application: &v1alpha1::SparkApplication) -> Result<ResolvedCleanup> {
    let spec = &merged_application.spec;
    if let Some(cleanup) = &spec.cleanup
        && spec.spark_conf.contains_key(SPARK_DRIVER_POD_NAME)
        && !cleanup.is_immediate()
    {
        return FixedDriverPodNameWithDelayedCleanupSnafu {
            cleanup: cleanup.clone(),
        }
        .fail();
    }
    Ok(resolved_cleanup(spec))
}

/// Resolves and validates the `containerdebug` configuration of each role of the given
/// application, which must already have its templates merged (see
/// [`DereferencedSparkApplication`]).
fn validate_containerdebug(
    merged_application: &v1alpha1::SparkApplication,
) -> Result<ValidatedContainerdebug> {
    let submit_config =
        merged_application
            .submit_config()
            .with_context(|_| ResolveRoleConfigSnafu {
                role: SparkApplicationRole::Submit.deref().clone(),
            })?;
    let driver_config =
        merged_application
            .driver_config()
            .with_context(|_| ResolveRoleConfigSnafu {
                role: SparkApplicationRole::Driver.deref().clone(),
            })?;
    let executor_config =
        merged_application
            .executor_config()
            .with_context(|_| ResolveRoleConfigSnafu {
                role: SparkApplicationRole::Executor.deref().clone(),
            })?;

    Ok(ValidatedContainerdebug {
        submit: validate_role_containerdebug(
            SparkApplicationRole::Submit,
            &submit_config.containerdebug,
        )?,
        driver: validate_role_containerdebug(
            SparkApplicationRole::Driver,
            &driver_config.containerdebug,
        )?,
        executor: validate_role_containerdebug(
            SparkApplicationRole::Executor,
            &executor_config.containerdebug,
        )?,
    })
}

fn validate_role_containerdebug(
    role: SparkApplicationRole,
    containerdebug: &ContainerdebugConfig,
) -> Result<ValidatedContainerdebugConfig> {
    containerdebug
        .validate()
        .with_context(|_| InvalidContainerdebugConfigSnafu {
            role: role.deref().clone(),
        })
}

/// Resolves the TTLs from exactly the given spec, filling in defaults.
///
/// Templates are not merged here. Pass the spec of a merged application (see
/// `DereferencedSparkApplication`) to get the TTLs that actually apply to it.
///
/// A driver whose name is fixed with `spark.kubernetes.driver.pod.name` is deleted as soon as
/// it finished, whatever `cleanup` says: the next submission or retry would otherwise fail
/// because a Pod with that name still exists.
fn resolved_cleanup(spec: &v1alpha1::SparkApplicationSpec) -> ResolvedCleanup {
    if spec.spark_conf.contains_key(SPARK_DRIVER_POD_NAME) {
        return ResolvedCleanup::IMMEDIATE;
    }
    let cleanup = spec.cleanup.clone().unwrap_or_default();
    ResolvedCleanup {
        after_succeeded: cleanup.after_succeeded.unwrap_or(DEFAULT_AFTER_SUCCEEDED),
        after_failed: cleanup.after_failed.unwrap_or(DEFAULT_AFTER_FAILED),
    }
}

#[cfg(test)]
mod tests {
    use indoc::{formatdoc, indoc};
    use rstest::rstest;
    use stackable_operator::{
        cli::OperatorEnvironmentOptions, crd::containerdebug::DEFAULT_INTERVAL,
        shared::time::Duration,
    };

    use super::*;
    use crate::test_support::app_version_label;

    /// Runs the validate step against the given `SparkApplication` YAML without templates or
    /// external connections.
    fn validate_yaml(yaml: &str) -> Result<ValidatedSparkApplication> {
        let spark_application: v1alpha1::SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(yaml)
                .expect("invalid test SparkApplication YAML");
        validate(
            DereferencedSparkApplication {
                spark_application,
                resolved_template_refs: Vec::new(),
                s3_connection: None,
                log_dir: None,
                open_lineage_connection: None,
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
    }

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *CONTROLLER_NAME;
        let _ = *OPERATOR_NAME;
        let _ = *PRODUCT_NAME;
    }

    /// Locks every value the validate step itself derives from the minimal fixture. The raw
    /// `SparkApplication` is deliberately retained on the validated type (see the field docs),
    /// so only the resolved identity, image and cluster config are derived here.
    #[test]
    fn validate_ok_derives_expected_values() {
        let yaml = indoc! {r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
        "#};
        let spark_application: v1alpha1::SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(yaml)
                .expect("invalid test SparkApplication YAML");

        let validated = validate(
            DereferencedSparkApplication {
                spark_application,
                resolved_template_refs: Vec::new(),
                s3_connection: None,
                log_dir: None,
                open_lineage_connection: None,
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("the minimal fixture validates");

        assert_eq!(validated.name.to_string(), "spark-example");
        assert_eq!(validated.namespace.to_string(), "default");
        assert_eq!(
            validated.uid.to_string(),
            "12345678-1234-1234-1234-123456789012"
        );
        assert_eq!(
            validated.resolved_product_image.image,
            format!(
                "oci.example.org/sdp/spark-k8s:{}",
                app_version_label("1.2.3")
            )
        );
        assert_eq!(validated.resolved_product_image.product_version, "1.2.3");

        // The minimal fixture references no templates, no S3 connection and no log directory,
        // and the raw spec is retained for the pod builders.
        assert!(validated.cluster_config.resolved_template_refs.is_empty());
        assert!(validated.cluster_config.s3_connection.is_none());
        assert!(validated.cluster_config.log_dir.is_none());
        assert_eq!(
            validated.spark_application.metadata.name.as_deref(),
            Some("spark-example")
        );
    }

    #[rstest]
    #[case::no_driver_name_no_cleanup("{}", "null", Some(("10m", "1d")))]
    #[case::no_driver_name_with_cleanup(
        "{}",
        "{afterSucceeded: 1h, afterFailed: 2d}",
        Some(("1h", "2d"))
    )]
    #[case::driver_name_no_cleanup(
        "{spark.kubernetes.driver.pod.name: my-driver}",
        "null",
        Some(("0s", "0s"))
    )]
    #[case::driver_name_immediate_cleanup(
        "{spark.kubernetes.driver.pod.name: my-driver}",
        "{afterSucceeded: 0s, afterFailed: 0s}",
        Some(("0s", "0s"))
    )]
    #[case::driver_name_delayed_cleanup(
        "{spark.kubernetes.driver.pod.name: my-driver}",
        "{afterSucceeded: 1h, afterFailed: 2d}",
        None
    )]
    #[case::driver_name_partially_immediate_cleanup(
        "{spark.kubernetes.driver.pod.name: my-driver}",
        "{afterSucceeded: 0s}",
        None
    )]
    #[case::driver_name_empty_cleanup("{spark.kubernetes.driver.pod.name: my-driver}", "{}", None)]
    fn validate_cleanup_requires_immediate_cleanup_for_a_fixed_driver_name(
        #[case] spark_conf: &str,
        #[case] cleanup: &str,
        #[case] expected: Option<(&str, &str)>,
    ) {
        let spark_application: v1alpha1::SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(&formatdoc! {"
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkApplication
                metadata:
                  name: spark-example
                spec:
                  mode: cluster
                  mainApplicationFile: test.py
                  sparkImage:
                    productVersion: 1.2.3
                  sparkConf: {spark_conf}
                  cleanup: {cleanup}
            "})
            .expect("invalid test SparkApplication YAML");

        let result = validate_cleanup(&spark_application);

        match expected {
            Some(expected) => {
                let resolved = result.expect("the application should be valid");
                assert_eq!(
                    (
                        resolved.after_succeeded.to_string().as_str(),
                        resolved.after_failed.to_string().as_str()
                    ),
                    expected
                );
            }
            None => assert!(matches!(
                result,
                Err(Error::FixedDriverPodNameWithDelayedCleanup { .. })
            )),
        }
    }

    /// A role `config` that does not mention `containerdebug` still gets the default, so the
    /// `containerdebug` field of the CRD must be optional in every role.
    #[test]
    fn containerdebug_defaults_when_not_configured() {
        let yaml = indoc! {r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              job:
                config:
                  retryOnFailureCount: 1
              driver:
                config:
                  requestedSecretLifetime: 2d
              executor:
                config:
                  requestedSecretLifetime: 2d
        "#};

        let validated = match validate_yaml(yaml) {
            Ok(validated) => validated,
            Err(error) => panic!("the SparkApplication is valid, but: {error:?}"),
        };

        let default = ValidatedContainerdebugConfig::Enabled {
            interval: DEFAULT_INTERVAL,
        };
        assert_eq!(
            validated.containerdebug,
            ValidatedContainerdebug {
                submit: default.clone(),
                driver: default.clone(),
                executor: default,
            }
        );
    }

    /// Every role gets its own `containerdebug` config, so a mix-up between the roles shows.
    #[test]
    fn containerdebug_is_validated_per_role() {
        let yaml = indoc! {r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              job:
                config:
                  containerdebug:
                    enabled: false
              driver:
                config:
                  containerdebug:
                    interval: 30s
        "#};

        let validated = match validate_yaml(yaml) {
            Ok(validated) => validated,
            Err(error) => panic!("the SparkApplication is valid, but: {error:?}"),
        };

        assert_eq!(
            validated.containerdebug,
            ValidatedContainerdebug {
                submit: ValidatedContainerdebugConfig::Disabled,
                driver: ValidatedContainerdebugConfig::Enabled {
                    interval: Duration::from_secs(30),
                },
                executor: ValidatedContainerdebugConfig::Enabled {
                    interval: DEFAULT_INTERVAL,
                },
            }
        );
    }

    /// An invalid `containerdebug` config fails the validation, naming the role.
    #[test]
    fn zero_containerdebug_interval_fails_validation() {
        let yaml = indoc! {r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              executor:
                config:
                  containerdebug:
                    interval: 0s
        "#};

        let Err(error) = validate_yaml(yaml) else {
            panic!("a zero containerdebug interval must fail the validation");
        };
        let chain: Vec<String> =
            std::iter::successors(Some(&error as &dyn std::error::Error), |error| {
                error.source()
            })
            .map(ToString::to_string)
            .collect();

        assert_eq!(
            chain,
            [
                "invalid containerdebug configuration for role executor",
                "the containerdebug interval must not be zero",
            ]
        );
    }
}
