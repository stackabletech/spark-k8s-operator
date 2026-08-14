//! The validate step in the SparkConnectServer controller.
//!
//! Resolves the product image and the server/executor configs.
//! Does not touch the Kubernetes API.

use std::{borrow::Cow, collections::HashMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    k8s_openapi::{api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    kube::Resource,
    kvp::Labels,
    product_logging::spec::Logging,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        kvp::label::{recommended_labels, role_group_selector, role_selector},
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
        role_utils::{self, JavaCommonConfig},
        types::{
            kubernetes::{ConfigMapName, ListenerClassName, NamespaceName, Uid},
            operator::{
                ClusterName, ControllerName, OperatorName, ProductName, ProductVersion,
                RoleGroupName, RoleName,
            },
        },
    },
};

use crate::{
    connect::{
        common::SparkConnectRole,
        controller::dereference::DereferencedSparkConnectServer,
        crd::{
            self, CONNECT_APP_NAME, CONNECT_CONTROLLER_NAME, DEFAULT_SPARK_CONNECT_GROUP_NAME,
            SparkConnectContainer, v1alpha1,
        },
        s3::ResolvedS3,
    },
    crd::constants::{CONTAINER_IMAGE_BASE_NAME, OPERATOR_NAME, UNVERSIONED_PRODUCT_VERSION},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

    #[snafu(display("failed to resolve server config"))]
    ServerConfig { source: crd::Error },

    #[snafu(display("failed to resolve executor config"))]
    ExecutorConfig { source: crd::Error },

    #[snafu(display("failed to resolve cluster name"))]
    ResolveClusterName {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve namespace"))]
    ResolveNamespace {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to resolve uid"))]
    ResolveUid {
        source: stackable_operator::v2::controller_utils::Error,
    },

    #[snafu(display("failed to validate the logging configuration"))]
    ValidateLoggingConfig {
        source: stackable_operator::v2::product_logging::framework::Error,
    },

    #[snafu(display(
        "the Vector aggregator discovery ConfigMap name must be set when the Vector agent is enabled"
    ))]
    MissingVectorAggregatorConfigMapName,
}

/// Validates the logging configuration for the (optional) Vector container.
///
/// `vector_aggregator_config_map_name` is the discovery ConfigMap name of the Vector aggregator;
/// it is required (and validated) only when the Vector agent is enabled.
fn validate_logging(
    logging: &Logging<SparkConnectContainer>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(
                logging,
                &SparkConnectContainer::Vector,
            )
            .context(ValidateLoggingConfigSnafu)?,
            vector_aggregator_config_map_name,
        })
    } else {
        None
    };

    Ok(ValidatedLogging {
        vector_container,
        enable_vector_agent: logging.enable_vector_agent,
    })
}

type Result<T, E = Error> = std::result::Result<T, E>;

stackable_operator::constant!(
    DEFAULT_SPARK_CONNECT_ROLE_GROUP: RoleGroupName = DEFAULT_SPARK_CONNECT_GROUP_NAME
);

/// Validated logging configuration for the (optional) Vector container.
///
/// Produced up-front by [`validate_logging`] so that an
/// invalid custom log ConfigMap name or a missing Vector aggregator discovery ConfigMap name fails
/// reconciliation during validation rather than at resource-build time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedLogging {
    pub vector_container: Option<VectorContainerLogConfig>,
    pub enable_vector_agent: bool,
}

pub struct ValidatedSparkConnectServer {
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    pub product_version: ProductVersion,
    pub resolved_product_image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_config: ValidatedRoleConfig,
    pub server_config: v1alpha1::ServerConfig,
    pub server_overrides: ValidatedOverrides,
    pub server_logging: ValidatedLogging,
    pub executor_config: v1alpha1::ExecutorConfig,
    pub executor_overrides: ValidatedOverrides,
    pub executor_logging: ValidatedLogging,
}

/// User-provided overrides for a role, captured during validation so the resource builders never
/// read the raw [`v1alpha1::SparkConnectServer`] spec.
#[derive(Clone, Debug, Default)]
pub struct ValidatedOverrides {
    pub config_overrides: v1alpha1::ConfigOverrides,
    pub env_overrides: HashMap<String, String>,
    pub pod_overrides: PodTemplateSpec,
    pub jvm_config: Option<JavaCommonConfig>,
}

/// Cluster-wide settings resolved during validation, so the resource builders never have to read
/// the raw [`v1alpha1::SparkConnectServer`] spec.
pub struct ValidatedClusterConfig {
    pub resolved_s3: ResolvedS3,
}

/// Per-role configuration extracted during validation (Spark Connect exposes only the server role).
pub struct ValidatedRoleConfig {
    pub listener_class: ListenerClassName,
}

impl ValidatedSparkConnectServer {
    /// Recommended labels for a resource that is not tied to a concrete [`SparkConnectRole`]
    /// (e.g. the cluster-shared RBAC resources), using a free-form role/role-group label value.
    pub fn recommended_labels_for(
        &self,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        self.recommended_labels_with(&self.product_version, role_name, role_group_name)
    }

    /// Recommended labels with a fixed placeholder version, for objects that live in immutable
    /// fields (e.g. the listener PVC in the StatefulSet's `volumeClaimTemplates`) and therefore
    /// must not carry labels that change on upgrade.
    pub fn unversioned_recommended_labels(&self, role: SparkConnectRole) -> Labels {
        self.recommended_labels_with(
            &UNVERSIONED_PRODUCT_VERSION,
            &role.into(),
            &DEFAULT_SPARK_CONNECT_ROLE_GROUP,
        )
    }

    /// Recommended labels for a resource of the given role.
    pub fn recommended_labels(&self, role: SparkConnectRole) -> Labels {
        self.recommended_labels_for(&role.into(), &DEFAULT_SPARK_CONNECT_ROLE_GROUP)
    }

    fn recommended_labels_with(
        &self,
        product_version: &ProductVersion,
        role_name: &RoleName,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            role_name,
            role_group_name,
        )
    }

    /// Selector labels matching the pods of the given role.
    pub fn role_selector(&self, role: SparkConnectRole) -> Labels {
        role_selector(self, &product_name(), &role.into())
    }

    /// Selector labels matching the pods of the given role's (single) role group.
    pub fn role_group_selector(&self, role: SparkConnectRole) -> Labels {
        role_group_selector(
            self,
            &product_name(),
            &role.into(),
            &DEFAULT_SPARK_CONNECT_ROLE_GROUP,
        )
    }

    /// Type-safe names for the per-cluster RBAC resources: the ServiceAccount,
    /// its (namespaced) RoleBinding, and the operator-deployed ClusterRole it binds.
    pub fn cluster_resource_names(&self) -> role_utils::ResourceNames {
        role_utils::ResourceNames {
            cluster_name: self.name.clone(),
            product_name: product_name(),
        }
    }
}

/// The product name (`spark-connect`) as a type-safe label value.
pub fn product_name() -> ProductName {
    ProductName::from_str(CONNECT_APP_NAME).expect("CONNECT_APP_NAME is a valid product name")
}

/// The operator name as a type-safe label value.
pub fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub fn controller_name() -> ControllerName {
    ControllerName::from_str(CONNECT_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

impl NameIsValidLabelValue for ValidatedSparkConnectServer {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl HasName for ValidatedSparkConnectServer {
    fn to_name(&self) -> String {
        String::from(&self.name)
    }
}

impl HasUid for ValidatedSparkConnectServer {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl Resource for ValidatedSparkConnectServer {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkConnectServer as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkConnectServer as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    scs: &v1alpha1::SparkConnectServer,
    dereferenced: DereferencedSparkConnectServer,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkConnectServer> {
    let resolved_product_image = scs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let product_version = ProductVersion::from_str(&resolved_product_image.app_version_label_value)
        .expect("the app version label value is a valid product version");

    let server_config = scs.server_config().context(ServerConfigSnafu)?;
    let executor_config = scs.executor_config().context(ExecutorConfigSnafu)?;

    // Capture the user overrides up-front so the resource builders never read the raw spec.
    let server_overrides = scs
        .spec
        .server
        .config
        .as_ref()
        .map(|cc| ValidatedOverrides {
            config_overrides: cc.config_overrides.clone(),
            env_overrides: cc.env_overrides.clone(),
            pod_overrides: cc.pod_overrides.clone(),
            jvm_config: Some(cc.product_specific_common_config.clone()),
        })
        .unwrap_or_default();
    let executor_overrides = scs
        .spec
        .executor
        .as_ref()
        .map(|cc| ValidatedOverrides {
            config_overrides: cc.config_overrides.clone(),
            env_overrides: cc.env_overrides.clone(),
            pod_overrides: cc.pod_overrides.clone(),
            jvm_config: Some(cc.product_specific_common_config.clone()),
        })
        .unwrap_or_default();

    let name = get_cluster_name(scs).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(scs).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(scs).context(ResolveUidSnafu)?;

    // The Vector aggregator discovery ConfigMap name. It is only required when the Vector agent is
    // enabled for the server.
    let vector_aggregator_config_map_name = scs.spec.vector_aggregator_config_map_name.clone();

    let server_logging =
        validate_logging(&server_config.logging, &vector_aggregator_config_map_name)?;
    let executor_logging =
        validate_logging(&executor_config.logging, &vector_aggregator_config_map_name)?;

    Ok(ValidatedSparkConnectServer {
        metadata: scs.meta().clone(),
        name,
        namespace,
        uid,
        product_version,
        resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            resolved_s3: dereferenced.resolved_s3,
        },
        role_config: ValidatedRoleConfig {
            listener_class: scs.spec.server.role_config.listener_class.clone(),
        },
        server_config,
        server_overrides,
        server_logging,
        executor_config,
        executor_overrides,
        executor_logging,
    })
}

#[cfg(test)]
mod tests {
    use crate::{
        connect::controller::build::test_support::minimal_validated_cluster,
        test_support::app_version_label,
    };

    /// Locks every value the validate step itself derives from the minimal fixture — so a
    /// validation regression fails here, with a validate-shaped message, instead of surfacing as
    /// a confusing build-test failure downstream.
    #[test]
    fn validate_ok_derives_expected_values() {
        let validated = minimal_validated_cluster();

        assert_eq!(validated.name.to_string(), "my-connect");
        assert_eq!(validated.namespace.to_string(), "default");
        assert_eq!(
            validated.uid.to_string(),
            "12345678-1234-1234-1234-123456789012"
        );
        assert_eq!(
            validated.resolved_product_image.image,
            format!(
                "oci.example.org/sdp/spark-k8s:{}",
                app_version_label("4.1.2")
            )
        );
        assert_eq!(validated.resolved_product_image.product_version, "4.1.2");
        assert_eq!(
            validated.product_version.to_string(),
            app_version_label("4.1.2")
        );

        // The role config falls back to the default cluster-internal listener.
        assert_eq!(
            validated.role_config.listener_class.to_string(),
            "cluster-internal"
        );

        // The minimal fixture has no overrides and no Vector agent for either role.
        for overrides in [&validated.server_overrides, &validated.executor_overrides] {
            assert!(overrides.env_overrides.is_empty());
            assert_eq!(overrides.jvm_config, None);
        }
        for logging in [&validated.server_logging, &validated.executor_logging] {
            assert!(!logging.enable_vector_agent);
            assert_eq!(logging.vector_container, None);
        }
    }
}
