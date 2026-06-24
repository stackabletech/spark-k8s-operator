//! The validate step in the SparkConnectServer controller.
//!
//! Resolves the product image and the server/executor configs.
//! Does not touch the Kubernetes API.

use std::{borrow::Cow, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    kvp::Labels,
    product_logging::spec::Logging,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::meta::ownerreference_from_resource,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        kvp::label::{recommended_labels, role_group_selector, role_selector},
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
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
            self, CONNECT_APP_NAME, CONNECT_CONTROLLER_NAME, CONNECT_EXECUTOR_ROLE_NAME,
            CONNECT_SERVER_ROLE_NAME, DEFAULT_SPARK_CONNECT_GROUP_NAME, SparkConnectContainer,
            v1alpha1,
        },
        s3::ResolvedS3,
    },
    crd::constants::{CONTAINER_IMAGE_BASE_NAME, OPERATOR_NAME},
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
    pub resolved_product_image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_config: ValidatedRoleConfig,
    pub server_config: v1alpha1::ServerConfig,
    pub server_logging: ValidatedLogging,
    pub executor_config: v1alpha1::ExecutorConfig,
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
    /// Recommended labels for a resource of the given role.
    pub(crate) fn recommended_labels(&self, role: SparkConnectRole) -> Labels {
        // `app_version_label_value` is constructed to be a valid label value, so it is also a
        // valid `ProductVersion`.
        let product_version =
            ProductVersion::from_str(&self.resolved_product_image.app_version_label_value)
                .expect("the app version label value is a valid product version");
        let role_group = RoleGroupName::from_str(DEFAULT_SPARK_CONNECT_GROUP_NAME)
            .expect("DEFAULT_SPARK_CONNECT_GROUP_NAME is a valid role group name");
        recommended_labels(
            self,
            &product_name(),
            &product_version,
            &operator_name(),
            &controller_name(),
            &role_name(role),
            &role_group,
        )
    }

    /// Selector labels matching the pods of the given role.
    pub(crate) fn role_selector(&self, role: SparkConnectRole) -> Labels {
        role_selector(self, &product_name(), &role_name(role))
    }

    /// Selector labels matching the pods of the given role's (single) role group.
    pub(crate) fn role_group_selector(&self, role: SparkConnectRole) -> Labels {
        let role_group = RoleGroupName::from_str(DEFAULT_SPARK_CONNECT_GROUP_NAME)
            .expect("DEFAULT_SPARK_CONNECT_GROUP_NAME is a valid role group name");
        role_group_selector(self, &product_name(), &role_name(role), &role_group)
    }

    /// Object metadata for a child resource named `name`, owned by this SparkConnectServer and
    /// carrying the recommended labels for the given role.
    pub(crate) fn object_meta(
        &self,
        name: impl Into<String>,
        role: SparkConnectRole,
    ) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(self.recommended_labels(role));
        builder
    }
}

/// The product name (`spark-connect`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(CONNECT_APP_NAME).expect("CONNECT_APP_NAME is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(CONNECT_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

/// The role name for the given Spark Connect role as a type-safe label value.
fn role_name(role: SparkConnectRole) -> RoleName {
    match role {
        SparkConnectRole::Server => RoleName::from_str(CONNECT_SERVER_ROLE_NAME)
            .expect("CONNECT_SERVER_ROLE_NAME is a valid role name"),
        SparkConnectRole::Executor => RoleName::from_str(CONNECT_EXECUTOR_ROLE_NAME)
            .expect("CONNECT_EXECUTOR_ROLE_NAME is a valid role name"),
    }
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

    let server_config = scs.server_config().context(ServerConfigSnafu)?;
    let executor_config = scs.executor_config().context(ExecutorConfigSnafu)?;
    let name = get_cluster_name(scs).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(scs).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(scs).context(ResolveUidSnafu)?;

    // The Vector aggregator discovery ConfigMap name. It is only required when the Vector agent is
    // enabled for the server.
    let vector_aggregator_config_map_name = scs.spec.vector_aggregator_config_map_name.clone();

    let server_logging =
        validate_logging(&server_config.logging, &vector_aggregator_config_map_name)?;

    Ok(ValidatedSparkConnectServer {
        metadata: scs.meta().clone(),
        name,
        namespace,
        uid,
        resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            resolved_s3: dereferenced.resolved_s3,
        },
        role_config: ValidatedRoleConfig {
            listener_class: scs.spec.server.role_config.listener_class.clone(),
        },
        server_config,
        server_logging,
        executor_config,
    })
}
