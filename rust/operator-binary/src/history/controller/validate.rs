//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image.
//! Does not touch the Kubernetes API.

use std::{borrow::Cow, collections::BTreeMap, str::FromStr};

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    cli::OperatorEnvironmentOptions,
    commons::{
        pdb::PdbConfig,
        product_image_selection::{self, ResolvedProductImage},
    },
    config::fragment,
    k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    kube::Resource,
    kvp::Labels,
    product_logging::spec::Logging,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        builder::{
            meta::ownerreference_from_resource,
            pod::container::{EnvVarName, EnvVarSet},
        },
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        kvp::label::{recommended_labels, role_group_selector},
        product_logging::framework::{
            VectorContainerLogConfig, validate_logging_configuration_for_container,
        },
        role_group_utils::ResourceNames,
        role_utils::{JavaCommonConfig, RoleGroupConfig, with_validated_config},
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
    crd::{
        constants::{
            CONTAINER_IMAGE_BASE_NAME, HISTORY_APP_NAME, HISTORY_CONTROLLER_NAME,
            HISTORY_ROLE_NAME, OPERATOR_NAME,
        },
        history::{HistoryConfig, HistoryConfigFragment, SparkHistoryServerContainer, v1alpha1},
        logdir::ResolvedLogDir,
    },
    history::controller::dereference::DereferencedSparkHistoryServer,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to resolve product image"))]
    ResolveProductImage {
        source: product_image_selection::Error,
    },

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

    #[snafu(display("invalid cleaner configuration"))]
    InvalidCleanerConfiguration { source: crate::crd::history::Error },

    #[snafu(display("invalid log directory settings"))]
    InvalidLogDirSettings { source: crate::crd::logdir::Error },

    #[snafu(display("invalid role group name {role_group}"))]
    ParseRoleGroupName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
    },

    #[snafu(display("failed to resolve and merge config for role group {role_group}"))]
    FailedToResolveConfig {
        source: fragment::ValidationError,
        role_group: String,
    },

    #[snafu(display("invalid environment variable override name in role group {role_group}"))]
    ParseEnvVarName {
        source: stackable_operator::v2::macros::attributed_string_type::Error,
        role_group: String,
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
    logging: &Logging<SparkHistoryServerContainer>,
    vector_aggregator_config_map_name: &Option<ConfigMapName>,
) -> Result<ValidatedLogging> {
    let vector_container = if logging.enable_vector_agent {
        let vector_aggregator_config_map_name = vector_aggregator_config_map_name
            .clone()
            .context(MissingVectorAggregatorConfigMapNameSnafu)?;
        Some(VectorContainerLogConfig {
            log_config: validate_logging_configuration_for_container(
                logging,
                &SparkHistoryServerContainer::Vector,
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

/// A validated, merged history server role-group config.
pub type HistoryRoleGroupConfig =
    RoleGroupConfig<HistoryConfig, JavaCommonConfig, v1alpha1::ConfigOverrides>;

/// A pre-validated history server role group: the per-role-group merge products that the build
/// steps used to recompute from the raw CRD on every reconcile.
pub struct ValidatedHistoryRoleGroup {
    pub config: HistoryRoleGroupConfig,
    pub logging: ValidatedLogging,
}

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

pub struct ValidatedSparkHistoryServer {
    metadata: ObjectMeta,
    pub name: ClusterName,
    pub namespace: NamespaceName,
    pub uid: Uid,
    /// The product version as a valid label value, used for the recommended
    /// `app.kubernetes.io/version` label. Derived from the resolved image's app version label
    /// value.
    pub product_version: ProductVersion,
    pub resolved_product_image: ResolvedProductImage,
    pub cluster_config: ValidatedClusterConfig,
    pub role_config: ValidatedRoleConfig,
    pub role_groups: BTreeMap<RoleGroupName, ValidatedHistoryRoleGroup>,
}

/// Cluster-wide settings resolved during validation and dereferencing, so the resource builders
/// never have to read the raw [`v1alpha1::SparkHistoryServer`] spec.
pub struct ValidatedClusterConfig {
    pub cleaner_rolegroup_name: Option<String>,
    pub spark_conf: BTreeMap<String, String>,
    /// The resolved log directory.
    pub log_dir: ResolvedLogDir,
    /// Spark configuration properties that configure event logging into the `log_dir`.
    pub log_dir_settings: BTreeMap<String, String>,
}

/// Per-role configuration extracted during validation.
pub struct ValidatedRoleConfig {
    pub pdb: PdbConfig,
    pub listener_class: ListenerClassName,
}

impl ValidatedSparkHistoryServer {
    /// The single history server role name (`node`).
    pub fn role_name() -> RoleName {
        RoleName::from_str(HISTORY_ROLE_NAME).expect("HISTORY_ROLE_NAME is a valid role name")
    }

    /// Type-safe names for the resources of a given role group.
    pub(crate) fn resource_names(&self, role_group_name: &RoleGroupName) -> ResourceNames {
        ResourceNames {
            cluster_name: self.name.clone(),
            role_name: Self::role_name(),
            role_group_name: role_group_name.clone(),
        }
    }

    /// Recommended labels for a role-group resource, using the given product version.
    fn recommended_labels_for(
        &self,
        product_version: &ProductVersion,
        role_group_name: &RoleGroupName,
    ) -> Labels {
        recommended_labels(
            self,
            &product_name(),
            product_version,
            &operator_name(),
            &controller_name(),
            &Self::role_name(),
            role_group_name,
        )
    }

    /// Recommended labels for a role-group resource.
    pub fn recommended_labels(&self, role_group_name: &RoleGroupName) -> Labels {
        self.recommended_labels_for(&self.product_version, role_group_name)
    }

    /// Selector labels matching the pods of a role group.
    pub fn role_group_selector(&self, role_group_name: &RoleGroupName) -> Labels {
        role_group_selector(self, &product_name(), &Self::role_name(), role_group_name)
    }

    /// Object metadata for a child resource named `name`, owned by this SparkHistoryServer and
    /// carrying the recommended labels for the given role group. Returns the builder so callers can
    /// add extra labels (e.g. Prometheus annotations) before building.
    pub(crate) fn object_meta(
        &self,
        name: impl Into<String>,
        role_group_name: &RoleGroupName,
    ) -> ObjectMetaBuilder {
        let mut builder = ObjectMetaBuilder::new();
        builder
            .name_and_namespace(self)
            .name(name)
            .ownerreference(ownerreference_from_resource(self, None, Some(true)))
            .with_labels(self.recommended_labels(role_group_name));
        builder
    }
}

/// The product name (`spark-history`) as a type-safe label value.
pub(crate) fn product_name() -> ProductName {
    ProductName::from_str(HISTORY_APP_NAME).expect("HISTORY_APP_NAME is a valid product name")
}

/// The operator name as a type-safe label value.
pub(crate) fn operator_name() -> OperatorName {
    OperatorName::from_str(OPERATOR_NAME).expect("the operator name is a valid label value")
}

/// The controller name as a type-safe label value.
pub(crate) fn controller_name() -> ControllerName {
    ControllerName::from_str(HISTORY_CONTROLLER_NAME)
        .expect("the controller name is a valid label value")
}

impl NameIsValidLabelValue for ValidatedSparkHistoryServer {
    fn to_label_value(&self) -> String {
        self.name.to_label_value()
    }
}

impl HasName for ValidatedSparkHistoryServer {
    fn to_name(&self) -> String {
        String::from(&self.name)
    }
}

impl HasUid for ValidatedSparkHistoryServer {
    fn to_uid(&self) -> Uid {
        self.uid.clone()
    }
}

impl Resource for ValidatedSparkHistoryServer {
    type DynamicType = ();
    type Scope = <v1alpha1::SparkHistoryServer as Resource>::Scope;

    fn kind(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::kind(&())
    }

    fn group(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::group(&())
    }

    fn version(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::version(&())
    }

    fn plural(_: &Self::DynamicType) -> Cow<'_, str> {
        <v1alpha1::SparkHistoryServer as Resource>::plural(&())
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

pub fn validate(
    shs: &v1alpha1::SparkHistoryServer,
    dereferenced: DereferencedSparkHistoryServer,
    operator_environment: &OperatorEnvironmentOptions,
) -> Result<ValidatedSparkHistoryServer> {
    let resolved_product_image = shs
        .spec
        .image
        .resolve(
            CONTAINER_IMAGE_BASE_NAME,
            &operator_environment.image_repository,
            crate::built_info::PKG_VERSION,
        )
        .context(ResolveProductImageSnafu)?;

    let name = get_cluster_name(shs).context(ResolveClusterNameSnafu)?;
    let namespace = get_namespace(shs).context(ResolveNamespaceSnafu)?;
    let uid = get_uid(shs).context(ResolveUidSnafu)?;

    let cleaner_rolegroup_name = shs
        .cleaner_rolegroup_name()
        .context(InvalidCleanerConfigurationSnafu)?;

    let log_dir_settings = dereferenced
        .log_dir
        .history_server_spark_config()
        .context(InvalidLogDirSettingsSnafu)?;

    // `app_version_label_value` is constructed to be a valid label value, so it is also a valid
    // `ProductVersion`.
    let product_version = ProductVersion::from_str(&resolved_product_image.app_version_label_value)
        .expect("the app version label value is a valid product version");

    // The Vector aggregator discovery ConfigMap name. It is only required when the Vector agent is
    // enabled for a role group.
    let vector_aggregator_config_map_name = shs.spec.vector_aggregator_config_map_name.clone();

    let role = shs.role();
    let default_config = HistoryConfig::default_config(name.as_ref());

    let mut role_groups = BTreeMap::new();
    for (rg_name, role_group) in &shs.spec.nodes.role_groups {
        let role_group_name =
            RoleGroupName::from_str(rg_name).with_context(|_| ParseRoleGroupNameSnafu {
                role_group: rg_name.clone(),
            })?;

        let merged = with_validated_config::<
            HistoryConfig,
            JavaCommonConfig,
            HistoryConfigFragment,
            v1alpha1::SparkHistoryServerRoleConfig,
            v1alpha1::ConfigOverrides,
        >(role_group, role, &default_config)
        .with_context(|_| FailedToResolveConfigSnafu {
            role_group: rg_name.clone(),
        })?;

        let mut env_overrides = EnvVarSet::new();
        for (env_var_name, env_var_value) in merged.config.env_overrides {
            env_overrides = env_overrides.with_value(
                &EnvVarName::from_str(&env_var_name).with_context(|_| ParseEnvVarNameSnafu {
                    role_group: rg_name.clone(),
                })?,
                env_var_value,
            );
        }

        let logging = validate_logging(
            &merged.config.config.logging,
            &vector_aggregator_config_map_name,
        )?;

        let config = HistoryRoleGroupConfig {
            replicas: Some(merged.replicas.unwrap_or(1)),
            config: merged.config.config,
            config_overrides: merged.config.config_overrides,
            env_overrides,
            // The history server does not use CLI overrides; the field is carried (and merged
            // upstream) but unused.
            cli_overrides: merged.config.cli_overrides,
            pod_overrides: merged.config.pod_overrides,
            product_specific_common_config: merged.config.product_specific_common_config,
        };

        role_groups.insert(
            role_group_name,
            ValidatedHistoryRoleGroup { config, logging },
        );
    }

    Ok(ValidatedSparkHistoryServer {
        metadata: shs.meta().clone(),
        name,
        namespace,
        uid,
        product_version,
        resolved_product_image,
        cluster_config: ValidatedClusterConfig {
            cleaner_rolegroup_name,
            spark_conf: shs.spec.spark_conf.clone(),
            log_dir: dereferenced.log_dir,
            log_dir_settings,
        },
        role_config: ValidatedRoleConfig {
            pdb: shs
                .spec
                .nodes
                .role_config
                .common
                .pod_disruption_budget
                .clone(),
            listener_class: shs.node_listener_class().clone(),
        },
        role_groups,
    })
}
