//! The validate step in the SparkHistoryServer controller.
//!
//! Resolves the product image.
//! Does not touch the Kubernetes API.

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    commons::product_image_selection::{self, ResolvedProductImage},
    config::merge::Merge,
    k8s_openapi::{
        DeepMerge, api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta,
    },
    kube::{Resource, runtime::reflector::ObjectRef},
    kvp::Labels,
    role_utils::RoleGroupRef,
    v2::{
        HasName, HasUid, NameIsValidLabelValue,
        controller_utils::{get_cluster_name, get_namespace, get_uid},
        kvp::label::{recommended_labels, role_group_selector},
        role_group_utils::ResourceNames,
        types::{
            kubernetes::{NamespaceName, Uid},
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
        history::{HistoryConfig, v1alpha1},
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
        source: crate::crd::history::Error,
        role_group: String,
    },

    #[snafu(display("cannot retrieve role group {role_group}"))]
    CannotRetrieveRoleGroup {
        source: crate::crd::history::Error,
        role_group: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// A pre-validated history server role group: the per-role-group merge products that the build
/// steps used to recompute from the raw CRD on every reconcile.
pub struct ValidatedHistoryRoleGroup {
    pub config: HistoryConfig,
    pub config_overrides: v1alpha1::ConfigOverrides,
    pub env_overrides: HashMap<String, String>,
    pub pod_overrides: PodTemplateSpec,
    pub replicas: Option<i32>,
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
    pub cleaner_rolegroup_name: Option<String>,
    pub spark_conf: BTreeMap<String, String>,
    pub resolved_product_image: ResolvedProductImage,
    // These two are a bit redundant right now.
    // This is a temporary situation until we remove all v1alpha1::SparkHistoryServer usages after validation.
    // Currently log_dir_settings is needed for  history::controller::build_configmap() function whereas log_dir
    // is needed for command args and volume mounts.
    pub log_dir: ResolvedLogDir,
    pub log_dir_settings: BTreeMap<String, String>,
    pub role_groups: BTreeMap<RoleGroupName, ValidatedHistoryRoleGroup>,
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

    let mut role_groups = BTreeMap::new();
    for rg_name in shs.spec.nodes.role_groups.keys() {
        let role_group_name =
            RoleGroupName::from_str(rg_name).with_context(|_| ParseRoleGroupNameSnafu {
                role_group: rg_name.clone(),
            })?;

        // A temporary reference used purely as the merge key for the existing CRD accessors.
        let rgr = RoleGroupRef {
            cluster: ObjectRef::from_obj(shs),
            role: HISTORY_ROLE_NAME.to_string(),
            role_group: rg_name.clone(),
        };

        let config = shs
            .merged_config(&rgr)
            .with_context(|_| FailedToResolveConfigSnafu {
                role_group: rg_name.clone(),
            })?;

        let role_group = shs
            .rolegroup(&rgr)
            .with_context(|_| CannotRetrieveRoleGroupSnafu {
                role_group: rg_name.clone(),
            })?;

        // Merge config_overrides from both nodes and role group levels.
        let mut config_overrides = role_group.config.config_overrides;
        config_overrides.merge(&shs.spec.nodes.config.config_overrides);

        // Merge pod_overrides: role-base first, then role-group on top.
        let mut pod_overrides = shs.role().config.pod_overrides.clone();
        pod_overrides.merge_from(role_group.config.pod_overrides);

        role_groups.insert(
            role_group_name,
            ValidatedHistoryRoleGroup {
                config,
                config_overrides,
                env_overrides: role_group.config.env_overrides,
                pod_overrides,
                replicas: shs.replicas(&rgr),
            },
        );
    }

    Ok(ValidatedSparkHistoryServer {
        metadata: shs.meta().clone(),
        name,
        namespace,
        uid,
        product_version,
        cleaner_rolegroup_name,
        spark_conf: shs.spec.spark_conf.clone(),
        log_dir: dereferenced.log_dir,
        log_dir_settings,
        resolved_product_image,
        role_groups,
    })
}
