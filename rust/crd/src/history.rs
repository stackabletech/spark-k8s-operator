use crate::{affinity::history_affinity, constants::*};

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        product_image_selection::{ProductImage, ResolvedProductImage},
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
        s3::S3BucketDef,
    },
    config::{
        fragment,
        fragment::{Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    kube::{runtime::reflector::ObjectRef, ResourceExt},
    product_config::{types::PropertyNameKind, ProductConfigManager},
    product_config_utils::{
        transform_all_roles_to_config, validate_all_roles_and_groups_config, Configuration,
        ValidatedRoleConfigByPropertyKind,
    },
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumIter};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
    #[snafu(display("the role group {role_group} is not defined"))]
    CannotRetrieveRoleGroup { role_group: String },
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkHistoryServer",
    shortname = "shs",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct SparkHistoryServerSpec {
    pub image: ProductImage,
    /// Global Spark history server configuration that applies to all roles and role groups
    #[serde(default)]
    pub cluster_config: SparkHistoryServerClusterConfig,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    pub log_file_directory: LogFileDirectorySpec,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<BTreeMap<String, String>>,
    pub nodes: Role<HistoryConfigFragment>,
}

#[derive(Clone, Deserialize, Debug, Default, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkHistoryServerClusterConfig {
    /// This field controls which type of Service the Operator creates for this HistoryServer:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// * external-stable: Use a LoadBalancer service
    ///
    /// This is a temporary solution with the goal to keep yaml manifests forward compatible.
    /// In the future, this setting will control which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service, and ListenerClass names will stay the same, allowing for a non-breaking change.
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
    #[serde(rename = "external-stable")]
    ExternalStable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
            CurrentlySupportedListenerClasses::ExternalStable => "LoadBalancer".to_string(),
        }
    }
}

impl SparkHistoryServer {
    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(&self) -> &Role<HistoryConfigFragment> {
        &self.spec.nodes
    }

    /// Returns a reference to the role group. Raises an error if the role or role group are not defined.
    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<SparkHistoryServer>,
    ) -> Result<RoleGroup<HistoryConfigFragment>, Error> {
        self.spec
            .nodes
            .role_groups
            .get(&rolegroup_ref.role_group)
            .with_context(|| CannotRetrieveRoleGroupSnafu {
                role_group: rolegroup_ref.role_group.to_owned(),
            })
            .cloned()
    }

    pub fn merged_config(
        &self,
        rolegroup_ref: &RoleGroupRef<SparkHistoryServer>,
    ) -> Result<HistoryConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = HistoryConfig::default_config(&self.name_any());

        let role = &self.spec.nodes;

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
            .role_groups
            .get(&rolegroup_ref.role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }

    pub fn replicas(&self, rolegroup_ref: &RoleGroupRef<Self>) -> Option<i32> {
        self.spec
            .nodes
            .role_groups
            .get(&rolegroup_ref.role_group)
            .and_then(|rg| rg.replicas)
            .map(i32::from)
    }

    pub fn cleaner_rolegroups(&self) -> Vec<RoleGroupRef<SparkHistoryServer>> {
        let mut rgs = vec![];
        for (rg_name, rg_config) in &self.spec.nodes.role_groups {
            if let Some(true) = rg_config.config.config.cleaner {
                rgs.push(RoleGroupRef {
                    cluster: ObjectRef::from_obj(self),
                    role: HISTORY_ROLE_NAME.into(),
                    role_group: rg_name.into(),
                });
            }
        }
        rgs
    }

    pub fn validated_role_config(
        &self,
        resolved_product_image: &ResolvedProductImage,
        product_config: &ProductConfigManager,
    ) -> Result<ValidatedRoleConfigByPropertyKind, Error> {
        let roles_to_validate: HashMap<
            String,
            (Vec<PropertyNameKind>, Role<HistoryConfigFragment>),
        > = vec![(
            HISTORY_ROLE_NAME.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_DEFAULTS_FILE_NAME.to_string()),
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                ],
                self.spec.nodes.clone(),
            ),
        )]
        .into_iter()
        .collect();

        let role_config = transform_all_roles_to_config(self, roles_to_validate);

        validate_all_roles_and_groups_config(
            &resolved_product_image.product_version,
            &role_config.context(ProductConfigTransformSnafu)?,
            product_config,
            false,
            false,
        )
        .context(InvalidProductConfigSnafu)
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Display)]
#[serde(rename_all = "camelCase")]
pub enum LogFileDirectorySpec {
    #[strum(serialize = "s3")]
    S3(S3LogFileDirectorySpec),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3LogFileDirectorySpec {
    pub prefix: String,
    pub bucket: S3BucketDef,
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
    allow(clippy::derive_partial_eq_without_eq),
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
pub struct HistoryStorageConfig {}

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
pub enum SparkHistoryServerContainer {
    SparkHistory,
    Vector,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
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
pub struct HistoryConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleaner: Option<bool>,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<HistoryStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<SparkHistoryServerContainer>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl HistoryConfig {
    fn default_config(cluster_name: &str) -> HistoryConfigFragment {
        HistoryConfigFragment {
            cleaner: None,
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: HistoryStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: history_affinity(cluster_name),
        }
    }
}

impl Configuration for HistoryConfigFragment {
    type Configurable = SparkHistoryServer;

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
