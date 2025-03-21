use std::collections::{BTreeMap, HashMap};

use const_format::concatcp;
use product_config::{types::PropertyNameKind, ProductConfigManager};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        product_image_selection::{ProductImage, ResolvedProductImage},
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::api::resource::Quantity},
    kube::{CustomResource, ResourceExt},
    product_config_utils::{
        transform_all_roles_to_config, validate_all_roles_and_groups_config, Configuration,
        ValidatedRoleConfigByPropertyKind,
    },
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, JavaCommonConfig, Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
    time::Duration,
};
use stackable_versioned::versioned;
use strum::{Display, EnumIter};

use crate::{
    connect::{affinity::affinity, jvm::construct_jvm_args},
    crd::constants::{
        JVM_SECURITY_PROPERTIES_FILE, OPERATOR_NAME, SPARK_DEFAULTS_FILE_NAME,
        SPARK_ENV_SH_FILE_NAME, VOLUME_MOUNT_PATH_LOG,
    },
};

pub const CONNECT_CONTROLLER_NAME: &str = "connect";
pub const CONNECT_FULL_CONTROLLER_NAME: &str =
    concatcp!(CONNECT_CONTROLLER_NAME, '.', OPERATOR_NAME);
pub const CONNECT_ROLE_NAME: &str = "node";
pub const CONNECT_CONTAINER_NAME: &str = "spark-connect";
pub const CONNECT_GRPC_PORT: i32 = 15002;
pub const CONNECT_UI_PORT: i32 = 4040;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },

    #[snafu(display("the role group {role_group} is not defined"))]
    CannotRetrieveRoleGroup { role_group: String },

    #[snafu(display("failed to construct JVM arguments"))]
    ConstructJvmArguments { source: crate::connect::jvm::Error },
}

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// A Spark cluster connect server component. This resource is managed by the Stackable operator
    /// for Apache Spark. Find more information on how to use it in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/connect-server).
    #[versioned(k8s(
        group = "spark.stackable.tech",
        shortname = "sparkconn",
        namespaced,
        crates(
            kube_core = "stackable_operator::kube::core",
            k8s_openapi = "stackable_operator::k8s_openapi",
            schemars = "stackable_operator::schemars"
        )
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkConnectServerSpec {
        pub image: ProductImage,

        /// Global Spark connect server configuration that applies to all roles and role groups.
        #[serde(default)]
        pub cluster_config: v1alpha1::SparkConnectServerClusterConfig,

        /// Name of the Vector aggregator discovery ConfigMap.
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// A map of key/value strings that will be passed directly to Spark when deploying the connect server.
        #[serde(default)]
        pub spark_conf: BTreeMap<String, String>,

        /// A connect server node role definition.
        pub nodes: Role<ConnectConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    }

    #[derive(Clone, Deserialize, Debug, Default, Eq, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkConnectServerClusterConfig {
        /// This field controls which type of Service the Operator creates for this ConnectServer:
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

impl v1alpha1::SparkConnectServer {
    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(&self) -> &Role<ConnectConfigFragment, GenericRoleConfig, JavaCommonConfig> {
        &self.spec.nodes
    }

    /// Returns a reference to the role group. Raises an error if the role or role group are not defined.
    pub fn rolegroup(
        &self,
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<RoleGroup<ConnectConfigFragment, JavaCommonConfig>, Error> {
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
        rolegroup_ref: &RoleGroupRef<Self>,
    ) -> Result<ConnectConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = ConnectConfig::default_config(&self.name_any());

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

    pub fn validated_role_config(
        &self,
        resolved_product_image: &ResolvedProductImage,
        product_config: &ProductConfigManager,
    ) -> Result<ValidatedRoleConfigByPropertyKind, Error> {
        #[allow(clippy::type_complexity)]
        let roles_to_validate: HashMap<
            String,
            (
                Vec<PropertyNameKind>,
                Role<ConnectConfigFragment, GenericRoleConfig, JavaCommonConfig>,
            ),
        > = vec![(
            CONNECT_ROLE_NAME.to_string(),
            (
                vec![
                    PropertyNameKind::File(SPARK_DEFAULTS_FILE_NAME.to_string()),
                    PropertyNameKind::File(SPARK_ENV_SH_FILE_NAME.to_string()),
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

    pub fn merged_env(
        &self,
        role_group: &str,
        role_group_env_overrides: HashMap<String, String>,
    ) -> Result<Vec<EnvVar>, Error> {
        let role = self.role();
        let connect_jvm_args =
            construct_jvm_args(role, role_group).context(ConstructJvmArgumentsSnafu)?;
        let mut envs = BTreeMap::from([
            // Needed by the `containerdebug` running in the background of the connect container
            // to log it's tracing information to.
            (
                "CONTAINERDEBUG_LOG_DIRECTORY".to_string(),
                format!("{VOLUME_MOUNT_PATH_LOG}/containerdebug"),
            ),
            // This env var prevents the connect server from detaching itself from the
            // start script because this leads to the Pod terminating immediately.
            ("SPARK_NO_DAEMONIZE".to_string(), "true".to_string()),
            (
                "SPARK_DAEMON_CLASSPATH".to_string(),
                "/stackable/spark/extra-jars/*".to_string(),
            ),
            // TODO: There is no SPARK_CONNECT_OPTS env var.
            ("SPARK_DAEMON_JAVA_OPTS".to_string(), connect_jvm_args),
        ]);

        envs.extend(role.config.env_overrides.clone());
        envs.extend(role_group_env_overrides);

        Ok(envs
            .into_iter()
            .map(|(name, value)| EnvVar {
                name: name.to_owned(),
                value: Some(value.to_owned()),
                value_from: None,
            })
            .collect())
    }
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
pub struct ConnectStorageConfig {}

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
pub enum SparkConnectServerContainer {
    SparkConnect,
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
pub struct ConnectConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleaner: Option<bool>,
    #[fragment_attrs(serde(default))]
    pub resources: Resources<ConnectStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<SparkConnectServerContainer>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

impl ConnectConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_CONNECT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    fn default_config(cluster_name: &str) -> ConnectConfigFragment {
        ConnectConfigFragment {
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
                storage: ConnectStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: affinity(cluster_name),
            requested_secret_lifetime: Some(Self::DEFAULT_CONNECT_SECRET_LIFETIME),
        }
    }
}

impl Configuration for ConnectConfigFragment {
    type Configurable = v1alpha1::SparkConnectServer;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }
}
