use const_format::concatcp;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::{
        cluster_operation::ClusterOperation,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    product_logging::{self, spec::Logging},
    role_utils::{CommonConfiguration, JavaCommonConfig},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
    time::Duration,
};
use stackable_versioned::versioned;
use strum::{Display, EnumIter};

pub const CONNECT_CONTROLLER_NAME: &str = "connect";
pub const CONNECT_FULL_CONTROLLER_NAME: &str = concatcp!(
    CONNECT_CONTROLLER_NAME,
    '.',
    crate::crd::constants::OPERATOR_NAME
);
pub const CONNECT_SERVER_ROLE_NAME: &str = "server";
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
    ConstructJvmArguments,
}

#[versioned(version(name = "v1alpha1"))]
pub mod versioned {
    /// A Spark cluster connect server component. This resource is managed by the Stackable operator
    /// for Apache Spark. Find more information on how to use it in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/connect-server).
    #[versioned(k8s(
        group = "spark.stackable.tech",
        kind = "SparkConnectServer",
        plural = "sparkconnectservers",
        shortname = "sparkconnect",
        status = "SparkConnectServerStatus",
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

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        /// Name of the Vector aggregator discovery ConfigMap.
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// A connect server definition.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub server: Option<CommonConfiguration<ServerConfigFragment, JavaCommonConfig>>,
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
pub struct ServerConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<ConnectStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<SparkConnectServerContainer>,

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

impl ServerConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_CONNECT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    fn default_config() -> ServerConfigFragment {
        ServerConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("250m".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1024Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: ConnectStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            requested_secret_lifetime: Some(Self::DEFAULT_CONNECT_SECRET_LIFETIME),
        }
    }
}

// This is the equivalent to merged_config() in other ops
// only here we only need to merge operator defaults with
// user configuration.
impl v1alpha1::SparkConnectServer {
    pub fn server_config(&self) -> Result<ServerConfig, Error> {
        let defaults = ServerConfig::default_config();
        fragment::validate(
            match self.spec.server.as_ref().map(|cc| cc.config.clone()) {
                Some(fragment) => {
                    let mut fc = fragment.clone();
                    fc.merge(&defaults);
                    fc
                }
                _ => defaults,
            },
        )
        .context(FragmentValidationFailureSnafu)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkConnectServerStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for v1alpha1::SparkConnectServer {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}
