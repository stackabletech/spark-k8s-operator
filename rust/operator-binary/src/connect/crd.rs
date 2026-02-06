use const_format::concatcp;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::{
        affinity::{StackableAffinity, StackableAffinityFragment, affinity_between_role_pods},
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
    crd::s3,
    deep_merger::ObjectOverrides,
    k8s_openapi::{api::core::v1::PodAntiAffinity, apimachinery::pkg::api::resource::Quantity},
    kube::{CustomResource, ResourceExt},
    product_logging::{
        self,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig, Logging,
        },
    },
    role_utils::{CommonConfiguration, JavaCommonConfig},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    status::condition::{ClusterCondition, HasStatusCondition},
    versioned::versioned,
};
use strum::{Display, EnumIter};

use super::common::SparkConnectRole;

pub const CONNECT_CONTROLLER_NAME: &str = "connect";
pub const CONNECT_FULL_CONTROLLER_NAME: &str = concatcp!(
    CONNECT_CONTROLLER_NAME,
    '.',
    crate::crd::constants::OPERATOR_NAME
);
pub const CONNECT_SERVER_ROLE_NAME: &str = "server";
pub const CONNECT_EXECUTOR_ROLE_NAME: &str = "executor";
pub const CONNECT_GRPC_PORT: i32 = 15002;
pub const CONNECT_UI_PORT: i32 = 4040;

pub const DEFAULT_SPARK_CONNECT_GROUP_NAME: &str = "default";

pub const CONNECT_APP_NAME: &str = "spark-connect";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
}

#[versioned(
    version(name = "v1alpha1"),
    crates(
        kube_core = "stackable_operator::kube::core",
        kube_client = "stackable_operator::kube::client",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars",
        versioned = "stackable_operator::versioned"
    )
)]
pub mod versioned {
    /// An Apache Spark Connect server component. This resource is managed by the Stackable operator
    /// for Apache Spark. Find more information on how to use it in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/connect-server).
    #[versioned(crd(
        group = "spark.stackable.tech",
        plural = "sparkconnectservers",
        shortname = "sparkconnect",
        status = "SparkConnectServerStatus",
        namespaced,
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkConnectServerSpec {
        pub image: ProductImage,

        // no doc string - See ClusterOperation struct
        #[serde(default)]
        pub cluster_operation: ClusterOperation,

        /// One or more S3 connections to be used by the Spark Connect server.
        #[serde(default)]
        connectors: Connectors,

        // Docs are on the ObjectOverrides struct
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        /// User provided command line arguments appended to the server entry point.
        #[serde(default)]
        pub args: Vec<String>,

        /// Name of the Vector aggregator discovery ConfigMap.
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<String>,

        /// A Spark Connect server definition.
        #[serde(default)]
        pub server: SparkConnectServerConfigWrapper,

        /// Spark Connect executor properties.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub executor: Option<CommonConfiguration<ExecutorConfigFragment, JavaCommonConfig>>,
    }

    /// This struct is a wrapper for the `ServerConfig` in order to keep the `spec.server.roleConfig` setting consistent.
    /// It is required since Spark Connect does not utilize the Stackable `Role` and therefore does not offer a `roleConfig`.
    #[derive(Clone, Debug, Default, JsonSchema, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkConnectServerConfigWrapper {
        #[serde(flatten)]
        pub config: Option<CommonConfiguration<ServerConfigFragment, JavaCommonConfig>>,
        #[serde(default)]
        pub role_config: SparkConnectServerRoleConfig,
    }

    /// Global role config settings for the Spark Connect Server.
    #[derive(Clone, Debug, JsonSchema, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct SparkConnectServerRoleConfig {
        /// This field controls which [ListenerClass](DOCS_BASE_URL_PLACEHOLDER/listener-operator/listenerclass.html)
        /// is used to expose the Spark Connect services.
        #[serde(default = "default_listener_class")]
        pub listener_class: String,
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
        pub resources: Resources<crate::connect::crd::ConnectStorageConfig, NoRuntimeLimits>,

        #[fragment_attrs(serde(default))]
        pub logging: Logging<SparkConnectContainer>,

        /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
        /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
        #[fragment_attrs(serde(default))]
        pub requested_secret_lifetime: Option<Duration>,
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
    pub struct ExecutorConfig {
        #[fragment_attrs(serde(default))]
        pub resources: Resources<crate::connect::crd::ConnectStorageConfig, NoRuntimeLimits>,
        #[fragment_attrs(serde(default))]
        pub logging: Logging<crate::connect::crd::SparkConnectContainer>,
        #[fragment_attrs(serde(default))]
        pub affinity: StackableAffinity,

        /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
        /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
        #[fragment_attrs(serde(default))]
        pub requested_secret_lifetime: Option<Duration>,
    }

    #[derive(Clone, Debug, Default, JsonSchema, PartialEq, Deserialize, Serialize)]
    struct Connectors {
        #[serde(default)]
        pub s3: Vec<s3::v1alpha1::InlineBucketOrReference>,
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
pub(crate) struct ConnectStorageConfig {}

#[derive(
    Clone,
    Debug,
    Deserialize,
    Display,
    EnumIter,
    Eq,
    JsonSchema,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub(crate) enum SparkConnectContainer {
    Spark,
    Vector,
}

impl v1alpha1::ServerConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_CONNECT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    fn default_config() -> v1alpha1::ServerConfigFragment {
        v1alpha1::ServerConfigFragment {
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

    pub fn log_config_map(&self) -> Option<String> {
        let container_log_config = self
            .logging
            .containers
            .get(&SparkConnectContainer::Spark)
            .cloned();

        match container_log_config {
            Some(ContainerLogConfig {
                choice:
                    Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                        custom: ConfigMapLogConfig { config_map },
                    })),
            }) => Some(config_map.clone()),
            _ => None,
        }
    }
}

// This is the equivalent to merged_config() in other ops
// only here we only need to merge operator defaults with
// user configuration.
impl v1alpha1::SparkConnectServer {
    pub fn server_config(&self) -> Result<v1alpha1::ServerConfig, Error> {
        let defaults = v1alpha1::ServerConfig::default_config();
        fragment::validate(
            match self.spec.server.config.as_ref().map(|cc| cc.config.clone()) {
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

    pub fn executor_config(&self) -> Result<v1alpha1::ExecutorConfig, Error> {
        let defaults = v1alpha1::ExecutorConfig::default_config(&self.name_unchecked());
        fragment::validate(
            match self.spec.executor.as_ref().map(|cc| cc.config.clone()) {
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

impl Default for v1alpha1::SparkConnectServerRoleConfig {
    fn default() -> Self {
        v1alpha1::SparkConnectServerRoleConfig {
            listener_class: default_listener_class(),
        }
    }
}

pub fn default_listener_class() -> String {
    "cluster-internal".to_string()
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

impl v1alpha1::ExecutorConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_CONNECT_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    fn default_config(cluster_name: &str) -> v1alpha1::ExecutorConfigFragment {
        v1alpha1::ExecutorConfigFragment {
            resources: ResourcesFragment {
                cpu: CpuLimitsFragment {
                    min: Some(Quantity("1".to_owned())),
                    max: Some(Quantity("1".to_owned())),
                },
                memory: MemoryLimitsFragment {
                    limit: Some(Quantity("1024M".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
                storage: ConnectStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: v1alpha1::ExecutorConfig::affinity(cluster_name),

            requested_secret_lifetime: Some(Self::DEFAULT_CONNECT_SECRET_LIFETIME),
        }
    }

    pub fn log_config_map(&self) -> Option<String> {
        let container_log_config = self
            .logging
            .containers
            .get(&SparkConnectContainer::Spark)
            .cloned();

        match container_log_config {
            Some(ContainerLogConfig {
                choice:
                    Some(ContainerLogConfigChoice::Custom(CustomContainerLogConfig {
                        custom: ConfigMapLogConfig { config_map },
                    })),
            }) => Some(config_map.clone()),
            _ => None,
        }
    }

    fn affinity(cluster_name: &str) -> StackableAffinityFragment {
        let affinity_between_role_pods = affinity_between_role_pods(
            CONNECT_APP_NAME,
            cluster_name,
            &SparkConnectRole::Executor.to_string(),
            70,
        );

        StackableAffinityFragment {
            pod_affinity: None,
            pod_anti_affinity: Some(PodAntiAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    affinity_between_role_pods,
                ]),
                required_during_scheduling_ignored_during_execution: None,
            }),
            node_affinity: None,
            node_selector: None,
        }
    }
}
#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_cr_minimal_deserialization() {
        let _spark_connect_cr = serde_yaml::from_str::<v1alpha1::SparkConnectServer>(indoc! { r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkConnectServer
        metadata:
          name: spark-connect
        spec:
          image:
            productVersion: 4.1.1
        "# })
        .expect("Failed to deserialize minimal SparkConnectServer CR");
    }

    #[test]
    fn test_cr_s3_deserialization() {
        let input = indoc! { r#"
          ---
          apiVersion: spark.stackable.tech/v1alpha1
          kind: SparkConnectServer
          metadata:
            name: spark-connect
          spec:
            image:
              productVersion: 4.1.1
            connectors:
              s3:
                - reference: my-s3-bucket
        "# };

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let _spark_connect_cr: v1alpha1::SparkConnectServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
                .expect("Failed to deserialize SparkConnectServer with S3 connectors CR");
    }
}
