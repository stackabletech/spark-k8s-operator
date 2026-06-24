use std::{collections::BTreeMap, str::FromStr};

use serde::{Deserialize, Serialize};
use snafu::Snafu;
use stackable_operator::{
    commons::{
        affinity::StackableAffinity,
        product_image_selection::ProductImage,
        resources::{
            CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
            Resources, ResourcesFragment,
        },
    },
    config::{fragment::Fragment, merge::Merge},
    crd::s3,
    deep_merger::ObjectOverrides,
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    product_logging::{self, spec::Logging},
    role_utils::{GenericRoleConfig, Role},
    schemars::{self, JsonSchema},
    shared::time::Duration,
    v2::{
        config_overrides::KeyValueConfigOverrides,
        role_utils::JavaCommonConfig,
        types::kubernetes::{ConfigMapName, ContainerName, ListenerClassName},
    },
    versioned::versioned,
};
use strum::{Display, EnumIter};

use crate::crd::{
    affinity::history_affinity, constants::DEFAULT_LISTENER_CLASS,
    history::v1alpha1::SparkHistoryServerRoleConfig,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("too many cleaner replicas"))]
    TooManyCleanerReplicas,

    #[snafu(display("too many cleaner role groups: {role_groups}"))]
    TooManyCleanerRoleGroups { role_groups: String },
}

pub type SparkHistoryRoleType = Role<
    HistoryConfigFragment,
    v1alpha1::ConfigOverrides,
    SparkHistoryServerRoleConfig,
    JavaCommonConfig,
>;

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
    /// A Spark cluster history server component. This resource is managed by the Stackable operator
    /// for Apache Spark. Find more information on how to use it in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/history-server).
    #[versioned(crd(
        group = "spark.stackable.tech",
        shortname = "sparkhist",
        shortname = "sparkhistory",
        shortname = "shs",
        namespaced,
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkHistoryServerSpec {
        pub image: ProductImage,

        /// Name of the Vector aggregator discovery ConfigMap.
        /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
        #[serde(skip_serializing_if = "Option::is_none")]
        pub vector_aggregator_config_map_name: Option<ConfigMapName>,

        /// The log file directory definition used by the Spark history server.
        pub log_file_directory: LogFileDirectorySpec,

        /// A map of key/value strings that will be passed directly to Spark when deploying the history server.
        #[serde(default)]
        pub spark_conf: BTreeMap<String, String>,

        // Docs are on the ObjectOverrides struct
        #[serde(default)]
        pub object_overrides: ObjectOverrides,

        /// A history server node role definition.
        pub nodes: SparkHistoryRoleType,
    }

    // TODO: move generic version to op-rs?
    #[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkHistoryServerRoleConfig {
        #[serde(flatten)]
        pub common: GenericRoleConfig,

        /// This field controls which [ListenerClass](https://docs.stackable.tech/home/nightly/listener-operator/listenerclass.html)
        /// is used to expose the history server.
        #[serde(default = "default_listener_class")]
        pub listener_class: ListenerClassName,
    }

    #[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize, Merge)]
    pub struct ConfigOverrides {
        #[serde(default, rename = "spark-defaults.conf")]
        pub spark_defaults_conf: KeyValueConfigOverrides,

        #[serde(default, rename = "spark-env.sh")]
        pub spark_env_sh: KeyValueConfigOverrides,

        #[serde(default, rename = "security.properties")]
        pub security_properties: KeyValueConfigOverrides,
    }
}

impl v1alpha1::SparkHistoryServer {
    /// Returns a reference to the role. Raises an error if the role is not defined.
    pub fn role(&self) -> &SparkHistoryRoleType {
        &self.spec.nodes
    }

    /// Return the listener class of the role config.
    pub fn node_listener_class(&self) -> &ListenerClassName {
        &self.spec.nodes.role_config.listener_class
    }

    // Returns the name of the cleaner role group if any.
    // Raises an error when:
    // * there are multiple cleaner role groups
    // * the cleaner role group has more than one replica.
    pub fn cleaner_rolegroup_name(&self) -> Result<Option<String>, Error> {
        let rgs = self
            .spec
            .nodes
            .role_groups
            .keys()
            .filter(|rg_name| {
                self.spec
                    .nodes
                    .role_groups
                    .get(*rg_name)
                    .and_then(|rg| rg.config.config.cleaner)
                    .unwrap_or(false)
            })
            .cloned()
            .collect::<Vec<_>>();

        match rgs.len() {
            0 => Ok(None),
            1 => match self
                .spec
                .nodes
                .role_groups
                .get(&rgs[0])
                .and_then(|rg| rg.replicas)
            {
                Some(replicas) if replicas > 1 => Err(TooManyCleanerReplicasSnafu.build()),
                _ => Ok(Some(rgs[0].clone())),
            },
            _ => Err(TooManyCleanerRoleGroupsSnafu {
                role_groups: rgs.join(","),
            }
            .build()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize, Display)]
#[serde(rename_all = "camelCase")]
pub enum LogFileDirectorySpec {
    /// An S3 bucket storing the log events
    #[strum(serialize = "s3")]
    S3(S3LogFileDirectorySpec),
    /// A custom log directory
    CustomLogDirectory(String),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3LogFileDirectorySpec {
    pub prefix: String,
    pub bucket: s3::v1alpha1::InlineBucketOrReference,
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

impl SparkHistoryServerContainer {
    /// The type-safe container name for this variant (matching its kebab-case serialization).
    pub fn to_container_name(&self) -> ContainerName {
        ContainerName::from_str(&self.to_string())
            .expect("a SparkHistoryServerContainer variant name is a valid container name")
    }
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

    /// Request secret (currently only autoTls certificates) lifetime from the secret operator, e.g. `7d`, or `30d`.
    /// This can be shortened by the `maxCertificateLifetime` setting on the SecretClass issuing the TLS certificate.
    #[fragment_attrs(serde(default))]
    pub requested_secret_lifetime: Option<Duration>,
}

impl HistoryConfig {
    // Auto TLS certificate lifetime
    const DEFAULT_HISTORY_SECRET_LIFETIME: Duration = Duration::from_days_unchecked(1);

    pub fn default_config(cluster_name: &str) -> HistoryConfigFragment {
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
            requested_secret_lifetime: Some(Self::DEFAULT_HISTORY_SECRET_LIFETIME),
        }
    }
}

impl Default for v1alpha1::SparkHistoryServerRoleConfig {
    fn default() -> Self {
        v1alpha1::SparkHistoryServerRoleConfig {
            listener_class: default_listener_class(),
            common: Default::default(),
        }
    }
}

fn default_listener_class() -> ListenerClassName {
    ListenerClassName::from_str(DEFAULT_LISTENER_CLASS)
        .expect("the default listener class is a valid listener class name")
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use indoc::indoc;
    use stackable_operator::{
        cli::OperatorEnvironmentOptions, commons::tls_verification::TlsClientDetails, crd::s3,
        v2::builder::pod::container::EnvVarName, versioned::test_utils::RoundtripTestData,
    };

    use super::*;
    use crate::{
        crd::logdir::{ResolvedLogDir, S3LogDir},
        history::controller::{
            dereference::DereferencedSparkHistoryServer,
            validate::{ValidatedSparkHistoryServer, validate},
        },
    };

    #[test]
    pub fn test_env_overrides() {
        let input = indoc! {r#"
        ---
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.5.8
          logFileDirectory:
            s3:
              prefix: eventlogs/
              bucket:
                reference: spark-history-s3-bucket
          nodes:
            envOverrides:
              TEST_SPARK_HIST_VAR: ROLE
            roleGroups:
              default:
                replicas: 1
                config:
                  cleaner: true
                envOverrides:
                  TEST_SPARK_HIST_VAR: ROLEGROUP
        "#};

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let history: v1alpha1::SparkHistoryServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let validated: ValidatedSparkHistoryServer = validate(
            &history,
            DereferencedSparkHistoryServer {
                log_dir: ResolvedLogDir::S3(S3LogDir {
                    bucket: s3::v1alpha1::ResolvedBucket {
                        bucket_name: "my-bucket".to_string(),
                        connection: s3::v1alpha1::ConnectionSpec {
                            host: "my-s3".to_string().try_into().unwrap(),
                            port: None,
                            access_style: Default::default(),
                            credentials: None,
                            tls: TlsClientDetails { tls: None },
                            region: Default::default(),
                        },
                    },
                    prefix: "prefix".to_string(),
                }),
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "default".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.stackable.tech/sdp".to_string(),
            },
        )
        .expect("validation should succeed");

        // The role group `envOverrides` value wins over the role-level one.
        let env_overrides = &validated
            .role_groups
            .get(&"default".parse().expect("valid role group name"))
            .expect("default role group should exist")
            .config
            .env_overrides;

        assert_eq!(
            env_overrides
                .get(&EnvVarName::from_str("TEST_SPARK_HIST_VAR").expect("valid env var name"))
                .and_then(|env_var| env_var.value.clone()),
            Some("ROLEGROUP".to_string())
        );
    }

    impl RoundtripTestData for v1alpha1::SparkHistoryServerSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            stackable_operator::utils::yaml_from_str_singleton_map(indoc! {r#"
              - image:
                  productVersion: 3.5.8
                  pullPolicy: IfNotPresent
                logFileDirectory:
                  s3:
                    prefix: eventlogs/
                    bucket:
                      reference: spark-history-s3-bucket
                sparkConf:
                  test.sparkConf: "true"
                nodes:
                  roleConfig:
                    listenerClass: external-unstable
                  envOverrides:
                    TEST_SPARK_HIST_VAR_ROLE: ROLE
                    TEST_SPARK_HIST_VAR_FROM_RG: ROLE
                  configOverrides:
                    security.properties:
                      test.securityProperties.role: role
                      test.securityProperties.fromRg: role
                    spark-env.sh:
                      TEST_SPARK-ENV-SH_ROLE: ROLE
                      TEST_SPARK-ENV-SH_FROM_RG: ROLE
                  roleGroups:
                    default:
                      replicas: 1
                      config:
                        cleaner: true
                      envOverrides:
                        TEST_SPARK_HIST_VAR_FROM_RG: ROLEGROUP
                        TEST_SPARK_HIST_VAR_RG: ROLEGROUP
                      configOverrides:
                        security.properties:
                          test.securityProperties.fromRg: rolegroup
                          test.securityProperties.rg: rolegroup
                        spark-env.sh:
                          TEST_SPARK-ENV-SH_FROM_RG: ROLEGROUP
                          TEST_SPARK-ENV-SH_RG: ROLEGROUP
                      podOverrides:
                        spec:
                          containers:
                            - name: spark-history
                              resources:
                                requests:
                                  cpu: 500m
                                  memory: 512Mi
                                limits:
                                  cpu: 1500m
                                  memory: 1024Mi
            "#})
            .expect("Failed to parse SparkHistoryServerSpec YAML")
        }
    }
}
