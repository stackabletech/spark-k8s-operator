use crate::s3logdir::S3LogDir;
use crate::tlscerts;
use crate::{affinity::history_affinity, constants::*};

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
        s3::S3BucketInlineOrReference,
    },
    config::{
        fragment::{self, Fragment, ValidationError},
        merge::Merge,
    },
    k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::api::resource::Quantity},
    kube::{runtime::reflector::ObjectRef, CustomResource, ResourceExt},
    product_config_utils::{
        transform_all_roles_to_config, validate_all_roles_and_groups_config, Configuration,
        ValidatedRoleConfigByPropertyKind,
    },
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleGroup, RoleGroupRef},
    schemars::{self, JsonSchema},
};
use std::collections::{BTreeMap, HashMap};
use strum::{Display, EnumIter};

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
}

/// A Spark cluster history server component. This resource is managed by the Stackable operator
/// for Apache Spark. Find more information on how to use it in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/history-server).
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

    /// Global Spark history server configuration that applies to all roles and role groups.
    #[serde(default)]
    pub cluster_config: SparkHistoryServerClusterConfig,

    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// The log file directory definition used by the Spark history server.
    /// Currently only S3 buckets are supported.
    pub log_file_directory: LogFileDirectorySpec,

    /// A map of key/value strings that will be passed directly to Spark when deploying the history server.
    #[serde(default)]
    pub spark_conf: BTreeMap<String, String>,

    /// A history server node role definition.
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
        s3logdir: &S3LogDir,
        role_group_env_overrides: HashMap<String, String>,
    ) -> Vec<EnvVar> {
        // Maps env var name to env var object. This allows env_overrides to work
        // as expected (i.e. users can override the env var value).
        let mut vars: BTreeMap<String, EnvVar> = BTreeMap::new();
        let role_env_overrides = &self.role().config.env_overrides;

        // This env var prevents the history server from detaching itself from the
        // start script because this leads to the Pod terminating immediately.
        vars.insert(
            "SPARK_NO_DAEMONIZE".to_string(),
            EnvVar {
                name: "SPARK_NO_DAEMONIZE".to_string(),
                value: Some("true".into()),
                value_from: None,
            },
        );
        vars.insert(
            "SPARK_DAEMON_CLASSPATH".to_string(),
            EnvVar {
                name: "SPARK_DAEMON_CLASSPATH".to_string(),
                value: Some("/stackable/spark/extra-jars/*".into()),
                value_from: None,
            },
        );

        let mut history_opts = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/config.yaml")
    ];

        // if TLS is enabled build truststore
        if tlscerts::tls_secret_name(&s3logdir.bucket.connection).is_some() {
            history_opts.extend(vec![
                format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
                format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
                format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
            ]);
        }

        vars.insert(
            "SPARK_HISTORY_OPTS".to_string(),
            EnvVar {
                name: "SPARK_HISTORY_OPTS".to_string(),
                value: Some(history_opts.join(" ")),
                value_from: None,
            },
        );

        // apply the role overrides
        let mut role_envs = role_env_overrides.iter().map(|(env_name, env_value)| {
            (
                env_name.clone(),
                EnvVar {
                    name: env_name.clone(),
                    value: Some(env_value.to_owned()),
                    value_from: None,
                },
            )
        });

        vars.extend(&mut role_envs);

        // apply the role-group overrides
        let mut role_group_envs =
            role_group_env_overrides
                .into_iter()
                .map(|(env_name, env_value)| {
                    (
                        env_name.clone(),
                        EnvVar {
                            name: env_name.clone(),
                            value: Some(env_value),
                            value_from: None,
                        },
                    )
                });

        vars.extend(&mut role_group_envs);

        // convert to Vec
        vars.into_values().collect()
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
    pub bucket: S3BucketInlineOrReference,
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

#[cfg(test)]
mod test {
    use super::*;
    use indoc::indoc;
    use stackable_operator::commons::{
        s3::{ResolvedS3Bucket, ResolvedS3Connection},
        tls_verification::TlsClientDetails,
    };

    #[test]
    pub fn test_env_overrides() {
        let input = indoc! {r#"
        ---
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
        spec:
          image:
            productVersion: 3.5.2
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
        let history: SparkHistoryServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let s3_log_dir: S3LogDir = S3LogDir {
            bucket: ResolvedS3Bucket {
                bucket_name: "my-bucket".to_string(),
                connection: ResolvedS3Connection {
                    host: "my-s3".to_string().try_into().unwrap(),
                    port: None,
                    access_style: Default::default(),
                    credentials: None,
                    tls: TlsClientDetails { tls: None },
                },
            },
            prefix: "prefix".to_string(),
        };

        let merged_env = history.merged_env(
            &s3_log_dir,
            history
                .spec
                .nodes
                .role_groups
                .get("default")
                .unwrap()
                .config
                .env_overrides
                .clone(),
        );

        let env_map: BTreeMap<&str, Option<String>> = merged_env
            .iter()
            .map(|env_var| (env_var.name.as_str(), env_var.value.clone()))
            .collect();

        assert_eq!(
            Some(&Some("ROLEGROUP".to_string())),
            env_map.get("TEST_SPARK_HIST_VAR")
        );
    }
}
