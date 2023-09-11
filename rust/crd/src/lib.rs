//! This module provides all required CRD definitions and additional helper methods.

pub mod affinity;
pub mod constants;
pub mod history;
pub mod roles;
pub mod s3logdir;
pub mod tlscerts;

pub use crate::roles::*;
use constants::*;
use history::LogFileDirectorySpec;
use s3logdir::S3LogDir;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::{
    builder::{SecretOperatorVolumeSourceBuilder, VolumeBuilder},
    commons::{
        product_image_selection::{ProductImage, ResolvedProductImage},
        resources::{CpuLimits, MemoryLimits, Resources},
        s3::{S3AccessStyle, S3ConnectionDef, S3ConnectionSpec},
    },
    config::{
        fragment,
        fragment::ValidationError,
        merge::{Atomic, Merge},
    },
    k8s_openapi::{
        api::core::v1::{EmptyDirVolumeSource, EnvVar, PodTemplateSpec, Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt},
    labels::ObjectLabels,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config::types::PropertyNameKind,
    product_logging,
    role_utils::{CommonConfiguration, Role, RoleGroup},
    schemars::{self, JsonSchema},
};
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object defines no deploy mode"))]
    ObjectHasNoDeployMode,
    #[snafu(display("object defines no application artifact"))]
    ObjectHasNoArtifact,
    #[snafu(display("object has no name"))]
    ObjectHasNoName,
    #[snafu(display("application has no Spark image"))]
    NoSparkImage,
    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::error::Error,
        unit: String,
    },
    #[snafu(display("failed to parse value"))]
    FailedParseToFloatConversion,
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
    #[snafu(display("failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    pub phase: String,
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkApplication",
    shortname = "sc",
    status = "SparkApplicationStatus",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_application_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    pub spark_image: ProductImage,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<CommonConfiguration<SubmitConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<CommonConfiguration<DriverConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<CommonConfiguration<ExecutorConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stopped: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deps: Option<JobDependencies>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3connection: Option<S3ConnectionDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_file_directory: Option<LogFileDirectorySpec>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDependencies {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirements: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub packages: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub repositories: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_packages: Option<Vec<String>>,
}

impl SparkApplication {
    pub fn submit_job_config_map_name(&self) -> String {
        format!("{app_name}-submit-job", app_name = self.name_any())
    }

    pub fn pod_template_config_map_name(&self, role: SparkApplicationRole) -> String {
        format!("{app_name}-{role}-pod-template", app_name = self.name_any())
    }

    pub fn mode(&self) -> Option<&str> {
        self.spec.mode.as_deref()
    }

    pub fn image(&self) -> Option<&str> {
        self.spec.image.as_deref()
    }

    pub fn version(&self) -> Option<&str> {
        self.spec.version.as_deref()
    }

    pub fn application_artifact(&self) -> Option<&str> {
        self.spec.main_application_file.as_deref()
    }

    pub fn requirements(&self) -> Option<String> {
        self.spec
            .deps
            .as_ref()
            .and_then(|deps| deps.requirements.as_ref())
            .map(|req| req.join(" "))
    }

    pub fn packages(&self) -> Vec<String> {
        self.spec
            .deps
            .as_ref()
            .and_then(|deps| deps.packages.clone())
            .unwrap_or_default()
    }

    pub fn volumes(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
        log_config_map: &str,
    ) -> Vec<Volume> {
        let mut result: Vec<Volume> = self
            .spec
            .volumes
            .as_ref()
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
            .collect();

        if self.spec.image.is_some() {
            result.push(
                VolumeBuilder::new(VOLUME_MOUNT_NAME_JOB)
                    .empty_dir(EmptyDirVolumeSource::default())
                    .build(),
            );
        }

        if self.requirements().is_some() {
            result.push(
                VolumeBuilder::new(VOLUME_MOUNT_NAME_REQ)
                    .empty_dir(EmptyDirVolumeSource::default())
                    .build(),
            );
        }

        if let Some(S3ConnectionSpec {
            credentials: Some(secret_class_volume),
            ..
        }) = s3conn
        {
            result.push(secret_class_volume.to_volume(secret_class_volume.secret_class.as_ref()));
        }

        if let Some(v) = s3logdir.as_ref().and_then(|o| o.credentials_volume()) {
            result.push(v);
        }

        result.push(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG_CONFIG)
                .with_config_map(log_config_map)
                .build(),
        );

        result.push(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_LOG)
                .with_empty_dir(
                    None::<String>,
                    Some(product_logging::framework::calculate_log_volume_size_limit(
                        &[MAX_SPARK_LOG_FILES_SIZE, MAX_INIT_LOG_FILES_SIZE],
                    )),
                )
                .build(),
        );

        if !self.packages().is_empty() {
            result.push(
                VolumeBuilder::new(VOLUME_MOUNT_NAME_IVY2)
                    .empty_dir(EmptyDirVolumeSource::default())
                    .build(),
            );
        }
        if let Some(cert_secrets) = tlscerts::tls_secret_names(s3conn, s3logdir) {
            result.push(
                VolumeBuilder::new(STACKABLE_TRUST_STORE_NAME)
                    .with_empty_dir(None::<String>, Some(Quantity("5Mi".to_string())))
                    .build(),
            );
            for cert_secret in cert_secrets {
                result.push(
                    VolumeBuilder::new(cert_secret)
                        .ephemeral(SecretOperatorVolumeSourceBuilder::new(cert_secret).build())
                        .build(),
                );
            }
        }

        result
    }

    pub fn spark_job_volume_mounts(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        let volume_mounts = vec![
            VolumeMount {
                name: VOLUME_MOUNT_NAME_DRIVER_POD_TEMPLATES.into(),
                mount_path: VOLUME_MOUNT_PATH_DRIVER_POD_TEMPLATES.into(),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES.into(),
                mount_path: VOLUME_MOUNT_PATH_EXECUTOR_POD_TEMPLATES.into(),
                ..VolumeMount::default()
            },
        ];
        self.add_common_volume_mounts(volume_mounts, s3conn, s3logdir)
    }

    fn add_common_volume_mounts(
        &self,
        mut mounts: Vec<VolumeMount>,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        if self.spec.image.is_some() {
            mounts.push(VolumeMount {
                name: VOLUME_MOUNT_NAME_JOB.into(),
                mount_path: VOLUME_MOUNT_PATH_JOB.into(),
                ..VolumeMount::default()
            });
        }
        if self.requirements().is_some() {
            mounts.push(VolumeMount {
                name: VOLUME_MOUNT_NAME_REQ.into(),
                mount_path: VOLUME_MOUNT_PATH_REQ.into(),
                ..VolumeMount::default()
            });
        }

        if let Some(S3ConnectionSpec {
            credentials: Some(secret_class_volume),
            ..
        }) = s3conn
        {
            let secret_class_name = secret_class_volume.secret_class.clone();
            let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret_class_name}");

            mounts.push(VolumeMount {
                name: secret_class_name,
                mount_path: secret_dir,
                ..VolumeMount::default()
            });
        }

        if let Some(vm) = s3logdir.as_ref().and_then(|o| o.credentials_volume_mount()) {
            mounts.push(vm);
        }

        mounts.push(VolumeMount {
            name: VOLUME_MOUNT_NAME_LOG_CONFIG.into(),
            mount_path: VOLUME_MOUNT_PATH_LOG_CONFIG.into(),
            ..VolumeMount::default()
        });

        mounts.push(VolumeMount {
            name: VOLUME_MOUNT_NAME_LOG.into(),
            mount_path: VOLUME_MOUNT_PATH_LOG.into(),
            ..VolumeMount::default()
        });

        if !self.packages().is_empty() {
            mounts.push(VolumeMount {
                name: VOLUME_MOUNT_NAME_IVY2.into(),
                mount_path: VOLUME_MOUNT_PATH_IVY2.into(),
                ..VolumeMount::default()
            });
        }
        if let Some(cert_secrets) = tlscerts::tls_secret_names(s3conn, s3logdir) {
            mounts.push(VolumeMount {
                name: STACKABLE_TRUST_STORE_NAME.into(),
                mount_path: STACKABLE_TRUST_STORE.into(),
                ..VolumeMount::default()
            });
            for cert_secret in cert_secrets {
                let secret_dir = format!("{STACKABLE_MOUNT_PATH_TLS}/{cert_secret}");
                mounts.push(VolumeMount {
                    name: cert_secret.to_string(),
                    mount_path: secret_dir,
                    ..VolumeMount::default()
                });
            }
        }

        mounts
    }

    pub fn build_recommended_labels<'a>(&'a self, role: &'a str) -> ObjectLabels<SparkApplication> {
        ObjectLabels {
            owner: self,
            app_name: APP_NAME,
            app_version: self.version().unwrap(),
            operator_name: OPERATOR_NAME,
            controller_name: CONTROLLER_NAME,
            role,
            role_group: CONTROLLER_NAME,
        }
    }

    pub fn build_command(
        &self,
        serviceaccount_name: &str,
        s3conn: &Option<S3ConnectionSpec>,
        s3_log_dir: &Option<S3LogDir>,
        spark_image: &str,
    ) -> Result<Vec<String>, Error> {
        // mandatory properties
        let mode = self.mode().context(ObjectHasNoDeployModeSnafu)?;
        let name = self.metadata.name.clone().context(ObjectHasNoNameSnafu)?;

        let mut submit_cmd: Vec<String> = vec![];

        submit_cmd.extend(vec![
            "/stackable/spark/bin/spark-submit".to_string(),
            "--verbose".to_string(),
            "--master k8s://https://${KUBERNETES_SERVICE_HOST}:${KUBERNETES_SERVICE_PORT_HTTPS}".to_string(),
            format!("--deploy-mode {mode}"),
            format!("--name {name}"),
            format!("--conf spark.kubernetes.driver.podTemplateFile={VOLUME_MOUNT_PATH_DRIVER_POD_TEMPLATES}/{POD_TEMPLATE_FILE}"),
            format!("--conf spark.kubernetes.executor.podTemplateFile={VOLUME_MOUNT_PATH_EXECUTOR_POD_TEMPLATES}/{POD_TEMPLATE_FILE}"),
            format!("--conf spark.kubernetes.driver.podTemplateContainerName={container_name}", container_name = SparkContainer::Spark),
            format!("--conf spark.kubernetes.executor.podTemplateContainerName={container_name}", container_name = SparkContainer::Spark),
            format!("--conf spark.kubernetes.namespace={}", self.metadata.namespace.as_ref().context(NoNamespaceSnafu)?),
            format!("--conf spark.kubernetes.driver.container.image={}", spark_image.to_string()),
            format!("--conf spark.kubernetes.executor.container.image={}", spark_image.to_string()),
            format!("--conf spark.kubernetes.authenticate.driver.serviceAccountName={}", serviceaccount_name),
            format!("--conf spark.driver.defaultJavaOptions=-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
            format!("--conf spark.driver.extraClassPath=/stackable/spark/extra-jars/*"),
            "--conf spark.driver.userClassPathFirst=true".to_string(),
            format!("--conf spark.executor.defaultJavaOptions=-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
            format!("--conf spark.executor.extraClassPath=/stackable/spark/extra-jars/*"),
            "--conf spark.executor.userClassPathFirst=true".to_string(),
        ]);

        // See https://spark.apache.org/docs/latest/running-on-kubernetes.html#dependency-management
        // for possible S3 related properties
        if let Some(endpoint) = s3conn.as_ref().and_then(|conn| conn.endpoint()) {
            submit_cmd.push(format!("--conf spark.hadoop.fs.s3a.endpoint={}", endpoint));
        }

        if let Some(conn) = s3conn.as_ref() {
            match conn.access_style {
                Some(S3AccessStyle::Path) => {
                    submit_cmd
                        .push("--conf spark.hadoop.fs.s3a.path.style.access=true".to_string());
                }
                Some(S3AccessStyle::VirtualHosted) => {}
                None => {}
            }
            if let Some(credentials) = &conn.credentials {
                let secret_class_name = credentials.secret_class.clone();
                let secret_dir = format!("{S3_SECRET_DIR_NAME}/{secret_class_name}");

                // We don't use the credentials at all here but assume they are available
                submit_cmd.push(format!(
                    "--conf spark.hadoop.fs.s3a.access.key=$(cat {secret_dir}/{ACCESS_KEY_ID})"
                ));
                submit_cmd.push(format!(
                    "--conf spark.hadoop.fs.s3a.secret.key=$(cat {secret_dir}/{SECRET_ACCESS_KEY})"
                ));
                submit_cmd.push("--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string());
            } else {
                submit_cmd.push("--conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider".to_string());
            }
        }

        // Extra JVM opts:
        // - java security properties
        // - s3 with TLS
        let mut extra_java_opts = vec![format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_LOG_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        )];
        if tlscerts::tls_secret_names(s3conn, s3_log_dir).is_some() {
            extra_java_opts.extend(
                vec![
                    format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
                    format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
                    format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
                ]
                .into_iter(),
            );
        }
        let str_extra_java_opts = extra_java_opts.join(" ");
        submit_cmd.extend(vec![
            format!("--conf spark.driver.extraJavaOptions=\"{str_extra_java_opts}\""),
            format!("--conf spark.executor.extraJavaOptions=\"{str_extra_java_opts}\""),
        ]);

        // repositories and packages arguments
        if let Some(deps) = self.spec.deps.clone() {
            submit_cmd.extend(
                deps.repositories
                    .map(|r| format!("--repositories {}", r.join(","))),
            );
            submit_cmd.extend(deps.packages.map(|p| format!("--packages {}", p.join(","))));
        }

        // some command elements need to be initially stored in a map (to allow overwrites) and
        // then added to the vector once complete.
        let mut submit_conf: BTreeMap<String, String> = BTreeMap::new();

        // Disable this. We subtract this factor out of the resource requests ourselves
        // when computing the Spark memory properties below. We do this to because otherwise
        // Spark computes and applies different container memory limits than the ones the
        // user has provided.
        // It can be overwritten by the user with the "sparkConf" property.
        submit_conf.insert(
            "spark.kubernetes.memoryOverheadFactor".to_string(),
            "0.0".to_string(),
        );

        resources_to_driver_props(
            self.spec.main_class.is_some(),
            &self.driver_config()?,
            &mut submit_conf,
        )?;
        resources_to_executor_props(
            self.spec.main_class.is_some(),
            &self.executor_config()?,
            &mut submit_conf,
        )?;

        if let Some(CommonConfiguration {
            config:
                ExecutorConfigFragment {
                    replicas: Some(replicas),
                    ..
                },
            ..
        }) = &self.spec.executor
        {
            submit_conf.insert("spark.executor.instances".to_string(), replicas.to_string());
        }

        if let Some(log_dir) = s3_log_dir {
            submit_conf.extend(log_dir.application_spark_config());
        }

        if !self.packages().is_empty() {
            submit_cmd.push(format!("--conf spark.jars.ivy={VOLUME_MOUNT_PATH_IVY2}"))
        }

        // conf arguments: these should follow - and thus override - values set from resource limits above
        if let Some(spark_conf) = self.spec.spark_conf.clone() {
            submit_conf.extend(spark_conf);
        }
        // ...before being added to the command collection
        for (key, value) in submit_conf {
            submit_cmd.push(format!("--conf \"{key}={value}\""));
        }

        submit_cmd.extend(
            self.spec
                .main_class
                .clone()
                .map(|mc| format! {"--class {mc}"}),
        );

        let artifact = self
            .application_artifact()
            .context(ObjectHasNoArtifactSnafu)?;
        submit_cmd.push(artifact.to_string());

        if let Some(job_args) = self.spec.args.clone() {
            submit_cmd.extend(job_args);
        }

        Ok(submit_cmd)
    }

    pub fn env(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<EnvVar> {
        let tmp = self.spec.env.as_ref();
        let mut e: Vec<EnvVar> = tmp.iter().flat_map(|e| e.iter()).cloned().collect();
        if self.requirements().is_some() {
            e.push(EnvVar {
                name: "PYTHONPATH".to_string(),
                value: Some(format!(
                    "$SPARK_HOME/python:{VOLUME_MOUNT_PATH_REQ}:$PYTHONPATH"
                )),
                value_from: None,
            });
        }
        if tlscerts::tls_secret_names(s3conn, s3logdir).is_some() {
            e.push(EnvVar {
                name: "STACKABLE_TLS_STORE_PASSWORD".to_string(),
                value: Some(STACKABLE_TLS_STORE_PASSWORD.to_string()),
                value_from: None,
            });
        }
        e
    }

    pub fn submit_config(&self) -> Result<SubmitConfig, Error> {
        if let Some(CommonConfiguration { mut config, .. }) = self.spec.job.clone() {
            config.merge(&SubmitConfig::default_config());
            fragment::validate(config).context(FragmentValidationFailureSnafu)
        } else {
            fragment::validate(SubmitConfig::default_config())
                .context(FragmentValidationFailureSnafu)
        }
    }

    pub fn driver_config(&self) -> Result<DriverConfig, Error> {
        if let Some(CommonConfiguration { mut config, .. }) = self.spec.driver.clone() {
            config.merge(&DriverConfig::default_config());
            fragment::validate(config).context(FragmentValidationFailureSnafu)
        } else {
            fragment::validate(DriverConfig::default_config())
                .context(FragmentValidationFailureSnafu)
        }
    }

    pub fn executor_config(&self) -> Result<ExecutorConfig, Error> {
        if let Some(CommonConfiguration { mut config, .. }) = self.spec.executor.clone() {
            config.merge(&ExecutorConfig::default_config());
            fragment::validate(config).context(FragmentValidationFailureSnafu)
        } else {
            fragment::validate(ExecutorConfig::default_config())
                .context(FragmentValidationFailureSnafu)
        }
    }

    pub fn pod_overrides(&self, role: SparkApplicationRole) -> Option<PodTemplateSpec> {
        match role {
            SparkApplicationRole::Submit => self.spec.job.clone().map(|j| j.pod_overrides),
            SparkApplicationRole::Driver => self.spec.driver.clone().map(|d| d.pod_overrides),
            SparkApplicationRole::Executor => self.spec.executor.clone().map(|e| e.pod_overrides),
        }
    }

    pub fn validated_role_config(
        &self,
        resolved_product_image: &ResolvedProductImage,
        product_config: &ProductConfigManager,
    ) -> Result<ValidatedRoleConfigByPropertyKind, Error> {
        let submit_conf = if self.spec.job.is_some() {
            self.spec.job.as_ref().unwrap().clone()
        } else {
            CommonConfiguration {
                config: SubmitConfig::default_config(),
                ..CommonConfiguration::default()
            }
        };

        let driver_conf = if self.spec.driver.is_some() {
            self.spec.driver.as_ref().unwrap().clone()
        } else {
            CommonConfiguration {
                config: DriverConfig::default_config(),
                ..CommonConfiguration::default()
            }
        };

        let executor_conf = if self.spec.executor.is_some() {
            self.spec.executor.as_ref().unwrap().clone()
        } else {
            CommonConfiguration {
                config: ExecutorConfig::default_config(),
                ..CommonConfiguration::default()
            }
        };

        let mut roles_to_validate = HashMap::new();
        roles_to_validate.insert(
            SparkApplicationRole::Submit.to_string(),
            (
                vec![
                    PropertyNameKind::Env,
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                ],
                Role {
                    config: submit_conf.clone(),
                    role_groups: [(
                        "default".to_string(),
                        RoleGroup {
                            config: submit_conf,
                            replicas: Some(1),
                            selector: None,
                        },
                    )]
                    .into(),
                }
                .erase(),
            ),
        );
        roles_to_validate.insert(
            SparkApplicationRole::Driver.to_string(),
            (
                vec![
                    PropertyNameKind::Env,
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                ],
                Role {
                    config: driver_conf.clone(),
                    role_groups: [(
                        "default".to_string(),
                        RoleGroup {
                            config: driver_conf,
                            replicas: Some(1),
                            selector: None,
                        },
                    )]
                    .into(),
                }
                .erase(),
            ),
        );
        roles_to_validate.insert(
            SparkApplicationRole::Executor.to_string(),
            (
                vec![
                    PropertyNameKind::Env,
                    PropertyNameKind::File(JVM_SECURITY_PROPERTIES_FILE.to_string()),
                ],
                Role {
                    config: executor_conf.clone(),
                    role_groups: [(
                        "default".to_string(),
                        RoleGroup {
                            config: executor_conf,
                            // This is a dummy value needed to be able to build the RoleGroup
                            // object. ExecutorConfig.replicas is the true value used to set the
                            // number of executor pods.
                            replicas: Some(1),
                            selector: None,
                        },
                    )]
                    .into(),
                }
                .erase(),
            ),
        );

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

/// CPU Limits can be defined as integer, decimal, or unitised values (see
/// <https://kubernetes.io/docs/tasks/configure-pod-container/assign-cpu-resource/#cpu-units>)
/// of which only "m" (milli-units) is allowed. The parsed value will be rounded up to the next
/// integer value.
// TODO: Move to operator-rs when needed in multiple operators
fn cores_from_quantity(q: String) -> Result<String, Error> {
    let start_of_unit = q.find('m');
    let cores = if let Some(start_of_unit) = start_of_unit {
        let (prefix, _) = q.split_at(start_of_unit);
        (prefix
            .parse::<f32>()
            .map_err(|_| Error::FailedParseToFloatConversion)?
            / 1000.0)
            .ceil()
    } else {
        q.parse::<f32>()
            .map_err(|_| Error::FailedParseToFloatConversion)?
            .ceil()
    };
    Ok((cores as u32).to_string())
}

/// A memory overhead will be applied using a factor of 0.1 (JVM jobs) or 0.4 (non-JVM jobs),
/// being not less than MIN_MEMORY_OVERHEAD. This implies that `limit` must be greater than
/// `MIN_MEMORY_OVERHEAD`
/// The resource limit should keep this transparent by reducing the
/// declared memory limit accordingly.
fn subtract_spark_memory_overhead(for_java: bool, limit: &Quantity) -> Result<String, Error> {
    // determine job-type using class name: scala/java will declare an application and main class;
    // R and python will just declare the application name/file (for python this could be .zip/.py/.egg).
    // Spark itself just checks the application name - See e.g.
    // https://github.com/apache/spark/blob/01c7a46f24fb4bb4287a184a3d69e0e5c904bc50/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1092
    let non_jvm_factor = if for_java {
        //self.spec.main_class.is_some() {
        1.0 / (1.0 + JVM_OVERHEAD_FACTOR)
    } else {
        1.0 / (1.0 + NON_JVM_OVERHEAD_FACTOR)
    };

    let original_memory = MemoryQuantity::try_from(limit)
        .context(FailedToConvertJavaHeapSnafu {
            unit: BinaryMultiple::Mebi.to_java_memory_unit(),
        })?
        .scale_to(BinaryMultiple::Mebi)
        .floor()
        .value as u32;

    if MIN_MEMORY_OVERHEAD > original_memory {
        tracing::warn!("Skip memory overhead since not enough memory ({original_memory}m). At least {MIN_MEMORY_OVERHEAD}m required");
        return Ok(format!("{original_memory}m"));
    }

    let reduced_memory =
        (MemoryQuantity::try_from(limit).context(FailedToConvertJavaHeapSnafu {
            unit: BinaryMultiple::Mebi.to_java_memory_unit(),
        })? * non_jvm_factor)
            .scale_to(BinaryMultiple::Mebi)
            .floor()
            .value as u32;

    let deduction = max(MIN_MEMORY_OVERHEAD, original_memory - reduced_memory);

    tracing::debug!("subtract_spark_memory_overhead: original_memory ({original_memory}) - deduction ({deduction})");
    Ok(format!("{}m", original_memory - deduction))
}

/// Translate resource limits to Spark config properties.
/// Spark will use these and *ignore* the resource limits in pod templates entirely.
fn resources_to_driver_props(
    for_java: bool,
    driver_config: &DriverConfig,
    props: &mut BTreeMap<String, String>,
) -> Result<(), Error> {
    if let Resources {
        cpu: CpuLimits { max: Some(max), .. },
        ..
    } = &driver_config.resources
    {
        let cores = cores_from_quantity(max.0.clone())?;
        // will have default value from resources to apply if nothing set specifically
        props.insert("spark.driver.cores".to_string(), cores.clone());
        props.insert(
            "spark.kubernetes.driver.request.cores".to_string(),
            cores.clone(),
        );
        props.insert("spark.kubernetes.driver.limit.cores".to_string(), cores);
    }

    if let Resources {
        memory: MemoryLimits {
            limit: Some(limit), ..
        },
        ..
    } = &driver_config.resources
    {
        let memory = subtract_spark_memory_overhead(for_java, limit)?;
        props.insert("spark.driver.memory".to_string(), memory);

        let limit_mb = format!(
            "{}m",
            MemoryQuantity::try_from(limit)
                .context(FailedToConvertJavaHeapSnafu {
                    unit: BinaryMultiple::Mebi.to_java_memory_unit(),
                })?
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32
        );
        props.insert(
            "spark.kubernetes.driver.request.memory".to_string(),
            limit_mb.clone(),
        );
        props.insert("spark.kubernetes.driver.limit.memory".to_string(), limit_mb);
    }

    Ok(())
}

/// Translate resource limits to Spark config properties.
/// Spark will use these and *ignore* the resource limits in pod templates entirely.
fn resources_to_executor_props(
    for_java: bool,
    executor_config: &ExecutorConfig,
    props: &mut BTreeMap<String, String>,
) -> Result<(), Error> {
    if let Resources {
        cpu: CpuLimits { max: Some(max), .. },
        ..
    } = &executor_config.resources
    {
        let cores = cores_from_quantity(max.0.clone())?;
        // will have default value from resources to apply if nothing set specifically
        props.insert("spark.executor.cores".to_string(), cores.clone());
        props.insert(
            "spark.kubernetes.executor.request.cores".to_string(),
            cores.clone(),
        );
        props.insert("spark.kubernetes.executor.limit.cores".to_string(), cores);
    }

    if let Resources {
        memory: MemoryLimits {
            limit: Some(limit), ..
        },
        ..
    } = &executor_config.resources
    {
        let memory = subtract_spark_memory_overhead(for_java, limit)?;
        props.insert("spark.executor.memory".to_string(), memory);

        let limit_mb = format!(
            "{}m",
            MemoryQuantity::try_from(limit)
                .context(FailedToConvertJavaHeapSnafu {
                    unit: BinaryMultiple::Mebi.to_java_memory_unit(),
                })?
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32
        );
        props.insert(
            "spark.kubernetes.executor.request.memory".to_string(),
            limit_mb.clone(),
        );
        props.insert(
            "spark.kubernetes.executor.limit.memory".to_string(),
            limit_mb,
        );
    }

    Ok(())
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSelector {
    pub node_selector: Option<BTreeMap<String, String>>,
}

impl Atomic for NodeSelector {}

#[cfg(test)]
mod tests {
    use crate::{cores_from_quantity, resources_to_executor_props, DriverConfig, ExecutorConfig};
    use crate::{resources_to_driver_props, SparkApplication};
    use crate::{Quantity, SparkStorageConfig};
    use rstest::rstest;
    use stackable_operator::builder::ObjectMetaBuilder;
    use stackable_operator::commons::affinity::StackableAffinity;
    use stackable_operator::commons::resources::{
        CpuLimits, MemoryLimits, NoRuntimeLimits, Resources,
    };
    use stackable_operator::product_config::ProductConfigManager;
    use stackable_operator::product_logging::spec::Logging;
    use std::collections::BTreeMap;

    #[test]
    fn test_spark_examples_s3() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
        r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples-s3
spec:
  version: "1.0"
  sparkImage:
    productVersion: 3.4.0
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/spark-examples.jar
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
  config:
    enableMonitoring: true
        "#).unwrap();

        assert_eq!("1.0", spark_application.spec.version.unwrap_or_default());
        assert_eq!(
            Some("org.apache.spark.examples.SparkPi".to_string()),
            spark_application.spec.main_class
        );
        assert_eq!(
            Some("s3a://stackable-spark-k8s-jars/jobs/spark-examples.jar".to_string()),
            spark_application.spec.main_application_file
        );
        assert_eq!(
            Some(1),
            spark_application.spec.spark_conf.map(|m| m.keys().len())
        );

        assert!(spark_application.spec.mode.is_some());
        assert!(spark_application.spec.driver.is_some());
        assert!(spark_application.spec.executor.is_some());

        assert!(spark_application.spec.args.is_none());
        assert!(spark_application.spec.deps.is_none());
        assert!(spark_application.spec.image.is_none());
    }

    #[test]
    fn test_ny_tlc_report_image() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
        r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: ny-tlc-report-image
  namespace: my-ns
spec:
  version: "1.0"
  image: docker.stackable.tech/stackable/ny-tlc-report:0.1.0
  sparkImage:
    productVersion: 3.2.1
  mode: cluster
  mainApplicationFile: local:///stackable/spark/jobs/ny_tlc_report.py
  args:
    - "--input 's3a://nyc-tlc/trip data/yellow_tripdata_2021-07.csv'"
  deps:
    requirements:
      - tabulate==0.8.9
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
        "#).unwrap();

        assert_eq!("1.0", spark_application.spec.version.unwrap_or_default());
        assert_eq!(
            Some("local:///stackable/spark/jobs/ny_tlc_report.py".to_string()),
            spark_application.spec.main_application_file
        );
        assert_eq!(
            Some(1),
            spark_application.spec.spark_conf.map(|m| m.keys().len())
        );

        assert!(spark_application.spec.image.is_some());
        assert!(spark_application.spec.mode.is_some());
        assert!(spark_application.spec.args.is_some());
        assert!(spark_application.spec.deps.is_some());
        assert!(spark_application.spec.driver.is_some());
        assert!(spark_application.spec.executor.is_some());

        assert!(spark_application.spec.main_class.is_none());
    }

    #[test]
    fn test_ny_tlc_report_external_dependencies() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
        r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: ny-tlc-report-external-dependencies
  namespace: default
  uid: 12345678asdfghj
spec:
  version: "1.0"
  sparkImage:
    productVersion: 3.4.0
  mode: cluster
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/ny_tlc_report.py
  args:
    - "--input 's3a://nyc-tlc/trip data/yellow_tripdata_2021-07.csv'"
  deps:
    requirements:
      - tabulate==0.8.9
  sparkConf:
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 3
    memory: "512m"
        "#).unwrap();

        let meta = ObjectMetaBuilder::new()
            .name_and_namespace(&spark_application)
            .ownerreference_from_resource(&spark_application, None, Some(true))
            .unwrap()
            .build();

        assert_eq!("12345678asdfghj", meta.owner_references.unwrap()[0].uid);

        assert_eq!("1.0", spark_application.spec.version.unwrap_or_default());
        assert_eq!(
            Some("s3a://stackable-spark-k8s-jars/jobs/ny_tlc_report.py".to_string()),
            spark_application.spec.main_application_file
        );
        assert_eq!(
            Some(1),
            spark_application.spec.spark_conf.map(|m| m.keys().len())
        );

        assert!(spark_application.spec.mode.is_some());
        assert!(spark_application.spec.args.is_some());
        assert!(spark_application.spec.deps.is_some());
        assert!(spark_application.spec.driver.is_some());
        assert!(spark_application.spec.executor.is_some());

        assert!(spark_application.spec.main_class.is_none());
        assert!(spark_application.spec.image.is_none());
    }

    #[test]
    fn test_default_resource_limits() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
            r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples
spec:
  sparkImage:
    productVersion: 1.2.3
  executor:
    instances: 1
  config:
    enableMonitoring: true
        "#,
        )
        .unwrap();

        let job_resources = &spark_application.submit_config().unwrap().resources;
        assert_eq!("100m", job_resources.cpu.min.as_ref().unwrap().0);
        assert_eq!("400m", job_resources.cpu.max.as_ref().unwrap().0);

        let driver_resources = &spark_application.driver_config().unwrap().resources;
        assert_eq!("250m", driver_resources.cpu.min.as_ref().unwrap().0);
        assert_eq!("1", driver_resources.cpu.max.as_ref().unwrap().0);

        let executor_resources = &spark_application.executor_config().unwrap().resources;
        assert_eq!("250m", executor_resources.cpu.min.as_ref().unwrap().0);
        assert_eq!("1", executor_resources.cpu.max.as_ref().unwrap().0);
    }

    #[test]
    fn test_merged_resource_limits() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
            r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples
spec:
  sparkImage:
    productVersion: 1.2.3
  job:
    config:
      resources:
        cpu:
          min: "100m"
          max: "200m"
        memory:
          limit: "1G"
  driver:
    config:
      resources:
        cpu:
          min: "1"
          max: "1300m"
        memory:
          limit: "512m"
  executor:
    replicas: 10
    config:
      resources:
        cpu:
          min: "500m"
          max: "1200m"
        memory:
          limit: "1Gi"
  config:
    enableMonitoring: true
        "#,
        )
        .unwrap();

        assert_eq!(
            "200m",
            &spark_application
                .submit_config()
                .unwrap()
                .resources
                .cpu
                .max
                .unwrap()
                .0
        );
        assert_eq!(
            "1300m",
            &spark_application
                .driver_config()
                .unwrap()
                .resources
                .cpu
                .max
                .unwrap()
                .0
        );
        assert_eq!(
            "500m",
            &spark_application
                .executor_config()
                .unwrap()
                .resources
                .cpu
                .min
                .unwrap()
                .0
        );
    }

    #[rstest]
    #[case("1800m", "2")]
    #[case("100m", "1")]
    #[case("1.5", "2")]
    #[case("2", "2")]
    fn test_quantity_to_cores(#[case] input: &str, #[case] output: &str) {
        let q = &Quantity(input.to_string());
        let cores = cores_from_quantity(q.0.clone()).unwrap();
        assert_eq!(output, cores);
    }

    #[test]
    fn test_resource_to_driver_props() {
        let driver_config = DriverConfig {
            resources: Resources {
                memory: MemoryLimits {
                    limit: Some(Quantity("128Mi".to_string())),
                    runtime_limits: NoRuntimeLimits {},
                },
                cpu: CpuLimits {
                    min: Some(Quantity("250m".to_string())),
                    max: Some(Quantity("1".to_string())),
                },
                storage: SparkStorageConfig {},
            },
            logging: Logging {
                enable_vector_agent: false,
                containers: BTreeMap::new(),
            },
            volume_mounts: None,
            affinity: StackableAffinity::default(),
        };

        let mut props = BTreeMap::new();

        resources_to_driver_props(true, &driver_config, &mut props).expect("blubb");

        let expected: BTreeMap<String, String> = vec![
            ("spark.driver.cores".to_string(), "1".to_string()),
            ("spark.driver.memory".to_string(), "128m".to_string()),
            (
                "spark.kubernetes.driver.limit.cores".to_string(),
                "1".to_string(),
            ),
            (
                "spark.kubernetes.driver.limit.memory".to_string(),
                "128m".to_string(),
            ),
            (
                "spark.kubernetes.driver.request.cores".to_string(),
                "1".to_string(),
            ),
            (
                "spark.kubernetes.driver.request.memory".to_string(),
                "128m".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(expected, props);
    }

    #[test]
    fn test_resource_to_executor_props() {
        let executor_config = ExecutorConfig {
            replicas: Some(3),
            resources: Resources {
                memory: MemoryLimits {
                    limit: Some(Quantity("512Mi".to_string())),
                    runtime_limits: NoRuntimeLimits {},
                },
                cpu: CpuLimits {
                    min: Some(Quantity("250m".to_string())),
                    max: Some(Quantity("2".to_string())),
                },
                storage: SparkStorageConfig {},
            },
            logging: Logging {
                enable_vector_agent: false,
                containers: BTreeMap::new(),
            },
            volume_mounts: None,
            affinity: StackableAffinity::default(),
        };

        let mut props = BTreeMap::new();

        resources_to_executor_props(true, &executor_config, &mut props).expect("blubb");

        let expected: BTreeMap<String, String> = vec![
            ("spark.executor.cores".to_string(), "2".to_string()),
            ("spark.executor.memory".to_string(), "128m".to_string()), // 128 and not 512 because memory overhead is subtracted
            (
                "spark.kubernetes.executor.limit.memory".to_string(),
                "512m".to_string(),
            ),
            (
                "spark.kubernetes.executor.request.cores".to_string(),
                "2".to_string(),
            ),
            (
                "spark.kubernetes.executor.request.memory".to_string(),
                "512m".to_string(),
            ),
            (
                "spark.kubernetes.executor.limit.cores".to_string(),
                "2".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(expected, props);
    }

    #[test]
    fn test_validated_config() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
            r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-examples
spec:
  sparkImage:
    productVersion: 1.2.3
  job:
    config:
    resources:
        cpu:
        min: "100m"
        max: "200m"
        memory:
        limit: "1G"
  config:
    enableMonitoring: true
        "#,
        )
        .unwrap();

        let resolved_product_image = spark_application
            .spec
            .spark_image
            .resolve("spark-k8s", "0.0.0-dev");

        let product_config =
            ProductConfigManager::from_yaml_file("../../deploy/config-spec/properties.yaml")
                .unwrap();
        let validated_config = spark_application
            .validated_role_config(&resolved_product_image, &product_config)
            .unwrap();

        println!("{:?}", validated_config);
    }
}
