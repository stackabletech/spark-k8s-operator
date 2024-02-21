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
use product_config::{types::PropertyNameKind, ProductConfigManager};
use s3logdir::S3LogDir;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::SecretFormat;
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::role_utils::EmptyRoleConfig;
use stackable_operator::{
    builder::{SecretOperatorVolumeSourceBuilder, VolumeBuilder},
    commons::{
        product_image_selection::{ProductImage, ResolvedProductImage},
        resources::{CpuLimits, MemoryLimits, Resources},
        s3::{S3AccessStyle, S3ConnectionDef, S3ConnectionSpec},
    },
    config::{fragment, fragment::ValidationError, merge::Merge},
    k8s_openapi::{
        api::core::v1::{EmptyDirVolumeSource, EnvVar, PodTemplateSpec, Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::{CustomResource, ResourceExt},
    kvp::ObjectLabels,
    memory::{BinaryMultiple, MemoryQuantity},
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

    #[snafu(display("failed to build TLS certificate SecretClass Volume"))]
    TlsCertSecretClassVolumeBuild {
        source: stackable_operator::builder::SecretOperatorVolumeSourceBuilderError,
    },

    #[snafu(display("failed to build S3 credentials Volume"))]
    S3CredentialsVolumeBuild {
        source: stackable_operator::commons::secret_class::SecretClassVolumeError,
    },

    #[snafu(display("failed to build S3 log directory credentials Volume"))]
    S3LogDirCredentialsVolumeBuild { source: s3logdir::Error },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    pub phase: String,
}

/// A Spark cluster stacklet. This resource is managed by the Stackable operator for Apache Spark.
/// Find more information on how to use it and the resources that the operator generates in the
/// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/).
///
/// The SparkApplication CRD looks a little different than the CRDs of the other products on the
/// Stackable Data Platform.
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
    /// Mode: cluster or client. Currently only cluster is supported.
    pub mode: SparkMode,

    /// The main class - i.e. entry point - for JVM artifacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub main_class: Option<String>,

    /// The actual application file that will be called by `spark-submit`.
    pub main_application_file: String,

    /// User-supplied image containing spark-job dependencies that will be copied to the specified volume mount.
    /// See the [examples](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/examples).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,

    // no doc - docs in ProductImage struct.
    pub spark_image: ProductImage,

    /// Name of the Vector aggregator [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    /// Follow the [logging tutorial](DOCS_BASE_URL_PLACEHOLDER/tutorials/logging-vector-aggregator)
    /// to learn how to configure log aggregation with Vector.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,

    /// The job builds a spark-submit command, complete with arguments and referenced dependencies
    /// such as templates, and passes it on to Spark.
    /// The reason this property uses its own type (SubmitConfigFragment) is because logging is not
    /// supported for spark-submit processes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<CommonConfiguration<SubmitConfigFragment>>,

    /// The driver role specifies the configuration that, together with the driver pod template, is used by
    /// Spark to create driver pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<CommonConfiguration<RoleConfigFragment>>,

    /// The executor role specifies the configuration that, together with the driver pod template, is used by
    /// Spark to create the executor pods.
    /// This is RoleGroup instead of plain CommonConfiguration because it needs to allows for the number of replicas.
    /// to be specified.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<RoleGroup<RoleConfigFragment>>,

    /// A map of key/value strings that will be passed directly to spark-submit.
    #[serde(default)]
    pub spark_conf: HashMap<String, String>,

    /// Job dependencies: a list of python packages that will be installed via pip, a list of packages
    /// or repositories that is passed directly to spark-submit, or a list of excluded packages
    /// (also passed directly to spark-submit).
    #[serde(default)]
    pub deps: JobDependencies,

    /// Configure an S3 connection that the SparkApplication has access to.
    /// Read more in the [Spark S3 usage guide](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/usage-guide/s3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3connection: Option<S3ConnectionDef>,

    /// Arguments passed directly to the job artifact.
    #[serde(default)]
    pub args: Vec<String>,

    /// A list of volumes that can be made available to the job, driver or executors via their volume mounts.
    #[serde(default)]
    pub volumes: Vec<Volume>,

    /// A list of environment variables that will be set in the job pod and the driver and executor
    /// pod templates.
    #[serde(default)]
    pub env: Vec<EnvVar>,

    /// The log file directory definition used by the Spark history server.
    /// Currently only S3 buckets are supported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_file_directory: Option<LogFileDirectorySpec>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDependencies {
    /// Under the `requirements` you can specify Python dependencies that will be installed with `pip`.
    /// Example: `tabulate==0.8.9`
    #[serde(default)]
    pub requirements: Vec<String>,

    /// A list of packages that is passed directly to `spark-submit`.
    #[serde(default)]
    pub packages: Vec<String>,

    /// A list of repositories that is passed directly to `spark-submit`.
    #[serde(default)]
    pub repositories: Vec<String>,

    /// A list of excluded packages that is passed directly to `spark-submit`.
    #[serde(default)]
    pub exclude_packages: Vec<String>,
}

impl SparkApplication {
    pub fn submit_job_config_map_name(&self) -> String {
        format!("{app_name}-submit-job", app_name = self.name_any())
    }

    pub fn pod_template_config_map_name(&self, role: SparkApplicationRole) -> String {
        format!("{app_name}-{role}-pod-template", app_name = self.name_any())
    }

    pub fn image(&self) -> Option<&str> {
        self.spec.image.as_deref()
    }

    pub fn application_artifact(&self) -> &str {
        self.spec.main_application_file.as_ref()
    }

    pub fn requirements(&self) -> Option<String> {
        if !self.spec.deps.requirements.is_empty() {
            return Some(self.spec.deps.requirements.join(" "));
        }
        None
    }

    pub fn packages(&self) -> Vec<String> {
        self.spec.deps.packages.clone()
    }

    pub fn volumes(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
        log_config_map: Option<&str>,
    ) -> Result<Vec<Volume>, Error> {
        let mut result: Vec<Volume> = self.spec.volumes.clone();

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
            result.push(
                secret_class_volume
                    .to_volume(secret_class_volume.secret_class.as_ref())
                    .context(S3CredentialsVolumeBuildSnafu)?,
            );
        }

        if let Some(log_dir) = s3logdir.as_ref() {
            if let Some(volume) = log_dir
                .credentials_volume()
                .context(S3LogDirCredentialsVolumeBuildSnafu)?
            {
                result.push(volume);
            }
        }

        if let Some(log_config_map) = log_config_map {
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
        }

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
                        .ephemeral(
                            SecretOperatorVolumeSourceBuilder::new(cert_secret)
                                .with_format(SecretFormat::TlsPkcs12)
                                .build()
                                .context(TlsCertSecretClassVolumeBuildSnafu)?,
                        )
                        .build(),
                );
            }
        }

        Ok(result)
    }

    /// Return the volume mounts for the spark-submit pod.
    ///
    /// These volume mounts are assembled from:
    /// * two pod template CMs for the driver and executors
    /// * volume mounts for accessing applications stored in S3 buckets
    /// * S3 credentials
    /// * S3 verification certificates
    /// * python packages (razvan: this was also a mistake since these packages are not used here.)
    /// * volume mounts additional java packages
    /// * finally user specified volume maps in `spec.job`.
    ///
    pub fn spark_job_volume_mounts(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        let mut tmpl_mounts = vec![
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

        tmpl_mounts = self.add_common_volume_mounts(tmpl_mounts, s3conn, s3logdir, false);

        if let Some(CommonConfiguration {
            config:
                SubmitConfigFragment {
                    volume_mounts:
                        Some(VolumeMounts {
                            volume_mounts: Some(job_vm),
                        }),
                    ..
                },
            ..
        }) = &self.spec.job
        {
            tmpl_mounts.extend(job_vm.clone());
        }

        tmpl_mounts
    }

    fn add_common_volume_mounts(
        &self,
        mut mounts: Vec<VolumeMount>,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
        logging_enabled: bool,
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

        if logging_enabled {
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
        }

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

    pub fn build_recommended_labels<'a>(
        &'a self,
        app_version: &'a str,
        role: &'a str,
    ) -> ObjectLabels<SparkApplication> {
        ObjectLabels {
            owner: self,
            app_name: APP_NAME,
            app_version,
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
        let mode = &self.spec.mode;
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
            format!("--conf spark.executor.defaultJavaOptions=-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
            format!("--conf spark.executor.extraClassPath=/stackable/spark/extra-jars/*"),
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
            extra_java_opts.extend(vec![
                format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
                format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
                format!("-Djavax.net.ssl.trustStoreType=pkcs12"),
            ]);
        }
        let str_extra_java_opts = extra_java_opts.join(" ");
        submit_cmd.extend(vec![
            format!("--conf spark.driver.extraJavaOptions=\"{str_extra_java_opts}\""),
            format!("--conf spark.executor.extraJavaOptions=\"{str_extra_java_opts}\""),
        ]);

        // repositories and packages arguments
        if !self.spec.deps.repositories.is_empty() {
            submit_cmd.extend(vec![format!(
                "--repositories {}",
                self.spec.deps.repositories.join(",")
            )]);
        }

        if !self.spec.deps.packages.is_empty() {
            submit_cmd.extend(vec![format!(
                "--conf spark.jars.packages={}",
                self.spec.deps.packages.join(",")
            )]);
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

        if let Some(RoleGroup {
            replicas: Some(replicas),
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
        submit_conf.extend(self.spec.spark_conf.clone());

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

        let artifact = self.application_artifact();
        submit_cmd.push(artifact.to_string());

        submit_cmd.extend(self.spec.args.clone());

        Ok(submit_cmd)
    }

    pub fn env(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<EnvVar> {
        let mut e: Vec<EnvVar> = self.spec.env.clone();
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

    pub fn driver_config(&self) -> Result<RoleConfig, Error> {
        if let Some(CommonConfiguration { mut config, .. }) = self.spec.driver.clone() {
            config.merge(&RoleConfig::default_config());
            fragment::validate(config).context(FragmentValidationFailureSnafu)
        } else {
            fragment::validate(RoleConfig::default_config()).context(FragmentValidationFailureSnafu)
        }
    }

    pub fn executor_config(&self) -> Result<RoleConfig, Error> {
        if let Some(RoleGroup {
            config: CommonConfiguration { mut config, .. },
            ..
        }) = self.spec.executor.clone()
        {
            config.merge(&RoleConfig::default_config());
            fragment::validate(config).context(FragmentValidationFailureSnafu)
        } else {
            fragment::validate(RoleConfig::default_config()).context(FragmentValidationFailureSnafu)
        }
    }

    pub fn pod_overrides(&self, role: SparkApplicationRole) -> Option<PodTemplateSpec> {
        match role {
            SparkApplicationRole::Submit => self.spec.job.clone().map(|j| j.pod_overrides),
            SparkApplicationRole::Driver => self.spec.driver.clone().map(|d| d.pod_overrides),
            SparkApplicationRole::Executor => {
                self.spec.executor.clone().map(|r| r.config.pod_overrides)
            }
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
                config: RoleConfig::default_config(),
                ..CommonConfiguration::default()
            }
        };

        let executor_conf = if self.spec.executor.is_some() {
            self.spec.executor.as_ref().unwrap().clone()
        } else {
            RoleGroup {
                replicas: Some(1),
                config: CommonConfiguration {
                    config: RoleConfig::default_config(),
                    ..CommonConfiguration::default()
                },
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
                    role_config: EmptyRoleConfig::default(),
                    role_groups: [(
                        "default".to_string(),
                        RoleGroup {
                            config: submit_conf,
                            replicas: Some(1),
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
                    role_config: EmptyRoleConfig::default(),
                    role_groups: [(
                        "default".to_string(),
                        RoleGroup {
                            config: driver_conf,
                            replicas: Some(1),
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
                    config: executor_conf.config.clone(),
                    role_config: EmptyRoleConfig::default(),
                    role_groups: [("default".to_string(), executor_conf)].into(),
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
    driver_config: &RoleConfig,
    props: &mut BTreeMap<String, String>,
) -> Result<(), Error> {
    if let Resources {
        cpu: CpuLimits {
            min: Some(min),
            max: Some(max),
        },
        ..
    } = &driver_config.resources
    {
        let min_cores = cores_from_quantity(min.0.clone())?;
        let max_cores = cores_from_quantity(max.0.clone())?;
        // will have default value from resources to apply if nothing set specifically
        props.insert("spark.driver.cores".to_string(), max_cores.clone());
        props.insert(
            "spark.kubernetes.driver.request.cores".to_string(),
            min_cores,
        );
        props.insert("spark.kubernetes.driver.limit.cores".to_string(), max_cores);
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
    }

    Ok(())
}

/// Translate resource limits to Spark config properties.
/// Spark will use these and *ignore* the resource limits in pod templates entirely.
fn resources_to_executor_props(
    for_java: bool,
    executor_config: &RoleConfig,
    props: &mut BTreeMap<String, String>,
) -> Result<(), Error> {
    if let Resources {
        cpu: CpuLimits {
            min: Some(min),
            max: Some(max),
        },
        ..
    } = &executor_config.resources
    {
        let min_cores = cores_from_quantity(min.0.clone())?;
        let max_cores = cores_from_quantity(max.0.clone())?;
        // will have default value from resources to apply if nothing set specifically
        props.insert("spark.executor.cores".to_string(), max_cores.clone());
        props.insert(
            "spark.kubernetes.executor.request.cores".to_string(),
            min_cores,
        );
        props.insert(
            "spark.kubernetes.executor.limit.cores".to_string(),
            max_cores,
        );
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
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    use crate::{cores_from_quantity, resources_to_executor_props, RoleConfig};
    use crate::{resources_to_driver_props, SparkApplication};
    use crate::{Quantity, SparkStorageConfig};
    use product_config::{types::PropertyNameKind, ProductConfigManager};
    use stackable_operator::commons::affinity::StackableAffinity;
    use stackable_operator::commons::resources::{
        CpuLimits, MemoryLimits, NoRuntimeLimits, Resources,
    };
    use stackable_operator::product_config_utils::ValidatedRoleConfigByPropertyKind;
    use stackable_operator::product_logging::spec::Logging;

    use indoc::indoc;
    use rstest::rstest;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn test_default_resource_limits() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(indoc! {"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-examples
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
        "})
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
        let spark_application = serde_yaml::from_str::<SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-examples
            spec:
              mode: cluster
              mainApplicationFile: test.py
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
                    "# })
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
        let driver_config = RoleConfig {
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
                "spark.kubernetes.driver.request.cores".to_string(),
                "1".to_string(),
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(expected, props);
    }

    #[test]
    fn test_resource_to_executor_props() {
        let executor_config = RoleConfig {
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
                "spark.kubernetes.executor.request.cores".to_string(),
                "1".to_string(),
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
        let spark_application = serde_yaml::from_str::<SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-examples
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
        "#})
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

        let expected_role_groups: HashMap<
            String,
            HashMap<PropertyNameKind, BTreeMap<String, String>>,
        > = vec![(
            "default".into(),
            vec![
                (PropertyNameKind::Env, BTreeMap::new()),
                (
                    PropertyNameKind::File("security.properties".into()),
                    vec![
                        ("networkaddress.cache.negative.ttl".into(), "0".into()),
                        ("networkaddress.cache.ttl".into(), "30".into()),
                    ]
                    .into_iter()
                    .collect(),
                ),
            ]
            .into_iter()
            .collect(),
        )]
        .into_iter()
        .collect();
        let expected: ValidatedRoleConfigByPropertyKind = vec![
            ("submit".into(), expected_role_groups.clone()),
            ("driver".into(), expected_role_groups.clone()),
            ("executor".into(), expected_role_groups),
        ]
        .into_iter()
        .collect();

        assert_eq!(expected, validated_config);
    }

    #[test]
    fn test_job_volume_mounts() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-examples
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              job:
                config:
                  volumeMounts:
                    - name: keytab
                      mountPath: /kerberos
              volumes:
                - name: keytab
                  configMap:
                    name: keytab
        "#})
        .unwrap();

        let got = spark_application.spark_job_volume_mounts(&None, &None);

        let expected = vec![
            VolumeMount {
                mount_path: "/stackable/spark/driver-pod-templates".into(),
                mount_propagation: None,
                name: "driver-pod-template".into(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
            },
            VolumeMount {
                mount_path: "/stackable/spark/executor-pod-templates".into(),
                mount_propagation: None,
                name: "executor-pod-template".into(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
            },
            VolumeMount {
                mount_path: "/kerberos".into(),
                mount_propagation: None,
                name: "keytab".into(),
                read_only: None,
                sub_path: None,
                sub_path_expr: None,
            },
        ];

        assert_eq!(got, expected);
    }
}
