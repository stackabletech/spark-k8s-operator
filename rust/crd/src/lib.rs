//! This module provides all required CRD definitions and additional helper methods.

pub mod affinity;
pub mod constants;
pub mod history;
pub mod s3logdir;

use constants::*;
use history::LogFileDirectorySpec;
use s3logdir::S3LogDir;
use stackable_operator::builder::VolumeBuilder;
use stackable_operator::commons::affinity::StackableAffinity;
use stackable_operator::commons::s3::{S3AccessStyle, S3ConnectionDef, S3ConnectionSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    EmptyDirVolumeSource, EnvVar, LocalObjectReference, Volume, VolumeMount,
};
use stackable_operator::memory::{BinaryMultiple, MemoryQuantity};
use std::cmp::max;

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::kube::ResourceExt;
use stackable_operator::labels::ObjectLabels;
use stackable_operator::{
    commons::resources::{
        CpuLimits, CpuLimitsFragment, MemoryLimits, MemoryLimitsFragment, NoRuntimeLimits,
        NoRuntimeLimitsFragment, Resources, ResourcesFragment,
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
};
use stackable_operator::{
    k8s_openapi::apimachinery::pkg::api::resource::Quantity,
    kube::CustomResource,
    role_utils::CommonConfiguration,
    schemars::{self, JsonSchema},
};
use strum::{Display, EnumString};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("object defines no deploy mode"))]
    ObjectHasNoDeployMode,
    #[snafu(display("object defines no main class"))]
    ObjectHasNoMainClass,
    #[snafu(display("object defines no application artifact"))]
    ObjectHasNoArtifact,
    #[snafu(display("object defines no pod image"))]
    ObjectHasNoImage,
    #[snafu(display("object has no name"))]
    ObjectHasNoName,
    #[snafu(display("application has no Spark image"))]
    NoSparkImage,
    #[snafu(display("failed to convert java heap config to unit [{unit}]"))]
    FailedToConvertJavaHeap {
        source: stackable_operator::error::Error,
        unit: String,
    },
    #[snafu(display("failed to convert to quantity"))]
    FailedQuantityConversion,
    #[snafu(display("failed to parse value"))]
    FailedParseToFloatConversion,
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
}

pub enum SparkApplicationRole {
    Driver,
    Executor,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    pub phase: String,
}

#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        Merge,
        JsonSchema,
        PartialEq,
        Serialize,
    ),
    allow(clippy::derive_partial_eq_without_eq),
    serde(rename_all = "camelCase")
)]
pub struct SparkStorageConfig {}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkConfig {
    pub resources: Option<ResourcesFragment<SparkStorageConfig, NoRuntimeLimits>>,
}

impl SparkConfig {
    fn default_resources() -> ResourcesFragment<SparkStorageConfig, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("500m".to_owned())),
                max: Some(Quantity("1".to_owned())),
            },
            memory: MemoryLimitsFragment {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
            storage: SparkStorageConfigFragment {},
        }
    }
}

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, Serialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_image: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_image_pull_policy: Option<ImagePullPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_image_pull_secrets: Option<Vec<LocalObjectReference>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<SparkConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver: Option<DriverConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<ExecutorConfig>,
    #[serde(flatten)]
    pub config: Option<CommonConfiguration<CommonConfig>>,
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

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize, Display, EnumString)]
pub enum ImagePullPolicy {
    Always,
    IfNotPresent,
    Never,
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
    pub fn enable_monitoring(&self) -> Option<bool> {
        let spec: &SparkApplicationSpec = &self.spec;
        spec.config
            .as_ref()
            .map(|common_configuration| &common_configuration.config)
            .and_then(|common_config| common_config.enable_monitoring)
    }

    pub fn pod_template_config_map_name(&self) -> String {
        format!("{}-pod-template", self.name_unchecked())
    }

    pub fn mode(&self) -> Option<&str> {
        self.spec.mode.as_deref()
    }

    pub fn image(&self) -> Option<&str> {
        self.spec.image.as_deref()
    }

    pub fn spark_image_pull_policy(&self) -> Option<ImagePullPolicy> {
        self.spec.spark_image_pull_policy.clone()
    }

    pub fn spark_image_pull_secrets(&self) -> Option<Vec<LocalObjectReference>> {
        self.spec.spark_image_pull_secrets.clone()
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

    pub fn volumes(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
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

        result
    }

    pub fn executor_volume_mounts(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        let result: Vec<VolumeMount> = self
            .spec
            .executor
            .as_ref()
            .and_then(|executor_conf| executor_conf.volume_mounts.clone())
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
            .collect();

        self.add_common_volume_mounts(result, s3conn, s3logdir)
    }

    pub fn driver_volume_mounts(
        &self,
        s3conn: &Option<S3ConnectionSpec>,
        s3logdir: &Option<S3LogDir>,
    ) -> Vec<VolumeMount> {
        let result: Vec<VolumeMount> = self
            .spec
            .driver
            .as_ref()
            .and_then(|driver_conf| driver_conf.volume_mounts.clone())
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
            .collect();

        self.add_common_volume_mounts(result, s3conn, s3logdir)
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
            format!("--conf spark.kubernetes.driver.podTemplateFile={VOLUME_MOUNT_PATH_POD_TEMPLATES}/driver.yml"),
            format!("--conf spark.kubernetes.executor.podTemplateFile={VOLUME_MOUNT_PATH_POD_TEMPLATES}/executor.yml"),
            format!("--conf spark.kubernetes.driver.podTemplateContainerName={CONTAINER_NAME_DRIVER}"),
            format!("--conf spark.kubernetes.executor.podTemplateContainerName={CONTAINER_NAME_EXECUTOR}"),
            format!("--conf spark.kubernetes.namespace={}", self.metadata.namespace.as_ref().context(NoNamespaceSnafu)?),
            format!("--conf spark.kubernetes.driver.container.image={}", self.spec.spark_image.as_ref().context(NoSparkImageSnafu)?),
            format!("--conf spark.kubernetes.executor.container.image={}", self.spec.spark_image.as_ref().context(NoSparkImageSnafu)?),
            format!("--conf spark.kubernetes.authenticate.driver.serviceAccountName={}", serviceaccount_name),
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

        // resource limits, either declared or taken from defaults
        if let Resources {
            cpu: CpuLimits { max: Some(max), .. },
            ..
        } = &self.driver_resources()?
        {
            submit_conf.insert(
                "spark.kubernetes.driver.limit.cores".to_string(),
                max.0.clone(),
            );
            let cores =
                cores_from_quantity(max.0.clone()).map_err(|_| Error::FailedQuantityConversion)?;
            // will have default value from resources to apply if nothing set specifically
            submit_conf.insert("spark.driver.cores".to_string(), cores);
        }
        if let Resources {
            cpu: CpuLimits { min: Some(min), .. },
            ..
        } = &self.driver_resources()?
        {
            submit_conf.insert(
                "spark.kubernetes.driver.request.cores".to_string(),
                min.0.clone(),
            );
        }
        if let Resources {
            memory: MemoryLimits {
                limit: Some(limit), ..
            },
            ..
        } = &self.driver_resources()?
        {
            let memory = self
                .subtract_spark_memory_overhead(limit)
                .map_err(|_| Error::FailedQuantityConversion)?;
            submit_conf.insert("spark.driver.memory".to_string(), memory);
        }

        if let Resources {
            cpu: CpuLimits { max: Some(max), .. },
            ..
        } = &self.executor_resources()?
        {
            submit_conf.insert(
                "spark.kubernetes.executor.limit.cores".to_string(),
                max.0.clone(),
            );
            let cores =
                cores_from_quantity(max.0.clone()).map_err(|_| Error::FailedQuantityConversion)?;
            // will have default value from resources to apply if nothing set specifically
            submit_conf.insert("spark.executor.cores".to_string(), cores);
        }
        if let Resources {
            cpu: CpuLimits { min: Some(min), .. },
            ..
        } = &self.executor_resources()?
        {
            submit_conf.insert(
                "spark.kubernetes.executor.request.cores".to_string(),
                min.0.clone(),
            );
        }
        if let Resources {
            memory: MemoryLimits {
                limit: Some(limit), ..
            },
            ..
        } = &self.executor_resources()?
        {
            let memory = self
                .subtract_spark_memory_overhead(limit)
                .map_err(|_| Error::FailedQuantityConversion)?;
            submit_conf.insert("spark.executor.memory".to_string(), memory);
        }

        if let Some(executors) = &self.spec.executor {
            if let Some(instances) = executors.instances {
                submit_conf.insert(
                    "spark.executor.instances".to_string(),
                    instances.to_string(),
                );
            }
        }

        if let Some(log_dir) = s3_log_dir {
            submit_conf.extend(log_dir.application_spark_config());
        }

        // conf arguments: these should follow - and thus override - values set from resource limits above
        if let Some(spark_conf) = self.spec.spark_conf.clone() {
            submit_conf.extend(spark_conf);
        }
        // ...before being added to the command collection
        for (key, value) in submit_conf {
            submit_cmd.push(format!("--conf {key}={value}"));
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

    /// A memory overhead will be applied using a factor of 0.1 (JVM jobs) or 0.4 (non-JVM jobs),
    /// being not less than 384MB. The resource limit should keep this transparent by reducing the
    /// declared memory limit accordingly.
    fn subtract_spark_memory_overhead(&self, limit: &Quantity) -> Result<String, Error> {
        // determine job-type using class name: scala/java will declare an application and main class;
        // R and python will just declare the application name/file (for python this could be .zip/.py/.egg).
        // Spark itself just checks the application name - See e.g.
        // https://github.com/apache/spark/blob/01c7a46f24fb4bb4287a184a3d69e0e5c904bc50/core/src/main/scala/org/apache/spark/deploy/SparkSubmit.scala#L1092
        let non_jvm_factor = if self.spec.main_class.is_some() {
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

        let reduced_memory =
            (MemoryQuantity::try_from(limit).context(FailedToConvertJavaHeapSnafu {
                unit: BinaryMultiple::Mebi.to_java_memory_unit(),
            })? * non_jvm_factor)
                .scale_to(BinaryMultiple::Mebi)
                .floor()
                .value as u32;

        let deduction = max(MIN_MEMORY_OVERHEAD, original_memory - reduced_memory);

        Ok(format!("{}m", original_memory - deduction))
    }

    pub fn env(&self) -> Vec<EnvVar> {
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
        e
    }

    pub fn affinity(&self, role: SparkApplicationRole) -> Option<StackableAffinity> {
        match role {
            SparkApplicationRole::Driver => self
                .spec
                .driver
                .as_ref()
                .and_then(|driver_config| driver_config.affinity.clone()),
            SparkApplicationRole::Executor => self
                .spec
                .executor
                .as_ref()
                .and_then(|executor_config| executor_config.affinity.clone()),
        }
    }

    pub fn job_resources(&self) -> Result<Resources<SparkStorageConfig, NoRuntimeLimits>, Error> {
        let conf = SparkConfig::default_resources();

        let mut resources = self
            .spec
            .job
            .clone()
            .and_then(|spark_config| spark_config.resources)
            .unwrap_or_default();

        resources.merge(&conf);
        fragment::validate(resources).context(FragmentValidationFailureSnafu)
    }

    pub fn driver_resources(
        &self,
    ) -> Result<Resources<SparkStorageConfig, NoRuntimeLimits>, Error> {
        let resources = if let Some(driver_config) = self.spec.driver.clone() {
            driver_config.spark_config()
        } else {
            DriverConfig::default_resources()
        };
        fragment::validate(resources).context(FragmentValidationFailureSnafu)
    }

    pub fn executor_resources(
        &self,
    ) -> Result<Resources<SparkStorageConfig, NoRuntimeLimits>, Error> {
        let resources = if let Some(executor_config) = self.spec.executor.clone() {
            executor_config.spark_config()
        } else {
            ExecutorConfig::default_resources()
        };
        fragment::validate(resources).context(FragmentValidationFailureSnafu)
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

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonConfig {
    pub secret: Option<String>,
    pub log_dir: Option<String>,
    pub max_port_retries: Option<usize>,
    pub enable_monitoring: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesFragment<SparkStorageConfig, NoRuntimeLimits>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<StackableAffinity>,
}

impl DriverConfig {
    fn default_resources() -> ResourcesFragment<SparkStorageConfig, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            memory: MemoryLimitsFragment {
                limit: Some(Quantity("2Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
            storage: SparkStorageConfigFragment {},
        }
    }

    fn spark_config(&self) -> ResourcesFragment<SparkStorageConfig, NoRuntimeLimits> {
        let default_resources = DriverConfig::default_resources();

        let mut resources = self.resources.clone().unwrap_or_default();

        resources.merge(&default_resources);
        resources
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub instances: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<ResourcesFragment<SparkStorageConfig, NoRuntimeLimits>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub affinity: Option<StackableAffinity>,
}

impl ExecutorConfig {
    fn default_resources() -> ResourcesFragment<SparkStorageConfig, NoRuntimeLimits> {
        ResourcesFragment {
            cpu: CpuLimitsFragment {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("4".to_owned())),
            },
            memory: MemoryLimitsFragment {
                limit: Some(Quantity("4Gi".to_owned())),
                runtime_limits: NoRuntimeLimitsFragment {},
            },
            storage: SparkStorageConfigFragment {},
        }
    }

    fn spark_config(&self) -> ResourcesFragment<SparkStorageConfig, NoRuntimeLimits> {
        let default_resources = ExecutorConfig::default_resources();

        let mut resources = self.resources.clone().unwrap_or_default();

        resources.merge(&default_resources);
        resources
    }
}

#[cfg(test)]
mod tests {
    use crate::LocalObjectReference;
    use crate::Quantity;
    use crate::SparkApplication;
    use crate::{cores_from_quantity, ImagePullPolicy};
    use rstest::rstest;
    use stackable_operator::builder::ObjectMetaBuilder;
    use stackable_operator::commons::s3::{
        S3AccessStyle, S3BucketSpec, S3ConnectionDef, S3ConnectionSpec,
    };
    use stackable_operator::commons::tls::{Tls, TlsVerification};
    use std::str::FromStr;

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
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar
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
            Some("s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar".to_string()),
            spark_application.spec.main_application_file
        );
        assert_eq!(
            Some(1),
            spark_application.spec.spark_conf.map(|m| m.keys().len())
        );

        assert!(spark_application.spec.spark_image.is_some());

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
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
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
        assert!(spark_application.spec.spark_image.is_some());
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
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
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

        assert!(spark_application.spec.spark_image.is_some());
        assert!(spark_application.spec.mode.is_some());
        assert!(spark_application.spec.args.is_some());
        assert!(spark_application.spec.deps.is_some());
        assert!(spark_application.spec.driver.is_some());
        assert!(spark_application.spec.executor.is_some());

        assert!(spark_application.spec.main_class.is_none());
        assert!(spark_application.spec.image.is_none());
    }

    #[test]
    fn test_image_actions() {
        let spark_application = serde_yaml::from_str::<SparkApplication>(
            r#"
---
apiVersion: spark.stackable.tech/v1alpha1
kind: SparkApplication
metadata:
  name: spark-pi-local
  namespace: default
spec:
  version: "1.0"
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-stackable0.4.0
  sparkImagePullPolicy: Always
  sparkImagePullSecrets:
    - name: myregistrykey
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: local:///stackable/spark/examples/jars/spark-examples_2.12-3.2.1.jar
  sparkConf:
    spark.kubernetes.node.selector.node: "2"
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
        "#,
        )
        .unwrap();

        assert_eq!(
            Some(vec![LocalObjectReference {
                name: Some("myregistrykey".to_string())
            }]),
            spark_application.spark_image_pull_secrets()
        );
        assert_eq!(
            Some(ImagePullPolicy::Always),
            spark_application.spark_image_pull_policy()
        );
    }

    #[test]
    fn test_image_pull_policy_ser() {
        assert_eq!("Never", ImagePullPolicy::Never.to_string());
        assert_eq!("Always", ImagePullPolicy::Always.to_string());
        assert_eq!("IfNotPresent", ImagePullPolicy::IfNotPresent.to_string());
    }

    #[test]
    fn test_image_pull_policy_de() {
        assert_eq!(
            ImagePullPolicy::Always,
            ImagePullPolicy::from_str("Always").unwrap()
        );
        assert_eq!(
            ImagePullPolicy::Never,
            ImagePullPolicy::from_str("Never").unwrap()
        );
        assert_eq!(
            ImagePullPolicy::IfNotPresent,
            ImagePullPolicy::from_str("IfNotPresent").unwrap()
        );
    }

    #[test]
    fn test_ser_inline() {
        let bucket = S3BucketSpec {
            bucket_name: Some("test-bucket-name".to_owned()),
            connection: Some(S3ConnectionDef::Inline(S3ConnectionSpec {
                host: Some("host".to_owned()),
                port: Some(8080),
                credentials: None,
                access_style: Some(S3AccessStyle::VirtualHosted),
                tls: Some(Tls {
                    verification: TlsVerification::None {},
                }),
            })),
        };

        assert_eq!(
            serde_yaml::to_string(&bucket).unwrap(),
            "---
bucketName: test-bucket-name
connection:
  inline:
    host: host
    port: 8080
    accessStyle: VirtualHosted
    tls:
      verification:
        none: {}
"
            .to_owned()
        )
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
  executor:
    instances: 1
  config:
    enableMonitoring: true
        "#,
        )
        .unwrap();

        let job_resources = &spark_application.job_resources();
        assert_eq!(
            "500m",
            job_resources.as_ref().unwrap().clone().cpu.min.unwrap().0
        );
        assert_eq!(
            "1",
            job_resources.as_ref().unwrap().clone().cpu.max.unwrap().0
        );

        let driver_resources = &spark_application.driver_resources();
        assert_eq!(
            "1",
            driver_resources
                .as_ref()
                .unwrap()
                .clone()
                .cpu
                .min
                .unwrap()
                .0
        );
        assert_eq!(
            "2",
            driver_resources
                .as_ref()
                .unwrap()
                .clone()
                .cpu
                .max
                .unwrap()
                .0
        );

        let executor_resources = &spark_application.executor_resources();
        assert_eq!(
            "1",
            executor_resources
                .as_ref()
                .unwrap()
                .clone()
                .cpu
                .min
                .unwrap()
                .0
        );
        assert_eq!(
            "4",
            executor_resources
                .as_ref()
                .unwrap()
                .clone()
                .cpu
                .max
                .unwrap()
                .0
        );
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
  job:
    resources:
      cpu:
        min: "100m"
        max: "200m"
      memory:
        limit: "1G"
  driver:
    resources:
      cpu:
        min: "1"
        max: "1300m"
      memory:
        limit: "512m"
  executor:
    instances: 1
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
            "1300m",
            &spark_application
                .driver_resources()
                .unwrap()
                .cpu
                .max
                .unwrap()
                .0
        );
        assert_eq!(
            "500m",
            &spark_application
                .executor_resources()
                .unwrap()
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
}
