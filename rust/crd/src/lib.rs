//! This module provides all required CRD definitions and additional helper methods.

pub mod constants;

use constants::*;
use stackable_operator::builder::VolumeBuilder;
use stackable_operator::commons::s3::{
    InlinedS3BucketSpec, S3AccessStyle, S3BucketDef, S3ConnectionSpec,
};
use stackable_operator::k8s_openapi::api::core::v1::{
    EmptyDirVolumeSource, EnvVar, LocalObjectReference, Volume, VolumeMount,
};

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use stackable_operator::commons::resources::{CpuLimits, MemoryLimits, NoRuntimeLimits, Resources};
use stackable_operator::kube::ResourceExt;
use stackable_operator::labels;
use stackable_operator::{
    config::merge::Merge,
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
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    pub phase: String,
}

#[derive(Clone, Eq, Debug, Default, Deserialize, Merge, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkStorageConfig {}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkConfig {
    pub resources: Option<Resources<SparkStorageConfig, NoRuntimeLimits>>,
}

impl SparkConfig {
    fn default_resources() -> Resources<SparkStorageConfig, NoRuntimeLimits> {
        Resources {
            cpu: CpuLimits {
                min: Some(Quantity("50m".to_owned())),
                max: Some(Quantity("100m".to_owned())),
            },
            memory: MemoryLimits {
                limit: Some(Quantity("1Gi".to_owned())),
                runtime_limits: NoRuntimeLimits {},
            },
            storage: SparkStorageConfig {},
        }
    }
}

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    pub s3bucket: Option<S3BucketDef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<EnvVar>>,
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
        format!("{}-pod-template", self.name())
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

    pub fn volumes(&self, s3bucket: &Option<InlinedS3BucketSpec>) -> Vec<Volume> {
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

        let s3_conn = s3bucket.as_ref().and_then(|i| i.connection.as_ref());

        if let Some(S3ConnectionSpec {
            credentials: Some(credentials),
            ..
        }) = s3_conn
        {
            result.push(credentials.to_volume("s3-credentials"));
        }
        result
    }

    pub fn executor_volume_mounts(
        &self,
        s3bucket: &Option<InlinedS3BucketSpec>,
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

        self.add_common_volume_mounts(result, s3bucket)
    }

    pub fn driver_volume_mounts(&self, s3bucket: &Option<InlinedS3BucketSpec>) -> Vec<VolumeMount> {
        let result: Vec<VolumeMount> = self
            .spec
            .driver
            .as_ref()
            .and_then(|driver_conf| driver_conf.volume_mounts.clone())
            .iter()
            .flat_map(|v| v.iter())
            .cloned()
            .collect();

        self.add_common_volume_mounts(result, s3bucket)
    }

    fn add_common_volume_mounts(
        &self,
        mut mounts: Vec<VolumeMount>,
        s3bucket: &Option<InlinedS3BucketSpec>,
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
        let s3_conn = s3bucket.as_ref().and_then(|i| i.connection.as_ref());

        if let Some(S3ConnectionSpec {
            credentials: Some(_credentials),
            ..
        }) = s3_conn
        {
            mounts.push(VolumeMount {
                name: "s3-credentials".into(),
                mount_path: S3_SECRET_DIR_NAME.into(),
                ..VolumeMount::default()
            });
        }
        mounts
    }

    pub fn recommended_labels(&self) -> BTreeMap<String, String> {
        let mut ls = labels::build_common_labels_for_all_managed_resources(APP_NAME, &self.name());
        if let Some(version) = self.version() {
            ls.insert(labels::APP_VERSION_LABEL.to_string(), version.to_string());
        }
        ls.insert(
            labels::APP_MANAGED_BY_LABEL.to_string(),
            format!("{}-operator", APP_NAME),
        );
        ls
    }

    pub fn build_command(
        &self,
        serviceaccount_name: &str,
        s3bucket: &Option<InlinedS3BucketSpec>,
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

        // Always use the upper bound of available cpu cores as otherwise the request can never be exceeded

        // TODO: Round up to the next whole number and use whole numbers
        if let Some(driver) = &self.spec.driver {
            if let Some(Resources {
                cpu: CpuLimits { max: Some(max), .. },
                ..
            }) = &driver.resources
            {
                submit_cmd.push(format!("--conf spark.driver.cores={}", max.0));
            }
        }
        if let Some(executors) = &self.spec.executor {
            if let Some(Resources {
                cpu: CpuLimits { max: Some(max), .. },
                ..
            }) = &executors.resources
            {
                submit_cmd.push(format!("--conf spark.executor.cores={}", max.0));
            }
            if let Some(instances) = executors.instances {
                submit_cmd.push(format!("--conf spark.executor.instances={}", instances));
            }
        }

        // See https://spark.apache.org/docs/latest/running-on-kubernetes.html#dependency-management
        // for possible S3 related properties
        if let Some(endpoint) = s3bucket.as_ref().and_then(|s3| s3.endpoint()) {
            submit_cmd.push(format!("--conf spark.hadoop.fs.s3a.endpoint={}", endpoint));
        }

        if let Some(conn) = s3bucket.as_ref().and_then(|i| i.connection.as_ref()) {
            match conn.access_style {
                Some(S3AccessStyle::Path) => {
                    submit_cmd
                        .push("--conf spark.hadoop.fs.s3a.path.style.access=true".to_string());
                }
                Some(S3AccessStyle::VirtualHosted) => {}
                None => {}
            }
            if conn.credentials.as_ref().is_some() {
                // We don't use the credentials at all here but assume they are available
                submit_cmd.push(format!(
                    "--conf spark.hadoop.fs.s3a.access.key=$(cat {secret_dir}/{file_name})",
                    secret_dir = S3_SECRET_DIR_NAME,
                    file_name = ACCESS_KEY_ID
                ));
                submit_cmd.push(format!(
                    "--conf spark.hadoop.fs.s3a.secret.key=$(cat {secret_dir}/{file_name})",
                    secret_dir = S3_SECRET_DIR_NAME,
                    file_name = SECRET_ACCESS_KEY
                ));
            }
        }

        // conf arguments that are not driver or executor specific
        if let Some(spark_conf) = self.spec.spark_conf.clone() {
            for (key, value) in spark_conf {
                submit_cmd.push(format!("--conf {key}={value}"));
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

    pub fn driver_node_selector(&self) -> Option<std::collections::BTreeMap<String, String>> {
        self.spec
            .driver
            .as_ref()
            .and_then(|driver_config| driver_config.node_selector.clone())
    }

    pub fn executor_node_selector(&self) -> Option<std::collections::BTreeMap<String, String>> {
        self.spec
            .executor
            .as_ref()
            .and_then(|executor_config| executor_config.node_selector.clone())
    }

    pub fn job_resources(&self) -> Option<Resources<SparkStorageConfig, NoRuntimeLimits>> {
        let conf = SparkConfig::default_resources();

        let mut resources = self
            .spec
            .job
            .clone()
            .and_then(|spark_config| spark_config.resources)
            .unwrap_or_default();

        resources.merge(&conf);
        Some(resources)
    }

    pub fn driver_resources(&self) -> Option<Resources<SparkStorageConfig, NoRuntimeLimits>> {
        if let Some(driver_config) = self.spec.driver.clone() {
            driver_config.spark_config()
        } else {
            Some(DriverConfig::default_resources())
        }
    }

    pub fn executor_resources(&self) -> Option<Resources<SparkStorageConfig, NoRuntimeLimits>> {
        if let Some(executor_config) = self.spec.executor.clone() {
            executor_config.spark_config()
        } else {
            Some(ExecutorConfig::default_resources())
        }
    }
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
    pub resources: Option<Resources<SparkStorageConfig, NoRuntimeLimits>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,
}

impl DriverConfig {
    fn default_resources() -> Resources<SparkStorageConfig, NoRuntimeLimits> {
        Resources {
            cpu: CpuLimits {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("2".to_owned())),
            },
            memory: MemoryLimits {
                limit: Some(Quantity("2Gi".to_owned())),
                runtime_limits: NoRuntimeLimits {},
            },
            storage: SparkStorageConfig {},
        }
    }

    pub fn spark_config(&self) -> Option<Resources<SparkStorageConfig, NoRuntimeLimits>> {
        let default_resources = DriverConfig::default_resources();

        let mut resources = self.resources.clone().unwrap_or_default();
        resources.merge(&default_resources);

        Some(resources)
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub instances: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Resources<SparkStorageConfig, NoRuntimeLimits>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_selector: Option<std::collections::BTreeMap<String, String>>,
}

impl ExecutorConfig {
    fn default_resources() -> Resources<SparkStorageConfig, NoRuntimeLimits> {
        Resources {
            cpu: CpuLimits {
                min: Some(Quantity("1".to_owned())),
                max: Some(Quantity("4".to_owned())),
            },
            memory: MemoryLimits {
                limit: Some(Quantity("4Gi".to_owned())),
                runtime_limits: NoRuntimeLimits {},
            },
            storage: SparkStorageConfig {},
        }
    }

    pub fn spark_config(&self) -> Option<Resources<SparkStorageConfig>> {
        let default_resources = ExecutorConfig::default_resources();

        let mut resources = self.resources.clone().unwrap_or_default();
        resources.merge(&default_resources);

        Some(resources)
    }
}

#[cfg(test)]
mod tests {
    use crate::ImagePullPolicy;
    use crate::LocalObjectReference;
    use crate::SparkApplication;
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
        assert_eq!("50m", job_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!("100m", job_resources.clone().unwrap().cpu.max.unwrap().0);

        let driver_resources = &spark_application.driver_resources();
        assert_eq!("1", driver_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!("2", driver_resources.clone().unwrap().cpu.max.unwrap().0);

        let executor_resources = &spark_application.executor_resources();
        assert_eq!("1", executor_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!("4", executor_resources.clone().unwrap().cpu.max.unwrap().0);
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
  namespace: default
spec:
  version: "1.0"
  sparkImage: docker.stackable.tech/stackable/spark-k8s:3.2.1-hadoop3.2-python39-aws1.11.375-stackable0.3.0
  mode: cluster
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: s3a://stackable-spark-k8s-jars/jobs/spark-examples_2.12-3.2.1.jar
  job:
    resources:
      cpu:
        min: "100m"
        max: "1"
      memory:
        limit: "1G"
  driver:
    resources:
      cpu:
        min: "1"
        max: "1500m"
      memory:
        limit: "512m"
  executor:
    instances: 8
    resources:
      cpu:
        min: "4"
        max: "6"
      memory:
        limit: "10Gi"
  config:
    enableMonitoring: true
        "#,
        )
        .unwrap();

        let command = spark_application
            .build_command("sa", &None)
            .map_err(|e| format!("{e}"))
            .unwrap();
        assert!(command.contains(&"--conf spark.executor.instances=8".to_string()));

        let job_resources = &spark_application.job_resources();
        assert_eq!("100m", job_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!("1", job_resources.clone().unwrap().cpu.max.unwrap().0);

        let driver_resources = &spark_application.driver_resources();
        assert_eq!("1", driver_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!(
            "1500m",
            driver_resources.clone().unwrap().cpu.max.unwrap().0
        );
        // TODO Switch to assert!(command.contains(&"--conf spark.driver.cores=2".to_string()));
        assert!(command.contains(&"--conf spark.driver.cores=150m".to_string()));

        let executor_resources = &spark_application.executor_resources();
        assert_eq!("4", executor_resources.clone().unwrap().cpu.min.unwrap().0);
        assert_eq!("6", executor_resources.clone().unwrap().cpu.max.unwrap().0);
        assert!(command.contains(&"--conf spark.executor.cores=6".to_string()));
    }
}
