//! This module provides all required CRD definitions and additional helper methods.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use stackable_operator::kube::ResourceExt;
use stackable_operator::{
    kube::CustomResource,
    role_utils::CommonConfiguration,
    schemars::{self, JsonSchema},
};
use std::env::{self, VarError};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("object has no namespace associated"))]
    NoNamespace,
    #[snafu(display("failed to detect api host"))]
    ApiHostMissing { source: VarError },
    #[snafu(display("failed to detect api https port"))]
    ApiHttpsPortMissing { source: VarError },
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
}
/// SparkApplicationStatus CommandStatus
#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkApplication",
    shortname = "sc",
    status = "CommandStatus",
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
    pub args: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
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

    pub fn version(&self) -> Option<&str> {
        self.spec.version.as_deref()
    }

    pub fn mode(&self) -> Option<&str> {
        self.spec.mode.as_deref()
    }

    pub fn main_class(&self) -> Option<&str> {
        self.spec.main_class.as_deref()
    }

    pub fn image(&self) -> Option<&str> {
        self.spec.image.as_deref()
    }

    pub fn application_artifact(&self) -> Option<&str> {
        self.spec.main_application_file.as_deref()
    }

    pub fn build_command(&self) -> Result<Vec<String>, Error> {
        // get API end-point from in-pod environment variables
        let host = env::var("KUBERNETES_SERVICE_HOST").context(ApiHostMissingSnafu)?;
        let https_port =
            env::var("KUBERNETES_SERVICE_PORT_HTTPS").context(ApiHttpsPortMissingSnafu)?;

        // mandatory properties
        let mode = self.mode().context(ObjectHasNoDeployModeSnafu)?;
        let artifact = self
            .application_artifact()
            .context(ObjectHasNoArtifactSnafu)?;
        let name = self.metadata.name.clone().context(ObjectHasNoNameSnafu)?;

        let mut submit_cmd = vec![
            "/stackable/spark/bin/spark-submit".to_string(),
            "--verbose".to_string(),
            format!("--master k8s://https://{host}:{https_port}"),
            format!("--deploy-mode {mode}"),
            format!("--name {name}"),
            "--conf spark.kubernetes.driver.podTemplateFile=/stackable/spark/pod-templates/driver.yml".to_string(),
            "--conf spark.kubernetes.executor.podTemplateFile=/stackable/spark/pod-templates/executor.yml".to_string(),
            "--conf spark.kubernetes.driver.podTemplateContainerName=spark-driver-container".to_string(),
            "--conf spark.kubernetes.executor.podTemplateContainerName=spark-executor-container".to_string(),
            format!("--conf spark.kubernetes.driver.container.image={}", self.spec.spark_image.as_ref().unwrap()), // TODO!!! handle error
            format!("--conf spark.kubernetes.executor.container.image={}", self.spec.spark_image.as_ref().unwrap()),
            //"--conf spark.kubernetes.file.upload.path=dummy://doesnotexist".to_string(),
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem".to_string(),
            "--conf spark.driver.extraClassPath=/stackable/.ivy2/cache".to_string(),
            //"--conf spark.hadoop.fs.s3a.fast.upload=true".to_string(),
        ];

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

        // optional properties
        if let Some(executor) = self.spec.executor.as_ref() {
            submit_cmd.extend(executor.spark_config());
        }
        if let Some(driver) = self.spec.driver.as_ref() {
            submit_cmd.extend(driver.spark_config());
        }

        submit_cmd.push(artifact.to_string());

        if let Some(job_args) = self.spec.args.clone() {
            submit_cmd.extend(job_args);
        }

        Ok(submit_cmd)
    }
}

#[derive(Clone, Default, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SparkApplicationStatus {
    /// An opaque value that changes every time a discovery detail does
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_hash: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommonConfig {
    pub secret: Option<String>,
    pub log_dir: Option<String>,
    pub max_port_retries: Option<usize>,
    pub enable_monitoring: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DriverConfig {
    pub cores: Option<usize>,
    pub core_limit: Option<String>,
    pub memory: Option<String>,
}

impl DriverConfig {
    pub fn spark_config(&self) -> Vec<String> {
        let mut cmd = vec![];
        if let Some(cores) = &self.cores {
            cmd.push(format!("--conf spark.driver.cores={cores}"));
        }
        if let Some(core_limit) = &self.core_limit {
            cmd.push(format!(
                "--conf spark.kubernetes.executor.limit.cores={core_limit}"
            ));
        }
        if let Some(memory) = &self.memory {
            cmd.push(format!("--conf spark.driver.memory={memory}"));
        }
        cmd
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutorConfig {
    pub cores: Option<usize>,
    pub instances: Option<usize>,
    pub memory: Option<String>,
}

impl ExecutorConfig {
    pub fn spark_config(&self) -> Vec<String> {
        let mut cmd = vec![];
        if let Some(cores) = &self.cores {
            cmd.push(format!("--conf spark.executor.cores={cores}"));
        }
        if let Some(instances) = &self.instances {
            cmd.push(format!("--conf spark.executor.instances={instances}"));
        }
        if let Some(memory) = &self.memory {
            cmd.push(format!("--conf spark.executor.memory={memory}"));
        }
        cmd
    }
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CommandStatus {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<Time>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<Time>,
}
