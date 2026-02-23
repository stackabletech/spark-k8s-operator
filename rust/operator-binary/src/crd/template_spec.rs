//! This module provides the SparkApplicationTemplateSpec CRD definition.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use stackable_operator::{
    commons::product_image_selection::ProductImage,
    crd::s3,
    k8s_openapi::api::core::v1::{EnvVar, Volume},
    kube::CustomResource,
    role_utils::{CommonConfiguration, JavaCommonConfig, RoleGroup},
    schemars::{self, JsonSchema},
    utils::crds::raw_object_list_schema,
    versioned::versioned,
};

use super::{
    history::LogFileDirectorySpec,
    job_dependencies::JobDependencies,
    roles::{RoleConfigFragment, SparkMode, SubmitConfigFragment},
};

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

    #[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkApplicationTemplateStatus {
        pub phase: String,
    }

    /// A Spark application template. This resource is managed by the Stackable operator for Apache Spark.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/).
    #[versioned(crd(
        group = "spark.stackable.tech",
        shortname = "sparkapptemplate",
        namespaced,
    ))]
    #[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
    #[serde(rename_all = "camelCase")]
    pub struct SparkApplicationTemplateSpec {
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
        //
        // IMPORTANT: Please note that the jvmArgumentOverrides have no effect here!
        // However, due to product-config things I wasn't able to remove them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub job: Option<CommonConfiguration<SubmitConfigFragment, JavaCommonConfig>>,

        /// The driver role specifies the configuration that, together with the driver pod template, is used by
        /// Spark to create driver pods.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub driver: Option<CommonConfiguration<RoleConfigFragment, JavaCommonConfig>>,

        /// The executor role specifies the configuration that, together with the driver pod template, is used by
        /// Spark to create the executor pods.
        /// This is RoleGroup instead of plain CommonConfiguration because it needs to allow for the number of replicas.
        /// to be specified.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub executor: Option<RoleGroup<RoleConfigFragment, JavaCommonConfig>>,

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
        pub s3connection: Option<s3::v1alpha1::InlineConnectionOrReference>,

        /// Arguments passed directly to the job artifact.
        #[serde(default)]
        pub args: Vec<String>,

        /// A list of volumes that can be made available to the job, driver or executors via their volume mounts.
        #[serde(default)]
        #[schemars(schema_with = "raw_object_list_schema")]
        pub volumes: Vec<Volume>,

        /// A list of environment variables that will be set in the job pod and the driver and executor
        /// pod templates.
        #[serde(default)]
        pub env: Vec<EnvVar>,

        /// The log file directory definition used by the Spark history server.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub log_file_directory: Option<LogFileDirectorySpec>,
    }
}

impl From<&v1alpha1::SparkApplicationTemplate> for super::v1alpha1::SparkApplication {
    fn from(template: &v1alpha1::SparkApplicationTemplate) -> super::v1alpha1::SparkApplication {
        let spec = super::v1alpha1::SparkApplicationSpec {
            mode: template.spec.mode.clone(),
            main_class: template.spec.main_class.clone(),
            main_application_file: template.spec.main_application_file.clone(),
            image: template.spec.image.clone(),
            spark_image: template.spec.spark_image.clone(),
            vector_aggregator_config_map_name: template
                .spec
                .vector_aggregator_config_map_name
                .clone(),
            job: template.spec.job.clone(),
            driver: template.spec.driver.clone(),
            executor: template.spec.executor.clone(),
            spark_conf: template.spec.spark_conf.clone(),
            deps: template.spec.deps.clone(),
            s3connection: template.spec.s3connection.clone(),
            args: template.spec.args.clone(),
            volumes: template.spec.volumes.clone(),
            env: template.spec.env.clone(),
            log_file_directory: template.spec.log_file_directory.clone(),
        };

        super::v1alpha1::SparkApplication {
            metadata: template.metadata.clone(),
            spec,
            status: None,
        }
    }
}
