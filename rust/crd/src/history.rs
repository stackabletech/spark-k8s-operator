use crate::constants::*;
use stackable_operator::builder::VolumeBuilder;
use stackable_operator::commons::product_image_selection::{ProductImage, ResolvedProductImage};
use stackable_operator::commons::s3::{
    InlinedS3BucketSpec, S3AccessStyle, S3BucketDef, S3ConnectionSpec,
};
use stackable_operator::k8s_openapi::api::core::v1::{
    EmptyDirVolumeSource, EnvVar, LocalObjectReference, Volume, VolumeMount,
};
use stackable_operator::memory::{to_java_heap_value, BinaryMultiple};

use std::cell::RefCell;
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
pub enum Error {}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, Serialize)]
#[kube(
    group = "spark.stackable.tech",
    version = "v1alpha1",
    kind = "SparkHistoryServer",
    shortname = "shs",
    status = "SparkHistoryStatus",
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleaner: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<HashMap<String, String>>,
    pub log_file_directory: LogFileDirectorySpec,
}

impl SparkHistoryServer {
    pub fn labels<'a>(
        &'a self,
        resolved_product_image: &'a ResolvedProductImage,
    ) -> ObjectLabels<SparkHistoryServer> {
        ObjectLabels {
            owner: self,
            app_name: APP_NAME,
            app_version: &resolved_product_image.app_version_label,
            operator_name: OPERATOR_NAME,
            controller_name: HISTORY_CONTROLLER_NAME,
            role: HISTORY_ROLE_NAME,
            role_group: HISTORY_GROUP_NAME,
        }
    }

    pub fn command_args(&self) -> Vec<String> {
        vec![
            "-c",
            "'mkdir -p /tmp/logs/spark && /stackable/spark/sbin/start-history-server.sh --properties-file /stackable/spark/conf/spark-defaults.conf'",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    pub fn config(&self) -> String {
        vec![
            ("spark.history.ui.port", "18080"),
            ("spark.history.fs.logDirectory", "file:///tmp/logs/spark"),
            (
                "spark.history.provider",
                "org.apache.spark.deploy.history.FsHistoryProvider",
            ),
            ("spark.history.fs.update.interval", "10s"),
            ("spark.history.retainedApplications", "50"),
            ("spark.history.ui.maxApplications", "2147483647"), // Integer.MAX_VALUE
            ("spark.history.fs.cleaner.enabled", "false"),
            ("spark.history.fs.cleaner.interval", "1d"),
            ("spark.history.fs.cleaner.maxAge", "7d"),
            ("spark.history.fs.cleaner.maxNum", "2147483647"),
            // local history cache of application data (default is off)
            //("spark.history.store.maxDiskUsage", "10g"),
            //("spark.history.store.path", "/tmp/logs/spark/cache"),
            ("", ""),
        ]
        .into_iter()
        .map(|(key, value)| format!("{key} {value}"))
        .collect::<Vec<String>>()
        .join("\n")
    }
}
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkHistoryStatus {
    pub phase: String,
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
    pub bucket: S3BucketDef,
}
