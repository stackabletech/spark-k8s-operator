use crate::constants::*;
use stackable_operator::builder::VolumeBuilder;
use stackable_operator::commons::s3::{
    InlinedS3BucketSpec, S3AccessStyle, S3BucketDef, S3ConnectionSpec,
};
use stackable_operator::k8s_openapi::api::core::v1::{
    EmptyDirVolumeSource, EnvVar, LocalObjectReference, Volume, VolumeMount,
};
use stackable_operator::memory::{to_java_heap_value, BinaryMultiple};
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
pub enum Error {}

#[derive(Clone, CustomResource, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleaner: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<HashMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, JsonSchema)]
#[allow(clippy::derive_partial_eq_without_eq)]
#[serde(rename_all = "camelCase")]
pub struct SparkHistoryStatus {
    pub phase: String,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize, Display)]
#[serde(rename_all = "camelCase")]
pub enum LogFileDirectorySpec {
    #[strum(serialize = "s3")]
    S3(S3LogFileDirectorySpec),
}

#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct S3LogFileDirectorySpec {
    pub prefix: String,
    pub bucket: S3BucketDef,
}
