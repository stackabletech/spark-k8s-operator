use crate::constants::*;
use stackable_operator::commons::product_image_selection::{ProductImage, ResolvedProductImage};
use stackable_operator::commons::s3::S3BucketDef;
use stackable_operator::product_config::types::PropertyNameKind;
use stackable_operator::product_config::ProductConfigManager;
use stackable_operator::product_config_utils::{
    transform_all_roles_to_config, validate_all_roles_and_groups_config, Configuration,
    ValidatedRoleConfigByPropertyKind,
};
use stackable_operator::role_utils::Role;

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::labels::ObjectLabels;
use stackable_operator::{
    commons::resources::{NoRuntimeLimits, ResourcesFragment},
    config::{fragment::Fragment, merge::Merge},
};
use stackable_operator::{
    kube::CustomResource,
    schemars::{self, JsonSchema},
};
use strum::Display;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
}

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
    pub log_file_directory: LogFileDirectorySpec,
    pub nodes: Role<HistoryConfig>,
}

impl SparkHistoryServer {
    pub fn labels<'a>(
        &'a self,
        app_version_label: &'a str,
        role_group: &'a str,
    ) -> ObjectLabels<SparkHistoryServer> {
        ObjectLabels {
            owner: self,
            app_name: APP_NAME,
            app_version: app_version_label,
            operator_name: OPERATOR_NAME,
            controller_name: HISTORY_CONTROLLER_NAME,
            role: HISTORY_ROLE_NAME,
            role_group,
        }
    }

    pub fn command_args(&self) -> Vec<String> {
        vec![
            "-c",
            "/stackable/spark/sbin/start-history-server.sh",
            "--properties-file",
            HISTORY_CONFIG_FILE_NAME_FULL,
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

    pub fn validated_role_config(
        &self,
        resolved_product_image: &ResolvedProductImage,
        product_config: &ProductConfigManager,
    ) -> Result<ValidatedRoleConfigByPropertyKind, Error> {
        let roles_to_validate: HashMap<String, (Vec<PropertyNameKind>, Role<HistoryConfig>)> =
            vec![(
                HISTORY_ROLE_NAME.to_string(),
                (
                    vec![PropertyNameKind::File(HISTORY_CONFIG_FILE_NAME.to_string())],
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

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cleaner: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spark_conf: Option<HashMap<String, String>>,
    pub resources: Option<ResourcesFragment<NoStorageConfig, NoRuntimeLimits>>,
}

#[derive(Clone, Debug, Default, JsonSchema, Fragment)]
#[fragment_attrs(
    derive(Clone, Debug, Default, Deserialize, Merge, JsonSchema, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct NoStorageConfig {}

impl Configuration for HistoryConfig {
    type Configurable = SparkHistoryServer;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        file: &str,
    ) -> stackable_operator::product_config_utils::ConfigResult<BTreeMap<String, Option<String>>>
    {
        let mut result = BTreeMap::new();
        if let HISTORY_CONFIG_FILE_NAME = file {
            // Copy user provided spark configuration
            result.extend(self.spark_conf.as_ref().map_or(vec![], |c| {
                c.iter()
                    .map(|(k, v)| (k.clone(), Some(v.clone())))
                    .collect()
            }));
        }
        Ok(result)
    }
}
