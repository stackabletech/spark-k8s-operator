//! This module provides the SparkApplicationTemplateSpec CRD definition.

use std::{collections::HashMap, num::ParseIntError, str::ParseBoolError};

use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::product_image_selection::ProductImage,
    crd::s3,
    k8s_openapi::api::core::v1::{EnvVar, Volume},
    kube::{Api, CustomResource, ResourceExt, api::ListParams},
    role_utils::{CommonConfiguration, JavaCommonConfig, RoleGroup},
    schemars::{self, JsonSchema},
    utils::crds::raw_object_list_schema,
    versioned::versioned,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use super::{
    history::LogFileDirectorySpec,
    job_dependencies::JobDependencies,
    roles::{RoleConfigFragment, SparkMode, SubmitConfigFragment},
};
use crate::crd::merger::deep_merge;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to build template merge options from application annotations"))]
    BuildMergeTemplateOptions,

    #[snafu(display(
        "invalid index value [{value}] for template names. value must be non negative integer"
    ))]
    InvalidAnnotationTemplateIndex {
        source: ParseIntError,
        value: String,
    },

    #[snafu(display("invalid regex for template names annotation"))]
    InvalidAnnotationTemplateNameRx { source: regex::Error },

    #[snafu(display("invalid value [{value}] for annotation [{name}]"))]
    InvalidAnnotationBooleanValue {
        source: ParseBoolError,
        name: String,
        value: String,
    },

    #[snafu(display("failed to list SparkApplicationTemplate named [{template_name}]"))]
    ListSparkApplicationTemplates {
        template_name: String,
        source: stackable_operator::kube::Error,
    },
}

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

    /// A Spark application template. This resource is managed by the Stackable operator for Apache Spark.
    /// Find more information on how to use it and the resources that the operator generates in the
    /// [operator documentation](DOCS_BASE_URL_PLACEHOLDER/spark-k8s/).
    #[versioned(crd(
        group = "spark.stackable.tech",
        plural = "sparkapptemplates",
        shortname = "sparkapptemplate",
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

impl From<&v1alpha1::SparkApplicationTemplate>
    for super::v1alpha1::ResolvedSparkApplicationTemplate
{
    fn from(value: &v1alpha1::SparkApplicationTemplate) -> Self {
        Self {
            name: value.name_any(),
            uid: value.metadata.uid.clone(),
        }
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

// Values of this type are built from the metadata.annotations of the spark application objects.
#[derive(Default)]
struct MergeTemplateOptions {
    merge: bool,
    template_names: Vec<String>,
    update_strategy: TemplateUpdateStrategy,
    apply_strategy: TemplateApplyStrategy,
}

#[derive(Default, Debug, PartialEq, strum::EnumString)]
#[strum(serialize_all = "camelCase")]
enum TemplateUpdateStrategy {
    #[default]
    OnCreate,
}

#[derive(Default, Debug, PartialEq, strum::EnumString)]
#[strum(serialize_all = "camelCase")]
enum TemplateApplyStrategy {
    #[default]
    Enforce,
}

// This annotation regex selects the template names to apply.
// The <index> value determines the merge order.
const ANNO_TEMPLATE_NAME_RX: &str = "^spark-application\\.template\\.(?P<index>\\d+)\\.name$";
// A boolean that enable/disables template merging.
const ANNO_TEMPLATE_MERGE: &str = "spark-application.template.merge";
// This annotation instructs the operator when to update patched applications.
// Currently templates are merged only once in the lifetime of a spark application, namely at creation time.
const ANNO_TEMPLATE_UPDATE_STRATEGY: &str = "spark-application.template.updateStrategy";
// This tells the operator how to handle merging errors.
// Currently only "enforce" is supported, meaning: fail in case of errors.
const ANNO_TEMPLATE_APPLY_STRATEGY: &str = "spark-application.template.applyStrategy";

impl TryFrom<&super::v1alpha1::SparkApplication> for MergeTemplateOptions {
    type Error = Error;

    // Build a `MergeTemplateOptions` value from the metadata annotations of a `SparkApplication`.
    // The `template_names` are sorted in the order in which they are applied as specified by the `index`.
    fn try_from(app: &super::v1alpha1::SparkApplication) -> Result<Self, Error> {
        if let Some(annos) = app.metadata.annotations.as_ref() {
            let merge: bool = match annos.get(ANNO_TEMPLATE_MERGE) {
                Some(v) => {
                    v.parse::<bool>()
                        .with_context(|_| InvalidAnnotationBooleanValueSnafu {
                            name: ANNO_TEMPLATE_MERGE.to_string(),
                            value: v.to_string(),
                        })?
                }
                _ => false,
            };
            let update_strategy = match annos.get(ANNO_TEMPLATE_UPDATE_STRATEGY) {
                Some(v) => v.parse::<TemplateUpdateStrategy>().unwrap_or_default(),
                _ => TemplateUpdateStrategy::default(),
            };
            let apply_strategy = match annos.get(ANNO_TEMPLATE_APPLY_STRATEGY) {
                Some(v) => v.parse::<TemplateApplyStrategy>().unwrap_or_default(),
                _ => TemplateApplyStrategy::default(),
            };

            // Extract template indexes and names.
            // Sort by indexes and discard them.
            let template_name_rx =
                Regex::new(ANNO_TEMPLATE_NAME_RX).context(InvalidAnnotationTemplateNameRxSnafu)?;

            let mut template_index_name = vec![];
            for (k, v) in annos.iter() {
                if let Some(caps) = template_name_rx.captures(k) {
                    let index = caps["index"].parse::<u8>().context(
                        InvalidAnnotationTemplateIndexSnafu {
                            value: caps["index"].to_string(),
                        },
                    )?;
                    template_index_name.push((index, v));
                }
            }
            template_index_name.sort_by_key(|(index, _)| *index);
            let template_names: Vec<String> = template_index_name
                .iter()
                .map(|(_, v)| (*v).clone())
                .collect();

            Ok(MergeTemplateOptions {
                merge,
                update_strategy,
                apply_strategy,
                template_names,
            })
        } else {
            Ok(MergeTemplateOptions::default())
        }
    }
}

pub(crate) struct MergeTemplateResult {
    pub app: Option<super::v1alpha1::SparkApplication>,
    pub resolved_template_ref: Vec<super::v1alpha1::ResolvedSparkApplicationTemplate>,
}

// Merges one or more [`SparkApplicationTemplate`](v1alpha1::SparkApplicationTemplate) resources
// into the given [`SparkApplication`](super::v1alpha1::SparkApplication).
//
// Template merging is controlled by annotations on the `SparkApplication`.
//
// The function returns an empty [`MergeTemplateResult`] immediately if:
//
// 1. `spark-application.template.merge` annotation is `"false"`.
// 2. `spark-application.template.merge` annotation is `"true"`
//    and `spark-application.template.updateStrategy` annotation is `"onCreate"`
//    and `spark_application.status.resolved_template_ref` is not empty.
//
// When merging is enabled, the function:
//
// 1. Parses the merge options (template names, update strategy, apply strategy) from the
//    application's annotations.
// 2. Checks whether templates have already been applied and, if the update strategy is
//    [`TemplateUpdateStrategy::OnCreate`], skips re-applying them.
// 3. Resolves the named templates from the Kubernetes API in the order defined by their index
//    annotations.
// 4. Performs a deep merge of all resolved templates followed by the `SparkApplication` itself
//    (left-to-right, with the application having the highest priority).
//
// # Returns
//
// - `Ok(MergeTemplateResult { app: Some(...), resolved_template_ref: [...] })` when at least one
//   template was found and merged.
// - `Ok(MergeTemplateResult { app: None, resolved_template_ref: [] })` when merging is disabled,
//   the update strategy prevents re-applying, or no templates were resolved.
// - `Err(Error)` if annotation parsing fails or a Kubernetes API call returns an error.
pub(crate) async fn merge_application_templates(
    client: &stackable_operator::client::Client,
    spark_application: &super::v1alpha1::SparkApplication,
) -> Result<MergeTemplateResult, Error> {
    let app_name = spark_application.name_any();

    tracing::info!("app [{app_name}] : begin template merging");

    let default_result = MergeTemplateResult {
        app: None,
        resolved_template_ref: vec![],
    };

    let merge_template_options = MergeTemplateOptions::try_from(spark_application)?;

    if merge_template_options.merge {
        let have_resolved_template_refs = spark_application
            .status
            .as_ref()
            .map(|s| s.resolved_template_ref.is_empty())
            .unwrap_or(false);
        if have_resolved_template_refs
            && merge_template_options.update_strategy == TemplateUpdateStrategy::OnCreate
        {
            tracing::info!("app [{app_name}] : templates already merged.");
            // Templates have already been applied and the update strategy (OnCreate) only allows
            // to apply them once (on creation).
            return Ok(default_result);
        }

        // Retrieve the template objects from the kube api.
        // In the future if we support additional strategies in addition to "enforce",
        // this list might not be identical to the one in `merge_template_options`
        // because some objects might be missing.
        let templates = resolve(
            client,
            &merge_template_options.template_names,
            merge_template_options.apply_strategy,
        )
        .await?;

        if !templates.is_empty() {
            // The list of apps from templates in the correct order.
            // The final element is the actual Spark application being reconciled
            // which has the highest priority during merging.
            let mut template_apps: Vec<super::v1alpha1::SparkApplication> = templates
                .iter()
                .map(super::v1alpha1::SparkApplication::from)
                .collect::<Vec<super::v1alpha1::SparkApplication>>();
            template_apps.push(spark_application.clone());

            // Deep merge app templates from left to right
            let merged_app = template_apps
                .into_iter()
                .reduce(|merge_app, app| deep_merge(&merge_app, &app));

            // In the future, when different apply strategies are supported, the effective template
            // list might differ from what is in the app annotations so make sure we return the correct one.
            let effective_template_list = templates
                .iter()
                .map(super::v1alpha1::ResolvedSparkApplicationTemplate::from)
                .collect::<Vec<super::v1alpha1::ResolvedSparkApplicationTemplate>>();

            tracing::info!(
                "app [{app_name}] : successfully merged templates [{tnames}]",
                tnames = effective_template_list
                    .iter()
                    .map(|rsat| rsat.name.clone())
                    .collect::<Vec<String>>()
                    .join(",")
            );
            return Ok(MergeTemplateResult {
                app: merged_app,
                resolved_template_ref: effective_template_list,
            });
        } else {
            tracing::warn!("app [{app_name}]: template merging enabled but no templates resolved")
        }
    } else {
        tracing::info!("app [{app_name}]: no templates to merge")
    }

    tracing::info!("app [{app_name}] : done template merging");

    Ok(default_result)
}

async fn resolve(
    client: &stackable_operator::client::Client,
    template_names: &[String],
    apply_strategy: TemplateApplyStrategy,
) -> Result<Vec<v1alpha1::SparkApplicationTemplate>, Error> {
    if template_names.is_empty() {
        return Ok(vec![]);
    }

    let templates_api = Api::<v1alpha1::SparkApplicationTemplate>::all(client.as_kube_client());
    let mut resolved_templates = Vec::new();
    for template_name in template_names {
        let template_res = templates_api
            .list(&ListParams::default().fields(&format!("metadata.name={template_name}")))
            .await
            .with_context(|_| ListSparkApplicationTemplatesSnafu { template_name })
            .map(|object_list| object_list.items.into_iter().next());

        let template = if apply_strategy == TemplateApplyStrategy::Enforce {
            // When the apply strategy is "enforce" we immediately raise an error,
            // otherwise we ignore it and skip to the next template in the list.
            template_res?
        } else {
            template_res.ok().flatten()
        };

        if let Some(template) = template {
            resolved_templates.push(template);
        }
    }

    Ok(resolved_templates)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn try_from_parses_annotations_and_sorts_template_names() {
        let spark_application =
            serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
                ---
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkApplication
                metadata:
                  name: app-with-templates
                  annotations:
                    spark-application.template.merge: "true"
                    spark-application.template.updateStrategy: "onCreate"
                    spark-application.template.applyStrategy: "enforce"
                    spark-application.template.2.name: "template-c"
                    spark-application.template.0.name: "template-a"
                    spark-application.template.1.name: "template-b"
                spec:
                  mode: cluster
                  mainApplicationFile: local:///app.py
                  sparkImage:
                    productVersion: "3.5.8"
            "#})
            .unwrap();

        let options = MergeTemplateOptions::try_from(&spark_application).unwrap();

        assert!(options.merge);
        assert!(matches!(
            options.update_strategy,
            TemplateUpdateStrategy::OnCreate
        ));
        assert!(matches!(
            options.apply_strategy,
            TemplateApplyStrategy::Enforce
        ));
        assert_eq!(
            options.template_names,
            vec![
                "template-a".to_string(),
                "template-b".to_string(),
                "template-c".to_string()
            ]
        );
    }

    #[test]
    fn try_from_without_annotations_returns_default_options() {
        let spark_application =
            serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
                ---
                apiVersion: spark.stackable.tech/v1alpha1
                kind: SparkApplication
                metadata:
                  name: app-without-annotations
                spec:
                  mode: cluster
                  mainApplicationFile: local:///app.py
                  sparkImage:
                    productVersion: "3.5.8"
            "#})
            .unwrap();

        let options = MergeTemplateOptions::try_from(&spark_application).unwrap();

        assert!(!options.merge);
        assert!(matches!(
            options.update_strategy,
            TemplateUpdateStrategy::OnCreate
        ));
        assert!(matches!(
            options.apply_strategy,
            TemplateApplyStrategy::Enforce
        ));
        assert!(options.template_names.is_empty());
    }
}
