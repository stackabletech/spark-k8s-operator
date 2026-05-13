//! This module provides the SparkApplicationTemplateSpec CRD definition.

use std::{num::ParseIntError, str::ParseBoolError};

use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    kube::{Api, CustomResource, ResourceExt, api::ListParams},
    schemars::{self, JsonSchema},
    versioned::versioned,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::template_merger::deep_merge;

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
        /// The template body mirrors a [`SparkApplication`](super::super::v1alpha1::SparkApplication)
        /// spec exactly — it is merged into a `SparkApplication` at reconcile time, see
        /// [`merge_application_templates`].
        #[serde(flatten)]
        pub spec: crate::crd::v1alpha1::SparkApplicationSpec,
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

impl From<v1alpha1::SparkApplicationTemplate> for super::v1alpha1::SparkApplication {
    fn from(template: v1alpha1::SparkApplicationTemplate) -> super::v1alpha1::SparkApplication {
        super::v1alpha1::SparkApplication {
            metadata: template.metadata,
            spec: template.spec.spec,
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
                .cloned()
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
    use stackable_operator::versioned::test_utils::RoundtripTestData;

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

    impl RoundtripTestData for v1alpha1::SparkApplicationTemplateSpec {
        fn roundtrip_test_data() -> Vec<Self> {
            vec![]
        }
    }
}
