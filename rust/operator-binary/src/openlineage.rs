//! OpenLineage lineage-emission types and helpers for [`SparkApplication`].
//!
//! [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use stackable_operator::{
    schemars::{self, JsonSchema},
    v2::types::kubernetes::ConfigMapName,
};

use crate::crd::{Error, ObjectHasNoNameSnafu, v1alpha1};

/// OpenLineage lineage emission for a [`SparkApplication`].
///
/// When enabled, the operator injects the OpenLineage Spark listener, points its HTTP transport
/// at the backend resolved from the discovery ConfigMap, and sets a stable job name. All injected
/// values are defaults: they can be overridden via `sparkConf`.
///
/// [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageSpec {
    /// Enable OpenLineage event emission. Defaults to `false` (nothing is injected).
    #[serde(default)]
    pub enabled: bool,

    /// The OpenLineage namespace lineage is reported under.
    /// Defaults to the application's Kubernetes namespace (`metadata.namespace`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// A stable OpenLineage job/application name. Setting this prevents fragmented run history
    /// (and the intermittent `unknown` job-name bug). If unset, the operator resolves it from
    /// `spark.app.name`, falling back to `metadata.name` (with a warning event).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_name: Option<String>,

    /// Name of the OpenLineage backend [discovery ConfigMap](DOCS_BASE_URL_PLACEHOLDER/concepts/service_discovery).
    /// It must contain the key `ADDRESS` with the base URL of the OpenLineage backend
    /// (e.g. `http://marquez:5000`). Mirrors the `vectorAggregatorConfigMapName` field on this CRD.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_map_name: Option<ConfigMapName>,
}

/// The resolved OpenLineage job/app name and where it came from.
/// See [`v1alpha1::SparkApplication::resolved_openlineage_app_name`].
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedOpenLineageAppName {
    /// The resolved, non-blank name written to `spark.openlineage.appName`.
    pub name: String,
    /// `true` when the name fell back to `metadata.name`; the controller then emits a warning event
    /// because a per-run-unique name (e.g. an orchestrator-generated `-<timestamp>` suffix) fragments
    /// backend run history.
    pub from_metadata_name: bool,
}

/// Appends `value` to a comma-separated `--conf` value in `submit_conf`, preserving any existing
/// (e.g. user-supplied) entries and skipping `value` if it is already present. Used for the
/// OpenLineage keys that must accumulate rather than clobber (`spark.extraListeners`, `spark.jars`).
pub(crate) fn append_conf_csv(submit_conf: &mut BTreeMap<String, String>, key: &str, value: &str) {
    match submit_conf.get_mut(key) {
        Some(existing) if !existing.is_empty() => {
            if !existing.split(',').any(|entry| entry.trim() == value) {
                existing.push(',');
                existing.push_str(value);
            }
        }
        _ => {
            submit_conf.insert(key.to_string(), value.to_string());
        }
    }
}

impl v1alpha1::SparkApplication {
    /// Resolves the stable OpenLineage job/app name and its provenance (MVP §5), in priority order:
    /// 1. `spec.openLineage.appName`, else
    /// 2. `spark.app.name` from `sparkConf`, else
    /// 3. `metadata.name` (a fallback the controller flags with a warning event, because a
    ///    per-run-unique name would fragment backend run history).
    ///
    /// Always yields a non-blank name — which is exactly what fixes the intermittent `unknown` bug.
    pub fn resolved_openlineage_app_name(&self) -> Result<ResolvedOpenLineageAppName, Error> {
        if let Some(app_name) = self
            .spec
            .open_lineage
            .as_ref()
            .and_then(|open_lineage| open_lineage.app_name.clone())
        {
            return Ok(ResolvedOpenLineageAppName {
                name: app_name,
                from_metadata_name: false,
            });
        }

        if let Some(app_name) = self.spec.spark_conf.get("spark.app.name") {
            return Ok(ResolvedOpenLineageAppName {
                name: app_name.clone(),
                from_metadata_name: false,
            });
        }

        Ok(ResolvedOpenLineageAppName {
            name: self.metadata.name.clone().context(ObjectHasNoNameSnafu)?,
            from_metadata_name: true,
        })
    }
}
