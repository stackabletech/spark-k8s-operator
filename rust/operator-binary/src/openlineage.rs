//! Operator-side OpenLineage helpers for [`SparkApplication`].
//!
//! The reusable OpenLineage types (`OpenLineageConnectionSpec`, `InlineConnectionOrReference`,
//! `OpenLineageJob`) live in the `stackable_operator::crd::openlineage` library module. This module
//! only holds the bits that depend on the operator's own `SparkApplication` (job-name resolution)
//! plus a spark-submit conf helper.
//!
//! [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication

use std::collections::BTreeMap;

use snafu::OptionExt;

use crate::crd::{Error, ObjectHasNoNameSnafu, v1alpha1};

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
