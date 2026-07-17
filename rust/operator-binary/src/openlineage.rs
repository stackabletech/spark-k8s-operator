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
    /// Resolves the stable OpenLineage job/app name (MVP §5), in priority order:
    /// 1. `spec.openLineage.appName`, else
    /// 2. `spark.app.name` from `sparkConf`, else
    /// 3. `metadata.name`.
    ///
    /// Always yields a non-blank name — which is exactly what fixes the intermittent `unknown` bug.
    pub fn resolved_openlineage_app_name(&self) -> Result<String, Error> {
        if let Some(app_name) = self
            .spec
            .open_lineage
            .as_ref()
            .and_then(|open_lineage| open_lineage.app_name.clone())
        {
            return Ok(app_name);
        }

        if let Some(app_name) = self.spec.spark_conf.get("spark.app.name") {
            return Ok(app_name.clone());
        }

        self.metadata.name.clone().context(ObjectHasNoNameSnafu)
    }
}
