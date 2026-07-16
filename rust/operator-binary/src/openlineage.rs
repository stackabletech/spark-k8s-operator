//! OpenLineage lineage-emission types and helpers for [`SparkApplication`].
//!
//! [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use stackable_operator::{
    commons::tls_verification::TlsClientDetails,
    schemars::{self, JsonSchema},
};

use crate::crd::{Error, ObjectHasNoNameSnafu, v1alpha1};

/// OpenLineage lineage emission for a [`SparkApplication`].
///
/// Adding this block enables OpenLineage: the operator injects the OpenLineage Spark listener,
/// points its transport at `<scheme>://<host>:<port>`, and sets a stable job name. Omit the block
/// to leave OpenLineage off. The injected `namespace` and `appName` are defaults that can be
/// overridden via `sparkConf`.
///
/// The transport scheme is `https` when `tls.verification.server` is configured, otherwise `http`.
///
/// [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageSpec {
    /// Host of the OpenLineage backend the transport points at (e.g. `marquez`).
    /// Combined with `port` into the transport URL `<scheme>://<host>:<port>`.
    pub host: String,

    /// Port of the OpenLineage backend (e.g. `5000`).
    /// Combined with `host` into the transport URL `<scheme>://<host>:<port>`.
    pub port: u16,

    /// TLS configuration for the connection to the OpenLineage backend. When
    /// `tls.verification.server` is set, the transport uses `https` instead of `http`.
    #[serde(flatten)]
    pub tls: TlsClientDetails,

    /// The OpenLineage namespace lineage is reported under.
    /// Defaults to the application's Kubernetes namespace (`metadata.namespace`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,

    /// A stable OpenLineage job/application name. Setting this prevents fragmented run history
    /// (and the intermittent `unknown` job-name bug). If unset, the operator resolves it from
    /// `spark.app.name`, falling back to `metadata.name` (with a warning event).
    /// TODO: maybe rename to job_name as it would be more appropriate. Trino users can put
    /// placeholders like $QUERY_ID, $USER, $SOURCE, $CLIENT_IP.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_name: Option<String>,
}

impl OpenLineageSpec {
    /// The OpenLineage transport URL, built from `host` and `port`. The scheme is `https` when
    /// `tls.verification.server` is configured, otherwise `http`.
    pub fn transport_url(&self) -> String {
        let scheme = if self.tls.uses_tls_verification() {
            "https"
        } else {
            "http"
        };
        format!(
            "{scheme}://{host}:{port}",
            host = self.host,
            port = self.port
        )
    }
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
