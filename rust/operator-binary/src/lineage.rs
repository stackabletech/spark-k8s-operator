//! Operator-side OpenLineage helpers for [`SparkApplication`].
//!
//! The reusable OpenLineage types (`OpenLineageConnectionSpec`, `InlineConnectionOrReference`,
//! `OpenLineageConfig`) live in the `stackable_operator::crd::openlineage` library module. This module
//! only holds the bits that depend on the operator's own `SparkApplication` (job-name resolution)
//! plus a spark-submit conf helper.
//!
//! [`SparkApplication`]: crate::crd::v1alpha1::SparkApplication

use std::collections::BTreeMap;

use snafu::OptionExt;
use stackable_operator::{
    crd::openlineage::{
        ResolvedOpenLineageConnection,
        v1alpha1::{HttpTransport, OpenLineageTransport},
    },
    k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, SecretKeySelector},
};

use crate::crd::{
    Error, ObjectHasNoNameSnafu,
    constants::{
        OPENLINEAGE_AUTH_API_KEY_ENV, OPENLINEAGE_AUTH_SECRET_KEY, OPENLINEAGE_AUTH_TYPE_API_KEY,
        OPENLINEAGE_AUTH_TYPE_ENV, OPENLINEAGE_TRANSPORT_ENDPOINT_ENV,
        OPENLINEAGE_TRANSPORT_TYPE_ENV, OPENLINEAGE_TRANSPORT_TYPE_HTTP,
        OPENLINEAGE_TRANSPORT_URL_ENV,
    },
    v1alpha1,
};

/// Returns the HTTP transport of a resolved OpenLineage connection.
///
/// `http` is currently the only [`OpenLineageTransport`] variant, so this cannot fail. When further
/// transports (for example Apache Kafka) are added upstream, this stops compiling and every caller
/// has to decide what the new transport means for Spark.
pub(crate) fn http_transport(conn: &ResolvedOpenLineageConnection) -> &HttpTransport {
    let OpenLineageTransport::Http(http) = &conn.transport;
    http
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

/// Builds the driver env vars that deliver the **entire** OpenLineage HTTP transport — type, URL and
/// bearer-token auth — via the OpenLineage Java client's `OPENLINEAGE__` env-var configuration.
///
/// This is used only when `credentialsSecretName` is configured. The whole transport must come from
/// one source: OpenLineage resolves `transport` as a unit, so if `spark.openlineage.transport.type`
/// or `.url` were set via `--conf`, the transport would be taken entirely from SparkConf and the
/// env-provided `auth` sub-tree would be silently dropped (verified against openlineage-spark
/// 1.51.0). Delivering the token as a `secretKeyRef` also keeps it out of the spark-submit `--conf`
/// args and the Job/pod spec — the operator never reads it.
pub(crate) fn openlineage_transport_env_vars(
    transport_url: &str,
    endpoint: &str,
    secret_name: &str,
) -> Vec<EnvVar> {
    let literal = |name: &str, value: &str| EnvVar {
        name: name.to_string(),
        value: Some(value.to_string()),
        value_from: None,
    };

    vec![
        literal(
            OPENLINEAGE_TRANSPORT_TYPE_ENV,
            OPENLINEAGE_TRANSPORT_TYPE_HTTP,
        ),
        literal(OPENLINEAGE_TRANSPORT_URL_ENV, transport_url),
        literal(OPENLINEAGE_TRANSPORT_ENDPOINT_ENV, endpoint),
        literal(OPENLINEAGE_AUTH_TYPE_ENV, OPENLINEAGE_AUTH_TYPE_API_KEY),
        EnvVar {
            name: OPENLINEAGE_AUTH_API_KEY_ENV.to_string(),
            value: None,
            value_from: Some(EnvVarSource {
                secret_key_ref: Some(SecretKeySelector {
                    name: secret_name.to_string(),
                    key: OPENLINEAGE_AUTH_SECRET_KEY.to_string(),
                    optional: None,
                }),
                ..EnvVarSource::default()
            }),
        },
    ]
}

impl v1alpha1::SparkApplication {
    /// Resolves the stable OpenLineage job/app name (MVP §5), in priority order:
    /// 1. `spec.lineage.jobName`, else
    /// 2. `spark.app.name` from `sparkConf`, else
    /// 3. `metadata.name`.
    ///
    /// Always yields a non-blank name — which is exactly what fixes the intermittent `unknown` bug.
    pub fn resolved_lineage_app_name(&self) -> Result<String, Error> {
        if let Some(app_name) = self
            .spec
            .lineage
            .as_ref()
            .and_then(|lineage| lineage.job_name.clone())
        {
            return Ok(app_name);
        }

        if let Some(app_name) = self.spec.spark_conf.get("spark.app.name") {
            return Ok(app_name.clone());
        }

        self.metadata.name.clone().context(ObjectHasNoNameSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_env_vars_carry_full_transport_with_token_secret_ref() {
        let vars = openlineage_transport_env_vars(
            "https://marquez:5000",
            HttpTransport::DEFAULT_PATH,
            "my-secret",
        );

        let get = |name: &str| {
            vars.iter()
                .find(|v| v.name == name)
                .unwrap_or_else(|| panic!("env var {name} present"))
        };

        // The whole transport is delivered via env so OpenLineage keeps the auth sub-tree.
        assert_eq!(
            get(OPENLINEAGE_TRANSPORT_TYPE_ENV).value.as_deref(),
            Some(OPENLINEAGE_TRANSPORT_TYPE_HTTP)
        );
        assert_eq!(
            get(OPENLINEAGE_TRANSPORT_URL_ENV).value.as_deref(),
            Some("https://marquez:5000")
        );
        assert_eq!(
            get(OPENLINEAGE_TRANSPORT_ENDPOINT_ENV).value.as_deref(),
            Some(HttpTransport::DEFAULT_PATH)
        );
        assert_eq!(
            get(OPENLINEAGE_AUTH_TYPE_ENV).value.as_deref(),
            Some(OPENLINEAGE_AUTH_TYPE_API_KEY)
        );

        let key_var = get(OPENLINEAGE_AUTH_API_KEY_ENV);
        assert!(
            key_var.value.is_none(),
            "the token must never be delivered as a literal value"
        );
        let selector = key_var
            .value_from
            .as_ref()
            .and_then(|source| source.secret_key_ref.as_ref())
            .expect("api key sourced from a secretKeyRef");
        assert_eq!(selector.name, "my-secret");
        assert_eq!(selector.key, OPENLINEAGE_AUTH_SECRET_KEY);
    }
}
