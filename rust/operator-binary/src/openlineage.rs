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
use stackable_operator::{
    crd::authentication::core::v1alpha1::{AuthenticationClass, AuthenticationClassProvider},
    k8s_openapi::api::core::v1::{EnvVar, EnvVarSource, SecretKeySelector},
};

use crate::crd::{
    Error, ObjectHasNoNameSnafu,
    constants::{
        OPENLINEAGE_AUTH_API_KEY_ENV, OPENLINEAGE_AUTH_SECRET_KEY, OPENLINEAGE_AUTH_TYPE_API_KEY,
        OPENLINEAGE_AUTH_TYPE_ENV, OPENLINEAGE_TRANSPORT_TYPE_ENV, OPENLINEAGE_TRANSPORT_TYPE_HTTP,
        OPENLINEAGE_TRANSPORT_URL_ENV,
    },
    v1alpha1,
};

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

/// Resolved OpenLineage authentication for a workload.
///
/// Holds the name of the Secret (in the workload's namespace) whose [`OPENLINEAGE_AUTH_SECRET_KEY`]
/// entry carries the bearer token. Produced during dereferencing from the connection's
/// `authenticationClassRef` (Static provider only) and consumed by
/// [`SparkApplication::env`](crate::crd::v1alpha1::SparkApplication::env).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedOpenLineageAuth {
    pub secret_name: String,
}

/// Extracts the credentials Secret name from a resolved OpenLineage [`AuthenticationClass`].
///
/// Only the `Static` provider is supported for OpenLineage (its Secret holds the bearer token
/// under [`OPENLINEAGE_AUTH_SECRET_KEY`]). Any other provider returns `Err(provider_name)` so the
/// caller can surface a clear error naming the offending provider.
pub(crate) fn openlineage_auth_secret_name(
    auth_class: &AuthenticationClass,
) -> Result<String, String> {
    match &auth_class.spec.provider {
        AuthenticationClassProvider::Static(provider) => {
            Ok(provider.user_credentials_secret.name.clone())
        }
        other => Err(other.to_string()),
    }
}

/// Builds the driver env vars that deliver the **entire** OpenLineage HTTP transport — type, URL and
/// bearer-token auth — via the OpenLineage Java client's `OPENLINEAGE__` env-var configuration.
///
/// This is used only when an `AuthenticationClass` is configured. The whole transport must come from
/// one source: OpenLineage resolves `transport` as a unit, so if `spark.openlineage.transport.type`
/// or `.url` were set via `--conf`, the transport would be taken entirely from SparkConf and the
/// env-provided `auth` sub-tree would be silently dropped (verified against openlineage-spark
/// 1.51.0). Delivering the token as a `secretKeyRef` also keeps it out of the spark-submit `--conf`
/// args and the Job/pod spec — the operator never reads it.
pub(crate) fn openlineage_transport_env_vars(
    transport_url: &str,
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

#[cfg(test)]
mod tests {
    use stackable_operator::{
        crd::authentication::{
            core::v1alpha1::{
                AuthenticationClass, AuthenticationClassProvider, AuthenticationClassSpec,
            },
            r#static, tls,
        },
        k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
    };

    use super::*;

    fn auth_class(provider: AuthenticationClassProvider) -> AuthenticationClass {
        AuthenticationClass {
            metadata: ObjectMeta::default(),
            spec: AuthenticationClassSpec { provider },
        }
    }

    #[test]
    fn secret_name_extracted_from_static_provider() {
        let ac = auth_class(AuthenticationClassProvider::Static(
            r#static::v1alpha1::AuthenticationProvider {
                user_credentials_secret: r#static::v1alpha1::UserCredentialsSecretRef {
                    name: "ol-token".to_string(),
                },
            },
        ));

        assert_eq!(openlineage_auth_secret_name(&ac).unwrap(), "ol-token");
    }

    #[test]
    fn non_static_provider_is_rejected_naming_the_provider() {
        let ac = auth_class(AuthenticationClassProvider::Tls(
            tls::v1alpha1::AuthenticationProvider {
                client_cert_secret_class: None,
            },
        ));

        let err = openlineage_auth_secret_name(&ac).unwrap_err();
        assert!(
            err.to_lowercase().contains("tls"),
            "error should name the offending provider, got: {err}"
        );
    }

    #[test]
    fn transport_env_vars_carry_full_transport_with_token_secret_ref() {
        let vars = openlineage_transport_env_vars("https://marquez:5000", "my-secret");

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
