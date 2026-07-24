use std::{collections::BTreeMap, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::v2::{
    config_file_writer::{PropertiesWriterError, to_java_properties_string},
    role_utils::JavaCommonConfig,
    types::operator::RoleName,
};
use strum::{Display, EnumIter};

use super::crd::CONNECT_EXECUTOR_ROLE_NAME;
use crate::{
    connect::crd::CONNECT_SERVER_ROLE_NAME,
    crd::constants::{
        DEFAULT_JVM_SECURITY_DNS_CACHE_NEGATIVE_TTL, DEFAULT_JVM_SECURITY_DNS_CACHE_TTL,
        JVM_SECURITY_PROPERTY_DNS_CACHE_NEGATIVE_TTL, JVM_SECURITY_PROPERTY_DNS_CACHE_TTL,
    },
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to serialize spark properties"))]
    SparkProperties { source: PropertiesWriterError },

    #[snafu(display("failed to serialize jvm security properties",))]
    JvmSecurityProperties { source: PropertiesWriterError },

    #[snafu(display("failed to serialize metrics properties",))]
    MetricsProperties { source: PropertiesWriterError },
}

#[derive(Clone, Debug, Display, EnumIter)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum SparkConnectRole {
    Server,
    Executor,
}

impl From<SparkConnectRole> for RoleName {
    fn from(value: SparkConnectRole) -> Self {
        (&value).into()
    }
}

impl From<&SparkConnectRole> for RoleName {
    fn from(value: &SparkConnectRole) -> Self {
        match value {
            SparkConnectRole::Server => RoleName::from_str(CONNECT_SERVER_ROLE_NAME)
                .expect("CONNECT_SERVER_ROLE_NAME is a valid role name"),
            SparkConnectRole::Executor => RoleName::from_str(CONNECT_EXECUTOR_ROLE_NAME)
                .expect("CONNECT_EXECUTOR_ROLE_NAME is a valid role name"),
        }
    }
}

pub(crate) fn object_name(stacklet_name: &str, role: SparkConnectRole) -> String {
    match role {
        SparkConnectRole::Server => format!("{}-{}", stacklet_name, CONNECT_SERVER_ROLE_NAME),
        SparkConnectRole::Executor => format!("{}-{}", stacklet_name, CONNECT_EXECUTOR_ROLE_NAME),
    }
}

// Returns the operator-generated jvm arguments with the user-provided overrides applied on top.
pub(crate) fn jvm_args(jvm_args: &[String], user_java_config: Option<&JavaCommonConfig>) -> String {
    match user_java_config {
        Some(user) => user
            .jvm_argument_overrides
            .apply_to(jvm_args.iter().cloned())
            .join(" "),
        None => jvm_args.join(" "),
    }
}

// Merges server and executor properties and renders the contents
// of the Spark properties file.
pub(crate) fn spark_properties(
    props: &[BTreeMap<String, Option<String>>],
) -> Result<String, Error> {
    let mut result = BTreeMap::new();
    for p in props {
        result.extend(p);
    }
    to_java_properties_string(
        result
            .into_iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v))),
    )
    .context(SparkPropertiesSnafu)
}

pub(crate) fn security_properties(
    config_overrides: BTreeMap<String, String>,
) -> Result<String, Error> {
    let mut result: BTreeMap<String, String> = [
        (
            JVM_SECURITY_PROPERTY_DNS_CACHE_TTL.to_string(),
            DEFAULT_JVM_SECURITY_DNS_CACHE_TTL.to_string(),
        ),
        (
            JVM_SECURITY_PROPERTY_DNS_CACHE_NEGATIVE_TTL.to_string(),
            DEFAULT_JVM_SECURITY_DNS_CACHE_NEGATIVE_TTL.to_string(),
        ),
    ]
    .into();

    result.extend(config_overrides);

    to_java_properties_string(result.iter()).context(JvmSecurityPropertiesSnafu)
}

pub(crate) fn metrics_properties(
    config_overrides: BTreeMap<String, String>,
) -> Result<String, Error> {
    let mut result: BTreeMap<String, String> = [
        (
            "*.sink.prometheusServlet.class".to_string(),
            "org.apache.spark.metrics.sink.PrometheusServlet".to_string(),
        ),
        (
            "*.sink.prometheusServlet.path".to_string(),
            "/metrics/prometheus".to_string(),
        ),
    ]
    .into();

    result.extend(config_overrides);

    to_java_properties_string(result.iter()).context(MetricsPropertiesSnafu)
}

#[cfg(test)]
mod tests {
    use stackable_operator::v2::types::operator::RoleName;
    use strum::IntoEnumIterator;

    use super::SparkConnectRole;

    /// Locks the invariant behind the `expect` in the `From<SparkConnectRole> for RoleName`
    /// impls: every variant (present and future) must map to a valid `RoleName`.
    #[test]
    fn every_spark_connect_role_maps_to_a_valid_role_name() {
        for role in SparkConnectRole::iter() {
            let _: RoleName = (&role).into();
            let _: RoleName = role.into();
        }
    }
}
