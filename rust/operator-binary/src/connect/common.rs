use std::{collections::BTreeMap, ops::Deref, str::FromStr};

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    constant,
    v2::{
        config_file_writer::{PropertiesWriterError, to_java_properties_string},
        role_utils::JavaCommonConfig,
        types::operator::RoleName,
    },
};

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

constant!(SERVER_ROLE_NAME: RoleName = CONNECT_SERVER_ROLE_NAME);
constant!(EXECUTOR_ROLE_NAME: RoleName = CONNECT_EXECUTOR_ROLE_NAME);

#[derive(Clone, Debug)]
pub(crate) enum SparkConnectRole {
    Server,
    Executor,
}

impl Deref for SparkConnectRole {
    type Target = RoleName;

    fn deref(&self) -> &Self::Target {
        match self {
            SparkConnectRole::Server => &SERVER_ROLE_NAME,
            SparkConnectRole::Executor => &EXECUTOR_ROLE_NAME,
        }
    }
}

pub(crate) fn object_name(stacklet_name: &str, role: SparkConnectRole) -> String {
    match role {
        SparkConnectRole::Server => format!("{}-{}", stacklet_name, CONNECT_SERVER_ROLE_NAME),
        SparkConnectRole::Executor => format!("{}-{}", stacklet_name, CONNECT_EXECUTOR_ROLE_NAME),
    }
}

// Returns the extra class path shared by the Connect server and its executors.
//
// The product image keeps the Spark Connect jars out of `/stackable/spark/jars` to
// avoid class path conflicts with regular Spark applications, so both roles have to add the Connect
// jar explicitly.
//
// `spark.driver.extraClassPath` and `spark.executor.extraClassPath` must be set to this same value:
// the Connect server ships closures defined in the Connect jar with every task that returns rows to
// a client.
pub(crate) fn extra_class_path(product_version: &str) -> String {
    format!(
        "/stackable/spark/extra-jars/*:/stackable/spark/connect/spark-connect-{product_version}.jar"
    )
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
    use super::*;

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *EXECUTOR_ROLE_NAME;
        let _ = *SERVER_ROLE_NAME;
    }
}
