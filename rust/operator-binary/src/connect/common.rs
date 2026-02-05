use std::collections::{BTreeMap, HashMap};

use product_config::writer::to_java_properties_string;
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    kvp::ObjectLabels,
    role_utils::{JavaCommonConfig, JvmArgumentOverrides},
};
use strum::Display;

use super::crd::CONNECT_EXECUTOR_ROLE_NAME;
use crate::{
    connect::crd::{
        CONNECT_APP_NAME, CONNECT_CONTROLLER_NAME, CONNECT_SERVER_ROLE_NAME,
        DEFAULT_SPARK_CONNECT_GROUP_NAME,
    },
    crd::constants::OPERATOR_NAME,
};

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides {
        source: stackable_operator::role_utils::Error,
    },

    #[snafu(display("failed to serialize spark properties"))]
    SparkProperties {
        source: product_config::writer::PropertiesWriterError,
    },

    #[snafu(display("failed to serialize jvm security properties",))]
    JvmSecurityProperties {
        source: product_config::writer::PropertiesWriterError,
    },

    #[snafu(display("failed to serialize metrics properties",))]
    MetricsProperties {
        source: product_config::writer::PropertiesWriterError,
    },
}

pub(crate) fn labels<'a, T>(
    scs: &'a T,
    app_version_label: &'a str,
    role: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: scs,
        app_name: CONNECT_APP_NAME,
        app_version: app_version_label,
        operator_name: OPERATOR_NAME,
        controller_name: CONNECT_CONTROLLER_NAME,
        role,
        role_group: DEFAULT_SPARK_CONNECT_GROUP_NAME,
    }
}

// The dead code annotation is to shut up complains about missing Executor instantiations
// These will come in the future.
#[allow(dead_code)]
#[derive(Clone, Debug, Display)]
#[strum(serialize_all = "lowercase")]
pub(crate) enum SparkConnectRole {
    Server,
    Executor,
}

pub(crate) fn object_name(stacklet_name: &str, role: SparkConnectRole) -> String {
    match role {
        SparkConnectRole::Server => format!("{}-{}", stacklet_name, CONNECT_SERVER_ROLE_NAME),
        SparkConnectRole::Executor => format!("{}-{}", stacklet_name, CONNECT_EXECUTOR_ROLE_NAME),
    }
}

// Returns the jvm arguments a user has provided merged with the operator props.
pub(crate) fn jvm_args(
    jvm_args: &[String],
    user_java_config: Option<&JavaCommonConfig>,
) -> Result<String, Error> {
    if let Some(user_jvm_props) = user_java_config {
        let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args.to_vec());
        let mut user_jvm_props_copy = user_jvm_props.jvm_argument_overrides.clone();
        user_jvm_props_copy
            .try_merge(&operator_generated)
            .context(MergeJvmArgumentOverridesSnafu)?;
        Ok(user_jvm_props_copy
            .effective_jvm_config_after_merging()
            .join(" "))
    } else {
        Ok(jvm_args.join(" "))
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
    to_java_properties_string(result.into_iter()).context(SparkPropertiesSnafu)
}

pub(crate) fn security_properties(
    config_overrides: Option<&HashMap<String, String>>,
) -> Result<String, Error> {
    let mut result: BTreeMap<String, Option<String>> = [
        (
            "networkaddress.cache.ttl".to_string(),
            Some("30".to_string()),
        ),
        (
            "networkaddress.cache.negative.ttl".to_string(),
            Some("0".to_string()),
        ),
    ]
    .into();

    if let Some(user_config) = config_overrides {
        result.extend(
            user_config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );
    }

    to_java_properties_string(result.iter()).context(JvmSecurityPropertiesSnafu)
}

pub(crate) fn metrics_properties(
    config_overrides: Option<&HashMap<String, String>>,
) -> Result<String, Error> {
    let mut result: BTreeMap<String, Option<String>> = [
        (
            "*.sink.prometheusServlet.class".to_string(),
            Some("org.apache.spark.metrics.sink.PrometheusServlet".to_string()),
        ),
        (
            "*.sink.prometheusServlet.path".to_string(),
            Some("/metrics/prometheus".to_string()),
        ),
    ]
    .into();

    if let Some(user_config) = config_overrides {
        result.extend(
            user_config
                .iter()
                .map(|(k, v)| (k.clone(), Some(v.clone()))),
        );
    }

    to_java_properties_string(result.iter()).context(MetricsPropertiesSnafu)
}
