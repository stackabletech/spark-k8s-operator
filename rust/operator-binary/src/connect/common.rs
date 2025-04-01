use snafu::{ResultExt, Snafu};
use stackable_operator::{
    kvp::ObjectLabels,
    role_utils::{JavaCommonConfig, JvmArgumentOverrides},
};
use strum::Display;

use crate::{
    connect::crd::{CONNECT_CONTROLLER_NAME, CONNECT_SERVER_ROLE_NAME},
    crd::constants::{
        APP_NAME, JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, OPERATOR_NAME,
        VOLUME_MOUNT_PATH_CONFIG, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
};

const DUMMY_SPARK_CONNECT_GROUP_NAME: &str = "default";

#[derive(Snafu, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides {
        source: stackable_operator::role_utils::Error,
    },
}

pub fn labels<'a, T>(scs: &'a T, app_version_label: &'a str, role: &'a str) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner: scs,
        app_name: APP_NAME,
        app_version: app_version_label,
        operator_name: OPERATOR_NAME,
        controller_name: CONNECT_CONTROLLER_NAME,
        role,
        role_group: DUMMY_SPARK_CONNECT_GROUP_NAME,
    }
}

// The dead code annotation is to shut up complains about missing Executor instantiations
// These will come in the future.
#[allow(dead_code)]
#[derive(Clone, Debug, Display)]
#[strum(serialize_all = "lowercase")]
pub enum SparkConnectRole {
    Server,
    Executor,
}

pub fn object_name(stacklet_name: &str, role: SparkConnectRole) -> String {
    match role {
        SparkConnectRole::Server => format!("{}-{}", stacklet_name, CONNECT_SERVER_ROLE_NAME),
        SparkConnectRole::Executor => todo!(),
    }
}

// Returns the jvm arguments a user has provided merged with the operator props.
pub fn jvm_args(user_java_config: Option<&JavaCommonConfig>) -> Result<String, Error> {
    let jvm_args = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
    ];

    if let Some(user_jvm_props) = user_java_config {
        let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args.clone());
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
