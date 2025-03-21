use snafu::{ResultExt, Snafu};
use stackable_operator::role_utils::{
    self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role,
};

use crate::connect::crd::ConnectConfigFragment;
use crate::crd::constants::{
    JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, METRICS_PORT, VOLUME_MOUNT_PATH_CONFIG,
    VOLUME_MOUNT_PATH_LOG_CONFIG,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// JVM arguments that go into `SPARK_HISTORY_OPTS`
pub fn construct_jvm_args(
    role: &Role<ConnectConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
) -> Result<String, Error> {
    let jvm_args = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/config.yaml")
    ];

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged.effective_jvm_config_after_merging().join(" "))
}
