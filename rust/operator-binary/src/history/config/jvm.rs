use snafu::{ResultExt, Snafu};
use stackable_operator::role_utils::{
    self, GenericRoleConfig, JavaCommonConfig, JvmArgumentOverrides, Role,
};

use crate::crd::{
    constants::{
        JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, METRICS_PORT,
        STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, VOLUME_MOUNT_PATH_CONFIG,
        VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    history::HistoryConfigFragment,
    logdir::ResolvedLogDir,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// JVM arguments that go into `SPARK_HISTORY_OPTS`
pub fn construct_history_jvm_args(
    role: &Role<HistoryConfigFragment, GenericRoleConfig, JavaCommonConfig>,
    role_group: &str,
    logdir: &ResolvedLogDir,
) -> Result<String, Error> {
    // Note (@sbernauer): As of 2025-03-04, we did not set any heap related JVM arguments, so I
    // kept the implementation as is. We can always re-visit this as needed.

    let mut jvm_args = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
        format!("-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/config.yaml")
    ];

    if logdir.tls_enabled() {
        jvm_args.extend([
            format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
            format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
            "-Djavax.net.ssl.trustStoreType=pkcs12".to_owned(),
        ]);
    }

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let merged = role
        .get_merged_jvm_argument_overrides(role_group, &operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    Ok(merged.effective_jvm_config_after_merging().join("\n"))
}
