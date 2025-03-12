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
    Ok(merged.effective_jvm_config_after_merging().join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::history::v1alpha1::SparkHistoryServer;

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
        spec:
          image:
            productVersion: 3.5.5
          logFileDirectory:
            s3:
              prefix: eventlogs/
              bucket:
                reference: spark-history-s3-bucket
          nodes:
            roleGroups:
              default:
                replicas: 1
                config:
                  cleaner: true
        "#;

        let jvm_config = construct_jvm_config_for_test(input);

        assert_eq!(
            jvm_config,
            "-Dlog4j.configurationFile=/stackable/log_config/log4j2.properties \
            -Djava.security.properties=/stackable/spark/conf/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=18081:/stackable/jmx/config.yaml"
        );
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
        spec:
          image:
            productVersion: 3.5.5
          logFileDirectory:
            s3:
              prefix: eventlogs/
              bucket:
                reference: spark-history-s3-bucket
          nodes:
            jvmArgumentOverrides:
              add:
                - -Dhttps.proxyHost=proxy.my.corp
                - -Dhttps.proxyPort=8080
                - -Djava.net.preferIPv4Stack=true
            roleGroups:
              default:
                replicas: 1
                jvmArgumentOverrides:
                  removeRegex:
                    - -Dhttps.proxyPort=.*
                  add:
                    - -Dhttps.proxyPort=1234
                config:
                  cleaner: true
        "#;

        let jvm_config = construct_jvm_config_for_test(input);

        assert_eq!(
            jvm_config,
            "-Dlog4j.configurationFile=/stackable/log_config/log4j2.properties \
            -Djava.security.properties=/stackable/spark/conf/security.properties \
            -javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar=18081:/stackable/jmx/config.yaml \
            -Dhttps.proxyHost=proxy.my.corp \
            -Djava.net.preferIPv4Stack=true \
            -Dhttps.proxyPort=1234"
        );
    }

    fn construct_jvm_config_for_test(history_server: &str) -> String {
        let deserializer = serde_yaml::Deserializer::from_str(history_server);
        let history_server: SparkHistoryServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let role = history_server.role();
        let resolved_log_dir = ResolvedLogDir::Custom("local:/tmp/foo".to_owned());

        construct_history_jvm_args(role, "default", &resolved_log_dir).unwrap()
    }
}
