use crate::{
    crd::{
        constants::{
            JVM_SECURITY_PROPERTIES_FILE, LOG4J2_CONFIG_FILE, METRICS_PORT,
            STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE, VOLUME_MOUNT_PATH_CONFIG,
            VOLUME_MOUNT_PATH_LOG_CONFIG,
        },
        logdir::ResolvedLogDir,
    },
    history::controller::validate::HistoryRoleGroupConfig,
};

/// JVM arguments that go into `SPARK_HISTORY_OPTS`
pub fn construct_history_jvm_args(rg: &HistoryRoleGroupConfig, logdir: &ResolvedLogDir) -> String {
    // Note (@sbernauer): As of 2025-03-04, we did not set any heap related JVM arguments, so I
    // kept the implementation as is. We can always re-visit this as needed.

    let mut jvm_args = vec![
        format!("-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"),
        format!(
            "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
        ),
        format!(
            "-javaagent:/stackable/jmx/jmx_prometheus_javaagent.jar={METRICS_PORT}:/stackable/jmx/config.yaml"
        ),
    ];

    if logdir.tls_enabled() {
        jvm_args.extend([
            format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
            format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
            "-Djavax.net.ssl.trustStoreType=pkcs12".to_owned(),
        ]);
    }

    // Apply the already-merged (role + role group) JVM argument overrides on top of the
    // operator-generated base arguments.
    rg.product_specific_common_config
        .jvm_argument_overrides
        .apply_to(jvm_args)
        .join(" ")
}

#[cfg(test)]
mod tests {
    use stackable_operator::{
        cli::OperatorEnvironmentOptions, commons::tls_verification::TlsClientDetails, crd::s3,
    };

    use super::*;
    use crate::{
        crd::{history::v1alpha1::SparkHistoryServer, logdir::S3LogDir},
        history::controller::{
            dereference::DereferencedSparkHistoryServer,
            validate::{ValidatedSparkHistoryServer, validate},
        },
    };

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.5.8
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
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.5.8
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

    /// Validates the given `SparkHistoryServer` YAML and returns the `default` role group's merged
    /// config, mirroring the controller's validate path so the JVM args are built from a real
    /// [`HistoryRoleGroupConfig`].
    fn construct_jvm_config_for_test(history_server: &str) -> String {
        let deserializer = serde_yaml::Deserializer::from_str(history_server);
        let history_server: SparkHistoryServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();

        let resolved_log_dir = ResolvedLogDir::Custom("local:/tmp/foo".to_owned());

        let validated: ValidatedSparkHistoryServer = validate(
            &history_server,
            DereferencedSparkHistoryServer {
                log_dir: ResolvedLogDir::S3(S3LogDir {
                    bucket: s3::v1alpha1::ResolvedBucket {
                        bucket_name: "my-bucket".to_string(),
                        connection: s3::v1alpha1::ConnectionSpec {
                            host: "my-s3".to_string().try_into().unwrap(),
                            port: None,
                            access_style: Default::default(),
                            credentials: None,
                            tls: TlsClientDetails { tls: None },
                            region: Default::default(),
                        },
                    },
                    prefix: "prefix".to_string(),
                }),
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "default".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.stackable.tech/sdp".to_string(),
            },
        )
        .expect("validation should succeed");

        let rg = validated
            .role_groups
            .get(&"default".parse().expect("valid role group name"))
            .expect("default role group should exist");

        construct_history_jvm_args(&rg.config, &resolved_log_dir)
    }
}
