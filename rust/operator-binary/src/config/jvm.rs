use stackable_operator::crd::s3;

use crate::crd::{
    constants::{
        JVM_SECURITY_PROPERTIES_FILE, OPENLINEAGE_ADD_OPENS, STACKABLE_TLS_STORE_PASSWORD,
        STACKABLE_TRUST_STORE, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    logdir::ResolvedLogDir,
    tlscerts::tls_secret_names,
    v1alpha1::SparkApplication,
};

/// JVM arguments that go into
/// 1. `spark.driver.extraJavaOptions`
/// 2. `spark.executor.extraJavaOptions`
///
/// Returns `(driver, executor)`: the operator-generated base arguments with the role's
/// `jvmArgumentOverrides` applied on top.
pub fn construct_extra_java_options(
    spark_application: &SparkApplication,
    s3_conn: &Option<s3::v1alpha1::ConnectionSpec>,
    log_dir: &Option<ResolvedLogDir>,
) -> (String, String) {
    // Note (@sbernauer): As of 2025-03-04, we did not set any heap related JVM arguments, so I
    // kept the implementation as is. We can always re-visit this as needed.

    let mut jvm_args = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_LOG_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if tls_secret_names(s3_conn, log_dir).is_some() {
        jvm_args.extend([
            format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
            format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
            "-Djavax.net.ssl.trustStoreType=pkcs12".to_string(),
        ]);
    }

    // OpenLineage on Spark 4.x needs `java.base/java.security` opened to the unnamed module,
    // otherwise the driver throws a non-fatal `InaccessibleObjectException` and silently degrades
    // extension-interface lineage (MVP §7). Added to both driver and executor.
    if spark_application
        .spec
        .open_lineage
        .as_ref()
        .is_some_and(|open_lineage| open_lineage.enabled)
    {
        jvm_args.push(OPENLINEAGE_ADD_OPENS.to_string());
    }

    // The role's `jvmArgumentOverrides` are applied on top of the operator-generated arguments
    // above. Note this is not purely additive: a role may also remove or replace operator-set
    // arguments (e.g. a `removeRegex` dropping the `-Djava.security.properties` default) — see the
    // unit tests below.
    let driver = match &spark_application.spec.driver {
        Some(driver) => driver
            .product_specific_common_config
            .jvm_argument_overrides
            .apply_to(jvm_args.clone()),
        None => jvm_args.clone(),
    };
    let executor = match &spark_application.spec.executor {
        Some(executor) => executor
            .config
            .product_specific_common_config
            .jvm_argument_overrides
            .apply_to(jvm_args.clone()),
        None => jvm_args.clone(),
    };

    (driver.join(" "), executor.join(" "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let input = r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
        "#;

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let spark_app: SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None);

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/log_config/security.properties"
        );
        assert_eq!(
            executor_extra_java_options,
            "-Djava.security.properties=/stackable/log_config/security.properties"
        );
    }

    #[test]
    fn test_construct_jvm_argument_overrides() {
        let input = r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              driver:
                jvmArgumentOverrides:
                  add:
                    - -Dhttps.proxyHost=from-driver
              executor:
                jvmArgumentOverrides:
                  add:
                    - -Dhttps.proxyHost=from-executor
                  removeRegex:
                    - -Djava.security.properties=.*
        "#;

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let spark_app: SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None);

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/log_config/security.properties -Dhttps.proxyHost=from-driver"
        );
        assert_eq!(
            executor_extra_java_options,
            "-Dhttps.proxyHost=from-executor"
        );
    }

    #[test]
    fn test_construct_jvm_arguments_openlineage_add_opens() {
        let input = r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              openLineage:
                enabled: true
        "#;

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let spark_app: SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None);

        assert!(driver_extra_java_options.contains(OPENLINEAGE_ADD_OPENS));
        assert!(executor_extra_java_options.contains(OPENLINEAGE_ADD_OPENS));
    }
}
