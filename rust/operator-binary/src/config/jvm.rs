use stackable_operator::crd::{openlineage, s3};

use crate::crd::{
    constants::{
        JVM_SECURITY_PROPERTIES_FILE, OPENLINEAGE_ADD_OPENS, STACKABLE_TLS_STORE_PASSWORD,
        STACKABLE_TRUST_STORE, VOLUME_MOUNT_PATH_CONFIG,
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
    open_lineage_conn: Option<&openlineage::ResolvedOpenLineageConnection>,
) -> (String, String) {
    // Note (@sbernauer): As of 2025-03-04, we did not set any heap related JVM arguments, so I
    // kept the implementation as is. We can always re-visit this as needed.

    let mut jvm_args = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if tls_secret_names(s3_conn, log_dir, open_lineage_conn).is_some() {
        jvm_args.extend([
            format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
            format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
            "-Djavax.net.ssl.trustStoreType=pkcs12".to_string(),
        ]);
    }

    // OpenLineage needs `java.base/java.security` opened to the unnamed module, otherwise the
    // driver throws a non-fatal `InaccessibleObjectException` and silently degrades
    // extension-interface lineage. Added to both driver and executor.
    if spark_application.lineage_enabled() {
        jvm_args.push(OPENLINEAGE_ADD_OPENS.to_string());
    }

    // The role's `jvmArgumentOverrides` are applied on top of the operator-generated arguments
    // above.
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
    use rstest::rstest;

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

        let spark_app: SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(input).unwrap();

        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, None);

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/spark/conf/security.properties"
        );
        assert_eq!(
            executor_extra_java_options,
            "-Djava.security.properties=/stackable/spark/conf/security.properties"
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
                productVersion: 4.1.2
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

        let spark_app: SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(input).unwrap();

        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, None);

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/spark/conf/security.properties -Dhttps.proxyHost=from-driver"
        );
        assert_eq!(
            executor_extra_java_options,
            "-Dhttps.proxyHost=from-executor"
        );
    }

    const OPENLINEAGE_ENABLED: &str = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkApplication
        metadata:
          name: spark-example
        spec:
          mode: cluster
          mainApplicationFile: test.py
          sparkImage:
            productVersion: PLACEHOLDER
          lineage:
            connection:
              inline:
                http:
                  host: marquez
                  port: 5000
    "#;

    const OPENLINEAGE_ABSENT: &str = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkApplication
        metadata:
          name: spark-example
        spec:
          mode: cluster
          mainApplicationFile: test.py
          sparkImage:
            productVersion: PLACEHOLDER
    "#;

    /// `--add-opens` is emitted on every Spark version when OpenLineage is enabled, and never when
    /// OpenLineage is absent.
    #[rstest]
    #[case::enabled_spark_3(OPENLINEAGE_ENABLED, "3.5.8", true)]
    #[case::enabled_spark_4(OPENLINEAGE_ENABLED, "4.1.2", true)]
    #[case::absent_spark_4(OPENLINEAGE_ABSENT, "4.1.2", false)]
    fn test_openlineage_add_opens(
        #[case] yaml_template: &str,
        #[case] product_version: &str,
        #[case] expect_add_opens: bool,
    ) {
        let spark_app = stackable_operator::utils::yaml_from_str_singleton_map(
            &yaml_template.replace("PLACEHOLDER", product_version),
        )
        .unwrap();
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, None);

        assert_eq!(
            driver_extra_java_options.contains(OPENLINEAGE_ADD_OPENS),
            expect_add_opens,
            "driver --add-opens presence mismatch for Spark {product_version}"
        );
        assert_eq!(
            executor_extra_java_options.contains(OPENLINEAGE_ADD_OPENS),
            expect_add_opens,
            "executor --add-opens presence mismatch for Spark {product_version}"
        );
    }
}
