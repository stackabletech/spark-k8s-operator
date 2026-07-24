use stackable_operator::crd::{openlineage, s3};

use crate::crd::{
    Error,
    constants::{
        JVM_SECURITY_PROPERTIES_FILE, OPENLINEAGE_ADD_OPENS, STACKABLE_TLS_STORE_PASSWORD,
        STACKABLE_TRUST_STORE, VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    logdir::ResolvedLogDir,
    spark_major_version,
    tlscerts::tls_secret_names,
    v1alpha1::SparkApplication,
};

/// JVM arguments that go into
/// 1. `spark.driver.extraJavaOptions`
/// 2. `spark.executor.extraJavaOptions`
///
/// Returns `(driver, executor)`: the operator-generated base arguments with the role's
/// `jvmArgumentOverrides` applied on top.
///
/// `product_version` is the resolved Spark product version (e.g. `4.1.2`); it gates the
/// version-specific OpenLineage `--add-opens` flag.
pub fn construct_extra_java_options(
    spark_application: &SparkApplication,
    s3_conn: &Option<s3::v1alpha1::ConnectionSpec>,
    log_dir: &Option<ResolvedLogDir>,
    product_version: &str,
    open_lineage_conn: Option<&openlineage::ResolvedOpenLineageConnection>,
) -> Result<(String, String), Error> {
    // Note (@sbernauer): As of 2025-03-04, we did not set any heap related JVM arguments, so I
    // kept the implementation as is. We can always re-visit this as needed.

    let mut jvm_args = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_LOG_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];

    if tls_secret_names(s3_conn, log_dir, open_lineage_conn).is_some() {
        jvm_args.extend([
            format!("-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"),
            format!("-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"),
            "-Djavax.net.ssl.trustStoreType=pkcs12".to_string(),
        ]);
    }

    // OpenLineage on Spark 4.x needs `java.base/java.security` opened to the unnamed module,
    // otherwise the driver throws a non-fatal `InaccessibleObjectException` and silently degrades
    // extension-interface lineage (MVP §7). Added to both driver and executor.
    //
    // This is scoped to Spark 4.x: the flag is unnecessary — and on the JDK 17 Spark 3.5.x images the
    // operator also ships, potentially a startup error — so it must not be emitted there.
    if spark_application.lineage_enabled() && spark_major_version(product_version)? >= 4 {
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

    Ok((driver.join(" "), executor.join(" ")))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn spark_app_from_yaml(input: &str) -> SparkApplication {
        let deserializer = serde_yaml::Deserializer::from_str(input);
        serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap()
    }

    #[test]
    fn test_construct_jvm_arguments_defaults() {
        let spark_app = spark_app_from_yaml(
            r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 4.1.2
        "#,
        );
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, "4.1.2", None).unwrap();

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
        let spark_app = spark_app_from_yaml(
            r#"
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
        "#,
        );
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, "4.1.2", None).unwrap();

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/log_config/security.properties -Dhttps.proxyHost=from-driver"
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

    /// `--add-opens` is emitted only on Spark 4.x with OpenLineage enabled — never on the Scala
    /// 2.12 / JDK 17 Spark 3.5.x images, and never when OpenLineage is absent.
    #[rstest]
    #[case::enabled_spark_3(OPENLINEAGE_ENABLED, "3.5.8", false)]
    #[case::enabled_spark_4(OPENLINEAGE_ENABLED, "4.1.2", true)]
    #[case::absent_spark_4(OPENLINEAGE_ABSENT, "4.1.2", false)]
    fn test_openlineage_add_opens_is_version_gated(
        #[case] yaml_template: &str,
        #[case] product_version: &str,
        #[case] expect_add_opens: bool,
    ) {
        let spark_app = spark_app_from_yaml(&yaml_template.replace("PLACEHOLDER", product_version));
        let (driver_extra_java_options, executor_extra_java_options) =
            construct_extra_java_options(&spark_app, &None, &None, product_version, None).unwrap();

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
