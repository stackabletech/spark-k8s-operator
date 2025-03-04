use snafu::{ResultExt, Snafu};
use stackable_operator::{
    commons::s3::S3ConnectionSpec,
    role_utils::{self, JvmArgumentOverrides},
};

use crate::crd::{
    constants::{
        JVM_SECURITY_PROPERTIES_FILE, STACKABLE_TLS_STORE_PASSWORD, STACKABLE_TRUST_STORE,
        VOLUME_MOUNT_PATH_LOG_CONFIG,
    },
    logdir::ResolvedLogDir,
    tlscerts::tls_secret_names,
    v1alpha1::SparkApplication,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to merge jvm argument overrides"))]
    MergeJvmArgumentOverrides { source: role_utils::Error },
}

/// JVM arguments that go into
/// 1.`spark.driver.extraJavaOptions`
/// 2. `spark.executor.extraJavaOptions`
pub fn construct_extra_java_options(
    spark_application: &SparkApplication,
    s3_conn: &Option<S3ConnectionSpec>,
    log_dir: &Option<ResolvedLogDir>,
) -> Result<(String, String), Error> {
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

    let operator_generated = JvmArgumentOverrides::new_with_only_additions(jvm_args);
    let from_driver = match &spark_application.spec.driver {
        Some(driver) => &driver.product_specific_common_config.jvm_argument_overrides,
        None => &JvmArgumentOverrides::default(),
    };
    let from_executor = match &spark_application.spec.executor {
        Some(executor) => {
            &executor
                .config
                .product_specific_common_config
                .jvm_argument_overrides
        }
        None => &JvmArgumentOverrides::default(),
    };

    // Please note that the merge order is different than we normally do!
    // This is not trivial, as the merge operation is not purely additive (as it is with e.g. `PodTemplateSpec).
    let mut from_driver = from_driver.clone();
    let mut from_executor = from_executor.clone();
    from_driver
        .try_merge(&operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;
    from_executor
        .try_merge(&operator_generated)
        .context(MergeJvmArgumentOverridesSnafu)?;

    Ok((
        from_driver.effective_jvm_config_after_merging().join(" "),
        from_executor.effective_jvm_config_after_merging().join(" "),
    ))
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
            construct_extra_java_options(&spark_app, &None, &None).unwrap();

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
            construct_extra_java_options(&spark_app, &None, &None).unwrap();

        assert_eq!(
            driver_extra_java_options,
            "-Djava.security.properties=/stackable/log_config/security.properties -Dhttps.proxyHost=from-driver"
        );
        assert_eq!(
            executor_extra_java_options,
            "-Dhttps.proxyHost=from-executor"
        );
    }
}
