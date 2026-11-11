use std::str::FromStr;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{meta::ObjectMetaBuilder, pod::volume::VolumeBuilder},
    constant,
    k8s_openapi::{
        DeepMerge,
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{Affinity, PodSpec, PodTemplateSpec, ServiceAccount},
        },
    },
    v2::builder::pod::container::{EnvVarName, EnvVarSet, new_container_builder},
};

use crate::{
    crd::{
        constants::*,
        roles::{SparkApplicationRole, SparkContainer, SubmitConfig},
        tlscerts,
    },
    spark_k8s_controller::{
        build::{
            SPARK_JOB_COMPONENT_NAME, SPARK_JOB_TEMPLATE_COMPONENT_NAME, object_meta,
            pod::security_context, recommended_labels_for_component_resources,
        },
        validate,
    },
};

// JVM settings of the spark-submit job.
constant!(SPARK_SUBMIT_OPTS: EnvVarName = "SPARK_SUBMIT_OPTS");
// The Spark configuration directory of the spark-submit job.
constant!(SPARK_CONF_DIR: EnvVarName = "SPARK_CONF_DIR");

const SPARK_DRIVER_POD_NAME: &str = "spark.kubernetes.driver.pod.name";

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to add needed volumeMount"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("missing secret lifetime"))]
    MissingSecretLifetime,

    #[snafu(display("failed to create Volumes for SparkApplication"))]
    CreateVolumes { source: crate::crd::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

pub(crate) fn spark_job(
    validated: &validate::ValidatedSparkApplication,
    serviceaccount: &ServiceAccount,
    env: &EnvVarSet,
    job_commands: &[String],
    job_config: &SubmitConfig,
) -> Result<Job> {
    let spark_application = &validated.spark_application;
    let spark_image = &validated.resolved_product_image;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let open_lineage_conn = validated.cluster_config.open_lineage_connection.as_ref();
    let mut cb = new_container_builder(SparkContainer::SparkSubmit.name());

    // The SPARK_SUBMIT_OPTS env var is used to configure the JVM settings of the spark-submit job.
    // Here we need to point the JVM to the security properties and if S3 is used for data or Spark
    // History, we also need to tell the JVM where the trust store is located. The same properties
    // are set for driver and executor via `spark.{driver,executor}.extraJavaOptions`.
    let mut spark_submit_opts_env = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];
    if tlscerts::tls_secret_names(s3conn, logdir, open_lineage_conn).is_some() {
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"
        ));
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"
        ));
    }

    // The env overrides are merged in last so that they override any operator-set environment
    // variable.
    let merged_env = spark_application.merged_env(
        SparkApplicationRole::Submit,
        env.clone()
            .with_value(&SPARK_SUBMIT_OPTS, spark_submit_opts_env.join(" "))
            // TODO: move this to the image
            .with_value(&SPARK_CONF_DIR, VOLUME_MOUNT_PATH_CONFIG),
    );

    cb.image_from_product_image(spark_image)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-euo".to_string(),
            "pipefail".to_string(),
            "-c".to_string(),
        ])
        .args(vec![job_commands.join("\n")])
        .resources(job_config.resources.clone().into())
        .add_volume_mount(VOLUME_MOUNT_NAME_CONFIG.as_ref(), VOLUME_MOUNT_PATH_CONFIG)
        .expect("The mount paths are statically defined and there should be no duplicates.")
        // These mounts include the user-supplied `volumeMounts`, so this add stays fallible.
        .add_volume_mounts(spark_application.spark_job_volume_mounts(
            s3conn,
            logdir,
            open_lineage_conn,
        ))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env);

    let mut volumes = vec![
        VolumeBuilder::new(VOLUME_MOUNT_NAME_CONFIG.as_ref())
            .with_config_map(spark_application.submit_job_config_map_name())
            .build(),
        VolumeBuilder::new(VOLUME_MOUNT_NAME_DRIVER_POD_TEMPLATES.as_ref())
            .with_config_map(
                spark_application.pod_template_config_map_name(SparkApplicationRole::Driver),
            )
            .build(),
        VolumeBuilder::new(VOLUME_MOUNT_NAME_EXECUTOR_POD_TEMPLATES.as_ref())
            .with_config_map(
                spark_application.pod_template_config_map_name(SparkApplicationRole::Executor),
            )
            .build(),
    ];
    let requested_secret_lifetime = job_config
        .requested_secret_lifetime
        .context(MissingSecretLifetimeSnafu)?;
    volumes.extend(
        spark_application
            .volumes(
                s3conn,
                logdir,
                None,
                &requested_secret_lifetime,
                open_lineage_conn,
            )
            .context(CreateVolumesSnafu)?,
    );

    let containers = vec![cb.build()];

    let mut pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name("spark-submit")
                .with_labels(recommended_labels_for_component_resources(
                    validated,
                    &SPARK_JOB_TEMPLATE_COMPONENT_NAME,
                ))
                .build(),
        ),
        spec: Some(PodSpec {
            containers,
            restart_policy: Some("Never".to_string()),
            service_account_name: serviceaccount.metadata.name.clone(),
            volumes: Some(volumes),
            affinity: Some(Affinity {
                node_affinity: job_config.affinity.node_affinity.clone(),
                pod_affinity: job_config.affinity.pod_affinity.clone(),
                pod_anti_affinity: job_config.affinity.pod_anti_affinity.clone(),
            }),
            image_pull_secrets: spark_image.pull_secrets.clone(),
            security_context: Some(security_context()),
            ..PodSpec::default()
        }),
    };

    if let Some(submit_pod_overrides) =
        spark_application.pod_overrides(SparkApplicationRole::Submit)
    {
        pod.merge_from(submit_pod_overrides);
    }

    let cleanup = spark_application.cleanup();
    let mut metadata = object_meta(
        validated,
        validated.name.to_string(),
        &SPARK_JOB_COMPONENT_NAME,
    )
    .build();
    let annotations = metadata.annotations.get_or_insert_default();
    annotations.insert(
        CLEANUP_AFTER_SUCCEEDED_ANNOTATION.to_owned(),
        cleanup.after_succeeded().to_string(),
    );
    annotations.insert(
        CLEANUP_AFTER_FAILED_ANNOTATION.to_owned(),
        cleanup.after_failed().to_string(),
    );
    if let Some(driver_pod_name) = spark_application.spec.spark_conf.get(SPARK_DRIVER_POD_NAME) {
        annotations.insert(
            FIXED_DRIVER_POD_NAME_ANNOTATION.to_owned(),
            driver_pod_name.clone(),
        );
    }

    let job = Job {
        metadata,
        spec: Some(JobSpec {
            template: pod,
            ttl_seconds_after_finished: cleanup.initial_job_ttl_seconds(),
            backoff_limit: Some(spark_application.retry_on_failure_count()),
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

#[cfg(test)]
mod tests {
    use indoc::{formatdoc, indoc};
    use rstest::rstest;
    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        k8s_openapi::{api::core::v1::EnvVar, apimachinery::pkg::apis::meta::v1::ObjectMeta},
    };

    use super::*;
    use crate::{
        crd::v1alpha1,
        spark_k8s_controller::{dereference::DereferencedSparkApplication, validate::validate},
    };

    #[test]
    fn test_constants() {
        // Test that dereferencing the constants does not panic.
        let _ = *SPARK_CONF_DIR;
        let _ = *SPARK_SUBMIT_OPTS;
    }

    fn spark_job_from_yaml(yaml: &str) -> Job {
        let spark_application: v1alpha1::SparkApplication =
            stackable_operator::utils::yaml_from_str_singleton_map(yaml)
                .expect("invalid test SparkApplication YAML");

        let validated = validate(
            DereferencedSparkApplication {
                open_lineage_connection: None,
                spark_application,
                resolved_template_refs: Vec::new(),
                s3_connection: None,
                log_dir: None,
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "stackable-operators".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.example.org/sdp".to_string(),
            },
        )
        .expect("the fixture validates");

        let submit_config = validated
            .spark_application
            .submit_config()
            .expect("the submit config resolves");
        let env = validated
            .spark_application
            .env(&None, &None, None)
            .expect("the base environment can be built");
        let service_account = ServiceAccount {
            metadata: ObjectMeta {
                name: Some("spark-example".to_string()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        };

        spark_job(
            &validated,
            &service_account,
            &env,
            &["echo test".to_string()],
            &submit_config,
        )
        .expect("the spark-submit Job can be built")
    }

    /// `envOverrides` must be applied after all operator-set environment variables, so a user
    /// override replaces the operator-set value instead of duplicating it or being ignored.
    #[test]
    fn env_overrides_override_operator_set_env_vars() {
        let yaml = indoc! {r#"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              job:
                envOverrides:
                  SPARK_CONF_DIR: /custom/conf
        "#};
        let job = spark_job_from_yaml(yaml);

        let env: Vec<EnvVar> = job
            .spec
            .expect("the Job has a spec")
            .template
            .spec
            .expect("the Job has a pod spec")
            .containers
            .iter()
            .find(|container| container.name == "spark-submit")
            .expect("the spark-submit container exists")
            .env
            .clone()
            .expect("the spark-submit container has env vars");

        let matching: Vec<&EnvVar> = env
            .iter()
            .filter(|env_var| env_var.name == "SPARK_CONF_DIR")
            .collect();

        // The override must replace the operator-set value, not duplicate it.
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].value.as_deref(), Some("/custom/conf"));
    }

    fn spark_job_with(spark_conf: &str, cleanup: &str) -> Job {
        spark_job_from_yaml(&formatdoc! {"
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: spark-example
              namespace: default
              uid: 12345678-1234-1234-1234-123456789012
            spec:
              mode: cluster
              mainApplicationFile: test.py
              sparkImage:
                productVersion: 1.2.3
              sparkConf: {spark_conf}
              cleanup: {cleanup}
        "})
    }

    fn annotation<'a>(job: &'a Job, key: &str) -> Option<&'a str> {
        job.metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(key))
            .map(String::as_str)
    }

    #[rstest]
    #[case::default("null", Some(600), ("10m", "10m"))]
    #[case::immediately("{afterSucceeded: 0s, afterFailed: 0s}", Some(0), ("0s", "0s"))]
    #[case::failed_longer("{afterSucceeded: 2s, afterFailed: 1h}", Some(3600), ("2s", "1h"))]
    #[case::succeeded_longer("{afterSucceeded: 1h, afterFailed: 0s}", Some(3600), ("1h", "0s"))]
    #[case::only_failed("{afterFailed: never}", None, ("10m", "never"))]
    #[case::never("{afterSucceeded: never, afterFailed: never}", None, ("never", "never"))]
    fn job_starts_with_the_longer_ttl_and_carries_both(
        #[case] cleanup: &str,
        #[case] expected_ttl: Option<i32>,
        #[case] expected_annotations: (&str, &str),
    ) {
        let job = spark_job_with("{}", cleanup);

        assert_eq!(
            job.spec
                .as_ref()
                .expect("the job has a spec")
                .ttl_seconds_after_finished,
            expected_ttl
        );
        assert_eq!(
            (
                annotation(&job, CLEANUP_AFTER_SUCCEEDED_ANNOTATION),
                annotation(&job, CLEANUP_AFTER_FAILED_ANNOTATION)
            ),
            (Some(expected_annotations.0), Some(expected_annotations.1))
        );
    }

    #[rstest]
    #[case::generated_name("{}", None)]
    #[case::fixed_name("{spark.kubernetes.driver.pod.name: fixed}", Some("fixed"))]
    fn fixed_driver_pod_name_is_annotated(
        #[case] spark_conf: &str,
        #[case] expected: Option<&str>,
    ) {
        let job = spark_job_with(spark_conf, "null");

        assert_eq!(annotation(&job, FIXED_DRIVER_POD_NAME_ANNOTATION), expected);
    }
}
