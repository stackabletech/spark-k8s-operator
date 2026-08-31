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
    let mut cb = new_container_builder(&SparkContainer::SparkSubmit.to_container_name());

    // The SPARK_SUBMIT_OPTS env var is used to configure the JVM settings of the spark-submit job.
    // Here we need to point the JVM to the security properties and if S3 is used for data or Spark
    // History, we also need to tell the JVM where the trust store is located. The same properties
    // are set for driver and executor via `spark.{driver,executor}.extraJavaOptions`.
    let mut spark_submit_opts_env = vec![format!(
        "-Djava.security.properties={VOLUME_MOUNT_PATH_CONFIG}/{JVM_SECURITY_PROPERTIES_FILE}"
    )];
    if tlscerts::tls_secret_names(s3conn, logdir).is_some() {
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
        .context(AddVolumeMountSnafu)?
        .add_volume_mounts(spark_application.spark_job_volume_mounts(s3conn, logdir))
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
            .volumes(s3conn, logdir, None, &requested_secret_lifetime)
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

    let job = Job {
        metadata: object_meta(
            validated,
            validated.name.to_string(),
            &SPARK_JOB_COMPONENT_NAME,
        )
        .build(),
        spec: Some(JobSpec {
            template: pod,
            ttl_seconds_after_finished: Some(600),
            backoff_limit: Some(spark_application.retry_on_failure_count()),
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

#[cfg(test)]
mod tests {
    use indoc::indoc;
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
        let deserializer = serde_yaml::Deserializer::from_str(yaml);
        let spark_application: v1alpha1::SparkApplication =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer)
                .expect("invalid test SparkApplication YAML");

        let validated = validate(
            DereferencedSparkApplication {
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
            .env(&None, &None)
            .expect("the base environment can be built");
        let service_account = ServiceAccount {
            metadata: ObjectMeta {
                name: Some("spark-example".to_string()),
                ..ObjectMeta::default()
            },
            ..ServiceAccount::default()
        };

        let job = spark_job(
            &validated,
            &service_account,
            &env,
            &["echo test".to_string()],
            &submit_config,
        )
        .expect("the spark-submit Job can be built");

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
}
