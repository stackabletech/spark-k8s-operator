use snafu::{OptionExt, ResultExt};
use stackable_operator::{
    builder::{meta::ObjectMetaBuilder, pod::volume::VolumeBuilder},
    k8s_openapi::{
        DeepMerge,
        api::{
            batch::v1::{Job, JobSpec},
            core::v1::{Affinity, EnvVar, PodSpec, PodTemplateSpec, ServiceAccount},
        },
    },
    v2::builder::pod::container::new_container_builder,
};

use crate::{
    crd::{
        constants::*,
        roles::{SparkApplicationRole, SparkContainer, SubmitConfig},
        tlscerts,
    },
    spark_k8s_controller::{
        AddVolumeMountSnafu, CreateVolumesSnafu, MissingSecretLifetimeSnafu, Result,
        build::pod::security_context, validate,
    },
};

pub(crate) fn spark_job(
    validated: &validate::ValidatedSparkApplication,
    serviceaccount: &ServiceAccount,
    env: &[EnvVar],
    job_commands: &[String],
    job_config: &SubmitConfig,
) -> Result<Job> {
    let spark_application = &validated.spark_application;
    let spark_image = &validated.resolved_product_image;
    let s3conn = &validated.cluster_config.s3_connection;
    let logdir = &validated.cluster_config.log_dir;
    let open_lineage_conn = validated.cluster_config.open_lineage_connection.as_ref();
    let mut cb = new_container_builder(&SparkContainer::SparkSubmit.to_container_name());

    let merged_env = spark_application.merged_env(SparkApplicationRole::Submit, env);

    // The SPARK_SUBMIT_OPTS env var is used to configure the JVM settings of the spark-submit job.
    // Here we need to point the JVM to our logging configuration and if S3 is used for data or Spark History,
    // we also need to tell the JVM where the trust store is located.
    // The same properties are also set for the driver and executor pods via the pod template config maps.
    let mut spark_submit_opts_env = vec![format!(
        "-Dlog4j.configurationFile={VOLUME_MOUNT_PATH_LOG_CONFIG}/{LOG4J2_CONFIG_FILE}"
    )];
    if tlscerts::tls_secret_names(s3conn, logdir, open_lineage_conn).is_some() {
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStore={STACKABLE_TRUST_STORE}/truststore.p12"
        ));
        spark_submit_opts_env.push(format!(
            "-Djavax.net.ssl.trustStorePassword={STACKABLE_TLS_STORE_PASSWORD}"
        ));
    }
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
        .add_volume_mounts(spark_application.spark_job_volume_mounts(
            s3conn,
            logdir,
            open_lineage_conn,
        ))
        .context(AddVolumeMountSnafu)?
        .add_env_vars(merged_env)
        .add_env_var("SPARK_SUBMIT_OPTS", spark_submit_opts_env.join(" "))
        // TODO: move this to the image
        .add_env_var("SPARK_CONF_DIR", "/stackable/spark/conf");

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
                .with_labels(validated.recommended_labels("spark-job-template"))
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
        metadata: validated
            .object_meta(validated.name.to_string(), "spark-job")
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
