use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder,
};
use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, EmptyDirVolumeSource, EnvVar, Pod, PodSpec,
    PodTemplateSpec, Volume,
};
use stackable_operator::logging::controller::ReconcilerError;
use stackable_operator::{
    kube::runtime::controller::{Action, Context},
    product_config::ProductConfigManager,
};
use stackable_spark_k8s_crd::constants::*;
use stackable_spark_k8s_crd::SparkApplication;
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

const FIELD_MANAGER_SCOPE: &str = "sparkapplication";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object defines no version"))]
    ObjectHasNoVersion,
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Job"))]
    ApplyApplication {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build stark-submit command"))]
    BuildCommand {
        source: stackable_spark_k8s_crd::Error,
    },
    #[snafu(display("failed to build the pod template config map"))]
    PodTemplateConfigMap {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("no job image specified"))]
    ObjectHasNoImage,
    #[snafu(display("no spark base image specified"))]
    ObjectHasNoSparkImage,
    #[snafu(display("invalid pod template"))]
    PodTemplate {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("driver pod template serialization"))]
    DriverPodTemplateSerde { source: serde_yaml::Error },
    #[snafu(display("executor pod template serialization"))]
    ExecutorPodTemplateSerde { source: serde_yaml::Error },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile(
    spark_application: Arc<SparkApplication>,
    ctx: Context<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;

    let spark_image = spark_application
        .spec
        .spark_image
        .as_deref()
        .context(ObjectHasNoSparkImageSnafu)?;

    let job_container = spark_application.spec.image.as_ref().map(|job_image| {
        ContainerBuilder::new(CONTAINER_NAME_JOB)
            .image(job_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("cp /jobs/* {VOLUME_MOUNT_PATH_JOB}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
            .build()
    });

    let requirements_container = spark_application.requirements().map(|req| {
        ContainerBuilder::new(CONTAINER_NAME_REQ)
            .image(spark_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("pip install --target={VOLUME_MOUNT_PATH_REQ} {req}"),
            ])
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
            .build()
    });

    let pod_template_config_map = pod_template_config_map(&spark_application, &job_container, &requirements_container)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = spark_job(&spark_application, spark_image, &job_container)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

fn pod_template(
    container_name: &str,
    job_container: &Option<Container>,
    requirements_container: &Option<Container>,
) -> Result<Pod> {
    let mut container = ContainerBuilder::new(container_name);

    if job_container.is_some() {
        container.add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB);
    }

    if requirements_container.is_some() {
        container
            .add_volume_mount(VOLUME_MOUNT_NAME_REQ, VOLUME_MOUNT_PATH_REQ)
            .add_env_var(
                "PYTHONPATH",
                format!("$SPARK_HOME/python:{VOLUME_MOUNT_PATH_REQ}:$PYTHONPATH"),
            );
    }

    let mut template = PodBuilder::new();
    template.metadata_default().add_container(container.build());

    if let Some(container) = requirements_container.clone() {
        template.add_init_container(container);
        template.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_REQ)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    if let Some(container) = job_container.clone() {
        template.add_init_container(container);
        template.add_volume(
            VolumeBuilder::new(VOLUME_MOUNT_NAME_JOB)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    template.build().context(PodTemplateSnafu)
}

fn pod_template_config_map(
    spark_application: &SparkApplication,
    job_container: &Option<Container>,
    requirements_container: &Option<Container>,
) -> Result<ConfigMap> {
    let driver_template = pod_template(
        CONTAINER_NAME_DRIVER,
        job_container,
        requirements_container,
    )?;
    let executor_template = pod_template(
        CONTAINER_NAME_EXECUTOR,
        job_container,
        requirements_container,
    )?;

    ConfigMapBuilder::new()
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(spark_application)
                .name(spark_application.pod_template_config_map_name())
                .ownerreference_from_resource(spark_application, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .build(),
        )
        .add_data(
            "driver.yml",
            serde_yaml::to_string(&driver_template).context(DriverPodTemplateSerdeSnafu)?,
        )
        .add_data(
            "executor.yml",
            serde_yaml::to_string(&executor_template).context(ExecutorPodTemplateSerdeSnafu)?,
        )
        .build()
        .context(PodTemplateConfigMapSnafu)
}

fn spark_job(spark_application: &SparkApplication, spark_image: &str, job_container: &Option<Container>) -> Result<Job> {
    let commands = spark_application
        .build_command()
        .context(BuildCommandSnafu)?;

    let mut container = ContainerBuilder::new("spark-submit");
    container.image(spark_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec!["-c".to_string(), "-x".to_string(), commands.join(" ")])
        .add_volume_mount(
            VOLUME_MOUNT_NAME_POD_TEMPLATES,
            VOLUME_MOUNT_PATH_POD_TEMPLATES,
        )
        .add_volume_mount(VOLUME_MOUNT_NAME_JOB, VOLUME_MOUNT_PATH_JOB)
        .add_env_vars(vec![EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("/stackable/spark/conf".to_string()),
            value_from: None,
        }]);

    let pod = PodTemplateSpec {
        metadata: Some(ObjectMetaBuilder::new().name("spark-submit").build()),
        spec: Some(PodSpec {
            containers: vec![container.build()],
            init_containers: job_container.as_ref().map(|c| vec![c.clone()]),
            restart_policy: Some("Never".to_string()),
            volumes: Some(vec![
                Volume {
                    name: String::from(VOLUME_MOUNT_NAME_POD_TEMPLATES),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(spark_application.pod_template_config_map_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                },
                Volume {
                    name: String::from(VOLUME_MOUNT_NAME_JOB),
                    empty_dir: Some(EmptyDirVolumeSource::default()),
                    ..Volume::default()
                },
            ]),
            ..PodSpec::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark_application)
            .ownerreference_from_resource(spark_application, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ttl_seconds_after_finished: Some(600),
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
