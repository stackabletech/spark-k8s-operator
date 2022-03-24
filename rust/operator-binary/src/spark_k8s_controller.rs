use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder,
};
use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, EnvVar, PodSpec, PodTemplateSpec,
    Volume,
};
use stackable_operator::logging::controller::ReconcilerError;
use stackable_operator::{
    kube::runtime::controller::{Action, Context},
    product_config::ProductConfigManager,
};
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
    let pod_template_config_map = build_pod_template_config_map(&spark_application)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = build_init_job(&spark_application)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    Ok(Action::await_change())
}

fn build_pod_template_config_map(spark_application: &SparkApplication) -> Result<ConfigMap> {
    let job_init_container = spark_application.spec.image.as_ref().map(|job_image| {
        ContainerBuilder::new("job-init-container")
            .image(job_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                "cp /jobs/* /stackable/spark/jobs".to_string(),
            ])
            .add_volume_mount("job-files", "/stackable/spark/jobs")
            .build()
    });

    let spark_image = spark_application
        .spec
        .spark_image
        .as_deref()
        .context(ObjectHasNoSparkImageSnafu)?;

    let requirements_init_container = spark_application.requirements().map(|req| {
        ContainerBuilder::new("requirements-init-container")
            .image(spark_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                format!("pip install --target=/stackable/spark/requirements {}", req),
            ])
            .add_volume_mount("requirements", "/stackable/spark/requirements")
            .build()
    });

    let mut spark_driver_container = ContainerBuilder::new("spark-driver-container");
    spark_driver_container.image("dummy-overwritten-by-command-line");
    if job_init_container.is_some() {
        spark_driver_container.add_volume_mount("job-files", "/stackable/spark/jobs");
    }

    if requirements_init_container.is_some() {
        spark_driver_container
            .add_volume_mount("requirements", "/stackable/spark/requirements")
            .add_env_var(
                "PYTHONPATH",
                "$SPARK_HOME/python:/stackable/spark/requirements:$PYTHONPATH",
            );
    }

    let mut spark_executor_container = ContainerBuilder::new("spark-executor-container");
    if job_init_container.is_some() {
        spark_executor_container.add_volume_mount("job-files", "/stackable/spark/jobs");
    }
    if requirements_init_container.is_some() {
        spark_executor_container
            .add_volume_mount("requirements", "/stackable/spark/requirements")
            .add_env_var(
                "PYTHONPATH",
                "$SPARK_HOME/python:/stackable/spark/requirements:$PYTHONPATH",
            );
    }

    let mut driver_template = PodBuilder::new();
    driver_template
        .metadata_default()
        .add_container(spark_driver_container.build());

    if let Some(container) = requirements_init_container.clone() {
        driver_template.add_init_container(container);
        driver_template.add_volume(
            VolumeBuilder::new("requirements")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    if let Some(container) = job_init_container.clone() {
        driver_template.add_init_container(container);
        driver_template.add_volume(
            VolumeBuilder::new("job-files")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }

    let mut executor_template = PodBuilder::new();
    executor_template
        .metadata_default()
        .add_container(spark_executor_container.build());

    if let Some(container) = requirements_init_container {
        executor_template.add_init_container(container);
        executor_template.add_volume(
            VolumeBuilder::new("requirements")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }
    if let Some(container) = job_init_container {
        executor_template.add_init_container(container);
        executor_template.add_volume(
            VolumeBuilder::new("job-files")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
    }

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
            serde_yaml::to_string(&driver_template.build().context(PodTemplateSnafu)?)
                .context(DriverPodTemplateSerdeSnafu)?,
        )
        .add_data(
            "executor.yml",
            serde_yaml::to_string(&executor_template.build().context(PodTemplateSnafu)?)
                .context(ExecutorPodTemplateSerdeSnafu)?,
        )
        .build()
        .context(PodTemplateConfigMapSnafu)
}

fn build_init_job(spark_application: &SparkApplication) -> Result<Job> {
    let commands = spark_application
        .build_command()
        .context(BuildCommandSnafu)?;

    let container = ContainerBuilder::new("spark-submit")
        .image(
            spark_application
                .spec
                .spark_image
                .as_deref()
                .context(ObjectHasNoSparkImageSnafu)?,
        )
        .command(vec!["/bin/bash".to_string()])
        .args(vec!["-c".to_string(), "-x".to_string(), commands.join(" ")])
        .add_volume_mount("pod-template", "/stackable/spark/pod-templates")
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .add_env_vars(vec![EnvVar {
            name: "SPARK_CONF_DIR".to_string(),
            value: Some("/stackable/spark/conf".to_string()),
            value_from: None,
        }])
        .build();

    let job_init_container = spark_application.spec.image.as_ref().map(|job_image| {
        ContainerBuilder::new("job-init-container")
            .image(job_image)
            .command(vec![
                "/bin/bash".to_string(),
                "-x".to_string(),
                "-c".to_string(),
                "cp /jobs/* /stackable/spark/jobs".to_string(),
            ])
            .add_volume_mount("job-files", "/stackable/spark/jobs")
            .build()
    });

    let pod = PodTemplateSpec {
        metadata: Some(ObjectMetaBuilder::new().name("spark-submit").build()),
        spec: Some(PodSpec {
            containers: vec![container],
            init_containers: Some(job_init_container.into_iter().collect()),
            restart_policy: Some("Never".to_string()),
            volumes: Some(vec![
                Volume {
                    name: "pod-template".to_string(),
                    config_map: Some(ConfigMapVolumeSource {
                        name: Some(spark_application.pod_template_config_map_name()),
                        ..ConfigMapVolumeSource::default()
                    }),
                    ..Volume::default()
                },
                Volume {
                    name: "job-files".to_string(),
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
