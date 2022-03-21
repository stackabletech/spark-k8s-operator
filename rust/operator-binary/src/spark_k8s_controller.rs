use crate::ListParams;
use futures::{future, StreamExt};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{
    ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder, VolumeBuilder,
};
use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, EmptyDirVolumeSource, PodSpec, PodTemplateSpec, Volume,
};
use stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use stackable_operator::k8s_openapi::chrono::Utc;
use stackable_operator::kube::{runtime, ResourceExt};
use stackable_operator::logging::controller::ReconcilerError;
use stackable_operator::{
    kube::runtime::controller::{Context, ReconcilerAction},
    product_config::ProductConfigManager,
};
use stackable_spark_k8s_crd::{CommandStatus, SparkApplication};
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
    spark: Arc<SparkApplication>,
    ctx: Context<Ctx>,
) -> Result<ReconcilerAction> {
    tracing::info!("Starting reconcile");

    let client = &ctx.get_ref().client;
    let pod_template_config_map = build_pod_template_config_map(&spark)?;
    client
        .apply_patch(
            FIELD_MANAGER_SCOPE,
            &pod_template_config_map,
            &pod_template_config_map,
        )
        .await
        .context(ApplyApplicationSnafu)?;

    let job = build_init_job(&spark)?;
    client
        .apply_patch(FIELD_MANAGER_SCOPE, &job, &job)
        .await
        .context(ApplyApplicationSnafu)?;

    if spark.status == None {
        let started_at = Some(Time(Utc::now()));
        client
            .apply_patch_status(
                FIELD_MANAGER_SCOPE,
                &*spark,
                &CommandStatus {
                    started_at: started_at.to_owned(),
                    finished_at: None,
                },
            )
            .await
            .context(ApplyStatusSnafu)?;

        wait_completed(client, &job).await;
        let finished_at = Some(Time(Utc::now()));

        client
            .apply_patch_status(
                FIELD_MANAGER_SCOPE,
                &*spark,
                &CommandStatus {
                    started_at,
                    finished_at,
                },
            )
            .await
            .context(ApplyStatusSnafu)?;
    }

    Ok(ReconcilerAction {
        requeue_after: None,
    })
}

fn build_pod_template_config_map(spark_application: &SparkApplication) -> Result<ConfigMap> {
    let job_init_container = ContainerBuilder::new("job-init-container")
        .image(spark_application.image().context(ObjectHasNoImageSnafu)?)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-c".to_string(),
            "cp /jobs/* /stackable/spark/jobs".to_string(),
        ])
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .build();

    let requirements_init_container = ContainerBuilder::new("requirements-init-container")
        .image(
            spark_application
                .spec
                .spark_image
                .as_deref()
                .context(ObjectHasNoSparkImageSnafu)?,
        )
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-c".to_string(),
            "pip install --user tabulate==0.8.9".to_string(),
        ])
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .build();

    let spark_driver_container = ContainerBuilder::new("spark-driver-container")
        .image("dummy-overwritten-by-command-line")
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .build();

    let spark_executor_container = ContainerBuilder::new("spark-executor-container")
        .image("dummy-overwritten-by-command-line")
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .build();

    let driver_template = PodBuilder::new()
        .metadata_default()
        .add_init_container(job_init_container.clone())
        .add_init_container(requirements_init_container.clone())
        .add_container(spark_driver_container)
        .add_volume(
            VolumeBuilder::new("job-files")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        )
        .build()
        .context(PodTemplateSnafu)?;

    let executor_template = PodBuilder::new()
        .metadata_default()
        .add_init_container(job_init_container)
        .add_init_container(requirements_init_container)
        .add_container(spark_executor_container)
        .add_volume(
            VolumeBuilder::new("job-files")
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        )
        .build()
        .context(PodTemplateSnafu)?;

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

fn build_init_job(spark_application: &SparkApplication) -> Result<Job> {
    let commands = spark_application
        .build_command()
        .context(BuildCommandSnafu)?;

    let container = ContainerBuilder::new("spark-client")
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
        .build();

    let job_init_container = ContainerBuilder::new("job-init-container")
        .image(spark_application.image().context(ObjectHasNoImageSnafu)?)
        .command(vec![
            "/bin/bash".to_string(),
            "-x".to_string(),
            "-c".to_string(),
            "cp /jobs/* /stackable/spark/jobs".to_string(),
        ])
        .add_volume_mount("job-files", "/stackable/spark/jobs")
        .build();

    let pod = PodTemplateSpec {
        metadata: Some(ObjectMetaBuilder::new().name("init").build()),
        spec: Some(PodSpec {
            containers: vec![container],
            init_containers: Some(vec![job_init_container]),
            restart_policy: Some("Never".to_string()),
            volumes: Some(vec![Volume {
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
            } 
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
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

// Waits until the given job is completed.
async fn wait_completed(client: &stackable_operator::client::Client, job: &Job) {
    let completed = |job: &Job| {
        job.status
            .as_ref()
            .and_then(|status| status.conditions.clone())
            .unwrap_or_default()
            .into_iter()
            .any(|condition| condition.type_ == "Complete" && condition.status == "True")
    };

    let lp = ListParams::default().fields(&format!("metadata.name={}", job.name()));
    let api = client.get_api(Some(job.namespace().as_deref().unwrap_or("default")));
    let watcher = runtime::watcher(api, lp).boxed();
    runtime::utils::try_flatten_applied(watcher)
        .any(|res| future::ready(res.as_ref().map(|job| completed(job)).unwrap_or(false)))
        .await;
}

pub fn error_policy(_error: &Error, _ctx: Context<Ctx>) -> ReconcilerAction {
    ReconcilerAction {
        requeue_after: Some(Duration::from_secs(5)),
    }
}
