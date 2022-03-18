use crate::ListParams;
use futures::{future, StreamExt};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::builder::{ContainerBuilder, ObjectMetaBuilder};
use stackable_operator::k8s_openapi::api::batch::v1::{Job, JobSpec};
use stackable_operator::k8s_openapi::api::core::v1::{PodSpec, PodTemplateSpec, Volume, ConfigMapVolumeSource};
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

fn build_init_job(spark: &SparkApplication) -> Result<Job> {
    let commands = spark.build_command().context(BuildCommandSnafu)?;

    let version = spark.version().context(ObjectHasNoVersionSnafu)?;
    let container = ContainerBuilder::new("spark-client")
        .image(qualified_image_name(version))
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join(" ")])
        .add_volume_mount("pod-template", "/stackable/spark/pod-templates")
        .build();

    let pod = PodTemplateSpec {
        metadata: Some(ObjectMetaBuilder::new().name("init").build()),
        spec: Some(PodSpec {
            containers: vec![container],
            restart_policy: Some("Never".to_string()),
            volumes: Some(vec![Volume {
                name: "pod-template".to_string(),
                config_map: Some(ConfigMapVolumeSource {
                        name: Some(format!("{}-pod-template", spark.name())),
                        ..ConfigMapVolumeSource::default()
                    }),
                ..Volume::default()
            }]),
            ..PodSpec::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(spark)
            .ownerreference_from_resource(spark, None, Some(true))
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

fn qualified_image_name(version: &str) -> String {
    format!(
        "docker.stackable.tech/stackable/spark-k8s:{}-stackable0",
        version
    )
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
