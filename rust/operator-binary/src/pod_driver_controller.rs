use stackable_operator::{
    client::Client, k8s_openapi::api::core::v1::Pod, kube::runtime::controller::Action,
    time::Duration,
};
use stackable_spark_k8s_crd::{
    constants::POD_DRIVER_CONTROLLER_NAME, SparkApplication, SparkApplicationStatus,
};
use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Label [{LABEL_NAME_INSTANCE}] not found for pod name [{pod_name}]"))]
    LabelInstanceNotFound { pod_name: String },
    #[snafu(display("Failed to update status for application [{name}]"))]
    ApplySparkApplicationStatus {
        source: stackable_operator::client::Error,
        name: String,
    },
    #[snafu(display("Pod name not found"))]
    PodNameNotFound,
    #[snafu(display("Namespace not found"))]
    NamespaceNotFound,
    #[snafu(display("Status phase not found for pod [{pod_name}]"))]
    PodStatusPhaseNotFound { pod_name: String },
    #[snafu(display("Spark application [{name}] not found"))]
    SparkApplicationNotFound {
        source: stackable_operator::client::Error,
        name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}
/// Updates the status of the SparkApplication that started the pod.
pub async fn reconcile(pod: Arc<Pod>, client: Arc<Client>) -> Result<Action> {
    tracing::info!("Starting reconcile driver pod");

    let pod_name = pod.metadata.name.as_ref().context(PodNameNotFoundSnafu)?;
    let app_name = pod
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(&String::from(LABEL_NAME_INSTANCE)))
        .context(LabelInstanceNotFoundSnafu {
            pod_name: pod_name.clone(),
        })?;
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref()).context(
        PodStatusPhaseNotFoundSnafu {
            pod_name: pod_name.clone(),
        },
    )?;

    let app = client
        .get::<SparkApplication>(
            app_name.as_ref(),
            pod.metadata
                .namespace
                .as_ref()
                .context(NamespaceNotFoundSnafu)?,
        )
        .await
        .context(SparkApplicationNotFoundSnafu {
            name: app_name.clone(),
        })?;

    tracing::info!("Update spark application [{app_name}] status to [{phase}]");

    client
        .apply_patch_status(
            POD_DRIVER_CONTROLLER_NAME,
            &app,
            &SparkApplicationStatus {
                phase: phase.clone(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: app_name.clone(),
        })?;

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<Pod>, _error: &Error, _ctx: Arc<Client>) -> Action {
    Action::requeue(*Duration::from_secs(5))
}
