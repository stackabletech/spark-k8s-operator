use stackable_operator::{k8s_openapi::api::core::v1::Pod, kube::runtime::controller::Action};
use stackable_spark_k8s_crd::{SparkApplication, SparkApplicationStatus, SparkHistoryServer};
use std::sync::Arc;
use std::time::Duration;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::logging::controller::ReconcilerError;
use strum::{EnumDiscriminants, IntoStaticStr};

pub const POD_DRIVER_CONTROLLER_NAME: &str = "pod-driver";

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("Label [{LABEL_NAME_INSTANCE}] not found for pod name [{pod_name}]"))]
    LabelInstanceNotFound { pod_name: String },
    #[snafu(display("Failed to update status for application [{name}]"))]
    ApplySparkApplicationStatus {
        source: stackable_operator::error::Error,
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
        source: stackable_operator::error::Error,
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
pub async fn reconcile(pod: Arc<SparkHistoryServer>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile history server");

    Ok(Action::await_change())
}

pub fn error_policy(_obj: Arc<Pod>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}
