use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    k8s_openapi::{
        api::core::v1::Pod,
        jiff::{SignedDuration, Timestamp},
    },
    kube::{
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::{constants::POD_DRIVER_CONTROLLER_NAME, v1alpha1};

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names, clippy::large_enum_variant)]
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

    #[snafu(display("Pod object is invalid"))]
    InvalidPod {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("cannot delete Spark driver pod {pod_name:?}"))]
    DeleteDriverPod {
        source: stackable_operator::client::Error,
        pod_name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

/// This function serves two purposes:
/// 1. It updates the status of the SparkApplication CR based on the status of the driver pod.
/// 2. It deletes the driver pod when the SparkApplication reaches a terminal state (Succeeded or Failed).
pub async fn reconcile(pod: Arc<DeserializeGuard<Pod>>, client: Arc<Client>) -> Result<Action> {
    tracing::info!("Starting reconcile driver pod");

    let pod = pod
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidPodSnafu)?;

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
        .get::<v1alpha1::SparkApplication>(
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
            &v1alpha1::SparkApplicationStatus {
                phase: phase.clone(),
                resolved_template_ref: app
                    .status
                    .as_ref()
                    .map(|s| s.resolved_template_ref.clone())
                    .unwrap_or_default(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: app_name.clone(),
        })?;

    // We must manually delete the driver pod when the application reached a terminal state
    // otherwise they are left hanging forever. The pod is optionally kept for a while so that
    // users can inspect its logs.
    if phase == "Succeeded" || phase == "Failed" {
        let ttl = app.spec.ttl_after_finished.map(|ttl| *ttl);
        if let Some(remaining) = remaining_ttl(pod, ttl, Timestamp::now()) {
            tracing::info!(
                "Spark application {app_name:?} completed with phase {phase:?}, keeping driver pod {pod_name:?} for another {remaining:?}"
            );
            return Ok(Action::requeue(remaining));
        }

        tracing::info!(
            "Spark application {app_name:?} completed with phase {phase:?}, deleting driver pod {pod_name:?}"
        );
        client
            .delete(pod)
            .await
            .with_context(|_| DeleteDriverPodSnafu {
                pod_name: pod_name.clone(),
            })?;
    }
    Ok(Action::await_change())
}

/// The point in time at which the driver pod finished: the latest termination time of its
/// containers, falling back to the latest condition transition.
fn finished_at(pod: &Pod) -> Option<Timestamp> {
    let status = pod.status.as_ref()?;
    status
        .container_statuses
        .iter()
        .flatten()
        .filter_map(|c| c.state.as_ref()?.terminated.as_ref()?.finished_at.as_ref())
        .map(|t| t.0)
        .max()
        .or_else(|| {
            status
                .conditions
                .iter()
                .flatten()
                .filter_map(|c| c.last_transition_time.as_ref())
                .map(|t| t.0)
                .max()
        })
}

/// How much longer a finished driver pod must be kept, or `None` if it can be deleted now.
/// The pod is deleted right away without a TTL, or if its finish time is unknown.
fn remaining_ttl(
    pod: &Pod,
    ttl: Option<std::time::Duration>,
    now: Timestamp,
) -> Option<std::time::Duration> {
    let ttl = SignedDuration::try_from(ttl?).unwrap_or(SignedDuration::MAX);
    let elapsed = now.duration_since(finished_at(pod)?);
    let remaining = ttl.checked_sub(elapsed)?;
    remaining.is_positive().then(|| remaining.unsigned_abs())
}

pub fn error_policy(_obj: Arc<DeserializeGuard<Pod>>, error: &Error, _ctx: Arc<Client>) -> Action {
    match error {
        Error::InvalidPod { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rstest::rstest;
    use stackable_operator::k8s_openapi::{
        api::core::v1::{ContainerState, ContainerStateTerminated, ContainerStatus, PodStatus},
        apimachinery::pkg::apis::meta::v1::Time,
    };

    use super::*;

    fn pod_finished_at(finished_at: &str) -> Pod {
        Pod {
            status: Some(PodStatus {
                container_statuses: Some(vec![ContainerStatus {
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            finished_at: Some(Time(finished_at.parse().unwrap())),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[rstest]
    #[case::no_ttl(None, "2026-01-01T00:00:10Z", None)]
    #[case::within_ttl(Some(60), "2026-01-01T00:00:10Z", Some(50))]
    #[case::ttl_elapsed(Some(60), "2026-01-01T00:01:00Z", None)]
    #[case::ttl_long_elapsed(Some(60), "2026-01-01T01:00:00Z", None)]
    #[case::zero_ttl(Some(0), "2026-01-01T00:00:00Z", None)]
    #[case::clock_skew(Some(60), "2025-12-31T23:59:59Z", Some(61))]
    fn test_remaining_ttl(
        #[case] ttl_secs: Option<u64>,
        #[case] now: &str,
        #[case] expected_secs: Option<u64>,
    ) {
        let pod = pod_finished_at("2026-01-01T00:00:00Z");
        assert_eq!(
            remaining_ttl(
                &pod,
                ttl_secs.map(Duration::from_secs),
                now.parse().unwrap()
            ),
            expected_secs.map(Duration::from_secs)
        );
    }

    #[test]
    fn test_remaining_ttl_unknown_finish_time() {
        assert_eq!(
            remaining_ttl(
                &Pod::default(),
                Some(Duration::from_secs(60)),
                Timestamp::now()
            ),
            None
        );
    }
}
