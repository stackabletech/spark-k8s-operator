use std::sync::Arc;

use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    k8s_openapi::{api::core::v1::Pod, jiff::Timestamp},
    kube::{
        Api, ResourceExt,
        api::ListParams,
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::{
    cleanup::Cleanup,
    constants::{POD_DRIVER_CONTROLLER_NAME, SPARK_DRIVER_POD_NAME},
    v1alpha1,
};

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";
const LABEL_SPARK_APP_SELECTOR: &str = "spark-app-selector";

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

    #[snafu(display("failed to get SparkApplication {name:?}"))]
    GetSparkApplication {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("Pod object is invalid"))]
    InvalidPod {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to list the Pending executors of driver pod {driver:?}"))]
    ListExecutors {
        source: stackable_operator::kube::Error,
        driver: String,
    },

    #[snafu(display("cannot delete pod {pod_name:?}"))]
    DeletePod {
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

/// Mirrors the phase of the driver into the SparkApplication status.
///
/// Once the driver finished, it is kept for the `cleanup` TTL that the app controller recorded in
/// the status (see [`decide`]), and deleted afterwards. Its executors are owned by the driver and
/// go with it. Executors that are still Pending are deleted as soon as the driver finished,
/// because they can never connect to it.
///
/// No state is kept between reconciles: the remaining time is computed from the finish time of the
/// driver each time, so an operator restart only delays the deletion.
pub async fn reconcile(pod: Arc<DeserializeGuard<Pod>>, client: Arc<Client>) -> Result<Action> {
    tracing::info!("Starting reconcile driver pod");

    let pod = pod
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidPodSnafu)?;

    let pod_name = pod.metadata.name.as_ref().context(PodNameNotFoundSnafu)?;
    let app_name = pod
        .labels()
        .get(LABEL_NAME_INSTANCE)
        .context(LabelInstanceNotFoundSnafu {
            pod_name: pod_name.clone(),
        })?;
    let phase = pod.status.as_ref().and_then(|s| s.phase.as_ref()).context(
        PodStatusPhaseNotFoundSnafu {
            pod_name: pod_name.clone(),
        },
    )?;
    let namespace = pod
        .metadata
        .namespace
        .as_ref()
        .context(NamespaceNotFoundSnafu)?;

    let Some(app) = client
        .get_opt::<v1alpha1::SparkApplication>(app_name, namespace)
        .await
        .context(GetSparkApplicationSnafu { name: app_name })?
    else {
        return Ok(Action::await_change());
    };

    if pod.metadata.creation_timestamp < app.metadata.creation_timestamp {
        tracing::info!(
            "Driver pod {pod_name:?} is older than SparkApplication {app_name:?}, so it belongs to a previous one with the same name, skipping"
        );
        return Ok(Action::await_change());
    }

    tracing::info!("Update spark application [{app_name}] status to [{phase}]");
    client
        .apply_patch_status(
            POD_DRIVER_CONTROLLER_NAME,
            &app,
            &v1alpha1::SparkApplicationStatus {
                phase: phase.clone(),
                ..app.status.clone().unwrap_or_default()
            },
        )
        .await
        .context(ApplySparkApplicationStatusSnafu { name: app_name })?;

    let decision = decide(
        phase,
        finished_at(pod),
        &app.status
            .as_ref()
            .and_then(|status| status.cleanup.clone())
            .unwrap_or_default(),
        app.spec.spark_conf.contains_key(SPARK_DRIVER_POD_NAME),
        Timestamp::now(),
    );
    tracing::info!("Driver pod {pod_name:?} has phase {phase:?}, decision: {decision:?}");

    match decision {
        Decision::NotFinished => Ok(Action::await_change()),
        Decision::DeleteNow => {
            delete_pod(&client, pod).await?;
            Ok(Action::await_change())
        }
        Decision::DeleteAfter(remaining) => {
            delete_pending_executors(&client, namespace, pod).await?;
            Ok(Action::requeue(remaining))
        }
    }
}

pub fn error_policy(_obj: Arc<DeserializeGuard<Pod>>, error: &Error, _ctx: Arc<Client>) -> Action {
    match error {
        Error::InvalidPod { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

#[derive(Debug, PartialEq)]
enum Decision {
    NotFinished,
    DeleteAfter(std::time::Duration),
    DeleteNow,
}

/// Decides what happens to a driver with `phase` that finished at `finished_at`.
///
/// A driver whose name is fixed by `spark.kubernetes.driver.pod.name` is deleted right away, so
/// that the next submission or retry can create a driver with the same name. A driver without a
/// finish time never ran a container, so there are no logs to keep.
fn decide(
    phase: &str,
    finished_at: Option<Timestamp>,
    cleanup: &Cleanup,
    fixed_name: bool,
    now: Timestamp,
) -> Decision {
    let ttl = match phase {
        "Succeeded" => cleanup.after_succeeded(),
        "Failed" => cleanup.after_failed(),
        _ => return Decision::NotFinished,
    };
    let (Some(finished_at), false) = (finished_at, fixed_name) else {
        return Decision::DeleteNow;
    };
    let elapsed =
        std::time::Duration::try_from(now.duration_since(finished_at)).unwrap_or_default();
    match ttl.checked_sub(elapsed) {
        Some(remaining) if !remaining.is_zero() => Decision::DeleteAfter(remaining),
        _ => Decision::DeleteNow,
    }
}

/// When the last container of the driver, init containers included, terminated.
fn finished_at(pod: &Pod) -> Option<Timestamp> {
    let status = pod.status.as_ref()?;
    status
        .init_container_statuses
        .iter()
        .chain(status.container_statuses.iter())
        .flatten()
        .filter_map(|container| {
            Some(
                container
                    .state
                    .as_ref()?
                    .terminated
                    .as_ref()?
                    .finished_at
                    .as_ref()?
                    .0,
            )
        })
        .max()
}

async fn delete_pending_executors(client: &Client, namespace: &str, driver: &Pod) -> Result<()> {
    let Some(app_selector) = driver.labels().get(LABEL_SPARK_APP_SELECTOR) else {
        return Ok(());
    };
    let executors = Api::<Pod>::namespaced(client.as_kube_client(), namespace)
        .list(
            &ListParams::default()
                .labels(&format!(
                    "spark-role=executor,{LABEL_SPARK_APP_SELECTOR}={app_selector}"
                ))
                .fields("status.phase=Pending"),
        )
        .await
        .context(ListExecutorsSnafu {
            driver: driver.name_any(),
        })?;
    for executor in &executors.items {
        tracing::info!(
            "Deleting executor pod {executor:?} of finished driver pod {driver:?}, it is still Pending",
            executor = executor.name_any(),
            driver = driver.name_any(),
        );
        delete_pod(client, executor).await?;
    }
    Ok(())
}

async fn delete_pod(client: &Client, pod: &Pod) -> Result<()> {
    client
        .delete(pod)
        .await
        .context(DeletePodSnafu {
            pod_name: pod.name_any(),
        })
        .map(drop)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use stackable_operator::k8s_openapi::{
        api::core::v1::{
            ContainerState, ContainerStateRunning, ContainerStateTerminated, ContainerStatus,
            PodStatus,
        },
        apimachinery::pkg::apis::meta::v1::Time,
    };

    use super::*;

    const FINISHED_AT: &str = "2026-11-12T12:00:00Z";

    fn at(time: &str) -> Timestamp {
        time.parse().expect("the timestamp is valid")
    }

    fn cleanup(after_succeeded: &str, after_failed: &str) -> Cleanup {
        Cleanup {
            after_succeeded: Some(after_succeeded.parse().expect("the TTL is valid")),
            after_failed: Some(after_failed.parse().expect("the TTL is valid")),
        }
    }

    fn secs(seconds: u64) -> Decision {
        Decision::DeleteAfter(std::time::Duration::from_secs(seconds))
    }

    #[rstest]
    #[case::pending("Pending", "10m", "10m", false, "12:05", Decision::NotFinished)]
    #[case::running("Running", "0s", "0s", true, "12:05", Decision::NotFinished)]
    #[case::unknown("Unknown", "0s", "0s", false, "12:05", Decision::NotFinished)]
    #[case::succeeded_waits("Succeeded", "10m", "1h", false, "12:04", secs(360))]
    #[case::failed_waits("Failed", "10m", "1h", false, "12:04", secs(3360))]
    #[case::succeeded_expired("Succeeded", "10m", "1h", false, "12:10", Decision::DeleteNow)]
    #[case::long_expired("Failed", "10m", "1h", false, "14:00", Decision::DeleteNow)]
    #[case::failed_not_expired("Failed", "10m", "1h", false, "12:10", secs(3000))]
    #[case::zero("Succeeded", "0s", "1h", false, "12:00", Decision::DeleteNow)]
    #[case::fixed_name("Succeeded", "1h", "1h", true, "12:01", Decision::DeleteNow)]
    #[case::clock_skew("Succeeded", "10m", "1h", false, "11:59", secs(600))]
    fn decision_follows_the_outcome_and_the_finish_time(
        #[case] phase: &str,
        #[case] after_succeeded: &str,
        #[case] after_failed: &str,
        #[case] fixed_name: bool,
        #[case] now: &str,
        #[case] expected: Decision,
    ) {
        let decision = decide(
            phase,
            Some(at(FINISHED_AT)),
            &cleanup(after_succeeded, after_failed),
            fixed_name,
            at(&format!("2026-11-12T{now}:00Z")),
        );

        assert_eq!(decision, expected);
    }

    #[test]
    fn unknown_finish_time_counts_as_expired() {
        let decision = decide(
            "Succeeded",
            None,
            &Cleanup::default(),
            false,
            at(FINISHED_AT),
        );

        assert_eq!(decision, Decision::DeleteNow);
    }

    #[test]
    fn defaults_keep_a_failed_driver_for_a_day() {
        let decision = decide(
            "Failed",
            Some(at(FINISHED_AT)),
            &Cleanup::default(),
            false,
            at("2026-11-12T13:00:00Z"),
        );

        assert_eq!(decision, secs(23 * 3600));
    }

    fn container(finished_at: Option<&str>) -> ContainerStatus {
        ContainerStatus {
            state: Some(match finished_at {
                Some(finished_at) => ContainerState {
                    terminated: Some(ContainerStateTerminated {
                        finished_at: Some(Time(at(finished_at))),
                        ..ContainerStateTerminated::default()
                    }),
                    ..ContainerState::default()
                },
                None => ContainerState {
                    running: Some(ContainerStateRunning::default()),
                    ..ContainerState::default()
                },
            }),
            ..ContainerStatus::default()
        }
    }

    #[rstest]
    #[case::last_container(
        vec![container(Some("2026-11-11T12:00:00Z"))],
        vec![container(Some("2026-11-11T12:00:20Z")), container(Some("2026-11-11T12:00:10Z"))],
        Some("2026-11-11T12:00:20Z")
    )]
    #[case::failed_init_container(
        vec![container(Some("2026-11-11T12:00:05Z"))],
        vec![],
        Some("2026-11-11T12:00:05Z")
    )]
    #[case::none_terminated(vec![], vec![container(None)], None)]
    fn finish_time_is_the_last_terminated_container(
        #[case] init_containers: Vec<ContainerStatus>,
        #[case] containers: Vec<ContainerStatus>,
        #[case] expected: Option<&str>,
    ) {
        let pod = Pod {
            status: Some(PodStatus {
                init_container_statuses: Some(init_containers),
                container_statuses: Some(containers),
                ..PodStatus::default()
            }),
            ..Pod::default()
        };

        assert_eq!(finished_at(&pod), expected.map(at));
    }
}
