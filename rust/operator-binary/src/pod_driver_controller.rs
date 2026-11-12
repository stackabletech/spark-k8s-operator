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

use crate::{
    crd::{DriverPod, cleanup::Cleanup, v1alpha1},
    spark_k8s_controller::update_status::{self, update_phase},
};

const LABEL_NAME_INSTANCE: &str = "app.kubernetes.io/instance";
const LABEL_SPARK_APP_SELECTOR: &str = "spark-app-selector";
const LABEL_SPARK_ROLE: &str = "spark-role";

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names, clippy::large_enum_variant)]
pub enum Error {
    #[snafu(display("Label [{LABEL_NAME_INSTANCE}] not found for pod name [{pod_name}]"))]
    LabelInstanceNotFound { pod_name: String },

    #[snafu(display("Failed to update the status of the application"))]
    UpdateStatus { source: update_status::Error },

    #[snafu(display("Namespace not found"))]
    NamespaceNotFound,

    #[snafu(display("Status phase not found for pod [{pod_name}]"))]
    PodStatusPhaseNotFound { pod_name: String },

    #[snafu(display("creation timestamp not found for pod {pod_name:?}"))]
    PodCreationTimestampNotFound { pod_name: String },

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

    let pod_name = pod.name_any();
    let app_name = pod
        .labels()
        .get(LABEL_NAME_INSTANCE)
        .context(LabelInstanceNotFoundSnafu {
            pod_name: pod_name.clone(),
        })?;
    let pod_phase = pod.status.as_ref().and_then(|s| s.phase.as_ref()).context(
        PodStatusPhaseNotFoundSnafu {
            pod_name: pod_name.clone(),
        },
    )?;
    let namespace = pod.namespace().context(NamespaceNotFoundSnafu)?;
    let driver_pod = DriverPod {
        name: pod_name.clone(),
        creation_timestamp: pod.metadata.creation_timestamp.clone().context(
            PodCreationTimestampNotFoundSnafu {
                pod_name: pod_name.clone(),
            },
        )?,
    };

    let Some(app) = client
        .get_opt::<v1alpha1::SparkApplication>(app_name, &namespace)
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

    if should_mirror_phase(app.status.as_ref(), pod_phase, &driver_pod) {
        tracing::info!("Update spark application [{app_name}] status to [{pod_phase}]");
        update_phase(&client, &app, pod_phase, &driver_pod)
            .await
            .context(UpdateStatusSnafu)?;
    }

    let decision = decide(
        pod_phase,
        finished_at(pod),
        &app.status
            .as_ref()
            .and_then(|status| status.effective_cleanup.clone())
            .unwrap_or_else(|| app.effective_cleanup()),
        Timestamp::now(),
    );
    tracing::info!("Driver pod {pod_name:?} has phase {pod_phase:?}, decision: {decision:?}");

    match decision {
        Decision::NotFinished => Ok(Action::await_change()),
        Decision::DeleteNow => {
            delete_pod(&client, pod).await?;
            Ok(Action::await_change())
        }
        Decision::DeleteAfter(remaining) => {
            delete_pending_executors(&client, &namespace, pod).await?;
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
/// A driver without a finish time never ran a container, so there are no logs to keep.
fn decide(
    pod_phase: &str,
    finished_at: Option<Timestamp>,
    cleanup: &Cleanup,
    now: Timestamp,
) -> Decision {
    if !is_terminal(pod_phase) {
        return Decision::NotFinished;
    }
    let ttl = if pod_phase == "Succeeded" {
        cleanup.after_succeeded()
    } else {
        cleanup.after_failed()
    };
    let Some(finished_at) = finished_at else {
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

/// Whether `pod_phase` is final, so the driver will not run again.
fn is_terminal(phase: &str) -> bool {
    matches!(phase, "Succeeded" | "Failed")
}

/// Whether `pod_phase` of `driver_pod` has to be written to the SparkApplication `status`.
///
/// A finished driver is kept for its TTL, so after a retried submission the previous driver is
/// still around and is reconciled again, for example when its TTL expires. By then the newer
/// driver may already be deleted, so the status records which driver it shows, and an older
/// driver never writes over it. A newer driver always does: a retry moves the application from
/// `Failed` back to `Pending`.
fn should_mirror_phase(
    status: Option<&v1alpha1::SparkApplicationStatus>,
    pod_phase: &str,
    driver_pod: &DriverPod,
) -> bool {
    let Some(status) = status else {
        return true;
    };
    let shown_driver = status.driver_pod.as_ref();
    let is_up_to_date = status.phase == pod_phase && shown_driver == Some(driver_pod);
    let is_superseded = shown_driver.is_some_and(|shown| driver_pod.is_older_than(shown));
    !is_up_to_date && !is_superseded
}

async fn delete_pending_executors(client: &Client, namespace: &str, driver: &Pod) -> Result<()> {
    let Some(app_selector) = driver.labels().get(LABEL_SPARK_APP_SELECTOR) else {
        return Ok(());
    };
    let executors = Api::<Pod>::namespaced(client.as_kube_client(), namespace)
        .list(
            &ListParams::default()
                .labels(&format!(
                    "{LABEL_SPARK_ROLE}=executor,{LABEL_SPARK_APP_SELECTOR}={app_selector}"
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

/// Deletes `pod`. A Pod that is already gone counts as deleted: Spark removes its own executors,
/// so they can vanish between listing and deleting them.
async fn delete_pod(client: &Client, pod: &Pod) -> Result<()> {
    match client.delete(pod).await {
        Ok(_) => Ok(()),
        Err(error) if is_not_found(&error) => {
            tracing::debug!("Pod {:?} is already deleted", pod.name_any());
            Ok(())
        }
        Err(source) => Err(Error::DeletePod {
            source,
            pod_name: pod.name_any(),
        }),
    }
}

fn is_not_found(error: &stackable_operator::client::Error) -> bool {
    matches!(
        error,
        stackable_operator::client::Error::DeleteResource {
            source: stackable_operator::kube::Error::Api(response),
            ..
        } if response.code == 404
    )
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
    #[case::pending("Pending", "10m", "10m", "12:05", Decision::NotFinished)]
    #[case::running("Running", "0s", "0s", "12:05", Decision::NotFinished)]
    #[case::unknown("Unknown", "0s", "0s", "12:05", Decision::NotFinished)]
    #[case::succeeded_waits("Succeeded", "10m", "1h", "12:04", secs(360))]
    #[case::failed_waits("Failed", "10m", "1h", "12:04", secs(3360))]
    #[case::succeeded_expired("Succeeded", "10m", "1h", "12:10", Decision::DeleteNow)]
    #[case::long_expired("Failed", "10m", "1h", "14:00", Decision::DeleteNow)]
    #[case::failed_not_expired("Failed", "10m", "1h", "12:10", secs(3000))]
    #[case::zero("Succeeded", "0s", "1h", "12:00", Decision::DeleteNow)]
    #[case::clock_skew("Succeeded", "10m", "1h", "11:59", secs(600))]
    fn decision_follows_the_outcome_and_the_finish_time(
        #[case] phase: &str,
        #[case] after_succeeded: &str,
        #[case] after_failed: &str,
        #[case] now: &str,
        #[case] expected: Decision,
    ) {
        let decision = decide(
            phase,
            Some(at(FINISHED_AT)),
            &cleanup(after_succeeded, after_failed),
            at(&format!("2026-11-12T{now}:00Z")),
        );

        assert_eq!(decision, expected);
    }

    #[test]
    fn unknown_finish_time_counts_as_expired() {
        let decision = decide("Succeeded", None, &Cleanup::default(), at(FINISHED_AT));

        assert_eq!(decision, Decision::DeleteNow);
    }

    #[test]
    fn defaults_keep_a_failed_driver_for_a_day() {
        let decision = decide(
            "Failed",
            Some(at(FINISHED_AT)),
            &Cleanup::default(),
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

    fn driver(name: &str, created: &str) -> DriverPod {
        DriverPod {
            name: name.to_owned(),
            creation_timestamp: Time(at(created)),
        }
    }

    fn status(phase: &str, shown_driver: Option<DriverPod>) -> v1alpha1::SparkApplicationStatus {
        v1alpha1::SparkApplicationStatus {
            phase: phase.to_owned(),
            driver_pod: shown_driver,
            ..v1alpha1::SparkApplicationStatus::default()
        }
    }

    const FIRST: &str = "2026-11-12T12:00:00Z";
    const RETRY: &str = "2026-11-12T12:05:00Z";

    #[rstest]
    #[case::no_status(None, "Pending", driver("a", FIRST), true)]
    #[case::submitted(Some(status("Unknown", None)), "Pending", driver("a", FIRST), true)]
    #[case::status_without_driver_recorded(
        Some(status("Running", None)),
        "Running",
        driver("a", FIRST),
        true
    )]
    #[case::same_driver_new_phase(
        Some(status("Running", Some(driver("a", FIRST)))),
        "Failed",
        driver("a", FIRST),
        true
    )]
    #[case::same_driver_same_phase(
        Some(status("Failed", Some(driver("a", FIRST)))),
        "Failed",
        driver("a", FIRST),
        false
    )]
    #[case::retry_overwrites_older_outcome(
        Some(status("Failed", Some(driver("a", FIRST)))),
        "Pending",
        driver("b", RETRY),
        true
    )]
    #[case::older_attempt_never_overwrites_retry(
        Some(status("Succeeded", Some(driver("b", RETRY)))),
        "Failed",
        driver("a", FIRST),
        false
    )]
    #[case::same_second_tie_broken_by_name(
        Some(status("Running", Some(driver("b", FIRST)))),
        "Failed",
        driver("a", FIRST),
        false
    )]
    fn only_the_newest_driver_writes_its_phase(
        #[case] status: Option<v1alpha1::SparkApplicationStatus>,
        #[case] pod_phase: &str,
        #[case] driver_pod: DriverPod,
        #[case] expected: bool,
    ) {
        assert_eq!(
            should_mirror_phase(status.as_ref(), pod_phase, &driver_pod),
            expected
        );
    }

    #[rstest]
    #[case::not_found(404, true)]
    #[case::conflict(409, false)]
    #[case::forbidden(403, false)]
    fn only_a_404_means_the_pod_is_already_deleted(#[case] code: u16, #[case] expected: bool) {
        let error = stackable_operator::client::Error::DeleteResource {
            source: stackable_operator::kube::Error::Api(
                stackable_operator::kube::core::Status::failure("failed", "Reason")
                    .with_code(code)
                    .into(),
            ),
            resource_name: "executor".to_owned(),
        };

        assert_eq!(is_not_found(&error), expected);
    }
}
