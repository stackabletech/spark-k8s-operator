use std::{
    fmt::{Debug, Display},
    sync::Arc,
};

use serde::de::DeserializeOwned;
use serde_json::json;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::meta::OwnerReferenceBuilder,
    client::Client,
    k8s_openapi::{
        NamespaceResourceScope,
        api::{
            batch::v1::Job,
            core::v1::{ConfigMap, Pod, ServiceAccount},
            rbac::v1::RoleBinding,
        },
        apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference},
    },
    kube::{
        Api, Resource, ResourceExt,
        api::{DeleteParams, Patch, PatchParams},
        core::{DeserializeGuard, error_boundary},
        runtime::controller::Action,
    },
    logging::controller::ReconcilerError,
    shared::time::Duration,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::crd::{constants::POD_DRIVER_CONTROLLER_NAME, roles::SparkApplicationRole, v1alpha1};

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

    #[snafu(display("failed to get SparkApplication {name:?}"))]
    GetSparkApplication {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("Pod object is invalid"))]
    InvalidPod {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to get the spark-submit Job {name:?}"))]
    GetJob {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("failed to build the owner reference to the spark-submit Job"))]
    BuildJobOwnerReference {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("SparkApplication {name:?} has no UID"))]
    SparkApplicationUidNotFound { name: String },

    #[snafu(display("failed to hand {kind} {name:?} over to the spark-submit Job"))]
    HandOver {
        source: stackable_operator::kube::Error,
        kind: String,
        name: String,
    },

    #[snafu(display("failed to delete {kind} {name:?}"))]
    DeleteObject {
        source: stackable_operator::kube::Error,
        kind: String,
        name: String,
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
/// Once a driver finished, it and the objects the operator created for its SparkApplication are
/// handed over to the spark-submit Job, which keeps them until its TTL expires. Without a usable
/// Job they are deleted right away.
///
/// The driver is handed over last: a driver owned by the Job marks the hand-over as complete and
/// ends all further reconciles of it, so the other objects must already be handed over by then.
pub async fn reconcile(pod: Arc<DeserializeGuard<Pod>>, client: Arc<Client>) -> Result<Action> {
    tracing::info!("Starting reconcile driver pod");

    let pod = pod
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidPodSnafu)?;

    if pod.metadata.deletion_timestamp.is_some() {
        return Ok(Action::await_change());
    }

    // if ownership has already transferred to job, nothing to do anymore
    if owner_uid::<Job>(&pod.metadata).is_some() {
        return Ok(Action::await_change());
    }

    let pod_name = pod.metadata.name.as_ref().context(PodNameNotFoundSnafu)?;
    let app_name = instance_label(pod).context(LabelInstanceNotFoundSnafu {
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
        if is_finished(phase) {
            tracing::info!(
                "SparkApplication {app_name:?} is gone, deleting finished driver pod {pod_name:?}"
            );
            apply_to::<Pod>(&client, namespace, pod_name, &ObjectAction::Delete).await?;
        }
        return Ok(Action::await_change());
    };

    let app_uid = app
        .metadata
        .uid
        .as_deref()
        .context(SparkApplicationUidNotFoundSnafu { name: app_name })?;

    if owner_uid::<v1alpha1::SparkApplication>(&pod.metadata)
        .is_some_and(|driver_owner_uid| driver_owner_uid != app_uid)
    {
        tracing::info!(
            "Driver pod {pod_name:?} belongs to a previous SparkApplication named {app_name:?}, skipping"
        );
        return Ok(Action::await_change());
    }

    update_status(&client, &app, phase).await?;

    if !is_finished(phase) {
        return Ok(Action::await_change());
    }

    let job = client
        .get_opt::<Job>(app_name, namespace)
        .await
        .context(GetJobSnafu { name: app_name })?;
    let retention = decide_retention(job.as_ref(), app_uid)?;
    tracing::info!(
        "Driver pod {pod_name:?} of SparkApplication {app_name:?} finished with phase {phase:?}, application objects: {objects}, driver: {driver}",
        objects = retention.objects,
        driver = retention.driver,
    );
    apply_to_app_objects(&client, &app, namespace, &retention.objects).await?;
    apply_to::<Pod>(&client, namespace, pod_name, &retention.driver).await?;
    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<Pod>>,
    error: &Error,
    _client: Arc<Client>,
) -> Action {
    match error {
        Error::InvalidPod { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}

fn is_finished(phase: &str) -> bool {
    matches!(phase, "Succeeded" | "Failed")
}

fn instance_label(pod: &Pod) -> Option<&str> {
    pod.metadata
        .labels
        .as_ref()?
        .get(LABEL_NAME_INSTANCE)
        .map(String::as_str)
}

fn owner_uid<K: Resource<DynamicType = ()>>(metadata: &ObjectMeta) -> Option<&str> {
    let (api_version, kind) = (K::api_version(&()), K::kind(&()));
    metadata
        .owner_references
        .iter()
        .flatten()
        .find(|owner| owner.api_version == api_version && owner.kind == kind)
        .map(|owner| owner.uid.as_str())
}

async fn update_status(
    client: &Client,
    app: &v1alpha1::SparkApplication,
    phase: &str,
) -> Result<()> {
    tracing::info!(
        "Update spark application [{app_name}] status to [{phase}]",
        app_name = app.name_any()
    );
    client
        .apply_patch_status(
            POD_DRIVER_CONTROLLER_NAME,
            app,
            &v1alpha1::SparkApplicationStatus {
                phase: phase.to_owned(),
                resolved_template_ref: app
                    .status
                    .as_ref()
                    .map(|s| s.resolved_template_ref.clone())
                    .unwrap_or_default(),
            },
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: app.name_any(),
        })?;
    Ok(())
}

#[derive(Debug)]
struct Retention {
    objects: ObjectAction,
    driver: ObjectAction,
}

/// Decides what happens to the application objects and the driver once the driver finished.
///
/// A Job TTL of 0 deletes the driver right away instead of handing it over. Besides a configured
/// `0s`, this is the TTL `ttl_seconds_after_finished` in `job.rs` forces for a fixed
/// `spark.kubernetes.driver.pod.name` with retries, so that the next attempt can reuse the name.
fn decide_retention(job: Option<&Job>, app_uid: &str) -> Result<Retention> {
    let Some(job) = job.filter(|job| {
        job.metadata.deletion_timestamp.is_none()
            && owner_uid::<v1alpha1::SparkApplication>(&job.metadata) == Some(app_uid)
    }) else {
        return Ok(Retention {
            objects: ObjectAction::Delete,
            driver: ObjectAction::Delete,
        });
    };

    let hand_over = ObjectAction::HandOver(
        OwnerReferenceBuilder::new()
            .initialize_from_resource(job)
            .build()
            .context(BuildJobOwnerReferenceSnafu)?,
    );
    let ttl_is_zero = job
        .spec
        .as_ref()
        .and_then(|spec| spec.ttl_seconds_after_finished)
        == Some(0);
    Ok(Retention {
        driver: if ttl_is_zero {
            ObjectAction::Delete
        } else {
            hand_over.clone()
        },
        objects: hand_over,
    })
}

#[derive(Clone, Debug)]
enum ObjectAction {
    HandOver(OwnerReference),
    Delete,
}

impl Display for ObjectAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HandOver(owner) => write!(f, "hand over to {} {:?}", owner.kind, owner.name),
            Self::Delete => f.write_str("delete"),
        }
    }
}

async fn apply_to_app_objects(
    client: &Client,
    app: &v1alpha1::SparkApplication,
    namespace: &str,
    action: &ObjectAction,
) -> Result<()> {
    let app_name = app.name_any();
    apply_to::<ServiceAccount>(client, namespace, &app_name, action).await?;
    apply_to::<RoleBinding>(client, namespace, &app_name, action).await?;
    for config_map in [
        app.submit_job_config_map_name(),
        app.pod_template_config_map_name(SparkApplicationRole::Driver),
        app.pod_template_config_map_name(SparkApplicationRole::Executor),
    ] {
        apply_to::<ConfigMap>(client, namespace, &config_map, action).await?;
    }
    Ok(())
}

async fn apply_to<K>(
    client: &Client,
    namespace: &str,
    name: &str,
    action: &ObjectAction,
) -> Result<()>
where
    K: Resource<Scope = NamespaceResourceScope, DynamicType = ()>
        + Clone
        + DeserializeOwned
        + Debug,
{
    let api = Api::<K>::namespaced(client.as_kube_client(), namespace);
    let kind = K::kind(&());
    match action {
        ObjectAction::HandOver(owner) => {
            let patch = json!({"metadata": { "ownerReferences": [owner] } });
            ignore_not_found(
                api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                    .await,
            )
            .context(HandOverSnafu { kind, name })
        }
        ObjectAction::Delete => ignore_not_found(api.delete(name, &DeleteParams::default()).await)
            .context(DeleteObjectSnafu { kind, name }),
    }
}

fn ignore_not_found<T>(
    result: Result<T, stackable_operator::kube::Error>,
) -> Result<(), stackable_operator::kube::Error> {
    match result {
        Ok(_) => Ok(()),
        Err(stackable_operator::kube::Error::Api(status)) if status.is_not_found() => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use stackable_operator::{
        k8s_openapi::{
            api::batch::v1::JobSpec, apimachinery::pkg::apis::meta::v1::Time, jiff::Timestamp,
        },
        kube::core::Status,
    };

    use super::*;

    const APP_UID: &str = "12345678-1234-1234-1234-123456789012";

    fn job(
        owner: Option<&str>,
        ttl_seconds_after_finished: Option<i32>,
        being_deleted: bool,
    ) -> Job {
        Job {
            metadata: ObjectMeta {
                name: Some("app".to_string()),
                namespace: Some("default".to_string()),
                uid: Some("87654321-4321-4321-4321-210987654321".to_string()),
                owner_references: owner.map(|uid| {
                    vec![OwnerReference {
                        api_version: v1alpha1::SparkApplication::api_version(&()).into_owned(),
                        kind: v1alpha1::SparkApplication::kind(&()).into_owned(),
                        name: "app".to_string(),
                        uid: uid.to_string(),
                        ..OwnerReference::default()
                    }]
                }),
                deletion_timestamp: being_deleted.then_some(Time(Timestamp::UNIX_EPOCH)),
                ..ObjectMeta::default()
            },
            spec: Some(JobSpec {
                ttl_seconds_after_finished,
                ..JobSpec::default()
            }),
            status: None,
        }
    }

    fn label(action: &ObjectAction) -> &'static str {
        match action {
            ObjectAction::HandOver(_) => "hand over",
            ObjectAction::Delete => "delete",
        }
    }

    #[rstest]
    #[case::missing(None, ("delete", "delete"))]
    #[case::default_ttl(Some(job(Some(APP_UID), Some(600), false)), ("hand over", "hand over"))]
    #[case::immediate(Some(job(Some(APP_UID), Some(0), false)), ("hand over", "delete"))]
    #[case::never(Some(job(Some(APP_UID), None, false)), ("hand over", "hand over"))]
    #[case::being_deleted(Some(job(Some(APP_UID), Some(600), true)), ("delete", "delete"))]
    #[case::other_instance(
        Some(job(Some("00000000-0000-0000-0000-000000000000"), Some(600), false)),
        ("delete", "delete")
    )]
    #[case::no_owner(Some(job(None, Some(600), false)), ("delete", "delete"))]
    fn retention(#[case] job: Option<Job>, #[case] expected: (&str, &str)) {
        let retention =
            decide_retention(job.as_ref(), APP_UID).expect("the Job has a name and a UID");

        assert_eq!(
            (label(&retention.objects), label(&retention.driver)),
            expected
        );
    }

    #[test]
    fn hand_over_points_at_the_job() {
        let job = job(Some(APP_UID), Some(600), false);

        let retention =
            decide_retention(Some(&job), APP_UID).expect("the Job has a name and a UID");
        let ObjectAction::HandOver(owner) = retention.objects else {
            panic!("expected the Job to take over");
        };

        assert_eq!(owner.kind, "Job");
        assert_eq!(Some(owner.name.as_str()), job.metadata.name.as_deref());
        assert_eq!(Some(owner.uid.as_str()), job.metadata.uid.as_deref());
        assert_eq!(owner.controller, None);
        assert_eq!(owner.block_owner_deletion, None);
    }

    #[rstest]
    #[case::ok(Ok(()), true)]
    #[case::not_found(Err((404, "NotFound")), true)]
    #[case::forbidden(Err((403, "Forbidden")), false)]
    #[case::conflict(Err((409, "Conflict")), false)]
    fn not_found_counts_as_done(#[case] result: Result<(), (u16, &str)>, #[case] done: bool) {
        let result = result.map_err(|(code, reason)| {
            stackable_operator::kube::Error::Api(Box::new(Status {
                code,
                reason: reason.to_string(),
                ..Status::default()
            }))
        });

        assert_eq!(ignore_not_found(result).is_ok(), done);
    }
}
