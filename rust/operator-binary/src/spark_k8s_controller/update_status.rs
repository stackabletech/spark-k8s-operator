//! The update_status step in the SparkApplication controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{client::Client, kube::ResourceExt};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::{
        DriverPod,
        constants::{POD_DRIVER_CONTROLLER_NAME, SPARK_CONTROLLER_NAME},
        v1alpha1,
    },
    spark_k8s_controller::{Applied, SparkResources, validate::ValidatedSparkApplication},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("Failed to update status for application {name:?}"))]
    ApplySparkApplicationStatus {
        source: stackable_operator::client::Error,
        name: String,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// The phase of an application whose driver was not observed yet.
const INITIAL_PHASE: &str = "Unknown";

/// Patches the initial status onto the [`v1alpha1::SparkApplication`] immediately after the Job
/// was applied (fix for #457): a non-empty status makes `k8s_job_has_been_created` skip later
/// reconcile runs, ensuring the Job is not created again after being recycled by Kubernetes.
/// The phase itself is maintained by the pod-driver controller from here on.
///
/// Takes [`SparkResources<Applied>`] purely as proof of ordering: nothing is read from it, but
/// requiring it means this patch can only happen after the apply step.
pub async fn update_status(
    client: &Client,
    validated: &ValidatedSparkApplication,
    _applied: &SparkResources<Applied>,
) -> Result<()> {
    let spark_application = &validated.spark_application;

    let status = v1alpha1::SparkApplicationStatus {
        phase: INITIAL_PHASE.to_string(),
        resolved_template_ref: validated.cluster_config.resolved_template_refs.clone(),
        effective_cleanup: Some(spark_application.effective_cleanup()),
        driver_pod: None,
    };

    client
        .apply_patch_status(SPARK_CONTROLLER_NAME, spark_application, &status)
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(())
}

/// Sets the `phase` of the [`v1alpha1::SparkApplication`] and the `driverPod` it comes from,
/// leaving the rest of its status as is.
///
/// Used by the pod-driver controller, which has no validated application to patch the initial
/// status from, to mirror the phase of the driver. Only these two fields are applied, so the
/// pod-driver controller owns only them and cannot write back a stale copy of the fields the
/// SparkApplication controller computes.
pub async fn update_phase(
    client: &Client,
    spark_application: &v1alpha1::SparkApplication,
    phase: &str,
    driver_pod: &DriverPod,
) -> Result<()> {
    client
        .apply_patch_status(
            POD_DRIVER_CONTROLLER_NAME,
            spark_application,
            &serde_json::json!({ "phase": phase, "driverPod": driver_pod }),
        )
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(())
}
