//! The update_status step in the SparkApplication controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{client::Client, kube::ResourceExt};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::{constants::SPARK_CONTROLLER_NAME, v1alpha1},
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
        phase: "Unknown".to_string(),
        resolved_template_ref: validated.cluster_config.resolved_template_refs.clone(),
    };

    client
        .apply_patch_status(SPARK_CONTROLLER_NAME, spark_application, &status)
        .await
        .with_context(|_| ApplySparkApplicationStatusSnafu {
            name: spark_application.name_any(),
        })?;

    Ok(())
}
