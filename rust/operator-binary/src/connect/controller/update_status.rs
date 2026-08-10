//! The update_status step in the SparkConnectServer controller.

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    connect::{
        controller::{Applied, SparkConnectResources},
        crd::{SparkConnectServerStatus, v1alpha1::SparkConnectServer},
    },
    crd::constants::OPERATOR_NAME,
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Computes the cluster status from the applied resources and patches it onto the
/// [`SparkConnectServer`]. Takes [`SparkConnectResources<Applied>`] so the type system proves
/// the status derives from applied resources, not merely built ones.
pub async fn update_status(
    client: &Client,
    connect_server: &SparkConnectServer,
    applied: &SparkConnectResources<Applied>,
) -> Result<()> {
    // TODO: This StatefulSet only contains the driver. We should probably also
    // consider the state of the executors to determine if the
    // SparkConnectServer is ready. This depends on the availability and
    // resilience properties of Spark and could e.g. be "driver and more than
    // 75% of the executors ready". Special care needs to be taken about
    // auto-scaling executors in this case (if/once supported).
    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    for stateful_set in &applied.stateful_sets {
        ss_cond_builder.add(stateful_set.clone());
    }

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&connect_server.spec.cluster_operation);

    let status = SparkConnectServerStatus {
        conditions: compute_conditions(
            connect_server,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, connect_server, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(())
}
