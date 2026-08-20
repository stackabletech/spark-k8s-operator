//! The apply step in the SparkHistoryServer controller.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::{
    client::Client,
    cluster_resources::{ClusterResource, ClusterResourceApplyStrategy, ClusterResources},
    deep_merger::ObjectOverrides,
    v2::cluster_resources::cluster_resources_new,
};
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::history::controller::{
    Applied, Prepared, SparkHistoryResources,
    validate::{CONTROLLER_NAME, OPERATOR_NAME, PRODUCT_NAME, ValidatedSparkHistoryServer},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
pub enum Error {
    #[snafu(display("failed to apply Kubernetes resource"))]
    ApplyResource {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
        source: stackable_operator::cluster_resources::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
///
/// The implementation is not tied to this controller and could theoretically be moved to
/// stackable_operator if [`SparkHistoryResources`] would contain all possible resource types.
pub struct Applier<'a> {
    client: &'a Client,
    cluster_resources: ClusterResources<'a>,
}

impl<'a> Applier<'a> {
    pub fn new(
        client: &'a Client,
        cluster: &ValidatedSparkHistoryServer,
        apply_strategy: ClusterResourceApplyStrategy,
        object_overrides: &'a ObjectOverrides,
    ) -> Applier<'a> {
        let cluster_resources = cluster_resources_new(
            &PRODUCT_NAME,
            &OPERATOR_NAME,
            &CONTROLLER_NAME,
            &cluster.name,
            &cluster.namespace,
            &cluster.uid,
            apply_strategy,
            object_overrides,
        );

        Applier {
            client,
            cluster_resources,
        }
    }

    /// Applies the given Kubernetes resources, deletes resources from earlier reconcile runs
    /// that were not applied in this one, and marks the resources as applied.
    ///
    /// Consumes the applier: a resource applied after the orphan deletion would itself be
    /// treated as an orphan and deleted by the next reconcile run.
    pub async fn apply(
        mut self,
        resources: SparkHistoryResources<Prepared>,
    ) -> Result<SparkHistoryResources<Applied>> {
        // Destructured without `..`, so adding a field to [`SparkHistoryResources`] fails to
        // compile here instead of silently never being applied.
        let SparkHistoryResources {
            metrics_services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            stateful_sets,
            status: _,
        } = resources;

        // Apply order is: StatefulSets last (a changed mounted ConfigMap/Secret
        // must exist first, else Pods restart -- commons-operator#111). The ServiceAccount comes
        // first because the Pods reference it at creation time.
        let service_accounts = self.add_resources(service_accounts).await?;
        let role_bindings = self.add_resources(role_bindings).await?;
        let config_maps = self.add_resources(config_maps).await?;
        let metrics_services = self.add_resources(metrics_services).await?;
        let listeners = self.add_resources(listeners).await?;
        let pod_disruption_budgets = self.add_resources(pod_disruption_budgets).await?;
        let stateful_sets = self.add_resources(stateful_sets).await?;

        self.cluster_resources
            .delete_orphaned_resources(self.client)
            .await
            .context(DeleteOrphanedResourcesSnafu)?;

        Ok(SparkHistoryResources {
            stateful_sets,
            metrics_services,
            listeners,
            config_maps,
            pod_disruption_budgets,
            service_accounts,
            role_bindings,
            status: PhantomData,
        })
    }

    async fn add_resources<T: ClusterResource + Sync>(
        &mut self,
        resources: Vec<T>,
    ) -> Result<Vec<T>> {
        let mut applied_resources = vec![];

        for resource in resources {
            let applied_resource = self
                .cluster_resources
                .add(self.client, resource)
                .await
                .context(ApplyResourceSnafu)?;
            applied_resources.push(applied_resource);
        }

        Ok(applied_resources)
    }
}
