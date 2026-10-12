//! The apply step in the SparkApplication controller.
//!
//! Unlike the SparkConnectServer and SparkHistoryServer controllers, this step deliberately
//! does not use `ClusterResources`: a SparkApplication is a one-shot Job that is applied
//! exactly once (guarded by the status set in the update_status step), so there are no
//! orphaned resources from earlier reconcile runs to track and delete, and the
//! SparkApplication CRD has no `objectOverrides` field. The resources are applied with plain
//! server-side apply patches, exactly as before this step was extracted.

use std::marker::PhantomData;

use snafu::{ResultExt, Snafu};
use stackable_operator::client::Client;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::{
    crd::constants::SPARK_CONTROLLER_NAME,
    spark_k8s_controller::{Applied, Prepared, SparkResources},
};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("failed to apply role ServiceAccount"))]
    ApplyServiceAccount {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply global RoleBinding"))]
    ApplyRoleBinding {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to apply Job"))]
    ApplyApplication {
        source: stackable_operator::client::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

/// Applier for the Kubernetes resource specifications produced by this controller.
pub struct Applier<'a> {
    client: &'a Client,
}

impl<'a> Applier<'a> {
    pub fn new(client: &'a Client) -> Applier<'a> {
        Applier { client }
    }

    /// Applies the given Kubernetes resources and marks them as applied.
    pub async fn apply(
        self,
        resources: SparkResources<Prepared>,
    ) -> Result<SparkResources<Applied>> {
        // Destructured without `..`, so adding a field to [`SparkResources`] fails to
        // compile here instead of silently never being applied.
        let SparkResources {
            service_accounts,
            role_bindings,
            config_maps,
            jobs,
            status: _,
        } = resources;

        // Apply the ServiceAccount and RoleBinding first, then the ConfigMaps, and finally the
        // Job: the Job runs under the ServiceAccount and mounts the ConfigMaps, so they must
        // exist first.
        let mut applied_service_accounts = vec![];
        for service_account in service_accounts {
            applied_service_accounts.push(
                self.client
                    .apply_patch(SPARK_CONTROLLER_NAME, &service_account, &service_account)
                    .await
                    .context(ApplyServiceAccountSnafu)?,
            );
        }

        let mut applied_role_bindings = vec![];
        for role_binding in role_bindings {
            applied_role_bindings.push(
                self.client
                    .apply_patch(SPARK_CONTROLLER_NAME, &role_binding, &role_binding)
                    .await
                    .context(ApplyRoleBindingSnafu)?,
            );
        }

        let mut applied_config_maps = vec![];
        for config_map in config_maps {
            applied_config_maps.push(
                self.client
                    .apply_patch(SPARK_CONTROLLER_NAME, &config_map, &config_map)
                    .await
                    .context(ApplyApplicationSnafu)?,
            );
        }

        let mut applied_jobs = vec![];
        for job in jobs {
            applied_jobs.push(
                self.client
                    .apply_patch(SPARK_CONTROLLER_NAME, &job, &job)
                    .await
                    .context(ApplyApplicationSnafu)?,
            );
        }

        Ok(SparkResources {
            service_accounts: applied_service_accounts,
            role_bindings: applied_role_bindings,
            config_maps: applied_config_maps,
            jobs: applied_jobs,
            status: PhantomData,
        })
    }
}
