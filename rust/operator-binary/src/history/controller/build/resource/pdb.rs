use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::history::controller::validate::{
    ValidatedSparkHistoryServer, controller_name, operator_name, product_name,
};

/// Builds the [`PodDisruptionBudget`] for the history server role, or `None` if PDBs are disabled.
pub fn build_pdb(
    pdb: &PdbConfig,
    validated: &ValidatedSparkHistoryServer,
) -> Option<PodDisruptionBudget> {
    if !pdb.enabled {
        return None;
    }
    let max_unavailable = pdb
        .max_unavailable
        .unwrap_or(max_unavailable_history_servers());
    let pdb = pod_disruption_budget_builder_with_role(
        validated,
        &product_name(),
        &ValidatedSparkHistoryServer::role_name(),
        &operator_name(),
        &controller_name(),
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_history_servers() -> u16 {
    1
}
