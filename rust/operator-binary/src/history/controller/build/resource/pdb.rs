use stackable_operator::{
    commons::pdb::PdbConfig, k8s_openapi::api::policy::v1::PodDisruptionBudget,
    v2::builder::pdb::pod_disruption_budget_builder_with_role,
};

use crate::history::controller::validate::{
    CONTROLLER_NAME, NODE_ROLE_NAME, OPERATOR_NAME, PRODUCT_NAME, ValidatedSparkHistoryServer,
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
        &PRODUCT_NAME,
        &NODE_ROLE_NAME,
        &OPERATOR_NAME,
        &CONTROLLER_NAME,
    )
    .with_max_unavailable(max_unavailable)
    .build();

    Some(pdb)
}

fn max_unavailable_history_servers() -> u16 {
    1
}
