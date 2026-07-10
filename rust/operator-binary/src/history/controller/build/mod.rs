pub mod resource;

use stackable_operator::k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding};

use crate::{
    crd::constants::HISTORY_ROLE_NAME,
    history::controller::{
        Error, SparkHistoryResources,
        build::resource::{
            config_map::build_config_map, listener::build_group_listener, pdb::build_pdb,
            service::build_rolegroup_metrics_service, statefulset::build_stateful_set,
        },
        validate::ValidatedSparkHistoryServer,
    },
};

/// Builds every Kubernetes resource for the given validated SparkHistoryServer.
pub fn build(
    validated: &ValidatedSparkHistoryServer,
    service_account: ServiceAccount,
    role_binding: RoleBinding,
) -> Result<SparkHistoryResources, Error> {
    let log_dir = &validated.cluster_config.log_dir;

    let mut config_maps = vec![];
    let mut metrics_services = vec![];
    let mut stateful_sets = vec![];

    for (role_group_name, rg) in &validated.role_groups {
        config_maps.push(build_config_map(validated, role_group_name, rg)?);
        metrics_services.push(build_rolegroup_metrics_service(validated, role_group_name));
        stateful_sets.push(build_stateful_set(
            validated,
            role_group_name,
            rg,
            log_dir,
            &service_account,
        )?);
    }

    let listener = build_group_listener(
        validated,
        HISTORY_ROLE_NAME,
        validated.role_config.listener_class.clone(),
    );

    let pod_disruption_budget = build_pdb(&validated.role_config.pdb, validated);

    Ok(SparkHistoryResources {
        service_account,
        role_binding,
        config_maps,
        metrics_services,
        stateful_sets,
        listener,
        pod_disruption_budget,
    })
}
