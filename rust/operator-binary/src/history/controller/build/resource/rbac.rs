//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use std::str::FromStr;

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    kvp::Labels,
    v2::{
        rbac,
        types::operator::{RoleGroupName, RoleName},
    },
};

use crate::history::controller::validate::ValidatedSparkHistoryServer;

stackable_operator::constant!(NONE_ROLE_NAME: RoleName = "none");
stackable_operator::constant!(NONE_ROLE_GROUP_NAME: RoleGroupName = "none");

pub fn build_service_account(server: &ValidatedSparkHistoryServer) -> ServiceAccount {
    rbac::build_service_account(
        server,
        &server.cluster_resource_names(),
        rbac_labels(server),
    )
}

pub fn build_role_binding(server: &ValidatedSparkHistoryServer) -> RoleBinding {
    rbac::build_role_binding(
        server,
        &server.cluster_resource_names(),
        rbac_labels(server),
    )
}

fn rbac_labels(server: &ValidatedSparkHistoryServer) -> Labels {
    server.recommended_labels_for(&NONE_ROLE_NAME, &NONE_ROLE_GROUP_NAME)
}
