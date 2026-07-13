use stackable_operator::k8s_openapi::{
    Resource,
    api::{
        core::v1::ServiceAccount,
        rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
    },
};

use crate::{crd::constants::*, spark_k8s_controller::validate};

/// For a given SparkApplication, we create a ServiceAccount with a RoleBinding to the ClusterRole
/// that allows the driver to create pods etc.
/// Both objects have an owner reference to the SparkApplication, as well as the same name as the app.
/// They are deleted when the job is deleted.
pub(crate) fn build_spark_role_serviceaccount(
    validated: &validate::ValidatedSparkApplication,
) -> (ServiceAccount, RoleBinding) {
    let sa_name = validated.name.to_string();
    let sa = ServiceAccount {
        metadata: validated.object_meta(&sa_name, "service-account").build(),
        ..ServiceAccount::default()
    };
    let binding_name = &sa_name;
    let binding = RoleBinding {
        metadata: validated.object_meta(binding_name, "role-binding").build(),
        role_ref: RoleRef {
            api_group: Some(ClusterRole::GROUP.to_string()),
            kind: ClusterRole::KIND.to_string(),
            name: SPARK_CLUSTER_ROLE.to_string(),
        },
        subjects: Some(vec![Subject {
            api_group: Some(ServiceAccount::GROUP.to_string()),
            kind: ServiceAccount::KIND.to_string(),
            name: sa_name,
            namespace: sa.metadata.namespace.clone(),
        }]),
    };
    (sa, binding)
}
