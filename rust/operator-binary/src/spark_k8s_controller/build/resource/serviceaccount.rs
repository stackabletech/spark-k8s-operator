use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::{
        Resource,
        api::{
            core::v1::ServiceAccount,
            rbac::v1::{ClusterRole, RoleBinding, RoleRef, Subject},
        },
    },
    v2::builder::meta::ownerreference_from_resource,
};

use crate::{
    crd::constants::*,
    spark_k8s_controller::{build::recommended_labels_for_cluster_resources, validate},
};

/// For a given SparkApplication, we create a ServiceAccount with a RoleBinding to the ClusterRole
/// that allows the driver to create pods etc.
/// Both objects have an owner reference to the SparkApplication, as well as the same name as the app.
/// They are deleted when the job is deleted.
pub(crate) fn build_spark_role_serviceaccount(
    validated: &validate::ValidatedSparkApplication,
) -> (ServiceAccount, RoleBinding) {
    let sa_name = validated.name.to_string();
    let sa = ServiceAccount {
        metadata: cluster_resource_object_meta(validated, &sa_name),
        ..ServiceAccount::default()
    };
    let binding_name = &sa_name;
    let binding = RoleBinding {
        metadata: cluster_resource_object_meta(validated, binding_name),
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

/// Object metadata for a cluster-shared resource named `name`, owned by the SparkApplication.
///
/// Unlike [`crate::spark_k8s_controller::build::object_meta`], the labels carry no
/// `app.kubernetes.io/component` label because the RBAC resources are not tied to a component.
fn cluster_resource_object_meta(
    validated: &validate::ValidatedSparkApplication,
    name: impl Into<String>,
) -> stackable_operator::k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
    ObjectMetaBuilder::new()
        .namespace(validated.namespace.clone())
        .name(name)
        .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
        .with_labels(recommended_labels_for_cluster_resources(validated))
        .build()
}
