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

use crate::connect::controller::validate::ValidatedSparkConnectServer;

stackable_operator::constant!(NONE_ROLE_NAME: RoleName = "none");
stackable_operator::constant!(NONE_ROLE_GROUP_NAME: RoleGroupName = "none");

pub fn build_service_account(server: &ValidatedSparkConnectServer) -> ServiceAccount {
    rbac::build_service_account(
        server,
        &server.cluster_resource_names(),
        rbac_labels(server),
    )
}

pub fn build_role_binding(server: &ValidatedSparkConnectServer) -> RoleBinding {
    rbac::build_role_binding(
        server,
        &server.cluster_resource_names(),
        rbac_labels(server),
    )
}

fn rbac_labels(server: &ValidatedSparkConnectServer) -> Labels {
    server.recommended_labels_for(&NONE_ROLE_NAME, &NONE_ROLE_GROUP_NAME)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        connect::controller::build::test_support::minimal_validated_cluster,
        test_support::app_version_label,
    };

    // `my-connect` vs `spark-connect`: see the swap-guard note on `CONNECT_YAML`.

    #[test]
    fn test_service_account() {
        let service_account = build_service_account(&minimal_validated_cluster());

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    // The RBAC resources are cluster-shared, so role and role group are `none`.
                    "labels": {
                        "app.kubernetes.io/component": "none",
                        "app.kubernetes.io/instance": "my-connect",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_connect",
                        "app.kubernetes.io/name": "spark-connect",
                        "app.kubernetes.io/role-group": "none",
                        "app.kubernetes.io/version": app_version_label("4.1.2"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-connect-serviceaccount",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "spark.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "SparkConnectServer",
                            "name": "my-connect",
                            "uid": "12345678-1234-1234-1234-123456789012"
                        }
                    ]
                }
            }),
            serde_json::to_value(service_account).expect("must be serializable")
        );
    }

    #[test]
    fn test_role_binding() {
        let role_binding = build_role_binding(&minimal_validated_cluster());

        assert_eq!(
            json!({
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "RoleBinding",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/component": "none",
                        "app.kubernetes.io/instance": "my-connect",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_connect",
                        "app.kubernetes.io/name": "spark-connect",
                        "app.kubernetes.io/role-group": "none",
                        "app.kubernetes.io/version": app_version_label("4.1.2"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-connect-rolebinding",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "spark.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "SparkConnectServer",
                            "name": "my-connect",
                            "uid": "12345678-1234-1234-1234-123456789012"
                        }
                    ]
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "spark-connect-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "my-connect-serviceaccount",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(role_binding).expect("must be serializable")
        );
    }
}
