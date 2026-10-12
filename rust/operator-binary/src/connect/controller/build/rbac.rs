//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by the whole cluster.

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    v2::rbac,
};

use crate::connect::controller::{
    build::recommended_labels_for_cluster_resources, validate::ValidatedSparkConnectServer,
};

pub fn build_service_account(server: &ValidatedSparkConnectServer) -> ServiceAccount {
    rbac::build_service_account(
        server,
        &server.cluster_resource_names(),
        recommended_labels_for_cluster_resources(server),
    )
}

pub fn build_role_binding(server: &ValidatedSparkConnectServer) -> RoleBinding {
    rbac::build_role_binding(
        server,
        &server.cluster_resource_names(),
        recommended_labels_for_cluster_resources(server),
    )
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
                    "labels": {
                        "app.kubernetes.io/instance": "my-connect",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_connect",
                        "app.kubernetes.io/name": "spark-connect",
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
                        "app.kubernetes.io/instance": "my-connect",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_connect",
                        "app.kubernetes.io/name": "spark-connect",
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
