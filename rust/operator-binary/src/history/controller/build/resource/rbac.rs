//! Builds the RBAC resources (ServiceAccount + RoleBinding) shared by all role groups.

use stackable_operator::{
    k8s_openapi::api::{core::v1::ServiceAccount, rbac::v1::RoleBinding},
    v2::rbac,
};

use crate::history::controller::{
    build::recommended_labels_for_cluster_resources, validate::ValidatedSparkHistoryServer,
};

pub fn build_service_account(server: &ValidatedSparkHistoryServer) -> ServiceAccount {
    rbac::build_service_account(
        server,
        &server.cluster_resource_names(),
        recommended_labels_for_cluster_resources(server),
    )
}

pub fn build_role_binding(server: &ValidatedSparkHistoryServer) -> RoleBinding {
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
        history::controller::build::test_support::minimal_validated_cluster,
        test_support::app_version_label,
    };

    // `my-history` vs `spark-history`: see the swap-guard note on `HISTORY_YAML`.

    #[test]
    fn test_service_account() {
        let service_account = build_service_account(&minimal_validated_cluster());

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "ServiceAccount",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/instance": "my-history",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_history",
                        "app.kubernetes.io/name": "spark-history",
                        "app.kubernetes.io/version": app_version_label("3.5.8"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-history-serviceaccount",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "spark.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "SparkHistoryServer",
                            "name": "my-history",
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
                        "app.kubernetes.io/instance": "my-history",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_history",
                        "app.kubernetes.io/name": "spark-history",
                        "app.kubernetes.io/version": app_version_label("3.5.8"),
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-history-rolebinding",
                    "namespace": "default",
                    "ownerReferences": [
                        {
                            "apiVersion": "spark.stackable.tech/v1alpha1",
                            "controller": true,
                            "kind": "SparkHistoryServer",
                            "name": "my-history",
                            "uid": "12345678-1234-1234-1234-123456789012"
                        }
                    ]
                },
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "spark-history-clusterrole"
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "my-history-serviceaccount",
                        "namespace": "default"
                    }
                ]
            }),
            serde_json::to_value(role_binding).expect("must be serializable")
        );
    }
}
