use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    v2::{
        builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
        types::operator::RoleGroupName,
    },
};

use crate::{
    crd::constants::METRICS_PORT,
    history::controller::{
        build::{object_meta, role_group_selector},
        validate::ValidatedSparkHistoryServer,
    },
};

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    validated: &ValidatedSparkHistoryServer,
    role_group_name: &RoleGroupName,
) -> Service {
    Service {
        metadata: object_meta(
            validated,
            validated
                .role_group_resource_names(role_group_name)
                .metrics_service_name()
                .to_string(),
            role_group_name,
        )
        .with_labels(prometheus_labels(&Scraping::Enabled))
        .with_annotations(prometheus_annotations(
            &Scraping::Enabled,
            &Scheme::Http,
            "/metrics",
            &METRICS_PORT,
        ))
        .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(role_group_selector(validated, role_group_name).into()),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some("metrics".to_string()),
        port: METRICS_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use stackable_operator::v2::types::operator::RoleGroupName;

    use super::*;
    use crate::{
        history::controller::build::test_support::minimal_validated_cluster,
        test_support::app_version_label,
    };

    /// Every metrics Service must carry the Prometheus scrape label and the
    /// `prometheus.io/path|port|scheme|scrape` annotations, or Prometheus stops discovering the
    /// endpoints.
    #[test]
    fn test_rolegroup_metrics_service() {
        let validated = minimal_validated_cluster();
        let role_group_name: RoleGroupName = "default".parse().expect("valid role group name");

        let service = build_rolegroup_metrics_service(&validated, &role_group_name);

        assert_eq!(
            json!({
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {
                    "annotations": {
                        "prometheus.io/path": "/metrics",
                        "prometheus.io/port": "18081",
                        "prometheus.io/scheme": "http",
                        "prometheus.io/scrape": "true"
                    },
                    "labels": {
                        "app.kubernetes.io/component": "node",
                        "app.kubernetes.io/instance": "my-history",
                        "app.kubernetes.io/managed-by": "spark.stackable.tech_history",
                        "app.kubernetes.io/name": "spark-history",
                        "app.kubernetes.io/role-group": "default",
                        "app.kubernetes.io/version": app_version_label("3.5.8"),
                        "prometheus.io/scrape": "true",
                        "stackable.tech/vendor": "Stackable"
                    },
                    "name": "my-history-node-default-metrics",
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
                "spec": {
                    "clusterIP": "None",
                    "ports": [
                        {
                            "name": "metrics",
                            "port": 18081,
                            "protocol": "TCP"
                        }
                    ],
                    "publishNotReadyAddresses": true,
                    "selector": {
                        "app.kubernetes.io/component": "node",
                        "app.kubernetes.io/instance": "my-history",
                        "app.kubernetes.io/name": "spark-history",
                        "app.kubernetes.io/role-group": "default"
                    },
                    "type": "ClusterIP"
                }
            }),
            serde_json::to_value(service).expect("must be serializable")
        );
    }
}
