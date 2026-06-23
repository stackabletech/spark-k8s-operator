use stackable_operator::{
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kube::ResourceExt,
    v2::builder::service::{Scheme, Scraping, prometheus_annotations, prometheus_labels},
};

use crate::connect::{
    GRPC, HTTP,
    common::SparkConnectRole,
    controller::validate::ValidatedSparkConnectServer,
    crd::{CONNECT_GRPC_PORT, CONNECT_UI_PORT, v1alpha1},
};

// This is the headless driver service used for the internal
// communication with the executors as recommended by the Spark docs.
pub(crate) fn build_headless_service(
    validated: &ValidatedSparkConnectServer,
    scs: &v1alpha1::SparkConnectServer,
) -> Service {
    let service_name = format!(
        "{cluster}-{role}-headless",
        cluster = scs.name_any(),
        role = SparkConnectRole::Server
    );

    let selector = validated.role_selector(SparkConnectRole::Server).into();

    Service {
        metadata: validated
            .object_meta(service_name, SparkConnectRole::Server)
            .build(),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_owned()),
            cluster_ip: Some("None".to_owned()),
            ports: Some(vec![
                ServicePort {
                    name: Some(String::from(GRPC)),
                    port: CONNECT_GRPC_PORT.into(),
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some(String::from(HTTP)),
                    port: CONNECT_UI_PORT.into(),
                    ..ServicePort::default()
                },
            ]),
            selector: Some(selector),
            // The flag `publish_not_ready_addresses` *must* be `true` to allow for readiness
            // probes. Without it, the driver runs into a deadlock beacuse the Pod cannot become
            // "ready" until the Service is "ready" and vice versa.
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

// This is the metrics service
pub(crate) fn build_metrics_service(
    validated: &ValidatedSparkConnectServer,
    scs: &v1alpha1::SparkConnectServer,
) -> Service {
    let service_name = format!(
        "{cluster}-{role}-metrics",
        cluster = scs.name_any(),
        role = SparkConnectRole::Server
    );

    let selector = validated.role_selector(SparkConnectRole::Server).into();

    Service {
        metadata: validated
            .object_meta(service_name, SparkConnectRole::Server)
            .with_labels(prometheus_labels(&Scraping::Enabled))
            .with_annotations(prometheus_annotations(
                &Scraping::Enabled,
                &Scheme::Http,
                "/metrics/prometheus",
                &CONNECT_UI_PORT,
            ))
            .build(),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_owned()),
            cluster_ip: Some("None".to_owned()),
            ports: Some(metrics_ports()),
            selector: Some(selector),
            // The flag `publish_not_ready_addresses` *must* be `true` to allow for readiness
            // probes. Without it, the driver runs into a deadlock beacuse the Pod cannot become
            // "ready" until the Service is "ready" and vice versa.
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    }
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some("metrics".to_string()),
        port: CONNECT_UI_PORT.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}
