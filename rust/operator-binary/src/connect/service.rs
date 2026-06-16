use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kube::ResourceExt,
    kvp::{Annotations, Labels},
    v2::builder::meta::ownerreference_from_resource,
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
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(service_name)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(validated.recommended_labels(SparkConnectRole::Server))
            .build(),
        spec: Some(ServiceSpec {
            type_: Some("ClusterIP".to_owned()),
            cluster_ip: Some("None".to_owned()),
            ports: Some(vec![
                ServicePort {
                    name: Some(String::from(GRPC)),
                    port: CONNECT_GRPC_PORT,
                    ..ServicePort::default()
                },
                ServicePort {
                    name: Some(String::from(HTTP)),
                    port: CONNECT_UI_PORT,
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
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(scs)
            .name(service_name)
            .ownerreference(ownerreference_from_resource(validated, None, Some(true)))
            .with_labels(validated.recommended_labels(SparkConnectRole::Server))
            .with_labels(prometheus_labels())
            .with_annotations(prometheus_annotations())
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
        port: CONNECT_UI_PORT,
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

/// Common labels for Prometheus
fn prometheus_labels() -> Labels {
    Labels::try_from([("prometheus.io/scrape", "true")]).expect("should be a valid label")
}

/// Common annotations for Prometheus
///
/// These annotations can be used in a ServiceMonitor.
///
/// see also <https://github.com/prometheus-community/helm-charts/blob/prometheus-27.32.0/charts/prometheus/values.yaml#L983-L1036>
fn prometheus_annotations() -> Annotations {
    Annotations::try_from([
        (
            "prometheus.io/path".to_owned(),
            "/metrics/prometheus".to_owned(),
        ),
        ("prometheus.io/port".to_owned(), CONNECT_UI_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}
