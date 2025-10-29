use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec},
    kvp::{Annotations, Labels},
    role_utils::RoleGroupRef,
};

use crate::{
    crd::{
        constants::{HISTORY_APP_NAME, METRICS_PORT},
        history::v1alpha1,
    },
    history::recommended_labels,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },
}

/// The rolegroup metrics [`Service`] is a service that exposes metrics and a prometheus scraping label
pub fn build_rolegroup_metrics_service(
    shs: &v1alpha1::SparkHistoryServer,
    resolved_product_image: &ResolvedProductImage,
    rolegroup_ref: &RoleGroupRef<v1alpha1::SparkHistoryServer>,
) -> Result<Service, Error> {
    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(shs)
            .name(rolegroup_ref.rolegroup_metrics_service_name())
            .ownerreference_from_resource(shs, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(recommended_labels(
                shs,
                &resolved_product_image.app_version_label_value,
                &rolegroup_ref.role_group,
            ))
            .context(MetadataBuildSnafu)?
            .with_labels(prometheus_labels())
            .with_annotations(prometheus_annotations())
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(metrics_ports()),
            selector: Some(
                Labels::role_group_selector(
                    shs,
                    HISTORY_APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )
                .context(LabelBuildSnafu)?
                .into(),
            ),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn metrics_ports() -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some("metrics".to_string()),
        port: METRICS_PORT.into(),
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
        ("prometheus.io/path".to_owned(), "/metrics".to_owned()),
        ("prometheus.io/port".to_owned(), METRICS_PORT.to_string()),
        ("prometheus.io/scheme".to_owned(), "http".to_owned()),
        ("prometheus.io/scrape".to_owned(), "true".to_owned()),
    ])
    .expect("should be valid annotations")
}
