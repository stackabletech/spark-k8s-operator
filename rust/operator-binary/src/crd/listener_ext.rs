use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder, crd::listener, kube::Resource, kvp::ObjectLabels,
};
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build object meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },
}

// TODO (@NickLarsenNZ): Move this functionality to stackable-operator
pub fn build_listener<T: Resource<DynamicType = ()>>(
    resource: &T,
    listener_name: &str,
    listener_class: &str,
    listener_labels: ObjectLabels<T>,
    listener_ports: &[listener::v1alpha1::ListenerPort],
) -> Result<listener::v1alpha1::Listener, Error> {
    Ok(listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(resource)
            .name(listener_name)
            .ownerreference_from_resource(resource, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(listener_labels)
            .context(ObjectMetaSnafu)?
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class.into()),
            ports: Some(listener_ports.to_vec()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    })
}
