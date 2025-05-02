use snafu::{ResultExt, Snafu};
use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    commons::listener::{Listener, ListenerPort, ListenerSpec},
    kube::Resource,
    kvp::ObjectLabels,
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

pub fn build_listener<T: Resource<DynamicType = ()>>(
    resource: &T,
    listener_name: &str,
    listener_class: &str,
    listener_labels: ObjectLabels<T>,
    listener_ports: &[ListenerPort],
) -> Result<Listener, Error> {
    Ok(Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(resource)
            .name(listener_name)
            .ownerreference_from_resource(resource, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(listener_labels)
            .context(ObjectMetaSnafu)?
            .build(),
        spec: ListenerSpec {
            class_name: Some(listener_class.into()),
            ports: Some(listener_ports.to_vec()),
            ..ListenerSpec::default()
        },
        status: None,
    })
}
