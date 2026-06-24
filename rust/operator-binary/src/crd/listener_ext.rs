use stackable_operator::{
    builder::meta::ObjectMetaBuilder,
    crd::listener,
    kube::Resource,
    kvp::Labels,
    v2::{
        HasName, HasUid, builder::meta::ownerreference_from_resource,
        types::kubernetes::ListenerClassName,
    },
};

// TODO (@NickLarsenNZ): Move this functionality to stackable-operator
pub fn build_listener<T: Resource<DynamicType = ()> + HasName + HasUid>(
    resource: &T,
    listener_name: &str,
    listener_class: &ListenerClassName,
    listener_labels: Labels,
    listener_ports: &[listener::v1alpha1::ListenerPort],
) -> listener::v1alpha1::Listener {
    listener::v1alpha1::Listener {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(resource)
            .name(listener_name)
            .ownerreference(ownerreference_from_resource(resource, None, Some(true)))
            .with_labels(listener_labels)
            .build(),
        spec: listener::v1alpha1::ListenerSpec {
            class_name: Some(listener_class.to_string()),
            ports: Some(listener_ports.to_vec()),
            ..listener::v1alpha1::ListenerSpec::default()
        },
        status: None,
    }
}
