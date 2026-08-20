use std::str::FromStr;

use stackable_operator::{
    crd::listener,
    v2::types::{
        kubernetes::{ListenerClassName, ListenerName},
        operator::RoleName,
    },
};

use crate::{
    crd::{constants::HISTORY_UI_PORT, listener_ext},
    history::controller::{build::recommended_labels_for_role_resources, validate},
};

pub(crate) fn build_group_listener(
    validated: &validate::ValidatedSparkHistoryServer,
    role_name: &RoleName,
    listener_class: ListenerClassName,
) -> listener::v1alpha1::Listener {
    let listener_name = group_listener_name(validated, role_name);

    // Group listeners are shared across all role groups of the role, so they carry role-level
    // labels without a role group label.
    let recommended_object_labels = recommended_labels_for_role_resources(validated, role_name);

    let listener_ports = [listener::v1alpha1::ListenerPort {
        name: "http".to_string(),
        port: HISTORY_UI_PORT.into(),
        protocol: Some("TCP".to_string()),
    }];

    listener_ext::build_listener(
        validated,
        listener_name.as_ref(),
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
}

pub(crate) fn group_listener_name(
    validated: &validate::ValidatedSparkHistoryServer,
    role_name: &RoleName,
) -> ListenerName {
    ListenerName::from_str(&format!(
        "{cluster}-{role}",
        cluster = validated.name,
        role = role_name
    ))
    .expect(
        "the group listener name is a valid ListenerName, because a ClusterName is at most 40 \
         characters long and a RoleName is a RFC 1123 label of at most 63 characters, so the \
         joined name is a RFC 1123 DNS subdomain within the length limit",
    )
}
