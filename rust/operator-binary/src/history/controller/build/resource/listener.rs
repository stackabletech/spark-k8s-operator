use std::str::FromStr;

use stackable_operator::{
    crd::listener,
    kube::ResourceExt,
    v2::types::{kubernetes::ListenerClassName, operator::RoleGroupName},
};

use crate::{
    crd::{constants::HISTORY_UI_PORT, listener_ext},
    history::controller::validate,
};

pub(crate) fn build_group_listener(
    validated: &validate::ValidatedSparkHistoryServer,
    role: &str,
    listener_class: ListenerClassName,
) -> listener::v1alpha1::Listener {
    let listener_name = group_listener_name(validated, role);

    // Group listeners are shared across role groups, so the role-group label is "none" (preserving
    // the previous behaviour).
    let recommended_object_labels = validated.recommended_labels(
        &RoleGroupName::from_str("none").expect("\"none\" is a valid role group name"),
    );

    let listener_ports = [listener::v1alpha1::ListenerPort {
        name: "http".to_string(),
        port: HISTORY_UI_PORT.into(),
        protocol: Some("TCP".to_string()),
    }];

    listener_ext::build_listener(
        validated,
        &listener_name,
        &listener_class,
        recommended_object_labels,
        &listener_ports,
    )
}

pub(crate) fn group_listener_name(
    validated: &validate::ValidatedSparkHistoryServer,
    role: &str,
) -> String {
    format!("{cluster}-{role}", cluster = validated.name_any())
}
