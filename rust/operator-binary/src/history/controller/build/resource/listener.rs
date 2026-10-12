use std::str::FromStr;

use stackable_operator::{
    crd::listener,
    v2::types::{
        kubernetes::{ListenerClassName, ListenerName},
        operator::{ClusterName, RoleName},
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

/// The returned ListenerName is a lowercase RFC 1035 label name (checked by a unit test).
pub(crate) fn group_listener_name(
    validated: &validate::ValidatedSparkHistoryServer,
    role_name: &RoleName,
) -> ListenerName {
    const _: () = assert!(
        ClusterName::MAX_LENGTH + 1 /* dash */ + RoleName::MAX_LENGTH <= ListenerName::MAX_LENGTH,
        "The string `<cluster_name>-<role_name>` must not exceed the limit of Listener names."
    );
    // Both halves are RFC 1123 labels joined by a dash, which is a valid RFC 1123 subdomain.
    let _ = ClusterName::IS_RFC_1123_SUBDOMAIN_NAME;
    let _ = RoleName::IS_RFC_1123_LABEL_NAME;

    ListenerName::from_str(&format!(
        "{cluster}-{role}",
        cluster = validated.name,
        role = role_name
    ))
    .expect("The role listener name is a valid Listener name.")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::controller::{
        build::test_support::minimal_validated_cluster, validate::NODE_ROLE_NAME,
    };

    #[test]
    fn group_listener_name_is_rfc_1035_label_name() {
        // Every ClusterName is a valid RFC 1035 label name, so we use just some string with maximum
        // length.
        let _ = ClusterName::IS_RFC_1035_LABEL_NAME;
        let mut validated = minimal_validated_cluster();
        validated.name = ClusterName::from_str(&"a".repeat(ClusterName::MAX_LENGTH))
            .expect("is a valid ClusterName");

        // The history server has a single role.
        let group_listener_name = group_listener_name(&validated, &NODE_ROLE_NAME);
        assert!(
            stackable_operator::validation::is_lowercase_rfc_1035_label(
                group_listener_name.as_ref()
            )
            .is_ok()
        );
    }
}
