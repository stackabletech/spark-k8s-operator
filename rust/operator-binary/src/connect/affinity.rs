use stackable_operator::{
    commons::affinity::{affinity_between_role_pods, StackableAffinityFragment},
    k8s_openapi::api::core::v1::PodAntiAffinity,
};

use crate::connect::crd::CONNECT_ROLE_NAME;
use crate::crd::constants::APP_NAME;

pub fn affinity(cluster_name: &str) -> StackableAffinityFragment {
    let affinity_between_role_pods =
        affinity_between_role_pods(APP_NAME, cluster_name, CONNECT_ROLE_NAME, 70);

    StackableAffinityFragment {
        pod_affinity: None,
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods,
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}
