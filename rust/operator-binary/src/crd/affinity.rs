use stackable_operator::{
    commons::affinity::{StackableAffinityFragment, affinity_between_role_pods},
    k8s_openapi::api::core::v1::PodAntiAffinity,
};

use crate::crd::constants::{APP_NAME, HISTORY_ROLE_NAME};

pub fn history_affinity(cluster_name: &str) -> StackableAffinityFragment {
    let affinity_between_role_pods =
        affinity_between_role_pods(APP_NAME, cluster_name, HISTORY_ROLE_NAME, 70);

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

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use stackable_operator::{
        cli::OperatorEnvironmentOptions,
        commons::{affinity::StackableAffinity, tls_verification::TlsClientDetails},
        crd::s3,
        k8s_openapi::{
            api::core::v1::{PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm},
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
    };

    use crate::{
        crd::{
            constants::HISTORY_ROLE_NAME,
            history::v1alpha1,
            logdir::{ResolvedLogDir, S3LogDir},
        },
        history::controller::{
            dereference::DereferencedSparkHistoryServer,
            validate::{ValidatedSparkHistoryServer, validate},
        },
    };

    #[test]
    pub fn test_history_affinity_defaults() {
        let input = r#"
        apiVersion: spark.stackable.tech/v1alpha1
        kind: SparkHistoryServer
        metadata:
          name: spark-history
          namespace: default
          uid: 12345678-1234-1234-1234-123456789012
        spec:
          image:
            productVersion: 3.5.8
          logFileDirectory:
            s3:
              prefix: eventlogs/
              bucket:
                reference: spark-history-s3-bucket
          nodes:
            roleGroups:
              default:
                replicas: 1
                config:
                  cleaner: true
        "#;

        let deserializer = serde_yaml::Deserializer::from_str(input);
        let history: v1alpha1::SparkHistoryServer =
            serde_yaml::with::singleton_map_recursive::deserialize(deserializer).unwrap();
        let expected: StackableAffinity = StackableAffinity {
            node_affinity: None,
            node_selector: None,
            pod_affinity: None,
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    (
                                        "app.kubernetes.io/name".to_string(),
                                        "spark-k8s".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "spark-history".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/component".to_string(),
                                        HISTORY_ROLE_NAME.to_string(),
                                    ),
                                ])),
                            }),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..PodAffinityTerm::default()
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let validated: ValidatedSparkHistoryServer = validate(
            &history,
            DereferencedSparkHistoryServer {
                log_dir: ResolvedLogDir::S3(S3LogDir {
                    bucket: s3::v1alpha1::ResolvedBucket {
                        bucket_name: "my-bucket".to_string(),
                        connection: s3::v1alpha1::ConnectionSpec {
                            host: "my-s3".to_string().try_into().unwrap(),
                            port: None,
                            access_style: Default::default(),
                            credentials: None,
                            tls: TlsClientDetails { tls: None },
                            region: Default::default(),
                        },
                    },
                    prefix: "prefix".to_string(),
                }),
            },
            &OperatorEnvironmentOptions {
                operator_namespace: "default".to_string(),
                operator_service_name: "spark-k8s-operator".to_string(),
                image_repository: "oci.stackable.tech/sdp".to_string(),
            },
        )
        .expect("validation should succeed");

        let affinity = validated
            .role_groups
            .get(&"default".parse().expect("valid role group name"))
            .expect("default role group should exist")
            .config
            .config
            .affinity
            .clone();

        assert_eq!(affinity, expected);
    }
}
