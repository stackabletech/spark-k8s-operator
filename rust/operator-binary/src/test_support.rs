//! Shared helpers for the crate's tests.

use std::{fmt::Debug, future::Future, str::FromStr, sync::Arc};

use serde::de::DeserializeOwned;
use stackable_operator::{
    cli::OperatorEnvironmentOptions,
    client::Client,
    commons::networking::DomainName,
    kube::{Client as KubeClient, Config, core::DeserializeGuard, runtime::controller::Action},
    utils::cluster_info::KubernetesClusterInfo,
};

use crate::Ctx;

/// The expected `app.kubernetes.io/version` label value for the given product version.
///
/// The `-stackable` suffix carries the operator's own version, which is `0.0.0-dev` on main
/// but rewritten by the release process — so tests must derive it rather than hardcode it,
/// or they fail on release branches.
pub fn app_version_label(product_version: &str) -> String {
    format!(
        "{product_version}-stackable{}",
        crate::built_info::PKG_VERSION
    )
}

/// Asserts that `reconcile` returns [`Action::await_change`] for the object described by `yaml`
/// without ever reaching the Kubernetes API.
///
/// The client points at a closed port, so any API call would fail the reconciliation: an `Ok`
/// proves that the reconciler returned before touching the Kubernetes API, and - for a `yaml`
/// whose spec is invalid - before the [`DeserializeGuard`] was unwrapped. Callers pass a `yaml`
/// carrying a `deletionTimestamp` and an invalid `spec` to cover both.
pub fn assert_reconcile_exits_early<K, F, Fut, E>(yaml: &str, reconcile: F)
where
    DeserializeGuard<K>: DeserializeOwned,
    F: FnOnce(Arc<DeserializeGuard<K>>, Arc<Ctx>) -> Fut,
    Fut: Future<Output = Result<Action, E>>,
    E: Debug,
{
    let object: DeserializeGuard<K> = yaml_serde::from_str(yaml)
        .expect("YAML parses; the invalid spec is captured inside the DeserializeGuard");

    let action = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread tokio runtime")
        .block_on(async {
            let ctx = Arc::new(Ctx {
                client: Client::new(
                    KubeClient::try_from(Config::new(
                        "http://127.0.0.1:1".parse().expect("valid static URI"),
                    ))
                    .expect("client from static config"),
                    None,
                    "default".to_owned(),
                    KubernetesClusterInfo {
                        cluster_domain: DomainName::from_str("cluster.local")
                            .expect("valid cluster domain"),
                    },
                ),
                operator_environment: OperatorEnvironmentOptions {
                    operator_namespace: "stackable-operators".to_owned(),
                    operator_service_name: "spark-k8s-operator".to_owned(),
                    image_repository: "oci.stackable.tech/sdp".to_owned(),
                },
            });

            reconcile(Arc::new(object), ctx).await
        })
        .expect("an object marked for deletion reconciles without any API call");

    assert_eq!(action, Action::await_change());
}

/// Asserts that `script` runs `set +x`, without a later `set -x`, before the first line
/// containing `needle`.
///
/// The containers run under `bash -x`, which prints every command with its variables
/// expanded.
/// Anything that reads a secret must have xtrace set to off.
///
/// Panics if no line contains `needle`.
pub fn assert_not_traced(script: &str, needle: &str) {
    let lines: Vec<&str> = script.lines().map(str::trim).collect();
    let position = lines
        .iter()
        .position(|line| line.contains(needle))
        .unwrap_or_else(|| panic!("no line contains {needle:?} in:\n{script}"));
    let last_toggle = lines[..position]
        .iter()
        .rev()
        .find(|line| matches!(**line, "set +x" | "set -x"));

    assert_eq!(
        last_toggle,
        Some(&"set +x"),
        "{needle:?} is traced in:\n{script}"
    );
}
