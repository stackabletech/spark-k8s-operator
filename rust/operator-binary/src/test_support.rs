//! Shared helpers for the crate's tests.

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
