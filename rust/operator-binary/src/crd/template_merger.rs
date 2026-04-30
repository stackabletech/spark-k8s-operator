//! This module provides functionality for merging SparkApplication instances.

use std::collections::HashMap;

use stackable_operator::{
    config::merge::Merge,
    k8s_openapi::{
        DeepMerge, api::core::v1::PodTemplateSpec, apimachinery::pkg::apis::meta::v1::ObjectMeta,
    },
};

use super::v1alpha1::SparkApplication;

/// Deep merge two SparkApplication instances.
///
/// This function merges all fields from the `overlay` SparkApplication into the `base` SparkApplication,
/// creating a new SparkApplication. The merge strategy is:
/// - Scalar fields: `overlay` value takes precedence if present
/// - Option fields: `overlay` value is used if `Some`, otherwise `base` value is kept
/// - Collections (HashMap, Vec): Items from both are combined
/// - Nested configurations: Deeply merged using the Merge trait
/// - Metadata: Combined with `overlay` metadata taking precedence for most fields
///
/// # Arguments
///
/// * `base` - The base SparkApplication to merge into
/// * `overlay` - The SparkApplication whose values will override/augment the base
///
/// # Returns
///
/// A new SparkApplication containing the merged result
pub fn deep_merge(base: &SparkApplication, overlay: &SparkApplication) -> SparkApplication {
    let metadata = merge_metadata(&base.metadata, &overlay.metadata);

    // Merge spec fields
    let spec = super::v1alpha1::SparkApplicationSpec {
        // Scalar fields: overlay takes precedence
        mode: overlay.spec.mode.clone(),
        main_application_file: overlay.spec.main_application_file.clone(),

        // Option fields: overlay if Some, otherwise base
        main_class: overlay
            .spec
            .main_class
            .clone()
            .or_else(|| base.spec.main_class.clone()),
        image: overlay
            .spec
            .image
            .clone()
            .or_else(|| base.spec.image.clone()),
        vector_aggregator_config_map_name: overlay
            .spec
            .vector_aggregator_config_map_name
            .clone()
            .or_else(|| base.spec.vector_aggregator_config_map_name.clone()),
        s3connection: overlay
            .spec
            .s3connection
            .clone()
            .or_else(|| base.spec.s3connection.clone()),
        log_file_directory: overlay
            .spec
            .log_file_directory
            .clone()
            .or_else(|| base.spec.log_file_directory.clone()),

        // Product image: overlay takes precedence
        spark_image: overlay.spec.spark_image.clone(),

        // Merge job configuration
        job: merge_common_config(base.spec.job.as_ref(), overlay.spec.job.as_ref()),

        // Merge driver configuration
        driver: merge_common_config(base.spec.driver.as_ref(), overlay.spec.driver.as_ref()),

        // Merge executor configuration (RoleGroup)
        executor: merge_role_group(base.spec.executor.as_ref(), overlay.spec.executor.as_ref()),

        // Merge collections
        spark_conf: merge_hashmap(&base.spec.spark_conf, &overlay.spec.spark_conf),
        args: merge_vec(&base.spec.args, &overlay.spec.args),
        volumes: merge_vec(&base.spec.volumes, &overlay.spec.volumes),
        env: merge_vec(&base.spec.env, &overlay.spec.env),

        // Merge deps
        deps: merge_deps(&base.spec.deps, &overlay.spec.deps),
    };

    // Status: use overlay if present, otherwise base
    let status = overlay.status.clone().or_else(|| base.status.clone());

    SparkApplication {
        metadata,
        spec,
        status,
    }
}

/// Merge ObjectMeta, with overlay taking precedence for most fields
// TODO: figure out if it is the right thing to copy all metadata from templates into apps
// or if this might also be made configurable via annotations
fn merge_metadata(base: &ObjectMeta, overlay: &ObjectMeta) -> ObjectMeta {
    ObjectMeta {
        name: overlay.name.clone().or_else(|| base.name.clone()),
        namespace: overlay.namespace.clone().or_else(|| base.namespace.clone()),
        labels: merge_option_hashmap(&base.labels, &overlay.labels),
        annotations: merge_option_hashmap(&base.annotations, &overlay.annotations),
        owner_references: overlay
            .owner_references
            .clone()
            .or_else(|| base.owner_references.clone()),
        finalizers: overlay
            .finalizers
            .clone()
            .or_else(|| base.finalizers.clone()),
        uid: overlay.uid.clone().or_else(|| base.uid.clone()),
        ..Default::default()
    }
}

/// Merge two Option<HashMap<String, String>>
fn merge_option_hashmap(
    base: &Option<std::collections::BTreeMap<String, String>>,
    overlay: &Option<std::collections::BTreeMap<String, String>>,
) -> Option<std::collections::BTreeMap<String, String>> {
    match (base, overlay) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => {
            let mut merged = b.clone();
            merged.extend(o.clone());
            Some(merged)
        }
    }
}

/// Merge two HashMaps, with overlay values taking precedence
fn merge_hashmap<K, V>(base: &HashMap<K, V>, overlay: &HashMap<K, V>) -> HashMap<K, V>
where
    K: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    let mut merged = base.clone();
    merged.extend(overlay.clone());
    merged
}

/// Merge two nested HashMaps with overlay precedence for inner keys.
fn merge_nested_hashmap<K1, K2, V>(
    base: &HashMap<K1, HashMap<K2, V>>,
    overlay: &HashMap<K1, HashMap<K2, V>>,
) -> HashMap<K1, HashMap<K2, V>>
where
    K1: Eq + std::hash::Hash + Clone,
    K2: Eq + std::hash::Hash + Clone,
    V: Clone,
{
    let mut merged = base.clone();
    for (outer_key, overlay_inner) in overlay {
        merged
            .entry(outer_key.clone())
            .and_modify(|base_inner| base_inner.extend(overlay_inner.clone()))
            .or_insert_with(|| overlay_inner.clone());
    }
    merged
}

/// Merge two PodTemplateSpecs using Kubernetes deep merge semantics.
fn merge_pod_template_spec(base: &PodTemplateSpec, overlay: &PodTemplateSpec) -> PodTemplateSpec {
    let mut merged = base.clone();
    merged.merge_from(overlay.clone());

    if let (Some(base_spec), Some(overlay_spec), Some(merged_spec)) = (
        base.spec.as_ref(),
        overlay.spec.as_ref(),
        merged.spec.as_mut(),
    ) {
        if let Some(overlay_node_selector) = overlay_spec.node_selector.as_ref() {
            let mut node_selector = base_spec.node_selector.clone().unwrap_or_default();
            node_selector.extend(overlay_node_selector.clone());
            merged_spec.node_selector = Some(node_selector);
        }
    }

    merged
}

/// Merge two Vecs by concatenating them
fn merge_vec<T: Clone>(base: &[T], overlay: &[T]) -> Vec<T> {
    let mut merged = base.to_vec();
    merged.extend_from_slice(overlay);
    merged
}

/// Merge CommonConfiguration using the Merge trait
fn merge_common_config<C, R>(
    base: Option<&stackable_operator::role_utils::CommonConfiguration<C, R>>,
    overlay: Option<&stackable_operator::role_utils::CommonConfiguration<C, R>>,
) -> Option<stackable_operator::role_utils::CommonConfiguration<C, R>>
where
    C: Clone + Merge,
    R: Clone,
{
    match (base, overlay) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => {
            // Clone the base and merge the overlay config into it
            let mut merged = b.clone();
            merged.config.merge(&o.config);
            merged.config_overrides =
                merge_nested_hashmap(&b.config_overrides, &o.config_overrides);
            merged.env_overrides = merge_hashmap(&b.env_overrides, &o.env_overrides);
            merged.pod_overrides = merge_pod_template_spec(&b.pod_overrides, &o.pod_overrides);
            Some(merged)
        }
    }
}

/// Merge RoleGroup
fn merge_role_group<C, R>(
    base: Option<&stackable_operator::role_utils::RoleGroup<C, R>>,
    overlay: Option<&stackable_operator::role_utils::RoleGroup<C, R>>,
) -> Option<stackable_operator::role_utils::RoleGroup<C, R>>
where
    C: Clone + Merge,
    R: Clone,
{
    match (base, overlay) {
        (None, None) => None,
        (Some(b), None) => Some(b.clone()),
        (None, Some(o)) => Some(o.clone()),
        (Some(b), Some(o)) => {
            // Clone the base and merge overlay
            let mut merged = b.clone();
            merged.config.config.merge(&o.config.config);
            merged.config.config_overrides =
                merge_nested_hashmap(&b.config.config_overrides, &o.config.config_overrides);
            merged.config.env_overrides =
                merge_hashmap(&b.config.env_overrides, &o.config.env_overrides);
            merged.config.pod_overrides =
                merge_pod_template_spec(&b.config.pod_overrides, &o.config.pod_overrides);
            // Use overlay replicas if present
            if o.replicas.is_some() {
                merged.replicas = o.replicas;
            }
            Some(merged)
        }
    }
}

/// Merge JobDependencies
fn merge_deps(
    base: &super::job_dependencies::JobDependencies,
    overlay: &super::job_dependencies::JobDependencies,
) -> super::job_dependencies::JobDependencies {
    super::job_dependencies::JobDependencies {
        requirements: merge_vec(&base.requirements, &overlay.requirements),
        packages: merge_vec(&base.packages, &overlay.packages),
        repositories: merge_vec(&base.repositories, &overlay.repositories),
        exclude_packages: merge_vec(&base.exclude_packages, &overlay.exclude_packages),
    }
}

#[cfg(test)]
mod tests {
    use indoc::indoc;

    use super::*;

    #[test]
    fn test_deep_merge_basic() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
              namespace: default
            spec:
              mode: cluster
              mainApplicationFile: base.py
              mainClass: BaseClass
              sparkImage:
                productVersion: "3.5.0"
              args:
                - arg1
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
              uid: e8990dae-3f4c-417c-8b08-b2770e347d07
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "3.5.1"
              args:
                - arg2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        // overlay name should win
        assert_eq!(merged.metadata.name, Some("overlay-app".to_string()));
        // namespace from base should be preserved
        assert_eq!(merged.metadata.namespace, Some("default".to_string()));
        // metadata uid should be preserved
        assert_eq!(
            merged.metadata.uid,
            Some("e8990dae-3f4c-417c-8b08-b2770e347d07".to_string())
        );
        // overlay main_application_file should win
        assert_eq!(merged.spec.main_application_file, "overlay.py");
        // base main_class should be preserved since overlay is None
        assert_eq!(merged.spec.main_class, Some("BaseClass".to_string()));
        // overlay image should be used
        assert_eq!(merged.spec.spark_image.product_version(), "3.5.1");
        // args should be concatenated
        assert_eq!(merged.spec.args, vec!["arg1", "arg2"]);
    }

    #[test]
    fn test_deep_merge_spark_conf() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              sparkImage:
                productVersion: "3.5.8"
              mainApplicationFile: base.py
              sparkConf:
                "spark.executor.memory": "4g"
                "spark.executor.cores": "2"
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              sparkImage:
                productVersion: "4.1.0"
              mainApplicationFile: overlay.py
              sparkConf:
                "spark.executor.cores": "4"
                "spark.executor.instances": "3"
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        // spark_conf should be merged with overlay taking precedence for conflicting keys
        assert_eq!(
            merged.spec.spark_conf.get("spark.executor.memory"),
            Some(&"4g".to_string())
        );
        assert_eq!(
            merged.spec.spark_conf.get("spark.executor.cores"),
            Some(&"4".to_string())
        );
        assert_eq!(
            merged.spec.spark_conf.get("spark.executor.instances"),
            Some(&"3".to_string())
        );
    }

    #[test]
    fn test_deep_merge_job() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                config:
                  retryOnFailureCount: 2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        // job config should be merged with overlay taking precedence for conflicting keys
        assert_eq!(
            merged
                .spec
                .job
                .as_ref()
                .unwrap()
                .config
                .retry_on_failure_count,
            Some(2u16)
        );
    }

    #[test]
    fn test_deep_merge_config_overrides() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                configOverrides:
                  security.properties:
                    test.base.only: base
                    test.overridden: base
              driver:
                configOverrides:
                  security.properties:
                    test.base.only: base
                    test.overridden: base
              executor:
                replicas: 1
                configOverrides:
                  security.properties:
                    test.base.only: base
                    test.overridden: base
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                configOverrides:
                  security.properties:
                    test.overridden: overlay
                    test.overlay.only: overlay
              driver:
                configOverrides:
                  security.properties:
                    test.overridden: overlay
                    test.overlay.only: overlay
              executor:
                replicas: 2
                configOverrides:
                  security.properties:
                    test.overridden: overlay
                    test.overlay.only: overlay
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        // configOverrides should be deep-merged so non-conflicting base keys remain,
        // conflicting keys are overridden by overlay values, and new overlay keys are added.
        let submit_security_props = merged
            .spec
            .job
            .as_ref()
            .and_then(|j| j.config_overrides.get("security.properties"))
            .unwrap();
        assert_eq!(
            submit_security_props.get("test.base.only"),
            Some(&"base".to_string())
        );
        assert_eq!(
            submit_security_props.get("test.overridden"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            submit_security_props.get("test.overlay.only"),
            Some(&"overlay".to_string())
        );

        let driver_security_props = merged
            .spec
            .driver
            .as_ref()
            .and_then(|d| d.config_overrides.get("security.properties"))
            .unwrap();
        assert_eq!(
            driver_security_props.get("test.base.only"),
            Some(&"base".to_string())
        );
        assert_eq!(
            driver_security_props.get("test.overridden"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            driver_security_props.get("test.overlay.only"),
            Some(&"overlay".to_string())
        );

        let executor_security_props = merged
            .spec
            .executor
            .as_ref()
            .and_then(|e| e.config.config_overrides.get("security.properties"))
            .unwrap();
        assert_eq!(
            executor_security_props.get("test.base.only"),
            Some(&"base".to_string())
        );
        assert_eq!(
            executor_security_props.get("test.overridden"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            executor_security_props.get("test.overlay.only"),
            Some(&"overlay".to_string())
        );
    }

    #[test]
    fn test_deep_merge_config_overrides_base_only() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                configOverrides:
                  security.properties:
                    test.base.only: base
                config:
                  retryOnFailureCount: 1
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                config:
                  retryOnFailureCount: 2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);
        let submit_security_props = merged
            .spec
            .job
            .as_ref()
            .and_then(|j| j.config_overrides.get("security.properties"))
            .unwrap();

        assert_eq!(
            submit_security_props.get("test.base.only"),
            Some(&"base".to_string())
        );
    }

    #[test]
    fn test_deep_merge_config_overrides_overlay_only() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                config:
                  retryOnFailureCount: 1
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                configOverrides:
                  security.properties:
                    test.overlay.only: overlay
                config:
                  retryOnFailureCount: 2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);
        let submit_security_props = merged
            .spec
            .job
            .as_ref()
            .and_then(|j| j.config_overrides.get("security.properties"))
            .unwrap();

        assert_eq!(
            submit_security_props.get("test.overlay.only"),
            Some(&"overlay".to_string())
        );
    }

    #[test]
    fn test_deep_merge_env_overrides() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                envOverrides:
                  TEST_BASE_ONLY: base
                  TEST_OVERRIDDEN: base
              driver:
                envOverrides:
                  TEST_BASE_ONLY: base
                  TEST_OVERRIDDEN: base
              executor:
                replicas: 1
                envOverrides:
                  TEST_BASE_ONLY: base
                  TEST_OVERRIDDEN: base
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                envOverrides:
                  TEST_OVERRIDDEN: overlay
                  TEST_OVERLAY_ONLY: overlay
              driver:
                envOverrides:
                  TEST_OVERRIDDEN: overlay
                  TEST_OVERLAY_ONLY: overlay
              executor:
                replicas: 2
                envOverrides:
                  TEST_OVERRIDDEN: overlay
                  TEST_OVERLAY_ONLY: overlay
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        let submit_env = &merged.spec.job.as_ref().unwrap().env_overrides;
        assert_eq!(submit_env.get("TEST_BASE_ONLY"), Some(&"base".to_string()));
        assert_eq!(
            submit_env.get("TEST_OVERRIDDEN"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            submit_env.get("TEST_OVERLAY_ONLY"),
            Some(&"overlay".to_string())
        );

        let driver_env = &merged.spec.driver.as_ref().unwrap().env_overrides;
        assert_eq!(driver_env.get("TEST_BASE_ONLY"), Some(&"base".to_string()));
        assert_eq!(
            driver_env.get("TEST_OVERRIDDEN"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            driver_env.get("TEST_OVERLAY_ONLY"),
            Some(&"overlay".to_string())
        );

        let executor_env = &merged.spec.executor.as_ref().unwrap().config.env_overrides;
        assert_eq!(
            executor_env.get("TEST_BASE_ONLY"),
            Some(&"base".to_string())
        );
        assert_eq!(
            executor_env.get("TEST_OVERRIDDEN"),
            Some(&"overlay".to_string())
        );
        assert_eq!(
            executor_env.get("TEST_OVERLAY_ONLY"),
            Some(&"overlay".to_string())
        );
    }

    #[test]
    fn test_deep_merge_pod_overrides() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                podOverrides:
                  spec:
                    serviceAccountName: base-sa
                    nodeSelector:
                      test.base.only: base
                      test.overridden: base
              driver:
                podOverrides:
                  spec:
                    serviceAccountName: base-sa
                    nodeSelector:
                      test.base.only: base
                      test.overridden: base
              executor:
                replicas: 1
                podOverrides:
                  spec:
                    serviceAccountName: base-sa
                    nodeSelector:
                      test.base.only: base
                      test.overridden: base
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                podOverrides:
                  spec:
                    serviceAccountName: overlay-sa
                    nodeSelector:
                      test.overridden: overlay
                      test.overlay.only: overlay
              driver:
                podOverrides:
                  spec:
                    serviceAccountName: overlay-sa
                    nodeSelector:
                      test.overridden: overlay
                      test.overlay.only: overlay
              executor:
                replicas: 2
                podOverrides:
                  spec:
                    serviceAccountName: overlay-sa
                    nodeSelector:
                      test.overridden: overlay
                      test.overlay.only: overlay
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        let submit_spec = merged
            .spec
            .job
            .as_ref()
            .unwrap()
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();
        assert_eq!(
            submit_spec.service_account_name.as_deref(),
            Some("overlay-sa")
        );
        let submit_node_selector = submit_spec.node_selector.as_ref().unwrap();
        assert_eq!(
            submit_node_selector
                .get("test.base.only")
                .map(String::as_str),
            Some("base")
        );
        assert_eq!(
            submit_node_selector
                .get("test.overridden")
                .map(String::as_str),
            Some("overlay")
        );
        assert_eq!(
            submit_node_selector
                .get("test.overlay.only")
                .map(String::as_str),
            Some("overlay")
        );

        let driver_spec = merged
            .spec
            .driver
            .as_ref()
            .unwrap()
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();
        assert_eq!(
            driver_spec.service_account_name.as_deref(),
            Some("overlay-sa")
        );
        let driver_node_selector = driver_spec.node_selector.as_ref().unwrap();
        assert_eq!(
            driver_node_selector
                .get("test.base.only")
                .map(String::as_str),
            Some("base")
        );
        assert_eq!(
            driver_node_selector
                .get("test.overridden")
                .map(String::as_str),
            Some("overlay")
        );
        assert_eq!(
            driver_node_selector
                .get("test.overlay.only")
                .map(String::as_str),
            Some("overlay")
        );

        let executor_spec = merged
            .spec
            .executor
            .as_ref()
            .unwrap()
            .config
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();
        assert_eq!(
            executor_spec.service_account_name.as_deref(),
            Some("overlay-sa")
        );
        let executor_node_selector = executor_spec.node_selector.as_ref().unwrap();
        assert_eq!(
            executor_node_selector
                .get("test.base.only")
                .map(String::as_str),
            Some("base")
        );
        assert_eq!(
            executor_node_selector
                .get("test.overridden")
                .map(String::as_str),
            Some("overlay")
        );
        assert_eq!(
            executor_node_selector
                .get("test.overlay.only")
                .map(String::as_str),
            Some("overlay")
        );
    }

    #[test]
    fn test_deep_merge_pod_overrides_container_resources() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                podOverrides:
                  spec:
                    containers:
                      - name: spark-submit
                        resources:
                          requests:
                            cpu: 500m
                            memory: 512Mi
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                podOverrides:
                  spec:
                    containers:
                      - name: spark-submit
                        resources:
                          requests:
                            cpu: 1500m
                          limits:
                            memory: 2Gi
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);

        let submit_spec = merged
            .spec
            .job
            .as_ref()
            .unwrap()
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();
        let submit_container = submit_spec
            .containers
            .iter()
            .find(|container| container.name == "spark-submit")
            .unwrap();
        let submit_resources = submit_container.resources.as_ref().unwrap();
        let submit_requests = submit_resources.requests.as_ref().unwrap();
        let submit_limits = submit_resources.limits.as_ref().unwrap();

        assert_eq!(
            submit_requests
                .get("cpu")
                .map(|quantity| quantity.0.as_str()),
            Some("1500m")
        );
        assert_eq!(
            submit_requests
                .get("memory")
                .map(|quantity| quantity.0.as_str()),
            Some("512Mi")
        );
        assert_eq!(
            submit_limits
                .get("memory")
                .map(|quantity| quantity.0.as_str()),
            Some("2Gi")
        );
    }

    #[test]
    fn test_deep_merge_pod_overrides_base_only() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                podOverrides:
                  spec:
                    serviceAccountName: base-sa
                    nodeSelector:
                      test.base.only: base
                config:
                  retryOnFailureCount: 1
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                config:
                  retryOnFailureCount: 2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);
        let submit_spec = merged
            .spec
            .job
            .as_ref()
            .unwrap()
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();

        assert_eq!(submit_spec.service_account_name.as_deref(), Some("base-sa"));
        assert_eq!(
            submit_spec
                .node_selector
                .as_ref()
                .and_then(|selector| selector.get("test.base.only"))
                .map(String::as_str),
            Some("base")
        );
    }

    #[test]
    fn test_deep_merge_pod_overrides_overlay_only() {
        let base = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: base-app
            spec:
              mode: cluster
              mainApplicationFile: base.py
              sparkImage:
                productVersion: "3.5.8"
              job:
                config:
                  retryOnFailureCount: 1
        "#})
        .unwrap();

        let overlay = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: overlay-app
            spec:
              mode: cluster
              mainApplicationFile: overlay.py
              sparkImage:
                productVersion: "4.1.0"
              job:
                podOverrides:
                  spec:
                    serviceAccountName: overlay-sa
                    nodeSelector:
                      test.overlay.only: overlay
                config:
                  retryOnFailureCount: 2
        "#})
        .unwrap();

        let merged = deep_merge(&base, &overlay);
        let submit_spec = merged
            .spec
            .job
            .as_ref()
            .unwrap()
            .pod_overrides
            .spec
            .as_ref()
            .unwrap();

        assert_eq!(
            submit_spec.service_account_name.as_deref(),
            Some("overlay-sa")
        );
        assert_eq!(
            submit_spec
                .node_selector
                .as_ref()
                .and_then(|selector| selector.get("test.overlay.only"))
                .map(String::as_str),
            Some("overlay")
        );
    }

    #[test]
    fn test_merge_two_templates_into_spark_application() {
        let template_a = serde_yaml::from_str::<
            crate::crd::template_spec::v1alpha1::SparkApplicationTemplate,
        >(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplicationTemplate
            metadata:
              name: template-a
            spec:
              mode: cluster
              mainApplicationFile: local:///placeholder.jar
              mainClass: com.example.Job
              sparkImage:
                productVersion: "3.5.8"
              sparkConf:
                "spark.executor.memory": "4g"
              args:
                - "--verbose"
              deps:
                requirements:
                  - "numpy==1.21"
        "#})
        .unwrap();

        let template_b = serde_yaml::from_str::<
            crate::crd::template_spec::v1alpha1::SparkApplicationTemplate,
        >(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplicationTemplate
            metadata:
              name: template-b
            spec:
              mode: cluster
              mainApplicationFile: local:///placeholder.jar
              sparkImage:
                productVersion: "3.5.8"
              sparkConf:
                "spark.executor.memory": "8g"
                "spark.executor.cores": "4"
              args:
                - "--output"
                - "/tmp/out"
              deps:
                requirements:
                  - "pandas==2.0"
        "#})
        .unwrap();

        let spark_app = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: my-spark-app
              namespace: default
            spec:
              mode: cluster
              mainApplicationFile: local:///app.jar
              sparkImage:
                productVersion: "3.5.8"
              sparkConf:
                "spark.driver.memory": "2g"
              args:
                - "--app-arg"
              deps:
                requirements:
                  - "scipy==1.9"
        "#})
        .unwrap();

        // Convert templates to SparkApplication and merge left-to-right:
        // template-a has the lowest priority, the spark application has the highest.
        let app_from_a = crate::crd::v1alpha1::SparkApplication::from(&template_a);
        let app_from_b = crate::crd::v1alpha1::SparkApplication::from(&template_b);
        // This how `merge_application_templates()` does it
        let template_apps = [app_from_a, app_from_b, spark_app];
        let merged = template_apps
            .into_iter()
            .reduce(|merge_app, app| deep_merge(&merge_app, &app))
            .expect("bad dev");

        // mainClass set only in template-a; neither template-b nor the app overrides it
        assert_eq!(
            merged.spec.main_class,
            Some("com.example.Job".to_string()),
            "mainClass should come from template-a"
        );

        // The app's mainApplicationFile wins
        assert_eq!(
            merged.spec.main_application_file, "local:///app.jar",
            "mainApplicationFile should come from the spark application"
        );

        // template-b overrides template-a's executor memory value
        assert_eq!(
            merged.spec.spark_conf.get("spark.executor.memory"),
            Some(&"8g".to_string()),
            "spark.executor.memory should be overridden by template-b"
        );

        // template-b contributes executor cores (not present in template-a or the app)
        assert_eq!(
            merged.spec.spark_conf.get("spark.executor.cores"),
            Some(&"4".to_string()),
            "spark.executor.cores should come from template-b"
        );

        // driver memory is set only by the spark application
        assert_eq!(
            merged.spec.spark_conf.get("spark.driver.memory"),
            Some(&"2g".to_string()),
            "spark.driver.memory should come from the spark application"
        );

        // args are concatenated in priority order
        assert_eq!(
            merged.spec.args,
            vec!["--verbose", "--output", "/tmp/out", "--app-arg"],
            "args should be concatenated from all sources in order"
        );

        // deps.requirements are concatenated in priority order
        assert_eq!(
            merged.spec.deps.requirements,
            vec!["numpy==1.21", "pandas==2.0", "scipy==1.9"],
            "deps.requirements should be concatenated from all sources in order"
        );
    }

    #[test]
    fn test_merge_template_role_affinities_into_spark_application() {
        let template = serde_yaml::from_str::<
            crate::crd::template_spec::v1alpha1::SparkApplicationTemplate,
        >(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplicationTemplate
            metadata:
              name: template-with-affinity
            spec:
              mode: cluster
              mainApplicationFile: local:///template.jar
              sparkImage:
                productVersion: "3.5.8"
              job:
                config:
                  affinity:
                    nodeSelector:
                      affinity-role: template-job
                    nodeAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 11
                          preference:
                            matchExpressions:
                              - key: topology.kubernetes.io/zone
                                operator: In
                                values:
                                  - fictional-zone-job
                    podAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 21
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-job
                    podAntiAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 31
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-job
              driver:
                config:
                  affinity:
                    nodeSelector:
                      affinity-role: template-driver
                    nodeAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 12
                          preference:
                            matchExpressions:
                              - key: topology.kubernetes.io/zone
                                operator: In
                                values:
                                  - fictional-zone-driver
                    podAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 22
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-driver
                    podAntiAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 32
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-driver
              executor:
                replicas: 1
                config:
                  affinity:
                    nodeSelector:
                      affinity-role: template-executor
                    nodeAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 13
                          preference:
                            matchExpressions:
                              - key: topology.kubernetes.io/zone
                                operator: In
                                values:
                                  - fictional-zone-executor
                    podAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 23
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-executor
                    podAntiAffinity:
                      preferredDuringSchedulingIgnoredDuringExecution:
                        - weight: 33
                          podAffinityTerm:
                            topologyKey: kubernetes.io/hostname
                            labelSelector:
                              matchLabels:
                                app.kubernetes.io/component: fictional-executor
        "#})
        .unwrap();

        let spark_app = serde_yaml::from_str::<crate::crd::v1alpha1::SparkApplication>(indoc! {r#"
            ---
            apiVersion: spark.stackable.tech/v1alpha1
            kind: SparkApplication
            metadata:
              name: my-spark-app
              namespace: default
            spec:
              mode: cluster
              mainApplicationFile: local:///app.jar
              sparkImage:
                productVersion: "3.5.8"
        "#})
        .unwrap();

        let app_from_template = crate::crd::v1alpha1::SparkApplication::from(&template);
        let merged = deep_merge(&app_from_template, &spark_app);

        let submit_affinity = merged.submit_config().unwrap().affinity;
        assert_eq!(
            Some("template-job"),
            submit_affinity
                .node_selector
                .as_ref()
                .and_then(|selectors| selectors.node_selector.get("affinity-role"))
                .map(String::as_str)
        );
        assert_eq!(
            Some(11),
            submit_affinity
                .node_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(21),
            submit_affinity
                .pod_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(31),
            submit_affinity
                .pod_anti_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );

        let driver_affinity = merged.driver_config().unwrap().affinity;
        assert_eq!(
            Some("template-driver"),
            driver_affinity
                .node_selector
                .as_ref()
                .and_then(|selectors| selectors.node_selector.get("affinity-role"))
                .map(String::as_str)
        );
        assert_eq!(
            Some(12),
            driver_affinity
                .node_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(22),
            driver_affinity
                .pod_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(32),
            driver_affinity
                .pod_anti_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );

        let executor_affinity = merged.executor_config().unwrap().affinity;
        assert_eq!(
            Some("template-executor"),
            executor_affinity
                .node_selector
                .as_ref()
                .and_then(|selectors| selectors.node_selector.get("affinity-role"))
                .map(String::as_str)
        );
        assert_eq!(
            Some(13),
            executor_affinity
                .node_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(23),
            executor_affinity
                .pod_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
        assert_eq!(
            Some(33),
            executor_affinity
                .pod_anti_affinity
                .as_ref()
                .and_then(|a| a
                    .preferred_during_scheduling_ignored_during_execution
                    .as_ref())
                .and_then(|terms| terms.first())
                .map(|term| term.weight)
        );
    }
}
