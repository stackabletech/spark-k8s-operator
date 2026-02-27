//! This module provides functionality for merging SparkApplication instances.

use std::collections::HashMap;

use stackable_operator::{
    config::merge::Merge, k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta,
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
}
