use stackable_operator::crd::CustomResourceExt;
use stackable_spark_k8s_crd::SparkApplication;

fn main() -> Result<(), stackable_operator::error::Error> {
    built::write_built_file().expect("Failed to acquire build-time information");

    SparkApplication::write_yaml_schema("../../deploy/crd/sparkapplication.crd.yaml")?;

    Ok(())
}
