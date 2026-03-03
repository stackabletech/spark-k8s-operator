//! This module provides the JobDependencies definition for Spark applications.

use serde::{Deserialize, Serialize};
use stackable_operator::schemars::{self, JsonSchema};

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobDependencies {
    /// Under the `requirements` you can specify Python dependencies that will be installed with `pip`.
    /// Example: `tabulate==0.8.9`
    #[serde(default)]
    pub requirements: Vec<String>,

    /// A list of packages that is passed directly to `spark-submit`.
    #[serde(default)]
    pub packages: Vec<String>,

    /// A list of repositories that is passed directly to `spark-submit`.
    #[serde(default)]
    pub repositories: Vec<String>,

    /// A list of excluded packages that is passed directly to `spark-submit`.
    #[serde(default)]
    pub exclude_packages: Vec<String>,
}
