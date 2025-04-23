use serde::{Deserialize, Serialize};
use stackable_operator::{
    config::merge::Atomic,
    schemars::{self, JsonSchema},
};
use strum::Display;

impl Atomic for SupportedListenerClasses {}

#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum SupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    #[strum(serialize = "cluster-internal")]
    ClusterInternal,

    #[serde(rename = "external-unstable")]
    #[strum(serialize = "external-unstable")]
    ExternalUnstable,

    #[serde(rename = "external-stable")]
    #[strum(serialize = "external-stable")]
    ExternalStable,
}
