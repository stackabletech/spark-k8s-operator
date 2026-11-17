//! How long the driver Pod of a finished SparkApplication is kept, depending on how it ended.
use serde::{Deserialize, Serialize};
use stackable_operator::{
    config::merge::Merge,
    schemars::{self, JsonSchema},
    shared::time::Duration,
};

pub const DEFAULT_AFTER_SUCCEEDED: Duration = Duration::from_minutes_unchecked(10);
pub const DEFAULT_AFTER_FAILED: Duration = Duration::from_hours_unchecked(24);

/// How long the driver Pod is kept after it finished, before the operator deletes it.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Merge, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cleanup {
    /// Time to live after the driver Pod succeeded. Defaults to `10m`, `0s` deletes it right away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_succeeded: Option<Duration>,

    /// Time to live after the driver Pod failed. Defaults to `24h`, `0s` deletes it right away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_failed: Option<Duration>,
}

impl Cleanup {
    /// Whether both TTLs are explicitly set to `0s`.
    pub fn is_immediate(&self) -> bool {
        let zero = Some(Duration::from_secs(0));
        self.after_succeeded == zero && self.after_failed == zero
    }
}

/// The TTLs that apply to the driver Pod, with defaults filled in.
#[derive(Clone, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedCleanup {
    /// Time to live after the driver Pod succeeded.
    pub after_succeeded: Duration,

    /// Time to live after the driver Pod failed.
    pub after_failed: Duration,
}

impl ResolvedCleanup {
    /// Both TTLs `0s`: the driver Pod is deleted as soon as it finished.
    pub const IMMEDIATE: Self = Self {
        after_succeeded: Duration::from_secs(0),
        after_failed: Duration::from_secs(0),
    };
}
