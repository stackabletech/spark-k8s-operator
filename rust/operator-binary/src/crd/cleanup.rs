//! How long the driver Pod of a finished SparkApplication is kept, depending on how it ended.
use serde::{Deserialize, Serialize};
use stackable_operator::{
    config::merge::Merge,
    schemars::{self, JsonSchema},
    shared::time::Duration,
};

/// How long the driver Pod is kept after it finished, before the operator deletes it.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, Merge, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cleanup {
    /// Time to live after the driver Pod succeeded. Defaults to `10m`, `0s` deletes it right away.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_succeeded: Option<Duration>,

    /// Time to live after the driver Pod failed. Defaults to `24h`, `0s` deletes it right away.
    /// Also the time to live of the spark-submit Job, whose logs are the only ones if the
    /// submission fails before a driver Pod exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_failed: Option<Duration>,
}

impl Cleanup {
    pub fn after_succeeded(&self) -> Duration {
        self.after_succeeded
            .unwrap_or(Duration::from_minutes_unchecked(10))
    }

    pub fn after_failed(&self) -> Duration {
        self.after_failed
            .unwrap_or(Duration::from_hours_unchecked(24))
    }

    /// Both TTLs with the defaults filled in, as recorded in the status.
    pub fn resolved(&self) -> Self {
        Self {
            after_succeeded: Some(self.after_succeeded()),
            after_failed: Some(self.after_failed()),
        }
    }
}
