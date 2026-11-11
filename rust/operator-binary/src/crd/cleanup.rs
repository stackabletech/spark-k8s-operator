//! How long the objects of a finished SparkApplication are kept, depending on how it ended.
use std::{borrow::Cow, fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    schemars::{self, JsonSchema},
    shared::time::{Duration, DurationParseError},
};

#[derive(Debug, Snafu)]
#[snafu(display(
    "invalid cleanup TTL {input:?}, expected \"never\" or a duration such as \"10m\""
))]
pub struct ParseCleanupTtlError {
    input: String,
    source: DurationParseError,
}

/// How long the spark-submit Job, the driver Pod and the objects the operator created for the
/// application are kept after the driver finished, before they are deleted.
#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Cleanup {
    /// Time to live after the driver Pod succeeded. A duration such as `10m` (default), `0s` to
    /// delete the objects right away, or `never` to keep them until the SparkApplication is
    /// deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_succeeded: Option<CleanupTtl>,

    /// Time to live after the driver Pod failed. A duration such as `10m` (default), `0s` to
    /// delete the objects right away, or `never` to keep them until the SparkApplication is
    /// deleted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_failed: Option<CleanupTtl>,
}

impl Cleanup {
    /// Takes each field from `overlay` if it is set, and from `base` otherwise.
    pub fn merge(base: Option<&Self>, overlay: Option<&Self>) -> Option<Self> {
        match (base, overlay) {
            (None, None) => None,
            (Some(only), None) | (None, Some(only)) => Some(only.clone()),
            (Some(base), Some(overlay)) => Some(Self {
                after_succeeded: overlay
                    .after_succeeded
                    .clone()
                    .or_else(|| base.after_succeeded.clone()),
                after_failed: overlay
                    .after_failed
                    .clone()
                    .or_else(|| base.after_failed.clone()),
            }),
        }
    }

    pub fn after_succeeded(&self) -> CleanupTtl {
        self.after_succeeded.clone().unwrap_or_default()
    }

    pub fn after_failed(&self) -> CleanupTtl {
        self.after_failed.clone().unwrap_or_default()
    }

    /// The TTL the spark-submit Job is created with: the longer of the two, so that the Job is
    /// not deleted before the pod driver controller knows the outcome and shortens the TTL.
    pub fn initial_job_ttl_seconds(&self) -> Option<i32> {
        match (
            self.after_succeeded().job_ttl_seconds(),
            self.after_failed().job_ttl_seconds(),
        ) {
            (Some(succeeded), Some(failed)) => Some(succeeded.max(failed)),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub enum CleanupTtl {
    Never,
    After(Duration),
}

impl CleanupTtl {
    const NEVER: &'static str = "never";

    pub fn job_ttl_seconds(&self) -> Option<i32> {
        match self {
            Self::Never => None,
            Self::After(duration) => Some(i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)),
        }
    }
}

impl FromStr for CleanupTtl {
    type Err = ParseCleanupTtlError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == Self::NEVER {
            return Ok(Self::Never);
        }
        Duration::from_str(input)
            .map(Self::After)
            .context(ParseCleanupTtlSnafu { input })
    }
}

impl Display for CleanupTtl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Never => f.write_str(Self::NEVER),
            Self::After(duration) => duration.fmt(f),
        }
    }
}

impl TryFrom<String> for CleanupTtl {
    type Error = ParseCleanupTtlError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<CleanupTtl> for String {
    fn from(value: CleanupTtl) -> Self {
        value.to_string()
    }
}

impl JsonSchema for CleanupTtl {
    fn schema_name() -> Cow<'static, str> {
        "CleanupTtl".into()
    }

    fn json_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

impl Default for CleanupTtl {
    fn default() -> Self {
        Self::After(Duration::from_minutes_unchecked(10))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn cleanup(after_succeeded: Option<&str>, after_failed: Option<&str>) -> Cleanup {
        Cleanup {
            after_succeeded: after_succeeded.map(|ttl| ttl.parse().expect("the TTL is valid")),
            after_failed: after_failed.map(|ttl| ttl.parse().expect("the TTL is valid")),
        }
    }

    #[rstest]
    #[case::never("never", CleanupTtl::Never)]
    #[case::zero("0s", CleanupTtl::After(Duration::from_secs(0)))]
    #[case::ten_minutes("10m", CleanupTtl::After(Duration::from_minutes_unchecked(10)))]
    fn cleanup_ttl_round_trips(#[case] input: &str, #[case] expected: CleanupTtl) {
        let parsed: CleanupTtl = input.parse().expect("the input is valid");
        assert_eq!(parsed, expected);
        assert_eq!(parsed.to_string(), input);
    }

    #[rstest]
    #[case::empty("")]
    #[case::typo("nevr")]
    fn cleanup_ttl_rejects_invalid_input(#[case] input: &str) {
        assert!(input.parse::<CleanupTtl>().is_err());
    }

    #[rstest]
    #[case::never(CleanupTtl::Never, None)]
    #[case::zero(CleanupTtl::After(Duration::from_secs(0)), Some(0))]
    fn cleanup_ttl_job_seconds(#[case] ttl: CleanupTtl, #[case] expected: Option<i32>) {
        assert_eq!(ttl.job_ttl_seconds(), expected);
    }

    #[rstest]
    #[case::defaults(None, None, Some(600))]
    #[case::failed_longer(Some("10m"), Some("24h"), Some(86400))]
    #[case::succeeded_longer(Some("1h"), Some("0s"), Some(3600))]
    #[case::failed_never(Some("0s"), Some("never"), None)]
    #[case::succeeded_never(Some("never"), Some("0s"), None)]
    #[case::only_failed_set(None, Some("0s"), Some(600))]
    fn initial_job_ttl_is_the_longer_one(
        #[case] after_succeeded: Option<&str>,
        #[case] after_failed: Option<&str>,
        #[case] expected: Option<i32>,
    ) {
        assert_eq!(
            cleanup(after_succeeded, after_failed).initial_job_ttl_seconds(),
            expected
        );
    }

    #[rstest]
    #[case::neither(None, None, None)]
    #[case::base_only(Some(cleanup(Some("1h"), None)), None, Some(cleanup(Some("1h"), None)))]
    #[case::overlay_only(
        None,
        Some(cleanup(None, Some("never"))),
        Some(cleanup(None, Some("never")))
    )]
    #[case::fields_merged(
        Some(cleanup(Some("1h"), Some("24h"))),
        Some(cleanup(None, Some("never"))),
        Some(cleanup(Some("1h"), Some("never")))
    )]
    #[case::overlay_wins(
        Some(cleanup(Some("1h"), Some("24h"))),
        Some(cleanup(Some("0s"), Some("1m"))),
        Some(cleanup(Some("0s"), Some("1m")))
    )]
    fn merge_takes_each_field_from_overlay_first(
        #[case] base: Option<Cleanup>,
        #[case] overlay: Option<Cleanup>,
        #[case] expected: Option<Cleanup>,
    ) {
        assert_eq!(Cleanup::merge(base.as_ref(), overlay.as_ref()), expected);
    }
}
