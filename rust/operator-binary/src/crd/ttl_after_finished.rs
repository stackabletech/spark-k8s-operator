//! How long the objects of a finished SparkApplication are kept.
use std::{borrow::Cow, fmt::Display, str::FromStr};

use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use stackable_operator::{
    schemars::{self, JsonSchema},
    shared::time::{Duration, DurationParseError},
};

#[derive(Debug, Snafu)]
#[snafu(display(
    "invalid ttlAfterFinished {input:?}, expected \"never\" or a duration such as \"10m\""
))]
pub struct ParseTtlAfterFinishedError {
    input: String,
    source: DurationParseError,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(try_from = "String", into = "String")]
pub enum TtlAfterFinished {
    Never,
    After(Duration),
}

impl TtlAfterFinished {
    const NEVER: &'static str = "never";

    pub fn job_ttl_seconds(&self) -> Option<i32> {
        match self {
            Self::Never => None,
            Self::After(duration) => Some(i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)),
        }
    }
}

impl FromStr for TtlAfterFinished {
    type Err = ParseTtlAfterFinishedError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input == Self::NEVER {
            return Ok(Self::Never);
        }
        Duration::from_str(input)
            .map(Self::After)
            .context(ParseTtlAfterFinishedSnafu { input })
    }
}

impl Display for TtlAfterFinished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Never => f.write_str(Self::NEVER),
            Self::After(duration) => duration.fmt(f),
        }
    }
}

impl TryFrom<String> for TtlAfterFinished {
    type Error = ParseTtlAfterFinishedError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<TtlAfterFinished> for String {
    fn from(value: TtlAfterFinished) -> Self {
        value.to_string()
    }
}

impl JsonSchema for TtlAfterFinished {
    fn schema_name() -> Cow<'static, str> {
        "TtlAfterFinished".into()
    }

    fn json_schema(generator: &mut schemars::generate::SchemaGenerator) -> schemars::Schema {
        String::json_schema(generator)
    }
}

impl Default for TtlAfterFinished {
    fn default() -> Self {
        Self::After(Duration::from_minutes_unchecked(10))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::never("never", TtlAfterFinished::Never)]
    #[case::zero("0s", TtlAfterFinished::After(Duration::from_secs(0)))]
    #[case::ten_minutes("10m", TtlAfterFinished::After(Duration::from_minutes_unchecked(10)))]
    fn ttl_after_finished_round_trips(#[case] input: &str, #[case] expected: TtlAfterFinished) {
        let parsed: TtlAfterFinished = input.parse().expect("the input is valid");
        assert_eq!(parsed, expected);
        assert_eq!(parsed.to_string(), input);
    }

    #[rstest]
    #[case::empty("")]
    #[case::typo("nevr")]
    fn ttl_after_finished_rejects_invalid_input(#[case] input: &str) {
        assert!(input.parse::<TtlAfterFinished>().is_err());
    }

    #[rstest]
    #[case::never(TtlAfterFinished::Never, None)]
    #[case::zero(TtlAfterFinished::After(Duration::from_secs(0)), Some(0))]
    fn ttl_after_finished_job_seconds(
        #[case] ttl: TtlAfterFinished,
        #[case] expected: Option<i32>,
    ) {
        assert_eq!(ttl.job_ttl_seconds(), expected);
    }
}
