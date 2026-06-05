//! Writers for Java `.properties` files.
//!
//! This is adapted from the `product-config` writer so we can render
//! configuration files without depending on the `product-config` crate.

use std::io::Write;

use java_properties::{PropertiesError, PropertiesWriter};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum PropertiesWriterError {
    #[snafu(display("failed to create properties file"))]
    Properties { source: PropertiesError },

    #[snafu(display("failed to convert properties file byte array to UTF-8"))]
    FromUtf8 { source: std::string::FromUtf8Error },
}

/// Creates a common Java properties file string in the format:
/// `property_1=value_1\nproperty_2=value_2\n`.
pub fn to_java_properties_string<'a, T>(properties: T) -> Result<String, PropertiesWriterError>
where
    T: Iterator<Item = (&'a String, &'a Option<String>)>,
{
    let mut output = Vec::new();
    write_java_properties(&mut output, properties)?;
    String::from_utf8(output).context(FromUtf8Snafu)
}

/// Writes Java properties to the given writer. A `None` value is written as an
/// empty value (`key=`).
fn write_java_properties<'a, W, T>(writer: W, properties: T) -> Result<(), PropertiesWriterError>
where
    W: Write,
    T: Iterator<Item = (&'a String, &'a Option<String>)>,
{
    let mut writer = PropertiesWriter::new(writer);
    for (key, value) in properties {
        let property_value = value.as_deref().unwrap_or_default();
        writer.write(key, property_value).context(PropertiesSnafu)?;
    }
    writer.flush().context(PropertiesSnafu)?;

    Ok(())
}
