use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};

use crate::cli::abstraction::auth::CredentialsLevel;

#[derive(Clone)]
pub struct CredentialsLevelParser;

impl CredentialsLevelParser {
	pub fn new() -> CredentialsLevelParser {
		Self
	}
}

impl TypedValueParser for CredentialsLevelParser {
	type Value = CredentialsLevel;

	fn parse_ref(
		&self,
		cmd: &clap::Command,
		arg: Option<&clap::Arg>,
		value: &std::ffi::OsStr,
	) -> Result<Self::Value, clap::Error> {
		let inner = NonEmptyStringValueParser::new();
		let v = inner.parse_ref(cmd, arg, value)?;

		match v.as_str() {
			"root" => Ok(CredentialsLevel::Root),
			"namespace" | "ns" => Ok(CredentialsLevel::Namespace),
			"database" | "db" => Ok(CredentialsLevel::Database),
			v => {
				let mut err = clap::Error::new(ErrorKind::InvalidValue);
				err.insert(ContextKind::InvalidValue, ContextValue::String(v.to_string()));
				Err(err)
			}
		}
	}

	fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
		Some(Box::new(
			[
				PossibleValue::new("root"),
				PossibleValue::new("namespace"),
				PossibleValue::new("ns"),
				PossibleValue::new("database"),
				PossibleValue::new("db"),
			]
			.into_iter(),
		))
	}
}
