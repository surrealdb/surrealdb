use crate::telemetry::filter_from_value;
use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
pub struct CustomFilter(pub EnvFilter);

impl Clone for CustomFilter {
	fn clone(&self) -> Self {
		Self(EnvFilter::builder().parse(self.0.to_string()).unwrap())
	}
}

#[derive(Clone)]
pub struct CustomFilterParser;

impl CustomFilterParser {
	pub fn new() -> CustomFilterParser {
		Self
	}
}

impl TypedValueParser for CustomFilterParser {
	type Value = CustomFilter;

	fn parse_ref(
		&self,
		cmd: &clap::Command,
		arg: Option<&clap::Arg>,
		value: &std::ffi::OsStr,
	) -> Result<Self::Value, clap::Error> {
		// Fetch the log filter input
		let input = if let Ok(input) = std::env::var("RUST_LOG") {
			input
		} else {
			let inner = NonEmptyStringValueParser::new();
			inner.parse_ref(cmd, arg, value)?
		};
		// Parse the log filter input
		let filter = filter_from_value(input.as_str()).map_err(|e| {
			let mut err = clap::Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
			err.insert(ContextKind::Custom, ContextValue::String(e.to_string()));
			err.insert(
				ContextKind::InvalidValue,
				ContextValue::String("Provide a valid log filter configuration string".to_string()),
			);
			err
		})?;
		// Return the custom targets
		Ok(CustomFilter(filter))
	}

	fn possible_values(&self) -> Option<Box<dyn Iterator<Item = PossibleValue> + '_>> {
		Some(Box::new(
			[
				PossibleValue::new("none"),
				PossibleValue::new("full"),
				PossibleValue::new("error"),
				PossibleValue::new("warn"),
				PossibleValue::new("info"),
				PossibleValue::new("debug"),
				PossibleValue::new("trace"),
			]
			.into_iter(),
		))
	}
}
