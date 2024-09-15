use crate::telemetry::filter_from_value;
use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
pub struct CustomEnvFilter(pub EnvFilter);

impl Clone for CustomEnvFilter {
	fn clone(&self) -> Self {
		Self(EnvFilter::builder().parse(self.0.to_string()).unwrap())
	}
}

#[derive(Clone)]
pub struct CustomEnvFilterParser;

impl CustomEnvFilterParser {
	pub fn new() -> CustomEnvFilterParser {
		Self
	}
}

impl TypedValueParser for CustomEnvFilterParser {
	type Value = CustomEnvFilter;

	fn parse_ref(
		&self,
		cmd: &clap::Command,
		arg: Option<&clap::Arg>,
		value: &std::ffi::OsStr,
	) -> Result<Self::Value, clap::Error> {
		if let Ok(dirs) = std::env::var("RUST_LOG") {
			return Ok(CustomEnvFilter(EnvFilter::builder().parse_lossy(dirs)));
		}

		let inner = NonEmptyStringValueParser::new();
		let v = inner.parse_ref(cmd, arg, value)?;
		let filter = filter_from_value(v.as_str()).map_err(|e| {
			let mut err = clap::Error::new(ErrorKind::ValueValidation).with_cmd(cmd);
			err.insert(ContextKind::Custom, ContextValue::String(e.to_string()));
			err.insert(
				ContextKind::InvalidValue,
				ContextValue::String("Provide a valid log filter configuration string".to_string()),
			);
			err
		})?;
		Ok(CustomEnvFilter(filter))
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
