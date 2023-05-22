use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use tracing::Level;
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
		let inner = NonEmptyStringValueParser::new();
		let v = inner.parse_ref(cmd, arg, value)?;
		let filter = (match v.as_str() {
			// Don't show any logs at all
			"none" => Ok(EnvFilter::default()),
			// Check if we should show all log levels
			"full" => Ok(EnvFilter::default().add_directive(Level::TRACE.into())),
			// Otherwise, let's only show errors
			"error" => Ok(EnvFilter::default().add_directive(Level::ERROR.into())),
			// Specify the log level for each code area
			"warn" | "info" | "debug" | "trace" => EnvFilter::builder()
				.parse(format!("error,surreal={v},surrealdb={v},surrealdb::txn=error")),
			// Let's try to parse the custom log level
			_ => EnvFilter::builder().parse(v),
		})
		.map_err(|e| {
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
