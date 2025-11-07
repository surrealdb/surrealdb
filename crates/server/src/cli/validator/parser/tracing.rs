use std::collections::HashMap;

use clap::builder::{NonEmptyStringValueParser, PossibleValue, TypedValueParser};
use clap::error::{ContextKind, ContextValue, ErrorKind};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{DynFilterFn, LevelFilter};

use crate::telemetry::{filter_from_value, span_filters_from_value};

#[derive(Debug)]
pub struct CustomFilter {
	pub(crate) env: EnvFilter,
	pub(crate) spans: HashMap<String, LevelFilter>,
}

impl Clone for CustomFilter {
	fn clone(&self) -> Self {
		Self {
			env: EnvFilter::builder().parse(self.env.to_string()).unwrap(),
			spans: self.spans.clone(),
		}
	}
}

impl CustomFilter {
	pub fn env(&self) -> EnvFilter {
		EnvFilter::builder().parse(self.env.to_string()).unwrap()
	}

	pub fn span_filter<S>(
		self,
	) -> DynFilterFn<
		S,
		impl Fn(&tracing::Metadata<'_>, &tracing_subscriber::layer::Context<'_, S>) -> bool + Clone,
	>
	where
		S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
	{
		tracing_subscriber::filter::dynamic_filter_fn(move |meta, cx| {
			if let Some(level) = self.spans.get(meta.name()) {
				return *meta.level() <= *level;
			}
			let mut current = cx.lookup_current();
			while let Some(span) = current {
				if let Some(level) = self.spans.get(span.name()) {
					return *meta.level() <= *level;
				}
				current = span.parent();
			}
			true
		})
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

		let spans = span_filters_from_value(input.as_str()).into_iter().collect();
		// Return the custom targets
		Ok(CustomFilter {
			env: filter,
			spans,
		})
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
