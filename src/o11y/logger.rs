use std::env;

use tracing::{Level, Subscriber};
use tracing_subscriber::{EnvFilter, Layer};

const LOG_FILTER_VAR: &str = "SURREAL_LOG_FILTER";

pub fn new<S>(log_level: String) -> Box<dyn Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	tracing_subscriber::fmt::layer()
		.with_writer(std::io::stderr)
		.with_filter(filter(log_level))
		.boxed()
}

/// Create a filter for the fmt subscriber
///
/// It creates an EnvFilter based on the LOG_FILTER_VAR's value. If it's empty or not set, use the default directives.
///
/// LOG_FILTER_VAR accepts the same syntax as RUST_LOG
fn filter(log_level: String) -> EnvFilter {
	let default = match log_level.as_str() {
		"warn" | "info" | "debug" | "trace" => {
			format!("error,surreal={},surrealdb={}", log_level, log_level)
		}
		"full" => Level::TRACE.to_string(),
		_ => Level::ERROR.to_string(),
	};

	let dirs = match env::var(LOG_FILTER_VAR) {
		Ok(value) if !value.trim().is_empty() => value,
		Ok(_) | Err(_) => default, // If the value is empty or value couldn't be read, use directives based on given log_level
	};

	EnvFilter::builder()
		.parse(dirs.clone())
		.expect(format!("error parsing directives `{}`", dirs).as_str())
}
