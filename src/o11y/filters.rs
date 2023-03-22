use std::env;

use tracing::Level;
use tracing_subscriber::EnvFilter;

const LOG_FILTER_VAR: &str = "SURREAL_LOG_FILTER";
const TRACING_FILTER_VAR: &str = "SURREAL_TRACING_FILTER";

/// Create a filter for the fmt subscriber
///
/// It creates an EnvFilter based on the LOG_FILTER_VAR's value. If it's empty or not set, use the default directives.
///
/// LOG_FILTER_VAR accepts the same syntax as RUST_LOG
pub fn fmt(default_dirs: String) -> EnvFilter {
    let dirs = match env::var(LOG_FILTER_VAR) {
        Ok(value) if !value.trim().is_empty() => value,
        Ok(_) | Err(_) => default_dirs, // If the value is empty or value couldn't be read
    };

    EnvFilter::builder()
                .parse(dirs.clone())
                .expect(format!("error parsing directives `{}`", dirs).as_str())
}

/// Create a filter for the OTLP subscriber
///
/// It creates an EnvFilter based on the TRACING_FILTER_VAR's value
///
/// TRACING_FILTER_VAR accepts the same syntax as RUST_LOG
pub fn otlp() -> EnvFilter {
    EnvFilter::builder()
        .with_env_var(TRACING_FILTER_VAR)
        .with_default_directive(Level::INFO.into())
        .from_env()
        .unwrap()
}
