mod logs;
pub mod metrics;
pub mod traces;

use crate::cli::validator::parser::tracing::CustomFilter;
use crate::cli::LogFormat;
use crate::err::Error;
use opentelemetry::global;
use opentelemetry::KeyValue;
use opentelemetry_sdk::resource::{
	EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry_sdk::Resource;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::NonBlockingBuilder;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::{LevelFilter, ParseError};
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

pub static OTEL_DEFAULT_RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
	// Set the default otel metadata if available
	let res = Resource::from_detectors(
		Duration::from_secs(5),
		vec![
			// set service.name from env OTEL_SERVICE_NAME > env OTEL_RESOURCE_ATTRIBUTES > option_env! CARGO_BIN_NAME > unknown_service
			Box::new(SdkProvidedResourceDetector),
			// detect res from env OTEL_RESOURCE_ATTRIBUTES (resources string like key1=value1,key2=value2,...)
			Box::new(EnvResourceDetector::new()),
			// set telemetry.sdk.{name, language, version}
			Box::new(TelemetryResourceDetector),
		],
	);
	// If no external service.name is set, set it to surrealdb
	if res.get("service.name".into()).unwrap_or("".into()).as_str() == "unknown_service" {
		res.merge(&Resource::new([KeyValue::new("service.name", "surrealdb")]))
	} else {
		res
	}
});

#[derive(Debug, Clone)]
pub struct Builder {
	format: LogFormat,
	filter: CustomFilter,
	file_filter: Option<CustomFilter>,
	otel_filter: Option<CustomFilter>,
	log_file_enabled: bool,
	log_file_format: LogFormat,
	log_file_path: Option<String>,
	log_file_name: Option<String>,
	log_file_rotation: Option<String>,
}

pub fn builder() -> Builder {
	Builder::default()
}

impl Default for Builder {
	fn default() -> Self {
		Self {
			filter: CustomFilter {
				env: EnvFilter::default(),
				spans: std::collections::HashMap::new(),
			},
			format: LogFormat::Text,
			file_filter: None,
			otel_filter: None,
			log_file_format: LogFormat::Text,
			log_file_enabled: false,
			log_file_path: Some("logs".to_string()),
			log_file_name: Some("surrealdb.log".to_string()),
			log_file_rotation: Some("daily".to_string()),
		}
	}
}

impl Builder {
	/// Install the tracing dispatcher globally
	#[allow(clippy::result_large_err)]
	pub fn init(self) -> Result<Vec<WorkerGuard>, Error> {
		// Setup logs, tracing, and metrics
		let (registry, guards) = self.build()?;
		// Initialise the registry
		registry.init();
		// Everything ok
		Ok(guards)
	}

	/// Set the log filter on the builder
	pub fn with_filter(mut self, filter: CustomFilter) -> Self {
		self.filter = filter;
		self
	}

	/// Set the log level on the builder
	pub fn with_log_level(mut self, log_level: &str) -> Self {
		if let Ok(filter) = filter_from_value(log_level) {
			self.filter = CustomFilter {
				env: filter,
				spans: std::collections::HashMap::new(),
			};
		}
		self
	}

	/// Set a custom log filter for file output
	pub fn with_file_filter(mut self, filter: Option<CustomFilter>) -> Self {
		self.file_filter = filter;
		self
	}

	/// Set a custom log filter for otel output
	pub fn with_otel_filter(mut self, filter: Option<CustomFilter>) -> Self {
		self.otel_filter = filter;
		self
	}

	/// Set the terminal log output format
	pub fn with_log_format(mut self, format: LogFormat) -> Self {
		self.format = format;
		self
	}

	/// Enable or disable the log file
	pub fn with_log_file_enabled(mut self, enabled: bool) -> Self {
		self.log_file_enabled = enabled;
		self
	}

	/// Set the log file output format
	pub fn with_log_file_format(mut self, format: LogFormat) -> Self {
		self.log_file_format = format;
		self
	}

	/// Set the log file path
	pub fn with_log_file_path(mut self, path: Option<String>) -> Self {
		self.log_file_path = path;
		self
	}

	/// Set the log file name
	pub fn with_log_file_name(mut self, name: Option<String>) -> Self {
		self.log_file_name = name;
		self
	}

	/// Set the log file rotation interval (daily, hourly, or never)
	pub fn with_log_file_rotation(mut self, rotation: Option<String>) -> Self {
		self.log_file_rotation = rotation;
		self
	}

	/// Build a tracing dispatcher with the logs and tracer subscriber
	#[allow(clippy::result_large_err)]
	pub fn build(
		&self,
	) -> Result<(Box<dyn Subscriber + Send + Sync + 'static>, Vec<WorkerGuard>), Error> {
		// Setup the metrics layer
		if let Some(provider) = metrics::init()? {
			global::set_meter_provider(provider);
		}
		// Create a non-blocking stdout log destination
		let (stdout, stdout_guard) = NonBlockingBuilder::default()
			.lossy(true)
			.thread_name("surrealdb-logger-stdout")
			.finish(std::io::stdout());
		// Create a non-blocking stderr log destination
		let (stderr, stderr_guard) = NonBlockingBuilder::default()
			.lossy(true)
			.thread_name("surrealdb-logger-stderr")
			.finish(std::io::stderr());
		// Create the display destination layer
		let output_layer = logs::output(self.filter.clone(), stdout, stderr, self.format)?;
		// Create the otel destination layer
		let telemetry_filter = self.otel_filter.clone().unwrap_or_else(|| self.filter.clone());
		let telemetry_layer = traces::new(telemetry_filter)?;
		// Setup a registry for composing layers
		let registry = tracing_subscriber::registry();
		// Setup output layer
		let registry = registry.with(output_layer);
		// Setup telemetry layer
		let registry = registry.with(telemetry_layer);
		// Setup file logging if enabled
		Ok(if self.log_file_enabled {
			// Create the file appender based on rotation setting
			let file_appender = {
				// Parse the path and name
				let path = self.log_file_path.as_deref().unwrap_or("logs");
				let name = self.log_file_name.as_deref().unwrap_or("surrealdb.log");
				// Create the file appender based on rotation setting
				match self.log_file_rotation.as_deref() {
					Some("hourly") => tracing_appender::rolling::hourly(path, name),
					Some("daily") => tracing_appender::rolling::daily(path, name),
					Some("never") => tracing_appender::rolling::never(path, name),
					_ => tracing_appender::rolling::daily(path, name),
				}
			};
			// Create a non-blocking file log destination
			let (file, file_guard) = NonBlockingBuilder::default()
				.lossy(false)
				.thread_name("surrealdb-logger-file")
				.finish(file_appender);
			// Create the file destination layer
			let file_filter = self.file_filter.clone().unwrap_or_else(|| self.filter.clone());
			let file_layer = logs::file(file_filter, file, self.log_file_format)?;
			// Setup logging layer
			let registry = registry.with(file_layer);
			// Return the registry
			(Box::new(registry), vec![stdout_guard, stderr_guard, file_guard])
		} else {
			// Return the registry
			(Box::new(registry), vec![stdout_guard, stderr_guard])
		})
	}
}

#[allow(clippy::result_large_err)]
pub fn shutdown() -> Result<(), Error> {
	// Output information to logs
	trace!("Shutting down telemetry service");
	// Flush all telemetry data and block until done
	opentelemetry::global::shutdown_tracer_provider();
	// Everything ok
	Ok(())
}

/// Create an EnvFilter from the given value. If the value is not a valid log level, it will be treated as EnvFilter directives.
pub fn filter_from_value(v: &str) -> Result<EnvFilter, ParseError> {
	match v {
		// Don't show any logs at all
		"none" => Ok(EnvFilter::default()),
		// Otherwise, let's show only errors
		"error" => Ok(EnvFilter::default().add_directive(Level::ERROR.into())),
		// Otherwise, let's show warnings and above
		"warn" => Ok(EnvFilter::default().add_directive(Level::WARN.into())),
		// Otherwise, let's show info and above
		"info" => Ok(EnvFilter::default().add_directive(Level::INFO.into())),
		// Otherwise, let's show debugs and above
		"debug" => Ok(EnvFilter::default()
			.add_directive(Level::WARN.into())
			.add_directive("surreal=debug".parse().unwrap())
			.add_directive("surrealdb=debug".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tx=debug".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tr=debug".parse().unwrap())),
		// Specify the log level for each code area
		"trace" => Ok(EnvFilter::default()
			.add_directive(Level::WARN.into())
			.add_directive("surreal=trace".parse().unwrap())
			.add_directive("surrealdb=trace".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tx=debug".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tr=debug".parse().unwrap())),
		// Check if we should show all surreal logs
		"full" => Ok(EnvFilter::default()
			.add_directive(Level::DEBUG.into())
			.add_directive("surreal=trace".parse().unwrap())
			.add_directive("surrealdb=trace".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tx=trace".parse().unwrap())
			.add_directive("surrealdb::core::kvs::tr=trace".parse().unwrap())),
		// Check if we should show all module logs
		"all" => Ok(EnvFilter::default().add_directive(Level::TRACE.into())),
		// Let's try to parse the custom log level
		_ => EnvFilter::builder().parse(v),
	}
}

/// Parse span level directives from the given value.
pub fn span_filters_from_value(v: &str) -> Vec<(String, LevelFilter)> {
	v.split(',')
		.filter_map(|d| {
			let d = d.trim();
			if !d.starts_with('[') {
				return None;
			}
			let close = d.find(']')?;
			let name = &d[1..close];
			let level = d[close + 1..].trim();
			let level = if let Some(stripped) = level.strip_prefix('=') {
				stripped.parse().ok()?
			} else {
				LevelFilter::TRACE
			};
			Some((name.to_string(), level))
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use std::{ffi::OsString, sync::Mutex};

	use crate::telemetry;
	use opentelemetry::global::shutdown_tracer_provider;
	use tracing::{span, Level};
	use tracing_subscriber::util::SubscriberInitExt;

	static ENV_MUTEX: Mutex<()> = Mutex::new(());

	fn with_vars<K, V, F, R>(vars: &[(K, Option<V>)], f: F) -> R
	where
		F: FnOnce() -> R,
		K: AsRef<str>,
		V: AsRef<str>,
	{
		let _guard = ENV_MUTEX.lock();

		let mut restore = Vec::new();

		for (k, v) in vars {
			restore.push((k.as_ref().to_string(), std::env::var_os(k.as_ref())));
			if let Some(x) = v {
				std::env::set_var(k.as_ref(), x.as_ref());
			} else {
				std::env::remove_var(k.as_ref());
			}
		}

		struct Dropper(Vec<(String, Option<OsString>)>);
		impl Drop for Dropper {
			fn drop(&mut self) {
				for (k, v) in self.0.drain(..) {
					if let Some(v) = v {
						std::env::set_var(k, v);
					} else {
						std::env::remove_var(k);
					}
				}
			}
		}
		let _drop_gaurd = Dropper(restore);
		f()
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_otlp_tracer() {
		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{addr}");
			with_vars(
				&[
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let (registry, guards) =
						telemetry::builder().with_log_level("info").build().unwrap();

					let _enter = registry.set_default();

					println!("Sending span...");

					{
						let span = span!(Level::INFO, "test-surreal-span");
						let _enter = span.enter();
						info!("test-surreal-event");
					}

					shutdown_tracer_provider();
					for guard in guards {
						drop(guard);
					}
				},
			)
		}

		println!("Waiting for request...");
		let req = tokio::select! {
			req = req_rx.recv() => req.expect("missing export request"),
			_ = tokio::time::sleep(std::time::Duration::from_secs(1)) => panic!("timeout waiting for request"),
		};

		let first_span =
			req.resource_spans.first().unwrap().scope_spans.first().unwrap().spans.first().unwrap();
		assert_eq!("test-surreal-span", first_span.name);
		let first_event = first_span.events.first().unwrap();
		assert_eq!("test-surreal-event", first_event.name);
	}

	#[tokio::test(flavor = "multi_thread")]
	async fn test_tracing_filter() {
		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{addr}");
			with_vars(
				&[
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let (registry, guards) =
						telemetry::builder().with_log_level("debug").build().unwrap();

					let _enter = registry.set_default();

					println!("Sending spans...");

					{
						let span = span!(Level::DEBUG, "debug");
						let _enter = span.enter();
						debug!("debug");
						trace!("trace");
					}

					{
						let span = span!(Level::TRACE, "trace");
						let _enter = span.enter();
						debug!("debug");
						trace!("trace");
					}

					shutdown_tracer_provider();
					for guard in guards {
						drop(guard);
					}
				},
			)
		}

		println!("Waiting for request...");
		let req = tokio::select! {
			req = req_rx.recv() => req.expect("missing export request"),
			_ = tokio::time::sleep(std::time::Duration::from_secs(1)) => panic!("timeout waiting for request"),
		};
		let spans = &req.resource_spans.first().unwrap().scope_spans.first().unwrap().spans;

		assert_eq!(1, spans.len());
		assert_eq!("debug", spans.first().unwrap().name);

		let events = &spans.first().unwrap().events;
		assert_eq!(1, events.len());
		assert_eq!("debug", events.first().unwrap().name);
	}
}
