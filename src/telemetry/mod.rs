mod console;
mod logs;
pub mod metrics;
pub mod traces;

use crate::cli::LogFormat;
use crate::cli::validator::parser::tracing::CustomFilter;
use crate::cnf::ENABLE_TOKIO_CONSOLE;
use anyhow::{Result, anyhow};
use opentelemetry::{KeyValue, global};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::resource::{
	EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use std::net::ToSocketAddrs;
use std::sync::LazyLock;
use std::time::Duration;
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{LevelFilter, ParseError};
use tracing_subscriber::prelude::*;

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
	socket: Option<String>,
	// Filter options
	file_filter: Option<CustomFilter>,
	otel_filter: Option<CustomFilter>,
	socket_filter: Option<CustomFilter>,
	// Socket options
	socket_format: LogFormat,
	// File options
	file_enabled: bool,
	file_format: LogFormat,
	file_path: Option<String>,
	file_name: Option<String>,
	file_rotation: Option<String>,
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
			socket: None,
			// Filter options
			file_filter: None,
			otel_filter: None,
			socket_filter: None,
			// Socket options
			socket_format: LogFormat::Text,
			// File options
			file_format: LogFormat::Text,
			file_enabled: false,
			file_path: Some("logs".to_string()),
			file_name: Some("surrealdb.log".to_string()),
			file_rotation: Some("daily".to_string()),
		}
	}
}

impl Builder {
	/// Install the tracing dispatcher globally
	pub fn init(self) -> Result<Vec<WorkerGuard>> {
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

	/// Set a custom log filter for socket output
	pub fn with_socket_filter(mut self, filter: Option<CustomFilter>) -> Self {
		self.socket_filter = filter;
		self
	}

	/// Send logs to the provided socket address
	pub fn with_socket(mut self, socket: Option<String>) -> Self {
		self.socket = socket;
		self
	}

	/// Set the terminal log output format
	pub fn with_log_format(mut self, format: LogFormat) -> Self {
		self.format = format;
		self
	}

	/// Set the log file output format
	pub fn with_file_format(mut self, format: LogFormat) -> Self {
		self.file_format = format;
		self
	}

	/// Set the terminal log output format
	pub fn with_socket_format(mut self, format: LogFormat) -> Self {
		self.format = format;
		self
	}

	/// Enable or disable the log file
	pub fn with_file_enabled(mut self, enabled: bool) -> Self {
		self.file_enabled = enabled;
		self
	}

	/// Set the log file path
	pub fn with_file_path(mut self, path: Option<String>) -> Self {
		self.file_path = path;
		self
	}

	/// Set the log file name
	pub fn with_file_name(mut self, name: Option<String>) -> Self {
		self.file_name = name;
		self
	}

	/// Set the log file rotation interval (daily, hourly, or never)
	pub fn with_file_rotation(mut self, rotation: Option<String>) -> Self {
		self.file_rotation = rotation;
		self
	}

	/// Build a tracing dispatcher with the logs and tracer subscriber
	pub fn build(&self) -> Result<(Box<dyn Subscriber + Send + Sync + 'static>, Vec<WorkerGuard>)> {
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
		let stdio_layer = logs::output(self.filter.clone(), stdout, stderr, self.format)?;
		// Setup a registry for composing layers
		let registry = tracing_subscriber::registry();
		// Setup stdio destination layer
		let registry = registry.with(stdio_layer);
		// Setup guards
		let mut guards = vec![stdout_guard, stderr_guard];
		// Setup layers
		let mut layers = Vec::new();

		// Setup logging to opentelemetry
		{
			// Get the otel filter or global filter
			let filter = self.otel_filter.clone().unwrap_or_else(|| self.filter.clone());
			// Create the otel destination layer
			if let Some(layer) = traces::new(filter)? {
				// Add the layer to the registry
				layers.push(layer);
			}
		}

		// Setup logging to socket if enabled
		if let Some(addr) = &self.socket {
			// Parse the first socket address
			let address =
				addr.to_socket_addrs()?.next().ok_or_else(|| anyhow!("No matching addresses"))?;
			// Connect to the socket address
			let socket = logs::socket::connect(address)?;
			// Create a non-blocking socket log destination
			let (writer, guard) = NonBlockingBuilder::default()
				.lossy(false)
				.thread_name("surrealdb-logger-socket")
				.finish(socket);
			// Get the file filter or global filter
			let filter = self.socket_filter.clone().unwrap_or_else(|| self.filter.clone());
			// Create the socket destination layer
			let layer = logs::file(filter, writer, self.socket_format)?;
			// Add the layer to the registry
			layers.push(layer);
			// Add the guard to the guards
			guards.push(guard);
		}

		// Setup logging to file if enabled
		if self.file_enabled {
			// Create the file appender based on rotation setting
			let file_appender = {
				// Parse the path and name
				let path = self.file_path.as_deref().unwrap_or("logs");
				let name = self.file_name.as_deref().unwrap_or("surrealdb.log");
				// Create the file appender based on rotation setting
				match self.file_rotation.as_deref() {
					Some("hourly") => tracing_appender::rolling::hourly(path, name),
					Some("daily") => tracing_appender::rolling::daily(path, name),
					Some("never") => tracing_appender::rolling::never(path, name),
					_ => tracing_appender::rolling::daily(path, name),
				}
			};
			// Create a non-blocking file log destination
			let (writer, guard) = NonBlockingBuilder::default()
				.lossy(false)
				.thread_name("surrealdb-logger-file")
				.finish(file_appender);
			// Get the file filter or global filter
			let filter = self.file_filter.clone().unwrap_or_else(|| self.filter.clone());
			// Create the file destination layer
			let layer = logs::file(filter, writer, self.file_format)?;
			// Add the layer to the registry
			layers.push(layer);
			// Add the guard to the guards
			guards.push(guard);
		}

		// Setup logging to console if enabled
		if *ENABLE_TOKIO_CONSOLE {
			// Create the console destination layer
			let layer = console::new()?;
			// Add the layer to the registry
			layers.push(layer);
		}

		match layers.len() {
			0 => {
				// Return the registry and guards
				Ok((Box::new(registry), guards))
			}
			_ => {
				// Setup the registry layers
				let registry = registry.with(layers);
				// Return the registry and guards
				Ok((Box::new(registry), guards))
			}
		}
	}
}

pub fn shutdown() {
	// Output information to logs
	trace!("Shutting down telemetry service");
	// Flush all telemetry data and block until done
	opentelemetry::global::shutdown_tracer_provider();
}

/// Create an EnvFilter from the given value. If the value is not a valid log level, it will be treated as EnvFilter directives.
pub fn filter_from_value(v: &str) -> std::result::Result<EnvFilter, ParseError> {
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
	use crate::telemetry;
	use opentelemetry::global::shutdown_tracer_provider;
	use std::{ffi::OsString, time::Duration};
	use tracing::{Level, span};
	use tracing_subscriber::util::SubscriberInitExt;

	/// Helper function to ensure proper telemetry cleanup and reset
	async fn cleanup_telemetry() {
		// Shutdown any existing tracer provider
		shutdown_tracer_provider();
		// Give some time for cleanup
		tokio::time::sleep(std::time::Duration::from_millis(1_000)).await;
	}

	async fn with_vars<K, V, F, R>(vars: &[(K, Option<V>)], f: F) -> R
	where
		F: AsyncFnOnce() -> R,
		K: AsRef<str>,
		V: AsRef<str>,
	{
		let mut restore = Vec::new();

		// Dropper to restore the environment variables
		struct Dropper(Vec<(String, Option<OsString>)>);

		// Ensure variables are restored when finished
		impl Drop for Dropper {
			fn drop(&mut self) {
				for (k, v) in self.0.drain(..) {
					if let Some(v) = v {
						unsafe { std::env::set_var(k, v) };
					} else {
						unsafe { std::env::remove_var(k) };
					}
				}
			}
		}

		// First, clear any existing telemetry-related environment variables
		let telemetry_vars = [
			"SURREAL_TELEMETRY_PROVIDER",
			"OTEL_EXPORTER_OTLP_ENDPOINT",
			"OTEL_BSP_SCHEDULE_DELAY",
			"OTEL_BSP_EXPORT_TIMEOUT",
			"OTEL_BSP_MAX_QUEUE_SIZE",
		];

		for var in &telemetry_vars {
			restore.push((var.to_string(), std::env::var_os(var)));
			unsafe { std::env::remove_var(var) };
		}

		// Then set the new variables
		for (k, v) in vars {
			restore.push((k.as_ref().to_string(), std::env::var_os(k.as_ref())));
			if let Some(x) = v {
				unsafe { std::env::set_var(k.as_ref(), x.as_ref()) };
			} else {
				unsafe { std::env::remove_var(k.as_ref()) };
			}
		}

		let _guard = Dropper(restore);
		f().await
	}

	#[tokio::test(flavor = "multi_thread")]
	#[serial_test::serial]
	async fn test_mock_server_basic() {
		println!("Testing basic mock server functionality...");
		let (addr, _req_rx) = telemetry::traces::tests::mock_otlp_server().await;
		println!("Mock server started on {}", addr);

		// Give the server a moment to start up
		tokio::time::sleep(std::time::Duration::from_millis(100)).await;

		// Test that the server is reachable
		let client = reqwest::Client::new();
		let response = client.get(format!("http://{}/", addr)).send().await;
		println!("Server response: {:?}", response);

		// The server should not respond to HTTP requests, but we can verify it's running
		assert!(response.is_err() || response.unwrap().status().is_client_error());
	}

	#[tokio::test(flavor = "multi_thread")]
	#[serial_test::serial]
	async fn test_otlp_exporter_direct() {
		// Ensure clean state
		cleanup_telemetry().await;

		println!("Starting mock otlp server...");
		let (addr, _req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		// Test direct OTLP exporter configuration
		with_vars(
			&[
				("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
				("OTEL_EXPORTER_OTLP_ENDPOINT", Some(&format!("http://{addr}"))),
				("OTEL_BSP_SCHEDULE_DELAY", Some("1")),
				("OTEL_BSP_EXPORT_TIMEOUT", Some("1000")),
				("OTEL_BSP_MAX_QUEUE_SIZE", Some("1")),
			],
			async || {
				println!("Environment variables set, testing telemetry builder...");
				match telemetry::builder().with_log_level("info").build() {
					Ok((_registry, _guards)) => {
						println!("Telemetry builder succeeded");
					}
					Err(e) => {
						println!("Telemetry builder failed: {e:?}");
						panic!("Telemetry builder failed: {e:?}");
					}
				}
			},
		)
		.await;
	}

	#[tokio::test(flavor = "multi_thread")]
	#[serial_test::serial]
	async fn test_otlp_tracer() {
		// Ensure clean state
		cleanup_telemetry().await;

		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{addr}");
			with_vars(
				&[
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
					// Add explicit configuration to ensure immediate export
					("OTEL_BSP_SCHEDULE_DELAY", Some("1")),
					("OTEL_BSP_EXPORT_TIMEOUT", Some("1000")),
					("OTEL_BSP_MAX_QUEUE_SIZE", Some("1")),
				],
				async || {
					let (registry, _guards) =
						telemetry::builder().with_log_level("info").build().unwrap();

					let _enter = registry.set_default();

					println!("Sending span...");

					{
						let span = span!(Level::INFO, "test-surreal-span");
						let _enter = span.enter();
						info!("test-surreal-event");
					}

					// Force flush the telemetry data
					cleanup_telemetry().await;
				},
			)
			.await;
		}

		println!("Waiting for request...");
		let req = tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
			.await
			.expect("timeout waiting for request")
			.expect("missing export request");

		let first_span =
			req.resource_spans.first().unwrap().scope_spans.first().unwrap().spans.first().unwrap();
		assert_eq!("test-surreal-span", first_span.name);
		let first_event = first_span.events.first().unwrap();
		assert_eq!("test-surreal-event", first_event.name);
	}

	#[tokio::test(flavor = "multi_thread")]
	#[serial_test::serial]
	async fn test_otlp_filter() {
		// Ensure clean state
		cleanup_telemetry().await;

		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{addr}");
			with_vars(
				&[
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
					// Add explicit configuration to ensure immediate export
					("OTEL_BSP_SCHEDULE_DELAY", Some("1")),
					("OTEL_BSP_EXPORT_TIMEOUT", Some("1000")),
					("OTEL_BSP_MAX_QUEUE_SIZE", Some("1")),
				],
				async || {
					let (registry, _guards) =
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

					// Force flush the telemetry data
					cleanup_telemetry().await;
				},
			)
			.await;
		}

		println!("Waiting for request...");
		let req = tokio::time::timeout(Duration::from_secs(5), req_rx.recv())
			.await
			.expect("timeout waiting for request")
			.expect("missing export request");

		let spans = &req.resource_spans.first().unwrap().scope_spans.first().unwrap().spans;

		assert_eq!(1, spans.len());
		assert_eq!("debug", spans.first().unwrap().name);

		let events = &spans.first().unwrap().events;
		assert_eq!(1, events.len());
		assert_eq!("debug", events.first().unwrap().name);
	}

	#[tokio::test(flavor = "multi_thread")]
	#[serial_test::serial]
	async fn test_log_to_socket() {
		use std::io::Read;
		use std::net::TcpListener;

		let listener = TcpListener::bind("127.0.0.1:0").unwrap();
		let addr = listener.local_addr().unwrap();

		let handle = std::thread::spawn(move || {
			let (mut stream, _) = listener.accept().unwrap();
			let mut buf = Vec::new();
			stream.read_to_end(&mut buf).unwrap();
			buf
		});

		let (registry, guards) = telemetry::builder()
			.with_socket(Some(addr.to_string()))
			.with_log_level("all")
			.build()
			.unwrap();

		let _enter = registry.set_default();
		info!("socket-output");

		for guard in guards {
			drop(guard);
		}

		let bytes = handle.join().unwrap();
		let msg = String::from_utf8_lossy(&bytes);
		assert!(msg.contains("socket-output"));
	}
}
