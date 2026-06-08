pub mod audit_logs;
mod console;
mod logs;
pub mod metrics;
pub mod traces;

use std::net::ToSocketAddrs;
use std::sync::{LazyLock, OnceLock};

use anyhow::{Result, anyhow};
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use tracing::{Level, Subscriber};
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::{LevelFilter, ParseError};
use tracing_subscriber::prelude::*;

use crate::cli::LogFormat;
use crate::cli::validator::parser::tracing::CustomFilter;
use crate::cnf::ENABLE_TOKIO_CONSOLE;
use crate::observe::ObservabilityRuntime;

/// Process-wide OTel `Resource` shared by every `SdkMeterProvider`,
/// `SdkLoggerProvider`, and `SdkTracerProvider` built by the server.
///
/// Carries:
/// - `service.name = "surrealdb"`.
/// - `service.edition = "community" | "enterprise"`. Build-flavour identifier set once at process
///   start via [`set_service_edition`]; defaults to `"community"` when no override is installed
///   before first read. Surfaces on every Prometheus series via `target_info{service_edition=…}`
///   and on every OTLP export via the resource bundle.
///
/// Read lazily on first telemetry init. Any subsequent call to
/// [`set_service_edition`] after the resource has been materialised is a
/// no-op; the enterprise composer therefore registers its edition BEFORE
/// CLI parsing kicks off the telemetry layer.
pub static OTEL_DEFAULT_RESOURCE: LazyLock<Resource> = LazyLock::new(|| {
	let edition = SERVICE_EDITION.get().copied().unwrap_or(DEFAULT_SERVICE_EDITION);
	// Build resource from environment variables and default attributes.
	// The Resource will automatically merge SDK, environment, and telemetry metadata.
	Resource::builder()
		.with_service_name("surrealdb")
		.with_attribute(KeyValue::new("service.edition", edition))
		.build()
});

/// Default edition surfaced as `service.edition` when the process did not
/// install a build-specific override before telemetry init.
const DEFAULT_SERVICE_EDITION: &str = "community";

/// Build-flavour edition installed by the binary `main`. Set once via
/// [`set_service_edition`] BEFORE the OTel resource bundle is first
/// materialised.
static SERVICE_EDITION: OnceLock<&'static str> = OnceLock::new();

/// Install the build flavour exposed via the `service.edition` resource
/// attribute. Must be called before the telemetry layer initialises (i.e.
/// before [`Builder::init`]); subsequent calls are silently dropped because
/// the resource bundle is built once and shared by every provider.
///
/// The default when no override is installed is `"community"`.
pub fn set_service_edition(edition: &'static str) {
	let _ = SERVICE_EDITION.set(edition);
}

/// Returns the build flavour installed via [`set_service_edition`], or
/// `"community"` when no override has been registered. Lookup is
/// lock-free and suitable for hot-path consumers.
pub fn service_edition() -> &'static str {
	SERVICE_EDITION.get().copied().unwrap_or(DEFAULT_SERVICE_EDITION)
}

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

/// Warns when telemetry environment variables that were once honoured are
/// still set in the process environment. Centralised so future deprecations
/// can be added alongside.
fn warn_removed_env_vars() {
	if std::env::var("SURREAL_TELEMETRY_NAMESPACE").ok().filter(|v| !v.trim().is_empty()).is_some()
	{
		warn!(
			"SURREAL_TELEMETRY_NAMESPACE is set but is no longer applied. \
			 The `namespace` attribute was removed from telemetry metrics \
			 because it is tenant-identifying in multi-tenant deployments. \
			 Remove this variable from your deployment configuration."
		);
	}
	if std::env::var("SURREAL_TELEMETRY_RPC_LIVE_ID")
		.ok()
		.filter(|v| !v.trim().is_empty())
		.is_some()
	{
		warn!(
			"SURREAL_TELEMETRY_RPC_LIVE_ID is set but is no longer applied. \
			 Per-notification OTLP attribution by `rpc.live_id` was removed \
			 when WebSocket telemetry was unified into the ExecutionObserver \
			 pipeline. Notification volume is still surfaced via the \
			 `surrealdb_live_query_notifications_total` Prometheus counter. \
			 Remove this variable from your deployment configuration."
		);
	}
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

/// Result of [`Builder::init`] / [`Builder::build`].
///
/// Carries the log [`WorkerGuard`]s that must outlive the application
/// (dropping them flushes the non-blocking appenders) and the
/// [`ObservabilityRuntime`] every observer reads to register
/// instruments.
pub struct TelemetryHandles {
	/// Log appender guards. Dropping them flushes the non-blocking
	/// stdout / stderr / file / socket appenders, so the [`Vec`] must
	/// outlive the program.
	pub guards: Vec<WorkerGuard>,
	/// Process-local observability handle. Pass into
	/// [`crate::cli::start::init`] (or whichever embedder entrypoint is
	/// being used) so observers register instruments against the same
	/// providers the telemetry layer just configured.
	pub runtime: ObservabilityRuntime,
}

impl Builder {
	/// Install the tracing dispatcher globally and return the log
	/// guards plus the [`ObservabilityRuntime`] every observer reads
	/// to register instruments.
	pub fn init(self) -> Result<TelemetryHandles> {
		// Setup logs, tracing, and metrics
		let (registry, handles) = self.build()?;
		// Initialise the registry
		registry.init();
		// Surface any deprecated telemetry env vars now that a subscriber is
		// installed so the warning is actually delivered.
		warn_removed_env_vars();
		// Everything ok
		Ok(handles)
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

	/// Set the socket log output format
	pub fn with_socket_format(mut self, format: LogFormat) -> Self {
		self.socket_format = format;
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

	/// Build a tracing dispatcher with the logs and tracer subscriber.
	///
	/// Returns the registry boxed as a [`Subscriber`] alongside the
	/// [`TelemetryHandles`] every observer needs (log guards plus the
	/// [`ObservabilityRuntime`] used to register instruments).
	pub fn build(&self) -> Result<(Box<dyn Subscriber + Send + Sync + 'static>, TelemetryHandles)> {
		// Setup the metrics layer. The provider plus any optional
		// Prometheus exporter end up on the runtime returned at the end
		// of this function. No `opentelemetry::global` provider is
		// installed, so multiple embedded server instances can each
		// own their own metrics surface.
		let metrics_init = metrics::init()?;
		// Setup the audit / slow-query logs provider. Returns `None`
		// when OTLP is not configured; emit sites that ask the runtime
		// for an audit logger then collapse to no-ops.
		let audit_logger_provider = audit_logs::init()?;
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
		// Create the display destination layers (separate stdout/stderr)
		let stdio_layers = logs::output(self.filter.clone(), stdout, stderr, self.format)?;
		// Setup a registry for composing layers
		let registry = tracing_subscriber::registry();
		// Setup stdio destination layers
		let registry = registry.with(stdio_layers);
		// Setup guards
		let mut guards = vec![stdout_guard, stderr_guard];
		// Setup layers
		let mut layers = Vec::new();

		// Setup logging to opentelemetry
		let mut tracer_provider = None;
		{
			// Get the otel filter, falling back to a curated minimal
			// default that surfaces the four user-facing spans (HTTP
			// `request`, WS `rpc/call`, `rpc.execute`, `executor`) while
			// keeping the deep core trace-level instrumentation hidden.
			// Operators who want full tracing override this via
			// `SURREAL_LOG_OTEL_LEVEL` (or `--log-otel-level`).
			let filter = self.otel_filter.clone().unwrap_or_else(default_otel_filter);
			// Create the otel destination layer
			if let Some(trace_layer) = traces::new(filter)? {
				// Add the layer to the registry
				layers.push(trace_layer.layer);
				// Retain the provider on the runtime so the batch span
				// processor stays alive for the process lifetime.
				tracer_provider = Some(trace_layer.provider);
				// Install the W3C trace-context propagator globally so
				// inbound `traceparent` / `tracestate` headers can be
				// extracted at the HTTP / WS boundary and used as the
				// OTel parent of the spans the server already emits.
				// Without this, `opentelemetry::global::get_text_map_propagator()`
				// returns a no-op and every server span is the root of a
				// fresh trace, unlinked from the SDK caller.
				opentelemetry::global::set_text_map_propagator(
					opentelemetry_sdk::propagation::TraceContextPropagator::new(),
				);
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

		// Build the runtime that bundles every provider built above.
		// When neither pipeline is configured the metrics path
		// degenerates to a no-op `SdkMeterProvider` so observers can
		// still construct themselves without branching on `Option`.
		let mut runtime_builder = match metrics_init {
			Some(init) => {
				let mut b = ObservabilityRuntime::builder(init.provider)
					.with_resource(OTEL_DEFAULT_RESOURCE.clone());
				if let Some(exporter) = init.prometheus_exporter {
					b = b.with_prometheus_exporter(exporter);
				}
				b
			}
			None => ObservabilityRuntime::builder(SdkMeterProvider::default())
				.with_resource(OTEL_DEFAULT_RESOURCE.clone()),
		};
		if let Some(provider) = audit_logger_provider {
			runtime_builder = runtime_builder.with_audit_logger_provider(provider);
		}
		if let Some(provider) = tracer_provider {
			runtime_builder = runtime_builder.with_tracer_provider(provider);
		}
		let handles = TelemetryHandles {
			guards,
			runtime: runtime_builder.build(),
		};

		match layers.len() {
			0 => {
				// Return the registry and runtime handles
				Ok((Box::new(registry), handles))
			}
			_ => {
				// Setup the registry layers
				let registry = registry.with(layers);
				// Return the registry and runtime handles
				Ok((Box::new(registry), handles))
			}
		}
	}
}

/// Output a final shutdown trace event before the runtime is dropped.
///
/// Provider shutdown (audit log batch processor flush, tracer span
/// processor flush, meter provider flush) lives on
/// [`crate::observe::ObservabilityRuntime::shutdown`] so the embedder
/// owns the order in which providers wind down.
pub fn shutdown() {
	trace!("Shutting down telemetry service");
}

/// Curated default filter for the OTel export layer when
/// `SURREAL_LOG_OTEL_LEVEL` is unset.
///
/// Defaults the world to `info` so the OTLP collector isn't flooded, then
/// raises only the four span targets that an SDK user expects to see
/// when they trace a query through the DB:
///
/// - `surrealdb_server::ntw::tracer` — HTTP `request` span (per HTTP request)
/// - `surrealdb_server::telemetry::traces::rpc` — WS `rpc/call` span (per WebSocket RPC message)
/// - `surrealdb::core::rpc` — `rpc.execute` span (per dispatched RPC method)
/// - `surrealdb::core::dbs` — `executor` span (per query batch)
///
/// Operators wanting the deep nested core instrumentation set
/// `SURREAL_LOG_OTEL_LEVEL=trace` (or any other `EnvFilter` directive
/// string accepted by [`filter_from_value`]).
fn default_otel_filter() -> CustomFilter {
	let env = EnvFilter::default()
		.add_directive(Level::INFO.into())
		.add_directive("surrealdb::core::rpc=debug".parse().expect("static filter directive"))
		.add_directive("surrealdb::core::dbs=debug".parse().expect("static filter directive"))
		.add_directive(
			"surrealdb_server::ntw::tracer=debug".parse().expect("static filter directive"),
		)
		.add_directive(
			"surrealdb_server::telemetry::traces::rpc=debug"
				.parse()
				.expect("static filter directive"),
		);
	CustomFilter {
		env,
		spans: std::collections::HashMap::new(),
	}
}

/// Create an EnvFilter from the given value. If the value is not a valid log
/// level, it will be treated as EnvFilter directives.
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
			.add_directive("surreal=debug".parse()?)
			.add_directive("surrealdb=debug".parse()?)
			.add_directive("surrealdb::core::kvs::tx=debug".parse()?)
			.add_directive("surrealdb::core::kvs::tr=debug".parse()?)),
		// Specify the log level for each code area
		"trace" => Ok(EnvFilter::default()
			.add_directive(Level::WARN.into())
			.add_directive("surreal=trace".parse()?)
			.add_directive("surrealdb=trace".parse()?)
			.add_directive("surrealdb::core::kvs::tx=debug".parse()?)
			.add_directive("surrealdb::core::kvs::tr=debug".parse()?)),
		// Check if we should show all surreal logs
		"full" => Ok(EnvFilter::default()
			.add_directive(Level::DEBUG.into())
			.add_directive("surreal=trace".parse()?)
			.add_directive("surrealdb=trace".parse()?)
			.add_directive("surrealdb::core::kvs::tx=trace".parse()?)
			.add_directive("surrealdb::core::kvs::tr=trace".parse()?)),
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
