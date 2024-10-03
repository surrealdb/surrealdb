mod logs;
pub mod metrics;
pub mod traces;

use crate::cli::validator::parser::env_filter::CustomEnvFilter;
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
use tracing_subscriber::filter::ParseError;
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
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
	filter: CustomEnvFilter,
}

pub fn builder() -> Builder {
	Builder::default()
}

impl Default for Builder {
	fn default() -> Self {
		Self {
			filter: CustomEnvFilter(EnvFilter::default()),
		}
	}
}

impl Builder {
	/// Install the tracing dispatcher globally
	pub fn init(self) -> Result<(), Error> {
		// Setup logs, tracing, and metrics
		self.build()?.init();
		// Everything ok
		Ok(())
	}

	/// Set the log filter on the builder
	pub fn with_filter(mut self, filter: CustomEnvFilter) -> Self {
		self.filter = filter;
		self
	}

	/// Set the log level on the builder
	pub fn with_log_level(mut self, log_level: &str) -> Self {
		if let Ok(filter) = filter_from_value(log_level) {
			self.filter = CustomEnvFilter(filter);
		}
		self
	}

	/// Build a tracing dispatcher with the logs and tracer subscriber
	pub fn build(&self) -> Result<Box<dyn Subscriber + Send + Sync + 'static>, Error> {
		// Setup a registry for composing layers
		let registry = tracing_subscriber::registry();
		// Setup logging layer
		let registry = registry.with(logs::new(self.filter.clone())?);
		// Setup tracing layer
		let registry = registry.with(traces::new(self.filter.clone())?);
		// Setup the metrics layer
		if let Some(provider) = metrics::init()? {
			global::set_meter_provider(provider);
		}
		// Return the registry
		Ok(Box::new(registry))
	}
}

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
		"debug" => EnvFilter::builder().parse(
			"warn,surreal=debug,surrealdb=debug,surrealcs=warn,surrealdb::core::kvs::tr=debug",
		),
		// Specify the log level for each code area
		"trace" => EnvFilter::builder().parse(
			"warn,surreal=trace,surrealdb=trace,surrealcs=warn,surrealdb::core::kvs::tr=debug",
		),
		// Check if we should show all surreal logs
		"full" => EnvFilter::builder().parse(
			"debug,surreal=trace,surrealdb=trace,surrealcs=debug,surrealdb::core::kvs::tr=trace",
		),
		// Check if we should show all module logs
		"all" => Ok(EnvFilter::default().add_directive(Level::TRACE.into())),
		// Let's try to parse the custom log level
		_ => EnvFilter::builder().parse(v),
	}
}

#[cfg(test)]
mod tests {
	use opentelemetry::global::shutdown_tracer_provider;
	use tracing::{span, Level};
	use tracing_subscriber::util::SubscriberInitExt;

	use crate::telemetry;

	#[tokio::test(flavor = "multi_thread")]
	async fn test_otlp_tracer() {
		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = telemetry::traces::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{addr}");
			temp_env::with_vars(
				vec![
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter =
						telemetry::builder().with_log_level("info").build().unwrap().set_default();

					println!("Sending span...");

					{
						let span = span!(Level::INFO, "test-surreal-span");
						let _enter = span.enter();
						info!("test-surreal-event");
					}

					shutdown_tracer_provider();
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
			temp_env::with_vars(
				vec![
					("SURREAL_TELEMETRY_PROVIDER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter =
						telemetry::builder().with_log_level("debug").build().unwrap().set_default();

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
