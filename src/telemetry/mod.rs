mod logs;
pub mod metrics;
pub mod traces;

use std::time::Duration;

use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use once_cell::sync::Lazy;
use opentelemetry::metrics::MetricsError;
use opentelemetry::sdk::resource::{
	EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};
use opentelemetry::sdk::Resource;
use opentelemetry::{Context as TelemetryContext, KeyValue};
use tracing::{Level, Subscriber};
use tracing_subscriber::prelude::*;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

pub static OTEL_DEFAULT_RESOURCE: Lazy<Resource> = Lazy::new(|| {
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
	/// Set the log level on the builder
	pub fn with_log_level(mut self, log_level: &str) -> Self {
		if let Ok(filter) = filter_from_value(log_level) {
			self.filter = CustomEnvFilter(filter);
		}
		self
	}

	/// Set the filter on the builder
	pub fn with_filter(mut self, filter: CustomEnvFilter) -> Self {
		self.filter = filter;
		self
	}

	/// Build a tracing dispatcher with the fmt subscriber (logs) and the chosen tracer subscriber
	pub fn build(self) -> Box<dyn Subscriber + Send + Sync + 'static> {
		let registry = tracing_subscriber::registry();

		// Setup logging layer
		let registry = registry.with(logs::new(self.filter.clone()));

		// Setup tracing layer
		let registry = registry.with(traces::new(self.filter));

		Box::new(registry)
	}

	/// Install the tracing dispatcher globally
	pub fn init(self) {
		self.build().init();
	}
}

pub fn shutdown() -> Result<(), MetricsError> {
	// Flush all telemetry data
	opentelemetry::global::shutdown_tracer_provider();
	metrics::shutdown(&TelemetryContext::current())?;

	Ok(())
}

/// Create an EnvFilter from the given value. If the value is not a valid log level, it will be treated as EnvFilter directives.
pub fn filter_from_value(v: &str) -> Result<EnvFilter, tracing_subscriber::filter::ParseError> {
	match v {
		// Don't show any logs at all
		"none" => Ok(EnvFilter::default()),
		// Check if we should show all log levels
		"full" => Ok(EnvFilter::default().add_directive(Level::TRACE.into())),
		// Otherwise, let's only show errors
		"error" => Ok(EnvFilter::default().add_directive(Level::ERROR.into())),
		// Specify the log level for each code area
		"warn" | "info" | "debug" | "trace" => EnvFilter::builder()
			.parse(format!("error,surreal={v},surrealdb={v},surrealdb::kvs::tx=error")),
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
			let otlp_endpoint = format!("http://{}", addr);
			temp_env::with_vars(
				vec![
					("SURREAL_TRACING_TRACER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter = telemetry::builder().with_log_level("info").build().set_default();

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
			let otlp_endpoint = format!("http://{}", addr);
			temp_env::with_vars(
				vec![
					("SURREAL_TRACING_TRACER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter = telemetry::builder().with_log_level("debug").build().set_default();

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
