pub mod rpc;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporterBuilder;
use opentelemetry_sdk::trace::{Config, TracerProvider};
use tracing::Subscriber;
use tracing_subscriber::Layer;

use crate::cli::validator::parser::tracing::CustomFilter;
use crate::cnf::{TELEMETRY_DISABLE_TRACING, TELEMETRY_PROVIDER};
use crate::telemetry::OTEL_DEFAULT_RESOURCE;

// Returns a tracer provider based on the SURREAL_TELEMETRY_PROVIDER environment
// variable
pub fn new<S>(filter: CustomFilter) -> Result<Option<Box<dyn Layer<S> + Send + Sync>>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	match TELEMETRY_PROVIDER.trim() {
		// The OTLP telemetry provider has been specified
		s if s.eq_ignore_ascii_case("otlp") && !*TELEMETRY_DISABLE_TRACING => {
			// Create a new OTLP exporter using gRPC
			let exporter = opentelemetry_otlp::new_exporter().tonic();
			// Build a new span exporter which uses gRPC
			let span_exporter = SpanExporterBuilder::Tonic(exporter).build_span_exporter()?;
			// Define the OTEL metadata configuration
			let config = Config::default().with_resource(OTEL_DEFAULT_RESOURCE.clone());
			// Create the provider with the Tokio runtime
			let provider = TracerProvider::builder()
				.with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
				.with_config(config)
				.build();
			// Set it as the global tracer provider
			let _ = opentelemetry::global::set_tracer_provider(provider.clone());
			// Return the tracing layer with the specified filter
			Ok(Some(
				tracing_opentelemetry::layer()
					.with_tracer(provider.tracer("surealdb"))
					.with_filter(filter.env())
					.with_filter(filter.span_filter::<S>())
					.boxed(),
			))
		}
		// No matching telemetry provider was found
		_ => Ok(None),
	}
}
