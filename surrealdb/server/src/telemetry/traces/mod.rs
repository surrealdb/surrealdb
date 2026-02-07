pub mod rpc;

use anyhow::Result;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithTonicConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::transport::ClientTlsConfig;
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
			// Build a new span exporter which uses gRPC via tonic
			let span_exporter = opentelemetry_otlp::SpanExporter::builder()
				.with_tonic()
				.with_tls_config(ClientTlsConfig::new().with_native_roots())
				.build()?;
			// Create a batch span processor with the exporter (uses Tokio runtime automatically)
			let batch_processor =
				opentelemetry_sdk::trace::BatchSpanProcessor::builder(span_exporter).build();
			// Create the provider
			let provider = SdkTracerProvider::builder()
				.with_span_processor(batch_processor)
				.with_resource(OTEL_DEFAULT_RESOURCE.clone())
				.build();
			// Set it as the global tracer provider
			opentelemetry::global::set_tracer_provider(provider.clone());
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
