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

/// Result of a successful tracing layer build: the
/// [`tracing_subscriber::Layer`] consumed by the registry plus the
/// [`SdkTracerProvider`] that the
/// [`ObservabilityRuntime`](crate::observe::ObservabilityRuntime) holds
/// for the lifetime of the server.
///
/// The provider must be retained alongside the registry: dropping it
/// shuts the batch span processor down and stops draining queued spans
/// to the exporter.
pub struct TraceLayer<S> {
	pub layer: Box<dyn Layer<S> + Send + Sync>,
	pub provider: SdkTracerProvider,
}

/// Build the OTLP trace layer + provider when
/// `SURREAL_TELEMETRY_PROVIDER=otlp` is configured. Returns `None` when
/// tracing is disabled or the provider env var is unset.
pub fn new<S>(filter: CustomFilter) -> Result<Option<TraceLayer<S>>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	match TELEMETRY_PROVIDER.trim() {
		// The OTLP telemetry provider has been specified
		s if s.eq_ignore_ascii_case("otlp") && !*TELEMETRY_DISABLE_TRACING => {
			// Build a new span exporter which uses gRPC via tonic with native TLS
			// roots so that HTTPS OTLP collector endpoints work out of the box.
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
			// Build the tracing-opentelemetry layer pointing at this
			// provider's tracer. The provider clone returned alongside
			// is retained by the
			// [`ObservabilityRuntime`](crate::observe::ObservabilityRuntime),
			// which keeps the batch span processor alive for the
			// process lifetime.
			let layer: Box<dyn Layer<S> + Send + Sync> = tracing_opentelemetry::layer()
				.with_tracer(provider.tracer("surealdb"))
				.with_filter(filter.env())
				.with_filter(filter.span_filter::<S>())
				.boxed();
			Ok(Some(TraceLayer {
				layer,
				provider,
			}))
		}
		// No matching telemetry provider was found
		_ => Ok(None),
	}
}
