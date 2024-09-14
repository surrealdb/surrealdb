use opentelemetry::trace::TraceError;
use opentelemetry_otlp::SpanExporterBuilder;
use opentelemetry_sdk::trace::{Config, TracerProvider};

use crate::telemetry::OTEL_DEFAULT_RESOURCE;

pub(super) fn build_tracer_provider() -> Result<TracerProvider, TraceError> {
	let exporter = opentelemetry_otlp::new_exporter().tonic();
	let span_exporter = SpanExporterBuilder::Tonic(exporter).build_span_exporter()?;
	let config = Config::default().with_resource(OTEL_DEFAULT_RESOURCE.clone());

	let provider = TracerProvider::builder()
		.with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
		.with_config(config)
		.build();

	Ok(provider)
}
