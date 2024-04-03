use opentelemetry::trace::TraceError;
use opentelemetry_otlp::SpanExporterBuilder;
use opentelemetry_sdk::trace::TracerProvider;

use crate::telemetry::OTEL_DEFAULT_RESOURCE;

pub(super) fn build_tracer_provider() -> Result<TracerProvider, TraceError> {
	let exporter = opentelemetry_otlp::new_exporter().tonic().with_env();
	let span_exporter = SpanExporterBuilder::Tonic(exporter).build_span_exporter()?;
	Ok(TracerProvider::builder()
		.with_batch_exporter(span_exporter, opentelemetry_sdk::runtime::Tokio)
		.with_config(
			opentelemetry_sdk::trace::config().with_resource(OTEL_DEFAULT_RESOURCE.clone()),
		)
		.build())
}
