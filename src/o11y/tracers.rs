use opentelemetry::sdk::{trace::Tracer, Resource};
use opentelemetry::trace::TraceError;
use opentelemetry::KeyValue;

pub fn oltp() -> Result<Tracer, TraceError> {
	let resource = Resource::new(vec![KeyValue::new("service.name", "surrealdb")]);

	opentelemetry_otlp::new_pipeline()
		.tracing()
		.with_exporter(opentelemetry_otlp::new_exporter().tonic())
		.with_trace_config(opentelemetry::sdk::trace::config().with_resource(resource))
		.install_batch(opentelemetry::runtime::Tokio)
}
