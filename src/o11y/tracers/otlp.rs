use opentelemetry::sdk::{trace::Tracer, Resource};
use opentelemetry::trace::TraceError;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use tracing::{Level, Subscriber};
use tracing_subscriber::{EnvFilter, Layer};

const TRACING_FILTER_VAR: &str = "SURREAL_TRACING_FILTER";

pub fn new<S>() -> Box<dyn Layer<S> + Send + Sync>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	tracing_opentelemetry::layer().with_tracer(tracer().unwrap()).with_filter(filter()).boxed()
}

fn tracer() -> Result<Tracer, TraceError> {
	let resource = Resource::new(vec![KeyValue::new("service.name", "surrealdb")]);

	opentelemetry_otlp::new_pipeline()
		.tracing()
		.with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
		.with_trace_config(opentelemetry::sdk::trace::config().with_resource(resource))
		.install_batch(opentelemetry::runtime::Tokio)
}

/// Create a filter for the OTLP subscriber
///
/// It creates an EnvFilter based on the TRACING_FILTER_VAR's value
///
/// TRACING_FILTER_VAR accepts the same syntax as RUST_LOG
fn filter() -> EnvFilter {
	EnvFilter::builder()
		.with_env_var(TRACING_FILTER_VAR)
		.with_default_directive(Level::INFO.into())
		.from_env()
		.unwrap()
}
