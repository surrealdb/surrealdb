pub mod filters;
pub mod tracers;

use tracing::Level;
use tracing_subscriber::prelude::*;

const TRACING_TRACER_VAR: &str = "SURREAL_TRACING_TRACER";

#[derive(Default, Debug, Clone)]
pub struct Builder {
	log_filter: String,
}

pub fn builder() -> Builder {
	Builder::default()
}

impl Builder {
	/// Translates the given log_level into a log_filter
	pub fn with_log_level(self, log_level: &str) -> Self {
		self.with_log_filter(match log_level {
			"error" => Level::ERROR.to_string(),
			"warn" | "info" | "debug" | "trace" => {
				format!("error,surreal={},surrealdb={}", log_level, log_level)
			}
			"full" => Level::TRACE.to_string(),
			_ => unreachable!(),
		})
	}

	pub fn with_log_filter(mut self, filter: String) -> Self {
		self.log_filter = filter;
		self
	}

	/// Setup the global tracing with the fmt subscriber (logs) and the chosen tracer subscriber
	pub fn init(self) {
		let tracing_registry = tracing_subscriber::registry()
			// Create the fmt subscriber for printing the tracing Events as logs to the stdout
			.with(
				tracing_subscriber::fmt::layer()
					.with_writer(std::io::stderr)
					.with_filter(filters::fmt(self.log_filter)),
			);

		// Init the tracing_registry with the selected tracer. If no tracer is provided, init without one
		match std::env::var(TRACING_TRACER_VAR)
			.unwrap_or_default()
			.trim()
			.to_ascii_lowercase()
			.as_str()
		{
			// If no tracer is selected, init with the fmt subscriber only
			"noop" | "" => {
				tracing_registry.init();
				debug!("No tracer defined");
			}
			// Init the registry with the OTLP tracer
			"otlp" => {
				tracing_registry
					.with(
						tracing_opentelemetry::layer()
							.with_tracer(tracers::oltp().unwrap())
							.with_filter(filters::otlp()),
					)
					.init();
				debug!("OTLP tracer setup");
			}
			tracer => {
				panic!("unsupported tracer {}", tracer);
			}
		};
	}
}


#[cfg(test)]
mod tests {
	use opentelemetry::{global::{shutdown_tracer_provider, tracer as global_tracer}, trace::{Tracer, Span, SpanKind}};

	#[tokio::test(flavor = "multi_thread")]
	async fn test_otlp_tracer() {
		println!("Starting server setup...");
		let (addr, mut req_rx) = super::tracers::tests::mock_otlp_server().await;
	
		{
			let otlp_endpoint = format!("http://{}", addr);
			temp_env::with_vars(vec![
				("SURREAL_TRACING_TRACER", Some("otlp")),
				("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str()))
			], || {
				super::builder().init();

				println!("Sending span...");
				let tracer = global_tracer("global tracer");
				let mut span = tracer
					.span_builder("test-surreal-span")
					.with_kind(SpanKind::Server)
					.start(&tracer);
				span.add_event("test-surreal-event", vec![]);
				span.end();

				shutdown_tracer_provider();
			})
		}
	
		println!("Waiting for request...");
		let req = req_rx.recv().await.expect("missing export request");
		let first_span = req
			.resource_spans
			.first()
			.unwrap()
			.instrumentation_library_spans
			.first()
			.unwrap()
			.spans
			.first()
			.unwrap();
		assert_eq!("test-surreal-span", first_span.name);
		let first_event = first_span.events.first().unwrap();
		assert_eq!("test-surreal-event", first_event.name);
	}
}
