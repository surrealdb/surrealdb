mod logger;
mod tracers;

use tracing::Subscriber;
use tracing_subscriber::{prelude::*, util::SubscriberInitExt};

#[derive(Default, Debug, Clone)]
pub struct Builder {
	log_level: String,
}

pub fn builder() -> Builder {
	Builder::default()
}

impl Builder {
	/// Set the log level on the builder
	pub fn with_log_level(mut self, log_level: &str) -> Self {
		self.log_level = log_level.to_string();
		self
	}
	/// Build a dispatcher with the fmt subscriber (logs) and the chosen tracer subscriber
	pub fn build(self) -> Box<dyn Subscriber + Send + Sync + 'static> {
		Box::new(
			tracing_subscriber::registry().with(logger::new(self.log_level)).with(tracers::new()),
		)
	}
	/// Build a dispatcher and set it as global
	pub fn init(self) {
		self.build().init()
	}
}

#[cfg(test)]
mod tests {
	use opentelemetry::global::shutdown_tracer_provider;
	use tracing::{span, Level};
	use tracing_subscriber::util::SubscriberInitExt;

	#[tokio::test(flavor = "multi_thread")]
	async fn test_otlp_tracer() {
		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = super::tracers::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{}", addr);
			temp_env::with_vars(
				vec![
					("SURREAL_TRACING_TRACER", Some("otlp")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter = super::builder().build().set_default();

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

	#[tokio::test(flavor = "multi_thread")]
	async fn test_tracing_filter() {
		println!("Starting mock otlp server...");
		let (addr, mut req_rx) = super::tracers::tests::mock_otlp_server().await;

		{
			let otlp_endpoint = format!("http://{}", addr);
			temp_env::with_vars(
				vec![
					("SURREAL_TRACING_TRACER", Some("otlp")),
					("SURREAL_TRACING_FILTER", Some("debug")),
					("OTEL_EXPORTER_OTLP_ENDPOINT", Some(otlp_endpoint.as_str())),
				],
				|| {
					let _enter = super::builder().build().set_default();

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
		let req = req_rx.recv().await.expect("missing export request");
		let spans = &req
			.resource_spans
			.first()
			.unwrap()
			.instrumentation_library_spans
			.first()
			.unwrap()
			.spans;

		assert_eq!(1, spans.len());
		assert_eq!("debug", spans.first().unwrap().name);

		let events = &spans.first().unwrap().events;
		assert_eq!(1, events.len());
		assert_eq!("debug", events.first().unwrap().name);
	}
}
