pub mod rpc;

use crate::cli::validator::parser::env_filter::CustomEnvFilter;
use crate::cnf::{TELEMETRY_PROVIDER, TELEMETRY_DISABLE_TRACING};
use crate::err::Error;
use crate::telemetry::OTEL_DEFAULT_RESOURCE;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::SpanExporterBuilder;
use opentelemetry_sdk::trace::{Config, TracerProvider};
use tracing::Subscriber;
use tracing_subscriber::Layer;

// Returns a tracer provider based on the SURREAL_TELEMETRY_PROVIDER environment variable
pub fn new<S>(filter: CustomEnvFilter) -> Result<Option<Box<dyn Layer<S> + Send + Sync>>, Error>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	let tracing_disabled = match TELEMETRY_DISABLE_TRACING.trim() {
		"true" => true,
		_ => false,
	};

	match TELEMETRY_PROVIDER.trim() {
		// The OTLP telemetry provider has been specified
		s if s.eq_ignore_ascii_case("otlp") && tracing_disabled => {
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
					.with_filter(filter.0)
					.boxed(),
			))
		}
		// No matching telemetry provider was found
		_ => Ok(None),
	}
}

#[cfg(test)]
pub mod tests {
	use futures::StreamExt;
	use opentelemetry_proto::tonic::collector::trace::v1::{
		trace_service_server::{TraceService, TraceServiceServer},
		ExportTraceServiceRequest, ExportTraceServiceResponse,
	};
	use std::{net::SocketAddr, sync::Mutex};
	use tokio::sync::mpsc;
	use tokio_stream::wrappers::TcpListenerStream;

	/// Server that mocks a TraceService and receives traces
	struct MockServer {
		tx: Mutex<mpsc::Sender<ExportTraceServiceRequest>>,
	}

	impl MockServer {
		pub fn new(tx: mpsc::Sender<ExportTraceServiceRequest>) -> Self {
			Self {
				tx: Mutex::new(tx),
			}
		}
	}

	#[tonic::async_trait]
	impl TraceService for MockServer {
		async fn export(
			&self,
			request: tonic::Request<ExportTraceServiceRequest>,
		) -> Result<tonic::Response<ExportTraceServiceResponse>, tonic::Status> {
			self.tx.lock().unwrap().try_send(request.into_inner()).expect("Channel full");
			Ok(tonic::Response::new(ExportTraceServiceResponse {
				partial_success: None,
			}))
		}
	}

	pub async fn mock_otlp_server() -> (SocketAddr, mpsc::Receiver<ExportTraceServiceRequest>) {
		let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
		let listener = tokio::net::TcpListener::bind(addr).await.expect("failed to bind");
		let addr = listener.local_addr().unwrap();
		let stream = TcpListenerStream::new(listener).map(|s| {
			if let Ok(ref s) = s {
				println!("Got new conn at {}", s.peer_addr().unwrap());
			}
			s
		});

		let (req_tx, req_rx) = mpsc::channel(10);
		let service = TraceServiceServer::new(MockServer::new(req_tx));
		tokio::task::spawn(async move {
			tonic::transport::Server::builder()
				.add_service(service)
				.serve_with_incoming(stream)
				.await
				.expect("Server failed");
		});
		(addr, req_rx)
	}
}
