use tracing::Subscriber;
use tracing_subscriber::Layer;

pub mod otlp;

const TRACING_TRACER_VAR: &str = "SURREAL_TRACING_TRACER";

// Returns a tracer based on the value of the TRACING_TRACER_VAR env var
pub fn new<S>() -> Option<Box<dyn Layer<S> + Send + Sync>>
where
	S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
	match std::env::var(TRACING_TRACER_VAR).unwrap_or_default().trim().to_ascii_lowercase().as_str()
	{
		// If no tracer is selected, init with the fmt subscriber only
		"noop" | "" => {
			debug!("No tracer selected");
			None
		}
		// Init the registry with the OTLP tracer
		"otlp" => {
			debug!("Setup the OTLP tracer");
			Some(otlp::new())
		}
		tracer => {
			panic!("unsupported tracer {}", tracer);
		}
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
			Ok(tonic::Response::new(ExportTraceServiceResponse {}))
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
