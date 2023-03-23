use opentelemetry::sdk::{trace::Tracer, Resource};
use opentelemetry::trace::TraceError;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;

pub fn oltp() -> Result<Tracer, TraceError> {
	let resource = Resource::new(vec![KeyValue::new("service.name", "surrealdb")]);

	opentelemetry_otlp::new_pipeline()
		.tracing()
		.with_exporter(opentelemetry_otlp::new_exporter().tonic().with_env())
		.with_trace_config(opentelemetry::sdk::trace::config().with_resource(resource))
		.install_batch(opentelemetry::runtime::Tokio)
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
		let addr: SocketAddr = "[::1]:0".parse().unwrap();
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