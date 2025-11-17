use std::error::Error as StdError;
use std::io;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_core::kvs::Datastore;
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::{
	SurrealDbService, SurrealDbServiceServer,
};
use surrealdb_protocol::proto::rpc::v1::{
	AuthenticateRequest, AuthenticateResponse, ExportMlModelRequest, ExportMlModelResponse,
	ExportSqlRequest, ExportSqlResponse, HealthRequest, HealthResponse, ImportSqlRequest,
	ImportSqlResponse, InvalidateRequest, InvalidateResponse, QueryRequest, QueryResponse,
	ResetRequest, ResetResponse, SetRequest, SetResponse, SigninRequest, SigninResponse,
	SignupRequest, SignupResponse, SubscribeRequest, SubscribeResponse, UnsetRequest,
	UnsetResponse, UseRequest, UseResponse, VersionRequest, VersionResponse,
};
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::cli::Config;

const LOG: &str = "surrealdb::grpc";

/// Initialize and start the gRPC server.
///
/// Sets up the Tonic gRPC server with the SurrealDB service implementation.
///
/// # Parameters
/// - `opt`: Server configuration including bind address and TLS settings
/// - `ds`: The datastore instance to serve
/// - `ct`: Cancellation token for graceful shutdown
pub async fn init(opt: &Config, ds: Arc<Datastore>, ct: CancellationToken) -> Result<()> {
	// Create the gRPC service implementation
	let grpc_service = SurrealDbGrpcService {
		_datastore: ds,
	};

	// Build the Tonic server
	let mut server = Server::builder();

	// If a certificate and key are specified, then setup TLS
	if let (Some(cert), Some(key)) = (&opt.crt, &opt.key) {
		// Read the certificate and key files
		let cert_pem = tokio::fs::read(cert).await?;
		let key_pem = tokio::fs::read(key).await?;

		// Create TLS identity from certificate and key
		let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);

		// Configure TLS for the server
		let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);

		// Apply TLS configuration
		server = server
			.tls_config(tls_config)
			.map_err(|e| anyhow::anyhow!("Failed to configure TLS: {}", e))?;

		// Log the server startup
		info!(target: LOG, "Started gRPC server on {} (TLS enabled)", &opt.grpc_bind);
	} else {
		// Log the server startup without TLS
		info!(target: LOG, "Started gRPC server on {}", &opt.grpc_bind);
	}

	// Add the service and start the server
	let res = server
		.add_service(SurrealDbServiceServer::new(grpc_service))
		.serve_with_shutdown(opt.grpc_bind, async move {
			ct.cancelled().await;
		})
		.await;

	// Catch the error and try to provide some guidance
	if let Err(e) = res {
		if opt.grpc_bind.port() < 1024 {
			if let Some(io_err) = e.source().and_then(|s| s.downcast_ref::<io::Error>()) {
				if let io::ErrorKind::PermissionDenied = io_err.kind() {
					error!(target: LOG, "Binding to ports below 1024 requires privileged access or special permissions.");
				}
			}
		}
		return Err(e.into());
	}

	// Log the server shutdown
	info!(target: LOG, "gRPC server stopped. Bye!");

	Ok(())
}

pub struct SurrealDbGrpcService {
	_datastore: Arc<Datastore>,
}

#[tonic::async_trait]
impl SurrealDbService for SurrealDbGrpcService {
	// Pin<Box<dyn Stream<Item = Result<EchoResponse, Status>> + Send>>;
	type ExportSqlStream =
		Pin<Box<dyn Stream<Item = Result<ExportSqlResponse, Status>> + Send + Sync>>;
	type ExportMlModelStream =
		Pin<Box<dyn Stream<Item = Result<ExportMlModelResponse, Status>> + Send + Sync>>;
	type QueryStream = Pin<Box<dyn Stream<Item = Result<QueryResponse, Status>> + Send + Sync>>;
	type SubscribeStream =
		Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send + Sync>>;

	async fn health(
		&self,
		_request: Request<HealthRequest>,
	) -> Result<Response<HealthResponse>, Status> {
		todo!()
	}

	async fn version(
		&self,
		_request: Request<VersionRequest>,
	) -> Result<Response<VersionResponse>, Status> {
		todo!()
	}

	async fn signup(
		&self,
		_request: Request<SignupRequest>,
	) -> Result<Response<SignupResponse>, Status> {
		todo!()
	}

	async fn signin(
		&self,
		_request: Request<SigninRequest>,
	) -> Result<Response<SigninResponse>, Status> {
		todo!()
	}

	async fn authenticate(
		&self,
		_request: Request<AuthenticateRequest>,
	) -> Result<Response<AuthenticateResponse>, Status> {
		todo!()
	}

	async fn r#use(&self, _request: Request<UseRequest>) -> Result<Response<UseResponse>, Status> {
		todo!()
	}

	async fn set(&self, _request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
		todo!()
	}

	async fn unset(
		&self,
		_request: Request<UnsetRequest>,
	) -> Result<Response<UnsetResponse>, Status> {
		todo!()
	}

	async fn invalidate(
		&self,
		_request: Request<InvalidateRequest>,
	) -> Result<Response<InvalidateResponse>, Status> {
		todo!()
	}

	async fn reset(
		&self,
		_request: Request<ResetRequest>,
	) -> Result<Response<ResetResponse>, Status> {
		todo!()
	}

	async fn import_sql(
		&self,
		_request: Request<Streaming<ImportSqlRequest>>,
	) -> Result<Response<ImportSqlResponse>, Status> {
		todo!()
	}

	async fn export_sql(
		&self,
		_request: Request<ExportSqlRequest>,
	) -> Result<Response<Self::ExportSqlStream>, Status> {
		todo!()
	}

	async fn export_ml_model(
		&self,
		_request: Request<ExportMlModelRequest>,
	) -> Result<Response<Self::ExportMlModelStream>, Status> {
		todo!()
	}

	async fn query(
		&self,
		_request: Request<QueryRequest>,
	) -> Result<Response<Self::QueryStream>, Status> {
		todo!()
	}

	async fn subscribe(
		&self,
		_request: Request<SubscribeRequest>,
	) -> Result<Response<Self::SubscribeStream>, Status> {
		todo!()
	}
}
