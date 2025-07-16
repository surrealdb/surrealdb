use crate::{
	api::{opt::Endpoint},
	engine::local::{
		grpc::{ConnectionsState, SurrealDBGrpcService},
		middleware::SessionManagementLayer,
	},
	opt::auth::Root,
};
use anyhow::Context;
use bytes::Bytes;
use futures::Stream;
use http::{Request, Response};
use tonic::{body::BoxBody, service::Routes, transport::server::Connected};
use tower::{Layer, Service};

use std::{sync::Arc, time::Duration};
use surrealdb_core::{dbs::SurrealDB, iam::Level, kvs::Datastore};
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbServiceServer;
use tokio::io::{AsyncRead, AsyncWrite};

pub async fn serve<I, IO, IE>(
	// The transport channel for the gRPC server. This will be a DuplexStream when running the server locally and a TcpStream when running as a remote instance.
	incoming: I,

	datastore: Arc<Datastore>,
) -> anyhow::Result<()>
where
	I: Stream<Item = Result<IO, IE>>,
	IO: AsyncRead + AsyncWrite + Connected + Unpin + Send + 'static,
	IO::ConnectInfo: Clone + Send + Sync + 'static,
	IE: Into<Box<dyn std::error::Error + Send + Sync>>,
	// L: Layer<Routes>,
	// L::Service:
	// 	Service<Request<BoxBody>, Response = Response<ResBody>> + Clone + Send + 'static,
	// <<L as Layer<Routes>>::Service as Service<Request<BoxBody>>>::Future: Send + 'static,
	// <<L as Layer<Routes>>::Service as Service<Request<BoxBody>>>::Error:
	// 	Into<Box<dyn std::error::Error + Send + Sync>> + Send,
	// ResBody: http_body::Body<Data = Bytes> + Send + 'static,
	// ResBody::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
	let state = Arc::new(ConnectionsState::default());

	let service = SurrealDbServiceServer::new(SurrealDBGrpcService::new(datastore, state))
		.max_encoding_message_size(usize::MAX)
		.max_decoding_message_size(usize::MAX);

	tonic::transport::Server::builder()
		.max_concurrent_streams(10000) // Increased from 1000
		.concurrency_limit_per_connection(10000) // Increased from 1000
		.http2_adaptive_window(Some(true))
		.http2_max_pending_accept_reset_streams(Some(10000)) // Increased
		.http2_keepalive_interval(Some(Duration::from_secs(30))) // Reduced from 60
		.http2_keepalive_timeout(Some(Duration::from_secs(10))) // Reduced from 20
		.tcp_keepalive(Some(Duration::from_secs(30))) // Reduced from 60
		.tcp_nodelay(true)
		.layer(SessionManagementLayer {})
		.add_service(service)
		.serve_with_incoming(incoming)
		.await
		.context("Failed to serve gRPC server")
}
