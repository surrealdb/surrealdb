use crate::{
	api::{
		ExtraFeatures, Result, Surreal, conn,
		method::BoxFuture,
		opt::{Endpoint, EndpointKind},
	},
	engine::{
		local::{
			grpc::{ConnectionsState, SurrealDBGrpcService},
			middleware::SessionManagementLayer,
		},
		tasks,
	},
	opt::{WaitFor, auth::Root},
};
use anyhow::Context;
use async_channel::{Receiver, Sender};
use futures::{StreamExt, stream::poll_fn};
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::{Arc, atomic::AtomicI64},
	task::Poll,
};
use surrealdb_core::{dbs::Session, iam::Level, kvs::Datastore, options::EngineOptions};
use surrealdb_protocol::proto::rpc::v1::surreal_db_service_server::SurrealDbServiceServer;
use tokio::{
	io::DuplexStream,
	sync::{RwLock, watch},
};
use tokio_util::sync::CancellationToken;

pub(crate) async fn serve(
	// The transport channel for the gRPC server. This will be a DuplexStream when running the server locally and a TcpStream when running as a remote instance.
	channel: DuplexStream,
	// The address of the database to connect to. This will be a URL when running as a remote instance and a path when running locally.
	address: Endpoint,
) -> Result<()> {
	let id = uuid::Uuid::new_v4();
	let session = Session::default().with_rt(true);

	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: &address.config.username,
			password: &address.config.password,
		}),
		_ => None,
	};

	let path = address.url.as_str();

	let kvs = match Datastore::new(path).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				return Err(error);
			};
			if let Err(error) = kvs.bootstrap().await {
				return Err(error);
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = configured_root {
				if let Err(error) = kvs.initialise_credentials(root.username, root.password).await {
					return Err(error);
				}
			}
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			return Err(error);
		}
	};

	let state = Arc::new(ConnectionsState::default());
	let canceller = CancellationToken::new();
	let shutdown = CancellationToken::new();

	let service = SurrealDbServiceServer::new(SurrealDBGrpcService::new(
		id,
		Arc::new(kvs),
		state,
		canceller,
		shutdown,
	));

	let server_task = tokio::spawn(async move {
		tonic::transport::Server::builder()
			.layer(SessionManagementLayer {})
			.add_service(service)
			.serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(channel)))
			.await
			.context("Failed to serve gRPC server")
	});

	server_task.await?
}
