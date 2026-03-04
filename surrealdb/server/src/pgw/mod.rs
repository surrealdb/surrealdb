mod results;
mod simple_query;

use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::{
	DefaultServerParameterProvider, StartupHandler, save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use surrealdb_core::kvs::Datastore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use self::simple_query::SurrealQueryHandler;

struct SurrealStartupHandler;

#[async_trait]
impl StartupHandler for SurrealStartupHandler {
	async fn on_startup<C>(
		&self,
		client: &mut C,
		message: PgWireFrontendMessage,
	) -> PgWireResult<()>
	where
		C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
		C::Error: Debug,
		PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
	{
		if let PgWireFrontendMessage::Startup(ref startup) = message {
			save_startup_parameters_to_metadata(client, startup);
		}
		client.set_state(pgwire::api::PgWireConnectionState::ReadyForQuery);
		pgwire::api::auth::finish_authentication(client, &DefaultServerParameterProvider::default())
			.await
	}
}

struct SurrealPgServer {
	handler: Arc<SurrealQueryHandler>,
	startup: Arc<SurrealStartupHandler>,
}

#[allow(refining_impl_trait)]
impl PgWireServerHandlers for SurrealPgServer {
	fn simple_query_handler(&self) -> Arc<SurrealQueryHandler> {
		self.handler.clone()
	}

	fn extended_query_handler(&self) -> Arc<SurrealQueryHandler> {
		self.handler.clone()
	}

	fn startup_handler(&self) -> Arc<SurrealStartupHandler> {
		self.startup.clone()
	}
}

pub async fn init(
	bind: SocketAddr,
	datastore: Arc<Datastore>,
	canceller: CancellationToken,
) -> Result<()> {
	let server = Arc::new(SurrealPgServer {
		handler: Arc::new(SurrealQueryHandler::new(datastore)),
		startup: Arc::new(SurrealStartupHandler),
	});

	let listener = TcpListener::bind(bind).await?;
	info!("PostgreSQL wire protocol listening on {}", bind);

	loop {
		tokio::select! {
			_ = canceller.cancelled() => {
				info!("pgwire server shutting down");
				break;
			}
			result = listener.accept() => {
				match result {
					Ok((socket, addr)) => {
						let srv = server.clone();
						tokio::spawn(async move {
							if let Err(e) = process_socket(socket, None, srv).await {
								error!("pgwire connection error from {addr}: {e}");
							}
						});
					}
					Err(e) => {
						error!("pgwire accept error: {e}");
					}
				}
			}
		}
	}

	Ok(())
}
