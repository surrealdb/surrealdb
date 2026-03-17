mod results;
mod simple_query;

use std::fmt::Debug;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use futures::{Sink, SinkExt};
use pgwire::api::auth::{
	DefaultServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
	save_startup_parameters_to_metadata,
};
use pgwire::api::{ClientInfo, METADATA_USER, PgWireConnectionState, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::startup::Authentication;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use pgwire::tokio::process_socket;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use self::simple_query::SurrealQueryHandler;

pub(crate) type SessionMap = DashMap<SocketAddr, Session>;

struct SurrealAuthStartupHandler {
	datastore: Arc<Datastore>,
	sessions: Arc<SessionMap>,
}

fn pg_fatal(sqlstate: &str, message: impl ToString) -> PgWireError {
	PgWireError::UserError(Box::new(ErrorInfo::new(
		"FATAL".to_owned(),
		sqlstate.to_owned(),
		message.to_string(),
	)))
}

fn parse_dbname(database: &str) -> Result<(String, String), PgWireError> {
	if let Some((ns_part, db_part)) = database.split_once('.') {
		if ns_part.is_empty() || db_part.is_empty() {
			return Err(pg_fatal(
				"08004",
				"database must be specified as namespace.database (e.g. dbname=myns.mydb)",
			));
		}
		Ok((ns_part.to_string(), db_part.to_string()))
	} else {
		Err(pg_fatal(
			"08004",
			"database must be specified as namespace.database (e.g. dbname=myns.mydb)",
		))
	}
}

#[async_trait]
impl StartupHandler for SurrealAuthStartupHandler {
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
		match message {
			PgWireFrontendMessage::Startup(ref startup) => {
				protocol_negotiation(client, startup).await?;
				save_startup_parameters_to_metadata(client, startup);
				client.set_state(PgWireConnectionState::AuthenticationInProgress);
				client
					.send(PgWireBackendMessage::Authentication(Authentication::CleartextPassword))
					.await?;
				Ok(())
			}
			PgWireFrontendMessage::PasswordMessageFamily(pwd_msg) => {
				let password = pwd_msg.into_password()?;
				let user = client.metadata().get(METADATA_USER).cloned().unwrap_or_default();
				let addr = client.socket_addr();

				let (ns, db) =
					if let Some(database) = client.metadata().get(pgwire::api::METADATA_DATABASE) {
						let (ns, db) = parse_dbname(database)?;
						(Some(ns), Some(db))
					} else {
						(None, None)
					};

				let pass = &password.password;

				let session =
					try_authenticate(&self.datastore, &user, pass, ns.as_deref(), db.as_deref())
						.await;

				// Fall back to an anonymous session when auth is disabled, matching
				// the HTTP handler's behaviour for --unauthenticated mode.
				let session = match session {
					Some(s) => s,
					None if !self.datastore.is_auth_enabled() => Session::default(),
					None => return Err(pg_fatal("28P01", "password authentication failed")),
				};

				let mut session = session;
				session.ip = Some(addr.ip().to_string());
				session.id = Some(uuid::Uuid::new_v4());
				session.ns = ns;
				session.db = db;
				self.sessions.insert(addr, session);
				client.set_state(PgWireConnectionState::ReadyForQuery);
				finish_authentication(client, &DefaultServerParameterProvider::default()).await
			}
			_ => Ok(()),
		}
	}
}

struct SurrealPgServer {
	handler: Arc<SurrealQueryHandler>,
	startup: Arc<SurrealAuthStartupHandler>,
}

#[allow(refining_impl_trait)]
impl PgWireServerHandlers for SurrealPgServer {
	fn simple_query_handler(&self) -> Arc<SurrealQueryHandler> {
		self.handler.clone()
	}

	fn extended_query_handler(&self) -> Arc<SurrealQueryHandler> {
		self.handler.clone()
	}

	fn startup_handler(&self) -> Arc<SurrealAuthStartupHandler> {
		self.startup.clone()
	}
}

fn build_tls_acceptor(crt: &PathBuf, key: &PathBuf) -> Result<pgwire::tokio::TlsAcceptor> {
	use rustls::pki_types::pem::PemObject;
	use rustls::pki_types::{CertificateDer, PrivateKeyDer};

	let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(crt)
		.map_err(|e| anyhow::anyhow!("failed to read certificate file: {e}"))?
		.collect::<Result<Vec<_>, _>>()
		.map_err(|e| anyhow::anyhow!("failed to parse certificate PEM: {e}"))?;
	let private_key = PrivateKeyDer::from_pem_file(key)
		.map_err(|e| anyhow::anyhow!("failed to read private key file: {e}"))?;
	let config = rustls::ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(certs, private_key)
		.map_err(|e| anyhow::anyhow!("failed to build TLS config: {e}"))?;
	Ok(Arc::new(config).into())
}

pub async fn init(
	bind: SocketAddr,
	datastore: Arc<Datastore>,
	canceller: CancellationToken,
	tls: Option<(PathBuf, PathBuf)>,
) -> Result<()> {
	let sessions: Arc<SessionMap> = Arc::new(DashMap::new());

	let tls_acceptor = match tls {
		Some((ref crt, ref key)) => {
			let acceptor = build_tls_acceptor(crt, key)?;
			info!("pgwire TLS enabled");
			Some(acceptor)
		}
		None => None,
	};

	let server = Arc::new(SurrealPgServer {
		handler: Arc::new(SurrealQueryHandler::new(datastore.clone(), sessions.clone())),
		startup: Arc::new(SurrealAuthStartupHandler {
			datastore,
			sessions: sessions.clone(),
		}),
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
						let sess_map = sessions.clone();
						let tls = tls_acceptor.clone();
						tokio::spawn(async move {
							let result = process_socket(socket, tls, srv).await;
							sess_map.remove(&addr);
							if let Err(e) = result {
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

/// Authenticate against the datastore, trying the most specific level first.
///
/// When `ns` and `db` are provided the cascade is database -> namespace -> root.
/// This prevents accidental privilege escalation when a namespace or database
/// user happens to share credentials with a higher-level user.
///
/// As a fallback, if the password looks like a JWT token (three dot-separated
/// base64 segments), token-based authentication is attempted via
/// `iam::verify::token`.
async fn try_authenticate(
	ds: &Datastore,
	user: &str,
	pass: &str,
	ns: Option<&str>,
	db: Option<&str>,
) -> Option<Session> {
	// Try the most specific level first, then fall back to broader scopes.
	if let Some(ns) = ns {
		if let Some(db) = db {
			let mut session = Session::default();
			if surrealdb_core::iam::verify::basic(ds, &mut session, user, pass, Some(ns), Some(db))
				.await
				.is_ok()
			{
				return Some(session);
			}
		}
		let mut session = Session::default();
		if surrealdb_core::iam::verify::basic(ds, &mut session, user, pass, Some(ns), None)
			.await
			.is_ok()
		{
			return Some(session);
		}
	}
	// Try root-level auth
	let mut session = Session::default();
	if surrealdb_core::iam::verify::basic(ds, &mut session, user, pass, None, None).await.is_ok() {
		return Some(session);
	}
	// If the password looks like a JWT, try token-based auth as a final fallback.
	if pass.split('.').count() == 3 {
		let mut session = Session::default();
		if surrealdb_core::iam::verify::token(ds, &mut session, pass).await.is_ok() {
			return Some(session);
		}
	}
	None
}
