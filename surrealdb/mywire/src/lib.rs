//! MySQL wire protocol server implementation for SurrealDB.
//!
//! Lets any standard MySQL client (mysql CLI, JDBC, Python, Go, Node.js, PHP drivers)
//! connect to SurrealDB and execute queries over the MySQL client/server protocol.

#![recursion_limit = "512"]

#[macro_use]
extern crate tracing;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, LazyLock};

use common::time::Instant;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use tls::TlsAcceptor;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "tls")]
pub use tokio_rustls;
use tokio_util::sync::CancellationToken;
#[cfg(feature = "listener")]
use {
	anyhow::{Context as _, Result},
	axum_server::tls_rustls::RustlsConfig,
	std::path::PathBuf,
	std::time::Duration,
	tokio::net::{TcpListener, TcpStream},
	tokio::sync::Semaphore,
	tokio::time::sleep,
	tokio_rustls::rustls::ServerConfig,
};

pub mod auth;
mod cells;
pub mod codec;
pub mod conn;
pub mod constants;
mod error;
pub mod msg;
pub mod query;
mod rsa_auth;
mod show;
pub mod tls;
pub mod typing;
mod uservars;

pub use conn::Connection;
pub use query::translate_query;

pub const LOG: &str = "surrealdb::mywire";

/// Start of the listener, forced by `start`; `COM_STATISTICS` reports its elapsed time as the
/// server uptime.
static STARTED: LazyLock<Instant> = LazyLock::new(Instant::now);

pub(crate) fn uptime() -> std::time::Duration {
	STARTED.elapsed()
}

/// A bidirectional byte stream a connection is served over.
///
/// On native targets the stream is `Send` so a connection can run on a multi-threaded runtime; on
/// `wasm32` it need not be, because the host's sockets are bound to one thread.
#[cfg(not(target_arch = "wasm32"))]
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {}
/// A bidirectional byte stream a connection is served over (single-threaded targets).
#[cfg(target_arch = "wasm32")]
pub trait Io: AsyncRead + AsyncWrite + Unpin + 'static {}

#[cfg(feature = "listener")]
impl Io for TcpStream {}

/// The type-erased stream a connection runs on.
pub type BoxedIo = Pin<Box<dyn Io>>;

/// The future an [`Authenticator`] returns.
#[cfg(not(target_arch = "wasm32"))]
pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = Option<Session>> + Send + 'a>>;
/// The future an [`Authenticator`] returns (single-threaded targets).
#[cfg(target_arch = "wasm32")]
pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = Option<Session>> + 'a>>;

/// What a client presented in the authentication exchange.
pub struct AuthRequest<'a> {
	/// The user name from the handshake response.
	pub user: &'a str,
	/// The password the client sent in clear, which is only asked for over TLS.
	pub password: &'a str,
	/// The namespace the handshake selected, if any.
	pub namespace: Option<&'a str>,
	/// The database the handshake selected, if any.
	pub database: Option<&'a str>,
}

/// Judges the credentials of a connection in place of the datastore's own users.
///
/// `authenticate` receives the session the connection built so far (address and id set, no
/// authentication) and returns it with the authenticated subject, or `None` to refuse. A connection
/// with an authenticator authenticates only by the full exchange, with the password sent in clear
/// over TLS (`caching_sha2_password` full authentication, or `mysql_clear_password`), accepts no
/// loopback exception, never consults or fills the fast-authentication cache, and refuses the
/// anonymous user.
#[cfg(not(target_arch = "wasm32"))]
pub trait Authenticator: Send + Sync {
	fn authenticate<'a>(&'a self, base: &'a Session, request: AuthRequest<'a>) -> AuthFuture<'a>;
}
/// Judges the credentials of a connection in place of the datastore's own users (single-threaded
/// targets).
#[cfg(target_arch = "wasm32")]
pub trait Authenticator {
	fn authenticate<'a>(&'a self, base: &'a Session, request: AuthRequest<'a>) -> AuthFuture<'a>;
}

/// How the stream a connection is served over begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Start {
	/// A fresh connection: the server sends the initial handshake packet.
	Greet,
	/// The initial handshake packet was already sent, with this connection id, before the stream
	/// was handed over; the next bytes are the client's first response (usually an `SSLRequest`).
	Greeted {
		connection_id: u32,
	},
	/// The greeting was sent and the client's TLS was ended before the stream was handed over: the
	/// next bytes are the client's handshake response in clear, which the connection treats as
	/// encrypted. For a host that terminates TLS itself.
	Secured {
		connection_id: u32,
	},
}

/// What `serve` needs beyond the stream and the datastore.
pub struct ServeOptions {
	/// The client's address, used for the session and for logging.
	pub peer: SocketAddr,
	/// The TLS acceptor, or `None` to refuse `SSLRequest`.
	pub tls: Option<Arc<TlsAcceptor>>,
	/// How the stream begins.
	pub start: Start,
	/// Whether the datastore accepts connections.
	pub ready: Arc<AtomicBool>,
	/// Closes the connection when cancelled.
	pub shutdown: CancellationToken,
	/// Judges credentials; `None` uses the datastore's users.
	pub authenticator: Option<Arc<dyn Authenticator>>,
}

/// The authentication nonce of a greeting built by [`greeting_packet`]. A connection handed over
/// with [`Start::Greeted`] never verifies a scramble against it, so it carries no secret.
pub const GREETING_NONCE: [u8; 20] = *b"earth-sql-greeting-1";

/// The initial handshake packet, header included (sequence 0), that a front end sends to the client
/// before handing the connection over with [`Start::Greeted`]. It advertises TLS support, so the
/// client answers with an `SSLRequest`.
pub fn greeting_packet(connection_id: u32) -> Vec<u8> {
	let handshake = msg::HandshakeV10 {
		server_version: surrealdb_sqlplan::MYSQL_SERVER_VERSION.to_string(),
		connection_id,
		auth_plugin_data: GREETING_NONCE,
		capabilities: constants::DEFAULT_SERVER_CAPABILITIES | constants::CLIENT_SSL,
		character_set: constants::CHARSET_UTF8MB4_GENERAL_CI as u8,
		status_flags: constants::SERVER_STATUS_AUTOCOMMIT,
		auth_plugin_name: auth::AUTH_PLUGIN_CACHING_SHA2_PASSWORD.to_string(),
	};
	let payload = handshake.serialize();
	let mut packet = Vec::with_capacity(payload.len() + 4);
	packet.extend_from_slice(&(payload.len() as u32).to_le_bytes()[..3]);
	packet.push(0);
	packet.extend_from_slice(&payload);
	packet
}

/// Serve one MySQL connection over `stream` until it ends.
///
/// The caller owns admission (connection limits) and accepting; this runs the protocol: handshake,
/// TLS upgrade, authentication and the command loop, against `ds`.
#[tracing::instrument(level = "debug", skip_all, fields(peer = %options.peer))]
pub async fn serve<S: Io>(stream: S, ds: Arc<Datastore>, options: ServeOptions) {
	conn::handle(
		Box::pin(stream),
		options.peer,
		ds,
		options.ready,
		options.shutdown,
		None,
		options.tls,
		Arc::new(auth::FastAuthCache::new()),
		options.start,
		options.authenticator,
	)
	.await;
}

#[cfg(feature = "listener")]
const MAX_CONNECTIONS: usize = 1024;

/// Build the TLS acceptor for the MySQL listener from the server certificate
/// configuration.
///
/// MySQL has no ALPN protocol, so the listener advertises none rather than
/// inheriting the HTTP server's defaults. A TLS server that advertises ALPN
/// protocols aborts the handshake with `no_application_protocol` when the
/// client offers a list sharing none of them; with an empty list the client's
/// ALPN offer is ignored and the handshake completes with no protocol selected.
#[cfg(feature = "listener")]
fn mysql_tls_acceptor(config: &RustlsConfig) -> TlsAcceptor {
	let mut server = ServerConfig::clone(&config.get_inner());
	server.alpn_protocols.clear();
	TlsAcceptor::from(Arc::new(server))
}

/// Bind the MySQL wire protocol listener and spawn its accept loop.
///
/// Binding happens before returning so startup fails loudly if the address
/// is unavailable; the accept loop then runs until `shutdown` fires.
#[cfg(feature = "listener")]
pub async fn start(
	addr: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	crt: Option<PathBuf>,
	key: Option<PathBuf>,
) -> Result<SocketAddr> {
	start_with_max_connections(addr, ds, ready, shutdown, crt, key, MAX_CONNECTIONS).await
}

/// Bind the MySQL wire protocol listener with a custom connection limit.
#[cfg(feature = "listener")]
pub async fn start_with_max_connections(
	addr: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	crt: Option<PathBuf>,
	key: Option<PathBuf>,
	max_connections: usize,
) -> Result<SocketAddr> {
	let acceptor = match (crt, key) {
		(Some(crt), Some(key)) => {
			let config = RustlsConfig::from_pem_file(&crt, &key)
				.await
				.with_context(|| "Failed to load the MySQL TLS certificate and key")?;
			Some(Arc::new(mysql_tls_acceptor(&config)))
		}
		_ => {
			warn!(
				target: LOG,
				"The MySQL wire protocol is serving plaintext connections; \
				 configure --web-crt/--web-key to enable TLS"
			);
			None
		}
	};

	let listener = TcpListener::bind(addr)
		.await
		.with_context(|| format!("Failed to bind the MySQL listener on {addr}"))?;

	LazyLock::force(&STARTED);
	let bound_addr = listener.local_addr()?;
	info!(target: LOG, "Started MySQL wire protocol server on {bound_addr}");

	let semaphore = Arc::new(Semaphore::new(max_connections));
	let auth_cache = Arc::new(auth::FastAuthCache::new());

	tokio::spawn(async move {
		let mut backoff = std::time::Duration::from_millis(5);
		loop {
			tokio::select! {
				biased;
				_ = shutdown.cancelled() => {
					info!(target: LOG, "MySQL listener on {bound_addr} shutting down");
					break;
				}
				accept_res = listener.accept() => {
					match accept_res {
						Ok((stream, peer)) => {
							backoff = std::time::Duration::from_millis(5);
							let _ = stream.set_nodelay(true);

							let permit = match Arc::clone(&semaphore).try_acquire_owned() {
								Ok(p) => p,
								Err(_) => {
									warn!(target: LOG, "Max MySQL connections reached ({max_connections}); rejecting {peer}");
									let mut stream = stream;
									let err_pkt = msg::ErrPacket::general(1040, b"08004", "Too many connections");
									let mut writer = codec::PacketWriter::new(&mut stream);
									let _ = writer.write_packet(0, &err_pkt.serialize()).await;
									continue;
								}
							};

							let conn_ds = Arc::clone(&ds);
							let conn_ready = Arc::clone(&ready);
							let conn_shutdown = shutdown.clone();
							let conn_acceptor = acceptor.clone();
							let conn_auth_cache = Arc::clone(&auth_cache);

							tokio::spawn(async move {
								debug!(target: LOG, "Accepted MySQL connection from {peer}");
								conn::handle(
									Box::pin(stream),
									peer,
									conn_ds,
									conn_ready,
									conn_shutdown,
									Some(permit),
									conn_acceptor,
									conn_auth_cache,
									Start::Greet,
									None,
								)
								.await;
							});
						}
						Err(err) => {
							warn!(target: LOG, "Failed to accept MySQL connection on {bound_addr}: {err}");
							backoff = (backoff * 2).min(Duration::from_millis(1000));
							sleep(backoff).await;
						}
					}
				}
			}
		}
	});

	Ok(bound_addr)
}
