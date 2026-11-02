//! Postgres wire protocol (v3.0) listener.
//!
//! Lets any Postgres client (psql, drivers) connect to SurrealDB and run
//! SurrealQL over the simple query protocol, with result columns typed from
//! the values they carry. See the `postgres` feature and the
//! `--postgres-bind` server option.

#![recursion_limit = "512"]

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

use dashmap::DashMap;
use rand::TryRngCore;
use rand::rngs::OsRng;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
pub use surrealdb_core::kvs::Dialect;
use tls::TlsAcceptor;
pub use tokio;
use tokio::io::{AsyncRead, AsyncWrite};
#[cfg(feature = "tls")]
pub use tokio_rustls;
pub use tokio_util;
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

#[macro_use]
extern crate tracing;

mod advisory;
pub mod codec;
pub mod conn;
mod copy;
pub mod encode;
pub mod error;
mod families;
pub(crate) mod guc;
mod maintenance;
pub mod msg;
pub mod sasl;
mod sqlprepare;
pub mod tls;
pub mod typing;
mod vector;

pub const LOG: &str = "surrealdb::pg";
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Maps a connection's (backend pid, secret key) to its cancellation handle so
/// a Postgres CancelRequest on a side connection can interrupt the in-flight
/// query. Shared across all connections of one listener.
pub(crate) type CancelRegistry = DashMap<(i32, i32), CancelHandle>;

/// Monotonic per-process source of backend "process ids". The secret key is
/// drawn from OS entropy (not the seedable `rnd` module) so CancelRequests
/// cannot be forged.
static NEXT_PID: AtomicI32 = AtomicI32::new(1);

fn next_backend_key() -> (i32, i32) {
	let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
	let mut bytes = [0u8; 4];
	let secret = if OsRng.try_fill_bytes(&mut bytes).is_ok() {
		i32::from_ne_bytes(bytes)
	} else {
		rand::random()
	};
	(pid, secret)
}

/// A bidirectional byte stream a connection is served over.
///
/// On native targets the stream is `Send` and `Sync` so a connection can run on a multi-threaded
/// runtime; on `wasm32` it need not be, because the host's sockets are bound to one thread.
#[cfg(not(target_arch = "wasm32"))]
pub trait Io: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static {
	/// Resolves when the peer has closed its end of the connection, if that can be seen without
	/// consuming bytes; the default never resolves. A connection waiting for an advisory lock ends
	/// the wait when this resolves.
	fn peer_closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(std::future::pending())
	}
}
/// A bidirectional byte stream a connection is served over (single-threaded targets).
#[cfg(target_arch = "wasm32")]
pub trait Io: AsyncRead + AsyncWrite + Unpin + 'static {
	/// Resolves when the peer has closed its end of the connection, if that can be seen without
	/// consuming bytes; the default never resolves.
	fn peer_closed(&self) -> Pin<Box<dyn Future<Output = ()> + '_>> {
		Box::pin(std::future::pending())
	}
}

#[cfg(feature = "listener")]
impl Io for TcpStream {
	fn peer_closed(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
		Box::pin(async move {
			let mut byte = [0u8; 1];
			if self.readable().await.is_err() {
				return;
			}
			if matches!(self.peek(&mut byte).await, Ok(read) if read > 0) {
				std::future::pending::<()>().await;
			}
		})
	}
}

/// The type-erased stream a connection runs on.
pub type BoxedIo = Pin<Box<dyn Io>>;

/// The future an [`Authenticator`] returns.
#[cfg(not(target_arch = "wasm32"))]
pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = Option<Session>> + Send + 'a>>;
/// The future an [`Authenticator`] returns (single-threaded targets).
#[cfg(target_arch = "wasm32")]
pub type AuthFuture<'a> = Pin<Box<dyn Future<Output = Option<Session>> + 'a>>;

/// What a client presented in the startup exchange.
pub struct AuthRequest<'a> {
	/// The user name from the startup packet.
	pub user: &'a str,
	/// The password the client sent in clear, which is only asked for over TLS.
	pub password: &'a str,
	/// The namespace the startup selected, if any.
	pub namespace: Option<&'a str>,
	/// The database the startup selected, if any.
	pub database: Option<&'a str>,
}

/// Judges the credentials of a connection in place of the datastore's own users.
///
/// `authenticate` receives the session the connection built so far (address and id set, no
/// authentication) and returns it with the authenticated subject, or `None` to refuse. A connection
/// with an authenticator asks for a cleartext password and only over TLS, with no loopback
/// exception, because the authenticator holds no SCRAM verifier.
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
	/// A fresh connection: the client's first packet may be an `SSLRequest`, a `GSSENCRequest`, a
	/// `CancelRequest` or the startup message.
	Negotiate,
	/// The client's `SSLRequest` was already answered `S` before the stream was handed over: the
	/// next bytes are the TLS `ClientHello`. Requires a TLS acceptor.
	Tls,
	/// The client's TLS was ended before the stream was handed over, so the next bytes are the
	/// `StartupMessage` in clear, with no `SSLRequest`; the connection counts as encrypted, which
	/// lets it ask for a cleartext password. For a host that terminates TLS itself.
	Secured,
}

/// State shared by every connection served in one process: the cancellation registry and the
/// advisory locks.
#[derive(Default)]
pub struct Shared {
	registry: Arc<CancelRegistry>,
	advisory_locks: Arc<advisory::AdvisoryLocks>,
}

/// What `serve` needs beyond the stream and the datastore.
pub struct ServeOptions {
	/// The client's address, used for the session and for logging.
	pub peer: SocketAddr,
	/// The TLS acceptor, or `None` to decline `SSLRequest`.
	pub tls: Option<Arc<TlsAcceptor>>,
	/// How the stream begins.
	pub start: Start,
	/// Whether the datastore accepts connections.
	pub ready: Arc<AtomicBool>,
	/// Closes the connection when cancelled.
	pub shutdown: CancellationToken,
	/// Judges credentials; `None` uses the datastore's users.
	pub authenticator: Option<Arc<dyn Authenticator>>,
	/// State shared with the other connections of this process.
	pub shared: Arc<Shared>,
	/// The dialect a connection starts in, until its startup options or a `SET` change it. This
	/// protocol speaks `PgSql` and `Gql`; any other value starts the connection in `PgSql`.
	pub dialect: Dialect,
}

/// Serve one PostgreSQL connection over `stream` until it ends.
///
/// The caller owns admission (connection limits) and accepting; this runs the protocol: TLS
/// negotiation, startup, authentication and the command loop, against `ds`.
#[tracing::instrument(level = "debug", skip_all, fields(peer = %options.peer))]
pub async fn serve<S: Io>(stream: S, ds: Arc<Datastore>, options: ServeOptions) {
	let (pid, secret) = next_backend_key();
	conn::handle(
		Box::pin(stream),
		options.peer,
		ds,
		options.ready,
		options.shutdown,
		None,
		Arc::clone(&options.shared.registry),
		Arc::clone(&options.shared.advisory_locks),
		pid,
		secret,
		options.tls,
		options.start,
		options.authenticator,
		match options.dialect {
			Dialect::Gql => Dialect::Gql,
			Dialect::SurrealQl | Dialect::PgSql | Dialect::MySql => Dialect::PgSql,
		},
	)
	.await;
}

/// Upper bound on concurrent Postgres connections, so a flood of clients
/// cannot spawn unbounded connection tasks. Connections beyond this are
/// accepted, told the server is full, and closed.
#[cfg(feature = "listener")]
const MAX_CONNECTIONS: usize = 1024;

/// The ALPN protocol name PostgreSQL clients offer (libpq 17 and later offer it
/// on every TLS connection, including one negotiated by `SSLRequest`).
#[cfg(feature = "listener")]
const ALPN_POSTGRESQL: &[u8] = b"postgresql";

/// Build the TLS acceptor for the Postgres listener from the server
/// certificate configuration.
///
/// The ALPN list is set explicitly to `postgresql` rather than inherited from
/// the HTTP server's defaults. A TLS server that advertises ALPN protocols
/// aborts the handshake with `no_application_protocol` when the client offers
/// a list sharing none of them, so a client that offers `postgresql` (libpq 17
/// and later) completes, while a client that offers only other protocols (for
/// example `h2` or `http/1.1`) is refused. A client that offers no ALPN list
/// is unaffected.
#[cfg(feature = "listener")]
fn postgres_tls_acceptor(config: &RustlsConfig) -> TlsAcceptor {
	let mut server = ServerConfig::clone(&config.get_inner());
	server.alpn_protocols = vec![ALPN_POSTGRESQL.to_vec()];
	TlsAcceptor::from(Arc::new(server))
}

/// Bind the Postgres wire protocol listener and spawn its accept loop.
///
/// Binding happens before returning so startup fails loudly when the address
/// is unavailable; the accept loop then runs until `shutdown` fires. When a
/// TLS certificate and key are configured, SSLRequests are accepted and
/// upgraded; otherwise they are declined and the connection stays plaintext.
#[cfg(feature = "listener")]
pub async fn start(
	addr: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	crt: Option<PathBuf>,
	key: Option<PathBuf>,
) -> Result<SocketAddr> {
	let acceptor = match (crt, key) {
		(Some(crt), Some(key)) => {
			let config = RustlsConfig::from_pem_file(&crt, &key)
				.await
				.with_context(|| "Failed to load the postgres TLS certificate and key")?;
			Some(Arc::new(postgres_tls_acceptor(&config)))
		}
		_ => {
			warn!(
				target: LOG,
				"The postgres wire protocol is serving plaintext connections; \
				 configure --web-crt/--web-key to enable TLS"
			);
			None
		}
	};
	let listener = TcpListener::bind(addr)
		.await
		.with_context(|| format!("Failed to bind the postgres listener on {addr}"))?;
	let bound_addr = listener.local_addr()?;
	info!(target: LOG, "Started postgres wire protocol server on {bound_addr}");
	tokio::spawn(accept_loop(listener, ds, ready, shutdown, acceptor));
	Ok(bound_addr)
}

#[cfg(feature = "listener")]
async fn accept_loop(
	listener: TcpListener,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	acceptor: Option<Arc<TlsAcceptor>>,
) {
	let limiter = Arc::new(Semaphore::new(MAX_CONNECTIONS));
	let registry: Arc<CancelRegistry> = Arc::new(DashMap::new());
	let advisory_locks = Arc::new(advisory::AdvisoryLocks::default());
	loop {
		tokio::select! {
			biased;
			_ = shutdown.cancelled() => break,
			accepted = listener.accept() => match accepted {
				Ok((stream, peer)) => {
					if let Err(err) = stream.set_nodelay(true) {
						debug!(target: LOG, "failed to set TCP_NODELAY for {peer}: {err}");
					}
					// Acquire a slot for the lifetime of the connection task.
					// When the pool is exhausted, reject rather than queue so a
					// flood cannot pile up unbounded tasks.
					let Ok(permit) = Arc::clone(&limiter).try_acquire_owned() else {
						warn!(target: LOG, "rejecting postgres connection from {peer}: too many connections");
						tokio::spawn(conn::reject_overloaded(stream));
						continue;
					};
					let (pid, secret) = next_backend_key();
					tokio::spawn(conn::handle(
						Box::pin(stream),
						peer,
						Arc::clone(&ds),
						Arc::clone(&ready),
						shutdown.clone(),
						Some(permit),
						Arc::clone(&registry),
						Arc::clone(&advisory_locks),
						pid,
						secret,
						acceptor.clone(),
						Start::Negotiate,
						None,
						Dialect::PgSql,
					));
				}
				Err(err) => {
					warn!(target: LOG, "failed to accept postgres connection: {err}");
					// Back off briefly so a persistent accept error (e.g. file
					// descriptor exhaustion) does not spin the loop hot.
					sleep(Duration::from_millis(20)).await;
				}
			}
		}
	}
	info!(target: LOG, "Stopped postgres wire protocol server");
}
