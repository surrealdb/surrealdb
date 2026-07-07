//! Postgres wire protocol (v3.0) listener.
//!
//! Lets any Postgres client (psql, drivers) connect to SurrealDB and run
//! SurrealQL over the simple query protocol, with result columns typed from
//! the values they carry. See the `postgres` feature and the
//! `--postgres-bind` server option.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Duration;

use anyhow::{Context as _, Result};
use dashmap::DashMap;
use rand::TryRngCore;
use surrealdb_core::ctx::CancelHandle;
use surrealdb_core::kvs::Datastore;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

mod conn;
mod encode;
mod error;
mod msg;
mod sasl;
mod typing;

const LOG: &str = "surrealdb::pg";

/// Maps a connection's (backend pid, secret key) to its cancellation handle so
/// a Postgres CancelRequest on a side connection can interrupt the in-flight
/// query. Shared across all connections of one listener.
pub(super) type CancelRegistry = DashMap<(i32, i32), CancelHandle>;

/// Monotonic per-process source of backend "process ids". The secret key is
/// drawn from OS entropy (not the seedable `rnd` module) so CancelRequests
/// cannot be forged.
static NEXT_PID: AtomicI32 = AtomicI32::new(1);

fn next_backend_key() -> (i32, i32) {
	let pid = NEXT_PID.fetch_add(1, Ordering::Relaxed);
	let mut bytes = [0u8; 4];
	let secret = if rand::rngs::OsRng.try_fill_bytes(&mut bytes).is_ok() {
		i32::from_ne_bytes(bytes)
	} else {
		rand::random()
	};
	(pid, secret)
}

/// Upper bound on concurrent Postgres connections, so a flood of clients
/// cannot spawn unbounded connection tasks. Connections beyond this are
/// accepted, told the server is full, and closed.
const MAX_CONNECTIONS: usize = 1024;

/// Bind the Postgres wire protocol listener and spawn its accept loop.
///
/// Binding happens before returning so startup fails loudly when the address
/// is unavailable; the accept loop then runs until `shutdown` fires. When a
/// TLS certificate and key are configured, SSLRequests are accepted and
/// upgraded; otherwise they are declined and the connection stays plaintext.
pub(crate) async fn start(
	addr: SocketAddr,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	crt: Option<PathBuf>,
	key: Option<PathBuf>,
) -> Result<()> {
	let acceptor = match (crt, key) {
		(Some(crt), Some(key)) => {
			let config = axum_server::tls_rustls::RustlsConfig::from_pem_file(&crt, &key)
				.await
				.with_context(|| "Failed to load the postgres TLS certificate and key")?;
			Some(Arc::new(TlsAcceptor::from(config.get_inner())))
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
	info!(target: LOG, "Started postgres wire protocol server on {}", addr);
	tokio::spawn(accept_loop(listener, ds, ready, shutdown, acceptor));
	Ok(())
}

async fn accept_loop(
	listener: TcpListener,
	ds: Arc<Datastore>,
	ready: Arc<AtomicBool>,
	shutdown: CancellationToken,
	acceptor: Option<Arc<TlsAcceptor>>,
) {
	let limiter = Arc::new(Semaphore::new(MAX_CONNECTIONS));
	let registry: Arc<CancelRegistry> = Arc::new(DashMap::new());
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
						stream,
						peer,
						Arc::clone(&ds),
						Arc::clone(&ready),
						shutdown.clone(),
						permit,
						Arc::clone(&registry),
						pid,
						secret,
						acceptor.clone(),
					));
				}
				Err(err) => {
					warn!(target: LOG, "failed to accept postgres connection: {err}");
					// Back off briefly so a persistent accept error (e.g. file
					// descriptor exhaustion) does not spin the loop hot.
					tokio::time::sleep(Duration::from_millis(20)).await;
				}
			}
		}
	}
	info!(target: LOG, "Stopped postgres wire protocol server");
}
