//! MySQL wire protocol server implementation for SurrealDB.
//!
//! Lets any standard MySQL client (mysql CLI, JDBC, Python, Go, Node.js, PHP drivers)
//! connect to SurrealDB and execute queries over the MySQL client/server protocol.

#![recursion_limit = "512"]

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use anyhow::{Context as _, Result};
use axum_server::tls_rustls::RustlsConfig;
use surrealdb_core::kvs::Datastore;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

pub mod auth;
pub mod codec;
pub mod conn;
pub mod constants;
pub mod msg;
pub mod query;
pub mod typing;

pub use conn::Connection;
pub use query::translate_query;

pub const LOG: &str = "surrealdb::mywire";
const MAX_CONNECTIONS: usize = 1024;

/// Bind the MySQL wire protocol listener and spawn its accept loop.
///
/// Binding happens before returning so startup fails loudly if the address
/// is unavailable; the accept loop then runs until `shutdown` fires.
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
			Some(Arc::new(TlsAcceptor::from(config.get_inner())))
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
									stream,
									peer,
									conn_ds,
									conn_ready,
									conn_shutdown,
									permit,
									conn_acceptor,
									conn_auth_cache,
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
