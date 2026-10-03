//! MySQL wire protocol server implementation for SurrealDB.
//!
//! Lets any standard MySQL client (mysql CLI, JDBC, Python, Go, Node.js, PHP drivers)
//! connect to SurrealDB and execute queries over the MySQL client/server protocol.

#![recursion_limit = "512"]

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Context as _, Result};
use surrealdb_core::kvs::Datastore;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
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
) -> Result<()> {
	let listener = TcpListener::bind(addr)
		.await
		.with_context(|| format!("Failed to bind the MySQL listener on {addr}"))?;

	let bound_addr = listener.local_addr()?;
	info!(target: LOG, "Started MySQL wire protocol server on {bound_addr}");

	let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));

	tokio::spawn(async move {
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
							let permit = match Arc::clone(&semaphore).try_acquire_owned() {
								Ok(p) => p,
								Err(_) => {
									warn!(target: LOG, "Max MySQL connections reached ({MAX_CONNECTIONS}); rejecting {peer}");
									continue;
								}
							};

							let conn_ds = Arc::clone(&ds);
							let conn_ready = Arc::clone(&ready);
							let conn_shutdown = shutdown.clone();

							tokio::spawn(async move {
								debug!(target: LOG, "Accepted MySQL connection from {peer}");
								let mut connection = Connection::new(
									stream,
									peer,
									conn_ds,
									conn_ready,
									conn_shutdown,
									permit,
								);

								if let Err(err) = connection.run().await {
									debug!(target: LOG, "MySQL connection from {peer} ended with error: {err}");
								} else {
									debug!(target: LOG, "MySQL connection from {peer} closed cleanly");
								}
							});
						}
						Err(err) => {
							warn!(target: LOG, "Failed to accept MySQL connection on {bound_addr}: {err}");
						}
					}
				}
			}
		}
	});

	Ok(())
}
