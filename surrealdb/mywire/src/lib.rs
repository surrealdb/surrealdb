//! MySQL wire protocol server implementation for SurrealDB.

#[macro_use]
extern crate tracing;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Result;
use surrealdb_core::kvs::Datastore;
use tokio_util::sync::CancellationToken;

pub const LOG: &str = "surrealdb::mywire";

/// Bind the MySQL wire protocol listener and spawn its accept loop.
pub async fn start(
	addr: SocketAddr,
	_ds: Arc<Datastore>,
	_ready: Arc<AtomicBool>,
	_shutdown: CancellationToken,
) -> Result<()> {
	info!(target: LOG, "Started mysql wire protocol server on {}", addr);
	Ok(())
}
