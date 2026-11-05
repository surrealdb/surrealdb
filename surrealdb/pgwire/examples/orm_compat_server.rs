//! In-memory listener for the ORM compatibility harness. It accepts local test connections.
#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use surrealdb_core::kvs::Datastore;
use tokio::runtime::Builder;
use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
	// Match the server runtime's worker stacks for parser and execution frames.
	let stack = if cfg!(debug_assertions) {
		20 * 1024 * 1024
	} else {
		10 * 1024 * 1024
	};
	Builder::new_multi_thread().enable_all().thread_stack_size(stack).build()?.block_on(run())
}

async fn run() -> anyhow::Result<()> {
	let ds = Datastore::new("memory").await?;
	let address = "127.0.0.1:15432".parse()?;
	let bound = surrealdb_pgwire::start(
		address,
		ds,
		Arc::new(AtomicBool::new(true)),
		CancellationToken::new(),
		None,
		None,
	)
	.await?;
	println!("ORM test listener: {bound}");
	std::future::pending::<()>().await;
	Ok(())
}
