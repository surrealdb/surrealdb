//! This binary is the web-platform server for [SurrealDB](https://surrealdb.com) the
//! ultimate cloud database for tomorrow's applications. SurrealDB is a
//! scalable, distributed, collaborative, document-graph database for the
//! realtime web.
//!
//! This binary can be used to start a database server instance using an
//! embedded in-memory datastore, or an embedded datastore persisted to disk. In
//! addition, it can be used in distributed mode by connecting to a distributed [TiKV](https://tikv.org)
//! key-value store.

// Temporary allow deprecated until the 3.0
#![allow(deprecated)]
#![deny(clippy::mem_forget)]

#[macro_use]
extern crate surrealdb_core;

#[macro_use]
extern crate tracing;

mod cli;
mod cnf;
mod dbs;
mod env;
//mod gql;
mod net;
mod rpc;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

use surrealdb_core as core;

fn main() -> ExitCode {
	// Initiate the command line
	with_enough_stack(cli::init())
}

/// Rust's default thread stack size of 2MiB doesn't allow sufficient recursion
/// depth.
fn with_enough_stack<T>(fut: impl Future<Output = T> + Send) -> T {
	// Start a Tokio runtime with custom configuration
	let mut b = tokio::runtime::Builder::new_multi_thread();
	b.enable_all()
		.max_blocking_threads(*cnf::RUNTIME_MAX_BLOCKING_THREADS)
		.worker_threads(*cnf::RUNTIME_WORKER_THREADS)
		.thread_stack_size(*cnf::RUNTIME_STACK_SIZE)
		.thread_name("surrealdb-worker");
	#[cfg(feature = "allocation-tracking")]
	b.on_thread_stop(|| crate::core::mem::ALLOC.stop_tracking());
	// Build the runtime
	b.build().unwrap().block_on(fut)
}
