//! Library entrypoints for embedding SurrealDB server inside another Rust application.
//! Exposes the same init() used by the `surreal` binary so external apps can
//! start SurrealDB within their own `main()`.

// Temporary allow deprecated until the 3.0
#![allow(deprecated)]
#![deny(clippy::mem_forget)]

#[macro_use]
pub extern crate surrealdb_core;

#[macro_use]
extern crate tracing;

mod cli;
mod cnf;
mod dbs;
mod env;
// mod gql; // currently disabled in binary too
pub mod net;
/// Make `rpc` public so embedders can access RpcState and related router definitions
/// when running SurrealDB as a library.
pub mod rpc;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

/// Re-export `RpcState` for convenience so embedders can `use surreal::RpcState`.
pub use rpc::RpcState;
pub use surrealdb_core as core;
use surrealdb_core::kvs::TransactionBuilderFactory;

// Re-export the core crate in the same path used across internal modules
// so that `crate::core::...` keeps working when used as a library target.
use crate::net::RouterFactory;

/// Initialize SurrealDB CLI/server with the same behavior as the `surreal` binary.
/// This spins up a Tokio runtime with a larger stack size and then runs the CLI
/// entrypoint (which starts the server when the `start` subcommand is used).
///
/// Generic parameters:
/// - T: `TransactionBuilderFactory` (selects/validates the datastore backend).
/// - R: `RouterFactory` (constructs the HTTP router).
pub fn init<T: TransactionBuilderFactory, R: RouterFactory>() -> ExitCode {
	with_enough_stack(cli::init::<T, R>())
}

/// Rust's default thread stack size of 2MiB doesn't allow sufficient recursion depth.
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
