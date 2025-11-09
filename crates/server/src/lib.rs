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
mod gql;
/// Make `ntw` public so embedders can access RouterFactory and related networking definitions
/// when running SurrealDB as a library.
pub mod ntw;
/// Make `rpc` public so embedders can access RpcState and related router definitions
/// when running SurrealDB as a library.
pub mod rpc;
mod telemetry;

use std::future::Future;
use std::process::ExitCode;

pub use cli::{Config, ConfigCheck, ConfigCheckRequirements};
/// Re-export `RpcState` for convenience so embedders can `use surreal::RpcState`.
pub use rpc::RpcState;
pub use surrealdb_core as core;
use surrealdb_core::kvs::TransactionBuilderFactory;

// Re-export the core crate in the same path used across internal modules
// so that `crate::core::...` keeps working when used as a library target.
use crate::ntw::RouterFactory;

/// Initialize SurrealDB CLI/server with the same behavior as the `surreal` binary.
/// This spins up a Tokio runtime with a larger stack size and then runs the CLI
/// entrypoint (which starts the server when the `start` subcommand is used).
///
/// # Parameters
/// - `composer`: A composer implementing the required traits for dependency injection.
///
/// # Generic parameters
/// - `C`: A composer type that implements:
///   - `TransactionBuilderFactory` (selects/validates the datastore backend)
///   - `RouterFactory` (constructs the HTTP router)
///   - `ConfigCheck` (validates configuration before initialization)
pub fn init<C: TransactionBuilderFactory + RouterFactory + ConfigCheck>(composer: C) -> ExitCode {
	with_enough_stack(cli::init::<C>(composer))
}

/// Rust's default thread stack size of 2MiB doesn't allow sufficient recursion depth.
fn with_enough_stack(fut: impl Future<Output = ExitCode> + Send) -> ExitCode {
	// Start a Tokio runtime with custom configuration
	let mut b = tokio::runtime::Builder::new_multi_thread();
	b.enable_all()
		.max_blocking_threads(*cnf::RUNTIME_MAX_BLOCKING_THREADS)
		.worker_threads(*cnf::RUNTIME_WORKER_THREADS)
		.thread_stack_size(*cnf::RUNTIME_STACK_SIZE)
		.thread_name("surrealdb-worker");
	#[cfg(feature = "allocation-tracking")]
	b.on_thread_stop(|| core::mem::ALLOC.stop_tracking());
	// Build the runtime and execute the future.
	// If runtime creation fails (e.g., insufficient system resources), log the error
	// and return FAILURE to indicate the application cannot start.
	match b.build() {
		Ok(b) => b.block_on(fut),
		Err(e) => {
			error!("Failed to build runtime: {}", e);
			ExitCode::FAILURE
		}
	}
}
