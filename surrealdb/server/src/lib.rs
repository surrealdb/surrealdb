//! Library entrypoints for embedding SurrealDB server inside another Rust application.
//! Exposes the same init() used by the `surreal` binary so external apps can
//! start SurrealDB within their own `main()`.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

// Temporarily allow deprecated items until version 3.0 for backward compatibility
#![allow(deprecated)]
#![deny(clippy::mem_forget)]

#[macro_use]
extern crate tracing;

mod cli;
mod cnf;
mod dbs;
mod env;
#[cfg(feature = "graphql")]
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
pub use cnf::ServerConfig;
/// Re-export `RouterFactory` for convenience so embedders can `use surreal::RouterFactory`.
#[doc(inline)]
pub use ntw::RouterFactory;
/// Re-export `RpcState` for convenience so embedders can `use surreal::RpcState`.
#[doc(inline)]
pub use rpc::RpcState;
#[doc(inline)]
pub use surrealdb as sdk;
/// Re-export `core` for convenience so embedders can `use surreal::core::...`.
#[doc(inline)]
pub use surrealdb_core as core;
use surrealdb_core::buc::BucketStoreProvider;
use surrealdb_core::kvs::TransactionBuilderFactory;

// Re-export the core crate in the same path used across internal modules
// so that `crate::core::...` keeps working when used as a library target.

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
pub fn init<C: TransactionBuilderFactory + RouterFactory + ConfigCheck + BucketStoreProvider>(
	composer: C,
) -> ExitCode {
	let config = cnf::ServerConfig::from_env();
	let rt = config.runtime.clone();
	with_enough_stack(&rt, cli::init::<C>(composer, config))
}

/// Rust's default thread stack size of 2MiB doesn't allow sufficient recursion depth
/// for SurrealDB's query parser and execution engine. This function creates a Tokio
/// runtime with a larger stack size configured via the [`cnf::RuntimeConfig`].
fn with_enough_stack(
	rt: &cnf::RuntimeConfig,
	fut: impl Future<Output = ExitCode> + Send,
) -> ExitCode {
	// Start a Tokio runtime with custom configuration
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.max_blocking_threads(rt.max_blocking_threads)
		.worker_threads(rt.worker_threads)
		.thread_stack_size(rt.stack_size)
		.thread_name("surrealdb-worker")
		// When a thread is parked, ensure that local memory
		// tracking is flushed to the global tracking counter.
		.on_thread_park(|| core::mem::ALLOC.flush_local_allocations())
		// When a thread is shutdown, ensure that local memory
		// tracking is flushed to the global tracking counter.
		.on_thread_stop(|| core::mem::ALLOC.flush_local_allocations())
		// Build the runtime
		.build();
	// Check the success of the runtime creation
	match runtime {
		Ok(r) => r.block_on(fut),
		Err(e) => {
			// The runtime creation failed (e.g. insufficient system resources)
			error!("Failed to build runtime: {e}");
			ExitCode::FAILURE
		}
	}
}
