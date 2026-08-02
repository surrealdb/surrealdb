//! The embedded SurrealDB engine driven by the Rust SDK.
//!
//! This crate owns everything that needs the database core: it opens a
//! [`Datastore`], runs the background maintenance [`tasks`], and answers the
//! [`Route`](surrealdb_engine_api::Route)s the SDK sends it. The SDK reaches it
//! only through [`surrealdb_engine_api`], which is why enabling one of this
//! crate's storage backends is the only thing that puts the core in a Rust SDK
//! build.
//!
//! # Stability
//!
//! This is an internal interface between crates released together. It carries
//! no stability guarantee and may change in any release, including a patch
//! release. Application code should use the
//! [`surrealdb`](https://docs.rs/surrealdb) crate and select a storage backend
//! with its `kv-*` features.

#![recursion_limit = "256"]

#[macro_use]
extern crate tracing;

use std::path::PathBuf;
use std::time::Duration;

#[doc(inline)]
pub use surrealdb_core::kvs::Datastore;
#[doc(inline)]
pub use surrealdb_core::options::EngineOptions;
use surrealdb_rpc::capabilities::Capabilities;

mod interval;
#[cfg(not(target_family = "wasm"))]
pub mod native;
mod router;
pub mod tasks;
#[cfg(target_family = "wasm")]
pub mod wasm;

/// The root credentials to initialise the datastore with on first start.
#[derive(Debug, Clone)]
pub struct Root {
	/// The username of the root user.
	pub username: String,
	/// The password of the root user.
	pub password: String,
}

/// Everything the engine needs to open a datastore and start serving routes.
///
/// The SDK derives this from the endpoint the caller connected to; the engine
/// never sees the SDK's own connection configuration.
#[derive(Debug, Clone, Default)]
pub struct LocalConfig {
	/// The datastore to open, as a scheme-prefixed path (`mem://`,
	/// `rocksdb://path`, `surrealkv://path`) or, for TiKV, the cluster URL.
	pub path: String,
	/// The root user to create when the datastore has no credentials yet.
	/// `None` starts the datastore with authentication disabled.
	pub root: Option<Root>,
	/// The maximum time a single query may run for.
	pub query_timeout: Option<Duration>,
	/// The maximum time a single transaction may run for.
	pub transaction_timeout: Option<Duration>,
	/// What the datastore is allowed to do.
	pub capabilities: Capabilities,
	/// Where the datastore spills large intermediate results, when the storage
	/// backends that need one are compiled in.
	pub temporary_directory: Option<PathBuf>,
	/// Overrides for the corresponding [`EngineOptions`] intervals. `None`
	/// keeps the default.
	pub node_membership_refresh_interval: Option<Duration>,
	/// Overrides the node membership check interval.
	pub node_membership_check_interval: Option<Duration>,
	/// Overrides the node membership cleanup interval.
	pub node_membership_cleanup_interval: Option<Duration>,
	/// Overrides the changefeed garbage collection interval.
	pub changefeed_gc_interval: Option<Duration>,
}

impl LocalConfig {
	/// The engine options this configuration implies, leaving every interval it
	/// does not override at its default.
	fn engine_options(&self) -> EngineOptions {
		let mut opt = EngineOptions::default();
		if let Some(interval) = self.node_membership_refresh_interval {
			opt.node_membership_refresh_interval = interval;
		}
		if let Some(interval) = self.node_membership_check_interval {
			opt.node_membership_check_interval = interval;
		}
		if let Some(interval) = self.node_membership_cleanup_interval {
			opt.node_membership_cleanup_interval = interval;
		}
		if let Some(interval) = self.changefeed_gc_interval {
			opt.changefeed_gc_interval = interval;
		}
		opt
	}
}

/// Convert an error that is only reportable as text into the SDK's error type.
fn std_error_to_types_error(error: impl std::fmt::Display) -> surrealdb_types::Error {
	surrealdb_types::Error::internal(error.to_string())
}
