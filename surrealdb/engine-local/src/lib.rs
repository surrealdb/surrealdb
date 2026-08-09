//! The embedded SurrealDB engine driven by the Rust SDK.
//!
//! This crate owns everything that needs the database core: it opens a
//! [`Datastore`], which runs its own background maintenance [`tasks`], and
//! serves it to the SDK as a [`SurrealEngine`](surrealdb_engine_api::SurrealEngine).
//! The SDK reaches it only through [`surrealdb_engine_api`], which is why
//! enabling one of this crate's storage backends is the only thing that puts
//! the core in a Rust SDK build.
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

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_channel::Receiver;
#[doc(inline)]
pub use surrealdb_core::kvs::Datastore;
#[doc(inline)]
pub use surrealdb_core::options::EngineOptions;
use surrealdb_engine_api::SessionId;
use surrealdb_rpc::capabilities::Capabilities;
use surrealdb_types::{Error, Notification};

mod engine;
mod session;

/// The datastore's background maintenance tasks.
///
/// Re-exported because the datastore starts them itself; this path is what
/// `surrealdb::engine::tasks` resolves to.
#[doc(inline)]
pub use surrealdb_core::kvs::tasks;

pub use crate::engine::LocalEngine;
use crate::session::SessionRegistry;

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

/// Spawns a detached task on whichever executor this target has.
///
/// Nothing joins one of these, which is what lets Wasm's `spawn_local` --
/// which returns no handle at all -- stand in for `tokio::spawn`.
#[cfg(not(target_family = "wasm"))]
fn spawn<F>(future: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	drop(tokio::spawn(future));
}

#[cfg(target_family = "wasm")]
fn spawn<F>(future: F)
where
	F: Future<Output = ()> + 'static,
{
	wasm_bindgen_futures::spawn_local(future);
}

/// Opens the datastore `config` describes and serves it to the SDK.
///
/// The returned engine owns the datastore. `session_rx` is the SDK's session
/// lifecycle channel: it registers the sessions requests run under, and its
/// closing -- which happens when the last connection handle is dropped -- is
/// what shuts the datastore down.
pub async fn connect(
	config: LocalConfig,
	session_rx: Receiver<SessionId>,
) -> Result<Arc<LocalEngine>, Error> {
	let builder = Datastore::builder()
		// The datastore starts its own maintenance tasks, so the cadences go to
		// the builder rather than to a `tasks::init` call here.
		.with_engine_options(config.engine_options())
		.with_query_timeout(config.query_timeout)
		.with_transaction_timeout(config.transaction_timeout)
		.with_auth(config.root.is_some());

	#[cfg(storage)]
	let builder = builder.with_temporary_directory(config.temporary_directory);

	let (notifications, builder) = if config.capabilities.allows_live_query_notifications() {
		let (send, recv) = async_channel::bounded(surrealdb_cnf::NOTIFICATIONS_CHANNEL_SIZE);
		(Some(recv), builder.with_notify(send))
	} else {
		(None, builder)
	};

	let builder = builder.with_capabilities(config.capabilities);

	let kvs = builder.build_with_path(&config.path).await.map_err(std_error_to_types_error)?;
	kvs.check_version().await.map_err(std_error_to_types_error)?;
	kvs.bootstrap().await.map_err(std_error_to_types_error)?;
	// If a root user is specified, setup the initial datastore credentials
	if let Some(root) = &config.root {
		kvs.initialise_credentials(&root.username, &root.password)
			.await
			.map_err(std_error_to_types_error)?;
	}

	Ok(serve(kvs, notifications, session_rx))
}

/// Serves an already-open datastore to the SDK.
///
/// The datastore arrives with its own maintenance tasks and its own
/// cancellation, both established when it was built, so neither is passed in
/// here. A caller that wants to control either sets them on the
/// [`Builder`](surrealdb_core::kvs::ds::builder::Builder) it constructs the
/// datastore with.
pub fn from_datastore(
	datastore: Arc<Datastore>,
	notifications: Option<Receiver<Notification>>,
	session_rx: Receiver<SessionId>,
) -> Arc<LocalEngine> {
	serve(datastore, notifications, session_rx)
}

/// Builds the engine and starts the two tasks that outlive an individual
/// request: the one tracking the SDK's sessions, and the one delivering
/// live-query notifications to them.
fn serve(
	kvs: Arc<Datastore>,
	notifications: Option<Receiver<Notification>>,
	session_rx: Receiver<SessionId>,
) -> Arc<LocalEngine> {
	let sessions = Arc::new(SessionRegistry::default());

	if let Some(notifications) = notifications.clone() {
		spawn(session::pump(Arc::clone(&kvs), Arc::clone(&sessions), notifications));
	}
	// Both tasks end without reference to the engine: `run` on the session
	// channel closing, and `pump` on the notification channel `run` then closes.
	// Holding the engine instead would make the engine's own liveness the
	// condition for its tasks ending, which nothing would ever satisfy.
	spawn(session::run(Arc::clone(&kvs), Arc::clone(&sessions), session_rx, notifications));

	Arc::new(LocalEngine {
		kvs,
		sessions,
	})
}
