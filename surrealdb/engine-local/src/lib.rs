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
// TODO: Remove once this crate no longer needs it; see `absolute_paths` in the workspace lints.
#![allow(clippy::absolute_paths)]

#[macro_use]
extern crate tracing;

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_channel::Receiver;
use surrealdb_cnf::ConfigMap;
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
	/// `surrealkv://path`).
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
	/// The directories a local `file:` bucket backend may be defined in. Empty
	/// refuses every such bucket.
	pub bucket_folder_allowlist: Vec<PathBuf>,
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
	/// Datastore configuration keys and values, as the server reads them
	/// from `SURREAL_<KEY>`: storage backend settings such as
	/// `surrealkv_block_cache_capacity` and runtime settings such as
	/// `memory_limit`. A parameter on `path` wins over a backend setting here.
	/// `bucket_folder_allowlist` is refused: it is set by its own field.
	pub datastore_options: Vec<(String, String)>,
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

	/// The configuration map the datastore is built with.
	///
	/// It holds only the keys this configuration sets. Nothing is read from the
	/// process environment, so an embedded datastore can do only what its
	/// caller configured.
	fn datastore_config(&self) -> Result<ConfigMap, Error> {
		let mut map = ConfigMap::empty();
		for (key, value) in &self.datastore_options {
			// Keys are matched as the environment produces them.
			let key = key.to_lowercase();
			if key == "bucket_folder_allowlist" {
				return Err(std_error_to_types_error(
					"the bucket folder allowlist is set by its own option, not as a datastore option",
				));
			}
			map = map.with_key_value(key, value.clone());
		}
		if !self.bucket_folder_allowlist.is_empty() {
			// The datastore reads the allowlist as a path list, the form the
			// server's environment variable takes.
			let joined = std::env::join_paths(&self.bucket_folder_allowlist)
				.map_err(|e| {
					std_error_to_types_error(format!("invalid bucket folder allowlist: {e}"))
				})?
				.into_string()
				.map_err(|_| {
					std_error_to_types_error("the bucket folder allowlist is not valid UTF-8")
				})?;
			map = map.with_key_value("bucket_folder_allowlist", joined);
		}
		Ok(map)
	}
}

/// Convert an error that is only reportable as text into the SDK's error type.
fn std_error_to_types_error(error: impl std::fmt::Display) -> surrealdb_types::Error {
	surrealdb_types::Error::internal(error.to_string())
}

/// Spawns a detached task on whichever executor this target has.
///
/// Nothing joins one of these, which is what lets the browser's `spawn_local`
/// -- which returns no handle at all -- stand in for `tokio::spawn`. Every
/// other target spawns on the ambient tokio runtime.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn spawn<F>(future: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	drop(tokio::spawn(future));
}

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
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
		.with_config(config.datastore_config()?)
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

	// The maintenance tasks are started by hand below rather than at
	// construction. Every one of them writes, and a write that lands before the
	// version key does makes a datastore this process just created look like one
	// written before that key existed — which `check_version` is obliged to
	// refuse, permanently, on this and every later open.
	let kvs = builder
		.without_maintenance_tasks()
		.build_with_path(&config.path)
		.await
		.map_err(std_error_to_types_error)?;
	// A failure past this point must close the datastore: the caller never
	// receives a handle to close, and an engine that holds an exclusive lock on
	// its storage would otherwise keep it until the process ends.
	let started = async {
		kvs.check_version().await.map_err(std_error_to_types_error)?;
		kvs.bootstrap().await.map_err(std_error_to_types_error)?;
		kvs.start_maintenance_tasks_checked().await.map_err(std_error_to_types_error)?;
		// If a root user is specified, setup the initial datastore credentials. A datastore
		// that is open read-only refuses the write, so the step is skipped and the store
		// serves what it already holds.
		if let Some(root) = &config.root {
			if kvs.is_read_only() {
				tracing::warn!(
					"The datastore is open read-only, so the root credentials were not initialised"
				);
			} else {
				kvs.initialise_credentials(&root.username, &root.password)
					.await
					.map_err(std_error_to_types_error)?;
			}
		}
		Ok(())
	}
	.await;
	if let Err(e) = started {
		kvs.shutdown().await.ok();
		return Err(e);
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

	// Both tasks end without reference to the engine: `run` on the session
	// channel closing, and `pump` on the notification channel `run` then closes.
	// Holding the engine instead would make the engine's own liveness the
	// condition for its tasks ending, which nothing would ever satisfy.
	let pump = notifications
		.clone()
		.map(|notifications| session::pump(Arc::clone(&kvs), Arc::clone(&sessions), notifications));
	let run = session::run(Arc::clone(&kvs), Arc::clone(&sessions), session_rx, notifications);
	// Answers once, when both tasks have finished and dropped their handles on
	// the datastore: what `run` reported of shutting it down.
	let (stopped_tx, stopped) = async_channel::bounded::<Result<(), Error>>(1);
	spawn(async move {
		let outcome = match pump {
			Some(pump) => futures::future::join(run, pump).await.0,
			None => run.await,
		};
		stopped_tx.send(outcome).await.ok();
	});

	Arc::new(LocalEngine {
		kvs,
		sessions,
		stopped,
	})
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
	use surrealdb_kvs_surrealkv::SurrealKvConfig;

	use super::LocalConfig;

	fn options(pairs: &[(&str, &str)]) -> LocalConfig {
		LocalConfig {
			datastore_options: pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
			..LocalConfig::default()
		}
	}

	/// The storage settings an embedded caller sets reach the backend
	/// configuration the datastore is opened with, whatever their case.
	#[test]
	fn datastore_options_reach_the_backend_config() {
		let config = options(&[
			("surrealkv_block_cache_capacity", "268435456"),
			("SURREALKV_MAX_MEMTABLE_SIZE", "67108864"),
		]);
		let engine = config.datastore_config().unwrap().load::<SurrealKvConfig>();
		assert_eq!(engine.block_cache_capacity, 256 * 1024 * 1024);
		assert_eq!(engine.max_memtable_size, 64 * 1024 * 1024);
	}

	/// Unset options leave the backend's defaults in place.
	#[test]
	fn unset_datastore_options_keep_the_backend_defaults() {
		let engine = LocalConfig::default().datastore_config().unwrap().load::<SurrealKvConfig>();
		let defaults = SurrealKvConfig::default();
		assert_eq!(engine.block_cache_capacity, defaults.block_cache_capacity);
		assert_eq!(engine.max_memtable_size, defaults.max_memtable_size);
	}

	/// The allowlist is a capability, so it is set only by its own field.
	#[test]
	fn bucket_folder_allowlist_is_refused_as_a_datastore_option() {
		for key in ["bucket_folder_allowlist", "BUCKET_FOLDER_ALLOWLIST"] {
			assert!(options(&[(key, "/")]).datastore_config().is_err(), "{key}");
		}
	}
}
