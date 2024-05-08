use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::env;
use std::fmt;
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};

use channel::{Receiver, Sender};
use futures::{lock::Mutex, Future};
use tokio::sync::RwLock;
use tracing::instrument;
use tracing::trace;

#[cfg(target_arch = "wasm32")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

use crate::cf;
use crate::cf::{ChangeSet, TableMutation};
use crate::ctx::Context;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::{
	node::Timestamp, Attach, Capabilities, Executor, Notification, Options, Response, Session,
	Statement, Variables, Workable,
};
use crate::doc::Document;
use crate::err::Error;
use crate::fflags::FFLAGS;
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::iam::{Action, Auth, Error as IamError, Resource, Role};
use crate::idx::trees::store::IndexStores;
use crate::key::root::hb::Hb;
use crate::kvs::clock::SizedClock;
#[allow(unused_imports)]
use crate::kvs::clock::SystemClock;
use crate::kvs::lq_structs::{
	LqIndexKey, LqIndexValue, LqSelector, LqValue, TrackedResult, UnreachableLqType,
};
use crate::kvs::{LockType, LockType::*, TransactionType, TransactionType::*};
use crate::options::EngineOptions;
use crate::sql::statements::show::ShowSince;
use crate::sql::{self, statements::DefineUserStatement, Base, Query, Uuid, Value};
use crate::syn;
use crate::vs::{conv, Oracle, Versionstamp};

use super::tx::Transaction;

// If there are an infinite number of heartbeats, then we want to go batch-by-batch spread over several checks
const HEARTBEAT_BATCH_SIZE: u32 = 1000;
const LQ_CHANNEL_SIZE: usize = 100;

// The batch size used for non-paged operations (i.e. if there are more results, they are ignored)
const NON_PAGED_BATCH_SIZE: u32 = 100_000;

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
#[non_exhaustive]
pub struct Datastore {
	// The inner datastore type
	inner: Inner,
	// The unique id of this datastore, used in notifications
	id: Uuid,
	// Whether this datastore runs in strict mode by default
	strict: bool,
	// Whether authentication is enabled on this datastore.
	auth_enabled: bool,
	// Whether authentication level is enabled on this datastore.
	// TODO(gguillemas): Remove this field once the legacy authentication is deprecated in v2.0.0
	auth_level_enabled: bool,
	// The maximum duration timeout for running multiple statements in a query
	query_timeout: Option<Duration>,
	// The maximum duration timeout for running multiple statements in a transaction
	transaction_timeout: Option<Duration>,
	// Capabilities for this datastore
	capabilities: Capabilities,
	engine_options: EngineOptions,
	// The versionstamp oracle for this datastore.
	// Used only in some datastores, such as tikv.
	versionstamp_oracle: Arc<Mutex<Oracle>>,
	// Whether this datastore enables live query notifications to subscribers
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
	// Map of Live Query identifier (ns+db+tb) for change feed tracking
	// the mapping is to a list of affected live queries
	local_live_queries: Arc<RwLock<BTreeMap<LqIndexKey, Vec<LqIndexValue>>>>,
	// Set of tracked change feeds with associated watermarks
	// This is updated with new/removed live queries and improves cf request performance
	cf_watermarks: Arc<Mutex<BTreeMap<LqSelector, Versionstamp>>>,
	// Clock for tracking time. It is read only and accessible to all transactions. It is behind a mutex as tests may write to it.
	clock: Arc<SizedClock>,
	// The index store cache
	index_stores: IndexStores,
	#[cfg(feature = "jwks")]
	// The JWKS object cache
	jwks_cache: Arc<RwLock<JwksCache>>,
	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	// The temporary directory
	temporary_directory: Arc<PathBuf>,
}

/// We always want to be circulating the live query information
/// And we will sometimes have an error attached but still not want to lose the LQ.
pub(crate) type BootstrapOperationResult = (LqValue, Option<Error>);

#[allow(clippy::large_enum_variant)]
pub(super) enum Inner {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Datastore),
	#[cfg(feature = "kv-speedb")]
	SpeeDB(super::speedb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
	#[cfg(feature = "kv-fdb")]
	FoundationDB(super::fdb::Datastore),
	#[cfg(feature = "kv-surrealkv")]
	SurrealKV(super::surrealkv::Datastore),
}

impl fmt::Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-speedb")]
			Inner::SpeeDB(_) => write!(f, "speedb"),
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			Inner::FoundationDB(_) => write!(f, "fdb"),
			#[cfg(feature = "kv-surrealkv")]
			Inner::SurrealKV(_) => write!(f, "surrealkv"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Datastore {
	/// Creates a new datastore instance
	///
	/// # Examples
	///
	/// ```rust,no_run
	/// # use surrealdb_core::kvs::Datastore;
	/// # use surrealdb_core::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("memory").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to create a file-backed store:
	///
	/// ```rust,no_run
	/// # use surrealdb_core::kvs::Datastore;
	/// # use surrealdb_core::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("file://temp.db").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to connect to a tikv-backed distributed store:
	///
	/// ```rust,no_run
	/// # use surrealdb_core::kvs::Datastore;
	/// # use surrealdb_core::err::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(), Error> {
	/// let ds = Datastore::new("tikv://127.0.0.1:2379").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub async fn new(path: &str) -> Result<Datastore, Error> {
		Self::new_full_impl(path, None).await
	}

	#[allow(dead_code)]
	#[cfg(test)]
	pub async fn new_full(
		path: &str,
		clock_override: Option<Arc<SizedClock>>,
	) -> Result<Datastore, Error> {
		Self::new_full_impl(path, clock_override).await
	}

	#[allow(dead_code)]
	async fn new_full_impl(
		path: &str,
		#[allow(unused_variables)] clock_override: Option<Arc<SizedClock>>,
	) -> Result<Datastore, Error> {
		#[allow(unused_variables)]
		let default_clock: Arc<SizedClock> = Arc::new(SizedClock::System(SystemClock::new()));

		// removes warning if no storage is enabled.
		#[cfg(not(any(
			feature = "kv-mem",
			feature = "kv-rocksdb",
			feature = "kv-speedb",
			feature = "kv-indxdb",
			feature = "kv-tikv",
			feature = "kv-fdb",
			feature = "kv-surrealkv"
		)))]
		let _ = (clock_override, default_clock);

		// Initiate the desired datastore
		let (inner, clock): (Result<Inner, Error>, Arc<SizedClock>) = match path {
			"memory" => {
				#[cfg(feature = "kv-mem")]
				{
					info!("Starting kvs store in {}", path);
					let v = super::mem::Datastore::new().await.map(Inner::Mem);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					info!("Started kvs store in {}", path);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-mem"))]
                return Err(Error::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an File database
			s if s.starts_with("file:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("file://");
					let s = s.trim_start_matches("file:");
					let v = super::rocksdb::Datastore::new(s).await.map(Inner::RocksDB);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					info!("Started kvs store at {}", path);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an RocksDB database
			s if s.starts_with("rocksdb:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("rocksdb://");
					let s = s.trim_start_matches("rocksdb:");
					let v = super::rocksdb::Datastore::new(s).await.map(Inner::RocksDB);
					info!("Started kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an SpeeDB database
			s if s.starts_with("speedb:") => {
				#[cfg(feature = "kv-speedb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("speedb://");
					let s = s.trim_start_matches("speedb:");
					let v = super::speedb::Datastore::new(s).await.map(Inner::SpeeDB);
					info!("Started kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-speedb"))]
                return Err(Error::Ds("Cannot connect to the `speedb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an IndxDB database
			s if s.starts_with("indxdb:") => {
				#[cfg(feature = "kv-indxdb")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("indxdb://");
					let s = s.trim_start_matches("indxdb:");
					let v = super::indxdb::Datastore::new(s).await.map(Inner::IndxDB);
					info!("Started kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-indxdb"))]
                return Err(Error::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a TiKV database
			s if s.starts_with("tikv:") => {
				#[cfg(feature = "kv-tikv")]
				{
					info!("Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("tikv://");
					let s = s.trim_start_matches("tikv:");
					let v = super::tikv::Datastore::new(s).await.map(Inner::TiKV);
					info!("Connected to kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-tikv"))]
                return Err(Error::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a FoundationDB database
			s if s.starts_with("fdb:") => {
				#[cfg(feature = "kv-fdb")]
				{
					info!("Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("fdb://");
					let s = s.trim_start_matches("fdb:");
					let v = super::fdb::Datastore::new(s).await.map(Inner::FoundationDB);
					info!("Connected to kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-fdb"))]
                return Err(Error::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a SurrealKV database
			s if s.starts_with("surrealkv:") => {
				#[cfg(feature = "kv-surrealkv")]
				{
					info!("Starting kvs store at {}", path);
					let s = s.trim_start_matches("surrealkv://");
					let s = s.trim_start_matches("surrealkv:");
					let v = super::surrealkv::Datastore::new(s).await.map(Inner::SurrealKV);
					info!("Started to kvs store at {}", path);
					let default_clock = Arc::new(SizedClock::System(SystemClock::new()));
					let clock = clock_override.unwrap_or(default_clock);
					Ok((v, clock))
				}
				#[cfg(not(feature = "kv-surrealkv"))]
                return Err(Error::Ds("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// The datastore path is not valid
			_ => {
				// use clock_override and default_clock to remove warning when no kv is enabled.
				let _ = default_clock;
				info!("Unable to load the specified datastore {}", path);
				Err(Error::Ds("Unable to load the specified datastore".into()))
			}
		}?;
		// Set the properties on the datastore
		inner.map(|inner| Self {
			id: Uuid::new_v4(),
			inner,
			strict: false,
			auth_enabled: false,
			// TODO(gguillemas): Remove this field once the legacy authentication is deprecated in v2.0.0
			auth_level_enabled: false,
			query_timeout: None,
			transaction_timeout: None,
			notification_channel: None,
			capabilities: Capabilities::default(),
			engine_options: EngineOptions::default(),
			versionstamp_oracle: Arc::new(Mutex::new(Oracle::systime_counter())),
			clock,
			index_stores: IndexStores::default(),
			local_live_queries: Arc::new(RwLock::new(BTreeMap::new())),
			cf_watermarks: Arc::new(Mutex::new(BTreeMap::new())),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			temporary_directory: Arc::new(env::temp_dir()),
		})
	}

	/// Specify whether this Datastore should run in strict mode
	pub fn with_node_id(mut self, id: Uuid) -> Self {
		self.id = id;
		self
	}

	/// Specify whether this Datastore should run in strict mode
	pub fn with_strict_mode(mut self, strict: bool) -> Self {
		self.strict = strict;
		self
	}

	/// Specify whether this datastore should enable live query notifications
	pub fn with_notifications(mut self) -> Self {
		self.notification_channel = Some(channel::bounded(LQ_CHANNEL_SIZE));
		self
	}

	/// Set a global query timeout for this Datastore
	pub fn with_query_timeout(mut self, duration: Option<Duration>) -> Self {
		self.query_timeout = duration;
		self
	}

	/// Set a global transaction timeout for this Datastore
	pub fn with_transaction_timeout(mut self, duration: Option<Duration>) -> Self {
		self.transaction_timeout = duration;
		self
	}

	/// Set whether authentication is enabled for this Datastore
	pub fn with_auth_enabled(mut self, enabled: bool) -> Self {
		self.auth_enabled = enabled;
		self
	}

	/// Set whether authentication levels are enabled for this Datastore
	/// TODO(gguillemas): Remove this method once the legacy authentication is deprecated in v2.0.0
	pub fn with_auth_level_enabled(mut self, enabled: bool) -> Self {
		self.auth_level_enabled = enabled;
		self
	}

	/// Set specific capabilities for this Datastore
	pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
		self.capabilities = caps;
		self
	}

	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	pub fn with_temporary_directory(mut self, path: Option<PathBuf>) -> Self {
		self.temporary_directory = Arc::new(path.unwrap_or_else(env::temp_dir));
		self
	}

	/// Set the engine options for the datastore
	pub fn with_engine_options(mut self, engine_options: EngineOptions) -> Self {
		self.engine_options = engine_options;
		self
	}

	pub fn index_store(&self) -> &IndexStores {
		&self.index_stores
	}

	/// Is authentication enabled for this Datastore?
	pub fn is_auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	#[cfg(any(
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
		feature = "kv-speedb"
	))]
	pub(crate) fn is_memory(&self) -> bool {
		#[cfg(feature = "kv-mem")]
		if matches!(self.inner, Inner::Mem(_)) {
			return true;
		};
		false
	}

	/// Is authentication level enabled for this Datastore?
	/// TODO(gguillemas): Remove this method once the legacy authentication is deprecated in v2.0.0
	pub fn is_auth_level_enabled(&self) -> bool {
		self.auth_level_enabled
	}

	/// Does the datastore allow connections to a network target?
	#[cfg(feature = "jwks")]
	pub(crate) fn allows_network_target(&self, net_target: &NetTarget) -> bool {
		self.capabilities.allows_network_target(net_target)
	}

	#[cfg(feature = "jwks")]
	pub(crate) fn jwks_cache(&self) -> &Arc<RwLock<JwksCache>> {
		&self.jwks_cache
	}

	/// Setup the initial credentials
	/// Trigger the `unreachable definition` compilation error, probably due to this issue:
	/// https://github.com/rust-lang/rust/issues/111370
	#[allow(unreachable_code, unused_variables)]
	pub async fn setup_initial_creds(&self, username: &str, password: &str) -> Result<(), Error> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Optimistic).await?.rollback_with_panic().enclose();
		// Fetch the root users from the storage
		let users = txn.lock().await.all_root_users().await;
		// Process credentials, depending on existing users
		match users {
			Ok(v) if v.is_empty() => {
				// Display information in the logs
				info!("Credentials were provided, and no root users were found. The root user '{}' will be created", username);
				// Create and save a new root users
				let stm = DefineUserStatement::from((Base::Root, username, password));
				let ctx = Context::default();
				let opt = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
				let _ = stm.compute(&ctx, &opt, &txn, None).await?;
				// We added a new user, so commit the transaction
				txn.lock().await.commit().await?;
				// Everything ok
				Ok(())
			}
			Ok(_) => {
				// Display warnings in the logs
				warn!("Credentials were provided, but existing root users were found. The root user '{}' will not be created", username);
				warn!("Consider removing the --user and --pass arguments from the server start command");
				// We didn't write anything, so just rollback
				txn.lock().await.cancel().await?;
				// Everything ok
				Ok(())
			}
			Err(e) => {
				// There was an unexpected error, so rollback
				txn.lock().await.cancel().await?;
				// Return any error
				Err(e)
			}
		}
	}

	// Initialise bootstrap with implicit values intended for runtime
	// An error indicates that a failure happened, but that does not mean that the bootstrap
	// completely failed. It may have partially completed. It certainly has side-effects
	// that weren't reversed, as it tries to bootstrap and garbage collect to the best of its
	// ability.
	// NOTE: If you get rust mutex deadlocks, check your transactions around this method.
	// This should be called before any transactions are made in release mode
	// In tests, it should be outside any other transaction - in isolation.
	// We cannot easily systematise this, since we aren't counting transactions created.
	pub async fn bootstrap(&self) -> Result<(), Error> {
		// First we clear unreachable state that could exist by upgrading from
		// previous beta versions
		trace!("Clearing unreachable state");
		let mut tx = self.transaction(Write, Optimistic).await?;
		match self.clear_unreachable_state(&mut tx).await {
			Ok(_) => tx.commit().await,
			Err(e) => {
				let msg = format!("Error clearing unreachable cluster state at bootstrap: {:?}", e);
				error!(msg);
				tx.cancel().await?;
				Err(Error::Tx(msg))
			}
		}?;

		trace!("Bootstrapping {}", self.id);
		let mut tx = self.transaction(Write, Optimistic).await?;
		let archived = match self.register_remove_and_archive(&mut tx, &self.id).await {
			Ok(archived) => {
				tx.commit().await?;
				archived
			}
			Err(e) => {
				error!("Error bootstrapping mark phase: {:?}", e);
				tx.cancel().await?;
				return Err(e);
			}
		};
		// Filtered includes all lqs that should be used in subsequent step
		// Currently that is all of them, no matter the error encountered
		let mut filtered: Vec<LqValue> = vec![];
		// err is used to aggregate all errors across all stages
		let mut err = vec![];
		for res in archived {
			match res {
				(lq, Some(e)) => {
					filtered.push(lq);
					err.push(e);
				}
				(lq, None) => {
					filtered.push(lq);
				}
			}
		}

		let mut tx = self.transaction(Write, Optimistic).await?;
		let val = self.remove_archived(&mut tx, filtered).await;
		let resolve_err = match val {
			Ok(_) => tx.commit().await,
			Err(e) => {
				error!("Error bootstrapping sweep phase: {:?}", e);
				match tx.cancel().await {
					Ok(_) => Err(e),
					Err(e) => {
						// We have a nested error
						Err(Error::Tx(format!("Error bootstrapping sweep phase: {:?} and error cancelling transaction: {:?}", e, e)))
					}
				}
			}
		};
		if let Err(e) = resolve_err {
			err.push(e);
		}
		if !err.is_empty() {
			error!("Error bootstrapping sweep phase: {:?}", err);
			return Err(Error::Tx(format!("Error bootstrapping sweep phase: {:?}", err)));
		}
		Ok(())
	}

	// Node registration + "mark" stage of mark-and-sweep gc
	pub async fn register_remove_and_archive(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
	) -> Result<Vec<BootstrapOperationResult>, Error> {
		trace!("Registering node {}", node_id);
		let timestamp = tx.clock().await;
		self.register_membership(tx, node_id, timestamp).await?;
		// Determine the timeout for when a cluster node is expired
		let ts_expired = (&timestamp - &sql::duration::Duration::from_secs(5))?;
		let dead = self.remove_dead_nodes(tx, &ts_expired).await?;
		trace!("Archiving dead nodes: {:?}", dead);
		self.archive_dead_lqs(tx, &dead, node_id).await
	}

	// Adds entries to the KV store indicating membership information
	pub async fn register_membership(
		&self,
		tx: &mut Transaction,
		node_id: &Uuid,
		timestamp: Timestamp,
	) -> Result<(), Error> {
		tx.set_nd(node_id.0).await?;
		tx.set_hb(timestamp, node_id.0).await?;
		Ok(())
	}

	/// Delete dead heartbeats and nodes
	/// Returns node IDs
	pub async fn remove_dead_nodes(
		&self,
		tx: &mut Transaction,
		ts: &Timestamp,
	) -> Result<Vec<Uuid>, Error> {
		let hbs = self.delete_dead_heartbeats(tx, ts).await?;
		trace!("Found {} expired heartbeats", hbs.len());
		let mut nodes = vec![];
		for hb in hbs {
			trace!("Deleting node {}", &hb.nd);
			// TODO should be delr in case of nested entries
			tx.del_nd(hb.nd).await?;
			nodes.push(crate::sql::uuid::Uuid::from(hb.nd));
		}
		Ok(nodes)
	}

	/// Accepts cluster IDs
	/// Archives related live queries
	/// Returns live query keys that can be used for deletes
	///
	/// The reason we archive first is to stop other nodes from picking it up for further updates
	/// This means it will be easier to wipe the range in a subsequent transaction
	pub async fn archive_dead_lqs(
		&self,
		tx: &mut Transaction,
		nodes: &[Uuid],
		this_node_id: &Uuid,
	) -> Result<Vec<BootstrapOperationResult>, Error> {
		let mut archived = vec![];
		for nd in nodes.iter() {
			trace!("Archiving node {}", &nd);
			// Scan on node prefix for LQ space
			let node_lqs = tx.scan_ndlq(nd, NON_PAGED_BATCH_SIZE).await?;
			trace!("Found {} LQ entries for {:?}", node_lqs.len(), nd);
			for lq in node_lqs {
				trace!("Archiving query {:?}", &lq);
				let node_archived_lqs =
					match self.archive_lv_for_node(tx, &lq.nd, *this_node_id).await {
						Ok(lq) => lq,
						Err(e) => {
							error!("Error archiving lqs during bootstrap phase: {:?}", e);
							vec![]
						}
					};
				// We need to add lv nodes not found so that they can be deleted in second stage
				for lq_value in node_archived_lqs {
					archived.push(lq_value);
				}
			}
		}
		Ok(archived)
	}

	pub async fn remove_archived(
		&self,
		tx: &mut Transaction,
		archived: Vec<LqValue>,
	) -> Result<(), Error> {
		trace!("Gone into removing archived: {:?}", archived.len());
		for lq in archived {
			// Delete the cluster key, used for finding LQ associated with a node
			let key = crate::key::node::lq::new(lq.nd.0, lq.lq.0, &lq.ns, &lq.db);
			tx.del(key).await?;
			// Delete the table key, used for finding LQ associated with a table
			let key = crate::key::table::lq::new(&lq.ns, &lq.db, &lq.tb, lq.lq.0);
			tx.del(key).await?;
		}
		Ok(())
	}

	pub async fn clear_unreachable_state(&self, tx: &mut Transaction) -> Result<(), Error> {
		// Scan nodes
		let cluster = tx.scan_nd(NON_PAGED_BATCH_SIZE).await?;
		trace!("Found {} nodes", cluster.len());
		let mut unreachable_nodes = BTreeMap::new();
		for cl in &cluster {
			unreachable_nodes.insert(cl.name.clone(), cl.clone());
		}
		// Scan all heartbeats
		let end_of_time = Timestamp {
			// We remove one, because the scan range adds one
			value: u64::MAX - 1,
		};
		let hbs = tx.scan_hb(&end_of_time, NON_PAGED_BATCH_SIZE).await?;
		trace!("Found {} heartbeats", hbs.len());
		for hb in hbs {
			match unreachable_nodes.remove(&hb.nd.to_string()) {
				None => {
					// Didnt exist in cluster and should be deleted
					tx.del_hb(hb.hb, hb.nd).await?;
				}
				Some(_) => {}
			}
		}
		// Remove unreachable nodes
		for (_, cl) in unreachable_nodes {
			trace!("Removing unreachable node {}", cl.name);
			tx.del_nd(
				uuid::Uuid::parse_str(&cl.name).map_err(|e| {
					Error::Unimplemented(format!("cluster id was not uuid: {:?}", e))
				})?,
			)
			.await?;
		}
		// Scan node live queries for every node
		let mut nd_lq_set: BTreeSet<UnreachableLqType> = BTreeSet::new();
		for cl in &cluster {
			let nds = tx.scan_ndlq(&uuid::Uuid::parse_str(&cl.name).map_err(|e| {
                Error::Unimplemented(format!("cluster id was not uuid when parsing to aggregate cluster live queries: {:?}", e))
            })?, NON_PAGED_BATCH_SIZE).await?;
			nd_lq_set.extend(nds.into_iter().map(UnreachableLqType::Nd));
		}
		trace!("Found {} node live queries", nd_lq_set.len());
		// Scan tables for all live queries
		// let mut tb_lqs: Vec<LqValue> = vec![];
		let mut tb_lq_set: BTreeSet<UnreachableLqType> = BTreeSet::new();
		for ndlq in &nd_lq_set {
			let lq = ndlq.get_inner();
			let tbs = tx.scan_tblq(&lq.ns, &lq.db, &lq.tb, NON_PAGED_BATCH_SIZE).await?;
			tb_lq_set.extend(tbs.into_iter().map(UnreachableLqType::Tb));
		}
		trace!("Found {} table live queries", tb_lq_set.len());
		// Find and delete missing
		for missing in nd_lq_set.symmetric_difference(&tb_lq_set) {
			match missing {
				UnreachableLqType::Nd(ndlq) => {
					warn!("Deleting ndlq {:?}", &ndlq);
					tx.del_ndlq(ndlq.nd.0, ndlq.lq.0, &ndlq.ns, &ndlq.db).await?;
				}
				UnreachableLqType::Tb(tblq) => {
					warn!("Deleting tblq {:?}", &tblq);
					tx.del_tblq(&tblq.ns, &tblq.db, &tblq.tb, tblq.lq.0).await?;
				}
			}
		}
		trace!("Successfully cleared cluster of unreachable state");
		Ok(())
	}

	// Garbage collection task to run when a client disconnects from a surrealdb node
	// i.e. we know the node, we are not performing a full wipe on the node
	// and the wipe must be fully performed by this node
	pub async fn garbage_collect_dead_session(
		&self,
		live_queries: &[uuid::Uuid],
	) -> Result<(), Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;

		// Find all the LQs we own, so that we can get the ns/ds from provided uuids
		// We may improve this in future by tracking in web layer
		let lqs = tx.scan_ndlq(&self.id, NON_PAGED_BATCH_SIZE).await?;
		let mut hits = vec![];
		for lq_value in lqs {
			if live_queries.contains(&lq_value.lq) {
				hits.push(lq_value.clone());
				let lq = crate::key::node::lq::Lq::new(
					lq_value.nd.0,
					lq_value.lq.0,
					lq_value.ns.as_str(),
					lq_value.db.as_str(),
				);
				tx.del(lq).await?;
				trace!("Deleted lq {:?} as part of session garbage collection", lq_value.clone());
			}
		}

		// Now delete the table entries for the live queries
		for lq in hits {
			let lv =
				crate::key::table::lq::new(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), lq.lq.0);
			tx.del(lv.clone()).await?;
			trace!("Deleted lv {:?} as part of session garbage collection", lv);
		}
		tx.commit().await
	}

	// Returns a list of live query IDs
	pub async fn archive_lv_for_node(
		&self,
		tx: &mut Transaction,
		nd: &Uuid,
		this_node_id: Uuid,
	) -> Result<Vec<BootstrapOperationResult>, Error> {
		let lqs = tx.all_lq(nd).await?;
		trace!("Archiving lqs and found {} LQ entries for {}", lqs.len(), nd);
		let mut ret: Vec<BootstrapOperationResult> = vec![];
		for lq in lqs {
			let lv_res =
				tx.get_tb_live(lq.ns.as_str(), lq.db.as_str(), lq.tb.as_str(), &lq.lq).await;
			if let Err(e) = lv_res {
				error!("Error getting live query for node {}: {:?}", nd, e);
				ret.push((lq, Some(e)));
				continue;
			}
			let lv = lv_res.unwrap();
			let archived_lvs = lv.clone().archive(this_node_id);
			tx.putc_tblq(&lq.ns, &lq.db, &lq.tb, archived_lvs, Some(lv)).await?;
			ret.push((lq, None));
		}
		Ok(ret)
	}

	/// Given a timestamp, delete all the heartbeats that have expired
	/// Return the removed heartbeats as they will contain node information
	pub async fn delete_dead_heartbeats(
		&self,
		tx: &mut Transaction,
		ts: &Timestamp,
	) -> Result<Vec<Hb>, Error> {
		let dead = tx.scan_hb(ts, HEARTBEAT_BATCH_SIZE).await?;
		// Delete the heartbeat and everything nested
		tx.delr_hb(dead.clone(), NON_PAGED_BATCH_SIZE).await?;
		for dead_node in dead.clone() {
			tx.del_nd(dead_node.nd).await?;
		}
		Ok::<Vec<Hb>, Error>(dead)
	}

	// tick is called periodically to perform maintenance tasks.
	// This is called every TICK_INTERVAL.
	pub async fn tick(&self) -> Result<(), Error> {
		let now = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|e| {
			Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration()))
		})?;
		let ts = now.as_secs();
		self.tick_at(ts).await?;
		Ok(())
	}

	// tick_at is the utility function that is called by tick.
	// It is handy for testing, because it allows you to specify the timestamp,
	// without depending on a system clock.
	pub async fn tick_at(&self, ts: u64) -> Result<(), Error> {
		trace!("Ticking at timestamp {}", ts);
		let _vs = self.save_timestamp_for_versionstamp(ts).await?;
		self.garbage_collect_stale_change_feeds(ts).await?;
		// TODO Add LQ GC
		// TODO Add Node GC?
		Ok(())
	}

	// save_timestamp_for_versionstamp saves the current timestamp for the each database's current versionstamp.
	// Note: the returned VS is flawed, as there are multiple {ts: vs} mappings per (ns, db)
	pub(crate) async fn save_timestamp_for_versionstamp(
		&self,
		ts: u64,
	) -> Result<Option<Versionstamp>, Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;
		match self.save_timestamp_for_versionstamp_impl(ts, &mut tx).await {
            Ok(vs) => Ok(vs),
            Err(e) => {
                match tx.cancel().await {
                    Ok(_) => {
                        Err(e)
                    }
                    Err(txe) => {
                        Err(Error::Tx(format!("Error saving timestamp for versionstamp: {:?} and error cancelling transaction: {:?}", e, txe)))
                    }
                }
            }
        }
	}

	/// Poll change feeds for live query notifications
	pub async fn process_lq_notifications(&self, opt: &Options) -> Result<(), Error> {
		// Runtime feature gate, as it is not production-ready
		if !FFLAGS.change_feed_live_queries.enabled() {
			return Ok(());
		}
		// Return if there are no live queries
		if self.notification_channel.is_none() {
			trace!("Channels is none, short-circuiting");
			return Ok(());
		}
		if self.local_live_queries.read().await.is_empty() {
			trace!("No live queries, short-circuiting");
			return Ok(());
		}

		// Change map includes a mapping of selector to changesets, ordered by versionstamp
		let mut change_map: BTreeMap<LqSelector, Vec<ChangeSet>> = BTreeMap::new();
		{
			let tx = self.transaction(Read, Optimistic).await?;
			let tracked_cfs_updates = find_required_cfs_to_catch_up(
				tx,
				self.cf_watermarks.clone(),
				self.engine_options.live_query_catchup_size,
				&mut change_map,
			)
			.await?;
			// Now we update since we are no longer iterating immutably
			let mut tracked_cfs = self.cf_watermarks.lock().await;
			for (selector, vs) in tracked_cfs_updates {
				tracked_cfs.insert(selector, vs);
			}
		};

		for (selector, change_sets) in change_map {
			// find matching live queries
			let lq_pairs: Vec<(LqIndexKey, LqIndexValue)> = {
				let lq_lock = self.local_live_queries.read().await;
				lq_lock
					.iter()
					.filter(|(k, _)| k.selector == selector)
					.flat_map(|(lq_index, lq_values)| {
						lq_values.iter().cloned().map(|x| (lq_index.clone(), x))
					})
					.to_owned()
					.collect()
			};

			// Find relevant changes
			let tx = Arc::new(Mutex::new(self.transaction(Read, Optimistic).await?));
			trace!("There are {} change sets", change_sets.len());
			trace!(
				"\n{}",
				change_sets
					.iter()
					.enumerate()
					.map(|(i, x)| format!("[{i}] {:?}", x))
					.collect::<Vec<String>>()
					.join("\n")
			);
			for change_set in change_sets {
				self.process_change_set_for_notifications(tx.clone(), opt, change_set, &lq_pairs)
					.await?;
			}
		}
		trace!("Finished process lq successfully");
		Ok(())
	}

	async fn process_change_set_for_notifications(
		&self,
		tx: Arc<Mutex<Transaction>>,
		opt: &Options,
		change_set: ChangeSet,
		lq_pairs: &[(LqIndexKey, LqIndexValue)],
	) -> Result<(), Error> {
		// TODO(phughk): this loop can be on the inside so we are only checking lqs relavant to cf change
		trace!("Moving to next change set, {:?}", change_set);
		for (lq_key, lq_value) in lq_pairs.iter() {
			trace!(
				"Processing live query for notification key={:?} and value={:?}",
				lq_key,
				lq_value
			);
			let change_vs = change_set.0;
			let database_mutation = &change_set.1;
			for table_mutations in database_mutation.0.iter() {
				if table_mutations.0 == lq_key.selector.tb {
					// Create a doc of the table value
					// Run the 'lives' logic on the doc, while providing live queries instead of reading from storage
					// This will generate and send notifications
					trace!(
						"There are {} table mutations being prepared for notifications",
						table_mutations.1.len()
					);
					for (i, mutation) in table_mutations.1.iter().enumerate() {
						trace!(
							"[{} @ {:?}] Processing table mutation: {:?}",
							i,
							change_vs,
							mutation
						);
						trace!("Constructing document from mutation");
						if let Some(doc) = Self::construct_document(mutation) {
							// We know we are only processing a single LQ at a time, so we can limit notifications to 1
							let notification_capacity = 1;
							// We track notifications as a separate channel in case we want to process
							// for the current state we only forward
							let (sender, receiver) = channel::bounded(notification_capacity);
							doc.check_lqs_and_send_notifications(
								opt,
								&Statement::Live(&lq_value.stm),
								&tx,
								[&lq_value.stm].as_slice(),
								&sender,
							)
							.await
							.map_err(|e| {
								Error::Internal(format!(
									"Error checking lqs for notifications: {:?}",
									e
								))
							})?;

							// Send the notifications to driver or api
							// TODO: evaluate if we want channel directly instead of proxy
							while let Ok(notification) = receiver.try_recv() {
								trace!("Sending notification to client");
								self.notification_channel
									.as_ref()
									.unwrap()
									.0
									.send(notification)
									.await
									.unwrap();
							}
							trace!("Ended notification sending")
						}

						self.update_versionstamp(&change_vs, lq_key, lq_value).await;
					}
				}
			}
		}
		Ok(())
	}

	async fn update_versionstamp(
		&self,
		change_vs: &Versionstamp,
		lq_key: &LqIndexKey,
		lq_value: &LqIndexValue,
	) {
		// We increase the watermark because scans are inclusive of first result
		// And we have already processed the input watermark - it is derived from the event
		// let change_vs = conv::try_u128_to_versionstamp(conv::to_u128_be(*change_vs) + 1).unwrap();

		// Update watermarks
		trace!("Updating watermark to {:?} for index key {:?}", change_vs, lq_key);
		// For each live query we have processed we update the watermarks
		self.local_live_queries.write().await.insert(
			lq_key.clone(),
			vec![LqIndexValue {
				vs: *change_vs,
				..lq_value.clone()
			}],
		);

		// TODO(phugk) We also update the tracked_cfs with a minimum watermark
		let mut tracked_cfs = self.cf_watermarks.lock().await;
		// TODO we may be able to re-use the key without cloning...
		tracked_cfs.insert(lq_key.selector.clone(), *change_vs).unwrap();
	}

	/// Construct a document from a Change Feed mutation
	/// This is required to perform document operations such as live query notifications
	fn construct_document(mutation: &TableMutation) -> Option<Document> {
		match mutation {
			TableMutation::Set(id, current_value) => {
				let doc = Document::new(None, Some(id), None, current_value, Workable::Normal);
				Some(doc)
			}
			TableMutation::Del(id) => {
				let doc = Document::new(None, Some(id), None, &Value::None, Workable::Normal);
				Some(doc)
			}
			TableMutation::Def(_) => None,
			TableMutation::SetWithDiff(id, current_value, _operations) => {
				let todo_original_after_reverse_applying_patches = Value::None;
				let doc = Document::new_artificial(
					None,
					Some(id),
					None,
					Cow::Borrowed(current_value),
					Cow::Owned(todo_original_after_reverse_applying_patches),
					Workable::Normal,
				);
				trace!("Constructed artificial document: {:?}, is_new={}", doc, doc.is_new());
				// TODO(SUR-328): reverse diff and apply to doc to retrieve original version of doc
				Some(doc)
			}
		}
	}

	/// Add and kill live queries being track on the datastore
	/// These get polled by the change feed tick
	pub(crate) async fn adapt_tracked_live_queries(
		&self,
		lqs: &Vec<TrackedResult>,
	) -> Result<(), Error> {
		// Lock the local live queries
		let mut lq_map = self.local_live_queries.write().await;
		let mut cf_watermarks = self.cf_watermarks.lock().await;
		let mut watermarks_to_check: Vec<LqIndexKey> = vec![];
		for lq in lqs {
			match lq {
				TrackedResult::LiveQuery(lq) => {
					let lq_index_key: LqIndexKey = lq.as_key();
					let m = lq_map.get_mut(&lq_index_key);
					match m {
						Some(lq_index_value) => lq_index_value.push(lq.as_value()),
						None => {
							let lq_vec = vec![lq.as_value()];
							lq_map.insert(lq_index_key.clone(), lq_vec);
						}
					}
					let selector = lq_index_key.selector;
					// TODO(phughk): - read watermark for catchup
					// We insert the current watermark.
					cf_watermarks.entry(selector).or_insert_with(Versionstamp::default);
				}
				TrackedResult::KillQuery(kill_entry) => {
					let found: Option<(LqIndexKey, LqIndexValue)> = lq_map
						.iter_mut()
						.filter(|(k, _)| {
							// Get all the live queries in the ns/db pair. We don't know table
							k.selector.ns == kill_entry.ns && k.selector.db == kill_entry.db
						})
						.filter_map(|(k, v)| {
							let index = v.iter().position(|a| a.stm.id == kill_entry.live_id);
							match index {
								Some(i) => {
									let v = v.remove(i);
									// Sadly we do need to clone out of mutable reference, because of Strings
									Some((k.clone(), v))
								}
								None => None,
							}
						})
						.next();
					match found {
						None => {
							// TODO(SUR-336): Make Live Query ID validation available at statement level, perhaps via transaction
							trace!(
								"Could not find live query {:?} to kill in ns/db pair {:?}",
								&kill_entry,
								&kill_entry.ns
							);
						}
						Some(found) => {
							trace!(
								"Killed live query {:?} with found key {:?} and found value {:?}",
								&kill_entry,
								&found.0,
								&found.1
							);
							// Check if we need to remove the LQ key from tracking
							let empty = match lq_map.get(&found.0) {
								None => false,
								Some(v) => v.is_empty(),
							};
							if empty {
								trace!("Removing live query index key {:?}", &found.0);
								lq_map.remove(&found.0);
							}
							// Now add the LQ to tracked watermarks
							watermarks_to_check.push(found.0.clone());
						}
					};
				}
			}
		}
		// Now check if we can stop tracking watermarks
		for watermark in watermarks_to_check {
			if let Some(lq) = lq_map.get(&watermark) {
				if lq.is_empty() {
					trace!("Removing watermark for {:?}", watermark);
					cf_watermarks.remove(&watermark.selector);
				}
			}
		}
		Ok(())
	}

	async fn save_timestamp_for_versionstamp_impl(
		&self,
		ts: u64,
		tx: &mut Transaction,
	) -> Result<Option<Versionstamp>, Error> {
		let mut vs: Option<Versionstamp> = None;
		let nses = tx.all_ns().await?;
		let nses = nses.as_ref();
		for ns in nses {
			let ns = ns.name.as_str();
			let dbs = tx.all_db(ns).await?;
			let dbs = dbs.as_ref();
			for db in dbs {
				let db = db.name.as_str();
				// TODO(SUR-341): This is incorrect, it's a [ns,db] to vs pair
				vs = Some(tx.set_timestamp_for_versionstamp(ts, ns, db, true).await?);
			}
		}
		tx.commit().await?;
		Ok(vs)
	}

	// garbage_collect_stale_change_feeds deletes all change feed entries that are older than the watermarks.
	pub(crate) async fn garbage_collect_stale_change_feeds(&self, ts: u64) -> Result<(), Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;
		if let Err(e) = self.garbage_collect_stale_change_feeds_impl(ts, &mut tx).await {
			return match tx.cancel().await {
                Ok(_) => {
                    Err(e)
                }
                Err(txe) => {
                    Err(Error::Tx(format!("Error garbage collecting stale change feeds: {:?} and error cancelling transaction: {:?}", e, txe)))
                }
            };
		}
		Ok(())
	}

	async fn garbage_collect_stale_change_feeds_impl(
		&self,
		ts: u64,
		tx: &mut Transaction,
	) -> Result<(), Error> {
		// TODO Make gc batch size/limit configurable?
		cf::gc_all_at(tx, ts, Some(100)).await?;
		tx.commit().await?;
		Ok(())
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive.
	// This is the preferred way of creating heartbeats inside the database, so try to use this.
	pub async fn heartbeat(&self) -> Result<(), Error> {
		let mut tx = self.transaction(Write, Optimistic).await?;
		let timestamp = tx.clock().await;
		self.heartbeat_full(&mut tx, timestamp, self.id).await?;
		tx.commit().await
	}

	// Creates a heartbeat entry for the member indicating to the cluster
	// that the node is alive. Intended for testing.
	// This includes all dependencies that are hard to control and is done in such a way for testing.
	// Inside the database, try to use the heartbeat() function instead.
	pub async fn heartbeat_full(
		&self,
		tx: &mut Transaction,
		timestamp: Timestamp,
		node_id: Uuid,
	) -> Result<(), Error> {
		tx.set_hb(timestamp, node_id.0).await
	}

	// -----
	// End cluster helpers, storage functions here
	// -----

	/// Create a new transaction on this datastore
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::{Datastore, TransactionType::*, LockType::*};
	/// use surrealdb_core::err::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("file://database.db").await?;
	///     let mut tx = ds.transaction(Write, Optimistic).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
	) -> Result<Transaction, Error> {
		#![allow(unused_variables)]
		let write = match write {
			TransactionType::Read => false,
			TransactionType::Write => true,
		};

		let lock = match lock {
			LockType::Pessimistic => true,
			LockType::Optimistic => false,
		};

		let inner = match &self.inner {
			#[cfg(feature = "kv-mem")]
			Inner::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::Mem(tx)
			}
			#[cfg(feature = "kv-rocksdb")]
			Inner::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::RocksDB(tx)
			}
			#[cfg(feature = "kv-speedb")]
			Inner::SpeeDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::SpeeDB(tx)
			}
			#[cfg(feature = "kv-indxdb")]
			Inner::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::IndxDB(tx)
			}
			#[cfg(feature = "kv-tikv")]
			Inner::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::TiKV(tx)
			}
			#[cfg(feature = "kv-fdb")]
			Inner::FoundationDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::FoundationDB(tx)
			}
			#[cfg(feature = "kv-surrealkv")]
			Inner::SurrealKV(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tx::Inner::SurrealKV(tx)
			}
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		};

		let (send, recv): (Sender<TrackedResult>, Receiver<TrackedResult>) =
			channel::bounded(LQ_CHANNEL_SIZE);

		#[allow(unreachable_code)]
		Ok(Transaction {
			inner,
			cache: super::cache::Cache::default(),
			cf: cf::Writer::new(),
			vso: self.versionstamp_oracle.clone(),
			clock: self.clock.clone(),
			prepared_async_events: (Arc::new(send), Arc::new(recv)),
			engine_options: self.engine_options,
		})
	}

	/// Parse and execute an SQL query
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::err::Error;
	/// use surrealdb_core::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::owner();
	///     let ast = "USE NS test DB test; SELECT * FROM person;";
	///     let res = ds.execute(ast, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Parse the SQL query text
		let ast = syn::parse(txt)?;
		// Process the AST
		self.process(ast, sess, vars).await
	}

	/// Execute a pre-parsed SQL query
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::err::Error;
	/// use surrealdb_core::dbs::Session;
	/// use surrealdb_core::sql::parse;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::owner();
	///     let ast = parse("USE NS test DB test; SELECT * FROM person;")?;
	///     let res = ds.process(ast, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub async fn process(
		&self,
		ast: Query,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		if self.auth_enabled && sess.au.is_anon() && !self.capabilities.allows_guest_access() {
			return Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "process".to_string(),
				resource: "query".to_string(),
			}
			.into());
		}
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let mut ctx = Context::from_ds(
			self.query_timeout,
			self.capabilities.clone(),
			self.index_stores.clone(),
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			self.is_memory(),
			#[cfg(any(
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
				feature = "kv-speedb"
			))]
			self.temporary_directory.clone(),
		)?;
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Process all statements
		let res = exe.execute(ctx, opt, ast).await;
		match res {
			Ok((responses, lives)) => {
				// Register live queries
				self.adapt_tracked_live_queries(&lives).await?;
				Ok(responses)
			}
			Err(e) => Err(e),
		}
	}

	/// Ensure a SQL [`Value`] is fully computed
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::err::Error;
	/// use surrealdb_core::dbs::Session;
	/// use surrealdb_core::sql::Future;
	/// use surrealdb_core::sql::Value;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::owner();
	///     let val = Value::Future(Box::new(Future::from(Value::Bool(true))));
	///     let res = ds.compute(val, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub async fn compute(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Check if anonymous actors can compute values when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		if self.auth_enabled && !self.capabilities.allows_guest_access() {
			return Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "compute".to_string(),
				resource: "value".to_string(),
			}
			.into());
		}
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
		// Create a default context
		let mut ctx = Context::default();
		// Set context capabilities
		ctx.add_capabilities(self.capabilities.clone());
		// Set the global query timeout
		if let Some(timeout) = self.query_timeout {
			ctx.add_timeout(timeout)?;
		}
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Compute the value
		let res = val.compute(&ctx, &opt, &txn, None).await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.lock().await.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.lock().await.cancel().await?,
		};
		// Return result
		res
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a SCOPE SIGNUP or SCOPE
	/// SIGNIN clause, which still needs to work without guest access.
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::err::Error;
	/// use surrealdb_core::dbs::Session;
	/// use surrealdb_core::sql::Future;
	/// use surrealdb_core::sql::Value;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::owner();
	///     let val = Value::Future(Box::new(Future::from(Value::Bool(true))));
	///     let res = ds.evaluate(val, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub async fn evaluate(
		&self,
		val: Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id.0)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
		// Create a default context
		let mut ctx = Context::default();
		// Set context capabilities
		ctx.add_capabilities(self.capabilities.clone());
		// Set the global query timeout
		if let Some(timeout) = self.query_timeout {
			ctx.add_timeout(timeout)?;
		}
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		let ctx = sess.context(ctx);
		// Store the query variables
		let ctx = vars.attach(ctx)?;
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Compute the value
		let res = val.compute(&ctx, &opt, &txn, None).await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.lock().await.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.lock().await.cancel().await?,
		};
		// Return result
		res
	}

	/// Subscribe to live notifications
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::err::Error;
	/// use surrealdb_core::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(), Error> {
	///     let ds = Datastore::new("memory").await?.with_notifications();
	///     let ses = Session::owner();
	/// 	if let Some(channel) = ds.notifications() {
	///     	while let Ok(v) = channel.recv().await {
	///     	    println!("Received notification: {v}");
	///     	}
	/// 	}
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", skip_all)]
	pub fn notifications(&self) -> Option<Receiver<Notification>> {
		self.notification_channel.as_ref().map(|v| v.1.clone())
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", skip(self, sess, sql))]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<Response>, Error> {
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}

	/// Performs a full database export as SQL
	#[instrument(level = "debug", skip(self, sess, chn))]
	pub async fn export(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<(), Error>>, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Retrieve the provided NS and DB
		let (ns, db) = crate::iam::check::check_ns_db(sess)?;
		// Create a new readonly transaction
		let mut txn = self.transaction(Read, Optimistic).await?;
		// Return an async export job
		Ok(async move {
			// Process the export
			txn.export(&ns, &db, chn).await?;
			// Everything ok
			Ok(())
		})
	}

	/// Checks the required permissions level for this session
	#[instrument(level = "debug", skip(self, sess))]
	pub fn check(&self, sess: &Session, action: Action, resource: Resource) -> Result<(), Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(action, &resource)?;
		}
		// All ok
		Ok(())
	}
}

async fn find_required_cfs_to_catch_up(
	mut tx: Transaction,
	tracked_cfs: Arc<Mutex<BTreeMap<LqSelector, Versionstamp>>>,
	catchup_size: u32,
	change_map: &mut BTreeMap<LqSelector, Vec<ChangeSet>>,
) -> Result<Vec<(LqSelector, Versionstamp)>, Error> {
	let tracked_cfs = tracked_cfs.lock().await;
	let mut tracked_cfs_updates = Vec::with_capacity(tracked_cfs.len());
	for (selector, vs) in tracked_cfs.iter() {
		// Read the change feed for the selector
		let res = cf::read(
			&mut tx,
			&selector.ns,
			&selector.db,
			// Technically, we can not fetch by table and do the per-table filtering this side.
			// That is an improvement though
			Some(&selector.tb),
			ShowSince::versionstamp(vs),
			Some(catchup_size),
		)
		.await?;
		// Confirm we do need to change watermark - this is technically already handled by the cf range scan
		if res.is_empty() {
			trace!(
				"There were no changes in the change feed for {:?} from versionstamp {:?}",
				selector,
				conv::versionstamp_to_u64(vs)
			)
		}
		if let Some(change_set) = res.last() {
			if conv::versionstamp_to_u64(&change_set.0) > conv::versionstamp_to_u64(vs) {
				trace!("Adding a change set for lq notification processing");
				// Update the cf watermark so we can progress scans
				// If the notifications fail from here-on, they are lost
				// this is a separate vec that we later insert to because we are iterating immutably
				// We shouldn't use a read lock because of consistency between watermark scans
				tracked_cfs_updates.push((selector.clone(), change_set.0));
				// This does not guarantee a notification, as a changeset an include many tables and many changes
				change_map.insert(selector.clone(), res);
			}
		}
	}
	tx.cancel().await?;
	Ok(tracked_cfs_updates)
}
