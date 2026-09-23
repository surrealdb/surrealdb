use super::export;
use super::tr::Transactor;
use super::tx::Transaction;
use super::version::{Revision, Version};
use crate::ctx::MutableContext;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::capabilities::{
	ArbitraryQueryTarget, ExperimentalTarget, MethodTarget, RouteTarget,
};
use crate::dbs::node::Timestamp;
use crate::dbs::{
	Attach, Capabilities, Executor, Notification, Options, Response, Session, Variables,
};
use crate::err::Error;
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::iam::{Action, Auth, Error as IamError, Resource, Role};
use crate::idx::index::IndexOperation;
use crate::idx::trees::store::IndexStores;
use crate::key::root::ic::IndexCompactionKey;
use crate::key::root::rc::{ReclaimKey, ReclaimState};
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::clock::SizedClock;
#[allow(unused_imports)]
use crate::kvs::clock::SystemClock;
use crate::kvs::index::IndexBuilder;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::Key;
use crate::kvs::{LockType, LockType::*, TransactionType, TransactionType::*};
use crate::sql::{statements::DefineUserStatement, Base, Index, Query, Value};
use crate::syn;
use crate::syn::parser::{ParserSettings, StatementStream};
use crate::{cf, cnf};
use async_channel::{Receiver, Sender};
use bytes::{Bytes, BytesMut};
use futures::{Future, Stream};
use reblessive::TreeStack;
use std::collections::HashSet;
use std::fmt;
#[cfg(storage)]
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::{ready, Poll};
use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tracing::instrument;
use tracing::trace;
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

const TARGET: &str = "surrealdb::core::kvs::ds";

// If there are an infinite number of heartbeats, then we want to go batch-by-batch spread over several checks
const LQ_CHANNEL_SIZE: usize = 15_000;

// The role assigned to the initial user created when starting the server with credentials for the first time
const INITIAL_USER_ROLE: &str = "owner";

// The number of keys the reclaim task deletes from a prefix in one committed page.
// Every page is its own transaction, so this bounds the write set of each
// transaction the reclaim task opens against the data of a removed object.
const RECLAIM_BATCH_SIZE: u32 = 1_000;

// The number of data keys one reclaim pass destroys before it yields.
// A single entry can name an arbitrarily large prefix, so a pass is bounded by
// the work it does rather than by the clock: it ends with the cursor of every
// page it committed durable, and the next tick continues from there. Bounding it
// this way is also what keeps a pass short enough that a shutdown does not wait
// on one.
const RECLAIM_PASS_KEY_BUDGET: u64 = 100_000;

// How many entries one pass divides its key budget between.
// A pass reads as much of the queue as its entry budget allows, because
// stamping a first sighting costs one write per removed object and so scales
// with the catalog rather than with user data. The key budget is then spent on
// the entries that have waited longest, each granted its share of what is left,
// so that an entry naming an oversized prefix takes its share and yields
// instead of consuming every page of every pass. Dividing the budget between
// every candidate instead would drive each share towards a single key as the
// queue grows.
const RECLAIM_PASS_ENTRY_QUOTA: usize =
	(RECLAIM_PASS_KEY_BUDGET / RECLAIM_BATCH_SIZE as u64) as usize;

// The number of queue entries one reclaim pass reads.
// Every entry carries a uid of its own, so a removal committed while a pass is
// under way lands anywhere in the queue, including above the cursor of the walk
// already reading it. A workload removing objects at the rate a walk reads them
// would keep it going for as long as the removals continued, and the entries it
// had already found due would wait on it for that whole time because their data
// is destroyed only once the walk returns. This bound is far above any backlog a
// catalog holds, so in ordinary operation a pass still reads the queue whole.
const RECLAIM_PASS_QUEUE_ENTRY_BUDGET: u64 = 100_000;

// The number of index compaction queue entries a reclaim pass drops while it
// observes removals, and separately what one entry may drop while its data is
// being destroyed.
// The queue is a root level queue whose length tracks write traffic against count
// indexes rather than the size of the catalog, so it needs a bound of its own.
// One pass spends this across everything it does: the removals it observes, and
// the entries it then destroys data for. Granting each of those a cap of its own
// would let the number of removals waiting decide how much a pass deletes, which
// is what the bound is here to stop. Each entry's share is at least one page, so
// no entry is starved of the progress it needs to retire.
const RECLAIM_IC_PURGE_BUDGET: u64 = 10_000;

// The number of index compaction queue entries processed in one committed page.
// The queue's length tracks write traffic against count indexes rather than the
// size of the catalog, so neither the entries a pass holds nor the deletions it
// issues may be bounded by the queue itself.
const INDEX_COMPACTION_BATCH_SIZE: u32 = 1_000;

// The number of count entries folded into an index's aggregate in one committed
// page. Every page is a whole fold of what it takes, so this bounds the write
// set of one transaction without the total it arrives at depending on where the
// pages fall.
const INDEX_COMPACTION_DELTA_BATCH_SIZE: u32 = 1_000;

// The number of count entries one drain of an index may fold.
// An index that is being written to accepts new deltas while its old ones are
// folded, so a drain that ran to exhaustion could hold the queue on one index
// for as long as the writes continued: every later entry would wait on it, and
// the task lease would go unrenewed. A drain that spends this returns with the
// entry that asked for the compaction still in the queue, so the pass moves on
// to the indexes behind it and reaches this one again afterwards. It is also
// what stops one index taking the whole of the pass allowance below, so a pass
// always reaches several of the indexes that are due.
const INDEX_COMPACTION_DRAIN_ENTRY_BUDGET: u64 = 10_000;

// The number of count entries one pass folds across every index it meets.
// The per-index cap above bounds one drain; how many indexes a pass meets is
// set by the catalog and by how many of them are being written to, neither of
// which the task controls. Without this a pass on a store with thousands of
// count indexes would fold its way through all of them in one tick, holding a
// lease granted for twice the interval long past its expiry. What is left stays
// queued for the next tick.
const INDEX_COMPACTION_PASS_DELTA_BUDGET: u64 = 100_000;

// The number of compaction queue entries one pass reads.
// The queue is walked once per pass, but each entry carries a fresh uid, so an
// index still being written to keeps placing entries above the cursor of a walk
// already under way. Without this the walk would end only when that traffic
// stopped, and the task awaits it before it can see a shutdown. What is left
// stays queued for the next tick.
const INDEX_COMPACTION_PASS_ENTRY_BUDGET: u64 = 100_000;

// The number of reclaim queue entries read in one transaction.
// The queue is walked in pages so that a large backlog is neither held whole in
// memory nor read inside a single long-lived transaction.
const RECLAIM_QUEUE_PAGE_SIZE: u32 = 100;

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
#[non_exhaustive]
pub struct Datastore {
	transaction_factory: TransactionFactory,
	/// The unique id of this datastore, used in notifications.
	id: Uuid,
	/// Whether this datastore runs in strict mode by default.
	strict: bool,
	/// Whether authentication is enabled on this datastore.
	auth_enabled: bool,
	/// The maximum duration timeout for running multiple statements in a query.
	query_timeout: Option<Duration>,
	/// The slow log configuration determining when a query should be logged
	slow_log: Option<SlowLog>,
	/// The maximum duration timeout for running multiple statements in a transaction.
	transaction_timeout: Option<Duration>,
	/// The security and feature capabilities for this datastore.
	capabilities: Arc<Capabilities>,
	// Whether this datastore enables live query notifications to subscribers.
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
	// The index store cache
	index_stores: IndexStores,
	// The cross transaction cache
	cache: Arc<DatastoreCache>,
	/// Where the next index compaction pass resumes its walk over the queue
	index_compaction_cursor: Arc<StdMutex<Option<Key>>>,
	/// Where the next reclaim pass resumes its walk over the queue
	reclaim_cursor: Arc<StdMutex<Option<Key>>>,
	// The index asynchronous builder
	index_builder: IndexBuilder,
	#[cfg(feature = "jwks")]
	// The JWKS object cache
	jwks_cache: Arc<RwLock<JwksCache>>,
	#[cfg(storage)]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
}

#[derive(Clone)]
pub(super) struct TransactionFactory {
	// Clock for tracking time. It is read only and accessible to all transactions. It is behind a mutex as tests may write to it.
	clock: Arc<SizedClock>,
	// The inner datastore type
	flavor: Arc<DatastoreFlavor>,
}

/// Represents a collection of metrics for a specific datastore flavor.
pub struct Metrics {
	/// The name of the metrics group (e.g., "surrealdb.rocksdb").
	pub name: &'static str,
	/// A list of u64-based metrics.
	pub u64_metrics: Vec<Metric>,
}

/// Represents a single metric with a name and description.
pub struct Metric {
	/// The name of the metric.
	pub name: &'static str,
	/// A human-readable description of the metric.
	pub description: &'static str,
}

impl TransactionFactory {
	pub(super) fn new(clock: Arc<SizedClock>, flavor: DatastoreFlavor) -> Self {
		Self {
			clock,
			flavor: Arc::new(flavor),
		}
	}

	#[allow(unreachable_code)]
	pub async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
	) -> Result<Transaction, Error> {
		// Specify if the transaction is writeable
		#[allow(unused_variables)]
		let write = match write {
			Read => false,
			Write => true,
		};
		// Specify if the transaction is lockable
		#[allow(unused_variables)]
		let lock = match lock {
			Pessimistic => true,
			Optimistic => false,
		};
		// Create a new transaction on the datastore
		#[allow(unused_variables)]
		let (inner, local) = match self.flavor.as_ref() {
			#[cfg(feature = "kv-mem")]
			DatastoreFlavor::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::Mem(tx), true)
			}
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::RocksDB(tx), true)
			}
			#[cfg(feature = "kv-indxdb")]
			DatastoreFlavor::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::IndxDB(tx), true)
			}
			#[cfg(feature = "kv-tikv")]
			DatastoreFlavor::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::TiKV(tx), false)
			}
			#[cfg(feature = "kv-fdb")]
			DatastoreFlavor::FoundationDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::FoundationDB(tx), false)
			}
			#[cfg(feature = "kv-surrealkv")]
			DatastoreFlavor::SurrealKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::SurrealKV(tx), true)
			}
			#[cfg(feature = "kv-surrealcs")]
			DatastoreFlavor::SurrealCS(v) => {
				let tx = v.transaction(write, lock).await?;
				(super::tr::Inner::SurrealCS(tx), false)
			}
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		};
		Ok(Transaction::new(
			local,
			write,
			Transactor {
				inner,
				stash: super::stash::Stash::default(),
				cf: cf::Writer::new(),
				clock: self.clock.clone(),
			},
		))
	}
}

#[allow(clippy::large_enum_variant)]
pub(super) enum DatastoreFlavor {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
	#[cfg(feature = "kv-fdb")]
	FoundationDB(super::fdb::Datastore),
	#[cfg(feature = "kv-surrealkv")]
	SurrealKV(super::surrealkv::Datastore),
	#[cfg(feature = "kv-surrealcs")]
	SurrealCS(super::surrealcs::Datastore),
}

impl fmt::Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match self.transaction_factory.flavor.as_ref() {
			#[cfg(feature = "kv-mem")]
			DatastoreFlavor::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-indxdb")]
			DatastoreFlavor::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			DatastoreFlavor::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-fdb")]
			DatastoreFlavor::FoundationDB(_) => write!(f, "fdb"),
			#[cfg(feature = "kv-surrealkv")]
			DatastoreFlavor::SurrealKV(_) => write!(f, "surrealkv"),
			#[cfg(feature = "kv-surrealcs")]
			DatastoreFlavor::SurrealCS(_) => write!(f, "surrealcs"),
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
	/// let ds = Datastore::new("surrealkv://temp.skv").await?;
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
	pub async fn new(path: &str) -> Result<Self, Error> {
		Self::new_with_clock(path, None).await
	}

	#[allow(unused_variables)]
	pub async fn new_with_clock(
		path: &str,
		clock: Option<Arc<SizedClock>>,
	) -> Result<Datastore, Error> {
		// Initiate the desired datastore
		let (flavor, clock): (Result<DatastoreFlavor, Error>, Arc<SizedClock>) = match path {
			// Initiate an in-memory datastore
			"memory" => {
				#[cfg(feature = "kv-mem")]
				{
					// Innitialise the storage engine
					info!(target: TARGET, "Starting kvs store in {}", path);
					let v = super::mem::Datastore::new().await.map(DatastoreFlavor::Mem);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store in {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-mem"))]
                return Err(Error::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a File datastore
			s if s.starts_with("file:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Innitialise the storage engine
					info!(target: TARGET, "Starting kvs store at {}", path);
					warn!("file:// is deprecated, please use surrealkv:// or rocksdb://");
					let s = s.trim_start_matches("file://");
					let s = s.trim_start_matches("file:");
					let v = super::rocksdb::Datastore::new(s).await.map(DatastoreFlavor::RocksDB);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a RocksDB datastore
			s if s.starts_with("rocksdb:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Innitialise the storage engine
					info!(target: TARGET, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("rocksdb://");
					let s = s.trim_start_matches("rocksdb:");
					let v = super::rocksdb::Datastore::new(s).await.map(DatastoreFlavor::RocksDB);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
                return Err(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a SurrealKV datastore
			s if s.starts_with("surrealkv") => {
				#[cfg(feature = "kv-surrealkv")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Innitialise the storage engine
					info!(target: TARGET, "Starting kvs store at {}", s);
					let (path, enable_versions) =
						super::surrealkv::Datastore::parse_start_string(s)?;
					let v = super::surrealkv::Datastore::new(path, enable_versions)
						.await
						.map(DatastoreFlavor::SurrealKV);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {} with versions {}", path, if enable_versions { "enabled" } else { "disabled" });
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-surrealkv"))]
                return Err(Error::Ds("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a SurrealCS datastore
			s if s.starts_with("surrealcs:") => {
				#[cfg(feature = "kv-surrealcs")]
				{
					info!(target: TARGET, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("surrealcs://");
					let s = s.trim_start_matches("surrealcs:");
					let v =
						super::surrealcs::Datastore::new(s).await.map(DatastoreFlavor::SurrealCS);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-surrealcs"))]
				return Err(Error::Ds("Cannot connect to the `surrealcs` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate an IndxDB database
			s if s.starts_with("indxdb:") => {
				#[cfg(feature = "kv-indxdb")]
				{
					info!(target: TARGET, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("indxdb://");
					let s = s.trim_start_matches("indxdb:");
					let v = super::indxdb::Datastore::new(s).await.map(DatastoreFlavor::IndxDB);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-indxdb"))]
                return Err(Error::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a TiKV datastore
			s if s.starts_with("tikv:") => {
				#[cfg(feature = "kv-tikv")]
				{
					info!(target: TARGET, "Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("tikv://");
					let s = s.trim_start_matches("tikv:");
					let v = super::tikv::Datastore::new(s).await.map(DatastoreFlavor::TiKV);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Connected to kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-tikv"))]
                return Err(Error::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a FoundationDB datastore
			s if s.starts_with("fdb:") => {
				#[cfg(feature = "kv-fdb")]
				{
					info!(target: TARGET, "Connecting to kvs store at {}", path);
					let s = s.trim_start_matches("fdb://");
					let s = s.trim_start_matches("fdb:");
					let v = super::fdb::Datastore::new(s).await.map(DatastoreFlavor::FoundationDB);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Connected to kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-fdb"))]
                return Err(Error::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// The datastore path is not valid
			_ => {
				info!(target: TARGET, "Unable to load the specified datastore {}", path);
				Err(Error::Ds("Unable to load the specified datastore".into()))
			}
		}?;
		// Set the properties on the datastore
		flavor.map(|flavor| {
			let tf = TransactionFactory::new(clock, flavor);
			Self {
				id: Uuid::new_v4(),
				transaction_factory: tf.clone(),
				strict: false,
				auth_enabled: false,
				query_timeout: None,
				slow_log: None,
				transaction_timeout: None,
				notification_channel: None,
				capabilities: Arc::new(Capabilities::default()),
				index_stores: IndexStores::default(),
				index_builder: IndexBuilder::new(tf),
				#[cfg(feature = "jwks")]
				jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
				#[cfg(storage)]
				temporary_directory: None,
				cache: Arc::new(DatastoreCache::new()),
				index_compaction_cursor: Arc::new(StdMutex::new(None)),
				reclaim_cursor: Arc::new(StdMutex::new(None)),
			}
		})
	}

	/// Registers metrics for the current datastore flavor if supported.
	pub fn register_metrics(&self) -> Option<Metrics> {
		match self.transaction_factory.flavor.as_ref() {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => Some(v.register_metrics()),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	pub fn collect_u64_metric(&self, _metric: &str) -> Option<u64> {
		match self.transaction_factory.flavor.as_ref() {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => v.collect_u64_metric(_metric),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}

	/// Create a new datastore with the same persistent data (inner), with flushed cache.
	/// Simulating a server restart
	#[allow(dead_code)]
	pub fn restart(self) -> Self {
		Self {
			id: self.id,
			strict: self.strict,
			auth_enabled: self.auth_enabled,
			query_timeout: self.query_timeout,
			slow_log: self.slow_log.clone(),
			transaction_timeout: self.transaction_timeout,
			capabilities: self.capabilities,
			notification_channel: self.notification_channel,
			index_stores: Default::default(),
			index_builder: IndexBuilder::new(self.transaction_factory.clone()),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(Default::default()),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			transaction_factory: self.transaction_factory,
			cache: Arc::new(DatastoreCache::new()),
			index_compaction_cursor: Arc::new(StdMutex::new(None)),
			reclaim_cursor: Arc::new(StdMutex::new(None)),
		}
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
		self.notification_channel = Some(async_channel::bounded(LQ_CHANNEL_SIZE));
		self
	}

	/// Set a global query timeout for this Datastore
	pub fn with_query_timeout(mut self, duration: Option<Duration>) -> Self {
		self.query_timeout = duration;
		self
	}

	/// Set a global slow log configuration
	///
	/// Parameters:
	/// - `duration`: Minimum execution time for a statement to be considered "slow". When `None`,
	///   slow logging is disabled.
	/// - `param_allow`: If non-empty, only parameters with names present in this list will be
	///   logged when a query is slow.
	/// - `param_deny`: Parameter names that should never be logged. This list always takes
	///   precedence over `param_allow`.
	pub fn with_slow_log(
		mut self,
		duration: Option<Duration>,
		param_allow: Vec<String>,
		param_deny: Vec<String>,
	) -> Self {
		self.slow_log = duration.map(|d| SlowLog::new(d, param_allow, param_deny));
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

	/// Set specific capabilities for this Datastore
	pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
		self.capabilities = Arc::new(caps);
		self
	}

	#[cfg(storage)]
	/// Set a temporary directory for ordering of large result sets
	pub fn with_temporary_directory(mut self, path: Option<PathBuf>) -> Self {
		self.temporary_directory = path.map(Arc::new);
		self
	}

	pub fn index_store(&self) -> &IndexStores {
		&self.index_stores
	}

	/// Is authentication enabled for this Datastore?
	pub fn is_auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	pub fn id(&self) -> Uuid {
		self.id
	}

	/// Does the datastore allow excecuting an RPC method?
	pub(crate) fn allows_rpc_method(&self, method_target: &MethodTarget) -> bool {
		self.capabilities.allows_rpc_method(method_target)
	}

	/// Does the datastore allow requesting an HTTP route?
	/// This function needs to be public to allow access from the CLI crate.
	pub fn allows_http_route(&self, route_target: &RouteTarget) -> bool {
		self.capabilities.allows_http_route(route_target)
	}

	/// Is the user allowed to query?
	pub fn allows_query_by_subject(&self, subject: impl Into<ArbitraryQueryTarget>) -> bool {
		self.capabilities.allows_query(&subject.into())
	}

	/// Does the datastore allow connections to a network target?
	#[cfg(feature = "jwks")]
	pub(crate) fn allows_network_target(&self, net_target: &NetTarget) -> bool {
		self.capabilities.allows_network_target(net_target)
	}

	/// Set specific capabilities for this Datastore
	pub fn get_capabilities(&self) -> &Capabilities {
		&self.capabilities
	}

	#[cfg(feature = "jwks")]
	pub(crate) fn jwks_cache(&self) -> &Arc<RwLock<JwksCache>> {
		&self.jwks_cache
	}

	pub(super) async fn clock_now(&self) -> Timestamp {
		self.transaction_factory.clock.now().await
	}

	// Used for testing live queries
	#[allow(dead_code)]
	pub fn get_cache(&self) -> Arc<DatastoreCache> {
		self.cache.clone()
	}

	/// Checks that this build can read the datastore's storage format.
	///
	/// Two gates, in order. The major version at `!v` must be this build's own:
	/// a datastore below it holds a layout this build cannot read at all, and is
	/// brought forward by `surreal fix`. The revision at `!vr` must then be one
	/// this build knows: a datastore above it holds a layout this build reads
	/// *incorrectly*, which is why the refusal belongs at startup rather than at
	/// the first statement that meets the changed data.
	///
	/// The revision gate only binds builds that ship it. A release that predates
	/// it ignores `!vr` and opens the datastore regardless, so a revision has to
	/// be readable by every build a deployment may roll back to before anything
	/// stamps it.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn check_version(&self) -> Result<Version, Error> {
		let version = self.get_version().await?;
		// Check we are running the latest version
		if !version.is_latest() {
			return Err(Error::OutdatedStorageVersion);
		}
		// Check no later release has changed the layout out from under us
		let revision = self.get_revision().await?;
		if !revision.is_known() {
			return Err(Error::NewerStorageRevision {
				stored: revision.into(),
				known: Revision::KNOWN,
			});
		}
		// Everything ok
		Ok(version)
	}

	/// Reads the datastore's storage revision, which is zero until stamped.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn get_revision(&self) -> Result<Revision, Error> {
		// A read is enough: this build never stamps a revision, and a datastore
		// without one is at zero by definition rather than by omission
		let txn = self.transaction(Read, Optimistic).await?;
		let res = txn.get(crate::key::version::revision(), None).await;
		let val = catch!(txn, res);
		txn.cancel().await?;
		match val {
			Some(v) => Revision::try_from(v),
			None => Ok(Revision::default()),
		}
	}

	// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn get_version(&self) -> Result<Version, Error> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Pessimistic).await?.enclose();
		// Create the key where the version is stored
		let key = crate::key::version::new();
		// Check if a version is already set in storage
		let val = match catch!(txn, txn.get(key.clone(), None).await) {
			// There is a version set in the storage
			Some(v) => {
				// Attempt to decode the current stored version
				let val = TryInto::<Version>::try_into(v);
				// Check for errors, and cancel the transaction
				match val {
					// There was en error getting the version
					Err(err) => {
						// We didn't write anything, so just rollback
						catch!(txn, txn.cancel().await);
						// Return the error
						return Err(err);
					}
					// We could decode the version correctly
					Ok(val) => {
						// We didn't write anything, so just rollback
						catch!(txn, txn.cancel().await);
						// Return the current version
						val
					}
				}
			}
			// There is no version set in the storage
			None => {
				// Fetch any keys immediately following the version key
				let rng = crate::key::version::proceeding();
				let keys = catch!(txn, txn.keys(rng, 1, None).await);
				// Check the storage if there are any other keys set
				let val = if keys.is_empty() {
					// There are no keys set in storage, so this is a new database
					Version::latest()
				} else {
					// There were keys in storage, so this is an upgrade
					Version::v1()
				};
				// Convert the version to binary
				let bytes: Vec<u8> = val.into();
				// Attempt to set the current version in storage
				catch!(txn, txn.replace(key, bytes).await);
				// We set the version, so commit the transaction
				catch!(txn, txn.commit().await);
				// Return the current version
				val
			}
		};
		// Everything ok
		Ok(val)
	}

	/// Setup the initial cluster access credentials
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn initialise_credentials(&self, user: &str, pass: &str) -> Result<(), Error> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Optimistic).await?.enclose();
		// Fetch the root users from the storage
		let users = catch!(txn, txn.all_root_users().await);
		// Process credentials, depending on existing users
		if users.is_empty() {
			// Display information in the logs
			info!(target: TARGET, "Credentials were provided, and no root users were found. The root user '{user}' will be created");
			// Create and new root user definition
			let stm = DefineUserStatement::from((Base::Root, user, pass, INITIAL_USER_ROLE));
			let opt = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
			let mut ctx = MutableContext::default();
			ctx.set_transaction(txn.clone());
			let ctx = ctx.freeze();
			catch!(txn, stm.compute(&ctx, &opt, None).await);
			// We added a user, so commit the transaction
			txn.commit().await
		} else {
			// Display information in the logs
			warn!(target: TARGET, "Credentials were provided, but existing root users were found. The root user '{user}' will not be created");
			warn!(target: TARGET, "Consider removing the --user and --pass arguments from the server start command");
			// We didn't write anything, so just rollback
			txn.cancel().await
		}
	}

	/// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn bootstrap(&self) -> Result<(), Error> {
		// Insert this node in the cluster
		self.insert_node(self.id).await?;
		// Mark inactive nodes as archived
		self.expire_nodes().await?;
		// Remove archived nodes
		self.remove_nodes().await?;
		// Restart deferred indexes
		self.restart_deferred_indexes().await?;
		// Everything ok
		Ok(())
	}

	/// Restart deferred indexes after database startup
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn restart_deferred_indexes(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Restarting deferred indexes");
		let tx = Arc::new(self.transaction(Read, Optimistic).await?);
		let mut ctx = self.setup_ctx()?;
		ctx.set_transaction(tx.clone());
		let ctx = ctx.freeze();
		let sess = Session::owner();
		for ns in tx.all_ns().await?.iter() {
			for db in tx.all_db(&ns.name).await?.iter() {
				let opt = self
					.setup_options(&sess)
					.with_ns(Some(Arc::from(ns.name.as_str())))
					.with_db(Some(Arc::from(db.name.as_str())));
				for tb in tx.all_tb(&ns.name, &db.name, None).await?.iter() {
					for ix in tx.all_tb_indexes(&ns.name, &db.name, &tb.name).await?.iter() {
						self.index_builder.restart_deferred_index(&ctx, &opt, &tb.name, ix).await?;
					}
				}
			}
		}
		// Everything ok
		Ok(())
	}

	/// Run the background task to update node registration information
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_update(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Updating node registration information");
		// Update this node in the cluster
		self.update_node(self.id).await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to process and archive inactive nodes
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_expire(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Processing and archiving inactive nodes");
		// Mark expired nodes as archived
		self.expire_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to process and cleanup archived nodes
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_remove(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Processing and cleaning archived nodes");
		// Cleanup expired nodes data
		self.remove_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to perform changefeed garbage collection
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn changefeed_process(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		// Calculate the current system time
		let ts = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|e| {
				Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration()))
			})?
			.as_secs();
		// Save timestamps for current versionstamps
		self.changefeed_versionstamp(ts).await?;
		// Garbage old changefeed data from all databases
		self.changefeed_cleanup(ts).await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to perform changefeed garbage collection
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn changefeed_process_at(&self, ts: u64) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		// Save timestamps for current versionstamps
		self.changefeed_versionstamp(ts).await?;
		// Garbage old changefeed data from all databases
		self.changefeed_cleanup(ts).await?;
		// Everything ok
		Ok(())
	}

	/// Processes the index compaction queue
	///
	/// This method is called periodically by the index compaction thread to
	/// process indexes that have been marked for compaction. It acquires a
	/// distributed lease to ensure only one node in a cluster performs the
	/// compaction at a time.
	///
	/// The method scans the index compaction queue (stored as `Ic` keys) and
	/// processes each index that needs compaction. Currently, only full-text
	/// indexes support compaction, which helps optimize their performance by
	/// consolidating changes and removing unnecessary data.
	///
	/// After processing an index, it is removed from the compaction queue.
	///
	/// # Arguments
	///
	/// * `interval` - The time interval between compaction runs, used to calculate the lease
	///   duration
	///
	/// # Returns
	///
	/// * `Result<()>` - Ok if the compaction was successful or if another node is handling the
	///   compaction, Error otherwise
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn index_compaction(&self, interval: Duration) -> Result<(), Error> {
		self.index_compaction_bounded(interval, INDEX_COMPACTION_DRAIN_ENTRY_BUDGET).await
	}

	/// [`Self::index_compaction`] with the entry budget of one index's drain
	/// given explicitly, so that a test can reach the cap without first writing
	/// [`INDEX_COMPACTION_DRAIN_ENTRY_BUDGET`] entries.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn index_compaction_bounded(
		&self,
		interval: Duration,
		drain: u64,
	) -> Result<(), Error> {
		self.index_compaction_within(
			interval,
			drain,
			INDEX_COMPACTION_PASS_ENTRY_BUDGET,
			INDEX_COMPACTION_PASS_DELTA_BUDGET,
		)
		.await
	}

	/// [`Self::index_compaction_bounded`] with the number of queue entries one
	/// pass reads and the entries it folds across every index given explicitly,
	/// so that a test can reach either cap without first writing
	/// [`INDEX_COMPACTION_PASS_ENTRY_BUDGET`] entries.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub(crate) async fn index_compaction_within(
		&self,
		interval: Duration,
		drain: u64,
		mut pass: u64,
		mut folds: u64,
	) -> Result<(), Error> {
		let lh = LeaseHandler::new(
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			interval.saturating_mul(2),
		)?;
		// Process all items in the queue. Once processing starts, we intentionally continue
		// to completion even if the lease is lost mid-process. This prevents leaving work
		// in an inconsistent state (see try_maintain_lease documentation for rationale).
		// The indexes whose drain has already spent its budget in this call. An
		// index with more queue entries than one page holds is met again on the
		// next page, and starting a fresh drain for it there would let one index
		// fold without limit and hold back every index behind it. Its entries
		// stay queued, so the next call reaches it again.
		let mut exhausted: HashSet<(String, String, String, String)> = HashSet::new();
		// Attempt to acquire a lease for the IndexCompaction task
		// If we don't get the lease, another node is handling this task
		if !lh.has_lease().await? {
			return Ok(());
		}
		// The queue's length tracks write traffic against count indexes
		// rather than the size of the catalog, so it is walked in committed
		// pages: neither the entries nor the deletions of a whole backlog
		// belong in one transaction.
		//
		// The queue is walked once. Entries written while it runs are left for
		// the next pass rather than met by walking it again: an index still
		// being written to produces them for as long as the writes continue, so
		// a call that kept going would only return when the traffic stopped,
		// and the task awaits this call before it can see a shutdown.
		//
		// A pass that stops short resumes where it stopped rather than at the
		// beginning. The entries of one index are contiguous, and an index whose
		// drain is exhausted keeps all of them, so a queue longer than one pass
		// may read would otherwise be the same entries every time and the indexes
		// behind them would never be reached at all. The resume point is this
		// node's: the lease means one node runs this, and a lease that moves costs
		// one sweep from the beginning.
		let (beg, end) = IndexCompactionKey::range();
		let mut cursor: Option<Key> = self.index_compaction_cursor.lock().unwrap().clone();
		// Where the next pass starts. A walk that reaches the end of the queue
		// leaves nothing behind it, so the next one starts again at the beginning.
		let mut resume: Option<Key> = None;
		loop {
			// A queue still being written to has no end this could reach, so
			// what is left belongs to the next tick
			if pass == 0 {
				trace!(target: TARGET, "Index compaction stopped: it read what one pass may read");
				resume = cursor.clone();
				break;
			}
			// Read one bounded page of the queue, starting strictly after
			// the last entry the page before it dropped
			let txn = self.transaction(Read, Optimistic).await?;
			let window = match &cursor {
				Some(k) => k.clone()..end.clone(),
				None => beg.clone()..end.clone(),
			};
			let limit = pass.min(INDEX_COMPACTION_BATCH_SIZE as u64) as u32;
			let res = txn.keys(window, limit, None).await;
			let page = catch!(txn, res);
			txn.cancel().await?;
			// The whole queue has been walked
			if page.is_empty() {
				break;
			}
			pass = pass.saturating_sub(page.len() as u64);
			// Resume strictly after the last entry of this page
			let mut next = page[page.len() - 1].clone();
			next.push(0x00);
			// One compaction folds every delta of its index that was
			// committed before this page was read, so the entries of an
			// index already compacted in this pass name nothing more. That
			// reasoning is confined to this page: an entry read by a later
			// one names deltas written after this compaction, and skipping
			// it would drop them and delete the entry that would have
			// brought them back.
			let mut previous: Option<(IndexCompactionKey<'static>, bool)> = None;
			// The entries this page is allowed to drop: an index that could
			// not be compacted, or that has more entries than one pass may
			// spend, keeps the entry that asked for it, so the next pass
			// asks again rather than leaving the deltas with nothing to
			// fold them.
			let mut done: Vec<Key> = Vec::with_capacity(page.len());
			// The entry this page stopped at, if it did not finish
			let mut stopped: Option<Key> = None;
			for k in page.iter() {
				// Lease maintenance: try_maintain_lease() returns a boolean indicating lease
				// ownership, but we intentionally ignore it and continue processing. This ensures
				// that work completes once started, preventing inconsistent state.
				let _ = lh.try_maintain_lease().await?;
				let ic = IndexCompactionKey::decode_key(k)?;
				// The entries of one index are contiguous, so the index met on
				// the entry before this one answers for most of them
				if let Some((p, compacted)) = &previous {
					if p.index_matches(&ic) {
						if *compacted {
							done.push(k.clone());
						}
						continue;
					}
				}
				// An index which has already spent a drain keeps its entries
				// for the next call rather than starting another here
				let name = Self::compaction_index_name(&ic);
				if exhausted.contains(&name) {
					previous = Some((ic.into_owned(), false));
					continue;
				}
				// A pass folds what its allowance leaves, and no index takes
				// more of it than one drain. What a pass cannot begin a page of
				// is left queued for the next one.
				let share = folds.min(drain);
				if share < INDEX_COMPACTION_DELTA_BATCH_SIZE as u64 {
					trace!(target: TARGET, "Index compaction stopped: it folded what one pass may fold");
					// This entry has not been accounted for, so the next pass starts
					// at it rather than past the page holding it
					stopped = Some(k.clone());
					pass = 0;
					break;
				}
				let mut spend = share;
				let compacted = match self.compact_index(&lh, &ic, &mut spend).await {
					Ok(true) => true,
					Ok(false) => {
						exhausted.insert(name);
						false
					}
					Err(e) => {
						warn!(target: TARGET, "Index compaction error: {e} - Index {:?}", ic.ix);
						false
					}
				};
				folds = folds.saturating_sub(share - spend);
				if compacted {
					done.push(k.clone());
				}
				previous = Some((ic.into_owned(), compacted));
			}
			// Drop exactly the entries this page accounted for, after the
			// compaction they triggered is durable: compacting twice folds
			// the same deltas to the same total, while dropping an entry
			// whose deltas were never folded loses them for good. Every
			// entry carries a fresh uid, so no later one ever writes over
			// these: tombstoning them would keep the whole queue on a
			// backend that retains versions, which is the growth this
			// drains.
			let txn = self.transaction(Write, Optimistic).await?;
			for k in done {
				catch!(txn, txn.clr(k).await);
			}
			catch!(txn, txn.commit().await);
			cursor = Some(stopped.unwrap_or(next));
			yield_now!();
		}
		*self.index_compaction_cursor.lock().unwrap() = resume;
		Ok(())
	}

	/// The four names that identify the index a compaction entry asks for.
	///
	/// A queue entry also carries the node and a fresh uid, so two entries for one
	/// index are distinct keys; this is what they have in common.
	fn compaction_index_name(ic: &IndexCompactionKey<'_>) -> (String, String, String, String) {
		(ic.ns.to_string(), ic.db.to_string(), ic.tb.to_string(), ic.ix.to_string())
	}

	/// Compacts one index the queue named, in committed pages of at most
	/// [`INDEX_COMPACTION_DELTA_BATCH_SIZE`] entries, spending at most
	/// [`INDEX_COMPACTION_DRAIN_ENTRY_BUDGET`] of them. Returns whether the index
	/// is now folded down to its aggregate.
	///
	/// How many count entries an index has accumulated is set by the write
	/// traffic against it, so they are folded in transactions of this method's
	/// own rather than in the one that read the queue: a store which has never
	/// compacted meets this with everything it has ever written, and one still
	/// being written to accepts more while these pages run. Neither may decide
	/// how long a pass holds the queue, so the budget bounds it and the caller
	/// keeps the entry that asked for the compaction when it is spent.
	///
	/// Each page re-reads the catalog in the transaction that writes, because
	/// compaction writes count entries back into the database prefix they belong
	/// to, and a name that has been removed is a prefix the reclaim is
	/// destroying. Every page is a whole fold of what it takes, so stopping
	/// between two of them leaves the count correct.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self, lh))]
	async fn compact_index(
		&self,
		lh: &LeaseHandler,
		ic: &IndexCompactionKey<'_>,
		budget: &mut u64,
	) -> Result<bool, Error> {
		loop {
			// A page beyond the budget belongs to the next pass, which reaches
			// it through the entry the caller is about to keep
			if *budget < INDEX_COMPACTION_DELTA_BATCH_SIZE as u64 {
				return Ok(false);
			}
			*budget -= INDEX_COMPACTION_DELTA_BATCH_SIZE as u64;
			// Lease maintenance: try_maintain_lease() returns a boolean indicating lease
			// ownership, but we intentionally ignore it and continue processing. This ensures
			// that work completes once started, preventing inconsistent state.
			let _ = lh.try_maintain_lease().await?;
			let txn = self.transaction(Write, Optimistic).await?;
			let res = txn.db_exists(&ic.ns, &ic.db).await;
			if !catch!(txn, res) {
				txn.cancel().await?;
				trace!(target: TARGET, "Index compaction: Database {:?}/{:?} has been removed, skipping", &ic.ns, &ic.db);
				return Ok(true);
			}
			// An index the catalog no longer holds has nothing to fold, and
			// nothing will write to it again. Any other failure to read the
			// definition is a failure to read the catalog rather than an answer
			// about it, and is propagated so that the entry which named this
			// index outlives it: reporting the index drained would drop the
			// entry, and a quiescent index would then keep its deltas with
			// nothing left to ask for them to be folded.
			let drained = match txn.get_tb_index(&ic.ns, &ic.db, &ic.tb, &ic.ix).await {
				Ok(ix) => match &ix.index {
					Index::Count => {
						let res = IndexOperation::index_count_compaction(
							ic,
							&txn,
							INDEX_COMPACTION_DELTA_BATCH_SIZE,
						)
						.await;
						catch!(txn, res)
					}
					_ => {
						trace!(target: TARGET, "Index compaction: Index {:?} does not support compaction, skipping", &ic.ix);
						true
					}
				},
				Err(Error::IxNotFound {
					..
				}) => {
					trace!(target: TARGET, "Index compaction: Index {:?} has been removed, skipping", &ic.ix);
					true
				}
				Err(e) => {
					txn.cancel().await?;
					return Err(e);
				}
			};
			catch!(txn, txn.commit().await);
			if drained {
				return Ok(true);
			}
			yield_now!();
		}
	}

	/// Drains the reclaim queue, destroying the data left behind by removed
	/// namespaces and databases.
	///
	/// `REMOVE NAMESPACE` and `REMOVE DATABASE` delete only the catalog
	/// definition and enqueue a job naming the orphaned key prefix, so the size
	/// of a removal's transaction is independent of how much data the removed
	/// object held. This task destroys those prefixes out of band.
	///
	/// A job is never destroyed on the pass that first sees it. That pass stamps
	/// it with the current wall clock and commits, and a later one destroys its
	/// prefix. The stamp is what orders the queue: a pass spends its budget on the
	/// entries that have waited longest, so a prefix too large for one pass cannot
	/// take every pass from the ones behind it. It is taken here rather than by
	/// the removing statement because a statement reads its clock before it
	/// commits, and that commit can be delayed arbitrarily or lost.
	///
	/// Each page of deletions commits together with the cursor that accounts
	/// for it, so an interrupted pass resumes at the last durably deleted key
	/// instead of rescanning the prefix from its start.
	///
	/// A pass destroys keys in pages of at most `RECLAIM_BATCH_SIZE`, which is
	/// the bound on the write set of every transaction it opens against the data
	/// of a removed object, and spends at most `RECLAIM_PASS_KEY_BUDGET` keys in
	/// total. That budget goes to the entries which have waited longest, at most
	/// `RECLAIM_PASS_ENTRY_QUOTA` of them, each granted its share of what is
	/// left. A prefix too large for one pass therefore takes its share and
	/// yields, and drains over as many passes as it takes without holding up the
	/// entries behind it.
	///
	/// Stamping a job with its first observation is not paced that way: it costs
	/// one write per removed object, so it scales with the catalog rather than
	/// with user data, and a pass that spends its whole budget on one large
	/// prefix must still stamp every other job it can see. What an
	/// observation drops from the index compaction queue is not that cheap and
	/// does not scale with the catalog, so the whole pass shares one allowance
	/// of `RECLAIM_IC_PURGE_BUDGET` for it. A queue that outlasts the allowance
	/// is finished by the passes that destroy the data, which do not retire a
	/// job until its entries are gone.
	///
	/// # Arguments
	///
	/// * `interval` - The time interval between reclaim passes, which sets the lease duration
	///
	/// # Returns
	///
	/// * The number of queue entries visited, and how many of those failed. A failing entry is
	///   logged and skipped so that one unreadable or contended job cannot stall the rest of the
	///   queue.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn reclaim_tombstones(&self, interval: Duration) -> Result<(u64, u64), Error> {
		let (visited, errors, _) = self
			.reclaim_pass(
				interval,
				RECLAIM_BATCH_SIZE,
				RECLAIM_PASS_KEY_BUDGET,
				RECLAIM_IC_PURGE_BUDGET,
				RECLAIM_PASS_QUEUE_ENTRY_BUDGET,
			)
			.await?;
		Ok((visited, errors))
	}

	/// Runs one reclaim pass with its bounds given explicitly, and additionally
	/// reports how many keys the pass destroyed.
	///
	/// [`Self::reclaim_tombstones`] is this pass with the production bounds.
	/// The bounds are parameters here so that a test can interrupt a prefix
	/// part-way through at an exact key.
	///
	/// # Arguments
	///
	/// * `batch` - The number of keys one committed page may destroy
	/// * `budget` - The number of keys the whole pass may destroy
	#[cfg(test)]
	pub(crate) async fn reclaim_tombstones_bounded(
		&self,
		interval: Duration,
		batch: u32,
		budget: u64,
	) -> Result<(u64, u64, u64), Error> {
		self.reclaim_pass(
			interval,
			batch,
			budget,
			RECLAIM_IC_PURGE_BUDGET,
			RECLAIM_PASS_QUEUE_ENTRY_BUDGET,
		)
		.await
	}

	/// [`Self::reclaim_tombstones_bounded`] with the cap on one purge of the
	/// index compaction queue and the number of queue entries one pass reads
	/// given too, so that a test can reach either without first queueing
	/// [`RECLAIM_IC_PURGE_BUDGET`] entries or removing
	/// [`RECLAIM_PASS_QUEUE_ENTRY_BUDGET`] objects.
	#[cfg(test)]
	pub(crate) async fn reclaim_tombstones_within(
		&self,
		interval: Duration,
		batch: u32,
		budget: u64,
		ic_budget: u64,
		queue: u64,
	) -> Result<(u64, u64, u64), Error> {
		self.reclaim_pass(interval, batch, budget, ic_budget, queue).await
	}

	/// One pass over the reclaim queue, destroying at most `batch` keys per
	/// committed page and `budget` keys in total.
	///
	/// The queue is walked in bounded pages, for at most `queue` entries: an
	/// unstamped entry is stamped as it is met, and an entry already stamped is
	/// set aside as a candidate. What the walk does not reach is left for the next
	/// pass, which resumes where this one stopped.
	/// The key budget is then spent on the longest-waiting candidates
	/// rather than on whichever entry sorts lowest, because the queue is keyed by
	/// kind and name and key order would otherwise make the entries that happen
	/// to sort first the only ones ever served while they still hold data.
	///
	/// # Returns
	///
	/// * The number of queue entries visited, how many of those failed, and how many keys were
	///   destroyed.
	async fn reclaim_pass(
		&self,
		interval: Duration,
		batch: u32,
		budget: u64,
		ic_budget: u64,
		mut queue: u64,
	) -> Result<(u64, u64, u64), Error> {
		// Attempt to acquire a lease for the ReclaimTombstones task
		// If we don't get the lease, another node is handling this task
		let lh = LeaseHandler::new(
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::ReclaimTombstones,
			interval.saturating_mul(2),
		)?;
		if !lh.has_lease().await? {
			return Ok((0, 0, 0));
		}
		// Walk the queue in bounded pages, so that a large backlog is never
		// held in memory nor read inside a single long-lived transaction.
		//
		// A pass that stops short resumes where it stopped rather than at the
		// beginning. An entry stays in the queue until its data is destroyed, so a
		// queue longer than one pass may read would otherwise be the same entries
		// every time and the removals behind them would not even be stamped, which
		// is what puts them in line for a budget at all. The resume point is this
		// node's: the lease means one node runs this, and a lease that moves costs
		// one sweep from the beginning.
		let (beg, end) = ReclaimKey::range();
		let mut cursor = self.reclaim_cursor.lock().unwrap().clone().unwrap_or(beg);
		// Where the next pass starts. A walk that reaches the end of the queue
		// leaves nothing behind it, so the next one starts again at the beginning.
		let mut resume: Option<Key> = None;
		let mut visited = 0;
		let mut errors = 0;
		// The entries this pass will spend its key budget on, ordered by first
		// observation and truncated to the quota after every queue page, so the
		// candidate set is bounded however long the queue is
		let mut due: Vec<(u64, Key, ReclaimState)> = Vec::new();
		// What the whole pass may delete from the index compaction queue while
		// it observes removals. Every entry in the queue is observed in one
		// pass, so this is the one place where the work a pass does would
		// otherwise be set by how many removals are waiting.
		let mut ic_left = ic_budget;
		loop {
			// A queue still being written to has no end this could reach, so
			// what is left belongs to the next pass
			if queue == 0 {
				trace!(target: TARGET, "Reclaim stopped: it read what one pass may read");
				resume = Some(cursor.clone());
				break;
			}
			// Read one page of queue entries, holding no transaction open while
			// the entries in it are worked on
			let txn = self.transaction(Read, Optimistic).await?;
			let rng = cursor.clone()..end.clone();
			let limit = queue.min(RECLAIM_QUEUE_PAGE_SIZE as u64) as u32;
			let page = catch!(txn, txn.scan(rng, limit, None).await);
			txn.cancel().await?;
			queue = queue.saturating_sub(page.len() as u64);
			// The whole queue has been walked
			let Some((last, _)) = page.last() else {
				break;
			};
			// A short page means the scan reached the end of the queue
			let drained = (page.len() as u32) < limit;
			// Resume the walk strictly after the last entry of this page
			cursor = last.clone();
			cursor.push(0x00);
			// The walk itself writes: an unobserved entry is stamped, and what it
			// left in the compaction queue is dropped before that. A backlog of
			// removals is long enough to outlive a lease granted for twice the
			// interval, and a second node taking it would destroy the same prefixes
			// alongside this one. Renewing per page rather than per entry is what
			// this walk costs: a page is bounded work, and the deletions inside it
			// renew on their own pages.
			let _ = lh.try_maintain_lease().await?;
			for (k, v) in page.iter() {
				visited += 1;
				let state: ReclaimState = match revision::from_slice(v) {
					Ok(state) => state,
					Err(e) => {
						errors += 1;
						error!(target: TARGET, "Error reading a reclaim queue entry: {e}");
						continue;
					}
				};
				// Stamping it is the only work the first sighting does
				if !state.is_observed() {
					if let Err(e) = self.reclaim_observe(&lh, k, &mut ic_left).await {
						errors += 1;
						error!(target: TARGET, "Error observing removed data: {e}");
					}
					yield_now!();
					continue;
				}
				due.push((state.observed_ms, k.clone(), state));
			}
			// Keep only the longest-waiting candidates, so that the set a pass
			// carries is bounded by the quota rather than by the queue
			if due.len() > RECLAIM_PASS_ENTRY_QUOTA {
				due.sort_unstable_by_key(|(observed, key, _)| (*observed, key.clone()));
				due.truncate(RECLAIM_PASS_ENTRY_QUOTA);
			}
			if drained {
				break;
			}
		}
		*self.reclaim_cursor.lock().unwrap() = resume;
		due.sort_unstable_by_key(|(observed, key, _)| (*observed, key.clone()));
		// Spend the key budget on the candidates, each granted its share of what
		// the ones before it left, so no single prefix can take the whole pass.
		// The compaction queue allowance is divided the same way, and it is the
		// one this pass has already spent part of on the removals it observed.
		let mut left = budget;
		let mut remaining = due.len() as u64;
		for (_, key, state) in due.iter() {
			if left == 0 {
				break;
			}
			let share = (left / remaining).max(batch as u64).min(left);
			let ic_share = (ic_left / remaining).max(batch as u64).min(ic_left);
			remaining = remaining.saturating_sub(1);
			let mut spend = share;
			let mut ic_spend = ic_share;
			let res = self.reclaim_entry(&lh, key, state, batch, &mut spend, &mut ic_spend).await;
			left -= share - spend;
			ic_left -= ic_share - ic_spend;
			if let Err(e) = res {
				errors += 1;
				error!(target: TARGET, "Error reclaiming removed data: {e}");
			}
			yield_now!();
		}
		Ok((visited, errors, budget - left))
	}

	/// The current wall clock as unix milliseconds.
	///
	/// A queue entry's stamp is persisted and so must survive a restart, which
	/// rules out a monotonic instant, and must be independent of the datastore
	/// clock, which tests move freely.
	fn unix_now_ms() -> Result<u64, Error> {
		Ok(SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|e| {
				Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration()))
			})?
			.as_millis() as u64)
	}

	/// Reads a reclaim queue entry back inside a transaction that is about to
	/// act on it, giving `None` when the entry is no longer there.
	///
	/// Every transaction which destroys data for a job, or writes the job back,
	/// must read it this way first. Reusing a removed name deletes the job in
	/// the same transaction as the data it named, so a job read from an earlier
	/// queue scan may already describe a prefix that belongs to a live object
	/// again. Reading it here is the check for that, against this transaction's
	/// own snapshot: an entry a reuse has already deleted reads as `None` and
	/// this transaction does nothing. It is not a read dependency that conflicts
	/// with a reuse it cannot see — the backends on this line validate write
	/// sets only — so what a reuse committing in that window relies on is that
	/// it destroys the prefix itself, in the transaction that takes the name.
	async fn reclaim_state(txn: &Transaction, key: &[u8]) -> Result<Option<ReclaimState>, Error> {
		match txn.get(key, None).await? {
			Some(val) => Ok(Some(revision::from_slice(&val)?)),
			None => Ok(None),
		}
	}

	/// Whether the catalog holds the object a reclaim job names.
	///
	/// A job names a prefix by the name of the namespace or database it belonged
	/// to, and an object recreated under that name owns the same prefix. The
	/// catalog is therefore the only authority on whether the prefix is still
	/// orphaned, and it is consulted inside the transaction that would destroy
	/// it.
	async fn reclaim_target_live(txn: &Transaction, job: &ReclaimKey<'_>) -> Result<bool, Error> {
		match job.db.as_deref() {
			Some(db) => txn.db_exists(&job.ns, db).await,
			None => txn.ns_exists(&job.ns).await,
		}
	}

	/// Stamps a queue entry the pass has not seen before.
	///
	/// The index compaction entries of the removed object go first: they name
	/// indexes inside the prefix about to be destroyed, and compaction would
	/// otherwise write index count keys back into it. They are dropped before
	/// the stamp rather than with it, so an interrupted purge is met again as an
	/// unobserved entry and retried, and they are dropped in their own committed
	/// pages, so no transaction here exceeds one page whatever the queue holds.
	///
	/// The stamp itself re-reads the entry inside the transaction that writes
	/// it, so an entry another writer has already advanced or deleted is left
	/// alone.
	async fn reclaim_observe(
		&self,
		lh: &LeaseHandler,
		key: &[u8],
		ic_budget: &mut u64,
	) -> Result<(), Error> {
		let job = ReclaimKey::decode_key(key)?;
		// The earliest chance to drop what this object left in the compaction
		// queue, and the only work here that is not a single small write. Every
		// removal the queue holds is observed in one pass, so this spends the
		// pass's allowance rather than one of its own: charging it per job would
		// let a backlog of removals decide how much a pass deletes. What it does
		// not reach is finished by the passes that destroy the data, which do
		// not retire the job until it is.
		self.purge_index_compaction(lh, key, &job, ic_budget).await?;
		let txn = self.transaction(Write, Optimistic).await?;
		let res = Self::reclaim_state(&txn, key).await;
		let Some(mut state) = catch!(txn, res) else {
			txn.cancel().await?;
			return Ok(());
		};
		// Read from the clock here rather than taking the instant the pass
		// started. The stamp is what orders the queue, and an entry written while
		// the walk was already under way commits after that instant, so stamping
		// it with the start of the pass would put it ahead of removals that were
		// already waiting.
		let now = catch!(txn, Self::unix_now_ms());
		if !state.observe(now) {
			txn.cancel().await?;
			return Ok(());
		}
		let enc = catch!(txn, revision::to_vec(&state).map_err(Error::from));
		catch!(txn, txn.set(key, enc, None).await);
		catch!(txn, txn.commit().await);
		Ok(())
	}

	/// Destroys pages of one entry's prefix until it is drained or `budget` is
	/// spent, leaving in `budget` what it did not use.
	///
	/// Each transaction re-reads the entry, checks that it still says what the
	/// queue scan reported and that the catalog still lacks the object it names,
	/// and only then destroys anything; an entry that fails either check is left
	/// alone, because reusing its name has already destroyed what it described.
	/// Each transaction commits the deletions it performed together with the
	/// cursor that accounts for them, so a cursor can never claim progress that
	/// was rolled back, and `observed_ms` is carried through every page
	/// unchanged, so a prefix too large for one pass keeps its place in line
	/// rather than going to the back of it.
	///
	/// Every version of every key goes, not just the visible one: names are
	/// reused on this line, so a prefix that was only tombstoned would be
	/// inherited version-for-version by the next object of the same name.
	async fn reclaim_entry(
		&self,
		lh: &LeaseHandler,
		key: &[u8],
		seen: &ReclaimState,
		batch: u32,
		budget: &mut u64,
		ic_budget: &mut u64,
	) -> Result<(), Error> {
		let job = ReclaimKey::decode_key(key)?;
		// The queue entries this object left behind go before the job that names
		// them is retired: nothing else would name them afterwards, and they are
		// root-level keys rather than part of the prefix destroyed below.
		//
		// A share of the pass's allowance rather than a cap of this entry's own:
		// how many entries reach here is set by how many removals are waiting,
		// which is exactly what the allowance is here to keep out of the size of
		// a pass. The share is at least one page, so an entry always has enough
		// to make progress towards retiring.
		let purged = self.purge_index_compaction(lh, key, &job, ic_budget).await?;
		let mut cursor = seen.cursor.clone();
		// Destroy the prefix one committed page at a time, until it is drained
		// or this entry has spent its share of the pass
		let rng = job.prefix_range()?;
		while *budget > 0 {
			// Lease maintenance: try_maintain_lease() returns a boolean indicating lease
			// ownership, but we intentionally ignore it and continue processing. Every page
			// is durable on its own, so a lost lease costs at most the pages of one prefix
			// being destroyed twice over, which is idempotent. It is renewed before the page
			// rather than after it, so a prefix which drains on its first page renews too.
			let _ = lh.try_maintain_lease().await?;
			// Never delete more keys than this entry has left
			let limit = (*budget).min(batch as u64) as u32;
			let txn = self.transaction(Write, Optimistic).await?;
			// The job authorises these deletions, so it is read in the same
			// transaction as them
			let res = Self::reclaim_state(&txn, key).await;
			let Some(state) = catch!(txn, res) else {
				txn.cancel().await?;
				return Ok(());
			};
			// An entry that has moved on belongs to another writer
			if state.observed_ms != seen.observed_ms || state.cursor != cursor {
				txn.cancel().await?;
				return Ok(());
			}
			// The name may have been reused, in which case the prefix is live
			let res = Self::reclaim_target_live(&txn, &job).await;
			if catch!(txn, res) {
				txn.cancel().await?;
				warn!(
					target: TARGET,
					"Data reclaim: {} exists again while a job still names it, leaving the job alone",
					Self::reclaim_object(&job),
				);
				return Ok(());
			}
			let res = txn.delp_bounded(rng.clone(), cursor.as_deref(), limit, true).await;
			let (last, count, drained) = catch!(txn, res);
			*budget = budget.saturating_sub(count);
			// Removing the job in the same transaction as its final deletions
			// means the queue never names a prefix that is already gone. The job
			// key holds a version for every page committed against it and its
			// uid is never reused, so it is cleared rather than tombstoned.
			if drained {
				// Drained describes the window after the cursor, not the prefix.
				// A removal writes only the catalog row and the queue entry, so
				// a transaction holding a snapshot from before it conflicts with
				// neither and may commit a key anywhere in the prefix, including
				// behind a cursor this walk has already passed. Retiring on the
				// suffix alone would leave such a key for whatever takes the name
				// next, which is the one thing the reclaim exists to prevent.
				//
				// Only a sweep that began at the start of the prefix and deleted
				// nothing shows it empty. Any other drained page starts one, and
				// because a write like that can only come from a transaction
				// older than the removal, the sweeps end when those have.
				//
				// That is as far as this side reaches: it holds as of the commit
				// that retires the job, and a transaction older than the removal
				// may still commit into the prefix after it. Nothing names such a
				// key afterwards, so what catches it is the other side of the
				// guard — taking the name empties the prefix it addresses whether
				// or not a job still describes it
				// ([`Transaction::finish_pending_reclaim`]).
				if cursor.is_none() && count == 0 {
					// A queue too large for one call's cap keeps the job, so that the
					// next pass purges the rest of it rather than leaving entries
					// behind with nothing left to name them
					if purged {
						catch!(txn, txn.clr(key).await);
					}
					catch!(txn, txn.commit().await);
					return Ok(());
				}
				let mut state = state;
				state.cursor = None;
				let enc = catch!(txn, revision::to_vec(&state).map_err(Error::from));
				catch!(txn, txn.set(key, enc, None).await);
				catch!(txn, txn.commit().await);
				cursor = None;
				yield_now!();
				continue;
			}
			// The cursor commits with the deletions it accounts for
			let mut state = state;
			state.cursor = last.clone();
			let enc = catch!(txn, revision::to_vec(&state).map_err(Error::from));
			catch!(txn, txn.set(key, enc, None).await);
			catch!(txn, txn.commit().await);
			cursor = last;
			yield_now!();
		}
		Ok(())
	}

	/// The namespace, or namespace and database, a reclaim job names.
	fn reclaim_object(job: &ReclaimKey<'_>) -> String {
		match job.db.as_deref() {
			Some(db) => format!("{}/{}", job.ns, db),
			None => job.ns.to_string(),
		}
	}

	/// Removes the index compaction queue entries of a removed object.
	///
	/// Compaction of a count index writes index count keys back into the
	/// database prefix they belong to, so an entry naming a removed namespace
	/// or database describes a prefix the reclaim task is about to destroy. The
	/// namespace and database are the leading fields of a compaction key, so
	/// the entries of one object are a contiguous range.
	///
	/// The range is dropped in committed pages of at most
	/// [`RECLAIM_BATCH_SIZE`] entries, so this bounds the write set of each of
	/// its transactions the same way the data reclaim does, and it is capped at
	/// [`RECLAIM_IC_PURGE_BUDGET`] entries per call. Returns whether the range
	/// is drained; the job that named it is not retired until it is, so a range
	/// larger than one call's cap is finished by the calls after it. A page
	/// reports the range drained only when it comes back short, so a call whose
	/// last page exactly consumed its cap reads the range once more before it
	/// answers. Compaction
	/// itself skips an entry whose object the catalog no longer holds, so what
	/// is still queued in the meantime writes nothing.
	///
	/// A compaction entry is addressed by name, so every page re-reads the job
	/// and the catalog in the transaction that deletes: a name reused while this
	/// runs owns the entries under it, and dropping them would leave the new
	/// object's count-index deltas uncompacted until another write enqueued a
	/// trigger. Both are read against this transaction's own snapshot, so a
	/// reuse it can see stops the page; a reuse it cannot see is not conflicted
	/// with, because the backends on this line validate write sets only. A page
	/// drops at most one batch, so that window costs at most one batch of
	/// triggers, which the next write to the new object enqueues again.
	async fn purge_index_compaction(
		&self,
		lh: &LeaseHandler,
		key: &[u8],
		job: &ReclaimKey<'_>,
		budget: &mut u64,
	) -> Result<bool, Error> {
		let rng = match job.db.as_deref() {
			Some(db) => IndexCompactionKey::database_range(&job.ns, db),
			None => IndexCompactionKey::namespace_range(&job.ns),
		};
		let mut cursor: Option<Key> = None;
		while *budget > 0 {
			// A purge of its own is the largest thing the walk over the queue does,
			// and the walk is otherwise a read: renewing here is what keeps a pass
			// over a backlog of removals inside the lease it was granted
			let _ = lh.try_maintain_lease().await?;
			let limit = (*budget).min(RECLAIM_BATCH_SIZE as u64) as u32;
			let txn = self.transaction(Write, Optimistic).await?;
			// The job authorises these deletions, so it is read in the same
			// transaction as them
			let res = Self::reclaim_state(&txn, key).await;
			if catch!(txn, res).is_none() {
				txn.cancel().await?;
				return Ok(true);
			}
			// The name may have been reused, in which case the entries under it
			// belong to the object that took it
			let res = Self::reclaim_target_live(&txn, job).await;
			if catch!(txn, res) {
				txn.cancel().await?;
				return Ok(true);
			}
			// Every entry carries a fresh uid, so no later one ever writes over
			// these: tombstoning them would leave the queue of a removed object
			// on disk for good on a backend that retains versions
			let res = txn.delp_bounded(rng.clone(), cursor.as_deref(), limit, true).await;
			let (last, count, drained) = catch!(txn, res);
			catch!(txn, txn.commit().await);
			*budget = budget.saturating_sub(count);
			// A short page means the queue holds nothing more for this object
			if drained {
				return Ok(true);
			}
			// A full page says nothing about what is behind it, so a call with
			// nothing left to spend asks once rather than report a queue it has
			// just emptied as one it ran out of room for, which would leave the
			// job naming that queue standing for another interval
			if *budget == 0 {
				let txn = self.transaction(Read, Optimistic).await?;
				let res = txn.any_version_in(rng.clone()).await;
				let held = catch!(txn, res);
				txn.cancel().await?;
				if !held {
					return Ok(true);
				}
				warn!(
					target: TARGET,
					"Data reclaim: the index compaction queue for {} still holds entries after this purge spent what it was given",
					Self::reclaim_object(job),
				);
				return Ok(false);
			}
			// Resume strictly after the last entry this page deleted
			cursor = last;
			yield_now!();
		}
		// A call given no allowance has not looked at the queue at all, and says
		// nothing about how large it is
		Ok(false)
	}

	/// Performs a database import from SQL
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn startup(&self, sql: &str, sess: &Session) -> Result<Vec<Response>, Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore startup import script");
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}

	/// Run the datastore shutdown tasks, perfoming any necessary cleanup
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn shutdown(&self) -> Result<(), Error> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore shutdown operations");
		// Delete this datastore from the cluster
		self.delete_node(self.id).await?;
		// Run any storag engine shutdown tasks
		match self.transaction_factory.flavor.as_ref() {
			#[cfg(feature = "kv-mem")]
			DatastoreFlavor::Mem(v) => v.shutdown().await,
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => v.shutdown().await,
			#[cfg(feature = "kv-indxdb")]
			DatastoreFlavor::IndxDB(v) => v.shutdown().await,
			#[cfg(feature = "kv-tikv")]
			DatastoreFlavor::TiKV(v) => v.shutdown().await,
			#[cfg(feature = "kv-fdb")]
			DatastoreFlavor::FoundationDB(v) => v.shutdown().await,
			#[cfg(feature = "kv-surrealkv")]
			DatastoreFlavor::SurrealKV(v) => v.shutdown().await,
			#[cfg(feature = "kv-surrealcs")]
			DatastoreFlavor::SurrealCS(v) => v.shutdown().await,
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

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
	#[allow(unreachable_code)]
	pub async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
	) -> Result<Transaction, Error> {
		self.transaction_factory.transaction(write, lock).await
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
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Variables,
	) -> Result<Vec<Response>, Error> {
		// Parse the SQL query text
		let ast = syn::parse_with_capabilities(txt, &self.capabilities)?;
		// Process the AST
		self.process(ast, sess, vars).await
	}

	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn execute_import<S>(
		&self,
		sess: &Session,
		vars: Variables,
		query: S,
	) -> Result<Vec<Response>, Error>
	where
		S: Stream<Item = Result<Bytes, Error>>,
	{
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}

		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		self.check_anon(sess).map_err(|_| IamError::NotAllowed {
			actor: "anonymous".to_string(),
			action: "process".to_string(),
			resource: "query".to_string(),
		})?;

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx()?;
		// Start an execution context
		sess.context(&mut ctx);
		// Store the query variables
		vars.attach(&mut ctx)?;
		// Process all statements

		let parser_settings = ParserSettings {
			references_enabled: ctx
				.get_capabilities()
				.allows_experimental(&ExperimentalTarget::RecordReferences),
			bearer_access_enabled: ctx
				.get_capabilities()
				.allows_experimental(&ExperimentalTarget::BearerAccess),
			define_api_enabled: ctx
				.get_capabilities()
				.allows_experimental(&ExperimentalTarget::DefineApi),
			..Default::default()
		};
		let mut statements_stream = StatementStream::new_with_settings(parser_settings);
		let mut buffer = BytesMut::new();
		let mut parse_size = 4096;
		let mut bytes_stream = pin!(query);
		let mut complete = false;
		let mut filling = true;

		let stream = futures::stream::poll_fn(move |cx| loop {
			// fill the buffer to at least parse_size when filling is required.
			while filling {
				let bytes = ready!(bytes_stream.as_mut().poll_next(cx));
				let bytes = match bytes {
					Some(Err(e)) => return Poll::Ready(Some(Err(e))),
					Some(Ok(x)) => x,
					None => {
						complete = true;
						filling = false;
						break;
					}
				};

				buffer.extend_from_slice(&bytes);
				filling = buffer.len() < parse_size
			}

			// if we finished streaming we can parse with complete so that the parser can be sure
			// of it's results.
			if complete {
				return match statements_stream.parse_complete(&mut buffer) {
					Err(e) => Poll::Ready(Some(Err(Error::InvalidQuery(e)))),
					Ok(None) => Poll::Ready(None),
					Ok(Some(x)) => Poll::Ready(Some(Ok(x))),
				};
			}

			// otherwise try to parse a single statement.
			match statements_stream.parse_partial(&mut buffer) {
				Err(e) => return Poll::Ready(Some(Err(Error::InvalidQuery(e)))),
				Ok(Some(x)) => return Poll::Ready(Some(Ok(x))),
				Ok(None) => {
					// Couldn't parse a statement for sure.
					if buffer.len() >= parse_size && parse_size < u32::MAX as usize {
						// the buffer already contained more or equal to parse_size bytes
						// this means we are trying to parse a statement of more then buffer size.
						// so we need to increase the buffer size.
						parse_size = (parse_size + 1).next_power_of_two();
					}
					// start filling the buffer again.
					filling = true;
				}
			}
		});

		Executor::execute_stream(
			self,
			Arc::new(ctx),
			opt,
			*cnf::SKIP_IMPORT_SUCCESS_RESULTS,
			stream,
		)
		.await
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
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
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
		self.check_anon(sess).map_err(|_| IamError::NotAllowed {
			actor: "anonymous".to_string(),
			action: "process".to_string(),
			resource: "query".to_string(),
		})?;

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx()?;
		// Start an execution context
		sess.context(&mut ctx);
		// Store the query variables
		vars.attach(&mut ctx)?;
		// Process all statements
		Executor::execute(self, ctx.freeze(), opt, ast).await
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
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
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
		self.check_anon(sess).map_err(|_| IamError::NotAllowed {
			actor: "anonymous".to_string(),
			action: "compute".to_string(),
			resource: "value".to_string(),
		})?;

		// Create a new memory stack
		let mut stack = TreeStack::new();
		// Create a new query options
		let opt = self.setup_options(sess);
		// Create a default context
		let mut ctx = MutableContext::default();
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
		sess.context(&mut ctx);
		// Store the query variables
		vars.attach(&mut ctx)?;
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let res = stack.enter(|stk| val.compute(stk, &ctx, &opt, None)).finish().await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.cancel().await?,
		};
		// Return result
		res
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a record access SIGNUP or
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
	///     let res = ds.evaluate(&val, &ses, None).await?;
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn evaluate(
		&self,
		val: &Value,
		sess: &Session,
		vars: Variables,
	) -> Result<Value, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Create a new memory stack
		let mut stack = TreeStack::new();
		// Create a new query options
		let opt = self.setup_options(sess);
		// Create a default context
		let mut ctx = MutableContext::default();
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
		sess.context(&mut ctx);
		// Store the query variables
		vars.attach(&mut ctx)?;
		// Start a new transaction
		let txn = self.transaction(val.writeable().into(), Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let res = stack.enter(|stk| val.compute(stk, &ctx, &opt, None)).finish().await;
		// Store any data
		match (res.is_ok(), val.writeable()) {
			// If the compute was successful, then commit if writeable
			(true, true) => txn.commit().await?,
			// Cancel if the compute was an error, or if readonly
			(_, _) => txn.cancel().await?,
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
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub fn notifications(&self) -> Option<Receiver<Notification>> {
		self.notification_channel.as_ref().map(|v| v.1.clone())
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<Response>, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn import_stream<S>(&self, sess: &Session, stream: S) -> Result<Vec<Response>, Error>
	where
		S: Stream<Item = Result<Bytes, Error>>,
	{
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Execute the SQL import
		self.execute_import(sess, None, stream).await
	}

	/// Performs a full database export as SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn export(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<(), Error>>, Error> {
		// Create a default export config
		let cfg = super::export::Config::default();
		self.export_with_config(sess, chn, cfg).await
	}

	/// Performs a full database export as SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn export_with_config(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
		cfg: export::Config,
	) -> Result<impl Future<Output = Result<(), Error>>, Error> {
		// Check if the session has expired
		if sess.expired() {
			return Err(Error::ExpiredSession);
		}
		// Retrieve the provided NS and DB
		let (ns, db) = crate::iam::check::check_ns_db(sess)?;
		// Create a new readonly transaction
		let txn = self.transaction(Read, Optimistic).await?;

		// Return an async export job
		Ok(async move {
			// Process the export
			if cfg.v3 {
				let mut buffer = Vec::new();
				crate::kvs::export::export_v3(&txn, &cfg, chn, &ns, &db, &mut buffer).await?;
				for b in buffer {
					if let Some(loc) = b.error_location {
						warn!(
							issue = b.kind.as_str(),
							severity = b.severity.as_str(),
							"Export for version 3 encountered issue: {}\n{}",
							b.error,
							loc
						);
					} else {
						warn!(
							issue = b.kind.as_str(),
							severity = b.severity.as_str(),
							"Export for version 3 encountered issue: {}",
							b.error
						);
					}
				}
			} else {
				txn.export(&ns, &db, cfg, chn).await?;
			}
			txn.cancel().await
		})
	}

	/// Checks the required permissions level for this session
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self, sess))]
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

	pub fn setup_options(&self, sess: &Session) -> Options {
		Options::default()
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled)
	}
	pub fn setup_ctx(&self) -> Result<MutableContext, Error> {
		let mut ctx = MutableContext::from_ds(
			self.query_timeout,
			self.slow_log.clone(),
			self.capabilities.clone(),
			self.index_stores.clone(),
			self.cache.clone(),
			self.index_builder.clone(),
			#[cfg(storage)]
			self.temporary_directory.clone(),
		)?;
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		Ok(ctx)
	}

	/// check for disallowed anonymous users
	pub fn check_anon(&self, sess: &Session) -> Result<(), IamError> {
		if self.auth_enabled && sess.au.is_anon() && !self.capabilities.allows_guest_access() {
			Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: String::new(),
				resource: String::new(),
			})
		} else {
			Ok(())
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;

	#[tokio::test]
	pub async fn very_deep_query() -> Result<(), Error> {
		use crate::kvs::Datastore;
		use crate::sql::{Expression, Future, Number, Operator, Value};
		use reblessive::{Stack, Stk};

		// build query manually to bypass query limits.
		let mut stack = Stack::new();
		async fn build_query(stk: &mut Stk, depth: usize) -> Value {
			if depth == 0 {
				Value::Expression(Box::new(Expression::Binary {
					l: Value::Number(Number::Int(1)),
					o: Operator::Add,
					r: Value::Number(Number::Int(1)),
				}))
			} else {
				let q = stk.run(|stk| build_query(stk, depth - 1)).await;
				Value::Future(Box::new(Future::from(q)))
			}
		}
		let val = stack.enter(|stk| build_query(stk, 1000)).finish();

		let dbs = Datastore::new("memory").await.unwrap().with_capabilities(Capabilities::all());

		let opt = Options::default()
			.with_id(dbs.id)
			.with_ns(Some("test".into()))
			.with_db(Some("test".into()))
			.with_live(false)
			.with_strict(false)
			.with_auth_enabled(false)
			.with_max_computation_depth(u32::MAX)
			.with_futures(true);

		// Create a default context
		let mut ctx = MutableContext::default();
		// Set context capabilities
		ctx.add_capabilities(dbs.capabilities.clone());
		// Start a new transaction
		let txn = dbs.transaction(val.writeable().into(), Optimistic).await?;
		// Store the transaction
		ctx.set_transaction(txn.enclose());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack.enter(|stk| val.compute(stk, &ctx, &opt, None)).finish().await.unwrap();
		assert_eq!(res, Value::Number(Number::Int(2)));
		Ok(())
	}
}
