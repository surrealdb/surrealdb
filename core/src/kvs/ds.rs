use super::tr::Transactor;
use super::tx::Transaction;
use crate::cf;
use crate::ctx::MutableContext;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::node::Timestamp;
use crate::dbs::{
	Attach, Capabilities, Executor, Notification, Options, Response, Session, Variables,
};
use crate::err::Error;
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::iam::{Action, Auth, Error as IamError, Resource, Role};
use crate::idx::trees::store::IndexStores;
use crate::kvs::clock::SizedClock;
#[allow(unused_imports)]
use crate::kvs::clock::SystemClock;
#[cfg(not(target_arch = "wasm32"))]
use crate::kvs::index::IndexBuilder;
use crate::kvs::{LockType, LockType::*, TransactionType, TransactionType::*};
use crate::sql::{statements::DefineUserStatement, Base, Query, Value};
use crate::syn;
use crate::vs::{conv, Versionstamp};
use channel::{Receiver, Sender};
use futures::Future;
use reblessive::TreeStack;
use std::fmt;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
))]
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
#[cfg(not(target_arch = "wasm32"))]
use std::time::{SystemTime, UNIX_EPOCH};
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tracing::instrument;
use tracing::trace;
use uuid::Uuid;
#[cfg(target_arch = "wasm32")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

const TARGET: &str = "surrealdb::core::kvs::tr";

// If there are an infinite number of heartbeats, then we want to go batch-by-batch spread over several checks
const LQ_CHANNEL_SIZE: usize = 100;

// The role assigned to the initial user created when starting the server with credentials for the first time
const INITIAL_USER_ROLE: &str = "owner";

/// The underlying datastore instance which stores the dataset.
#[allow(dead_code)]
#[non_exhaustive]
pub struct Datastore {
	transaction_factory: TransactionFactory,
	// The unique id of this datastore, used in notifications
	id: Uuid,
	// Whether this datastore runs in strict mode by default
	strict: bool,
	// Whether authentication is enabled on this datastore.
	auth_enabled: bool,
	// The maximum duration timeout for running multiple statements in a query
	query_timeout: Option<Duration>,
	// The maximum duration timeout for running multiple statements in a transaction
	transaction_timeout: Option<Duration>,
	// Capabilities for this datastore
	capabilities: Capabilities,
	// Whether this datastore enables live query notifications to subscribers
	pub(super) notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
	// The index store cache
	index_stores: IndexStores,
	// The index asynchronous builder
	#[cfg(not(target_arch = "wasm32"))]
	index_builder: IndexBuilder,
	#[cfg(feature = "jwks")]
	// The JWKS object cache
	jwks_cache: Arc<RwLock<JwksCache>>,
	#[cfg(any(
		feature = "kv-mem",
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
	))]
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

impl TransactionFactory {
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
		let inner = match self.flavor.as_ref() {
			#[cfg(feature = "kv-mem")]
			DatastoreFlavor::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::Mem(tx)
			}
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::RocksDB(tx)
			}
			#[cfg(feature = "kv-indxdb")]
			DatastoreFlavor::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::IndxDB(tx)
			}
			#[cfg(feature = "kv-tikv")]
			DatastoreFlavor::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::TiKV(tx)
			}
			#[cfg(feature = "kv-fdb")]
			DatastoreFlavor::FoundationDB(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::FoundationDB(tx)
			}
			#[cfg(feature = "kv-surrealkv")]
			DatastoreFlavor::SurrealKV(v) => {
				let tx = v.transaction(write, lock).await?;
				super::tr::Inner::SurrealKV(tx)
			}
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		};
		Ok(Transaction::new(Transactor {
			inner,
			stash: super::stash::Stash::default(),
			cf: cf::Writer::new(),
			clock: self.clock.clone(),
		}))
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

	#[cfg(debug_assertions)]
	/// Create a new datastore with the same persistent data (inner), with flushed cache.
	/// Simulating a server restart
	pub fn restart(self) -> Self {
		Self {
			id: self.id,
			strict: self.strict,
			auth_enabled: self.auth_enabled,
			query_timeout: self.query_timeout,
			transaction_timeout: self.transaction_timeout,
			capabilities: self.capabilities,
			notification_channel: self.notification_channel,
			index_stores: Default::default(),
			#[cfg(not(target_arch = "wasm32"))]
			index_builder: IndexBuilder::new(self.transaction_factory.clone()),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(Default::default()),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			temporary_directory: self.temporary_directory,
			transaction_factory: self.transaction_factory,
		}
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
			// Parse and initiate a SurrealKV datastore
			s if s.starts_with("surrealkv:") => {
				#[cfg(feature = "kv-surrealkv")]
				{
					info!(target: TARGET, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("surrealkv://");
					let s = s.trim_start_matches("surrealkv:");
					let v =
						super::surrealkv::Datastore::new(s).await.map(DatastoreFlavor::SurrealKV);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started to kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-surrealkv"))]
                return Err(Error::Ds("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// The datastore path is not valid
			_ => {
				info!(target: TARGET, "Unable to load the specified datastore {}", path);
				Err(Error::Ds("Unable to load the specified datastore".into()))
			}
		}?;
		// Set the properties on the datastore
		flavor.map(|flavor| {
			let tf = TransactionFactory {
				clock,
				flavor: Arc::new(flavor),
			};
			Self {
				id: Uuid::new_v4(),
				transaction_factory: tf.clone(),
				strict: false,
				auth_enabled: false,
				query_timeout: None,
				transaction_timeout: None,
				notification_channel: None,
				capabilities: Capabilities::default(),
				index_stores: IndexStores::default(),
				#[cfg(not(target_arch = "wasm32"))]
				index_builder: IndexBuilder::new(tf),
				#[cfg(feature = "jwks")]
				jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
				#[cfg(any(
					feature = "kv-mem",
					feature = "kv-surrealkv",
					feature = "kv-rocksdb",
					feature = "kv-fdb",
					feature = "kv-tikv",
				))]
				temporary_directory: None,
			}
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

	/// Set specific capabilities for this Datastore
	pub fn with_capabilities(mut self, caps: Capabilities) -> Self {
		self.capabilities = caps;
		self
	}

	#[cfg(any(
		feature = "kv-mem",
		feature = "kv-surrealkv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-tikv",
	))]
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

	/// Does the datastore allow connections to a network target?
	#[cfg(feature = "jwks")]
	pub(crate) fn allows_network_target(&self, net_target: &NetTarget) -> bool {
		self.capabilities.allows_network_target(net_target)
	}

	#[cfg(feature = "jwks")]
	pub(crate) fn jwks_cache(&self) -> &Arc<RwLock<JwksCache>> {
		&self.jwks_cache
	}

	pub(super) async fn clock_now(&self) -> Timestamp {
		self.transaction_factory.clock.now().await
	}

	// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn bootstrap(&self) -> Result<(), Error> {
		// Insert this node in the cluster
		self.insert_node(self.id).await?;
		// Mark expired nodes as archived
		self.expire_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Setup the initial cluster access credentials
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn initialise_credentials(&self, user: &str, pass: &str) -> Result<(), Error> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Optimistic).await?.enclose();
		// Fetch the root users from the storage
		let users = catch!(txn, txn.all_root_users());
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
			catch!(txn, stm.compute(&ctx, &opt, None));
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

	// tick is called periodically to perform maintenance tasks.
	// This is called every TICK_INTERVAL.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
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
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn tick_at(&self, ts: u64) -> Result<(), Error> {
		trace!(target: TARGET, "Ticking at timestamp {ts} ({:?})", conv::u64_to_versionstamp(ts));
		let _vs = self.save_timestamp_for_versionstamp(ts).await?;
		self.garbage_collect_stale_change_feeds(ts).await?;
		// Update this node in the cluster
		self.update_node(self.id).await?;
		// Mark expired nodes as archived
		self.expire_nodes().await?;
		// Cleanup expired nodes data
		self.cleanup_nodes().await?;
		// Garbage collect other data
		self.garbage_collect().await?;
		// Everything ok
		Ok(())
	}

	// save_timestamp_for_versionstamp saves the current timestamp for the each database's current versionstamp.
	// Note: the returned VS is flawed, as there are multiple {ts: vs} mappings per (ns, db)
	pub(crate) async fn save_timestamp_for_versionstamp(
		&self,
		ts: u64,
	) -> Result<Option<Versionstamp>, Error> {
		let tx = self.transaction(Write, Optimistic).await?;
		match self.save_timestamp_for_versionstamp_impl(ts, &tx).await {
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

	async fn save_timestamp_for_versionstamp_impl(
		&self,
		ts: u64,
		tx: &Transaction,
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
				vs = Some(tx.lock().await.set_timestamp_for_versionstamp(ts, ns, db).await?);
			}
		}
		tx.commit().await?;
		Ok(vs)
	}

	// garbage_collect_stale_change_feeds deletes all change feed entries that are older than the watermarks.
	pub(crate) async fn garbage_collect_stale_change_feeds(&self, ts: u64) -> Result<(), Error> {
		let tx = self.transaction(Write, Optimistic).await?;
		if let Err(e) = self.garbage_collect_stale_change_feeds_impl(&tx, ts).await {
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
		tx: &Transaction,
		ts: u64,
	) -> Result<(), Error> {
		cf::gc_all_at(tx, ts).await?;
		tx.commit().await?;
		Ok(())
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
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
		// Create a new query executor
		let mut exe = Executor::new(self);
		// Create a default context
		let mut ctx = MutableContext::from_ds(
			self.query_timeout,
			self.capabilities.clone(),
			self.index_stores.clone(),
			#[cfg(not(target_arch = "wasm32"))]
			self.index_builder.clone(),
			#[cfg(any(
				feature = "kv-mem",
				feature = "kv-surrealkv",
				feature = "kv-rocksdb",
				feature = "kv-fdb",
				feature = "kv-tikv",
			))]
			self.temporary_directory.clone(),
		)?;
		// Setup the notification channel
		if let Some(channel) = &self.notification_channel {
			ctx.add_notifications(Some(&channel.0));
		}
		// Start an execution context
		sess.context(&mut ctx);
		// Store the query variables
		vars.attach(&mut ctx)?;
		// Process all statements
		exe.execute(ctx.freeze(), opt, ast).await
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
		if sess.au.is_anon() && self.auth_enabled && !self.capabilities.allows_guest_access() {
			return Err(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "compute".to_string(),
				resource: "value".to_string(),
			}
			.into());
		}
		// Create a new memory stack
		let mut stack = TreeStack::new();
		// Create a new query options
		let opt = Options::default()
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
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
		let opt = Options::default()
			.with_id(self.id)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_strict(self.strict)
			.with_auth_enabled(self.auth_enabled);
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
		// Free the context
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

	/// Performs a full database export as SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
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
		let txn = self.transaction(Read, Optimistic).await?;
		// Return an async export job
		Ok(async move {
			// Process the export
			txn.export(&ns, &db, chn).await?;
			// Everything ok
			Ok(())
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
