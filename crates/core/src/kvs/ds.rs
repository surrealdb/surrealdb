use std::fmt;
#[cfg(storage)]
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Poll, ready};
use std::time::Duration;
#[cfg(not(target_family = "wasm"))]
use std::time::{SystemTime, UNIX_EPOCH};

#[allow(unused_imports)]
use anyhow::bail;
use anyhow::{Result, ensure};
use async_channel::{Receiver, Sender};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::{Future, Stream};
use reblessive::TreeStack;
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tracing::{instrument, trace};
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

use super::export;
use super::tr::Transactor;
use super::tx::Transaction;
use super::version::MajorVersion;
use crate::buc::BucketConnections;
use crate::catalog::Index;
use crate::ctx::MutableContext;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::capabilities::{
	ArbitraryQueryTarget, ExperimentalTarget, MethodTarget, RouteTarget,
};
use crate::dbs::node::Timestamp;
use crate::dbs::{Capabilities, Executor, Notification, Options, Response, Session, Variables};
use crate::err::Error;
use crate::expr::statements::DefineUserStatement;
use crate::expr::{Base, Expr, FlowResultExt as _, Ident, LogicalPlan};
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::iam::{Action, Auth, Error as IamError, Resource, Role};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::trees::store::IndexStores;
use crate::key::root::ic::Ic;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::clock::SizedClock;
#[expect(unused_imports)]
use crate::kvs::clock::SystemClock;
#[cfg(not(target_family = "wasm"))]
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::{LockType, TransactionType};
use crate::sql::Ast;
use crate::syn::parser::{ParserSettings, StatementStream};
use crate::val::{Strand, Value};
use crate::{cf, syn};

const TARGET: &str = "surrealdb::core::kvs::ds";

// If there are an infinite number of heartbeats, then we want to go
// batch-by-batch spread over several checks
const LQ_CHANNEL_SIZE: usize = 15_000;

// The role assigned to the initial user created when starting the server with
// credentials for the first time
const INITIAL_USER_ROLE: &str = "owner";

/// The underlying datastore instance which stores the dataset.
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
	/// The duration threshold determining when a query should be logged
	slow_log_threshold: Option<Duration>,
	/// The maximum duration timeout for running multiple statements in a
	/// transaction.
	transaction_timeout: Option<Duration>,
	/// The security and feature capabilities for this datastore.
	capabilities: Arc<Capabilities>,
	// Whether this datastore enables live query notifications to subscribers.
	notification_channel: Option<(Sender<Notification>, Receiver<Notification>)>,
	// The index store cache
	index_stores: IndexStores,
	// The cross transaction cache
	cache: Arc<DatastoreCache>,
	// The index asynchronous builder
	#[cfg(not(target_family = "wasm"))]
	index_builder: IndexBuilder,
	#[cfg(feature = "jwks")]
	// The JWKS object cache
	jwks_cache: Arc<RwLock<JwksCache>>,
	#[cfg(storage)]
	// The temporary directory
	temporary_directory: Option<Arc<PathBuf>>,
	// Map of bucket connections
	buckets: Arc<BucketConnections>,
	// The sequences
	sequences: Sequences,
}

#[derive(Clone)]
pub(super) struct TransactionFactory {
	// Clock for tracking time. It is read-only and accessible to all transactions.
	clock: Arc<SizedClock>,
	// The inner datastore type
	flavor: Arc<DatastoreFlavor>,
}

impl TransactionFactory {
	pub(super) fn new(clock: Arc<SizedClock>, flavor: DatastoreFlavor) -> Self {
		Self {
			clock,
			flavor: flavor.into(),
		}
	}

	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	pub async fn transaction(&self, write: TransactionType, lock: LockType) -> Result<Transaction> {
		// Specify if the transaction is writeable
		let write = match write {
			Read => false,
			Write => true,
		};
		// Specify if the transaction is lockable
		let lock = match lock {
			Pessimistic => true,
			Optimistic => false,
		};
		// Create a new transaction on the datastore
		let (inner, local) = match self.flavor.as_ref() {
			#[cfg(feature = "kv-mem")]
			DatastoreFlavor::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-indxdb")]
			DatastoreFlavor::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-tikv")]
			DatastoreFlavor::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, false)
			}
			#[cfg(feature = "kv-fdb")]
			DatastoreFlavor::FoundationDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, false)
			}
			#[cfg(feature = "kv-surrealkv")]
			DatastoreFlavor::SurrealKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			_ => unreachable!(),
		};
		Ok(Transaction::new(
			local,
			Transactor {
				inner,
				stash: super::stash::Stash::default(),
				cf: cf::Writer::new(),
			},
		))
	}
}

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
	/// # use anyhow::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(),Error> {
	/// let ds = Datastore::new("memory").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to create a file-backed store:
	///
	/// ```rust,no_run
	/// # use surrealdb_core::kvs::Datastore;
	/// # use anyhow::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(),Error> {
	/// let ds = Datastore::new("surrealkv://temp.skv").await?;
	/// # Ok(())
	/// # }
	/// ```
	///
	/// Or to connect to a tikv-backed distributed store:
	///
	/// ```rust,no_run
	/// # use surrealdb_core::kvs::Datastore;
	/// # use anyhow::Error;
	/// # #[tokio::main]
	/// # async fn main() -> Result<(),Error> {
	/// let ds = Datastore::new("tikv://127.0.0.1:2379").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub async fn new(path: &str) -> Result<Self> {
		Self::new_with_clock(path, None).await
	}

	#[allow(unused_variables)]
	pub async fn new_with_clock(path: &str, clock: Option<Arc<SizedClock>>) -> Result<Datastore> {
		// Initiate the desired datastore
		let (flavor, clock): (Result<DatastoreFlavor>, Arc<SizedClock>) = match path {
			// Initiate an in-memory datastore
			"memory" => {
				#[cfg(feature = "kv-mem")]
				{
					// Initialise the storage engine
					info!(target: TARGET, "Starting kvs store in {}", path);
					let v = super::mem::Datastore::new().await.map(DatastoreFlavor::Mem);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store in {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-mem"))]
				bail!(Error::Ds("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a File datastore
			s if s.starts_with("file:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
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
				bail!(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a RocksDB datastore
			s if s.starts_with("rocksdb:") => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
					info!(target: TARGET, "Starting kvs store at {}", path);
					let s = s.trim_start_matches("rocksdb://");
					let s = s.trim_start_matches("rocksdb:");
					let v = super::rocksdb::Datastore::new(s).await.map(DatastoreFlavor::RocksDB);
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store at {}", path);
					Ok((v, c))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
				bail!(Error::Ds("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
			}
			// Parse and initiate a SurrealKV datastore
			s if s.starts_with("surrealkv") => {
				#[cfg(feature = "kv-surrealkv")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
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
				bail!(Error::Ds("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
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
				bail!(Error::Ds("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
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
				bail!(Error::Ds("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
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
				bail!(Error::Ds("Cannot connect to the `foundationdb` storage engine as it is not enabled in this build of SurrealDB".to_owned()));
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
				slow_log_threshold: None,
				transaction_timeout: None,
				notification_channel: None,
				capabilities: Arc::new(Capabilities::default()),
				index_stores: IndexStores::default(),
				#[cfg(not(target_family = "wasm"))]
				index_builder: IndexBuilder::new(tf.clone()),
				#[cfg(feature = "jwks")]
				jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
				#[cfg(storage)]
				temporary_directory: None,
				cache: Arc::new(DatastoreCache::new()),
				buckets: Arc::new(DashMap::new()),
				sequences: Sequences::new(tf),
			}
		})
	}

	/// Create a new datastore with the same persistent data (inner), with
	/// flushed cache. Simulating a server restart
	pub fn restart(self) -> Self {
		Self {
			id: self.id,
			strict: self.strict,
			auth_enabled: self.auth_enabled,
			query_timeout: self.query_timeout,
			slow_log_threshold: self.slow_log_threshold,
			transaction_timeout: self.transaction_timeout,
			capabilities: self.capabilities,
			notification_channel: self.notification_channel,
			index_stores: Default::default(),
			#[cfg(not(target_family = "wasm"))]
			index_builder: IndexBuilder::new(self.transaction_factory.clone()),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(Default::default()),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			cache: Arc::new(DatastoreCache::new()),
			buckets: Arc::new(DashMap::new()),
			sequences: Sequences::new(self.transaction_factory.clone()),
			transaction_factory: self.transaction_factory,
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

	pub fn is_strict_mode(&self) -> bool {
		self.strict
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

	/// Set a global slow log threshold
	pub fn with_slow_log_threshold(mut self, duration: Option<Duration>) -> Self {
		self.slow_log_threshold = duration;
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
	pub fn get_cache(&self) -> Arc<DatastoreCache> {
		self.cache.clone()
	}

	// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn check_version(&self) -> Result<MajorVersion> {
		let version = self.get_version().await?;
		// Check we are running the latest version
		if !version.is_latest() {
			bail!(Error::OutdatedStorageVersion);
		}
		// Everything ok
		Ok(version)
	}

	// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn get_version(&self) -> Result<MajorVersion> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Pessimistic).await?.enclose();
		// Create the key where the version is stored
		let key = crate::key::version::new();
		// Check if a version is already set in storage
		let val = match catch!(txn, txn.get(&key, None).await) {
			// There is a version set in the storage
			Some(val) => {
				// We didn't write anything, so just rollback
				catch!(txn, txn.cancel().await);
				// Return the current version
				val
			}
			// There is no version set in the storage
			None => {
				// Fetch any keys immediately following the version key
				let rng = crate::key::version::proceeding();
				let keys = catch!(txn, txn.keys(rng, 1, None).await);
				// Check the storage if there are any other keys set
				let version = if keys.is_empty() {
					// There are no keys set in storage, so this is a new database
					MajorVersion::latest()
				} else {
					// There were keys in storage, so this is an upgrade
					MajorVersion::v1()
				};
				// Attempt to set the current version in storage
				catch!(txn, txn.replace(&key, &version).await);
				// We set the version, so commit the transaction
				catch!(txn, txn.commit().await);
				// Return the current version
				version
			}
		};
		// Everything ok
		Ok(val)
	}

	/// Setup the initial cluster access credentials
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn initialise_credentials(&self, user: &str, pass: &str) -> Result<()> {
		// Start a new writeable transaction
		let txn = self.transaction(Write, Optimistic).await?.enclose();
		// Fetch the root users from the storage
		let users = catch!(txn, txn.all_root_users().await);
		// Process credentials, depending on existing users
		if users.is_empty() {
			// Display information in the logs
			info!(target: TARGET, "Credentials were provided, and no root users were found. The root user '{user}' will be created");
			// Create and new root user definition
			let stm = DefineUserStatement::new_with_password(
				Base::Root,
				// TODO: Null byte validity.
				Strand::new(user.to_owned()).unwrap(),
				pass,
				// TODO: Null byte validity, always correct here probably.
				Ident::new(INITIAL_USER_ROLE.to_owned()).unwrap(),
			);
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
	pub async fn bootstrap(&self) -> Result<()> {
		// Insert this node in the cluster
		self.insert_node(self.id).await?;
		// Mark inactive nodes as archived
		self.expire_nodes().await?;
		// Remove archived nodes
		self.remove_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to update node registration information
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_update(&self) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Updating node registration information");
		// Update this node in the cluster
		self.update_node(self.id).await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to process and archive inactive nodes
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_expire(&self) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Processing and archiving inactive nodes");
		// Mark expired nodes as archived
		self.expire_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Run the background task to process and cleanup archived nodes
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn node_membership_remove(&self) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Processing and cleaning archived nodes");
		// Cleanup expired nodes data
		self.remove_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Performs changefeed garbage collection as a background task.
	///
	/// This method is responsible for cleaning up old changefeed data across
	/// all databases. It uses a distributed task lease mechanism to ensure
	/// that only one node in a cluster performs this maintenance operation at
	/// a time, preventing duplicate work and potential conflicts.
	///
	/// The process involves:
	/// 1. Acquiring a lease for the ChangeFeedCleanup task
	/// 2. Calculating the current system time
	/// 3. Saving timestamps for current versionstamps
	/// 4. Cleaning up old changefeed data from all databases
	///
	/// # Parameters
	/// * `delay` - Duration specifying how long the lease should be valid
	///
	/// # Returns
	/// * `Ok(())` - If the operation completes successfully or if this node doesn't have the lease
	/// * `Err` - If any step in the process fails
	///
	/// # Errors
	/// * Returns an error if the system clock appears to have gone backwards
	/// * Propagates any errors from the underlying database operations
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn changefeed_process(&self, gc_interval: &Duration) -> Result<()> {
		let lh = LeaseHandler::new(
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::ChangeFeedCleanup,
			*gc_interval * 2,
		)?;
		// Attempt to acquire a lease for the ChangeFeedCleanup task
		// If we don't get the lease, another node is handling this task
		if !lh.has_lease().await? {
			return Ok(());
		}
		let lh = Some(lh);
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		// Calculate the current system time in seconds since UNIX epoch
		// This will be used as a reference point for cleanup operations
		let ts = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|e| {
				Error::Internal(format!("Clock may have gone backwards: {:?}", e.duration()))
			})?
			.as_secs();
		// Save timestamps for current versionstamps to track cleanup progress
		self.changefeed_versionstamp(lh.as_ref(), ts).await?;
		// Remove old changefeed data from all databases based on retention policies
		self.changefeed_cleanup(lh.as_ref(), ts).await?;
		// Everything completed successfully
		Ok(())
	}

	/// Performs changefeed garbage collection using a specified timestamp.
	///
	/// This method is similar to `changefeed_process` but accepts an explicit
	/// timestamp instead of calculating the current time. This allows for more
	/// controlled testing and specific cleanup operations at predetermined
	/// points in time.
	///
	/// Unlike `changefeed_process`, this method does not use the task lease
	/// mechanism, making it suitable for direct invocation in controlled
	/// environments or testing scenarios where lease coordination is not
	/// required.
	///
	/// The process involves:
	/// 1. Saving timestamps for current versionstamps using the provided timestamp
	/// 2. Cleaning up old changefeed data from all databases
	///
	/// # Parameters
	/// * `ts` - Explicit timestamp (in seconds since UNIX epoch) to use for cleanup operations
	///
	/// # Returns
	/// * `Ok(())` - If the operation completes successfully
	/// * `Err` - If any step in the process fails
	///
	/// # Errors
	/// * Propagates any errors from the underlying database operations
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self, lh))]
	pub async fn changefeed_process_at(&self, lh: Option<&LeaseHandler>, ts: u64) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		// Save timestamps for current versionstamps using the provided timestamp
		self.changefeed_versionstamp(lh, ts).await?;
		// Remove old changefeed data from all databases based on retention policies
		// using the provided timestamp as the reference point
		self.changefeed_cleanup(lh, ts).await?;
		// Everything completed successfully
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
	pub async fn index_compaction(&self, interval: Duration) -> Result<()> {
		let lh = LeaseHandler::new(
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			interval * 2,
		)?;
		// We continue without interruptions while there are keys and the lease
		loop {
			// Attempt to acquire a lease for the ChangeFeedCleanup task
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok(());
			}
			// Create a new transaction
			let txn = self.transaction(Write, Optimistic).await?;
			// Collect every item in the queue
			let (beg, end) = Ic::range();
			let range = beg..end;
			let mut previous: Option<IndexKeyBase> = None;
			let mut count = 0;
			// Returns an ordered list of indexes that require compaction
			for (k, _) in txn.getr(range.clone(), None).await? {
				count += 1;
				lh.try_maintain_lease().await?;
				let ic = Ic::decode_key(&k)?;
				// If the index has already been compacted, we can ignore the task
				if let Some(p) = &previous {
					if p.match_ic(&ic) {
						continue;
					}
				}
				match txn.get_tb_index(ic.ns, ic.db, ic.tb, ic.ix).await {
					Ok(ix) => {
						if let Index::FullText(p) = &ix.index {
							let ft = FullTextIndex::new(
								self.id(),
								&self.index_stores,
								&txn,
								IndexKeyBase::from_ic(&ic),
								p,
							)
							.await?;
							ft.compaction(&txn).await?;
						}
					}
					Err(e) => {
						error!(target: TARGET, "Index compaction: Failed to get index: {}", e);
						if matches!(e.downcast_ref(), Some(Error::IxNotFound { .. })) {
							trace!(target: TARGET, "Index compaction: Index {} not found, skipping", ic.ix);
						} else {
							bail!(e);
						}
					}
				}
				previous = Some(IndexKeyBase::from_ic(&ic));
			}
			if count > 0 {
				txn.delr(range).await?;
				txn.commit().await?;
			} else {
				txn.cancel().await?;
				return Ok(());
			}
		}
	}

	/// Performs a database import from SQL
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn startup(&self, sql: &str, sess: &Session) -> Result<Vec<Response>> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore startup import script");
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}

	/// Run the datastore shutdown tasks, performing any necessary cleanup
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn shutdown(&self) -> Result<()> {
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
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}

	/// Create a new transaction on this datastore
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::{Datastore, TransactionType::*, LockType::*};
	/// use anyhow::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(),Error> {
	///     let ds = Datastore::new("file://database.db").await?;
	///     let mut tx = ds.transaction(Write, Optimistic).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(&self, write: TransactionType, lock: LockType) -> Result<Transaction> {
		self.transaction_factory.transaction(write, lock).await
	}

	pub async fn health_check(&self) -> Result<()> {
		let tx = self.transaction(Read, Optimistic).await?;

		// Cancel the transaction
		trace!("Cancelling health check transaction");
		// Attempt to fetch data
		match tx.get(&vec![0x00], None).await {
			Err(err) => {
				// Ensure the transaction is cancelled
				let _ = tx.cancel().await;
				// Return an error for this endpoint
				Err(err)
			}
			Ok(_) => {
				// Ensure the transaction is cancelled
				let _ = tx.cancel().await;
				// Return success for this endpoint
				Ok(())
			}
		}
	}

	/// Parse and execute an SQL query
	///
	/// ```rust,no_run
	/// use anyhow::Error;
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(),Error> {
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
		vars: Option<Variables>,
	) -> Result<Vec<Response>> {
		// Parse the SQL query text
		let ast = syn::parse_with_capabilities(txt, &self.capabilities)?;
		// Process the AST
		self.process(ast, sess, vars).await
	}

	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn execute_import<S>(
		&self,
		sess: &Session,
		vars: Option<Variables>,
		query: S,
	) -> Result<Vec<Response>>
	where
		S: Stream<Item = Result<Bytes>>,
	{
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);

		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		self.check_anon(sess).map_err(|_| {
			Error::from(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "process".to_string(),
				resource: "query".to_string(),
			})
		})?;

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx()?;
		// Start an execution context
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars)?;
		}
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
			files_enabled: ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Files),
			..Default::default()
		};
		let mut statements_stream = StatementStream::new_with_settings(parser_settings);
		let mut buffer = BytesMut::new();
		let mut parse_size = 4096;
		let mut bytes_stream = pin!(query);
		let mut complete = false;
		let mut filling = true;

		let stream = futures::stream::poll_fn(move |cx| {
			loop {
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

				// if we finished streaming we can parse with complete so that the parser can be
				// sure of it's results.
				if complete {
					return match statements_stream.parse_complete(&mut buffer) {
						Err(e) => {
							Poll::Ready(Some(Err(anyhow::Error::new(Error::InvalidQuery(e)))))
						}
						Ok(None) => Poll::Ready(None),
						Ok(Some(x)) => Poll::Ready(Some(Ok(x))),
					};
				}

				// otherwise try to parse a single statement.
				match statements_stream.parse_partial(&mut buffer) {
					Err(e) => {
						return Poll::Ready(Some(Err(anyhow::Error::new(Error::InvalidQuery(e)))));
					}
					Ok(Some(x)) => return Poll::Ready(Some(Ok(x))),
					Ok(None) => {
						// Couldn't parse a statement for sure.
						if buffer.len() >= parse_size && parse_size < u32::MAX as usize {
							// the buffer already contained more or equal to parse_size bytes
							// this means we are trying to parse a statement of more then buffer
							// size. so we need to increase the buffer size.
							parse_size = (parse_size + 1).next_power_of_two();
						}
						// start filling the buffer again.
						filling = true;
					}
				}
			}
		});

		Executor::execute_stream(self, Arc::new(ctx), opt, true, stream).await
	}

	/// Execute a pre-parsed SQL query
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn process(
		&self,
		ast: Ast,
		sess: &Session,
		vars: Option<Variables>,
	) -> Result<Vec<Response>> {
		//TODO: Insert planner here.
		self.process_plan(ast.into(), sess, vars).await
	}

	pub async fn process_plan(
		&self,
		plan: LogicalPlan,
		sess: &Session,
		vars: Option<Variables>,
	) -> Result<Vec<Response>> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		self.check_anon(sess).map_err(|_| {
			Error::from(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "process".to_string(),
				resource: "query".to_string(),
			})
		})?;

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx()?;
		// Start an execution context
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars)?;
		}

		// Process all statements
		Executor::execute_plan(self, ctx.freeze(), opt, plan).await
	}

	/// Ensure a SQL [`Value`] is fully computed
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn compute(
		&self,
		val: Expr,
		sess: &Session,
		vars: Option<Variables>,
	) -> Result<Value> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Check if anonymous actors can compute values when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		self.check_anon(sess).map_err(|_| {
			Error::from(IamError::NotAllowed {
				actor: "anonymous".to_string(),
				action: "compute".to_string(),
				resource: "value".to_string(),
			})
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
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars)?;
		}
		let txn_type = if val.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		// Start a new transaction
		let txn = self.transaction(txn_type, Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let res =
			stack.enter(|stk| val.compute(stk, &ctx, &opt, None)).finish().await.catch_return();
		// Store any data
		if res.is_ok() && matches!(txn_type, TransactionType::Read) {
			// If the compute was successful, then commit if writeable
			txn.commit().await?
		} else {
			// Cancel if the compute was an error, or if readonly
			txn.cancel().await?
		};
		// Return result
		res
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a record access SIGNUP or
	/// SIGNIN clause, which still needs to work without guest access.
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn evaluate(
		&self,
		val: &Expr,
		sess: &Session,
		vars: Option<Variables>,
	) -> Result<Value> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
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
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars)?;
		}
		let txn_type = if val.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		// Start a new transaction
		let txn = self.transaction(txn_type, Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let res =
			stack.enter(|stk| val.compute(stk, &ctx, &opt, None)).finish().await.catch_return();
		// Store any data
		if res.is_ok() && txn_type == TransactionType::Write {
			// If the compute was successful, then commit if writeable
			txn.commit().await?;
		} else {
			// Cancel if the compute was an error, or if readonly
			txn.cancel().await?;
		};
		// Return result
		res
	}

	/// Subscribe to live notifications
	///
	/// ```rust,no_run
	/// use surrealdb_core::kvs::Datastore;
	/// use surrealdb_core::dbs::Session;
	/// use anyhow::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(),Error> {
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
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<Response>> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Execute the SQL import
		self.execute(sql, sess, None).await
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn import_stream<S>(&self, sess: &Session, stream: S) -> Result<Vec<Response>>
	where
		S: Stream<Item = Result<Bytes>>,
	{
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Execute the SQL import
		self.execute_import(sess, None, stream).await
	}

	/// Performs a full database export as SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn export(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<()>>> {
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
	) -> Result<impl Future<Output = Result<()>> + use<>> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Retrieve the provided NS and DB
		let (ns, db) = crate::iam::check::check_ns_db(sess)?;
		// Create a new readonly transaction
		let txn = self.transaction(Read, Optimistic).await?;
		// Return an async export job
		Ok(async move {
			// Process the export
			txn.export(&ns, &db, cfg, chn).await?;
			// Everything ok
			Ok(())
		})
	}

	/// Checks the required permissions level for this session
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self, sess))]
	pub fn check(&self, sess: &Session, action: Action, resource: Resource) -> Result<()> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
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

	pub fn setup_ctx(&self) -> Result<MutableContext> {
		let mut ctx = MutableContext::from_ds(
			self.query_timeout,
			self.slow_log_threshold,
			self.capabilities.clone(),
			self.index_stores.clone(),
			#[cfg(not(target_family = "wasm"))]
			self.index_builder.clone(),
			self.sequences.clone(),
			self.cache.clone(),
			#[cfg(storage)]
			self.temporary_directory.clone(),
			self.buckets.clone(),
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
	pub async fn very_deep_query() -> Result<()> {
		use reblessive::{Stack, Stk};

		use crate::expr::{BinaryOperator, Expr, Literal};
		use crate::kvs::Datastore;
		use crate::val::{Number, Value};

		// build query manually to bypass query limits.
		let mut stack = Stack::new();
		async fn build_query(stk: &mut Stk, depth: usize) -> Expr {
			if depth == 0 {
				Expr::Binary {
					left: Box::new(Expr::Literal(Literal::Integer(1))),
					op: BinaryOperator::Add,
					right: Box::new(Expr::Literal(Literal::Integer(1))),
				}
			} else {
				let q = stk.run(|stk| build_query(stk, depth - 1)).await;
				Expr::Binary {
					left: Box::new(q),
					op: BinaryOperator::Add,
					right: Box::new(Expr::Literal(Literal::Integer(1))),
				}
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
			.with_max_computation_depth(u32::MAX);

		// Create a default context
		let mut ctx = MutableContext::default();
		// Set context capabilities
		ctx.add_capabilities(dbs.capabilities.clone());
		// Start a new transaction
		let txn = dbs.transaction(TransactionType::Read, Optimistic).await?;
		// Store the transaction
		ctx.set_transaction(txn.enclose());
		// Freeze the context
		let ctx = ctx.freeze();
		// Compute the value
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| val.compute(stk, &ctx, &opt, None))
			.finish()
			.await
			.catch_return()
			.unwrap();
		assert_eq!(res, Value::Number(Number::Int(1002)));
		Ok(())
	}
}
