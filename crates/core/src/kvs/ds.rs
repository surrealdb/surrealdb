use std::collections::BTreeMap;
use std::fmt::{self, Display};
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
use anyhow::{Context, Result, ensure};
use async_channel::{Receiver, Sender};
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use futures::{Future, Stream};
use http::HeaderMap;
use reblessive::TreeStack;
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tracing::{instrument, trace};
use uuid::Uuid;
#[cfg(target_family = "wasm")]
use wasmtimer::std::{SystemTime, UNIX_EPOCH};

use super::api::Transactable;
use super::export;
use super::tr::Transactor;
use super::tx::Transaction;
use super::version::MajorVersion;
use crate::api::body::ApiBody;
use crate::api::invocation::ApiInvocation;
use crate::api::response::{ApiResponse, ResponseInstruction};
use crate::buc::BucketConnections;
use crate::catalog::providers::{
	ApiProvider, CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider, UserProvider,
};
use crate::catalog::{ApiDefinition, ApiMethod, Index};
use crate::ctx::MutableContext;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::capabilities::{
	ArbitraryQueryTarget, ExperimentalTarget, MethodTarget, RouteTarget,
};
use crate::dbs::node::Timestamp;
use crate::dbs::{Capabilities, Executor, Options, QueryResult, QueryResultBuilder, Session};
use crate::err::Error;
use crate::expr::model::get_model_path;
use crate::expr::statements::{DefineModelStatement, DefineStatement, DefineUserStatement};
use crate::expr::{Base, Expr, FlowResultExt as _, Literal, LogicalPlan, TopLevelExpr};
#[cfg(feature = "jwks")]
use crate::iam::jwks::JwksCache;
use crate::iam::{Action, Auth, Error as IamError, Resource, ResourceKind, Role};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::index::IndexOperation;
use crate::idx::trees::store::IndexStores;
use crate::key::root::ic::IndexCompactionKey;
use crate::kvs::LockType::*;
use crate::kvs::TransactionType::*;
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::clock::SizedClock;
#[expect(unused_imports)]
use crate::kvs::clock::SystemClock;
use crate::kvs::ds::requirements::{
	TransactionBuilderFactoryRequirements, TransactionBuilderRequirements,
};
use crate::kvs::index::IndexBuilder;
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::{LockType, TransactionType};
use crate::rpc::DbResultError;
use crate::sql::Ast;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCache;
use crate::syn::parser::{ParserSettings, StatementStream};
use crate::types::{PublicNotification, PublicValue, PublicVariables};
use crate::val::convert_value_to_public_value;
use crate::{CommunityComposer, syn};

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
	/// Whether authentication is enabled on this datastore.
	auth_enabled: bool,
	/// The maximum duration timeout for running multiple statements in a query.
	query_timeout: Option<Duration>,
	/// The slow log configuration determining when a query should be logged
	slow_log: Option<SlowLog>,
	/// The maximum duration timeout for running multiple statements in a
	/// transaction.
	transaction_timeout: Option<Duration>,
	/// The security and feature capabilities for this datastore.
	capabilities: Arc<Capabilities>,
	// Whether this datastore enables live query notifications to subscribers.
	notification_channel: Option<(Sender<PublicNotification>, Receiver<PublicNotification>)>,
	// The index store cache
	index_stores: IndexStores,
	// The cross transaction cache
	cache: Arc<DatastoreCache>,
	// The index asynchronous builder
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
	// The surrealism cache
	#[cfg(feature = "surrealism")]
	surrealism_cache: Arc<SurrealismCache>,
}

#[derive(Clone)]
pub(super) struct TransactionFactory {
	// Clock for tracking time. It is read-only and accessible to all transactions.
	clock: Arc<SizedClock>,
	// The inner datastore type
	builder: Arc<Box<dyn TransactionBuilder>>,
}

impl TransactionFactory {
	pub(super) fn new(clock: Arc<SizedClock>, builder: Box<dyn TransactionBuilder>) -> Self {
		Self {
			clock,
			builder: Arc::new(builder),
		}
	}

	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	pub async fn transaction(
		&self,
		write: TransactionType,
		lock: LockType,
		sequences: Sequences,
	) -> Result<Transaction> {
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
		let (inner, local) = self.builder.new_transaction(write, lock).await?;
		Ok(Transaction::new(
			local,
			sequences,
			Transactor {
				inner,
			},
		))
	}
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
/// Abstraction over storage backends for creating and managing transactions.
///
/// This trait allows decoupling `Datastore` from concrete KV engines (memory,
/// RocksDB, TiKV, SurrealKV, SurrealDS, etc.). Implementors translate the
/// generic transaction parameters into a backend-specific transaction and
/// report whether the transaction is considered "local" (used internally to
/// enable some optimizations).
///
/// This was introduced to make the server more composable/embeddable. External
/// crates can implement `TransactionBuilder` to plug in custom backends while
/// reusing the rest of SurrealDB.
pub trait TransactionBuilder: TransactionBuilderRequirements {
	/// Create a new backend transaction.
	///
	/// - `write`: whether the transaction is writable (Write vs Read)
	/// - `lock`: whether pessimistic locking is requested
	///
	/// Returns the backend transaction object and a flag indicating if the
	/// transaction is local to the process (true) or requires external resources
	/// (false).
	async fn new_transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<(Box<dyn Transactable>, bool)>;

	/// Perform any backend-specific shutdown/cleanup.
	async fn shutdown(&self) -> Result<()>;
}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
/// Factory that parses a datastore path and returns a concrete `TransactionBuilder`.
///
/// Implementations can decide how to interpret connection strings (e.g. "memory",
/// "rocksdb:...", "tikv:...") and which clock to use. This lets the CLI and
/// server be generic over different storage backends without hard-coding them.
///
/// The `path_valid` helper is used by the CLI to validate the path early and
/// provide better error messages before starting the runtime.
pub trait TransactionBuilderFactory: TransactionBuilderFactoryRequirements {
	/// Create a new transaction builder and the clock to use throughout the datastore.
	async fn new_transaction_builder(
		&self,
		path: &str,
		clock: Option<Arc<SizedClock>>,
	) -> Result<(Box<dyn TransactionBuilder>, Arc<SizedClock>)>;

	/// Validate a datastore path string.
	fn path_valid(v: &str) -> Result<String>;
}

pub mod requirements {
	use std::fmt::Display;

	#[cfg(target_family = "wasm")]
	pub trait TransactionBuilderRequirements: Display {}

	#[cfg(not(target_family = "wasm"))]
	pub trait TransactionBuilderRequirements: Display + Send + Sync + 'static {}

	#[cfg(target_family = "wasm")]
	pub trait TransactionBuilderFactoryRequirements {}

	#[cfg(not(target_family = "wasm"))]
	pub trait TransactionBuilderFactoryRequirements: Send + Sync + 'static {}
}

pub enum DatastoreFlavor {
	#[cfg(feature = "kv-mem")]
	Mem(super::mem::Datastore),
	#[cfg(feature = "kv-rocksdb")]
	RocksDB(super::rocksdb::Datastore),
	#[cfg(feature = "kv-indxdb")]
	IndxDB(super::indxdb::Datastore),
	#[cfg(feature = "kv-tikv")]
	TiKV(super::tikv::Datastore),
	#[cfg(feature = "kv-surrealkv")]
	SurrealKV(super::surrealkv::Datastore),
}

impl TransactionBuilderFactoryRequirements for CommunityComposer {}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl TransactionBuilderFactory for CommunityComposer {
	#[allow(unused_variables)]
	async fn new_transaction_builder(
		&self,
		path: &str,
		clock: Option<Arc<SizedClock>>,
	) -> Result<(Box<dyn TransactionBuilder>, Arc<SizedClock>)> {
		let (flavour, path) = match path.split_once("://").or_else(|| path.split_once(':')) {
			None if path == "memory" => ("memory", ""),
			Some((flavour, path)) => (flavour, path),
			// Validated already in the CLI, should never happen
			_ => bail!(Error::Unreachable("Provide a valid database path parameter".to_owned())),
		};

		let path = if path.starts_with("/") {
			// if absolute, remove all slashes except one
			let normalised = format!("/{}", path.trim_start_matches("/"));
			info!(target: TARGET, "Starting kvs store at absolute path {flavour}:{normalised}");
			normalised
		} else if path.is_empty() {
			info!(target: TARGET, "Starting kvs store in memory");
			"".to_string()
		} else {
			info!(target: TARGET, "Starting kvs store at relative path {flavour}://{path}");
			path.to_string()
		};
		// Initiate the desired datastore
		match (flavour, path) {
			// Initiate an in-memory datastore
			(flavour @ "memory", _) => {
				#[cfg(feature = "kv-mem")]
				{
					// Initialise the storage engine
					let v = super::mem::Datastore::new().await.map(DatastoreFlavor::Mem)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started kvs store in {flavour}");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-mem"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `memory` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate a File (RocksDB) datastore
			(flavour @ "file", path) => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
					warn!(
						"file:// is deprecated, please use surrealkv:// or surrealkv+versioned:// or rocksdb://"
					);

					let v = super::rocksdb::Datastore::new(&path)
						.await
						.map(DatastoreFlavor::RocksDB)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate a RocksDB datastore
			(flavour @ "rocksdb", path) => {
				#[cfg(feature = "kv-rocksdb")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
					let v = super::rocksdb::Datastore::new(&path)
						.await
						.map(DatastoreFlavor::RocksDB)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-rocksdb"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `rocksdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate a SurrealKV versioned database
			(flavour @ "surrealkv+versioned", path) => {
				#[cfg(feature = "kv-surrealkv")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
					let v = super::surrealkv::Datastore::new(&path, true)
						.await
						.map(DatastoreFlavor::SurrealKV)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store with versions enabled");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-surrealkv"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate a SurrealKV non-versioned database
			(flavour @ "surrealkv", path) => {
				#[cfg(feature = "kv-surrealkv")]
				{
					// Create a new blocking threadpool
					super::threadpool::initialise();
					// Initialise the storage engine
					let v = super::surrealkv::Datastore::new(&path, false)
						.await
						.map(DatastoreFlavor::SurrealKV)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store with versions not enabled");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-surrealkv"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `surrealkv` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate an IndxDB database
			(flavour @ "indxdb", path) => {
				#[cfg(feature = "kv-indxdb")]
				{
					let v =
						super::indxdb::Datastore::new(&path).await.map(DatastoreFlavor::IndxDB)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-indxdb"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `indxdb` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// Initiate a TiKV datastore
			(flavour @ "tikv", path) => {
				#[cfg(feature = "kv-tikv")]
				{
					let v = super::tikv::Datastore::new(&path).await.map(DatastoreFlavor::TiKV)?;
					let c = clock.unwrap_or_else(|| Arc::new(SizedClock::system()));
					info!(target: TARGET, "Started {flavour} kvs store");
					Ok((Box::<DatastoreFlavor>::new(v), c))
				}
				#[cfg(not(feature = "kv-tikv"))]
				bail!(Error::Kvs(crate::kvs::Error::Datastore("Cannot connect to the `tikv` storage engine as it is not enabled in this build of SurrealDB".to_owned())));
			}
			// The datastore path is not valid
			(flavour, path) => {
				info!(target: TARGET, "Unable to load the specified datastore {flavour}{path}");
				bail!(Error::Kvs(crate::kvs::Error::Datastore(
					"Unable to load the specified datastore".into()
				)))
			}
		}
	}

	fn path_valid(v: &str) -> Result<String> {
		match v {
			"memory" => Ok(v.to_string()),
			v if v.starts_with("file:") => Ok(v.to_string()),
			v if v.starts_with("rocksdb:") => Ok(v.to_string()),
			v if v.starts_with("surrealkv:") => Ok(v.to_string()),
			v if v.starts_with("surrealkv+versioned:") => Ok(v.to_string()),
			v if v.starts_with("tikv:") => Ok(v.to_string()),
			_ => bail!("Provide a valid database path parameter"),
		}
	}
}

impl TransactionBuilderRequirements for DatastoreFlavor {}

#[cfg_attr(target_family = "wasm", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait::async_trait)]
impl TransactionBuilder for DatastoreFlavor {
	#[allow(
		unreachable_code,
		unreachable_patterns,
		unused_variables,
		reason = "Some variables are unused when no backends are enabled."
	)]
	async fn new_transaction(
		&self,
		write: bool,
		lock: bool,
	) -> Result<(Box<dyn Transactable>, bool)> {
		//-> Pin<Box<dyn Future<Output = Result<(Box<dyn api::Transaction>, bool)>> + Send + 'a>> {
		//Box::pin(async move {
		Ok(match self {
			#[cfg(feature = "kv-mem")]
			Self::Mem(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-rocksdb")]
			Self::RocksDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-indxdb")]
			Self::IndxDB(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			#[cfg(feature = "kv-tikv")]
			Self::TiKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, false)
			}
			#[cfg(feature = "kv-surrealkv")]
			Self::SurrealKV(v) => {
				let tx = v.transaction(write, lock).await?;
				(tx, true)
			}
			_ => unreachable!(),
		})
	}

	async fn shutdown(&self) -> Result<()> {
		match self {
			#[cfg(feature = "kv-mem")]
			Self::Mem(v) => Ok(v.shutdown().await?),
			#[cfg(feature = "kv-rocksdb")]
			Self::RocksDB(v) => Ok(v.shutdown().await?),
			#[cfg(feature = "kv-indxdb")]
			Self::IndxDB(v) => Ok(v.shutdown().await?),
			#[cfg(feature = "kv-tikv")]
			Self::TiKV(v) => Ok(v.shutdown().await?),
			#[cfg(feature = "kv-surrealkv")]
			Self::SurrealKV(v) => Ok(v.shutdown().await?),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Display for DatastoreFlavor {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		#![allow(unused_variables)]
		match self {
			#[cfg(feature = "kv-mem")]
			Self::Mem(_) => write!(f, "memory"),
			#[cfg(feature = "kv-rocksdb")]
			Self::RocksDB(_) => write!(f, "rocksdb"),
			#[cfg(feature = "kv-indxdb")]
			Self::IndxDB(_) => write!(f, "indxdb"),
			#[cfg(feature = "kv-tikv")]
			Self::TiKV(_) => write!(f, "tikv"),
			#[cfg(feature = "kv-surrealkv")]
			Self::SurrealKV(_) => write!(f, "surrealkv"),
			#[allow(unreachable_patterns)]
			_ => unreachable!(),
		}
	}
}

impl Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.transaction_factory.builder.fmt(f)
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
		Self::new_with_factory(&CommunityComposer(), path).await
	}

	/// Creates a new datastore instance with a custom transaction builder factory.
	///
	/// This allows embedders to provide their own factory implementation for custom
	/// backend selection or configuration.
	///
	/// # Parameters
	/// - `factory`: Transaction builder factory for backend selection
	/// - `path`: Database path (e.g., "memory", "surrealkv://path", "tikv://host:port")
	///
	/// # Generic parameters
	/// - `F`: Transaction builder factory type implementing `TransactionBuilderFactory`
	pub async fn new_with_factory<F: TransactionBuilderFactory>(
		factory: &F,
		path: &str,
	) -> Result<Self> {
		Self::new_with_clock::<F>(factory, path, None).await
	}

	/// Creates a new datastore instance with a custom factory and clock.
	///
	/// This is the most flexible constructor, allowing full control over both
	/// the backend and the clock used for timestamps.
	///
	/// # Parameters
	/// - `factory`: Transaction builder factory for backend selection
	/// - `path`: Database path (e.g., "memory", "surrealkv://path", "tikv://host:port")
	/// - `clock`: Optional custom clock for timestamp generation (uses system clock if None)
	///
	/// # Generic parameters
	/// - `F`: Transaction builder factory type implementing `TransactionBuilderFactory`
	pub async fn new_with_clock<F: TransactionBuilderFactory>(
		factory: &F,
		path: &str,
		clock: Option<Arc<SizedClock>>,
	) -> Result<Datastore> {
		// Initiate the desired datastore
		let (builder, clock) = factory.new_transaction_builder(path, clock).await?;
		// Set the properties on the datastore
		Self::new_with_builder(builder, clock)
	}

	pub fn new_with_builder(
		builder: Box<dyn TransactionBuilder>,
		clock: Arc<SizedClock>,
	) -> Result<Self> {
		let tf = TransactionFactory::new(clock, builder);
		let id = Uuid::new_v4();
		Ok(Self {
			id,
			transaction_factory: tf.clone(),
			auth_enabled: false,
			query_timeout: None,
			slow_log: None,
			transaction_timeout: None,
			notification_channel: None,
			capabilities: Arc::new(Capabilities::default()),
			index_stores: IndexStores::default(),
			index_builder: IndexBuilder::new(tf.clone()),
			#[cfg(feature = "jwks")]
			jwks_cache: Arc::new(RwLock::new(JwksCache::new())),
			#[cfg(storage)]
			temporary_directory: None,
			cache: Arc::new(DatastoreCache::new()),
			buckets: Arc::new(DashMap::new()),
			sequences: Sequences::new(tf, id),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new()),
		})
	}

	/// Create a new datastore with the same persistent data (inner), with
	/// flushed cache. Simulating a server restart
	pub fn restart(self) -> Self {
		Self {
			id: self.id,
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
			cache: Arc::new(DatastoreCache::new()),
			buckets: Arc::new(DashMap::new()),
			sequences: Sequences::new(self.transaction_factory.clone(), self.id),
			transaction_factory: self.transaction_factory,
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new()),
		}
	}

	/// Set the node id for this datastore.
	pub fn with_node_id(mut self, id: Uuid) -> Self {
		self.id = id;
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
	#[cfg(test)]
	pub(crate) fn get_cache(&self) -> Arc<DatastoreCache> {
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
		let txn = self.transaction(Write, Optimistic).await?.enclose();
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
				user.to_owned(),
				pass,
				INITIAL_USER_ROLE.to_owned(),
			);
			let opt = Options::new().with_auth(Arc::new(Auth::for_root(Role::Owner)));
			let mut ctx = MutableContext::default();
			ctx.set_transaction(txn.clone());
			let ctx = ctx.freeze();
			let mut stack = reblessive::TreeStack::new();
			let res = stack.enter(|stk| stm.compute(stk, &ctx, &opt, None)).finish().await;
			catch!(txn, res);
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
			self.sequences.clone(),
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
			self.sequences.clone(),
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
			let (beg, end) = IndexCompactionKey::range();
			let range = beg..end;
			let mut previous: Option<IndexCompactionKey<'static>> = None;
			let mut count = 0;
			// Returns an ordered list of indexes that require compaction
			for (k, _) in txn.getr(range.clone(), None).await? {
				count += 1;
				lh.try_maintain_lease().await?;
				let ic = IndexCompactionKey::decode_key(&k)?;
				// If the index has already been compacted, we can ignore the task
				if let Some(p) = &previous {
					if p.index_matches(&ic) {
						continue;
					}
				}
				match txn.get_tb_index_by_id(ic.ns, ic.db, ic.tb.as_ref(), ic.ix).await? {
					Some(ix) => match &ix.index {
						Index::FullText(p) => {
							let ft = FullTextIndex::new(
								&self.index_stores,
								&txn,
								IndexKeyBase::new(ic.ns, ic.db, &ix.table_name, ix.index_id),
								p,
							)
							.await?;
							ft.compaction(&txn).await?;
						}
						Index::Count(_) => {
							IndexOperation::index_count_compaction(&ic, &txn).await?;
						}
						_ => {
							trace!(target: TARGET, "Index compaction: Index {:?} does not support compaction, skipping", ic.ix);
						}
					},
					None => {
						trace!(target: TARGET, "Index compaction: Index {:?} not found, skipping", ic.ix);
					}
				}
				previous = Some(ic.into_owned());
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
	pub async fn startup(&self, sql: &str, sess: &Session) -> Result<Vec<QueryResult>> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore startup import script");
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Execute the SQL import
		self.execute(sql, sess, None).await.map_err(|e| anyhow::anyhow!(e))
	}

	/// Run the datastore shutdown tasks, performing any necessary cleanup
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn shutdown(&self) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore shutdown operations");
		// Delete this datastore from the cluster
		self.delete_node(self.id).await?;
		// Run any storag engine shutdown tasks
		self.transaction_factory.builder.shutdown().await
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
		self.transaction_factory.transaction(write, lock, self.sequences.clone()).await
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
		vars: Option<PublicVariables>,
	) -> std::result::Result<Vec<QueryResult>, DbResultError> {
		// Parse the SQL query text
		let ast = syn::parse_with_capabilities(txt, &self.capabilities)
			.map_err(|e| DbResultError::ParseError(e.to_string()))?;
		// Process the AST
		self.process(ast, sess, vars).await
	}

	/// Execute a query with an existing transaction
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn execute_with_transaction(
		&self,
		txt: &str,
		sess: &Session,
		vars: Option<PublicVariables>,
		tx: Arc<Transaction>,
	) -> std::result::Result<Vec<QueryResult>, DbResultError> {
		// Parse the SQL query text
		let ast = syn::parse_with_capabilities(txt, &self.capabilities)
			.map_err(|e| DbResultError::ParseError(e.to_string()))?;
		// Process the AST with the transaction
		self.process_with_transaction(ast, sess, vars, tx).await
	}

	/// Process an AST with an existing transaction
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn process_with_transaction(
		&self,
		ast: Ast,
		sess: &Session,
		vars: Option<PublicVariables>,
		tx: Arc<Transaction>,
	) -> std::result::Result<Vec<QueryResult>, DbResultError> {
		// Check if the session has expired
		if sess.expired() {
			return Err(DbResultError::InvalidAuth("The session has expired".to_string()));
		}

		// Check if anonymous actors can execute queries when auth is enabled
		if let Err(e) = self.check_anon(sess) {
			return Err(DbResultError::InvalidAuth(format!("Anonymous access not allowed: {}", e)));
		}

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx().map_err(|e| match e.downcast_ref::<Error>() {
			Some(Error::ExpiredSession) => {
				DbResultError::InvalidAuth("The session has expired".to_string())
			}
			_ => DbResultError::InternalError(e.to_string()),
		})?;

		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars.into()).map_err(|e| match e {
				Error::InvalidParam {
					..
				} => DbResultError::InvalidParams("Invalid query variables".to_string()),
				_ => DbResultError::InternalError(e.to_string()),
			})?;
		}

		// Set the transaction in the context
		ctx.set_transaction(tx);

		// Process all statements with the transaction
		Executor::execute_plan_with_transaction(ctx.freeze(), opt, ast.into()).await.map_err(|e| {
			match e.downcast_ref::<Error>() {
				Some(Error::ExpiredSession) => {
					DbResultError::InvalidAuth("The session has expired".to_string())
				}
				_ => DbResultError::InternalError(e.to_string()),
			}
		})
	}

	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn execute_import<S>(
		&self,
		sess: &Session,
		vars: Option<PublicVariables>,
		query: S,
	) -> Result<Vec<QueryResult>>
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
			ctx.attach_variables(vars.into())?;
		}
		// Process all statements

		let parser_settings = ParserSettings {
			define_api_enabled: ctx
				.get_capabilities()
				.allows_experimental(&ExperimentalTarget::DefineApi),
			files_enabled: ctx.get_capabilities().allows_experimental(&ExperimentalTarget::Files),
			surrealism_enabled: ctx
				.get_capabilities()
				.allows_experimental(&ExperimentalTarget::Surrealism),
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
		vars: Option<PublicVariables>,
	) -> std::result::Result<Vec<QueryResult>, DbResultError> {
		//TODO: Insert planner here.
		self.process_plan(ast.into(), sess, vars).await
	}

	pub(crate) async fn process_plan(
		&self,
		plan: LogicalPlan,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> Result<Vec<QueryResult>, DbResultError> {
		// Check if the session has expired
		if sess.expired() {
			return Err(DbResultError::InvalidAuth("The session has expired".to_string()));
		}

		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		if let Err(e) = self.check_anon(sess) {
			return Err(DbResultError::InvalidAuth(format!("Anonymous access not allowed: {}", e)));
		}

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context
		let mut ctx = self.setup_ctx().map_err(|e| match e.downcast_ref::<Error>() {
			Some(Error::ExpiredSession) => {
				DbResultError::InvalidAuth("The session has expired".to_string())
			}
			Some(Error::InvalidAuth) => {
				DbResultError::InvalidAuth("Authentication failed".to_string())
			}
			Some(Error::UnexpectedAuth) => {
				DbResultError::InvalidAuth("Unexpected authentication error".to_string())
			}
			Some(Error::MissingUserOrPass) => {
				DbResultError::InvalidAuth("Missing username or password".to_string())
			}
			Some(Error::InvalidPass) => DbResultError::InvalidAuth("Invalid password".to_string()),
			Some(Error::NoSigninTarget) => {
				DbResultError::InvalidAuth("No signin target specified".to_string())
			}
			Some(Error::TokenMakingFailed) => {
				DbResultError::InvalidAuth("Failed to create authentication token".to_string())
			}
			Some(Error::IamError(iam_err)) => {
				DbResultError::InvalidAuth(format!("IAM error: {}", iam_err))
			}
			Some(Error::Kvs(kvs_err)) => {
				DbResultError::InternalError(format!("Key-value store error: {}", kvs_err))
			}
			Some(Error::InvalidQuery(_)) => {
				DbResultError::ParseError("Invalid query syntax".to_string())
			}
			Some(Error::Internal(msg)) => DbResultError::InternalError(msg.clone()),
			Some(Error::Unimplemented(msg)) => {
				DbResultError::InternalError(format!("Unimplemented: {}", msg))
			}
			Some(Error::Io(e)) => DbResultError::InternalError(format!("I/O error: {}", e)),
			Some(Error::Http(msg)) => DbResultError::InternalError(format!("HTTP error: {}", msg)),
			Some(Error::Channel(msg)) => {
				DbResultError::InternalError(format!("Channel error: {}", msg))
			}
			Some(Error::QueryTimedout) => DbResultError::QueryTimedout,
			Some(Error::QueryCancelled) => DbResultError::QueryCancelled,
			Some(Error::QueryNotExecuted {
				message,
			}) => DbResultError::QueryNotExecuted(message.clone()),
			Some(Error::ScriptingNotAllowed) => {
				DbResultError::MethodNotAllowed("Scripting functions are not allowed".to_string())
			}
			Some(Error::FunctionNotAllowed(func)) => {
				DbResultError::MethodNotAllowed(format!("Function '{}' is not allowed", func))
			}
			Some(Error::NetTargetNotAllowed(target)) => DbResultError::MethodNotAllowed(format!(
				"Network target '{}' is not allowed",
				target
			)),
			Some(Error::Thrown(msg)) => DbResultError::Thrown(msg.clone()),
			_ => DbResultError::InternalError(e.to_string()),
		})?;

		// Start an execution context
		ctx.attach_session(sess).map_err(|e| match e {
			Error::ExpiredSession => {
				DbResultError::InvalidAuth("The session has expired".to_string())
			}
			Error::InvalidAuth => DbResultError::InvalidAuth("Authentication failed".to_string()),
			Error::UnexpectedAuth => {
				DbResultError::InvalidAuth("Unexpected authentication error".to_string())
			}
			Error::IamError(iam_err) => {
				DbResultError::InvalidAuth(format!("IAM error: {}", iam_err))
			}
			_ => DbResultError::InternalError(e.to_string()),
		})?;

		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars.into()).map_err(|e| match e {
				Error::InvalidParam {
					..
				} => DbResultError::InvalidParams("Invalid query variables".to_string()),
				Error::Internal(msg) => DbResultError::InternalError(msg),
				_ => DbResultError::InternalError(e.to_string()),
			})?;
		}

		// Process all statements
		Executor::execute_plan(self, ctx.freeze(), opt, plan).await.map_err(|e| {
			match e.downcast_ref::<Error>() {
				Some(Error::ExpiredSession) => {
					DbResultError::InvalidAuth("The session has expired".to_string())
				}
				Some(Error::InvalidAuth) => {
					DbResultError::InvalidAuth("Authentication failed".to_string())
				}
				Some(Error::UnexpectedAuth) => {
					DbResultError::InvalidAuth("Unexpected authentication error".to_string())
				}
				Some(Error::MissingUserOrPass) => {
					DbResultError::InvalidAuth("Missing username or password".to_string())
				}
				Some(Error::InvalidPass) => {
					DbResultError::InvalidAuth("Invalid password".to_string())
				}
				Some(Error::NoSigninTarget) => {
					DbResultError::InvalidAuth("No signin target specified".to_string())
				}
				Some(Error::TokenMakingFailed) => {
					DbResultError::InvalidAuth("Failed to create authentication token".to_string())
				}
				Some(Error::IamError(iam_err)) => {
					DbResultError::InvalidAuth(format!("IAM error: {}", iam_err))
				}
				Some(Error::Kvs(kvs_err)) => {
					DbResultError::InternalError(format!("Key-value store error: {}", kvs_err))
				}
				Some(Error::NsEmpty) => {
					DbResultError::InvalidParams("No namespace specified".to_string())
				}
				Some(Error::DbEmpty) => {
					DbResultError::InvalidParams("No database specified".to_string())
				}
				Some(Error::InvalidQuery(_)) => {
					DbResultError::ParseError("Invalid query syntax".to_string())
				}
				Some(Error::InvalidContent {
					..
				}) => DbResultError::InvalidParams("Invalid content clause".to_string()),
				Some(Error::InvalidMerge {
					..
				}) => DbResultError::InvalidParams("Invalid merge clause".to_string()),
				Some(Error::InvalidPatch(_)) => {
					DbResultError::InvalidParams("Invalid patch operation".to_string())
				}
				Some(Error::Internal(msg)) => DbResultError::InternalError(msg.clone()),
				Some(Error::Unimplemented(msg)) => {
					DbResultError::InternalError(format!("Unimplemented: {}", msg))
				}
				Some(Error::Io(e)) => DbResultError::InternalError(format!("I/O error: {}", e)),
				Some(Error::Http(msg)) => {
					DbResultError::InternalError(format!("HTTP error: {}", msg))
				}
				Some(Error::Channel(msg)) => {
					DbResultError::InternalError(format!("Channel error: {}", msg))
				}
				Some(Error::QueryTimedout) => DbResultError::QueryTimedout,
				Some(Error::QueryCancelled) => DbResultError::QueryCancelled,
				Some(Error::QueryNotExecuted {
					message,
				}) => DbResultError::QueryNotExecuted(message.clone()),
				Some(Error::ScriptingNotAllowed) => DbResultError::MethodNotAllowed(
					"Scripting functions are not allowed".to_string(),
				),
				Some(Error::FunctionNotAllowed(func)) => {
					DbResultError::MethodNotAllowed(format!("Function '{}' is not allowed", func))
				}
				Some(Error::NetTargetNotAllowed(target)) => DbResultError::MethodNotAllowed(
					format!("Network target '{}' is not allowed", target),
				),
				Some(Error::Thrown(msg)) => DbResultError::Thrown(msg.clone()),
				Some(Error::Coerce(_)) => {
					DbResultError::InvalidParams("Type coercion error".to_string())
				}
				Some(Error::Cast(_)) => {
					DbResultError::InvalidParams("Type casting error".to_string())
				}
				Some(Error::TryAdd(_, _))
				| Some(Error::TrySub(_, _))
				| Some(Error::TryMul(_, _))
				| Some(Error::TryDiv(_, _))
				| Some(Error::TryRem(_, _))
				| Some(Error::TryPow(_, _))
				| Some(Error::TryNeg(_)) => {
					DbResultError::InvalidParams("Arithmetic operation error".to_string())
				}
				Some(Error::TryFrom(_, _)) => {
					DbResultError::InvalidParams("Type conversion error".to_string())
				}
				Some(Error::Unencodable) => {
					DbResultError::SerializationError("Value cannot be serialized".to_string())
				}
				Some(Error::Decode(_)) => {
					DbResultError::DeserializationError("Key decoding error".to_string())
				}
				Some(Error::Revision(_)) => {
					DbResultError::DeserializationError("Versioned data error".to_string())
				}
				Some(Error::CorruptedIndex(_)) => {
					DbResultError::InternalError("Index corruption detected".to_string())
				}
				Some(Error::NoIndexFoundForMatch {
					..
				}) => DbResultError::InternalError("No suitable index found".to_string()),
				Some(Error::AnalyzerError(msg)) => {
					DbResultError::InternalError(format!("Analyzer error: {}", msg))
				}
				Some(Error::HighlightError(msg)) => {
					DbResultError::InternalError(format!("Highlight error: {}", msg))
				}
				Some(Error::Bincode(_)) => {
					DbResultError::SerializationError("Bincode serialization error".to_string())
				}
				Some(Error::FstError(_)) => DbResultError::InternalError("FST error".to_string()),
				Some(Error::Utf8Error(_)) => {
					DbResultError::DeserializationError("UTF-8 decoding error".to_string())
				}
				Some(Error::ObsError(_)) => {
					DbResultError::InternalError("Object store error".to_string())
				}
				Some(Error::DuplicatedMatchRef {
					..
				}) => DbResultError::InvalidParams("Duplicated match reference".to_string()),
				Some(Error::TimestampOverflow(msg)) => {
					DbResultError::InternalError(format!("Timestamp overflow: {}", msg))
				}
				Some(Error::CorruptedVersionstampInKey(_)) => {
					DbResultError::InternalError("Corrupted versionstamp in key".to_string())
				}
				Some(Error::NoRecordFound) => {
					DbResultError::InternalError("No record found".to_string())
				}
				Some(Error::InvalidSignup) => {
					DbResultError::InvalidAuth("Signup failed".to_string())
				}
				Some(Error::ClAlreadyExists {
					..
				}) => DbResultError::InternalError("Cluster node already exists".to_string()),
				Some(Error::ApAlreadyExists {
					..
				}) => DbResultError::InternalError("API already exists".to_string()),
				Some(Error::AzAlreadyExists {
					..
				}) => DbResultError::InternalError("Analyzer already exists".to_string()),
				Some(Error::BuAlreadyExists {
					..
				}) => DbResultError::InternalError("Bucket already exists".to_string()),
				Some(Error::DbAlreadyExists {
					..
				}) => DbResultError::InternalError("Database already exists".to_string()),
				_ => DbResultError::InternalError(e.to_string()),
			}
		})
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a record access SIGNUP or
	/// SIGNIN clause, which still needs to work without guest access.
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub(crate) async fn evaluate(
		&self,
		val: &Expr,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> Result<PublicValue> {
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

		let txn_type = if val.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		// Start a new transaction
		let txn = self.transaction(txn_type, Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());

		// Start an execution context
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_public_variables(vars)?;
		}

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
		convert_value_to_public_value(res?)
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
	///     	    println!("Received notification: {v:?}");
	///     	}
	/// 	}
	///     Ok(())
	/// }
	/// ```
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub fn notifications(&self) -> Option<Receiver<PublicNotification>> {
		self.notification_channel.as_ref().map(|v| v.1.clone())
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<QueryResult>> {
		// Check if the session has expired
		ensure!(!sess.expired(), Error::ExpiredSession);
		// Execute the SQL import
		self.execute(sql, sess, None).await.map_err(|e| anyhow::anyhow!(e))
	}

	/// Performs a database import from SQL
	#[instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn import_stream<S>(&self, sess: &Session, stream: S) -> Result<Vec<QueryResult>>
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
			.with_auth_enabled(self.auth_enabled)
	}

	pub fn setup_ctx(&self) -> Result<MutableContext> {
		let mut ctx = MutableContext::from_ds(
			self.query_timeout,
			self.slow_log.clone(),
			self.capabilities.clone(),
			self.index_stores.clone(),
			self.index_builder.clone(),
			self.sequences.clone(),
			self.cache.clone(),
			#[cfg(storage)]
			self.temporary_directory.clone(),
			self.buckets.clone(),
			#[cfg(feature = "surrealism")]
			self.surrealism_cache.clone(),
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

	pub async fn process_use(
		&self,
		ctx: Option<&MutableContext>,
		session: &mut Session,
		namespace: Option<String>,
		database: Option<String>,
	) -> std::result::Result<QueryResult, DbResultError> {
		let new_tx = || async {
			self.transaction(Write, Optimistic)
				.await
				.map_err(|err| DbResultError::InternalError(err.to_string()))
		};
		let commit_tx = |txn: Transaction| async move {
			txn.commit().await.map_err(|err| DbResultError::InternalError(err.to_string()))
		};

		let query_result = QueryResultBuilder::started_now();
		match (namespace, database) {
			(Some(ns), Some(db)) => {
				let tx = new_tx().await?;
				tx.ensure_ns_db(ctx, &ns, &db)
					.await
					.map_err(|err| DbResultError::InternalError(err.to_string()))?;
				commit_tx(tx).await?;
				session.ns = Some(ns);
				session.db = Some(db);
			}
			(Some(ns), None) => {
				let tx = new_tx().await?;
				tx.get_or_add_ns(ctx, &ns)
					.await
					.map_err(|err| DbResultError::InternalError(err.to_string()))?;
				commit_tx(tx).await?;
				session.ns = Some(ns);
			}
			(None, Some(db)) => {
				let Some(ns) = session.ns.clone() else {
					return Err(DbResultError::InvalidRequest(
						"Cannot use database without namespace".to_string(),
					));
				};
				let tx = new_tx().await?;
				tx.ensure_ns_db(ctx, &ns, &db)
					.await
					.map_err(|err| DbResultError::InternalError(err.to_string()))?;
				commit_tx(tx).await?;
				session.db = Some(db);
			}
			(None, None) => {
				session.ns = None;
				session.db = None;
			}
		}

		Ok(query_result.finish())
	}

	/// Get a db model by name.
	///
	/// TODO: This should not be public, but it is used in `crates/sdk/src/api/engine/local/mod.rs`.
	pub async fn get_db_model(
		&self,
		ns: &str,
		db: &str,
		model_name: &str,
		model_version: &str,
	) -> Result<Option<Arc<crate::catalog::MlModelDefinition>>> {
		let tx = self.transaction(Read, Optimistic).await?;
		let db = tx.expect_db_by_name(ns, db).await?;
		let model =
			tx.get_db_model(db.namespace_id, db.database_id, model_name, model_version).await?;
		tx.cancel().await?;
		Ok(model)
	}

	/// Get a table by name.
	///
	/// TODO: This should not be public, but it is used in `src/net/key.rs`.
	pub async fn ensure_tb_exists(&self, ns: &str, db: &str, tb: &str) -> Result<()> {
		let tx = self.transaction(TransactionType::Read, LockType::Optimistic).await?;

		tx.expect_tb_by_name(ns, db, tb).await?;
		tx.cancel().await?;

		Ok(())
	}

	/// Invoke an API handler.
	///
	/// TODO: This should not need to be public, but it is used in `src/net/api.rs`.
	#[expect(clippy::too_many_arguments)]
	pub async fn invoke_api_handler<S>(
		&self,
		ns: &str,
		db: &str,
		path: &str,
		session: &Session,
		method: ApiMethod,
		headers: HeaderMap,
		query: BTreeMap<String, String>,
		body: S,
	) -> Result<Option<(ApiResponse, ResponseInstruction)>>
	where
		S: Stream<Item = std::result::Result<Bytes, Box<dyn Display + Send + Sync>>>
			+ Send
			+ Unpin
			+ 'static,
	{
		let tx = Arc::new(self.transaction(TransactionType::Write, LockType::Optimistic).await?);

		let db = tx.ensure_ns_db(None, ns, db).await?;

		let apis = tx.all_db_apis(db.namespace_id, db.database_id).await?;
		let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

		let res = match ApiDefinition::find_definition(apis.as_ref(), segments, method) {
			Some((api, params)) => {
				let invocation = ApiInvocation {
					params: params.try_into()?,
					method,
					headers,
					query,
				};

				let opt = self.setup_options(session);

				let mut ctx = self.setup_ctx()?;
				ctx.set_transaction(Arc::clone(&tx));
				ctx.attach_session(session)?;
				let ctx = &ctx.freeze();

				invocation.invoke_with_transaction(ctx, &opt, api, ApiBody::from_stream(body)).await
			}
			_ => {
				return Err(anyhow::anyhow!(Error::ApNotFound {
					value: path.to_owned(),
				}));
			}
		};

		// Handle committing or cancelling the transaction
		if res.is_ok() {
			tx.commit().await?;
		} else {
			tx.cancel().await?;
		}

		res
	}

	pub async fn put_ml_model(
		&self,
		session: &Session,
		name: &str,
		version: &str,
		description: &str,
		data: Vec<u8>,
	) -> Result<()> {
		let ns = session.ns.as_ref().context("Namespace is required")?;
		let db = session.db.as_ref().context("Database is required")?;

		self.check(session, Action::Edit, ResourceKind::Model.on_db(ns, db))?;

		// Calculate the hash of the model file
		let hash = crate::obs::hash(&data);
		// Calculate the path of the model file
		let path = get_model_path(ns, db, name, version, &hash);
		// Insert the file data in to the store
		crate::obs::put(&path, data).await?;
		// Insert the model in to the database
		let model = DefineModelStatement {
			name: name.to_string(),
			version: version.to_string(),
			comment: Some(Expr::Literal(Literal::String(description.to_string()))),
			hash,
			..Default::default()
		};

		let q = LogicalPlan {
			expressions: vec![TopLevelExpr::Expr(Expr::Define(Box::new(DefineStatement::Model(
				model,
			))))],
		};

		self.process_plan(q, session, None).await.map_err(|e| anyhow::anyhow!(e))?;

		Ok(())
	}
}

#[cfg(test)]
mod test {
	use super::*;
	use crate::iam::verify::verify_root_creds;
	use crate::types::{PublicValue, PublicVariables};

	#[tokio::test]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let username = "root";
		let password = "root";

		// Setup the initial user if there are no root users
		assert_eq!(
			ds.transaction(Read, Optimistic).await.unwrap().all_root_users().await.unwrap().len(),
			0
		);
		ds.initialise_credentials(username, password).await.unwrap();
		assert_eq!(
			ds.transaction(Read, Optimistic).await.unwrap().all_root_users().await.unwrap().len(),
			1
		);
		verify_root_creds(&ds, username, password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = ds
			.transaction(Read, Optimistic)
			.await
			.unwrap()
			.expect_root_user(username)
			.await
			.unwrap()
			.hash
			.clone();

		ds.initialise_credentials(username, password).await.unwrap();
		assert_eq!(
			pass_hash,
			ds.transaction(Read, Optimistic)
				.await
				.unwrap()
				.expect_root_user(username)
				.await
				.unwrap()
				.hash
				.clone()
		)
	}

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

	#[tokio::test]
	async fn cross_transaction_caching_uuids_updated() -> Result<()> {
		let ds = Datastore::new("memory")
			.await?
			.with_capabilities(Capabilities::all())
			.with_notifications();
		let cache = ds.get_cache();
		let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

		let db = {
			let txn = ds.transaction(TransactionType::Write, LockType::Pessimistic).await?;
			let db = txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
			db
		};

		// Define the table, set the initial uuids
		let (initial, initial_live_query_version) = {
			let sql = r"DEFINE TABLE test;".to_owned();
			let res = &mut ds.execute(&sql, &ses, None).await?;
			assert_eq!(res.len(), 1);
			res.remove(0).result.unwrap();
			// Obtain the initial uuids
			let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
			let initial = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
			let initial_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
			txn.cancel().await?;
			(initial, initial_live_query_version)
		};

		// Define some resources to refresh the UUIDs
		let lqid = {
			let sql = r"
		DEFINE FIELD test ON test;
		DEFINE EVENT test ON test WHEN {} THEN {};
		DEFINE TABLE view AS SELECT * FROM test;
		DEFINE INDEX test ON test FIELDS test;
		LIVE SELECT * FROM test;
	"
			.to_owned();
			let res = &mut ds.execute(&sql, &ses, None).await?;
			assert_eq!(res.len(), 5);
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			let lqid = res.remove(0).result?;
			assert!(matches!(lqid, PublicValue::Uuid(_)));
			lqid
		};

		// Obtain the uuids after definitions
		let (after_define, after_define_live_query_version) = {
			let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
			let after_define = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
			let after_define_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
			txn.cancel().await?;
			// Compare uuids after definitions
			assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
			assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
			assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
			assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
			assert_ne!(initial_live_query_version, after_define_live_query_version);
			(after_define, after_define_live_query_version)
		};

		// Remove the defined resources to refresh the UUIDs
		{
			let sql = r"
		REMOVE FIELD test ON test;
		REMOVE EVENT test ON test;
		REMOVE TABLE view;
		REMOVE INDEX test ON test;
		KILL $lqid;
	"
			.to_owned();
			let vars = PublicVariables::from(map! { "lqid".to_string() => lqid });
			let res = &mut ds.execute(&sql, &ses, Some(vars)).await?;
			assert_eq!(res.len(), 5);
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
			res.remove(0).result.unwrap();
		}
		// Obtain the uuids after definitions
		{
			let txn = ds.transaction(TransactionType::Read, LockType::Pessimistic).await?;
			let after_remove = txn.get_tb(db.namespace_id, db.database_id, "test").await?.unwrap();
			let after_remove_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, "test")?;
			drop(txn);
			// Compare uuids after definitions
			assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
			assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
			assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
			assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
			assert_ne!(after_define_live_query_version, after_remove_live_query_version);
		}
		//
		Ok(())
	}
}
