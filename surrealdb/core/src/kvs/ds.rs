use std::fmt::{self, Display};
#[cfg(storage)]
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Poll, ready};
use std::time::Duration;

#[allow(unused_imports)]
use anyhow::bail;
use anyhow::{Context as _, Result, ensure};
use async_channel::{Receiver, Sender};
use bytes::{Bytes, BytesMut};
use futures::{Future, Stream};
use reblessive::TreeStack;
use surrealdb_types::{SurrealValue, object};
use tokio::sync::Notify;
#[cfg(feature = "jwks")]
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, trace};
use uuid::Uuid;

use super::api::Transactable;
use super::export;
use super::tr::Transactor;
use super::tx::Transaction;
use super::version::MajorVersion;
use crate::api::err::ApiError;
use crate::api::invocation::process_api_request;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::buc::BucketStoreProvider;
use crate::buc::manager::BucketsManager;
use crate::catalog::providers::{
	ApiProvider, CatalogProvider, DatabaseProvider, NamespaceProvider, NodeProvider, TableProvider,
	UserProvider,
};
use crate::catalog::{ApiDefinition, Index, NodeLiveQuery, SubscriptionDefinition};
use crate::cnf::NORMAL_FETCH_SIZE;
use crate::cnf::dynamic::DynamicConfiguration;
use crate::ctx::Context;
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::capabilities::{
	ArbitraryQueryTarget, ExperimentalTarget, MethodTarget, RouteTarget,
};
use crate::dbs::node::{Node, Timestamp};
use crate::dbs::{Capabilities, Executor, Options, QueryResult, QueryResultBuilder, Session};
use crate::doc::AsyncEventRecord;
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
use crate::kvs::{KVValue, LockType, TransactionType};
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
	dynamic_configuration: DynamicConfiguration,
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
	buckets: BucketsManager,
	// The sequences
	sequences: Sequences,
	// The surrealism cache
	#[cfg(feature = "surrealism")]
	surrealism_cache: Arc<SurrealismCache>,
	// Async event processing trigger
	async_event_trigger: Arc<Notify>,
}

/// Represents a collection of metrics for a specific datastore flavor.
///
/// This structure is used to expose datastore-specific metrics to the telemetry system.
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

#[derive(Clone)]
pub(crate) struct TransactionFactory {
	// Clock for tracking time. It is read-only and accessible to all transactions.
	clock: Arc<SizedClock>,
	// The inner datastore type
	builder: Arc<Box<dyn TransactionBuilder>>,
	// Async event processing trigger
	async_event_trigger: Arc<Notify>,
}

impl TransactionFactory {
	pub(super) fn new(
		clock: Arc<SizedClock>,
		async_event_trigger: Arc<Notify>,
		builder: Box<dyn TransactionBuilder>,
	) -> Self {
		Self {
			clock,
			builder: Arc::new(builder),
			async_event_trigger,
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
			self.async_event_trigger.clone(),
			Transactor {
				inner,
			},
		))
	}

	/// Registers metrics for the current datastore flavor if supported.
	fn register_metrics(&self) -> Option<Metrics> {
		self.builder.register_metrics()
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.builder.collect_u64_metric(metric)
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

	/// Registers metrics for the current datastore flavor if supported.
	///
	/// This will return a list of available metrics and their descriptions.
	fn register_metrics(&self) -> Option<Metrics>;

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	///
	/// - `metric`: The name of the metric to collect.
	fn collect_u64_metric(&self, metric: &str) -> Option<u64>;
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
	///
	/// # Parameters
	/// - `path`: Database connection path string
	/// - `clock`: Optional clock for timestamp generation (uses system clock if None)
	/// - `canceller`: Token for graceful shutdown and cancellation of long-running operations
	async fn new_transaction_builder(
		&self,
		path: &str,
		clock: Option<Arc<SizedClock>>,
		canceller: CancellationToken,
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
		_canceller: CancellationToken,
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
					// Create a new blocking threadpool
					super::threadpool::initialise();
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

	/// Registers metrics for the current datastore flavor if supported.
	fn register_metrics(&self) -> Option<Metrics> {
		match self {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => Some(v.register_metrics()),
			#[allow(unreachable_patterns)]
			_ => None,
		}
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	// Allow unused variable when kv-rocksdb feature is not enabled
	#[allow(unused_variables)]
	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		match self {
			#[cfg(feature = "kv-rocksdb")]
			DatastoreFlavor::RocksDB(v) => v.collect_u64_metric(metric),
			#[allow(unreachable_patterns)]
			_ => None,
		}
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
		Self::new_with_factory(CommunityComposer(), path, CancellationToken::new()).await
	}

	/// Creates a new datastore instance with a custom transaction builder factory.
	///
	/// This allows embedders to provide their own factory implementation for custom
	/// backend selection or configuration.
	///
	/// # Parameters
	/// - `factory`: Transaction builder factory for backend selection
	/// - `path`: Database path (e.g., "memory", "surrealkv://path", "tikv://host:port")
	/// - `canceller`: Token for graceful shutdown and cancellation of long-running operations
	///
	/// # Generic parameters
	/// - `F`: Transaction builder factory type implementing `TransactionBuilderFactory`
	pub async fn new_with_factory<F: TransactionBuilderFactory + BucketStoreProvider + 'static>(
		composer: F,
		path: &str,
		canceller: CancellationToken,
	) -> Result<Self> {
		Self::new_with_clock::<F>(composer, path, None, canceller).await
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
	/// - `canceller`: Token for graceful shutdown and cancellation of long-running operations
	///
	/// # Generic parameters
	/// - `F`: Transaction builder factory type implementing `TransactionBuilderFactory`
	pub(crate) async fn new_with_clock<
		C: TransactionBuilderFactory + BucketStoreProvider + 'static,
	>(
		composer: C,
		path: &str,
		clock: Option<Arc<SizedClock>>,
		canceller: CancellationToken,
	) -> Result<Datastore> {
		// Initiate the desired datastore
		let (builder, clock) = composer.new_transaction_builder(path, clock, canceller).await?;
		//
		let buckets = BucketsManager::new(Arc::new(composer));
		// Set the properties on the datastore
		Self::new_with_builder(builder, buckets, clock)
	}

	pub(crate) fn new_with_builder(
		builder: Box<dyn TransactionBuilder>,
		buckets: BucketsManager,
		clock: Arc<SizedClock>,
	) -> Result<Self> {
		let async_event_trigger = Arc::new(Notify::new());
		let tf = TransactionFactory::new(clock, async_event_trigger.clone(), builder);
		let id = Uuid::new_v4();
		Ok(Self {
			id,
			transaction_factory: tf.clone(),
			auth_enabled: false,
			dynamic_configuration: DynamicConfiguration::default(),
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
			buckets,
			sequences: Sequences::new(tf, id),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new()),
			async_event_trigger,
		})
	}

	/// Registers metrics for the current datastore flavor if supported.
	///
	/// This will return a list of available metrics and their descriptions.
	pub fn register_metrics(&self) -> Option<Metrics> {
		self.transaction_factory.register_metrics()
	}

	/// Collects a specific u64 metric by name if supported by the datastore flavor.
	///
	/// - `metric`: The name of the metric to collect.
	pub fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.transaction_factory.collect_u64_metric(metric)
	}

	/// Create a new datastore with the same persistent data (inner), with
	/// flushed cache. Simulating a server restart
	pub fn restart(self) -> Self {
		self.buckets.clear();
		Self {
			id: self.id,
			auth_enabled: self.auth_enabled,
			dynamic_configuration: DynamicConfiguration::default(),
			slow_log: self.slow_log,
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
			buckets: self.buckets,
			sequences: Sequences::new(self.transaction_factory.clone(), self.id),
			transaction_factory: self.transaction_factory,
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new()),
			async_event_trigger: self.async_event_trigger,
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
	pub fn with_query_timeout(self, duration: Option<Duration>) -> Self {
		self.dynamic_configuration.set_query_timeout(duration);
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
	// Returns the current version and a flag indicating if this is a new datastore
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn check_version(&self) -> Result<(MajorVersion, bool)> {
		let (version, is_new) = self.get_version().await?;
		// Check we are running the latest version
		if !version.is_latest() {
			bail!(Error::OutdatedStorageVersion);
		}
		// Everything ok
		Ok((version, is_new))
	}

	// Initialise the cluster and run bootstrap utilities
	// Returns the current version and a flag indicating if this is a new datastore
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn get_version(&self) -> Result<(MajorVersion, bool)> {
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
				(val, false)
			}
			// There is no version set in the storage
			None => {
				// Fetch any keys immediately following the version key
				let rng = crate::key::version::proceeding();
				let keys = catch!(txn, txn.keys(rng, 1, 0, None).await);
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
				(version, true)
			}
		};
		// Everything ok
		Ok(val)
	}

	// --------------------------------------------------
	// Initialisation functions
	// --------------------------------------------------

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
			let opt = Options::new(self.id, self.dynamic_configuration.clone())
				.with_auth(Arc::new(Auth::for_root(Role::Owner)));
			let mut ctx = Context::default();
			ctx.set_transaction(txn.clone());
			let ctx = ctx.freeze();
			let mut stack = TreeStack::new();
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

	/// Setup the default namespace and database
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn initialise_defaults(&self, namespace: &str, database: &str) -> Result<()> {
		info!(target: TARGET, "This is a new SurrealDB instance. Initialising default namespace '{namespace}' and database '{database}'");
		// Create the SQL statement
		let sql = r"
			DEFINE NAMESPACE $namespace COMMENT 'Default namespace generated by SurrealDB';
			USE NS $namespace;
			DEFINE DATABASE $database COMMENT 'Default database generated by SurrealDB';
			DEFINE CONFIG DEFAULT NAMESPACE $namespace DATABASE $database;
		"
		.to_string();

		// Create the variables
		let vars = map! {
			"namespace".to_string() => namespace.to_string().into_value(),
			"database".to_string() => database.to_string().into_value(),
		};

		// Execute the SQL statement
		self.execute(&sql, &Session::owner(), Some(vars.into())).await?;
		// Everything ok
		Ok(())
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
		self.delete_node().await?;
		// Run any storage engine shutdown tasks
		self.transaction_factory.builder.shutdown().await
	}

	// --------------------------------------------------
	// Node functions
	// --------------------------------------------------

	/// Initialise the cluster and run bootstrap utilities
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn bootstrap(&self) -> Result<()> {
		// Insert this node in the cluster
		self.insert_node().await?;
		// Mark inactive nodes as archived
		self.expire_nodes().await?;
		// Remove archived nodes
		self.remove_nodes().await?;
		// Everything ok
		Ok(())
	}

	/// Inserts a node for the first time into the cluster.
	///
	/// This function should be run at server or database startup.
	///
	/// This function ensures that this node is entered into the clister
	/// membership entries. This function must be run at server or database
	/// startup, in order to write the initial entry and timestamp to storage.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn insert_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id,"Inserting node in the cluster");
		// Refresh system usage metrics
		crate::sys::refresh().await;
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::Nd::new(self.id);
		let now = self.clock_now().await;
		let node = Node::new(self.id, now, false);
		let res = run!(txn, txn.put(&key, &node, None).await);
		match res {
			Err(e) => {
				if matches!(
					e.downcast_ref::<Error>(),
					Some(Error::Kvs(crate::kvs::Error::TransactionKeyAlreadyExists))
				) {
					Err(anyhow::Error::new(Error::ClAlreadyExists {
						id: self.id.to_string(),
					}))
				} else {
					Err(e)
				}
			}
			x => x,
		}
	}

	/// Updates an already existing node in the cluster.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// This function updates the entry for this node with an up-to-date
	/// timestamp. This ensures that the node is not marked as expired by any
	/// garbage collection tasks, preventing any data cleanup for this node.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn update_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id, "Updating node in the cluster");
		// Refresh system usage metrics
		crate::sys::refresh().await;
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(self.id);
		let now = self.clock_now().await;
		let node = Node::new(self.id, now, false);
		run!(txn, txn.replace(&key, &node).await)
	}

	/// Deletes a node from the cluster.
	///
	/// This function should be run when a node is shutting down.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn delete_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id, "Archiving node in the cluster");
		// Open transaction and set node data
		let txn = self.transaction(Write, Optimistic).await?;
		let key = crate::key::root::nd::new(self.id);
		let val = catch!(txn, txn.get_node(self.id).await);
		let node = val.as_ref().archive();
		run!(txn, txn.replace(&key, &node).await)
	}

	/// Expires nodes which have timedout from the cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn expire_nodes(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, "Archiving expired nodes in the cluster");
		// Fetch all of the inactive nodes
		let inactive = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = catch!(txn, txn.all_nodes().await);
			let now = self.clock_now().await;
			catch!(txn, txn.cancel().await);
			// Filter the inactive nodes
			nds.iter()
				.filter_map(|n| {
					// Check that the node is active and has expired
					match n.is_active() && n.heartbeat < now - Duration::from_secs(30) {
						true => Some(n.to_owned()),
						false => None,
					}
				})
				.collect::<Vec<_>>()
		};
		// Check if there are inactive nodes
		if !inactive.is_empty() {
			// Open a writeable transaction
			let txn = self.transaction(Write, Optimistic).await?;
			// Archive the inactive nodes
			for nd in inactive.iter() {
				// Log the live query scanning
				trace!(target: TARGET, id = %nd.id, "Archiving node in the cluster");
				// Mark the node as archived
				let node = nd.archive();
				// Get the key for the node entry
				let key = crate::key::root::nd::new(nd.id);
				// Update the node entry
				catch!(txn, txn.replace(&key, &node).await);
			}
			// Commit the changes
			catch!(txn, txn.commit().await);
		}
		// Everything was successful
		Ok(())
	}

	/// Removes and cleans up nodes which are no longer in this cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function clears up all nodes which have been marked as archived.
	/// When a matching node is found, all node queries, and table queries are
	/// garbage collected, before the node itself is completely deleted.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn remove_nodes(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, "Cleaning up archived nodes in the cluster");
		// Fetch all of the archived nodes
		let archived = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = catch!(txn, txn.all_nodes().await);
			catch!(txn, txn.cancel().await);
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Loop over the archived nodes
		for id in archived.iter() {
			// Open a writeable transaction
			let beg = crate::key::node::lq::prefix(*id)?;
			let end = crate::key::node::lq::suffix(*id)?;
			let mut next = Some(beg..end);
			let txn = self.transaction(Write, Optimistic).await?;
			{
				// Log the live query scanning
				trace!(target: TARGET, id = %id, "Deleting live queries for node");
				// Scan the live queries for this node
				while let Some(rng) = next {
					// Fetch the next batch of keys and values
					let res = catch!(txn, txn.batch_keys_vals(rng, *NORMAL_FETCH_SIZE, None).await);
					next = res.next;
					for (k, v) in res.result.iter() {
						// Decode the data for this live query
						let val: NodeLiveQuery = KVValue::kv_decode_value(v.clone())?;
						// Get the key for this node live query
						let nlq = catch!(txn, crate::key::node::lq::Lq::decode_key(k.clone()));
						// Check that the node for this query is archived
						if archived.contains(&nlq.nd) {
							// Get the key for this table live query
							let tlq = crate::key::table::lq::new(val.ns, val.db, &val.tb, nlq.lq);
							// Delete the table live query
							catch!(txn, txn.clr(&tlq).await);
							// Delete the node live query
							catch!(txn, txn.clr(&nlq).await);
						}
					}
					// Pause and yield execution
					yield_now!();
				}
			}
			{
				// Log the node deletion
				trace!(target: TARGET, id = %id, "Deleting node from the cluster");
				// Get the key for the node entry
				let key = crate::key::root::nd::new(*id);
				// Delete the cluster node entry
				catch!(txn, txn.clr(&key).await);
			}
			// Commit the changes
			catch!(txn, txn.commit().await);
		}
		// Everything was successful
		Ok(())
	}

	/// Clean up all other miscellaneous data.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function clears up all data which might have been missed from
	/// previous cleanup runs, or when previous runs failed. This function
	/// currently deletes all live queries, for nodes which no longer exist
	/// in the cluster, from all namespaces, databases, and tables. It uses
	/// a number of transactions in order to prevent failure of large or
	/// long-running transactions on distributed storage engines.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn garbage_collect(&self) -> Result<()> {
		// Log the node deletion
		trace!(target: TARGET, "Garbage collecting all miscellaneous data");
		// Fetch archived nodes
		let archived = {
			let txn = self.transaction(Read, Optimistic).await?;
			let nds = catch!(txn, txn.all_nodes().await);
			txn.cancel().await?;
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Fetch all namespaces
		let nss = {
			let txn = self.transaction(Read, Optimistic).await?;
			let res = catch!(txn, txn.all_ns().await);
			txn.cancel().await?;
			res
		};
		// Loop over all namespaces
		for ns in nss.iter() {
			// Log the namespace
			trace!(target: TARGET, "Garbage collecting data in namespace {}", ns.name);
			// Fetch all databases
			let dbs = {
				let txn = self.transaction(Read, Optimistic).await?;
				let res = catch!(txn, txn.all_db(ns.namespace_id).await);
				txn.cancel().await?;
				res
			};
			// Loop over all databases
			for db in dbs.iter() {
				// Log the namespace
				trace!(target: TARGET, "Garbage collecting data in database {}/{}", ns.name, db.name);
				// Fetch all tables
				let tbs = {
					let txn = self.transaction(Read, Optimistic).await?;
					let res = catch!(txn, txn.all_tb(ns.namespace_id, db.database_id, None).await);
					txn.cancel().await?;
					res
				};
				// Loop over all tables
				for tb in tbs.iter() {
					// Log the namespace
					trace!(target: TARGET, "Garbage collecting data in table {}/{}/{}", ns.name, db.name, tb.name);
					// Iterate over the table live queries
					let beg =
						crate::key::table::lq::prefix(db.namespace_id, db.database_id, &tb.name)?;
					let end =
						crate::key::table::lq::suffix(db.namespace_id, db.database_id, &tb.name)?;
					let mut next = Some(beg..end);
					let txn = self.transaction(Write, Optimistic).await?;
					while let Some(rng) = next {
						// Fetch the next batch of keys and values
						let max = *NORMAL_FETCH_SIZE;
						let res = catch!(txn, txn.batch_keys_vals(rng, max, None).await);
						next = res.next;
						for (k, v) in res.result.iter() {
							// Decode the LIVE query statement
							let stm: SubscriptionDefinition = KVValue::kv_decode_value(v.clone())?;
							// Get the node id and the live query id
							let (nid, lid) = (stm.node, stm.id);
							// Check that the node for this query is archived
							if archived.contains(&stm.node) {
								// Get the key for this node live query
								let tlq = catch!(txn, crate::key::table::lq::Lq::decode_key(k));
								// Get the key for this table live query
								let nlq = crate::key::node::lq::new(nid, lid);
								// Delete the node live query
								catch!(txn, txn.clr(&nlq).await);
								// Delete the table live query
								catch!(txn, txn.clr(&tlq).await);
							}
						}
						// Pause and yield execution
						yield_now!();
					}
					// Commit the changes
					catch!(txn, txn.commit().await);
				}
			}
		}
		// All ok
		Ok(())
	}

	// --------------------------------------------------
	// Live query functions
	// --------------------------------------------------

	/// Clean up the live queries for a disconnected connection.
	///
	/// This function should be run when a WebSocket disconnects.
	///
	/// This function clears up the live queries on the current node, which
	/// are specified by uique live query UUIDs. This is necessary when a
	/// WebSocket disconnects, and any associated live queries need to be
	/// cleaned up and removed.
	#[instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn delete_queries(&self, ids: Vec<uuid::Uuid>) -> Result<()> {
		// Log the node deletion
		trace!(target: TARGET, "Deleting live queries for a connection");
		// Fetch expired nodes
		let txn = self.transaction(Write, Optimistic).await?;
		// Loop over the live query unique ids
		for id in ids {
			// Get the key for this node live query
			let nlq = crate::key::node::lq::new(self.id(), id);
			// Fetch the LIVE meta data node entry
			if let Some(lq) = catch!(txn, txn.get(&nlq, None).await) {
				// Get the key for this node live query
				let nlq = crate::key::node::lq::new(self.id(), id);
				// Get the key for this table live query
				let tlq = crate::key::table::lq::new(lq.ns, lq.db, &lq.tb, id);
				// Delete the table live query
				catch!(txn, txn.clr(&tlq).await);
				// Delete the node live query
				catch!(txn, txn.clr(&nlq).await);
			}
		}
		// Commit the changes
		catch!(txn, txn.commit().await);
		// All ok
		Ok(())
	}

	// --------------------------------------------------
	// Changefeed functions
	// --------------------------------------------------

	/// Performs changefeed garbage collection as a background task.
	///
	/// This method is responsible for cleaning up old changefeed data across
	/// all databases. It uses a distributed task lease mechanism to coordinate
	/// which node performs this maintenance operation. Once a batch starts it
	/// runs to completion even if the lease expires, so brief overlap is
	/// possible.
	///
	/// The process involves:
	/// 1. Acquiring a lease for the ChangeFeedCleanup task
	/// 2. Cleaning up old changefeed data from all databases
	///
	/// # Arguments
	/// * `interval` - The interval between compaction runs, to calculate the lease duration
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn changefeed_process(&self, interval: &Duration) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting changefeed garbage collection");
		// Create a new lease handler
		let lh = LeaseHandler::new(
			self.sequences.clone(),
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::ChangeFeedCleanup,
			*interval * 2,
		)?;
		// If we don't get the lease, another node is handling this task
		if !lh.has_lease().await? {
			return Ok(());
		}
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		// Create a new transaction
		let txn = self.transaction(Write, Optimistic).await?;
		// Perform the garbage collection
		catch!(txn, crate::cf::gc_all_at(&lh, &txn).await);
		// Commit the changes
		catch!(txn, txn.commit().await);
		// Everything ok
		Ok(())
	}

	// --------------------------------------------------
	// Indexing functions
	// --------------------------------------------------

	/// Processes the index compaction queue
	///
	/// This method is called periodically by the index compaction thread to
	/// process indexes that have been marked for compaction. It acquires a
	/// distributed lease to coordinate compaction across the cluster. Once a
	/// batch starts it runs to completion even if the lease expires, so brief
	/// overlap is possible.
	///
	/// The method scans the index compaction queue (stored as `Ic` keys) and
	/// processes each index that needs compaction. Currently, only full-text
	/// indexes support compaction, which helps optimize their performance by
	/// consolidating changes and removing unnecessary data.
	///
	/// After processing an index, it is removed from the compaction queue.
	///
	/// # Arguments
	/// * `interval` - The interval between compaction runs, to calculate the lease duration
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn index_compaction(&self, interval: Duration) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting index compaction process");
		// Create a new lease handler
		let lh = LeaseHandler::new(
			self.sequences.clone(),
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			interval * 2,
		)?;
		// We continue without interruptions while there are keys and the lease
		loop {
			// Attempt to acquire a lease for the IndexCompaction task
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok(());
			}
			// Output function invocation details to logs
			trace!(target: TARGET, "Running index compaction process");
			// Create a new transaction
			let txn = self.transaction(Write, Optimistic).await?;
			// Collect every item in the queue
			let (beg, end) = IndexCompactionKey::range();
			let range = beg..end;
			let mut previous: Option<IndexCompactionKey<'static>> = None;
			let mut count = 0;
			// Returns an ordered list of indexes that require compaction
			let items = catch!(txn, txn.getr(range.clone(), None).await);
			for (k, _) in items {
				count += 1;
				lh.try_maintain_lease().await?;
				let ic = IndexCompactionKey::decode_key(&k)?;
				// If the index has already been compacted, we can ignore the task
				if let Some(p) = &previous
					&& p.index_matches(&ic)
				{
					continue;
				}
				match catch!(txn, txn.get_tb_index_by_id(ic.ns, ic.db, ic.tb.as_ref(), ic.ix).await)
				{
					Some(ix) if !ix.prepare_remove => match &ix.index {
						Index::FullText(p) => {
							let ft = catch!(
								txn,
								FullTextIndex::new(
									&self.index_stores,
									&txn,
									IndexKeyBase::new(
										ic.ns,
										ic.db,
										ix.table_name.clone(),
										ix.index_id
									),
									p,
								)
								.await
							);
							catch!(txn, ft.compaction(&txn).await);
						}
						Index::Count(_) => {
							catch!(txn, IndexOperation::index_count_compaction(&ic, &txn).await);
						}
						_ => {
							trace!(target: TARGET, "Index compaction: Index {:?} does not support compaction, skipping", ic.ix);
						}
					},
					_ => {
						trace!(target: TARGET, "Index compaction: Index {:?} not found, skipping", ic.ix);
					}
				}
				previous = Some(ic.into_owned());
			}
			if count > 0 {
				catch!(txn, txn.delr(range).await);
				catch!(txn, txn.commit().await);
			} else {
				txn.cancel().await?;
				return Ok(());
			}
		}
	}

	/// Process queued async events using a distributed lease to coordinate batches.
	/// Once a batch starts it runs to completion even if the lease expires, so
	/// brief overlap is possible.
	#[instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))]
	pub async fn event_processing(&self, interval: Duration) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting event processing process");
		// Create a new lease handler
		let lh = LeaseHandler::new(
			self.sequences.clone(),
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::EventProcessing,
			interval * 2,
		)?;
		// We continue without interruptions while there are keys and the lease
		loop {
			// Attempt to acquire a lease for the EventProcessing task
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok(());
			}
			// Output function invocation details to logs
			trace!(target: TARGET, "Running event processing process");
			if AsyncEventRecord::process_next_events_batch(self, Some(&lh)).await? == 0 {
				// The last batch didn't have any events to process,
				// we can sleep until the next wake-up call
				return Ok(());
			}
		}
	}

	// --------------------------------------------------
	// Other functions
	// --------------------------------------------------

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

	pub(crate) fn sequences(&self) -> &Sequences {
		&self.sequences
	}

	pub(crate) fn transaction_factory(&self) -> &TransactionFactory {
		&self.transaction_factory
	}
	pub fn async_event_trigger(&self) -> &Arc<Notify> {
		&self.async_event_trigger
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
			Some(Error::QueryTimedout(msg)) => DbResultError::QueryTimedout(format!("{}", msg)),
			Some(Error::QueryCancelled) => DbResultError::QueryCancelled,
			Some(Error::QueryNotExecuted {
				message,
			}) => DbResultError::QueryNotExecuted(format!("{message} - plan: {plan:?}")),
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
				Some(Error::QueryTimedout(timeout)) => {
					DbResultError::QueryTimedout(format!("Timed out: {}", timeout))
				}
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
				Some(Error::Storekey(_)) => {
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
		let mut ctx = Context::default();
		// Set context capabilities
		ctx.add_capabilities(self.capabilities.clone());
		// Set the global query timeout
		if let Some(timeout) = self.dynamic_configuration.get_query_timeout() {
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
			let res = txn.export(&ns, &db, cfg, chn).await;
			txn.cancel().await?;
			res
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
		Options::new(self.id, self.dynamic_configuration.clone())
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_live(sess.live())
			.with_auth(sess.au.clone())
			.with_auth_enabled(self.auth_enabled)
	}

	pub fn setup_ctx(&self) -> Result<Context> {
		let mut ctx = Context::from_ds(
			self.dynamic_configuration.get_query_timeout(),
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
		ctx: Option<&Context>,
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

		let value = PublicValue::from_t(object! {
			namespace: session.ns.clone(),
			database: session.db.clone(),
		});

		Ok(query_result.finish_with_result(Ok(value)))
	}

	/// Get a db model by name.
	///
	/// TODO: This should not be public, but it is used in `surrealdb/src/api/engine/local/mod.rs`.
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

	/// Invoke an API handler.
	///
	/// TODO: This should not need to be public, but it is used in `src/net/api.rs`.
	pub async fn invoke_api_handler(
		&self,
		ns: &str,
		db: &str,
		path: &str,
		session: &Session,
		mut req: ApiRequest,
	) -> Result<ApiResponse> {
		let tx = Arc::new(self.transaction(TransactionType::Write, LockType::Optimistic).await?);

		let db = tx.ensure_ns_db(None, ns, db).await?;

		let apis = tx.all_db_apis(db.namespace_id, db.database_id).await?;
		let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

		let res = match ApiDefinition::find_definition(apis.as_ref(), segments, req.method) {
			Some((api, params)) => {
				debug!(
					request_id = %req.request_id,
					path = %path,
					"API definition found, dispatching to process_api_request"
				);
				req.params = params.try_into()?;

				let opt = self.setup_options(session);

				let mut ctx = self.setup_ctx()?;
				ctx.set_transaction(Arc::clone(&tx));
				ctx.attach_session(session)?;
				let ctx = &ctx.freeze();

				process_api_request(ctx, &opt, api, req).await
			}
			None => {
				trace!(
					request_id = %req.request_id,
					path = %path,
					"No API definition found for path"
				);
				tx.cancel().await?;
				return Ok(ApiResponse::from_error(
					ApiError::NotFound.into(),
					req.request_id.clone(),
				));
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
			comment: Expr::Literal(Literal::String(description.to_string())),
			hash,
			kind: Default::default(),
			permissions: Default::default(),
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
	use crate::val::TableName;

	#[tokio::test]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let username = "root";
		let password = "root";

		// Setup the initial user if there are no root users
		{
			let txn = ds.transaction(Read, Optimistic).await.unwrap();
			assert_eq!(txn.all_root_users().await.unwrap().len(), 0);
			txn.cancel().await.unwrap();
		}
		ds.initialise_credentials(username, password).await.unwrap();
		{
			let txn = ds.transaction(Read, Optimistic).await.unwrap();
			assert_eq!(txn.all_root_users().await.unwrap().len(), 1);
			txn.cancel().await.unwrap();
		}
		verify_root_creds(&ds, username, password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = {
			let txn = ds.transaction(Read, Optimistic).await.unwrap();
			let res = txn.expect_root_user(username).await.unwrap().hash.clone();
			txn.cancel().await.unwrap();
			res
		};

		ds.initialise_credentials(username, password).await.unwrap();
		{
			let txn = ds.transaction(Read, Optimistic).await.unwrap();
			assert_eq!(pass_hash, txn.expect_root_user(username).await.unwrap().hash.clone());
			txn.cancel().await.unwrap();
		}
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

		let opt = Options::new(dbs.id(), DynamicConfiguration::default())
			.with_ns(Some("test".into()))
			.with_db(Some("test".into()))
			.with_live(false)
			.with_auth_enabled(false)
			.with_max_computation_depth(u32::MAX);

		// Create a default context
		let mut ctx = Context::default();
		// Set context capabilities
		ctx.add_capabilities(dbs.capabilities.clone());
		// Start a new transaction
		let txn = dbs.transaction(TransactionType::Read, Optimistic).await?.enclose();
		// Store the transaction
		ctx.set_transaction(txn.clone());
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
		txn.cancel().await?;
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
			let tb = TableName::from("test");
			let initial = txn.get_tb(db.namespace_id, db.database_id, &tb).await?.unwrap();
			let initial_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, &tb)?;
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
			let tb = TableName::from("test");
			let after_define = txn.get_tb(db.namespace_id, db.database_id, &tb).await?.unwrap();
			let after_define_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, &tb)?;
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
			let tb = TableName::from("test");
			let after_remove = txn.get_tb(db.namespace_id, db.database_id, &tb).await?.unwrap();
			let after_remove_live_query_version =
				cache.get_live_queries_version(db.namespace_id, db.database_id, &tb)?;
			txn.cancel().await?;
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
