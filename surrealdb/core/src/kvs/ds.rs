use std::any::TypeId;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};
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
use arc_swap::ArcSwap;
use async_channel::Sender;
use bytes::{Bytes, BytesMut};
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
use common::rt::spawn;
use common::time::{Instant, MissedTickBehavior, interval, sleep, timeout, timeout_at};
use futures::{Future, Stream, StreamExt};
use rand::Rng;
use rand::distr::{Alphanumeric, SampleString};
use surrealdb_cnf::ConfigMap;
use surrealdb_cnf::dynamic::DynamicConfiguration;
use surrealdb_datastore::index_state::index_has_unmaterialised_updates;
use surrealdb_datastore::out_of_line::OutOfLine;
use surrealdb_datastore::triggers::CommitTriggers;
use surrealdb_datastore::values::graph::GraphFoldScope;
use surrealdb_datastore::{FeatureLevelCell, FeatureLevelState};
use surrealdb_expr::parser::Query as Ast;
use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::value::EncodeContext;
use surrealdb_kvs::{DestroyRange, DestroyRangeHandle, FeatureLevel, Release, TransactionType};
use surrealdb_types::{AuthError, Error as TypesError, SurrealValue, object};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use super::tx::Transaction;
use super::version::{MajorVersion, MigrationRecord, StorageVersion, VersionHistoryEntry};
use super::{INDEX_COMPACTION_QUEUE_BATCH_SIZE, migration};
use crate::api::err::ApiError;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::buc::manager::BucketsManager;
#[cfg(test)]
use crate::catalog::Index;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, NodeProvider, RootProvider, TableProvider,
};
use crate::catalog::{FromStored, NodeLiveQuery, StoredFeatureLevel};
use crate::config::RuntimeConfig;
use crate::ctx::{CancelHandle, Context, FrozenContext};
#[cfg(feature = "jwks")]
use crate::dbs::capabilities::NetTarget;
use crate::dbs::capabilities::{
	ArbitraryQueryTarget, EvalQueryTarget, ExperimentalTarget, MethodTarget, RouteTarget,
};
use crate::dbs::executor::CarryBound;
use crate::dbs::node::{Node, NodeRelease, Timestamp};
use crate::dbs::{
	Capabilities, DurableSession, Executor, MessageBroker, Options, QueryResult,
	QueryResultBuilder, QueryStreamItem, QueryStreamJob, Session, TopLevelStatement,
	durable_session, restore_session,
};
use crate::doc::process_next_events_batch;
use crate::err::{EngineError, Error};
use crate::exec::Error as ExecError;
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::timers::compile_phase;
use crate::exec::surface::{FULL, Surface};
use crate::expr::logical::LogicalOp;
use crate::expr::model::get_model_path;
use crate::expr::statements::DefineUserStatement;
use crate::expr::statements::define::DefineKind;
use crate::expr::user::UserDuration;
use crate::expr::{Base, Expr, Idiom, Literal};
#[cfg(feature = "http")]
use crate::http::HttpClient;
use crate::iam::{Action, Auth, PolicyError, Resource, ResourceKind, Role};
#[cfg(test)]
use crate::idx::CompactionApply;
use crate::idx::IndexKeyBase;
#[cfg(test)]
use crate::idx::index::IndexOperation;
#[cfg(test)]
use crate::idx::trees::hnsw::index::HnswCompactionOutcome;
use crate::idx::trees::store::IndexStores;
use crate::key::reclaim::{Expunge, ReclaimKind, ReclaimState};
use crate::key::schema::{
	BootstrapKey, DatabasePrefix, DbRoot, DocKeyPrefix, DocLookupIdentityPrefix, DocLookupPrefix,
	DocPendingPrefix, FeatureLevelKey, GraphFoldDirPrefix, GraphFoldKey, GraphFoldPrefix, IdxRoot,
	IndexCompactionIxPrefix, IndexCompactionKey, IndexCompactionPrefix, IndexDefPrefix,
	MigrationKey, MigrationPrefix, NamespacePrefix, NodeKey, NodeLiveQueryKey, NodeLiveQueryPrefix,
	NodeReleaseKey, NodeReleasePrefix, NsRoot, ReclaimKey, ReclaimPrefix, RootUserPrefix,
	SessionKey, SessionPrefix, StorageVersionKey, SubscriptionKey, TablePrefix,
	VersionHistoryPrefix, VersionKey,
};
use crate::key::{
	AnyRange, KVKey, KVKeyDecode, KVSubspace, KVValue, Key, KeyRange, RawRange, Resumable,
};
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::clock::SystemClock;
use crate::kvs::index::IndexBuilder;
pub(crate) use crate::kvs::index::materialise::INDEX_COMPACTION_PASS_MAX_RETRIES;
#[cfg(test)]
use crate::kvs::index::materialise::INDEX_COMPACTION_PASS_PLAN_BUDGET;
use crate::kvs::index::materialise::{
	CompactionPassOutcome, CompactionPassReport, Materialiser, PassLease,
};
#[cfg(all(test, feature = "kv-rocksdb"))]
use crate::kvs::index::materialise::{CompactionWriteGuard, compaction_write_guard};
use crate::kvs::paging::{PageCompanion, PagedDelete, PagedOutcome};
use crate::kvs::request::{Dialect, QueryRequest, QuerySource};
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::{
	CHANGEFEED_GC_PASS_KEY_BUDGET, DatastoreError, Direction, NORMAL_BATCH_SIZE,
	RECLAIM_BATCH_SIZE, RECLAIM_PASS_ENTRY_QUOTA, RECLAIM_PASS_KEY_BUDGET, TransactionFactory,
	is_retryable_transaction_conflict,
};
use crate::lq::LiveQueryRouter;
use crate::observe::ExecutionObserver;
use crate::options::EngineOptions;
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCache;
use crate::syn::{ParseError, ParserConfig, ParserSettings, StatementStream};
use crate::types::{PublicNotification, PublicValue, PublicVariables};
use crate::val::{MAX_VALUE_DEPTH, convert_value_to_public_value};
use crate::{CommunityComposer, syn};

mod builder;
pub(crate) mod config;
mod edge;
#[cfg(all(test, feature = "kv-mem"))]
mod edge_tests;
mod table_migration;

pub use builder::Builder;
pub use config::LiveQueryEngine;

const TARGET: &str = "surrealdb::core::kvs::ds";

/// How long a node commits nothing under its DiskANN writer leases on an index
/// before it releases them ([`Datastore::release_idle_diskann_writer_leases`]).
/// Until then no node returns those shards' guards to `Empty`, so lookups keep
/// scanning them; past it, a write takes the lease again.
const DISKANN_WRITER_LEASE_IDLE: Duration = Duration::from_secs(10);

/// Whether a feature-level raise consults the other nodes' recorded releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeCheck {
	/// Refuse while a running node records a release older than the target, or none.
	Require,
	/// Raise without consulting any node. For a store whose engine admits no other
	/// writer: a node it lists is a record a process left behind, and it cannot
	/// write between the raise and its commit.
	Skip,
}

const NODE_DELETE_TIMEOUT: Duration = Duration::from_secs(60);

/// How long [`Datastore::shutdown`] waits for the maintenance tasks to finish
/// their in-flight pass before giving up on a clean stop.
///
/// A pass is left to finish because the storage engine shutdown that follows
/// refuses every commit, so an interrupted pass fails rather than completes.
/// That cannot be an unbounded wait: an async event runs user-defined
/// SurrealQL, whose duration nothing here bounds, so a single non-terminating
/// event would otherwise hold shutdown open forever. On expiry the remaining
/// handles are dropped and shutdown proceeds; the abandoned pass then has its
/// commits refused, which is the same outcome as a crash at that instant.
const MAINTENANCE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`Datastore::retry_bootstrap`] waits for storage to become able to
/// serve the version bootstrap before giving up.
///
/// Sized to outlast the cluster formation of a distributed backend, which is
/// the condition it exists to wait out. The server's own
/// `SURREAL_STARTUP_OPERATION_TIMEOUT` is a shorter ceiling by default and is
/// the knob an operator reaches for; this bound only stops an embedder that has
/// no such ceiling from waiting forever.
const BOOTSTRAP_RETRY_BUDGET: Duration = Duration::from_secs(120);

/// How long one bootstrap attempt may run before it is abandoned and retried.
const BOOTSTRAP_RETRY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// The gap before the first bootstrap retry, doubled on each further attempt up
/// to [`BOOTSTRAP_RETRY_MAX_BACKOFF`].
const BOOTSTRAP_RETRY_MIN_BACKOFF: Duration = Duration::from_millis(100);

/// The longest gap between two bootstrap attempts.
const BOOTSTRAP_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How many times the back-off may double, which is as many as it takes to
/// reach [`BOOTSTRAP_RETRY_MAX_BACKOFF`] from
/// [`BOOTSTRAP_RETRY_MIN_BACKOFF`].
const BOOTSTRAP_RETRY_MAX_DOUBLINGS: u32 = 6;

// Capping the exponent is only sound while the cap still reaches the ceiling:
// stop short of it and the back-off would silently settle below its documented
// maximum if either bound were retuned.
const _: () = assert!(
	BOOTSTRAP_RETRY_MIN_BACKOFF.as_millis() << BOOTSTRAP_RETRY_MAX_DOUBLINGS
		>= BOOTSTRAP_RETRY_MAX_BACKOFF.as_millis(),
	"the bootstrap back-off cannot double its way up to its own ceiling"
);

/// How often a bootstrap that is still retrying reports that it is still going.
///
/// The cadence is on the log, not on the retries: attempts run as fast as the
/// back-off allows, and one in every interval is reported.
const BOOTSTRAP_RETRY_LOG_INTERVAL: Duration = Duration::from_secs(5);

/// What the top-level `LET`s of an import may retain between statements.
///
/// An import is read as a stream and never held in memory as a whole, but every
/// top-level `LET` binds into the request context for the rest of the stream,
/// so the carried parameters would otherwise grow with the input. A stream that
/// binds more than 10 000 distinct names, or values estimated at more than
/// 64 MiB, fails the `LET` that crosses the bound and ends there; the statements
/// before it have run. Exports define no parameters, so a restore stays far
/// below the bound.
pub(crate) const IMPORT_CARRY_BOUND: CarryBound = CarryBound {
	names: Some(10_000),
	bytes: Some(64 * 1024 * 1024),
};

/// The role assigned to the initial user created when starting the server with
/// credentials for the first time
const INITIAL_USER_ROLE: &str = "owner";

/// How recently a node must have heartbeat to count as running: a node whose
/// heartbeat is older is expired by [`Datastore::expire_nodes`], and is not
/// consulted when the feature level is raised.
pub(crate) const NODE_LIVENESS: Duration = Duration::from_secs(30);

/// Whether a reclaim pass emptied the prefix it was given.
///
/// `Complete` licenses the caller to retire the queue entry. `Incomplete` means
/// the entry's durable resume cursor has advanced but keys remain, so the entry
/// must stay queued for a later pass to finish — retiring it then would orphan
/// the remainder with nothing left to find it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimOutcome {
	Complete,
	Incomplete,
	/// The queue entry was deleted by someone else while the reclaim was
	/// running, withdrawing the claim that the prefix is orphaned. Nothing more
	/// may be deleted from it.
	Cancelled,
	/// The task lease passed to another node mid-reclaim. Like `Incomplete` for
	/// this entry, but the pass itself must stop rather than move on to the next
	/// entry: a lease check is throttled to one datastore read per maintenance
	/// period, so the reclaim consumed the read a fresh check here would depend
	/// on and that check would report the lease held without asking.
	LeaseLost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShutdownNodeDeleteOutcome {
	Archived,
	Failed,
	TimedOut,
}

async fn await_node_step<T, Fut>(
	deadline: Instant,
	timeout_duration: Duration,
	canceller: Option<&CancellationToken>,
	step: Fut,
) -> Result<T>
where
	Fut: Future<Output = Result<T>>,
{
	if let Some(canceller) = canceller {
		tokio::select! {
			biased;
			_ = canceller.cancelled() => bail!(EngineError::QueryCancelled),
			result = timeout_at(deadline, step) => match result {
				Ok(result) => result,
				Err(_) => bail!(EngineError::QueryTimedout(timeout_duration)),
			},
		}
	} else {
		match timeout_at(deadline, step).await {
			Ok(result) => result,
			Err(_) => bail!(EngineError::QueryTimedout(timeout_duration)),
		}
	}
}

async fn await_node_tx_step<T, Fut>(
	txn: &Transaction,
	deadline: Instant,
	timeout_duration: Duration,
	canceller: Option<&CancellationToken>,
	step: Fut,
) -> Result<T>
where
	Fut: Future<Output = Result<T>>,
{
	let result = if let Some(canceller) = canceller {
		tokio::select! {
			biased;
			_ = canceller.cancelled() => {
				let _ = txn.cancel().await;
				bail!(EngineError::QueryCancelled);
			}
			result = timeout_at(deadline, step) => result,
		}
	} else {
		timeout_at(deadline, step).await
	};

	match result {
		Ok(Ok(value)) => Ok(value),
		Ok(Err(e)) => {
			let _ = txn.cancel().await;
			Err(e)
		}
		Err(_) => {
			let _ = txn.cancel().await;
			bail!(EngineError::QueryTimedout(timeout_duration))
		}
	}
}

fn archive_node_for_shutdown(
	timeout_duration: Duration,
	result: Result<()>,
) -> ShutdownNodeDeleteOutcome {
	match result {
		Ok(()) => ShutdownNodeDeleteOutcome::Archived,
		Err(e) => {
			if matches!(crate::err::engine_error(&e), Some(EngineError::QueryTimedout(_))) {
				warn!(
					target: TARGET,
					timeout = ?timeout_duration,
					"Timed out archiving node during shutdown; continuing shutdown"
				);
				return ShutdownNodeDeleteOutcome::TimedOut;
			}

			warn!(
				target: TARGET,
				error = %e,
				"Failed to archive node during shutdown; continuing shutdown"
			);
			ShutdownNodeDeleteOutcome::Failed
		}
	}
}

/// How a datastore served the requests for compiled top-level statements
/// ([`Datastore::statement_cache_stats`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatementCacheStats {
	/// Requests served a kept compilation.
	pub hits: u64,
	/// Requests that compiled the statement.
	pub misses: u64,
	/// Lookups that found a kept compilation whose catalog dependencies, or
	/// the variable values it was compiled for, no longer held; each is also
	/// counted as a miss.
	pub invalidations: u64,
}

/// The underlying datastore instance which stores the dataset.
pub struct Datastore {
	transaction_factory: TransactionFactory,
	/// Cancellation for this datastore's own background work, tripped by
	/// [`Self::shutdown`].
	shutdown: CancellationToken,
	/// Handles for this datastore's maintenance tasks, so [`Self::shutdown`]
	/// can wait for them to stop.
	///
	/// A datastore is not a working database without them — index compaction,
	/// tombstone reclaim, changefeed GC, index-build recovery and the cluster
	/// heartbeat all live there — so the builder starts them rather than
	/// leaving it to the embedder. `None` for a datastore built with
	/// [`Builder::without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks),
	/// until [`Self::start_maintenance_tasks`] is called on it.
	maintenance: parking_lot::Mutex<Option<crate::kvs::tasks::Tasks>>,
	/// The cadences the maintenance tasks run at, kept so
	/// [`Self::start_maintenance_tasks`] starts them on the same schedule the
	/// builder was given rather than on whatever its caller passes in.
	engine_options: EngineOptions,
	/// Serialises [`Self::shutdown`] across the holders of this datastore.
	///
	/// The sequence has to run start to finish for exactly one caller at a time:
	/// it stops the maintenance tasks *before* closing the storage engine, and a
	/// second caller that skipped straight to the storage shutdown would close
	/// the engine out from under a pass the first caller is still waiting on.
	/// Later callers therefore block here and observe the completed sequence.
	shutdown_lock: tokio::sync::Mutex<()>,
	/// The unique id of this datastore, used in notifications.
	id: Uuid,
	/// Whether this process created the datastore's storage.
	///
	/// Latched by [`Self::check_version`], because the underlying signal is
	/// one-shot: it comes from writing the version key, and a caller that
	/// retries after a later failure would otherwise see the key already there
	/// and be told the datastore is pre-existing.
	created_here: std::sync::atomic::AtomicBool,
	/// Whether [`Self::settle_feature_level`] has read the store's feature
	/// level into the shared cell, so [`Self::check_version`] reads it at most
	/// once whether or not the builder did.
	feature_level_settled: std::sync::atomic::AtomicBool,
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
	capabilities: ArcSwap<Capabilities>,
	/// Broker used to deliver live-query notifications after their write commits.
	///
	/// `Some` iff live-query subscribers exist for this datastore (the broker owns the sender
	/// half of the notification channel internally). `None` disables live-query work entirely
	/// at the executor boundary.
	live_query_broker: Option<Arc<dyn MessageBroker>>,
	/// Per-node live-query router state (the tail cursor over the `lqe`
	/// keyspace). Only used when `config.datastore.live_query_engine` is
	/// `Router`, where the router is the sole notification delivery path. See
	/// [`crate::lq`].
	live_query_router: Arc<LiveQueryRouter>,
	/// How a running program gives up its thread once it has spent its
	/// quantum; the default scheduler when `None`.
	scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	/// The memory governor and temporary store a host supplied; see
	/// [`Self::memory_host`] for what applies without them.
	memory_governor: Option<Arc<dyn crate::exec::memory::MemoryGovernor>>,
	temp_store: Option<Arc<dyn crate::exec::memory::TempStore>>,
	/// Whether queries spill nowhere even where the platform has a default
	/// store.
	#[cfg(test)]
	no_temp_store: bool,
	/// The governor and store every query of this datastore shares, settled
	/// at its first query.
	memory_host: Arc<std::sync::OnceLock<crate::exec::memory::MemoryHost>>,
	/// Public HTTP endpoint this datastore publishes on its `Node` catalog row so other
	/// cluster members can route cross-node messages (e.g. live-query relay) to it.
	/// `None` in deployments that don't expose such an endpoint.
	http_endpoint: Option<String>,
	// The index store cache
	index_stores: IndexStores,
	// The cross transaction cache
	cache: Arc<DatastoreCache>,
	/// Cache of generated GraphQL schemas, shared across every transport that
	/// serves GraphQL: the HTTP `/graphql` route, GraphQL-over-WebSocket
	/// subscriptions, the `graphql` RPC method, and the MCP `graphql` tool.
	/// Keyed by `(ns, db, config, schema-fingerprint)` so DDL changes invalidate
	/// it automatically.
	#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
	graphql_schema_cache: crate::graphql::cache::GraphQLSchemaCache,
	/// Registry of built-in scalar, aggregate, projection and index
	/// functions, along with the method-dispatch table. Built once when the
	/// datastore is constructed and shared across all transactions via the
	/// `Arc`. The executor clones this `Arc` onto the `RootContext` of every
	/// statement rather than rebuilding the registry, which is otherwise the
	/// single biggest per-query cost.
	function_registry: Arc<FunctionRegistry>,
	/// The live-subscription units this node has compiled for the VM.
	live_units: Arc<crate::exec::vm::live::LiveUnits>,
	/// The function-body units this node has compiled for the VM.
	compiled_units: Arc<crate::exec::vm::units::CompiledUnits>,
	/// The function-body units compiled for the edge entry. A unit compiled
	/// under one surface is never served to a context of another, so the edge
	/// entry has a cache of its own.
	edge_compiled_units: Arc<crate::exec::vm::units::CompiledUnits>,
	// The index asynchronous builder
	index_builder: IndexBuilder,
	/// When [`Self::index_compaction`] last swept the defined indexes for
	/// unmaterialised updates on finding its queue empty, or `None` before the
	/// first sweep.
	compaction_swept_at: parking_lot::Mutex<Option<Instant>>,
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
	/// When `true`, extensions are prepared on first invocation instead of
	/// at startup.
	#[cfg(feature = "surrealism")]
	lazy_extensions: bool,
	/// Post-commit wake-ups shared with every transaction: async event
	/// processing, and index compaction so the compactor runs on the write
	/// instead of waiting out its interval.
	triggers: Arc<CommitTriggers>,
	/// The per-layer configuration every query executed on this datastore
	/// reaches through its context, plus the datastore's own knobs.
	config: Arc<RuntimeConfig>,
	/// The parser depth limits applied to every query text parsed for this
	/// datastore. Shared with the transports, which decode request bodies
	/// against the same limits.
	parser_config: Arc<ParserConfig>,
	// Http client used to make requests.
	#[cfg(feature = "http")]
	http_client: ArcSwap<HttpClient>,
	/// Observer invoked on significant events. Defaults to [`NoopObserver`].
	observer: Arc<dyn ExecutionObserver>,
}

pub(crate) use surrealdb_kvs::{Metrics, TransactionBuilder};

/// Transaction-builder construction result with router startup state.
///
/// The datastore consumes `builder`; server startup threads `router_state` into
/// the router factory so embedders can make immutable handles available to
/// their HTTP routes without process globals.
pub struct TransactionBuilderParts<S> {
	/// Transaction builder consumed by the datastore.
	pub builder: Box<dyn TransactionBuilder>,
	/// Immutable router startup state produced by the composer.
	pub router_state: S,
}

impl<S> TransactionBuilderParts<S> {
	/// Construct transaction-builder parts with router startup state.
	pub fn new(builder: Box<dyn TransactionBuilder>, router_state: S) -> Self {
		Self {
			builder,
			router_state,
		}
	}
}

impl TransactionBuilderParts<()> {
	/// Construct transaction-builder parts for composers without router state.
	pub fn without_router_state(builder: Box<dyn TransactionBuilder>) -> Self {
		Self::new(builder, ())
	}
}

/// Factory that parses a datastore path and returns a concrete `TransactionBuilder`.
///
/// Implementations can decide how to interpret connection strings (e.g. "memory",
/// "rocksdb:...", "tikv:...") and which clock to use. This lets the CLI and
/// server be generic over different storage backends without hard-coding them.
///
/// The `path_valid` helper is used by the CLI to validate the path early and
/// provide better error messages before starting the runtime.
pub trait TransactionBuilderFactory: Send + Sync + 'static {
	/// Immutable state threaded into router construction after datastore startup.
	type RouterState: Clone + Send + Sync + 'static;

	/// Create a new transaction builder for the datastore.
	///
	/// # Parameters
	/// - `path`: Database connection path string
	/// - `canceller`: Token for graceful shutdown and cancellation of long-running operations
	fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> impl Future<Output = Result<TransactionBuilderParts<Self::RouterState>>> + Send;

	/// Validate a datastore path string.
	fn path_valid(&self, v: &str) -> Result<String>;

	/// Returns the stable datastore node id used for live-query ownership metadata.
	///
	/// Composers that run SurrealDB inside a clustered product should return a deterministic
	/// value so remote writers can route notifications back to the node that owns each
	/// subscriber connection.
	fn datastore_node_id(&self) -> Option<[u8; 16]> {
		None
	}

	/// Creates the broker that receives buffered live-query notifications after commit.
	///
	/// The default broker is local-only and preserves community behaviour. Clustered composers
	/// can return a broker that forwards remote targets without changing transaction results on
	/// delivery failure.
	fn live_query_broker(&self, channel: Sender<PublicNotification>) -> Arc<dyn MessageBroker> {
		crate::dbs::LocalMessageBroker::new(channel)
	}

	/// Public HTTP endpoint this datastore should publish for cross-node messaging.
	///
	/// Clustered composers surface their local node's endpoint here so the [`Datastore`]
	/// can record it on the `Node` catalog row, making it discoverable by peer nodes
	/// (e.g. for the live-query relay). Returns `None` in single-node and shared-backend
	/// deployments that don't expose a cross-node messaging endpoint.
	fn http_endpoint(&self) -> Option<String> {
		None
	}

	/// Runs on every start, after the backend can take writes and before
	/// [`Datastore::check_version`] settles the storage version.
	///
	/// Community's implementation does nothing. A composer that fills this datastore
	/// from another keyspace does so here: the keys it writes are what
	/// `check_version` then finds, so a store that arrives with its version key is
	/// gated and migrated as an existing store, not created and recorded as already
	/// migrated. Its root users are the store's, so credentials given on the command
	/// line are not created alongside them, as with a startup import that brings its
	/// own.
	///
	/// Every start runs it, including a restart of a store that has served traffic
	/// and a node joining a populated cluster. The datastore may hold a complete
	/// copy, a partial one, or a store that was never a copy, so a composer decides
	/// from its own state whether there is anything to fill. The maintenance
	/// schedule has not started, and the readiness gate admits only the probe routes
	/// and the composer's own startup routes, without credentials. The datastore is
	/// built without startup transactions, so its feature level is not settled
	/// either: `check_version` settles it from what the fill leaves.
	///
	/// A fill keeps the rules [`crate::kvs::copy`] sets out, which are what make a
	/// fill cut short safe to restart:
	///
	/// - Check the source with [`crate::kvs::copy::KeyspaceVersion::check`] before writing
	///   anything, so a source this build cannot start from is refused before the copy rather than
	///   after it.
	/// - Write the version key last. Until it lands, the copied keys read as a store from before
	///   versioning and `check_version` refuses them, so a partial copy is never started from.
	/// - Never write the bootstrap sentinel, and keep the composer's own state at or above `0xff`,
	///   outside the range a copy carries and the version gate probes.
	///
	/// Must not call [`Datastore::get_version`] or [`Datastore::check_version`]:
	/// `get_version` writes the version key when it is missing, which would stamp
	/// the store this hook is about to fill.
	///
	/// The call is outside the startup operation timeout and its retries, so it may
	/// take as long as the fill does and owns whatever deadline it needs. An error
	/// ends startup.
	fn before_check_version(
		&self,
		datastore: &Arc<Datastore>,
	) -> impl Future<Output = Result<()>> + Send {
		async move {
			let _ = datastore;
			Ok(())
		}
	}

	/// The runner that executes extensions, used when the builder was given
	/// none. `None` leaves the datastore without one: definitions are accepted,
	/// invocations fail.
	#[cfg(feature = "surrealism")]
	fn extension_runner(
		&self,
		_config: &ConfigMap,
	) -> Option<Arc<dyn crate::surrealism::runner::ExtensionRunner>> {
		None
	}
}

impl TransactionBuilderFactory for CommunityComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> Result<TransactionBuilderParts<Self::RouterState>> {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder(path, canceller, config)
			.await?;
		Ok(TransactionBuilderParts::without_router_state(builder))
	}

	fn path_valid(&self, v: &str) -> Result<String> {
		Ok(surrealdb_kvs_any::Backends::community().path_valid(v)?)
	}

	/// Surrealism archives, on wasmtime.
	#[cfg(feature = "surrealism")]
	fn extension_runner(
		&self,
		config: &ConfigMap,
	) -> Option<Arc<dyn crate::surrealism::runner::ExtensionRunner>> {
		Some(Arc::new(crate::surrealism::runner::SurrealismRunner::from_config_map(config)))
	}
}

impl Display for Datastore {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(self.transaction_factory.backend_name())
	}
}

/// One reclaim queue entry in the running for this pass's key budget.
///
/// Ordered by how long the entry has been waiting — oldest observation first,
/// and the queue's key order between entries observed in the same millisecond so
/// selection is deterministic. Held in a max-heap capped at
/// [`RECLAIM_PASS_ENTRY_QUOTA`], where "greatest" is the newest and so the first
/// to lose its place.
struct ReclaimCandidate {
	/// When the reclaim task first observed the entry. Zero only where the grace
	/// is disabled, in which case every candidate ties and key order decides.
	observed_ms: u64,
	/// The queue key, as the scan returned it.
	key: Vec<u8>,
	/// The state read during the walk, carrying the resume cursor the delete
	/// continues from.
	state: ReclaimState,
}

impl Ord for ReclaimCandidate {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.observed_ms.cmp(&other.observed_ms).then_with(|| self.key.cmp(&other.key))
	}
}

impl PartialOrd for ReclaimCandidate {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl PartialEq for ReclaimCandidate {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == std::cmp::Ordering::Equal
	}
}

impl Eq for ReclaimCandidate {}

/// How often the index-compaction lease is maintained while a batch's passes
/// run.
///
/// No longer than the shortest maintenance period [`LeaseHandler`] throttles
/// its checks to, so a renewal it is due is never more than a tick late; a
/// call it throttles costs nothing.
const INDEX_COMPACTION_LEASE_TICK: Duration = Duration::from_secs(1);

/// What one queue batch's passes leave its cleanup to act on.
#[derive(Default)]
struct CompactionBatchReport {
	/// Indexes whose pass failed, or could not queue the request it owed.
	errors: usize,
	/// Indexes left without the fresh request their pass owed them, whose
	/// requests in the batch the cleanup keeps.
	unqueued: HashSet<IndexKeyBase>,
	/// Indexes whose pass could not act on their requests
	/// ([`CompactionPassOutcome::retains_request`]), of whose requests in the
	/// batch the cleanup keeps one.
	retained: HashSet<IndexKeyBase>,
	/// Whether a pass stopped at its budget with work left and queued the
	/// request for it ([`CompactionPassOutcome::Budget`]).
	budgeted: bool,
}

impl CompactionBatchReport {
	/// Records how one index's pass ended. A pass ended by cancellation ends
	/// the batch with its error.
	fn record(
		&mut self,
		ikb: IndexKeyBase,
		report: CompactionPassReport,
		canceller: &CancellationToken,
	) -> Result<()> {
		let mut retains = false;
		let failed = match report.result {
			Ok(outcome) => {
				trace!(target: TARGET, "Index compaction {ikb} ended: {outcome:?}");
				retains = outcome.retains_request();
				self.budgeted |= outcome == CompactionPassOutcome::Budget && !report.unqueued;
				false
			}
			Err(e) if canceller.is_cancelled() => return Err(e),
			Err(e) => {
				warn!(target: TARGET, "Index compaction {ikb} fails: {e}");
				true
			}
		};
		if failed || report.unqueued {
			self.errors += 1;
		}
		if report.unqueued {
			self.unqueued.insert(ikb);
		} else if retains {
			self.retained.insert(ikb);
		}
		Ok(())
	}
}

/// How one queue batch's cleanup ended.
struct CompactionBatchCleanup {
	/// Indexes whose pass failed, or could not queue the request it owed.
	errors: usize,
	/// Whether the cleanup committed.
	deleted: bool,
	/// The batch's keys the committed cleanup left queued.
	kept: Vec<Vec<u8>>,
	/// The indexes of which the committed cleanup kept one request, as their
	/// pass could not act on it.
	retained: HashSet<IndexKeyBase>,
	/// Whether a pass stopped at its budget with work left, and queued the
	/// request a later batch of the same drain acts on.
	budgeted: bool,
	/// The indexes the batch ran a pass over.
	passed: Vec<IndexKeyBase>,
}

/// One index definition the catalog holds, with where it is defined.
struct DefinedIndex {
	ns: crate::catalog::NamespaceId,
	ns_name: surrealdb_strand::Strand,
	db: crate::catalog::DatabaseId,
	db_name: surrealdb_strand::Strand,
	tb: crate::catalog::TableId,
	ix: Arc<crate::catalog::IndexDefinition>,
}

/// What one drain of the compaction queue has left queued so far, which its
/// later batches do not pass again, so a drain passes each index's kept
/// requests at most once.
#[derive(Default)]
struct CompactionDrain {
	/// The keys earlier batches left queued.
	kept: HashSet<Vec<u8>>,
	/// The indexes of which an earlier batch kept one request. A later batch
	/// deletes their other requests without passing them again.
	retained: HashSet<IndexKeyBase>,
	/// Every index a batch of this drain ran a pass over. The drain's sweep
	/// queues none of them: whatever their pass left — a request it requeued,
	/// one the batch kept, or a failure left to the index's next write — is
	/// settled for this drain.
	passed: HashSet<IndexKeyBase>,
	/// Whether this drain has swept the defined indexes, which it does at most
	/// once whatever the sweep's throttle allows.
	swept: bool,
}

impl CompactionDrain {
	/// Whether a batch passes the index `key` names: an undecodable key never
	/// passes, nor does a key this drain kept, nor one of an index it kept a
	/// request for.
	fn passes(&self, key: &[u8], ikb: Option<&IndexKeyBase>) -> bool {
		ikb.is_some_and(|ikb| !self.kept.contains(key) && !self.retained.contains(ikb))
	}
}

/// The index each queue key names, or `None` for a key that does not decode,
/// which is reported here. An undecodable entry names no index to compact, so
/// it has no pass, and the batch's cleanup deletes it, so one corrupt key
/// cannot wedge every future compaction cycle.
fn decode_index_compaction_keys(keys: &[Vec<u8>]) -> Vec<Option<IndexKeyBase>> {
	keys.iter()
		.map(|k| match IndexCompactionKey::decode_key(k) {
			Ok(ic) => Some(IndexKeyBase::new(ic.ns, ic.db, ic.tb.into_owned(), ic.ix)),
			Err(e) => {
				warn!(target: TARGET, "Skipping undecodable index compaction queue entry: {e}");
				None
			}
		})
		.collect()
}

/// The index-compaction lease, as the passes of one queue batch observe it.
///
/// [`LeaseHandler::try_maintain_lease`] reads the store at most once per
/// maintenance period and reports the lease held to every call in between, so
/// only the call that made the read learns of a loss. This keeps that answer
/// where every pass of the batch reads it at its plan boundaries.
#[derive(Clone)]
struct CompactionLease {
	lh: LeaseHandler,
	lost: Arc<std::sync::atomic::AtomicBool>,
}

impl CompactionLease {
	fn new(lh: &LeaseHandler) -> Self {
		Self {
			lh: lh.clone(),
			lost: Arc::new(std::sync::atomic::AtomicBool::new(false)),
		}
	}

	/// Renews the lease once its maintenance period has passed, and reports
	/// whether this node still holds it. A loss, once seen, stands for the rest
	/// of the batch.
	async fn maintain(&self) -> Result<bool> {
		if self.is_lost() {
			return Ok(false);
		}
		if self.lh.try_maintain_lease().await? {
			return Ok(true);
		}
		self.lost.store(true, std::sync::atomic::Ordering::Release);
		Ok(false)
	}

	/// Whether a renewal has found the lease held by another node.
	fn is_lost(&self) -> bool {
		self.lost.load(std::sync::atomic::Ordering::Acquire)
	}
}

impl PassLease for CompactionLease {
	fn is_lost(&self) -> bool {
		CompactionLease::is_lost(self)
	}
}

/// Holds the compaction passes a node starts until a test releases them.
#[cfg(test)]
mod compaction_pass_park {
	use std::collections::HashMap;
	use std::sync::{Arc, LazyLock, Mutex};

	use tokio::sync::Notify;
	use tokio_util::sync::CancellationToken;
	use uuid::Uuid;

	use crate::idx::IndexKeyBase;

	struct Park {
		/// The one index whose passes park, or every index's when `None`.
		only: Option<IndexKeyBase>,
		parked: Notify,
		release: CancellationToken,
	}

	static PARKS: LazyLock<Mutex<HashMap<Uuid, Arc<Park>>>> = LazyLock::new(Default::default);

	/// The passes each node has started, per index.
	static PASSES: LazyLock<Mutex<HashMap<(Uuid, IndexKeyBase), usize>>> =
		LazyLock::new(Default::default);

	/// Parks every compaction pass `node` starts, until the returned guard
	/// releases them or is dropped.
	pub(super) fn park(node: Uuid) -> ParkGuard {
		park_passes(node, None)
	}

	/// Parks every compaction pass `node` starts over `ikb`, until the
	/// returned guard releases them or is dropped.
	pub(super) fn park_index(node: Uuid, ikb: IndexKeyBase) -> ParkGuard {
		park_passes(node, Some(ikb))
	}

	fn park_passes(node: Uuid, only: Option<IndexKeyBase>) -> ParkGuard {
		let park = Arc::new(Park {
			only,
			parked: Notify::new(),
			release: CancellationToken::new(),
		});
		PARKS.lock().unwrap().insert(node, Arc::clone(&park));
		ParkGuard {
			node,
			park,
		}
	}

	/// The passes each node has ended, per index.
	static ENDED: LazyLock<Mutex<HashMap<(Uuid, IndexKeyBase), usize>>> =
		LazyLock::new(Default::default);

	/// How many passes `node` has started over `ikb`.
	pub(super) fn passes(node: Uuid, ikb: &IndexKeyBase) -> usize {
		PASSES.lock().unwrap().get(&(node, ikb.clone())).copied().unwrap_or(0)
	}

	/// How many passes `node` has ended over `ikb`.
	pub(super) fn ended(node: Uuid, ikb: &IndexKeyBase) -> usize {
		ENDED.lock().unwrap().get(&(node, ikb.clone())).copied().unwrap_or(0)
	}

	/// Counts a pass as it ends.
	pub(super) fn end(node: Uuid, ikb: &IndexKeyBase) {
		*ENDED.lock().unwrap().entry((node, ikb.clone())).or_default() += 1;
	}

	/// Counts a pass as it starts, and waits here while its node is parked
	/// for it.
	pub(super) async fn wait(node: Uuid, ikb: &IndexKeyBase) {
		*PASSES.lock().unwrap().entry((node, ikb.clone())).or_default() += 1;
		let park = PARKS
			.lock()
			.unwrap()
			.get(&node)
			.filter(|park| park.only.as_ref().is_none_or(|only| only == ikb))
			.map(Arc::clone);
		if let Some(park) = park {
			park.parked.notify_one();
			park.release.cancelled().await;
		}
	}

	pub(super) struct ParkGuard {
		node: Uuid,
		park: Arc<Park>,
	}

	impl ParkGuard {
		/// Resolves once a pass has reached the park.
		pub(super) async fn parked(&self) {
			self.park.parked.notified().await;
		}

		/// Lets every parked pass, and every later one, go on.
		pub(super) fn release(&self) {
			PARKS.lock().unwrap().remove(&self.node);
			self.park.release.cancel();
		}
	}

	impl Drop for ParkGuard {
		fn drop(&mut self) {
			self.release();
		}
	}
}
/// The indexes a compaction loop has yet to claim, shared by the loop and its
/// helpers.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
type CompactionQueue = Arc<parking_lot::Mutex<std::collections::VecDeque<IndexKeyBase>>>;

/// What a run of compaction passes, or one of its helpers, hands back: the
/// report of every pass it ran, and the first failure that stopped it.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
struct CompactionRun<R> {
	results: Vec<(IndexKeyBase, R)>,
	failure: Option<anyhow::Error>,
}

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
impl<R> Default for CompactionRun<R> {
	fn default() -> Self {
		Self {
			results: Vec::new(),
			failure: None,
		}
	}
}

#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
impl<R> CompactionRun<R> {
	/// Takes in what a joined helper ran. A helper that panicked or was
	/// aborted fails the run. The first failure is kept; a later one is logged,
	/// unless it is a cancellation the run already failed on.
	fn absorb(&mut self, joined: std::result::Result<Self, common::rt::JoinError>) {
		let failure = match joined {
			Ok(helper) => {
				self.results.extend(helper.results);
				helper.failure
			}
			Err(e) => Some(anyhow::Error::from(e)),
		};
		let Some(failure) = failure else {
			return;
		};
		if self.failure.is_none() {
			self.failure = Some(failure);
		} else if !matches!(crate::err::engine_error(&failure), Some(EngineError::QueryCancelled)) {
			warn!("Index compaction fails while its run stops: {failure}");
		}
	}
}

/// The message a panicking compaction pass carried, as its payload holds it.
#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
fn compaction_panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
	if let Some(message) = payload.downcast_ref::<&str>() {
		message
	} else if let Some(message) = payload.downcast_ref::<String>() {
		message
	} else {
		"a non-string payload"
	}
}

impl Datastore {
	pub fn builder() -> Builder {
		Builder::new()
	}

	pub(crate) async fn retry_index_operation_conflict(
		err: &anyhow::Error,
		operation: impl Into<String>,
	) -> bool {
		if is_retryable_transaction_conflict(err) {
			let operation = operation.into();
			debug!(
				target: TARGET,
				operation = %operation,
				error = %err,
				"retryable index operation conflict, retrying"
			);
			sleep(Duration::from_millis(100)).await;
			true
		} else {
			false
		}
	}

	async fn cancel_and_retry_index_operation_conflict(
		txn: &Transaction,
		err: &anyhow::Error,
		operation: impl Into<String>,
	) -> bool {
		let _ = txn.cancel().await;
		Self::retry_index_operation_conflict(err, operation).await
	}

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
	/// The datastore is returned shared, because it starts background
	/// maintenance tasks that hold a [`Weak`](std::sync::Weak) reference to it.
	/// Because those tasks write, this also settles the datastore's storage
	/// version before any of them starts, so the datastore is handed back
	/// already stamped; [`Self::check_version`] is still what checks that this
	/// build can read what was found, and what runs any migrations owed.
	///
	/// Use [`Builder::without_maintenance_tasks`] for a quiescent datastore, or
	/// for one whose tasks start later under the caller's own ordering.
	pub async fn new(path: &str) -> Result<Arc<Self>> {
		Builder::new().build_with_path(path).await
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

	/// How the requests for compiled top-level statements have been served
	/// since this datastore started.
	///
	/// A request's read-only statement is served the compilation kept for the
	/// same statement text under the same namespace, database, principal class
	/// and capabilities (a hit), or compiled and kept for later requests (a
	/// miss). A kept compilation reads the request's variables when it runs, so
	/// requests that bind other values to them hit it. One whose catalog
	/// dependencies no longer hold when it is looked up counts as an
	/// invalidation and a miss.
	pub fn statement_cache_stats(&self) -> StatementCacheStats {
		let stats = self.compiled_units.statement_stats();
		StatementCacheStats {
			hits: stats.hits + stats.waits,
			misses: stats.compiles + stats.private,
			invalidations: stats.stale,
		}
	}

	/// The currently installed observer. Cheap to clone; internal handle is
	/// an `Arc`.
	///
	/// Exposed so higher layers (server, SDK) can emit transport-layer events
	/// such as session connect/disconnect that the core engine never sees.
	pub fn observer(&self) -> &Arc<dyn ExecutionObserver> {
		&self.observer
	}

	/// Create a new datastore with the same persistent data (inner), with
	/// flushed cache. Simulating a server restart
	///
	/// The restarted datastore carries **no** maintenance tasks: it is returned
	/// unshared, so there is nothing for a task to hold a `Weak` to. Callers
	/// that need them call [`Self::start_maintenance_tasks`] once they have
	/// shared it.
	pub fn restart(self) -> Self {
		self.buckets.clear();
		// One registry per datastore, shared by the executor and by the index
		// builder, so a build resolves the same names a query does.
		let function_registry = Arc::new(FunctionRegistry::with_builtins());
		Self {
			shutdown: CancellationToken::new(),
			maintenance: parking_lot::Mutex::new(None),
			engine_options: self.engine_options,
			shutdown_lock: tokio::sync::Mutex::new(()),
			id: self.id,
			auth_enabled: self.auth_enabled,
			dynamic_configuration: DynamicConfiguration::default(),
			slow_log: self.slow_log,
			transaction_timeout: self.transaction_timeout,
			capabilities: ArcSwap::new(self.capabilities.load_full()),
			live_query_broker: self.live_query_broker,
			// Fresh router cursor: a restarted node re-establishes its baseline.
			live_query_router: Arc::new(LiveQueryRouter::new()),
			scheduler: self.scheduler,
			memory_governor: self.memory_governor,
			temp_store: self.temp_store,
			#[cfg(test)]
			no_temp_store: self.no_temp_store,
			memory_host: Default::default(),
			http_endpoint: self.http_endpoint,
			index_stores: IndexStores::new(
				self.config.idx.hnsw_cache_size,
				self.config.idx.diskann_cache_size,
				self.config.idx.graph_resolve_cache_size,
				self.config.idx.segment_dictionary_path.clone(),
			),
			index_builder: IndexBuilder::new(self.transaction_factory.clone()),
			compaction_swept_at: parking_lot::Mutex::new(None),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory,
			cache: Arc::new(DatastoreCache::new(self.config.datastore.datastore_cache_size)),
			#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
			graphql_schema_cache: crate::graphql::cache::GraphQLSchemaCache::default(),
			function_registry,
			live_units: Arc::default(),
			compiled_units: Arc::new(crate::exec::vm::units::CompiledUnits::for_config(
				&self.config.exec,
			)),
			edge_compiled_units: Arc::new(crate::exec::vm::units::CompiledUnits::for_config(
				&self.config.exec,
			)),
			buckets: self.buckets,
			sequences: Sequences::new(self.transaction_factory.clone(), self.id),
			transaction_factory: self.transaction_factory,
			created_here: std::sync::atomic::AtomicBool::new(false),
			feature_level_settled: std::sync::atomic::AtomicBool::new(
				self.feature_level_settled.load(std::sync::atomic::Ordering::Acquire),
			),
			triggers: self.triggers,
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new(
				self.config.surrealism.surrealism_cache_size,
				self.surrealism_cache.runner(),
			)),
			#[cfg(feature = "surrealism")]
			lazy_extensions: self.lazy_extensions,
			#[cfg(feature = "http")]
			http_client: self.http_client,
			observer: self.observer,
			config: self.config,
			parser_config: self.parser_config,
		}
	}

	/// Create a test-only datastore facade that shares the same durable KV engine
	/// while resetting process-local state.
	///
	/// This lets unit tests model two SurrealDB compute nodes connected to the
	/// same storage backend without starting an external service. The cloned
	/// facade deliberately reuses the transaction factory, but gets its own node
	/// id, index builder, index stores, datastore cache, sequences, and other
	/// process-local caches. Tests that exercise cluster liveness should call
	/// [`Self::insert_node`] for both the original datastore and the fork.
	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) fn fork_for_test_with_node_id(&self, id: Uuid) -> Self {
		let transaction_factory = self.transaction_factory.clone();
		let function_registry = Arc::new(FunctionRegistry::with_builtins());
		Self {
			// A fork is unshared, so it carries no maintenance tasks.
			shutdown: CancellationToken::new(),
			maintenance: parking_lot::Mutex::new(None),
			engine_options: self.engine_options,
			shutdown_lock: tokio::sync::Mutex::new(()),
			id,
			auth_enabled: self.auth_enabled,
			dynamic_configuration: self.dynamic_configuration.clone(),
			slow_log: self.slow_log.clone(),
			transaction_timeout: self.transaction_timeout,
			capabilities: ArcSwap::new(self.capabilities.load_full()),
			live_query_broker: self.live_query_broker.clone(),
			// A fork models a separate node, so it gets its own router cursor.
			live_query_router: Arc::new(LiveQueryRouter::new()),
			scheduler: self.scheduler.clone(),
			memory_governor: self.memory_governor.clone(),
			temp_store: self.temp_store.clone(),
			#[cfg(test)]
			no_temp_store: self.no_temp_store,
			memory_host: Default::default(),
			http_endpoint: self.http_endpoint.clone(),
			index_stores: IndexStores::new(
				self.config.idx.hnsw_cache_size,
				self.config.idx.diskann_cache_size,
				self.config.idx.graph_resolve_cache_size,
				self.config.idx.segment_dictionary_path.clone(),
			),
			index_builder: IndexBuilder::new(transaction_factory.clone()),
			compaction_swept_at: parking_lot::Mutex::new(None),
			#[cfg(storage)]
			temporary_directory: self.temporary_directory.clone(),
			cache: Arc::new(DatastoreCache::new(self.config.datastore.datastore_cache_size)),
			#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
			graphql_schema_cache: crate::graphql::cache::GraphQLSchemaCache::default(),
			function_registry,
			live_units: Arc::default(),
			compiled_units: Arc::new(crate::exec::vm::units::CompiledUnits::for_config(
				&self.config.exec,
			)),
			edge_compiled_units: Arc::new(crate::exec::vm::units::CompiledUnits::for_config(
				&self.config.exec,
			)),
			buckets: self.buckets.clone(),
			sequences: Sequences::new(transaction_factory.clone(), id),
			transaction_factory,
			created_here: std::sync::atomic::AtomicBool::new(false),
			feature_level_settled: std::sync::atomic::AtomicBool::new(
				self.feature_level_settled.load(std::sync::atomic::Ordering::Acquire),
			),
			triggers: Arc::clone(&self.triggers),
			#[cfg(feature = "surrealism")]
			surrealism_cache: Arc::new(SurrealismCache::new(
				self.config.surrealism.surrealism_cache_size,
				self.surrealism_cache.runner(),
			)),
			#[cfg(feature = "surrealism")]
			lazy_extensions: self.lazy_extensions,
			#[cfg(feature = "http")]
			http_client: ArcSwap::new(self.http_client.load_full()),
			observer: Arc::clone(&self.observer),
			config: Arc::clone(&self.config),
			parser_config: Arc::clone(&self.parser_config),
		}
	}

	/// Set the node id for this datastore.
	pub fn with_node_id(mut self, id: Uuid) -> Self {
		self.id = id;
		self
	}

	/// Set a global transaction timeout for this Datastore
	pub fn with_transaction_timeout(mut self, duration: Option<Duration>) -> Self {
		self.transaction_timeout = duration;
		self
	}

	/// Get the configured transaction timeout, if any
	pub(crate) fn transaction_timeout(&self) -> Option<Duration> {
		self.transaction_timeout
	}

	/// The configured write-cardinality limit for statement transactions
	/// (`transaction_max_write_keys`), or `None` when the guard is disabled.
	pub(crate) fn transaction_max_write_keys(&self) -> Option<std::num::NonZeroU64> {
		std::num::NonZeroU64::new(self.transaction_factory.max_write_keys())
	}

	/// Get the configured global query timeout, if any.
	///
	/// This is the deadline set by `--query-timeout` /
	/// `SURREAL_QUERY_TIMEOUT`. Beyond being applied as an executor deadline
	/// in [`Self::setup_ctx`], the transports reuse this value as a
	/// wall-clock guard so a non-yielding hang (or engine work not bounded by
	/// the deadline) still terminates the request. A return value of `None`
	/// means no timeout is enforced.
	pub fn query_timeout(&self) -> Option<Duration> {
		self.dynamic_configuration.get_query_timeout()
	}

	/// Returns the broker used to flush live-query notifications after commit.
	pub(crate) fn live_query_broker(&self) -> Option<Arc<dyn MessageBroker>> {
		self.live_query_broker.clone()
	}

	/// Looks up the public HTTP endpoint that the cluster member with `node_id`
	/// has published on its `Node` catalog row.
	///
	/// Returns `Ok(None)` when the row exists but no endpoint is set, or when
	/// no such node has registered. Used by clustered live-query relays to
	/// resolve the target node's address at delivery time without consulting
	/// any in-memory cluster topology.
	pub async fn lookup_node_endpoint(&self, node_id: Uuid) -> Result<Option<String>> {
		let txn = self.transaction(Read).await?;
		let key = NodeKey {
			nd: node_id,
		};
		let res = txn.get_key(&key, None).await?;
		// Always cancel a read transaction; we don't write through it.
		let _ = txn.cancel().await;
		Ok(res.and_then(|node: Node| node.http_endpoint))
	}

	/// Set the scheduler a running program yields through once it has spent
	/// its quantum of work, for a host that drives its own event loop. Without
	/// one, a program yields to the tokio runtime it runs on, or through a
	/// future that is pending once where there is none.
	pub fn with_scheduler(mut self, scheduler: Arc<dyn crate::exec::scheduler::Scheduler>) -> Self {
		self.scheduler = Some(scheduler);
		self
	}

	/// Set the governor that grants each query its memory, for a host that
	/// sizes and shares memory itself (an edge isolate). Without one, the
	/// datastore holds its queries to the `memory_limit` setting.
	pub fn with_memory_governor(
		mut self,
		governor: Arc<dyn crate::exec::memory::MemoryGovernor>,
	) -> Self {
		self.memory_governor = Some(governor);
		self
	}

	/// Set the store a query's breakers spill to once they outgrow their
	/// query's memory. Without one, a native datastore spills to files in its
	/// temporary directory, or the system's.
	pub fn with_temp_store(mut self, store: Arc<dyn crate::exec::memory::TempStore>) -> Self {
		self.temp_store = Some(store);
		self
	}

	/// The governor and store every query of this datastore shares.
	///
	/// A host's own are used where it supplied them. Otherwise a native
	/// datastore spills to a [`FileTempStore`](crate::exec::memory::FileTempStore)
	/// in its temporary directory (the system's when none is set) and holds
	/// its queries to `memory_limit` bytes; a datastore with no store to spill
	/// to bounds nothing, since a breaker that cannot spill gains nothing from a
	/// limit but waits and overcommits.
	pub(crate) fn memory_host(&self) -> crate::exec::memory::MemoryHost {
		use crate::exec::memory::{FairGovernor, MemoryHost, MemorySettings, Unlimited};
		self.memory_host
			.get_or_init(|| {
				let exec = &self.config.exec;
				let settings = MemorySettings {
					floor: exec.query_memory_minimum,
					fan_in: exec.spill_merge_fan_in,
					chunk: exec.memory_reserve_chunk,
					wait: std::time::Duration::from_millis(exec.memory_wait),
					max: exec.query_memory_max(),
				};
				let store = self.temp_store.clone().or_else(|| self.default_temp_store());
				#[cfg(test)]
				let store = store.filter(|_| !self.no_temp_store);
				let governor: Arc<dyn crate::exec::memory::MemoryGovernor> =
					match (&self.memory_governor, &store) {
						(Some(governor), _) => Arc::clone(governor),
						(None, Some(_)) => Arc::new(FairGovernor::new(exec.memory_limit())),
						(None, None) => Arc::new(Unlimited),
					};
				MemoryHost {
					governor,
					store,
					settings,
				}
			})
			.clone()
	}

	/// The node memory limit the datastore holds its queries to, or `None`
	/// where its governor has none.
	pub fn memory_limit(&self) -> Option<usize> {
		self.memory_host().governor.limit()
	}

	/// The bytes a single query may hold that it cannot spill before it fails
	/// with the memory threshold error.
	pub fn query_memory_max(&self) -> usize {
		self.memory_host().settings.max
	}

	/// The morsel settings the datastore holds, as `(key, value)` pairs in the form
	/// the configuration reads them, for a harness to check that a setting it
	/// passed reached the store.
	pub fn morsel_knobs(&self) -> Vec<(&'static str, String)> {
		let exec = &self.config.exec;
		vec![
			("morsel_split_rows", exec.morsel_split_rows.to_string()),
			("morsel_target_bytes", exec.morsel_target_bytes.to_string()),
			("morsel_places", exec.morsel_places.to_string()),
			("morsel_oltp_reserve", exec.morsel_oltp_reserve.to_string()),
			("morsel_window_bytes", exec.morsel_window_bytes.to_string()),
			("morsel_steal_always", exec.morsel_steal_always.to_string()),
			("morsel_place_quantum", exec.morsel_place_quantum.to_string()),
			("morsel_explain_analyze", exec.morsel_explain_analyze.to_string()),
		]
	}

	/// The longest a query waits in all for other queries to give memory back
	/// before it proceeds past the node limit.
	pub fn memory_wait(&self) -> std::time::Duration {
		self.memory_host().settings.wait
	}

	#[cfg(all(storage, not(target_family = "wasm")))]
	fn default_temp_store(&self) -> Option<Arc<dyn crate::exec::memory::TempStore>> {
		let parent = match &self.temporary_directory {
			Some(dir) => dir.as_ref().clone(),
			None => std::env::temp_dir().join("surrealdb"),
		};
		match crate::exec::memory::FileTempStore::open(
			&parent,
			self.config.exec.spill_part_size,
			None,
		) {
			Ok(store) => Some(Arc::new(store)),
			Err(error) => {
				warn!("Queries will not spill to disk: {error:#}");
				None
			}
		}
	}

	#[cfg(not(all(storage, not(target_family = "wasm"))))]
	fn default_temp_store(&self) -> Option<Arc<dyn crate::exec::memory::TempStore>> {
		None
	}

	#[cfg(storage)]
	/// Set a temporary directory for ordering of large result sets
	pub fn with_temporary_directory(mut self, path: Option<PathBuf>) -> Self {
		self.temporary_directory = path.map(Arc::new);
		self
	}

	/// Configure whether extensions are prepared on first invocation rather
	/// than at startup.
	#[cfg(feature = "surrealism")]
	pub fn with_lazy_extensions(mut self, lazy: bool) -> Self {
		self.lazy_extensions = lazy;
		self
	}

	/// Returns `true` if extensions are prepared on first invocation.
	#[cfg(feature = "surrealism")]
	pub fn is_lazy_extensions(&self) -> bool {
		self.lazy_extensions
	}

	pub fn index_store(&self) -> &IndexStores {
		&self.index_stores
	}

	/// Set whether authentication is enabled for this Datastore.
	pub fn with_auth_enabled(mut self, auth_enabled: bool) -> Self {
		self.auth_enabled = auth_enabled;
		self
	}

	/// Is authentication enabled for this Datastore?
	pub fn is_auth_enabled(&self) -> bool {
		self.auth_enabled
	}

	pub fn id(&self) -> Uuid {
		self.id
	}

	/// Does the datastore allow excecuting an RPC method?
	///
	/// Dispatch enforces this itself, so a transport does not have to. It is
	/// public so a transport can also *report* the answer ahead of time: the
	/// gRPC service advertises the methods an operator has denied, so a client
	/// can avoid calls it already knows will be refused.
	pub fn allows_rpc_method(&self, method_target: &MethodTarget) -> bool {
		self.capabilities.load().allows_rpc_method(method_target)
	}

	/// Does the datastore allow requesting an HTTP route?
	/// This function needs to be public to allow access from the CLI crate.
	pub fn allows_http_route(&self, route_target: &RouteTarget) -> bool {
		self.capabilities.load().allows_http_route(route_target)
	}

	/// Is the user allowed to query?
	pub fn allows_query_by_subject(&self, subject: impl Into<ArbitraryQueryTarget>) -> bool {
		self.capabilities.load().allows_query(&subject.into())
	}

	/// Is the user allowed to invoke the `eval::*` functions?
	pub fn allows_eval_query_by_subject(&self, subject: impl Into<EvalQueryTarget>) -> bool {
		self.capabilities.load().allows_eval_query(&subject.into())
	}

	/// Does the datastore allow connections to a network target?
	#[cfg(feature = "jwks")]
	pub(crate) fn allows_network_target(&self, net_target: &NetTarget) -> bool {
		self.capabilities.load().allows_network_target(net_target)
	}

	/// The datastore's current capabilities snapshot.
	pub fn get_capabilities(&self) -> Arc<Capabilities> {
		self.capabilities.load_full()
	}

	/// Replace the datastore's capabilities at runtime.
	///
	/// The swap is atomic and lock-free. In-flight queries keep the snapshot
	/// they captured when they started (the executor clones the `Arc` once at
	/// query start), so only queries begun after this call observe the new
	/// capabilities. This cannot exceed what the binary was compiled to
	/// support — a capability whose machinery is not compiled in stays inert
	/// regardless of what is set here.
	///
	/// When the `http` feature is enabled this also rebuilds the outbound HTTP
	/// client so its redirect and DNS net filter track the new `allow_net` /
	/// `deny_net`. Without that, a runtime net *tightening* would be enforced on
	/// a request's initial URL (checked against the live snapshot) but not on
	/// redirect hops, whose filter is baked in at client construction. Building
	/// the client can fail, hence the `Result`; on failure the capabilities are
	/// left unchanged.
	pub fn set_capabilities(&self, capabilities: Capabilities) -> anyhow::Result<()> {
		#[cfg(feature = "http")]
		let http_client = HttpClient::new(
			capabilities.allow_net.clone(),
			capabilities.deny_net.clone(),
			&self.config.http,
		)?;
		self.capabilities.store(Arc::new(capabilities));
		#[cfg(feature = "http")]
		self.http_client.store(Arc::new(http_client));
		Ok(())
	}

	#[cfg(feature = "jwks")]
	pub(crate) fn cache(&self) -> &Arc<DatastoreCache> {
		&self.cache
	}

	/// The function registry every query on this datastore resolves names
	/// through. Handed to the executor, which seats it on the `RootContext`
	/// of each statement.
	pub(crate) fn function_registry(&self) -> &Arc<FunctionRegistry> {
		&self.function_registry
	}

	pub(super) fn clock_now(&self) -> Timestamp {
		SystemClock::new().now()
	}

	/// Verifies the datastore's storage version and applies any data migrations
	/// it owes, returning the version the datastore carried when it was settled,
	/// before any raise of its feature level this call makes, and whether this
	/// process created the datastore.
	///
	/// A datastore stamped [`MajorVersion::LEGACY`] or [`MajorVersion::LATEST`]
	/// opens. The raise of its feature level stamps [`MajorVersion::LATEST`] in
	/// the transaction that records it, so a release that checks only the stamp
	/// refuses the datastore from then on.
	///
	/// **Call this before serving any query against a datastore that may
	/// predate this build.** A datastore is not fully readable until its
	/// migrations have run: [`Datastore::new`] and the builder do not call this,
	/// so an embedder that skips it against pre-3.3 storage sees sequences that
	/// appear not to exist — which `DEFINE SEQUENCE ... IF NOT EXISTS` will then
	/// recreate, resetting an allocator that has already issued values.
	///
	/// A datastore whose ledger records features this build does not maintain is
	/// open read-only: nothing is migrated, stamped, pinned or raised, and every
	/// write other than this node's own membership rows is refused (see
	/// [`Self::is_read_only`]). A datastore at [`FeatureLevel::Legacy`] whose
	/// storage engine admits no other writer is raised to
	/// [`Release::FIRST_STORED`] here, unless the Legacy hold keeps it (see
	/// [`Builder::with_legacy_feature_level`](self::builder::Builder::with_legacy_feature_level)).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn check_version(&self) -> Result<(MajorVersion, bool)> {
		// A datastore built without startup transactions reads its feature level
		// here. It precedes the version stamp, which would otherwise make a store
		// this process creates read as an upgraded one.
		if !self.feature_level_settled.load(std::sync::atomic::Ordering::Acquire) {
			self.settle_feature_level().await?;
		}
		self.establish_router_baseline().await?;
		let (version, is_new) = self.settle_storage_version().await?;
		// Check this build opens the stamp: the latest, or that of a store not yet raised.
		if !version.is_supported() {
			bail!(DatastoreError::OutdatedStorageVersion {
				expected: MajorVersion::latest().into(),
				actual: version.into(),
			});
		}
		// `get_version` reports a datastore as new only on the start that wrote
		// `!v`. Everything below can fail transiently and be retried by the
		// caller, and that retry finds the key present — so the flag is latched on
		// the datastore rather than re-derived, or a caller keying default
		// namespace creation off it would silently skip that on the second pass.
		let is_new =
			self.created_here.fetch_or(is_new, std::sync::atomic::Ordering::SeqCst) || is_new;
		// Bring the datastore's semantic version stamp up to this build, applying
		// any data migrations the gap between the two calls for. Not wrapped in
		// `retry`, which caps each attempt at ten seconds: a migration may run for
		// much longer, and the driver handles contention itself through a task
		// lease.
		migration::run(self, is_new).await?;
		// A read-only datastore records no feature state, so neither the operator's pin
		// nor the single-process raise applies to it. The migration driver may be what
		// found it read-only, so this reads the mark after it ran.
		if !self.is_read_only() {
			// Raise the feature level to the operator's pin, now that the version and
			// the migrations are settled.
			self.apply_feature_level_pin().await?;
			// A store whose engine admits no other writer has no node to wait for and
			// no operator to coordinate with, so it moves to the first stored level
			// itself.
			self.raise_sole_writer_store().await?;
		}
		// Everything ok
		Ok((version, is_new))
	}

	/// Attaches a store to an edge build, in place of [`Self::check_version`], which runs
	/// migrations and raises feature levels and which the edge build never calls.
	///
	/// Refuses the store with an error when any of these holds:
	///
	/// * `verdict` is [`migration::LedgerVerdict::Refused`], because the ledger records bytes this
	///   build cannot read;
	/// * the major version (`!v`) is not one this build supports;
	/// * a migration this build ships has not been applied;
	/// * the version stamp is missing on a store this call did not create, or is not exactly this
	///   build's release.
	///
	/// A store that is new on this start is written what [`migration::run`] writes for a new store:
	/// the major version marker, the baseline ledger rows, and the stamp with its history entry. An
	/// existing store is written nothing. A `verdict` of [`migration::LedgerVerdict::ReadOnly`]
	/// opens the store read-only and checks nothing further.
	///
	/// Never runs a migration, pins or raises the feature level, or starts a maintenance task, and
	/// leaves the router baseline to the builder.
	pub(crate) async fn attach_edge(&self, verdict: migration::LedgerVerdict) -> Result<()> {
		if let migration::LedgerVerdict::Refused {
			migrations,
		} = verdict
		{
			bail!(DatastoreError::MigratedBeyondStorageVersion {
				stored: self
					.storage_version()
					.await?
					.map(|s| s.to_string())
					.unwrap_or_else(|| "unknown".into()),
				running: StorageVersion::current().to_string(),
				migrations,
			});
		}
		let (version, is_new) = self.settle_storage_version().await?;
		if !version.is_supported() {
			bail!(DatastoreError::OutdatedStorageVersion {
				expected: MajorVersion::latest().into(),
				actual: version.into(),
			});
		}
		let is_new =
			self.created_here.fetch_or(is_new, std::sync::atomic::Ordering::SeqCst) || is_new;
		if let migration::LedgerVerdict::ReadOnly {
			features,
		} = verdict
		{
			self.open_read_only(features);
			return Ok(());
		}
		migration::attach(self, is_new).await
	}

	/// Settles the datastore's major storage version, stamping it if it carries
	/// none, and reports whether this call was the one that stamped it.
	///
	/// The marker half of [`Self::check_version`], separated because the builder
	/// owes it before it starts any writer but must not do the rest: the
	/// migrations that follow the gate run under a lease with no bound on how
	/// long they take, which is not something a constructor may hold a caller
	/// for.
	///
	/// Retries because the storage layer may not be able to serve a transaction
	/// yet, and because concurrent instances conflict over the version key.
	/// Callers that want the gate — the check that this build can read what it
	/// found — want [`Self::check_version`].
	pub(crate) async fn settle_storage_version(&self) -> Result<(MajorVersion, bool)> {
		Self::retry_bootstrap("Check version", || self.get_version()).await
	}

	/// The full semantic version the datastore has been advanced to.
	///
	/// `None` for a datastore last written before 3.3, the release that
	/// introduced the stamp. Its major version is still available from
	/// [`Self::get_version`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn storage_version(&self) -> Result<Option<StorageVersion>> {
		let txn = self.transaction(Read).await?;
		let version = catch!(txn, txn.get_key(&StorageVersionKey {}, None).await);
		txn.cancel().await?;
		Ok(version)
	}

	/// Every transition in this datastore's version history, oldest first.
	///
	/// One entry per transition, not per startup. An upward entry (its `from` is `None` or below
	/// its `to`) records a release advancing the stamp, or a release at the stamp finishing the
	/// rechecks that earlier releases' writes made necessary. A downward note (its `from`, the
	/// stamp, is above its `to`, the running release) records that a release below the stamp opened
	/// the datastore. A node that starts against a datastore already stamped at its own version, or
	/// a release that restarts below the stamp, adds nothing.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn version_history(&self) -> Result<Vec<VersionHistoryEntry>> {
		let txn = self.transaction(Read).await?;
		let range = catch!(txn, VersionHistoryPrefix {}.range());
		let entries = catch!(txn, txn.getr(range, None).await);
		txn.cancel().await?;
		Ok(entries.into_iter().map(|(_, entry)| entry).collect())
	}

	/// Every data migration applied to this datastore, in the order they were
	/// declared.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	/// Paired with the ledger id the entry was filed under, which is a
	/// migration's identity; the name in the record is documentation and may be
	/// reworded between releases.
	pub async fn applied_migrations(&self) -> Result<Vec<(u32, MigrationRecord)>> {
		Ok(self
			.applied_migration_ids()
			.await?
			.into_iter()
			.filter_map(|(id, record)| record.map(|r| (id, r)))
			.collect())
	}

	/// Every applied migration's ledger id, with its record when that record is
	/// readable by this build.
	///
	/// A record written in a revision this build does not know decodes to
	/// `None` rather than failing the call. The id comes from the key, and the
	/// key's presence is what marks a migration as applied — so a record this
	/// build cannot read must still be reportable, since that is exactly the
	/// state a downgrade needs to be told about.
	pub async fn applied_migration_ids(&self) -> Result<Vec<(u32, Option<MigrationRecord>)>> {
		let txn = self.transaction(Read).await?;
		let range = catch!(txn, MigrationPrefix {}.range());
		let rows = catch!(txn, txn.getr_raw(range, None).await);
		txn.cancel().await?;
		rows.into_iter()
			.map(|(key, value)| {
				let id = MigrationKey::decode_key(&key)?.id;
				Ok((id, MigrationRecord::kv_decode_value(&value, ()).ok()))
			})
			.collect()
	}

	/// Reads the datastore's major storage version, stamping it on a datastore
	/// that carries none, and reports whether this call was the one that
	/// stamped it.
	///
	/// A datastore that carries `!v` is answered from a read transaction, so a
	/// read-only datastore, which refuses write transactions, settles its version
	/// here. Only an absent `!v` opens the write transaction that stamps it.
	///
	/// # What `!v`'s absence means
	///
	/// `!v` was introduced in 2.0, so a datastore that holds data but no `!v`
	/// was either written by a build that predates it — a genuine v1 store,
	/// which must be refused as out of date — or created by a later build whose
	/// bootstrap was interrupted after something else wrote but before `!v`
	/// landed. Nothing in the bytes tells those apart, so the second case
	/// records itself: [`BootstrapKey`] is committed before `!v` and removed in
	/// the same transaction that writes it, and its presence is what licenses
	/// this call to complete the interrupted bootstrap instead of refusing it.
	/// Without it, `MajorVersion::v1()` is the only safe reading, and an
	/// interrupted bootstrap would be stamped v1 permanently.
	///
	/// # Ordering
	///
	/// The sentinel is only a fallback: nothing may write to a datastore before
	/// its version is settled, with two exceptions beside the sentinel itself.
	/// That ordering is structural — the builder settles the version before it
	/// starts any maintenance task, and a caller that starts them itself does so
	/// after this has run (see [`Datastore::start_maintenance_tasks`]). The
	/// sentinel is what keeps a writer that escapes it anyway — a future job, a
	/// storage engine with keys of its own — from turning a fresh datastore into
	/// an unstartable one.
	///
	/// The first exception is `!fl`, the feature level a new datastore records
	/// when the builder creates it ([`Datastore::settle_feature_level`]). Like
	/// `!bv` it sorts before `!v`, outside the range the emptiness probe scans, so
	/// it never makes a datastore that holds nothing else read as data of unknown
	/// provenance. It is written only into a datastore that holds no key but
	/// `!bv`.
	///
	/// The second is [`TransactionBuilderFactory::before_check_version`], which
	/// may fill the datastore from another keyspace before [`Self::check_version`]
	/// runs. It writes `!v` last and never writes the sentinel, so a fill cut
	/// short reads here as storage from before `!v` and is refused, rather than
	/// completed as a new datastore with its migrations recorded and never run.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn get_version(&self) -> Result<(MajorVersion, bool)> {
		#[cfg(test)]
		maybe_inject_non_retryable_error(NonRetryableErrorSite::VersionBootstrapBegin, self.id)?;
		// The common case is a stamped datastore, which needs no write transaction.
		{
			let txn = self.transaction(Read).await?;
			let stamped = catch!(txn, txn.get_key(&VersionKey {}, None).await);
			txn.cancel().await?;
			if let Some(version) = stamped {
				return Ok((version, false));
			}
		}
		// Start a new writeable transaction
		let txn = self.transaction(Write).await?.enclose();
		// Create the key where the version is stored
		let key = VersionKey {};
		// Check if a version is already set in storage
		if let Some(val) = catch!(txn, txn.get_key(&key, None).await) {
			// We didn't write anything, so just rollback
			catch!(txn, txn.cancel().await);
			// Return the current version
			return Ok((val, false));
		}
		// No version is set. Either nothing has ever stamped this datastore, or
		// a previous start began stamping it and did not finish.
		if let Some(target) = catch!(txn, txn.get_key(&BootstrapKey {}, None).await) {
			// An interrupted bootstrap left the sentinel behind, so whatever is
			// in storage was written by a versioned build and no pre-versioning
			// layout can be present: the sentinel is only ever written after the
			// probe below has found the datastore empty, and from that moment
			// the only writers are versioned builds, none of which can serve a
			// query until this call has passed. That is what licenses completing
			// the job rather than refusing it — but not at just any version.
			//
			// The two directions are not symmetric. Finishing a bootstrap an
			// *older* build began is sound at this build's own version: the
			// residue such a build can leave is cluster state a newer build
			// reads, and no user data can exist to owe a migration, because
			// serving a query requires the stamp this call has not yet written.
			// Finishing one a *newer* build began is not: stamping the datastore
			// down a major version would pass this build's own gate against
			// bytes it may not understand, and would leave the build that
			// started the job reading back a stamp it refuses. So that case is
			// declined without writing, which keeps the sentinel intact for the
			// build that can finish it, and returns the target so the gate above
			// reports what is actually on disk.
			if u16::from(target) > MajorVersion::LATEST {
				catch!(txn, txn.cancel().await);
				warn!(
					target: TARGET,
					target = u16::from(target),
					running = MajorVersion::LATEST,
					"This datastore's initialisation was begun by a newer version of SurrealDB \
					and can only be completed by it. Run that version, or use a clean storage \
					directory if this is intended to be a new instance."
				);
				return Ok((target, false));
			}
			let version = MajorVersion::latest();
			warn!(
				target: TARGET,
				target = u16::from(target),
				version = u16::from(version),
				"Completing a datastore initialisation that a previous start left unfinished."
			);
			catch!(txn, txn.replace_key(&key, &version).await);
			catch!(txn, txn.del_key(&BootstrapKey {}).await);
			catch!(txn, txn.commit().await);
			return Ok((version, true));
		}
		// Fetch any key at all, other than the version keys themselves. This has
		// to span the whole byte space rather than the declared root: the point
		// of the probe is data this release does not describe, which by
		// definition need not sit under `/`. Finding any such key means the
		// store predates versioning and must not be stamped as current.
		let range = RawRange::every_key_after(&VersionKey {}.encode_key()?);
		let keys = catch!(txn, txn.keys_raw(range, 1, 0, None).await);
		if !keys.is_empty() {
			// There were keys in storage, so this is an upgrade.
			// Log the first key found for diagnostic purposes.
			warn!(
				target: TARGET,
				first_key = ?keys.first().map(|k| format!("{:?}", k)),
				"No version key found but existing data detected in storage. \
				This storage contains data from a previous SurrealDB version. \
				The server will not start until the data is migrated or removed."
			);
			let version = MajorVersion::v1();
			catch!(txn, txn.replace_key(&key, &version).await);
			catch!(txn, txn.commit().await);
			return Ok((version, true));
		}
		// The datastore is empty, so this start is creating it. Claim it with the
		// sentinel in a transaction of its own: the two writes cannot share one,
		// because the whole point is for the sentinel to outlive a failure of the
		// stamp below. The sentinel sorts before `!v` and so below the probe
		// above, which is what keeps a datastore holding nothing else reading as
		// empty rather than as data of unknown provenance.
		let version = MajorVersion::latest();
		catch!(txn, txn.set_key(&BootstrapKey {}, &version).await);
		catch!(txn, txn.commit().await);
		// Stamp the version and retire the sentinel. Should this fail, the next
		// attempt takes the resume branch above and finishes the job.
		#[cfg(test)]
		maybe_inject_non_retryable_error(NonRetryableErrorSite::VersionBootstrapStamp, self.id)?;
		self.finish_bootstrap(version).await
	}

	/// Stamps `!v` and retires the sentinel, completing a bootstrap this node
	/// claimed.
	///
	/// Re-reads `!v` rather than writing over it blindly, for two reasons that
	/// both come from this being a second transaction. Another node may have
	/// claimed the same empty datastore, seen this node's sentinel and finished
	/// the job in between, in which case this node did not create the datastore
	/// and must not report that it did — two nodes both returning `is_new` have
	/// both of them creating the default namespace, and the loser fails startup
	/// on a namespace that already exists. And reading the key in the same
	/// transaction that writes it is what arms a write conflict on backends that
	/// detect one, which a blind replace would not.
	pub(crate) async fn finish_bootstrap(
		&self,
		version: MajorVersion,
	) -> Result<(MajorVersion, bool)> {
		let key = VersionKey {};
		let txn = self.transaction(Write).await?.enclose();
		if let Some(existing) = catch!(txn, txn.get_key(&key, None).await) {
			catch!(txn, txn.cancel().await);
			return Ok((existing, false));
		}
		catch!(txn, txn.replace_key(&key, &version).await);
		catch!(txn, txn.del_key(&BootstrapKey {}).await);
		catch!(txn, txn.commit().await);
		Ok((version, true))
	}

	// --------------------------------------------------
	// Feature level functions
	// --------------------------------------------------

	/// The cell holding the feature level this datastore last read, shared by
	/// every transaction of the store and by every datastore forked from this
	/// one.
	fn feature_cell(&self) -> &Arc<FeatureLevelCell> {
		self.transaction_factory.feature_level()
	}

	/// The release the operator pinned the feature level to, as the builder
	/// validated it.
	fn feature_level_pin(&self) -> Option<Release> {
		match self.config.datastore.feature_level {
			config::FeatureLevelSetting::Pin(release) => Some(release),
			config::FeatureLevelSetting::Unset
			| config::FeatureLevelSetting::Legacy
			| config::FeatureLevelSetting::Invalid => None,
		}
	}

	/// Whether the operator asked to keep a store that records no feature level at that
	/// level, as the builder validated it.
	fn holds_legacy(&self) -> bool {
		self.config.datastore.feature_level.holds_legacy()
	}

	/// The release a new store is created at: the validated pin, or this
	/// build's release.
	fn initial_feature_level(&self) -> Release {
		self.feature_level_pin().unwrap_or(Release::CURRENT)
	}

	/// The store's recorded feature level: the newest release whose features
	/// the store may use, and the newest release whose features anything in it
	/// uses. A store that records none is at [`FeatureLevel::Legacy`], where
	/// the catalog is written in the encoding every 3.x release reads.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn feature_level(&self) -> Result<FeatureLevelState> {
		let txn = self.transaction(Read).await?;
		let state = catch!(txn, txn.feature_level_state().await);
		txn.cancel().await?;
		Ok(state)
	}

	/// Raises the store's feature level to `target`, so that this and later
	/// releases may write what `target` reads. A store already at or above
	/// `target` is left as it is.
	///
	/// Refused while a node that is running (it has heartbeat within
	/// [`NODE_LIVENESS`]) records a release older than `target`, or none: that
	/// node could not read what the raised level lets the others write. The
	/// check is a guard against a partial rollout, not a fence, so a node that
	/// joins after the raise reads the level when it starts and writes no more
	/// than its own release reads.
	///
	/// # Errors
	///
	/// - [`DatastoreError::FeatureLevelAboveRelease`] when `target` is newer than this build;
	/// - [`DatastoreError::FeatureLevelBelowFirst`] when `target` precedes the first release with a
	///   feature level;
	/// - [`DatastoreError::FeatureLevelNodeBehind`] when a running node is older than `target`.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn raise_feature_level(&self, target: Release) -> Result<()> {
		Self::check_feature_level_target(target)?;
		Self::retry("Raise feature level", || {
			self.raise_feature_level_attempt(target, NodeCheck::Require)
		})
		.await
	}

	/// Lowers the store's feature level to `target`. A store already at or
	/// below `target` is left as it is.
	///
	/// Refused while the store records that something in it uses the features of
	/// a release newer than `target`. That record only rises, so a store that
	/// has used a newer feature can never be lowered below it.
	///
	/// # Errors
	///
	/// - [`DatastoreError::FeatureLevelBelowFirst`] when `target` precedes the first release with a
	///   feature level;
	/// - [`DatastoreError::FeatureLevelUnset`] when the store records no feature level;
	/// - [`DatastoreError::FeatureLevelInUse`] when entries already need a release newer than
	///   `target`.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn lower_feature_level(&self, target: Release) -> Result<()> {
		if target < Release::FIRST_STORED {
			bail!(DatastoreError::FeatureLevelBelowFirst {
				first: Release::FIRST_STORED,
			});
		}
		Self::retry("Lower feature level", || self.lower_feature_level_attempt(target)).await
	}

	/// Refuses a feature level no build can hold: newer than this build, or
	/// older than the first release that has one.
	pub(super) fn check_feature_level_target(target: Release) -> Result<()> {
		if target > Release::CURRENT {
			bail!(DatastoreError::FeatureLevelAboveRelease {
				target,
				current: Release::CURRENT,
			});
		}
		if target < Release::FIRST_STORED {
			bail!(DatastoreError::FeatureLevelBelowFirst {
				first: Release::FIRST_STORED,
			});
		}
		Ok(())
	}

	async fn raise_feature_level_attempt(&self, target: Release, nodes: NodeCheck) -> Result<()> {
		let txn = self.transaction(Write).await?.enclose();
		// A locked read, so a concurrent raise or lower conflicts with this one.
		let raw = catch!(txn, txn.getu_key_raw(&FeatureLevelKey {}).await);
		let prior = catch!(
			txn,
			raw.as_deref()
				.map(|bytes| <StoredFeatureLevel as KVValue>::kv_decode_value(bytes, ()))
				.transpose()
		);
		if let Some(record) = &prior
			&& record.feature_level().admits(target)
		{
			catch!(txn, txn.cancel().await);
			self.feature_cell().observe(Some(record));
			return Ok(());
		}
		// The nodes that are running, other than this one, which is this build.
		let live: Vec<(Uuid, Timestamp)> = match nodes {
			NodeCheck::Skip => Vec::new(),
			NodeCheck::Require => {
				let all = catch!(txn, txn.all_nodes().await);
				let now = self.clock_now();
				let cutoff = Timestamp {
					value: now.value.saturating_sub(NODE_LIVENESS.as_millis() as u64),
				};
				all.iter()
					.filter(|n| n.id != self.id && n.is_active() && n.heartbeat >= cutoff)
					.map(|n| (n.id, n.heartbeat))
					.collect()
			}
		};
		if !live.is_empty() {
			let releases = catch!(txn, txn.node_releases(&live).await);
			for ((node, _), release) in live.iter().zip(releases) {
				if release.is_none_or(|release| release < target) {
					let _ = txn.cancel().await;
					bail!(DatastoreError::FeatureLevelNodeBehind {
						node: *node,
						release,
						target,
					});
				}
			}
		}
		// The first raise from Legacy starts `in_use` at the first stored
		// release: no level below it can be set, so nothing is lost.
		let in_use = prior
			.as_ref()
			.map_or(Release::FIRST_STORED, |record| record.in_use().max(Release::FIRST_STORED));
		let record =
			catch!(txn, StoredFeatureLevel::try_new(target, in_use).map_err(anyhow::Error::from));
		let key = catch!(txn, FeatureLevelKey {}.encode_key());
		let bytes = catch!(
			txn,
			record.kv_encode_value_at(&EncodeContext {
				level: FeatureLevel::At(Release::CURRENT),
				prior: raw.as_deref(),
			})
		)
		.bytes;
		catch!(txn, txn.set(key, bytes).await);
		// The entry that makes a release without the stored-value grammar refuse the store
		// commits with the raise, so no such release joins between the raise and the first
		// stored byte.
		catch!(txn, txn.admit_stored_format().await);
		catch!(txn, txn.commit().await);
		self.feature_cell().observe(Some(&record));
		Ok(())
	}

	async fn lower_feature_level_attempt(&self, target: Release) -> Result<()> {
		let txn = self.transaction(Write).await?.enclose();
		let raw = catch!(txn, txn.getu_key_raw(&FeatureLevelKey {}).await);
		let Some(raw) = raw else {
			let _ = txn.cancel().await;
			bail!(DatastoreError::FeatureLevelUnset);
		};
		let prior = catch!(txn, <StoredFeatureLevel as KVValue>::kv_decode_value(&raw, ()));
		if prior.level() <= target {
			catch!(txn, txn.cancel().await);
			self.feature_cell().observe(Some(&prior));
			return Ok(());
		}
		if prior.in_use() > target {
			let _ = txn.cancel().await;
			bail!(DatastoreError::FeatureLevelInUse {
				in_use: prior.in_use(),
				target,
			});
		}
		let record = catch!(
			txn,
			StoredFeatureLevel::try_new(target, prior.in_use()).map_err(anyhow::Error::from)
		);
		let key = catch!(txn, FeatureLevelKey {}.encode_key());
		let bytes = catch!(
			txn,
			record.kv_encode_value_at(&EncodeContext {
				level: FeatureLevel::At(Release::CURRENT),
				prior: Some(&raw),
			})
		)
		.bytes;
		catch!(txn, txn.set(key, bytes).await);
		catch!(txn, txn.commit().await);
		self.feature_cell().observe(Some(&record));
		Ok(())
	}

	/// Under the Router live-query engine, seeds the router's baseline cursor
	/// with the safe watermark, once: every subscription, and every write a
	/// subscriber expects to see, then lands strictly after it, where a lazy
	/// baseline on the first router pass would discard the events captured
	/// before it. Runs before the datastore serves a query. The Inline engine
	/// never runs the router and reads nothing here.
	pub(crate) async fn establish_router_baseline(&self) -> Result<()> {
		if self.config.datastore.live_query_engine != LiveQueryEngine::Router
			|| self.live_query_router.has_baseline()
		{
			return Ok(());
		}
		let txn = self.transaction(TransactionType::Read).await?;
		let baseline = txn.safe_timestamp().await?.as_versionstamp();
		txn.cancel().await?;
		self.live_query_router.set_baseline(baseline);
		Ok(())
	}

	/// Settles the feature level of a datastore the builder has just opened.
	///
	/// Reads the store's record into the shared cell. A store that holds no key
	/// but the version sentinel is new, and is created at the validated pin or
	/// at this build's release, with nothing yet in use beyond the first stored
	/// release. A store that holds anything else and records no level is an
	/// upgraded 3.x store and stays at [`FeatureLevel::Legacy`]; an operator
	/// raises it, or [`Self::check_version`] does where the engine admits no
	/// other writer.
	///
	/// Under the Legacy hold a store that settles above Legacy, whether it
	/// recorded a level or was created here, logs one warning that the hold did
	/// not apply.
	///
	/// This is the one write, beside the version sentinel, that may precede the
	/// version stamp (see [`Self::get_version`]). It retries with the version
	/// bootstrap, because concurrent instances conflict over the empty store.
	pub(crate) async fn settle_feature_level(&self) -> Result<()> {
		Self::retry_bootstrap("Settle feature level", || self.settle_feature_level_attempt())
			.await?;
		self.feature_level_settled.store(true, std::sync::atomic::Ordering::Release);
		if self.holds_legacy() && !self.feature_cell().level().is_legacy() {
			warn!(
				target: TARGET,
				level = ?self.feature_cell().level(),
				"The legacy feature level hold applies only to a store at the legacy level; \
				 this store records a level and keeps it"
			);
		}
		Ok(())
	}

	async fn settle_feature_level_attempt(&self) -> Result<()> {
		let txn = self.transaction(Write).await?.enclose();
		if let Some(record) = catch!(txn, txn.get_key(&FeatureLevelKey {}, None).await) {
			catch!(txn, txn.cancel().await);
			self.feature_cell().observe(Some(&record));
			return Ok(());
		}
		// Whether the store holds any key but the sentinel. Spans the whole byte
		// space: a key this release does not describe is still data.
		let range = RawRange::every_key_after(&Key::from(&b""[..]));
		let keys = catch!(txn, txn.keys_raw(range, 2, 0, None).await);
		let sentinel = catch!(txn, BootstrapKey {}.encode_key());
		if keys.iter().any(|key| key.as_slice() != sentinel.as_slice()) {
			catch!(txn, txn.cancel().await);
			return Ok(());
		}
		let record = catch!(
			txn,
			StoredFeatureLevel::try_new(self.initial_feature_level(), Release::FIRST_STORED)
				.map_err(anyhow::Error::from)
		);
		catch!(txn, txn.put_key(&FeatureLevelKey {}, &record).await);
		catch!(txn, txn.commit().await);
		self.feature_cell().observe(Some(&record));
		Ok(())
	}

	/// Raises the store's feature level to at least the operator's pin, once the
	/// storage version and the migrations are settled. It never lowers: a store
	/// at or above the pin is left alone, so a binary rolled back onto a store
	/// that a newer release raised keeps that level.
	///
	/// A raise refused because a running node is older than the pin logs a
	/// warning and keeps the stored level, so a rolling restart with the pin in
	/// its configuration never stops a node from starting. Any other refusal
	/// fails the call.
	///
	/// Does nothing under the Legacy hold, which names no release.
	pub(crate) async fn apply_feature_level_pin(&self) -> Result<()> {
		if self.holds_legacy() {
			return Ok(());
		}
		let Some(pin) = self.feature_level_pin() else {
			return Ok(());
		};
		match self.raise_feature_level(pin).await {
			Ok(()) => Ok(()),
			Err(e)
				if e.chain().any(|cause| {
					matches!(
						cause.downcast_ref::<DatastoreError>(),
						Some(DatastoreError::FeatureLevelNodeBehind { .. })
					)
				}) =>
			{
				warn!(
					target: TARGET,
					pin = %pin,
					error = %e,
					"The configured feature level was not applied; the store keeps its level"
				);
				Ok(())
			}
			Err(e) => Err(e),
		}
	}

	/// Raises a store at [`FeatureLevel::Legacy`] to [`Release::FIRST_STORED`] when its
	/// engine admits no other writer.
	///
	/// A store on such an engine has one process, so there is no older node to wait for
	/// and no operator to coordinate the switch with: the raise skips the node check and
	/// the records after it are written in the stored grammar. A distributed store, and
	/// one on an engine that may have several writers (a browser store shared between
	/// tabs), waits for an operator ([`Self::raise_feature_level`]).
	///
	/// Does nothing when the datastore is read-only, when the Legacy hold applies, when
	/// the engine may have other writers, or when the store records a level. The state is
	/// read from storage, not from the cell, because the raise it replaces is the
	/// operator's and may have happened since this node read it.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub(crate) async fn raise_sole_writer_store(&self) -> Result<()> {
		if self.is_read_only() || self.holds_legacy() {
			return Ok(());
		}
		let txn = self.transaction(Read).await?;
		if !txn.sole_writer() {
			txn.cancel().await?;
			return Ok(());
		}
		let state = catch!(txn, txn.feature_level_state().await);
		txn.cancel().await?;
		if !state.level.is_legacy() {
			return Ok(());
		}
		let target = Release::FIRST_STORED;
		Self::retry("Raise feature level", || {
			self.raise_feature_level_attempt(target, NodeCheck::Skip)
		})
		.await?;
		info!(
			target: TARGET,
			release = %target,
			"This datastore has one writer, so its feature level was raised and new records are \
			 written in the stored format"
		);
		Ok(())
	}

	/// Brings the shared cell up to the store's record, which another node may
	/// have raised or lowered since this datastore last read it.
	pub(crate) async fn refresh_feature_level(&self) -> Result<()> {
		let txn = self.transaction(Read).await?;
		let record = catch!(txn, txn.get_key(&FeatureLevelKey {}, None).await);
		txn.cancel().await?;
		self.feature_cell().observe(record.as_ref());
		Ok(())
	}

	// --------------------------------------------------
	// Initialisation functions
	// --------------------------------------------------

	/// Setup the initial cluster access credentials
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn initialise_credentials(&self, user: &str, pass: &str) -> Result<()> {
		// Retry because concurrent instances may conflict when creating the root user
		Self::retry("Initialise credentials", || self.initialise_credentials_attempt(user, pass))
			.await
	}

	/// Single attempt to create the root user if none exists.
	/// Separated from `initialise_credentials` so it can be wrapped in the retry loop.
	async fn initialise_credentials_attempt(&self, user: &str, pass: &str) -> Result<()> {
		// Start a new writeable transaction
		let txn = self.transaction(Write).await?.enclose();
		// Whether any root user exists. Only the keys are probed: a user this
		// build cannot read (a newer release wrote it) is still a root user, so
		// no second one is created and startup proceeds.
		let range = catch!(txn, RootUserPrefix {}.range());
		let users = catch!(txn, txn.keys_raw(range, 1, 0, None).await);
		// Process credentials, depending on existing users
		if users.is_empty() {
			// Display information in the logs
			info!(target: TARGET, "Credentials were provided, and no root users were found. The root user '{user}' will be created");
			// Create and new root user definition
			let stm = define_user_statement_new_with_password(
				Base::Root,
				user.to_owned(),
				pass,
				INITIAL_USER_ROLE.to_owned(),
			);
			let opt = Options::new(&ExecConfig::default())
				.with_auth(Arc::new(Auth::for_root(Role::Owner)));
			let mut ctx = self.setup_ctx()?;
			ctx.set_transaction(Arc::clone(&txn));
			let ctx = ctx.freeze();
			let res = crate::ddl::define_user_statement_compute(&stm, &ctx, &opt, None).await;
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
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
		self.execute(
			&sql,
			&Session::owner(),
			Some(vars.into_iter().collect::<std::collections::BTreeMap<_, _>>().into()),
		)
		.await?;
		// Everything ok
		Ok(())
	}

	/// Performs a database import from SQL, for a startup script.
	///
	/// Delegates to [`Self::import`] rather than repeating it, so that a dump
	/// restored by `--import-file` is parsed under the same profile as one
	/// restored by `surreal import`. Parsed under the datastore's configured
	/// query limits instead, this entry point would refuse depths that an
	/// export is required to be able to write back.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn startup(&self, sql: &str, sess: &Session) -> Result<Vec<QueryResult>> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore startup import script");
		// Execute the SQL import
		self.import(sql, sess).await
	}

	/// Starts this datastore's maintenance tasks, if they are not already
	/// running.
	///
	/// The builder starts them itself, and settles the storage version first, so
	/// this exists for a caller that built with
	/// [`Builder::without_maintenance_tasks`] in order to own that ordering.
	/// Such a caller owes the same sequence, because these tasks write: the
	/// node-membership refresh blindly upserts this node's row, and every leased
	/// job writes its lease. A write that lands before `!v` does makes a
	/// datastore this process created indistinguishable from one written before
	/// `!v` existed, which [`Datastore::get_version`] is obliged to refuse as
	/// out of date. Call this only once [`Datastore::check_version`] has
	/// returned.
	///
	/// A datastore that is open read-only ([`Self::is_read_only`]) starts only the
	/// node-membership refresh, because every other task writes state it refuses.
	///
	/// Idempotent: a datastore that already carries its tasks is left alone.
	pub fn start_maintenance_tasks(self: &Arc<Self>) {
		let mut maintenance = self.maintenance.lock();
		if maintenance.is_some() {
			return;
		}
		let tasks = crate::kvs::tasks::init(self, self.shutdown.clone(), &self.engine_options);
		// A build without `maintenance-tasks` started none, and records none.
		if cfg!(feature = "maintenance-tasks") {
			*maintenance = Some(tasks);
		}
	}

	/// Whether this datastore's maintenance tasks are running.
	///
	/// A datastore that never starts them is a database with no index
	/// compaction, no tombstone reclaim, no changefeed GC, no index-build
	/// recovery and no cluster heartbeat — none of which announces itself, so a
	/// caller that defers the start with
	/// [`Builder::without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks)
	/// has this to assert against.
	pub fn maintenance_tasks_running(&self) -> bool {
		self.maintenance.lock().is_some()
	}

	/// Run the datastore shutdown tasks, performing any necessary cleanup
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn shutdown(&self) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Running datastore shutdown operations");
		// One caller runs the whole sequence at a time. The steps below are
		// ordered — tasks stopped before the storage engine closes — and a
		// concurrent caller that found the handles already taken would otherwise
		// skip the wait and close the engine while the first caller's pass was
		// still using it. Later callers block here and return once the sequence
		// they were waiting on has finished.
		let _shutting_down = self.shutdown_lock.lock().await;
		// Stop this datastore's maintenance tasks first. They open transactions
		// of their own, and the storage engine shutdown below refuses every
		// commit once it has run, so a pass still in flight would fail rather
		// than finish. Cancelling is idempotent, so a caller that already
		// cancelled its own token — as the server and the embedded engine do —
		// loses nothing by reaching here.
		self.shutdown.cancel();
		// Taken rather than borrowed: the guard must not be held across the
		// await, and the handles are consumed by resolving them.
		let tasks = self.maintenance.lock().take();
		if let Some(tasks) = tasks {
			// Bounded: a pass runs user-defined SurrealQL in the async event
			// case, so waiting for one to finish cannot be waiting forever.
			if timeout(MAINTENANCE_SHUTDOWN_TIMEOUT, tasks.resolve()).await.is_err() {
				warn!(
					target: TARGET,
					"Maintenance tasks did not stop within {MAINTENANCE_SHUTDOWN_TIMEOUT:?}; \
					 continuing shutdown without them"
				);
			}
		}
		// Local index builder tasks are deliberately left running: the storage
		// engine shutdown below stops the commit coordinator first, after which
		// every commit is refused before it applies (see the engine
		// `Transaction::commit` gate). An in-flight build therefore makes no
		// durable change during shutdown and stays at its last committed
		// checkpoint, so the periodic resume scan continues it after restart —
		// exactly as it would after a crash.
		// Archive this datastore in the cluster, but don't let a blocked
		// metadata transaction prevent storage engine shutdown.
		let _ = archive_node_for_shutdown(
			NODE_DELETE_TIMEOUT,
			self.delete_node_with_timeout(NODE_DELETE_TIMEOUT).await,
		);
		// Run any storage engine shutdown tasks
		self.transaction_factory.shutdown().await
	}

	/// Waits until the storage backend is able to serve transactions.
	///
	/// Startup awaits this before opening its first transaction. A backend that
	/// can serve as soon as it is built returns immediately; one that has to
	/// join a cluster and catch up first blocks here for as long as that takes,
	/// which keeps a legitimately slow join from being charged against a
	/// caller's retry budget. `Err` means the backend will never serve.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn wait_until_serve_ready(&self) -> Result<()> {
		self.transaction_factory.wait_until_serve_ready().await
	}

	/// Drop every version of every key in the half-open range `[start, end)`
	/// **outside** any user transaction.
	///
	/// Resolves the backend's [`DestroyRange`] capability through the
	/// [`TransactionBuilder::extension`] hook and delegates to it. A backend
	/// that does not offer the capability yields
	/// [`crate::kvs::Error::RangeDestroyNotSupported`]: nothing was deleted, so
	/// a caller holding a transactional fallback matches on that error and
	/// takes it, and every other caller propagates rather than mistaking an
	/// unperformed destroy for a completed one.
	///
	/// The destruction bypasses MVCC, so the caller owns two guarantees: the
	/// data is already logically inaccessible (typically because a committed
	/// transaction cleared the catalog entry naming it), and any snapshot that
	/// could still read it has expired.
	#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self), fields(rng = range.to_string())))]
	pub async fn unsafe_destroy_range(&self, range: KeyRange<'static>) -> Result<()> {
		let Some(ops) = self.destroy_range_ops() else {
			return Err(crate::kvs::Error::RangeDestroyNotSupported.into());
		};
		ops.destroy_range(range).await.map_err(Into::into)
	}

	/// Resolve the backend's out-of-transaction range-destroy capability.
	///
	/// `None` means the backend cannot destroy a range outside a transaction —
	/// either it has no such primitive, or its current configuration cannot use
	/// the one it has. Resolved by capability rather than by backend name, so
	/// callers stay free of per-backend `cfg` arms and a new capable backend
	/// needs no change here.
	fn destroy_range_ops(&self) -> Option<Arc<dyn DestroyRange>> {
		let ext = self.transaction_factory.extension(TypeId::of::<DestroyRangeHandle>())?;
		let handle = ext.downcast::<DestroyRangeHandle>().ok()?;
		Some(Arc::clone(&handle.0))
	}

	/// Advance the MVCC garbage-collection safepoint by `lifetime`.
	///
	/// Routes through the backend's [`TransactionBuilder::extension`] hook;
	/// no-op on backends other than TiKV. Background tasks call this on
	/// `EngineOptions::tikv_gc_interval` and shutdown runs one final
	/// advisory pass.
	pub(crate) async fn run_mvcc_gc(&self, lifetime: Duration) -> Result<()> {
		#[cfg(feature = "kv-tikv")]
		if let Some(ops) = self.tikv_ops() {
			return ops.run_mvcc_gc(lifetime).await.map_err(Into::into);
		}
		let _ = lifetime;
		Ok(())
	}

	/// Resolve stale transactional locks left by crashed clients.
	///
	/// Routes through the backend's [`TransactionBuilder::extension`] hook;
	/// no-op on backends other than TiKV. Background tasks call this on
	/// `EngineOptions::tikv_lock_cleanup_interval`.
	pub(crate) async fn run_lock_cleanup(&self, lifetime: Duration) -> Result<()> {
		#[cfg(feature = "kv-tikv")]
		if let Some(ops) = self.tikv_ops() {
			return ops.run_lock_cleanup(lifetime).await.map_err(Into::into);
		}
		let _ = lifetime;
		Ok(())
	}

	/// Number of in-flight transactions tracked by the backend, when
	/// available. `None` indicates the backend does not track this.
	pub fn in_flight_transaction_count(&self) -> Option<usize> {
		#[cfg(feature = "kv-tikv")]
		if let Some(ops) = self.tikv_ops() {
			return Some(ops.in_flight_transaction_count());
		}
		None
	}

	/// Resolve the TiKV operational extension handle, if the backend is
	/// TiKV. Returns `None` for every other flavour.
	#[cfg(feature = "kv-tikv")]
	fn tikv_ops(&self) -> Option<Arc<surrealdb_kvs_any::tikv::TikvOpsHandle>> {
		let ext = self
			.transaction_factory
			.extension(TypeId::of::<surrealdb_kvs_any::tikv::TikvOpsHandle>())?;
		ext.downcast::<surrealdb_kvs_any::tikv::TikvOpsHandle>().ok()
	}

	// --------------------------------------------------
	// Extension preparation
	// --------------------------------------------------

	/// Start the extension runner when any extension is defined, and, unless
	/// extensions load lazily, prepare every extension archive in use, so that
	/// the first invocation and query planning (which reads each export's
	/// `writeable` flag) find it compiled.
	///
	/// Starting the runner reserves what every invocation needs, such as the
	/// guarded engine's address space, so a host that refuses it is reported
	/// here, at startup, rather than on every call. A server with extensions
	/// and no `surrealism_max_memory` is warned that every concurrent
	/// invocation may grow to the full 32-bit address space.
	///
	/// Each definition is resolved to its archive's digest; definitions that
	/// share a digest share one prepared entry, so it is compiled once.
	/// Definitions are loaded in parallel, and a failure is logged without
	/// stopping the others.
	#[cfg(feature = "surrealism")]
	#[tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)]
	pub async fn prepare_extensions(&self) {
		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
		use crate::surrealism::cache::SurrealismCacheLookup;

		let txn = match self.transaction(Read).await {
			Ok(txn) => Arc::new(txn),
			Err(e) => {
				warn!(target: TARGET, error = %e, "Surrealism eager load: failed to open transaction");
				return;
			}
		};

		let mut ctx = match self.setup_ctx() {
			Ok(ctx) => ctx,
			Err(e) => {
				warn!(target: TARGET, error = %e, "Surrealism eager load: failed to set up context");
				return;
			}
		};
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();

		let nss = match txn.all_ns(None).await {
			Ok(nss) => nss,
			Err(e) => {
				warn!(target: TARGET, error = %e, "Surrealism eager load: failed to list namespaces");
				return;
			}
		};

		// Collect all definitions first, then load in parallel.
		struct ModuleLookup {
			ns_id: crate::catalog::NamespaceId,
			db_id: crate::catalog::DatabaseId,
			display_name: String,
			definition: crate::catalog::ModuleDefinition,
		}

		let mut lookups = Vec::new();
		for ns in nss.iter() {
			let dbs = match txn.all_db(ns.namespace_id, None).await {
				Ok(dbs) => dbs,
				Err(e) => {
					warn!(
						target: TARGET,
						error = %e, ns = %ns.name,
						"Surrealism eager load: failed to list databases"
					);
					continue;
				}
			};
			for db in dbs.iter() {
				let modules = match txn.all_db_modules(ns.namespace_id, db.database_id, None).await
				{
					Ok(m) => m,
					Err(e) => {
						warn!(
							target: TARGET,
							error = %e, ns = %ns.name, db = %db.name,
							"Surrealism eager load: failed to list modules"
						);
						continue;
					}
				};
				for md in modules.iter() {
					// A definition stored before revision 3 records no DIGEST
					// and does not run, so there is nothing to load.
					if md.needs_reinstall() {
						continue;
					}
					lookups.push(ModuleLookup {
						ns_id: ns.namespace_id,
						db_id: db.database_id,
						display_name: md.name.clone().unwrap_or_else(|| "<unnamed>".to_string()),
						definition: md.clone(),
					});
				}
			}
		}

		if lookups.is_empty() {
			debug!(target: TARGET, "Surrealism eager load: no modules to load");
			return;
		}

		if let Some(runner) = self.surrealism_cache.runner()
			&& let Err(e) = runner.start()
		{
			error!(
				target: TARGET,
				runner = runner.name(),
				error = %e,
				"The extension runner could not start, so every extension call will fail"
			);
		}
		if self.config.surrealism.surrealism_max_memory.is_none() {
			warn!(
				target: TARGET,
				slots = self.config.surrealism.surrealism_max_concurrent_invocations,
				"No SURREAL_SURREALISM_MAX_MEMORY is set: every concurrent extension invocation \
				 may grow to 4 GiB of linear memory"
			);
		}
		if self.lazy_extensions {
			return;
		}

		let total = lookups.len();
		debug!(target: TARGET, count = total, "Surrealism eager load: loading modules");

		let concurrency =
			std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8).clamp(2, 16);
		let load_sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));

		let mut join_set = tokio::task::JoinSet::new();
		for lookup in lookups {
			let ctx = Arc::clone(&ctx);
			let load_sem = Arc::clone(&load_sem);
			join_set.spawn(async move {
				let _permit = load_sem
					.acquire_owned()
					.await
					.expect("Surrealism eager load semaphore must not be closed");
				let cache_lookup = match &lookup.definition.executable {
					crate::catalog::ModuleExecutable::Surrealism(s) => {
						SurrealismCacheLookup::File(&lookup.ns_id, &lookup.db_id, &s.bucket, &s.key)
					}
					crate::catalog::ModuleExecutable::Silo(s) => SurrealismCacheLookup::Silo(
						&lookup.ns_id,
						&lookup.db_id,
						&s.organisation,
						&s.package,
						s.major,
						s.minor,
						s.patch,
					),
				};
				// Prepared, and compiled for the engine its grant selects, so
				// the first call compiles nothing.
				let load = async {
					let module = ctx
						.get_surrealism_module(cache_lookup, (&lookup.definition).into())
						.await?;
					if let Some(capabilities) = &lookup.definition.capabilities {
						let grant = crate::fnc::module::extension_grant(&ctx, capabilities).await?;
						module.prepared.warm(&grant).await?;
					}
					anyhow::Ok(())
				};
				match load.await {
					Ok(()) => {
						debug!(
							target: TARGET,
							module = %lookup.display_name,
							"Surrealism eager load: loaded module"
						);
						true
					}
					Err(e) => {
						warn!(
							target: TARGET,
							module = %lookup.display_name,
							error = %e,
							"Surrealism eager load: failed to load module"
						);
						false
					}
				}
			});
		}

		let mut loaded = 0usize;
		let mut failed = 0usize;
		while let Some(result) = join_set.join_next().await {
			match result {
				Ok(true) => loaded += 1,
				Ok(false) => failed += 1,
				Err(e) => {
					warn!(target: TARGET, error = %e, "Surrealism eager load: task panicked");
					failed += 1;
				}
			}
		}

		if failed > 0 {
			warn!(
				target: TARGET,
				loaded, failed, total,
				"Surrealism eager load: completed with failures"
			);
		} else {
			tracing::info!(
				target: TARGET,
				loaded, total,
				"Surrealism eager load: all modules loaded"
			);
		}
	}

	// --------------------------------------------------
	// Node functions
	// --------------------------------------------------

	/// Initialise the cluster and run bootstrap utilities
	///
	/// A datastore that is open read-only ([`Self::is_read_only`]) only registers this node:
	/// that row is bookkeeping a read-only store still accepts. Archiving and removing other
	/// nodes open ordinary write transactions, which such a store refuses, so those steps are
	/// left to a node that can write.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn bootstrap(&self) -> Result<()> {
		// Each bootstrap step is retried independently, because concurrent instances
		// writing to the same cluster metadata keys may cause transaction conflicts.
		// Insert this node in the cluster
		Self::retry("Insert node", || self.insert_node()).await?;
		if self.is_read_only() {
			return Ok(());
		}
		// Mark inactive nodes as archived
		Self::retry("Expire nodes", || self.expire_nodes()).await?;
		// Remove archived nodes
		Self::retry("Remove nodes", || self.remove_nodes()).await?;
		// Everything ok
		Ok(())
	}

	/// Runs a startup precondition until it succeeds or its budget runs out,
	/// retrying *every* failure rather than only a transaction conflict.
	///
	/// Almost every failure [`Self::get_version`] can report is the storage
	/// layer declining to serve a transaction. On a distributed backend that is
	/// what forming a cluster looks like from the outside — a replica refuses
	/// writes while a view change is in flight — and it clears on its own once
	/// formation settles. Those refusals are not distinguishable from each other
	/// by class, which is why the loop retries by default rather than by
	/// allow-list.
	///
	/// The exception is [`Self::is_permanent_bootstrap_failure`]: a marker whose
	/// bytes this build cannot read is a property of the disk, and waiting on it
	/// costs twice over — the caller waits out the whole budget, and the server,
	/// whose own ceiling is shorter, then reports a timeout in place of the error
	/// that would have named the problem.
	///
	/// The wait is bounded, and progress is reported on a fixed cadence rather
	/// than per attempt, so a long formation is visible in the log without the
	/// log growing with the retry rate.
	///
	/// The server wraps this in its own `SURREAL_STARTUP_OPERATION_TIMEOUT`
	/// budget, which is the operator-facing ceiling and is shorter than
	/// [`BOOTSTRAP_RETRY_BUDGET`] by default. This bound is what stops an
	/// embedder, which has no such wrapper, from waiting forever.
	/// Whether a bootstrap failure is a property of the bytes on disk rather
	/// than of the storage layer's availability.
	///
	/// These name a datastore this build cannot read, and no amount of waiting
	/// changes that. Matched by type rather than by rendered text, which is what
	/// the server's equivalent has to fall back on across the crate boundary.
	///
	/// A feature level record this build cannot read is one of them: it names
	/// the store's state in bytes that no retry changes. So is a store that is
	/// open read-only, whose ledger no retry changes either.
	fn is_permanent_bootstrap_failure(err: &anyhow::Error) -> bool {
		Error::entry_refusal(err).is_some()
			|| err.chain().any(|e| {
				matches!(
					e.downcast_ref::<DatastoreError>(),
					Some(
						DatastoreError::InvalidStorageVersion
							| DatastoreError::OutdatedStorageVersion { .. }
							| DatastoreError::MigratedBeyondStorageVersion { .. }
							| DatastoreError::StoreReadOnly { .. }
					)
				)
			})
	}

	async fn retry_bootstrap<F, Fut, R>(task: &str, func: F) -> Result<R>
	where
		F: Fn() -> Fut,
		Fut: Future<Output = Result<R>>,
	{
		let started = Instant::now();
		let mut last_reported = started;
		let mut attempt = 1u32;
		// `None` after an attempt that never answered, which has no error to
		// carry; the report below distinguishes the two.
		let mut last_error = None;
		loop {
			let remaining = BOOTSTRAP_RETRY_BUDGET.saturating_sub(started.elapsed());
			if remaining.is_zero() {
				break;
			}
			// Bound the attempt itself as well as the budget: a backend that
			// accepts the transaction and then never answers would otherwise
			// spend the whole budget on one attempt.
			let attempt_timeout = BOOTSTRAP_RETRY_ATTEMPT_TIMEOUT.min(remaining);
			match timeout(attempt_timeout, func()).await {
				Ok(Ok(result)) => {
					if attempt > 1 {
						info!(
							target: TARGET,
							task, attempt, elapsed = ?started.elapsed(),
							"Storage became available and the startup step completed."
						);
					}
					return Ok(result);
				}
				Ok(Err(e)) if Self::is_permanent_bootstrap_failure(&e) => return Err(e),
				Ok(Err(e)) => last_error = Some(e),
				Err(_) => last_error = None,
			}
			// One line per interval, whatever the attempt rate, and the first
			// failure always reports so a wait is never silent.
			if attempt == 1 || last_reported.elapsed() >= BOOTSTRAP_RETRY_LOG_INTERVAL {
				last_reported = Instant::now();
				match &last_error {
					Some(e) => info!(
						target: TARGET,
						task, attempt, elapsed = ?started.elapsed(),
						budget = ?BOOTSTRAP_RETRY_BUDGET, error = %e,
						"Storage is not ready for the startup step yet; still retrying."
					),
					None => info!(
						target: TARGET,
						task, attempt, elapsed = ?started.elapsed(),
						budget = ?BOOTSTRAP_RETRY_BUDGET,
						timeout = ?attempt_timeout,
						"The startup step did not answer within its attempt timeout; still retrying."
					),
				}
			}
			let remaining = BOOTSTRAP_RETRY_BUDGET.saturating_sub(started.elapsed());
			if remaining.is_zero() {
				break;
			}
			// Exponential back-off with jitter, so the nodes of a cluster that
			// all restarted together do not retry in lockstep. The exponent is
			// capped before it is applied, not after: `Duration`'s multiplication
			// panics on overflow, so a doubling left to run free would abort the
			// process rather than saturate at the ceiling below it.
			let doublings = (attempt - 1).min(BOOTSTRAP_RETRY_MAX_DOUBLINGS);
			let backoff = (BOOTSTRAP_RETRY_MIN_BACKOFF * 2u32.pow(doublings))
				.min(BOOTSTRAP_RETRY_MAX_BACKOFF)
				.mul_f64(rand::rng().random_range(0.5..=1.5))
				.min(remaining);
			sleep(backoff).await;
			attempt = attempt.saturating_add(1);
		}
		match last_error {
			Some(e) => Err(e).context(format!(
				"{task} failed after {attempt} attempts over {:?}",
				started.elapsed()
			)),
			None => bail!(EngineError::Internal(format!(
				"{task} timed out after {attempt} attempts over {:?}",
				started.elapsed()
			))),
		}
	}

	/// Retries an async operation until it succeeds or the global timeout elapses.
	///
	/// Only [`TransactionConflict`](crate::kvs::Error::TransactionConflict)
	/// errors are retried; any other error is returned immediately to the
	/// caller. On each retryable failure a randomized delay (0–10 s) is
	/// applied before the next attempt, adding jitter to reduce repeated
	/// collisions when multiple instances start concurrently against the
	/// same storage backend.
	///
	/// The global timeout is checked only after a *failed* attempt; a successful
	/// result is always returned immediately, even if the elapsed time
	/// exceeds the budget. Each attempt's timeout is the lesser of its
	/// natural timeout (10 s * attempt number) and the remaining global
	/// budget, so total wall-clock time never significantly exceeds the
	/// global timeout. If no attempt succeeds within the budget, an error
	/// is returned.
	async fn retry<F, Fut, R>(task: &str, func: F) -> Result<R>
	where
		F: Fn() -> Fut,
		Fut: Future<Output = Result<R>>,
	{
		let global_timeout = Duration::from_secs(120);
		let per_attempt_timeout = Duration::from_secs(10);
		let time = Instant::now();
		let mut last_error = None;
		let mut attempt = 1;
		loop {
			// Cap each attempt to the remaining global budget
			let remaining = global_timeout.saturating_sub(time.elapsed());
			if remaining.is_zero() {
				break;
			}
			let attempt_timeout = (per_attempt_timeout * attempt).min(remaining);
			if let Ok(result) = timeout(attempt_timeout, func()).await {
				match result {
					Ok(result) => return Ok(result),
					Err(e) => {
						// Only retry on transaction conflict errors. Both the
						// bare `kvs::Error` and the `err::Error::Kvs`-wrapped
						// shape occur here: every `Transaction` write goes
						// through `.map_err(Error::from)`, and the operations
						// this loop retries all reach the store that way, so a
						// backend that surfaces a conflict at set/delete time
						// rather than at commit produces the wrapped one.
						if crate::kvs::is_retryable_transaction_conflict(&e) {
							last_error = Some(e);
						} else {
							return Err(e);
						}
					}
				}
			}
			// Check if the global timeout has been exceeded
			if time.elapsed() >= global_timeout {
				break;
			}
			// Randomized back-off capped to the remaining budget
			let remaining = global_timeout.saturating_sub(time.elapsed());
			if remaining.is_zero() {
				break;
			}
			let tempo = Duration::from_secs(rand::rng().random_range(0..10)).min(remaining);
			sleep(tempo).await;
			attempt += 1;
		}
		if let Some(e) = last_error {
			error!(target: TARGET, "{task} - All {attempt} attempts failed. Last error: {e}");
		} else {
			error!(target: TARGET, "{task} - All {attempt} attempts failed.");
		}
		bail!(EngineError::Internal(format!(
			"{task} failed after {attempt} attempts due to timeout"
		)));
	}

	/// Writes this node's own membership row and, beside it, the release this build runs.
	///
	/// Both writes belong to the caller's transaction, so a heartbeat commits only together
	/// with the record of the release that wrote it. The release record is an upsert: the row it
	/// replaces may be absent (the node's previous heartbeat came from a release that records
	/// none) or describe an earlier release. `replace` makes the membership row an update of an
	/// existing row rather than an upsert.
	///
	/// Only this node's own rows go through here. A record for another node would claim that
	/// node runs this release.
	async fn write_node(
		&self,
		txn: &Transaction,
		key: &NodeKey,
		node: &Node,
		replace: bool,
	) -> Result<()> {
		if replace {
			txn.replace_key(key, node).await?;
		} else {
			txn.set_key(key, node).await?;
		}
		txn.set_key(
			&NodeReleaseKey {
				nd: node.id,
			},
			&NodeRelease {
				release: Release::CURRENT,
				heartbeat: node.heartbeat,
			},
		)
		.await
	}

	/// Registers this node's cluster membership entry with a fresh heartbeat.
	///
	/// Must be run at server or database startup. The write is idempotent:
	/// the entry at `Nd::new(self.id)` is owned by this node, so the call
	/// upserts the row whether or not a previous lifetime of the same node
	/// id left a record behind. This supports deployments where the node id
	/// is stable across restarts (e.g. a stateful cluster member reusing
	/// its durable storage) without forcing the operator to clean up state
	/// between runs.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn insert_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id,"Inserting node in the cluster");
		// Refresh system usage metrics
		crate::sys::refresh().await;
		// Open transaction and set node data
		let txn = self.bookkeeping_transaction().await?;
		let key = NodeKey {
			nd: self.id,
		};
		let now = self.clock_now();
		let node = Node::new_with_endpoint(self.id, now, false, self.http_endpoint.clone());
		run!(txn, self.write_node(&txn, &key, &node, false).await)
	}

	/// Updates an already existing node in the cluster.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// This function updates the entry for this node with an up-to-date
	/// timestamp. This ensures that the node is not marked as expired by any
	/// garbage collection tasks, preventing any data cleanup for this node.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn update_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id, "Updating node in the cluster");
		// Open transaction and set node data
		let txn = self.bookkeeping_transaction().await?;
		let key = NodeKey {
			nd: self.id,
		};
		let now = self.clock_now();
		let node = Node::new_with_endpoint(self.id, now, false, self.http_endpoint.clone());
		run!(txn, self.write_node(&txn, &key, &node, true).await)
	}

	/// Updates this node, bounding each step and explicitly cancelling any
	/// open write transaction before returning on timeout or cancellation.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, canceller)
		)
	)]
	pub(crate) async fn update_node_with_timeout(
		&self,
		timeout_duration: Duration,
		canceller: &CancellationToken,
	) -> Result<()> {
		trace!(target: TARGET, id = %self.id, timeout = ?timeout_duration, "Updating node in the cluster with timeout");

		let deadline = Instant::now() + timeout_duration;

		let txn = await_node_step(
			deadline,
			timeout_duration,
			Some(canceller),
			self.bookkeeping_transaction(),
		)
		.await?;
		let key = NodeKey {
			nd: self.id,
		};
		let now = self.clock_now();
		let node = Node::new_with_endpoint(self.id, now, false, self.http_endpoint.clone());

		await_node_tx_step(
			&txn,
			deadline,
			timeout_duration,
			Some(canceller),
			self.write_node(&txn, &key, &node, true),
		)
		.await?;
		await_node_tx_step(&txn, deadline, timeout_duration, Some(canceller), txn.commit()).await
	}

	/// Deletes a node from the cluster.
	///
	/// This function should be run when a node is shutting down.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn delete_node(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %self.id, "Archiving node in the cluster");
		// Open transaction and set node data
		let txn = self.bookkeeping_transaction().await?;
		let key = NodeKey {
			nd: self.id,
		};
		let Some(val) = catch!(txn, txn.get_node(self.id).await) else {
			// Nothing to archive: this node either never registered, or a peer's
			// expiry scan has already reaped the row. Neither is a failure of
			// this shutdown, and reporting one would put an error above whatever
			// actually stopped the node.
			let _ = txn.cancel().await;
			return Ok(());
		};
		let node = val.as_ref().archive();
		run!(txn, self.write_node(&txn, &key, &node, true).await)
	}

	/// Archives this node, bounding each step and explicitly cancelling any
	/// open write transaction before returning on timeout.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn delete_node_with_timeout(&self, timeout_duration: Duration) -> Result<()> {
		trace!(target: TARGET, id = %self.id, timeout = ?timeout_duration, "Archiving node in the cluster with timeout");

		let deadline = Instant::now() + timeout_duration;
		let txn = await_node_step(deadline, timeout_duration, None, self.bookkeeping_transaction())
			.await?;
		let key = NodeKey {
			nd: self.id,
		};
		let val = await_node_tx_step(&txn, deadline, timeout_duration, None, txn.get_node(self.id))
			.await?;
		let Some(val) = val else {
			// Nothing to archive; see `delete_node`.
			let _ = txn.cancel().await;
			return Ok(());
		};
		let node = val.as_ref().archive();

		await_node_tx_step(
			&txn,
			deadline,
			timeout_duration,
			None,
			self.write_node(&txn, &key, &node, true),
		)
		.await?;
		await_node_tx_step(&txn, deadline, timeout_duration, None, txn.commit()).await
	}

	/// Expires nodes which have timedout from the cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn expire_nodes(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, "Archiving expired nodes in the cluster");
		// Fetch all of the inactive nodes
		let inactive = {
			let txn = self.transaction(Read).await?;
			let nds = catch!(txn, txn.all_nodes().await);
			let now = self.clock_now();
			catch!(txn, txn.cancel().await);
			// Filter the inactive nodes
			nds.iter()
				.filter_map(|n| {
					// Check that the node is active and has expired
					match n.is_active() && n.heartbeat < now - NODE_LIVENESS {
						true => Some(n.to_owned()),
						false => None,
					}
				})
				.collect::<Vec<_>>()
		};
		// Check if there are inactive nodes
		if !inactive.is_empty() {
			// Open a writeable transaction
			let txn = self.transaction(Write).await?;
			// Whether any candidate still qualified once re-read below.
			let mut expired = false;
			// Archive the inactive nodes
			for nd in inactive.iter() {
				// Get the key for the node entry
				let key = NodeKey {
					nd: nd.id,
				};
				// The candidate set was sampled in an earlier transaction. A node
				// id that survives a restart can be re-registered inside that
				// window — `insert_node` upserts an active row under the same id,
				// with a fresh heartbeat — and archiving it here would retire a
				// live member. The write is therefore conditional on the heartbeat
				// the scan saw: a row whose heartbeat has moved has re-registered
				// or refreshed itself, and is not the row that expired.
				//
				// That closes the window in both directions. A commit that landed
				// before this transaction's snapshot is visible to this re-read,
				// and one racing this transaction writes the same key, which the
				// engine's write-conflict detection rejects.
				let current = catch!(txn, txn.get_key(&key, None).await);
				let Some(current) =
					current.filter(|n| n.is_active() && n.heartbeat == nd.heartbeat)
				else {
					trace!(target: TARGET, id = %nd.id, "Skipping node that is no longer expired");
					continue;
				};
				// Log the node archival
				trace!(target: TARGET, id = %nd.id, "Archiving node in the cluster");
				// Mark the node as archived, carrying the re-read row forward so
				// no field of the stale snapshot is written back
				let node = current.archive();
				// Update the node entry
				catch!(txn, txn.replace_key(&key, &node).await);
				expired = true;
			}
			// Commit the changes, or release the transaction when every candidate
			// turned out to be live after all
			match expired {
				true => catch!(txn, txn.commit().await),
				false => catch!(txn, txn.cancel().await),
			}
		}
		// Everything was successful
		Ok(())
	}

	/// Removes and cleans up nodes which are not in this cluster any more.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function clears up all nodes which have been marked as archived.
	/// When a matching node is found, all node queries, and table queries are
	/// garbage collected, before the node itself is completely deleted.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn remove_nodes(&self) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, "Cleaning up archived nodes in the cluster");
		// Fetch all of the archived nodes
		let archived = {
			let txn = self.transaction(Read).await?;
			let nds = catch!(txn, txn.all_nodes().await);
			catch!(txn, txn.cancel().await);
			// Filter the archived nodes
			nds.iter().filter_map(Node::archived).collect::<Vec<_>>()
		};
		// Loop over the archived nodes
		for id in archived.iter() {
			// Open a writeable transaction
			let mut next = Some(
				NodeLiveQueryPrefix {
					nd: *id,
				}
				.range()?,
			);
			let txn = self.transaction(Write).await?;
			{
				// The archived set was sampled in an earlier transaction. A node
				// id that survives a restart can be re-registered inside that
				// window — `insert_node` upserts an active row under the same id
				// — and reaping it here would delete a live member along with the
				// live queries it has already registered. Re-read the row in this
				// transaction and only proceed while it is still archived.
				let key = NodeKey {
					nd: *id,
				};
				let node = catch!(txn, txn.get_key(&key, None).await);
				if !node.is_some_and(|nd| nd.is_archived()) {
					trace!(target: TARGET, id = %id, "Skipping node that is no longer archived");
					catch!(txn, txn.cancel().await);
					continue;
				}
			}
			{
				// Log the live query scanning
				trace!(target: TARGET, id = %id, "Deleting live queries for node");
				// Scan the live queries for this node
				while let Some(rng) = next {
					// Fetch the next batch of keys and values
					let res = catch!(
						txn,
						txn.batch_keys_vals(rng.clone(), NORMAL_BATCH_SIZE, None).await
					);
					// A full page carries a continuation: resume the range after
					// the last key this page returned.
					next = match (&res.next, res.result.last()) {
						(Some(_), Some((k, _))) => Some(rng.resume_after(k, Direction::Forward)),
						_ => None,
					};
					for (k, v) in res.result.iter() {
						// Decode the data for this live query
						let val: NodeLiveQuery = KVValue::kv_decode_value(v, ())?;
						// Get the key for this node live query
						let nlq = catch!(txn, NodeLiveQueryKey::decode_key(k));
						// Check that the node for this query is archived
						if archived.contains(&nlq.nd) {
							// Get the key for this table live query
							let tlq = SubscriptionKey {
								ns: val.ns,
								db: val.db,
								tb: Cow::Borrowed(&val.tb),
								lq: nlq.lq,
							};
							// Delete the table live query
							catch!(txn, txn.clr_key(&tlq).await);
							// Delete the node live query
							catch!(txn, txn.clr_key(&nlq).await);
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
				let key = NodeKey {
					nd: *id,
				};
				// Delete the cluster node entry, and the release it recorded
				catch!(txn, txn.clr_key(&key).await);
				catch!(
					txn,
					txn.clr_key(&NodeReleaseKey {
						nd: *id,
					})
					.await
				);
			}
			// Commit the changes
			catch!(txn, txn.commit().await);
		}
		// Release records whose node row is gone
		self.remove_orphaned_node_releases().await?;
		// Everything was successful
		Ok(())
	}

	/// Deletes the release records whose node row does not exist.
	///
	/// A release that does not know the records removes a node's row without removing its
	/// record, so the record outlives the node. The records are read in batches without a write
	/// transaction, and a batch with orphans is deleted in a write transaction that checks each
	/// candidate again, so a node that registers in between keeps its record.
	async fn remove_orphaned_node_releases(&self) -> Result<()> {
		let mut next = Some(NodeReleasePrefix {}.range()?);
		while let Some(rng) = next {
			let candidates = {
				let txn = self.transaction(Read).await?;
				let res =
					catch!(txn, txn.batch_keys_raw(rng.clone(), NORMAL_BATCH_SIZE, None).await);
				next = match (&res.next, res.result.last()) {
					(Some(_), Some(last)) => Some(rng.resume_after(last, Direction::Forward)),
					_ => None,
				};
				// A key that does not spell a node id holds no record of a node, and is left alone
				let nodes: Vec<Uuid> = res
					.result
					.iter()
					.filter_map(|k| NodeReleaseKey::decode_key(k).ok())
					.map(|k| k.nd)
					.collect();
				let orphans = catch!(txn, Self::nodes_without_row(&txn, nodes).await);
				catch!(txn, txn.cancel().await);
				orphans
			};
			if candidates.is_empty() {
				continue;
			}
			let txn = self.transaction(Write).await?;
			let orphans = catch!(txn, Self::nodes_without_row(&txn, candidates).await);
			for nd in orphans {
				trace!(target: TARGET, id = %nd, "Deleting release record of a node that is gone");
				catch!(
					txn,
					txn.clr_key(&NodeReleaseKey {
						nd,
					})
					.await
				);
			}
			catch!(txn, txn.commit().await);
		}
		Ok(())
	}

	/// The nodes of `nodes` that have no membership row, in one batched read.
	async fn nodes_without_row(txn: &Transaction, nodes: Vec<Uuid>) -> Result<Vec<Uuid>> {
		if nodes.is_empty() {
			return Ok(nodes);
		}
		let keys: Vec<NodeKey> = nodes
			.iter()
			.map(|nd| NodeKey {
				nd: *nd,
			})
			.collect();
		let rows = txn.getm_raw(keys, None).await?;
		Ok(nodes
			.into_iter()
			.zip(rows)
			.filter_map(|(nd, row)| row.is_none().then_some(nd))
			.collect())
	}

	// ------------------------------
	// Durable RPC session functions
	// ------------------------------

	/// Creates or replaces the durable copy of a client-attached RPC session,
	/// expiring `ttl` from now (an unconditional upsert).
	///
	/// Session creation goes through [`create_rpc_session`](Self::create_rpc_session)
	/// (insert-if-absent), local changes through
	/// [`update_rpc_session`](Self::update_rpc_session) (update-if-present),
	/// and cross-node reconciliation through
	/// [`load_rpc_session`](Self::load_rpc_session), so none resurrects or
	/// duplicates a session across nodes. This blind upsert is retained as a
	/// primitive for callers that own the id exclusively (and for test setup).
	///
	/// A read-only store refuses this with [`DatastoreError::StoreReadOnly`]: it creates and
	/// refreshes no durable session, while [`delete_rpc_session`](Self::delete_rpc_session)
	/// still removes one.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, session)
		)
	)]
	pub async fn persist_rpc_session(
		&self,
		id: Uuid,
		session: &Session,
		ttl: Duration,
	) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %id, "Persisting durable RPC session");
		// Capture the durable form with a refreshed expiry
		let expires_at = self.clock_now().value + ttl.as_millis() as u64;
		let value = durable_session(session, expires_at);
		// Open transaction and set the session data
		let key = SessionKey {
			id,
		};
		let txn = self.transaction(Write).await?;
		run!(txn, txn.set_key(&key, &value).await)
	}

	/// Creates the durable copy of a client-attached RPC session, but only if
	/// one does not already exist. Returns `true` if it created the entry,
	/// `false` if a session with this id already exists.
	///
	/// Uses a conditional insert (`put_compare_key` with no expected value),
	/// which is atomic even on last-writer-wins backends (TiKV) — a blind
	/// `set` is not — so two concurrent `attach`es for the same id on
	/// different nodes cannot both create it: one wins and the other is
	/// reported as an existing session, letting `attach` return
	/// `session_exists` instead of silently overwriting.
	///
	/// A read-only store refuses this with [`DatastoreError::StoreReadOnly`]: it creates and
	/// refreshes no durable session, while [`delete_rpc_session`](Self::delete_rpc_session)
	/// still removes one.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, session)
		)
	)]
	pub async fn create_rpc_session(
		&self,
		id: Uuid,
		session: &Session,
		ttl: Duration,
	) -> Result<bool> {
		trace!(target: TARGET, id = %id, "Creating durable RPC session");
		let key = SessionKey {
			id,
		};
		let expires_at = self.clock_now().value + ttl.as_millis() as u64;
		let value = durable_session(session, expires_at);
		let txn = self.transaction(Write).await?;
		// `put_compare_key(.., None)` writes only if the key is absent.
		match txn.put_compare_key(&key, &value, None).await {
			Ok(()) => match txn.commit().await {
				Ok(()) => Ok(true),
				// Cancel after any failed commit (a conflict or otherwise), so
				// the transaction is rolled back consistently with the `run!`
				// macro; only the error classification differs.
				Err(e) if super::is_conditional_write_conflict(&e) => {
					let _ = txn.cancel().await;
					Ok(false)
				}
				Err(e) => {
					let _ = txn.cancel().await;
					Err(e)
				}
			},
			Err(e) => {
				let _ = txn.cancel().await;
				if super::is_conditional_write_conflict(&e) {
					Ok(false)
				} else {
					Err(e)
				}
			}
		}
	}

	/// Refreshes the durable copy of an *existing* RPC session with the given
	/// value, expiring `ttl` from now, **without recreating it** if it is gone.
	///
	/// Returns `true` if the entry was present and updated, `false` if it does
	/// not exist (or was changed on another node). Uses a conditional
	/// write (`put_compare_key` guarded by the value just read), so it is
	/// atomic on last-writer-wins backends (TiKV): a session deleted elsewhere
	/// between the read and the write is neither resurrected nor clobbered — a
	/// blind `set` would be last-writer-wins and could resurrect it. This is
	/// the "push a local change to storage" path (a session-mutating method);
	/// the authoritative *reload* path is
	/// [`load_rpc_session`](Self::load_rpc_session).
	///
	/// A read-only store refuses this with [`DatastoreError::StoreReadOnly`]: it creates and
	/// refreshes no durable session, while [`delete_rpc_session`](Self::delete_rpc_session)
	/// still removes one.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, session)
		)
	)]
	pub async fn update_rpc_session(
		&self,
		id: Uuid,
		session: &Session,
		ttl: Duration,
	) -> Result<bool> {
		trace!(target: TARGET, id = %id, "Updating durable RPC session");
		let key = SessionKey {
			id,
		};
		let txn = self.transaction(Write).await?;
		// Read the current stored value to use as the conditional-write guard.
		let Some(current) = catch!(txn, txn.get_key(&key, None).await) else {
			let _ = txn.cancel().await;
			return Ok(false);
		};
		let expires_at = self.clock_now().value + ttl.as_millis() as u64;
		let value = durable_session(session, expires_at);
		// Write only if the stored value is still the one we read, so a delete
		// or change committed on another node in between wins (no resurrection,
		// no clobber) instead of being overwritten by this blind write.
		match txn.put_compare_key(&key, &value, Some(&current)).await {
			Ok(()) => match txn.commit().await {
				Ok(()) => Ok(true),
				// Cancel after any failed commit (a conflict or otherwise), so
				// the transaction is rolled back consistently with the `run!`
				// macro; only the error classification differs.
				Err(e) if super::is_conditional_write_conflict(&e) => {
					let _ = txn.cancel().await;
					Ok(false)
				}
				Err(e) => {
					let _ = txn.cancel().await;
					Err(e)
				}
			},
			Err(e) => {
				let _ = txn.cancel().await;
				if super::is_conditional_write_conflict(&e) {
					Ok(false)
				} else {
					Err(e)
				}
			}
		}
	}

	/// Loads the current durable copy of a client-attached RPC session — the
	/// authoritative cross-node state — returning the restored session and its
	/// absolute expiry (ms since the UNIX epoch), or `None` if it is gone or
	/// expired.
	///
	/// This is a pure read plus lazy expiry: an entry past its `expires_at` is
	/// deleted and reported as a miss, but a live entry is returned **without**
	/// being rewritten. It never extends a session's life, so it is safe to
	/// call before the caller is authorized — both when `get_session`
	/// rehydrates an evicted session and when the transport reconciles a
	/// cached session against storage before dispatch. The TTL is slid only
	/// after authorization, by [`update_rpc_session`](Self::update_rpc_session).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn load_rpc_session(&self, id: Uuid) -> Result<Option<(Session, u64)>> {
		// Log when this method is run
		trace!(target: TARGET, id = %id, "Loading durable RPC session");
		let key = SessionKey {
			id,
		};
		let now = self.clock_now().value;
		// Pure read.
		let durable = {
			let txn = self.transaction(Read).await?;
			let val = catch!(txn, txn.get_key(&key, None).await);
			catch!(txn, txn.cancel().await);
			val
		};
		let Some(durable) = durable else {
			return Ok(None);
		};
		// Lazily delete an expired entry with a conditional delete guarded by
		// the value we read, so a refresh committed on another node between the
		// read and the delete is not clobbered (a blind `clr` is
		// last-writer-wins on TiKV). On a condition conflict the entry changed
		// — leave it and report a miss for this request; it rehydrates fresh on
		// the next one.
		if durable.expires_at <= now {
			trace!(target: TARGET, id = %id, "Durable RPC session has expired");
			let txn = self.transaction(Write).await?;
			match txn.del_compare_key(&key, Some(&durable)).await {
				Ok(()) => match txn.commit().await {
					Ok(()) => {}
					Err(e) if super::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				},
				Err(e) => {
					let _ = txn.cancel().await;
					if !super::is_conditional_write_conflict(&e) {
						return Err(e);
					}
				}
			}
			return Ok(None);
		}
		// Restore the in-memory session without touching the durable copy,
		// handing back the stored expiry so the caller can enforce the TTL.
		let expires_at = durable.expires_at;
		restore_session(durable).map(|session| Some((session, expires_at)))
	}

	/// Deletes the durable copy of a client-attached RPC session, when it is
	/// still the value being torn down.
	///
	/// Run on detach/teardown so the session cannot be rehydrated. It reads the
	/// current value and deletes only that (`del_compare_key`), rather than
	/// clearing the key blindly: a blind `clr` is last-writer-wins on TiKV, so
	/// a slow detach could otherwise delete a *newer* session that another node
	/// re-attached under the same id after the old one was removed. An entry
	/// already absent is a no-op.
	///
	/// A condition conflict — the stored value changed between the read and the
	/// delete (a concurrent refresh/re-attach on another node) — is surfaced as
	/// an **error**, not swallowed: the durable copy was *not* removed, so
	/// reporting success would let [`RpcProtocol::forget_session`]/`detach`
	/// claim the session was torn down while it remains rehydratable. The
	/// caller (whose `del_session` removes the durable copy before local state)
	/// then fails the teardown and can retry, keeping detach fail-closed. In
	/// the supported single-node-per-session model such a conflict does not
	/// arise (the owning node serializes the session's writes).
	///
	/// Runs through the bookkeeping transaction, so a read-only store still removes the durable
	/// copy.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn delete_rpc_session(&self, id: Uuid) -> Result<()> {
		// Log when this method is run
		trace!(target: TARGET, id = %id, "Deleting durable RPC session");
		let key = SessionKey {
			id,
		};
		let txn = self.bookkeeping_transaction().await?;
		// Read the value we intend to remove; if it is already gone, done.
		let Some(current) = catch!(txn, txn.get_key(&key, None).await) else {
			let _ = txn.cancel().await;
			return Ok(());
		};
		// Delete only if the stored value is still the one we read. A conflict
		// (value changed concurrently) means the delete did not happen, so it
		// is surfaced as an error rather than reported as a successful teardown.
		match txn.del_compare_key(&key, Some(&current)).await {
			Ok(()) => match txn.commit().await {
				Ok(()) => Ok(()),
				Err(e) => {
					let _ = txn.cancel().await;
					Err(e)
				}
			},
			Err(e) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Purges expired durable RPC sessions.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// It uses a distributed task lease so only one node in the cluster
	/// performs the purge per interval. Entries are scanned in batches, and
	/// only entries whose expiry has passed are deleted; an entry that fails
	/// to decode (for example one written by a newer node during a rolling
	/// upgrade) is skipped, never deleted.
	///
	/// # Arguments
	/// * `interval` - The interval between purge runs, to calculate the lease duration
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub(crate) async fn purge_expired_rpc_sessions(&self, interval: &Duration) -> Result<()> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting expired RPC session purge");
		// Create a new lease handler
		let lh = LeaseHandler::new(
			self.sequences.clone(),
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::RpcSessionCleanup,
			*interval * 2,
		)?;
		// If we don't get the lease, another node is handling this task
		if !lh.has_lease().await? {
			return Ok(());
		}
		// Output function invocation details to logs
		trace!(target: TARGET, "Purging expired RPC sessions");
		let now = self.clock_now().value;
		// Scan the durable session entries in batches under read transactions,
		// deleting each expired entry in its own conditional-delete write
		// transaction. The delete is guarded by the value read
		// (`del_compare_key`), so a request that refreshed the session after
		// the scan read the old expired value is not clobbered — a blind `clr`
		// is last-writer-wins on TiKV. A per-entry transaction keeps one such
		// conflict from aborting the whole purge.
		let mut next = Some(SessionPrefix {}.range()?);
		while let Some(rng) = next {
			// Fetch the next batch of keys and values under a read transaction.
			let batch = {
				let txn = self.transaction(Read).await?;
				let res =
					catch!(txn, txn.batch_keys_vals(rng.clone(), NORMAL_BATCH_SIZE, None).await);
				catch!(txn, txn.cancel().await);
				res
			};
			// A full page carries a continuation: resume the range after the last
			// key this page returned.
			next = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) => Some(rng.resume_after(k, Direction::Forward)),
				_ => None,
			};
			for (k, v) in batch.result.iter() {
				// Decode the data for this session entry. Skip (but never
				// delete) an entry we cannot decode: it may have been written
				// by a newer node during a rolling upgrade.
				let val: DurableSession = match KVValue::kv_decode_value(v, ()) {
					Ok(val) => val,
					Err(e) => {
						warn!(target: TARGET, "Skipping undecodable durable RPC session entry: {e}");
						continue;
					}
				};
				// Only delete an expired entry, and only if it is still the
				// value we read.
				if val.expires_at <= now {
					let key = SessionKey::decode_key(k)?;
					trace!(target: TARGET, id = %key.id, "Purging expired RPC session");
					let txn = self.transaction(Write).await?;
					match txn.del_compare_key(&key, Some(&val)).await {
						Ok(()) => match txn.commit().await {
							Ok(()) => {}
							// Refreshed concurrently at commit (TiKV): leave it.
							Err(e) if super::is_conditional_write_conflict(&e) => {
								let _ = txn.cancel().await;
							}
							Err(e) => {
								let _ = txn.cancel().await;
								return Err(e);
							}
						},
						Err(e) => {
							let _ = txn.cancel().await;
							if !super::is_conditional_write_conflict(&e) {
								return Err(e);
							}
						}
					}
				}
			}
			// Pause and yield execution
			yield_now!();
		}
		// Everything was successful
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn delete_queries(&self, ids: Vec<uuid::Uuid>) -> Result<()> {
		// Log the node deletion
		trace!(target: TARGET, "Deleting live queries for a connection");
		// Fetch expired nodes
		let txn = self.transaction(Write).await?;
		// Loop over the live query unique ids
		for id in ids {
			// Get the key for this node live query
			let nlq = NodeLiveQueryKey {
				nd: self.id(),
				lq: id,
			};
			// Fetch the LIVE meta data node entry
			if let Some(lq) = catch!(txn, txn.get_key(&nlq, None).await) {
				// Get the key for this node live query
				let nlq = NodeLiveQueryKey {
					nd: self.id(),
					lq: id,
				};
				// Get the key for this table live query
				let tlq = SubscriptionKey {
					ns: lq.ns,
					db: lq.db,
					tb: Cow::Borrowed(&lq.tb),
					lq: id,
				};
				// Delete the table live query
				catch!(txn, txn.clr_key(&tlq).await);
				// Delete the node live query
				catch!(txn, txn.clr_key(&nlq).await);
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn changefeed_process(
		&self,
		interval: &Duration,
		canceller: &CancellationToken,
	) -> Result<()> {
		self.changefeed_process_bounded(interval, CHANGEFEED_GC_PASS_KEY_BUDGET, canceller).await
	}

	/// [`Self::changefeed_process`] with the per-pass key budget lowered, so a
	/// test can drive a pass that stops mid-backlog and inspect what it left
	/// behind.
	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) async fn changefeed_gc_with_budget(
		&self,
		interval: &Duration,
		budget: u64,
		canceller: &CancellationToken,
	) -> Result<()> {
		self.changefeed_process_bounded(interval, budget, canceller).await
	}

	/// One changefeed-collection pass, deleting at most `budget` entries from
	/// each keyspace it collects.
	///
	/// The changefeed backlog and, under the Router engine, the live-query event
	/// backlog are both sized by write throughput rather than by the catalog, so
	/// neither is deleted in one transaction: each is paged behind committed
	/// transactions of at most one page (see [`crate::kvs::paging`]). A pass
	/// therefore costs a bounded amount of work however far either backlog has
	/// run ahead, and the next tick resumes at the head of what survives.
	///
	/// The two keyspaces share this pass's lease but hold separate budgets. The
	/// same writes fill both, so a budget spanning them would let whichever is
	/// collected first spend it whenever the write rate outran it, leaving the
	/// other uncollected on every pass and growing on disk without limit.
	async fn changefeed_process_bounded(
		&self,
		interval: &Duration,
		budget: u64,
		canceller: &CancellationToken,
	) -> Result<()> {
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
		// Entries this pass may delete from the changefeed keyspace, shared out
		// across the databases it visits.
		let mut cf_budget = budget;
		// Perform the changefeed garbage collection. A pass that lost the lease
		// ends this one: the live-query collector below shares the lease, and its
		// own first check would be answered from the maintenance throttle by the
		// very check that lost it — so it would delete alongside the new holder.
		if !crate::cf::gc_all_at(self, &lh, canceller, &mut cf_budget).await? {
			return Ok(());
		}
		// When the Router engine is active, also garbage-collect the dedicated
		// live-query event keyspace, retaining entries for the configured window so
		// reconnecting/lagging subscribers can still replay. This shares the
		// pass's lease with the changefeed GC above and spends its own budget.
		if self.config.datastore.live_query_engine == LiveQueryEngine::Router {
			let retention = self.config.datastore.live_query_retention;
			let mut lq_budget = budget;
			crate::lq::gc::gc_all_at(self, &lh, retention, canceller, &mut lq_budget).await?;
		}
		// Everything ok
		Ok(())
	}

	/// Run one live-query router pass.
	///
	/// Under the [`LiveQueryEngine::Router`] engine this tails the dedicated
	/// `lqe` keyspace since the node's cursor and delivers notifications off the
	/// write path (see [`crate::lq::router`]); it is the sole delivery path in
	/// that mode. Under the default [`LiveQueryEngine::Inline`] engine it is a
	/// cheap no-op, so the engine's background task can call it unconditionally.
	/// Unlike changefeed GC this runs on every node without a lease: each node
	/// delivers only to the subscriptions it owns.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::lq", skip(self))
	)]
	pub(crate) async fn live_query_router_process(&self) -> Result<()> {
		// Only the Router engine delivers via the router.
		if self.config.datastore.live_query_engine != LiveQueryEngine::Router {
			return Ok(());
		}
		crate::lq::router::process(self, &self.live_query_router).await
	}

	// --------------------------------------------------
	// Indexing functions
	// --------------------------------------------------

	pub(crate) fn ensure_not_cancelled(canceller: &CancellationToken) -> Result<()> {
		if canceller.is_cancelled() {
			bail!(EngineError::QueryCancelled);
		}
		Ok(())
	}

	/// Every index definition in the catalog that is not pending removal, with
	/// the namespace, database and table it is defined on.
	///
	/// Read in one short read transaction, with the definitions cloned out so
	/// a caller's per-index work does not hold the catalog transaction open.
	///
	/// # Returns
	/// The definitions, and how many catalog entries a newer release wrote that
	/// this build cannot read, which are skipped.
	async fn defined_indexes(&self) -> Result<(Vec<DefinedIndex>, usize)> {
		let mut defined = Vec::new();
		let mut skipped = 0usize;
		let txn = self.transaction(Read).await?;
		let res: Result<()> = async {
			let nss = txn.all_readable(NamespacePrefix {}.range()?).await?;
			skipped += nss.refused;
			for ns in &nss.values {
				let dbs = txn
					.all_readable(
						DatabasePrefix {
							ns: ns.namespace_id,
						}
						.range()?,
					)
					.await?;
				skipped += dbs.refused;
				for db in &dbs.values {
					let tbs = txn
						.all_readable(
							TablePrefix {
								ns: ns.namespace_id,
								db: db.database_id,
							}
							.range()?,
						)
						.await?;
					skipped += tbs.refused;
					for tb in &tbs.values {
						let tb_name = crate::val::TableName::from(tb.name.clone());
						let ixs = txn
							.all_readable(
								IndexDefPrefix {
									ns: ns.namespace_id,
									db: db.database_id,
									tb: Cow::Borrowed(&tb_name),
								}
								.range()?,
							)
							.await?;
						skipped += ixs.refused;
						for ix in &ixs.values {
							// An index pending removal is cleared by the
							// tombstone reaper.
							if ix.prepare_remove {
								continue;
							}
							defined.push(DefinedIndex {
								ns: ns.namespace_id,
								ns_name: ns.name.clone(),
								db: db.database_id,
								db_name: db.name.clone(),
								tb: tb.table_id,
								ix: Arc::new(crate::catalog::IndexDefinition::from_stored(ix)?),
							});
						}
					}
				}
			}
			Ok(())
		}
		.await;
		let _ = txn.cancel().await;
		res?;
		Ok((defined, skipped))
	}

	/// Processes the index compaction queue.
	///
	/// This method is called periodically by the index compaction thread to
	/// process indexes that have been marked for compaction. It acquires a
	/// distributed lease to coordinate compaction across the cluster. Once a
	/// batch starts it runs to completion even if the lease expires, so brief
	/// overlap is possible.
	///
	/// The method scans the index compaction queue (stored as `Ic` keys) and
	/// delegates to [`Self::index_compaction_loop`], which compacts each
	/// distinct index exactly once — duplicate queue entries for the same
	/// index are skipped. On native targets the indexes are compacted in
	/// parallel as far as one compaction ticket is granted places of the
	/// node's pool, while on wasm they run sequentially.
	/// Indexes that support compaction include full-text, count, and HNSW.
	///
	/// The queue is read in a short-lived read transaction so that user
	/// transactions enqueueing new compaction requests do not conflict with
	/// the compaction cycle. Each index compaction runs on its own write
	/// transaction. Once all compactions have completed, a separate write
	/// transaction removes the processed queue entries. Compaction failures
	/// are logged but do not prevent other indexes from being processed.
	///
	/// # Arguments
	/// * `dbs` - The shared datastore instance, cloned into each compaction task
	/// * `interval` - The interval between compaction runs, used to calculate the lease duration
	/// * `canceller` - Token checked before starting each lease, batch, and compaction unit
	///
	/// # Returns
	/// A tuple `(iterations, errors)` where `iterations` is the number of
	/// compaction batches processed and `errors` is the total number of
	/// individual index compaction failures across all batches.
	/// Resume index builds stranded by a crashed or expired owner node.
	///
	/// A `CONCURRENTLY` index build runs as a detached task. If its owning node
	/// dies mid-build, nothing waits on that generation again, so the durable
	/// build state stays in `Building`/`Closing` and the index reports
	/// `status: indexing` with a frozen counter indefinitely. This scan adopts
	/// such builds — once their owner lease has expired — via the existing
	/// expired-lease takeover (see [`IndexBuilder::resume_stalled`]) and drives
	/// them to completion. A published build whose owner's lease has expired
	/// has its owner cleared instead, so compaction is no longer fenced off
	/// it.
	///
	/// The scan is lease-guarded so a single node runs it per cluster; the
	/// per-index takeover is additionally CAS-guarded, so correctness does not
	/// depend on the lease. Enumeration walks the catalog, so cost is
	/// proportional to the total index count; operators who prefer to recover
	/// stalled builds manually (with `REBUILD INDEX`) can disable the scan by
	/// setting its interval to zero.
	///
	/// Recovery is serialized: a pass adopts at most one stalled build, and
	/// no pass adopts anything while a local builder task is still running.
	/// A restart with several stranded builds (for example multiple full-text
	/// indexes on a small instance) therefore rebuilds them one at a time
	/// instead of multiplying the node's memory and CPU footprint; every
	/// stalled build is still recovered, one scan pass after the previous one
	/// finishes. User-initiated `REBUILD INDEX` is not throttled by this.
	///
	/// Returns the number of stalled builds adopted this pass.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, canceller)
		)
	)]
	pub(crate) async fn resume_stalled_index_builds(
		&self,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<usize> {
		Self::ensure_not_cancelled(&canceller)?;
		// Single-node-per-cluster guard. The lease lasts two intervals so an
		// in-flight scan isn't preempted between ticks.
		let lh = LeaseHandler::new_with_canceller(
			self.sequences.clone(),
			self.id,
			self.transaction_factory.clone(),
			TaskLeaseType::IndexBuildResume,
			interval * 2,
			canceller.clone(),
		)?;
		if !lh.has_lease().await? {
			return Ok(0);
		}
		// Serialize recovery: while any local builder task is still running
		// (an already adopted build or a user-started one), defer further
		// adoptions to a later pass. Without this, a restart with several
		// stranded builds starts them all at once, and the concurrent initial
		// scans can overwhelm a small instance's memory and CPU.
		if self.index_builder.has_unfinished_build().await {
			trace!(
				target: TARGET,
				"Deferring stalled index build adoption; a local index build is still running"
			);
			return Ok(0);
		}
		// An entry a newer release wrote, which this build cannot read, is
		// skipped and counted: an index this build cannot read is not adopted,
		// and the pass reports the count once.
		let (candidates, skipped) = self.defined_indexes().await?;
		if skipped > 0 {
			warn!(
				target: TARGET,
				skipped,
				"Index build adoption skipped catalog entries that need a newer release than \
				 this build"
			);
		}
		// Attempt a takeover for each candidate. `resume_stalled` is a cheap
		// no-op for healthy/online/live builds, so this is safe to call for
		// every index every pass. The snapshot may be stale by the time a
		// candidate is taken up; `resume_stalled` adopts a build only while the
		// catalog still holds the candidate's table and definition.
		let index_builder = &self.index_builder;
		let mut resumed = 0;
		for DefinedIndex {
			ns: ns_id,
			ns_name,
			db: db_id,
			db_name,
			tb: tb_id,
			ix,
		} in candidates
		{
			Self::ensure_not_cancelled(&canceller)?;
			#[cfg(test)]
			if let Some(hook) = test::take_before_resume_candidate(self.id) {
				hook().await;
			}
			// A scan that has lost its lease adopts nothing more: the node now
			// holding it recovers the stalled builds, one at a time.
			if !lh.try_maintain_lease().await? {
				trace!(target: TARGET, "Index build resume lease lost, stopping the scan");
				break;
			}
			let ctx = self.setup_ctx()?.freeze();
			let opt = self.setup_options(
				&Session::owner().with_ns(ns_name.as_str()).with_db(db_name.as_str()),
			);
			match index_builder
				.resume_stalled(&ctx, opt, ns_id, db_id, tb_id, Arc::clone(&ix))
				.await
			{
				Ok(true) => {
					resumed += 1;
					info!(
						target: TARGET,
						"Resuming stalled index build '{}' on table '{}'",
						ix.name, ix.table_name
					);
					// One adoption per pass: a later pass adopts the next
					// stalled build once this one has finished, keeping
					// recovery sequential on this node.
					break;
				}
				Ok(false) => {}
				Err(e) => {
					warn!(
						target: TARGET,
						"Failed to resume stalled index build '{}' on table '{}': {e}",
						ix.name, ix.table_name
					);
				}
			}
		}
		Ok(resumed)
	}

	/// Drains the index-compaction queue (`/!ic` keys) in bounded batches.
	///
	/// Each iteration scans at most [`INDEX_COMPACTION_QUEUE_BATCH_SIZE`]
	/// queue keys, runs one pass over every distinct index they reference, and
	/// then deletes exactly those keys in one write transaction, so no single
	/// transaction's key cardinality exceeds the batch size regardless of how
	/// many entries have accumulated. Entries enqueued after a batch's scan
	/// are untouched by its cleanup and are picked up by a later iteration
	/// (or a later invocation), so a queue entry is only ever removed after
	/// its index has been compacted at-or-after the entry was written, after
	/// a pass that stopped short of that queued a fresh entry in its place (see
	/// [`Self::run_index_compaction_pass`]), or after its index was found
	/// removed. A pass that could not act on an index's entries keeps one of
	/// them in place (see [`Self::index_compaction_batch`]); a drain visits such
	/// an entry once, and ends with it still queued for the next drain.
	///
	/// Batches rotate across indexes: each batch's scan resumes past the last
	/// index the previous batch covered and wraps at the end of the queue
	/// range, so every index with pending entries is visited once per
	/// rotation regardless of relative enqueue and drain rates — an index
	/// sustaining a full batch of new entries per iteration still cannot pin
	/// the scan to itself (a start-anchored or key-successor scan only stays
	/// fair while the drain outpaces the enqueue).
	///
	/// Where this datastore is the store's sole writer, a pass ends once it has
	/// compacted a batch that held everything queued when it was read: entries
	/// queued during the pass are left for the next one, which the write that
	/// queued them wakes after a debounce, so each pass compacts the writes of
	/// that interval together. On a store other nodes write to, whose commits
	/// do not wake this node, and under a backlog of full batches, the drain
	/// keeps running — every iteration stays bounded — and stops when the queue
	/// empties, the lease is lost, or the task is cancelled.
	///
	/// A drain that finds the queue empty, or ends with a batch that held the
	/// whole queue, sweeps the defined indexes for work no request names, at
	/// most once per lease period (see [`Self::sweep_unmaterialised_indexes`]),
	/// and drains what the sweep queued.
	///
	/// Coordinated across the cluster by a [`TaskLeaseType::IndexCompaction`]
	/// lease. Returns the number of processed batches and the number of
	/// indexes that failed to compact.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(dbs, canceller)
		)
	)]
	pub async fn index_compaction(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<(usize, usize)> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting index compaction process");
		// Create a new lease handler
		let lh = LeaseHandler::new_with_canceller(
			dbs.sequences.clone(),
			dbs.id,
			dbs.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			interval * 2,
			canceller.clone(),
		)?;
		let mut count_iteration = 0;
		let mut count_error = 0;
		// The queue range and a rotating window within it. After each batch
		// the window resumes past the last index that batch covered, so
		// indexes later in the keyspace are reached even while an
		// earlier-sorting index keeps enqueueing new entries (queue keys
		// sort by ns/db/tb/ix before their time-ordered UUID, so a hot
		// index would otherwise pin a start-anchored scan to itself). When
		// the scan reaches the end of the range it wraps to the start, and
		// a wrap that finds nothing means the queue is drained.
		let queue = IndexCompactionPrefix {}.range()?;
		let mut window = queue.clone();
		// What this drain's batches left queued, which later batches do not pass
		// again (see `CompactionDrain`). A lap of the queue that finds nothing
		// else ends the drain with those requests still queued. Keeping a
		// request writes nothing and wakes nothing, so it waits for the next
		// drain.
		let mut drain = CompactionDrain::default();
		// Whether the current lap of the queue acted on a request: passed it, or
		// deleted it.
		let mut lap_passed = false;
		// We continue without interruptions while there are keys and the lease
		'compaction: loop {
			Self::ensure_not_cancelled(&canceller)?;
			// Attempt to acquire a lease for the IndexCompaction task
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok((count_iteration, count_error));
			}
			Self::ensure_not_cancelled(&canceller)?;
			// Output function invocation details to logs
			trace!(target: TARGET, "Running index compaction process");
			// Read one bounded batch of queue keys in a short-lived read
			// transaction. The queue holds one entry per indexed record
			// write since it last drained, so an unbounded snapshot would
			// make the cleanup transaction below arbitrarily large — every
			// key it deletes is a per-replica write reservation on
			// distributed backends. Queue values carry no payload, so a
			// keys-only scan suffices.
			let (keys, sole_writer) = {
				let txn = dbs.transaction(Read).await?;
				let res =
					txn.keys(window.clone(), INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None).await;
				let sole_writer = txn.sole_writer();
				let _ = txn.cancel().await;
				(res?, sole_writer)
			};
			Self::ensure_not_cancelled(&canceller)?;
			// A batch read from the start of the queue that came back short of a
			// full batch is everything queued when it was read. Where this
			// datastore is the store's sole writer, the pass ends once it is
			// compacted, rather than going straight back for the entries queued
			// meanwhile: each was queued by a commit of this datastore, which wakes
			// the next pass after a debounce, so a pass compacts the writes of that
			// interval together instead of a handful at a time. On a store other
			// nodes write to, their commits do not wake this node, so the pass keeps
			// going until the queue is empty. A full batch, or one read part-way
			// through the queue, is a backlog, and the pass keeps going until it is
			// drained. So is a batch one of whose passes stopped at its budget: the
			// request it queued is that pass's own work left, not writes still
			// arriving, so the drain goes on with it at once rather than ending
			// and waiting a debounce per budget for the wake-up its commit sent.
			let mut whole_queue = sole_writer
				&& window.start() == queue.start()
				&& keys.len() < INDEX_COMPACTION_QUEUE_BATCH_SIZE as usize;
			if keys.is_empty() {
				if window.start() == queue.start() {
					// Nothing left anywhere in the queue. An index can still hold
					// work no request names, so the queue is swept for it, and a
					// sweep that queued requests is drained by this same call.
					if Self::sweep_after_drain(&dbs, interval, &canceller, &mut drain).await {
						continue;
					}
					return Ok((count_iteration, count_error));
				}
				if !lap_passed {
					// Nothing but the requests this drain already kept, which is
					// a queue drained as far as this drain can: swept as an
					// empty one is.
					if Self::sweep_after_drain(&dbs, interval, &canceller, &mut drain).await {
						window = queue.clone();
						continue;
					}
					return Ok((count_iteration, count_error));
				}
				// End of the range: wrap to re-scan entries that were
				// skipped when the window seeked past a partially-drained
				// index.
				window = queue.clone();
				lap_passed = false;
				continue;
			}
			// Every key the drain has not kept is either passed or deleted.
			let acts = keys.iter().any(|k| !drain.kept.contains(k));
			if acts {
				let Some(cleanup) =
					Self::index_compaction_batch(Arc::clone(&dbs), &lh, &keys, &drain, &canceller)
						.await?
				else {
					// Another node holds the lease now, and the batch's requests
					// are its to act on.
					return Ok((count_iteration, count_error));
				};
				count_iteration += 1;
				lap_passed = true;
				count_error += cleanup.errors;
				if !cleanup.deleted {
					break 'compaction;
				}
				whole_queue &= !cleanup.budgeted;
				drain.kept.extend(cleanup.kept);
				drain.retained.extend(cleanup.retained);
				drain.passed.extend(cleanup.passed);
			}
			// Seek the next batch past the last index this batch covered.
			// Entries of that index beyond this batch are picked up again
			// after the window wraps, so a continuously-refilling index
			// cannot starve later-sorting ones. A key that fails to decode
			// cannot name an index to seek past: fall back to resuming
			// immediately after the key itself, so the scan still advances past
			// the undecodable entry the batch's cleanup removed (it references
			// no valid index, so deleting it cannot violate the
			// compact-before-delete invariant).
			if let Some(last_key) = keys.last() {
				window = match IndexCompactionKey::decode_key(last_key) {
					Ok(last) => {
						// The first key ordering after the whole index's run of
						// queue entries.
						let covered = IndexCompactionIxPrefix {
							ns: last.ns,
							db: last.db,
							tb: Cow::Owned(last.tb.into_owned()),
							ix: last.ix,
						}
						.skip_extensions()?;
						queue.clone().resume_after(&covered, Direction::Forward)
					}
					Err(e) => {
						warn!(target: TARGET, "Skipping undecodable index compaction queue entry: {e}");
						queue.clone().resume_after(last_key, Direction::Forward)
					}
				};
			}
			#[cfg(test)]
			if acts && let Some(hook) = test::take_after_compaction_batch(dbs.id) {
				hook().await;
			}
			if whole_queue {
				// The batch held the whole queue, so the queue this drain
				// leaves is empty but for what it kept and what was queued
				// meanwhile: swept, as a drain that reads it empty sweeps it.
				if Self::sweep_after_drain(&dbs, interval, &canceller, &mut drain).await {
					window = queue.clone();
					lap_passed = false;
					continue;
				}
				break;
			}
		}
		Ok((count_iteration, count_error))
	}

	/// Runs one pass over every distinct index a batch of queue keys names, and
	/// then deletes the keys whose pass acted on them.
	///
	/// A request is deleted only by a pass that could act on it:
	///
	/// * A pass that compacted its index, or found it removed, deletes every key of the index in
	///   the batch. One that stopped short of folding the backlog — with work left, on a lost race,
	///   or on a retryable failure it could not retry away — has already queued a fresh request
	///   under a key this batch does not hold. A pass that fails on anything else leaves its index
	///   to its next write.
	/// * A pass whose fresh request failed to commit keeps every key of the index in the batch, as
	///   the only record of its remaining work.
	/// * A pass that could not act on its requests ([`CompactionPassOutcome::retains_request`])
	///   keeps one key of the index in place and deletes the rest, so a build that queues a request
	///   per batch leaves the queue one request per index rather than one per batch. The key kept
	///   is one the drain already kept, else the index's first key in the batch, so the request a
	///   later call finds is the one kept first.
	///
	/// An index the drain has already passed is not passed again (see
	/// [`CompactionDrain`]). Of an index whose one request an earlier batch kept,
	/// the batch keeps that request if it holds it and deletes every other: the
	/// index's request is already queued, so they record nothing more. Of any
	/// other index whose keys in the batch the drain has all kept, the batch
	/// keeps them.
	///
	/// The batch first confirms the lease. One that finds another node holds
	/// it runs no pass, queues nothing and deletes nothing, leaving its requests
	/// to that node, and returns `None`.
	///
	/// Returns what the committed cleanup left queued, for the drain to record.
	async fn index_compaction_batch(
		dbs: Arc<Datastore>,
		lh: &LeaseHandler,
		keys: &[Vec<u8>],
		drain: &CompactionDrain,
		canceller: &CancellationToken,
	) -> Result<Option<CompactionBatchCleanup>> {
		// Each key is decoded once, and the decoded form serves both the passes
		// and the cleanup.
		let decoded = decode_index_compaction_keys(keys);
		// The indexes to pass, each once, in queue order.
		let mut seen = HashSet::new();
		let indexes: Vec<IndexKeyBase> = keys
			.iter()
			.zip(&decoded)
			.filter_map(|(k, ikb)| ikb.as_ref().filter(|ikb| drain.passes(k, Some(ikb))))
			.filter(|ikb| seen.insert(*ikb))
			.cloned()
			.collect();
		let lease = CompactionLease::new(lh);
		Self::ensure_not_cancelled(canceller)?;
		if !lease.maintain().await? {
			trace!(target: TARGET, "Index compaction lease lost, leaving the batch to its holder");
			return Ok(None);
		}
		let report =
			Self::index_compaction_loop(Arc::clone(&dbs), &lease, &indexes, canceller.clone())
				.await?;
		let (deletes, still_queued) =
			Self::partition_index_compaction_batch(keys, &decoded, drain, &report);
		let deleted = Self::delete_index_compaction_batch(&dbs, &deletes, canceller).await?;
		Ok(Some(if deleted {
			CompactionBatchCleanup {
				errors: report.errors,
				deleted,
				kept: still_queued,
				retained: report.retained,
				budgeted: report.budgeted,
				passed: indexes,
			}
		} else {
			CompactionBatchCleanup {
				errors: report.errors,
				deleted,
				kept: Vec::new(),
				retained: HashSet::new(),
				budgeted: false,
				passed: indexes,
			}
		}))
	}

	/// Splits a batch's keys into those its cleanup deletes and those it keeps,
	/// by the rules on [`Self::index_compaction_batch`].
	///
	/// Undecodable keys are always deleted: an undecodable entry names no
	/// index, so it can never be compacted, and leaving it would park it at the
	/// head of the queue forever.
	fn partition_index_compaction_batch<'a>(
		keys: &'a [Vec<u8>],
		decoded: &[Option<IndexKeyBase>],
		drain: &CompactionDrain,
		report: &CompactionBatchReport,
	) -> (Vec<&'a [u8]>, Vec<Vec<u8>>) {
		let kept = &drain.kept;
		// The indexes this batch passed.
		let mut passed = HashSet::new();
		// For each index that keeps one request, the key it keeps.
		let mut chosen: HashMap<&IndexKeyBase, &[u8]> = HashMap::new();
		for (key, ikb) in keys.iter().zip(decoded) {
			let Some(ikb) = ikb else {
				continue;
			};
			let already_kept = kept.contains(key);
			if drain.passes(key, Some(ikb)) {
				passed.insert(ikb);
			}
			if report.retained.contains(ikb) {
				match chosen.entry(ikb) {
					Entry::Vacant(e) => {
						e.insert(key);
					}
					Entry::Occupied(mut e) => {
						if already_kept && !kept.contains(*e.get()) {
							e.insert(key);
						}
					}
				}
			}
		}
		let mut deletes = Vec::new();
		let mut still_queued = Vec::new();
		for (key, ikb) in keys.iter().zip(decoded) {
			let keep = match ikb {
				None => false,
				Some(ikb) if report.unqueued.contains(ikb) => true,
				Some(ikb) if drain.retained.contains(ikb) => kept.contains(key),
				Some(ikb) if !passed.contains(ikb) => true,
				Some(ikb) if report.retained.contains(ikb) => {
					chosen.get(ikb).is_some_and(|c| *c == key.as_slice())
				}
				Some(_) => false,
			};
			if keep {
				still_queued.push(key.clone());
			} else {
				deletes.push(key.as_slice());
			}
		}
		(deletes, still_queued)
	}

	/// Deletes `deletes`, one batch's queue entries, in a write transaction of
	/// its own, bounded by the batch size, which keeps the cleanup from
	/// conflicting with the user transactions that enqueue new compaction
	/// requests. The entries are deleted as bytes rather than through the
	/// decoded key, because a batch deliberately includes entries that do not
	/// decode, which a typed delete could never address.
	///
	/// Returns whether the cleanup committed. A retryable conflict on a delete
	/// or the commit is retried, at most [`INDEX_COMPACTION_PASS_MAX_RETRIES`]
	/// times and after the same wait as a pass's retries. A failure on
	/// anything else, or a conflict that outlasts the retries, is logged and
	/// leaves every entry queued, as the cleanup's transaction is discarded:
	/// the cleanup never loses a request, and the drain that ran it stops.
	async fn delete_index_compaction_batch(
		dbs: &Datastore,
		deletes: &[&[u8]],
		canceller: &CancellationToken,
	) -> Result<bool> {
		if deletes.is_empty() {
			Self::ensure_not_cancelled(canceller)?;
			return Ok(true);
		}
		let mut retries = 0;
		loop {
			match Self::try_delete_index_compaction_batch(dbs, deletes, canceller).await? {
				Ok(()) => return Ok(true),
				Err(e) => {
					if retries < INDEX_COMPACTION_PASS_MAX_RETRIES
						&& Self::retry_index_operation_conflict(
							&e,
							"Retryable conflict cleaning up the compaction queue, retrying",
						)
						.await
					{
						retries += 1;
						continue;
					}
					warn!(target: TARGET, "Failed to clean up the compaction queue: {e}");
					return Ok(false);
				}
			}
		}
	}

	/// One attempt of [`Self::delete_index_compaction_batch`]. The outer error
	/// is a cancellation; the inner one is the failure of a delete or the
	/// commit, after which the attempt's transaction has been discarded.
	///
	/// Each cancel here ignores its own error: the cancellation or the failure
	/// before it is what the attempt returns, and a cleanup that has not
	/// committed deletes no request, so a cancel that fails leaves every entry
	/// queued, as the caller assumes.
	async fn try_delete_index_compaction_batch(
		dbs: &Datastore,
		deletes: &[&[u8]],
		canceller: &CancellationToken,
	) -> Result<Result<()>> {
		let txn = dbs.transaction(Write).await?;
		if let Err(e) = Self::ensure_not_cancelled(canceller) {
			let _ = txn.cancel().await;
			return Err(e);
		}
		for k in deletes {
			if let Err(e) = txn.del(Key::from(*k)).await {
				let _ = txn.cancel().await;
				return Ok(Err(e));
			}
		}
		if let Err(e) = Self::ensure_not_cancelled(canceller) {
			let _ = txn.cancel().await;
			return Err(e);
		}
		#[cfg(test)]
		if let Err(e) = maybe_inject_retryable_conflict(
			RetryableConflictSite::IndexCompactionQueueCleanup,
			dbs.id,
		) {
			let _ = txn.cancel().await;
			return Ok(Err(e));
		}
		if let Err(e) = txn.commit().await {
			let _ = txn.cancel().await;
			return Ok(Err(e));
		}
		Ok(Ok(()))
	}

	/// Periodically drains the background reclaim queue (`/!rc` keys),
	/// destroying the data prefix of namespaces/databases/indexes that were
	/// removed by a committed `REMOVE` statement.
	///
	/// `REMOVE NAMESPACE/DATABASE/INDEX` delete only the catalog definition
	/// inside the user transaction and enqueue a [`ReclaimKey`]; this task
	/// performs the expensive data deletion out-of-band so the user statement
	/// returns immediately and never trips a request/transaction timeout.
	///
	/// Coordinated across the cluster by a [`TaskLeaseType::ReclaimTombstones`]
	/// lease so only one node reclaims at a time. The queue is walked in batches
	/// of at most [`RECLAIM_BATCH_SIZE`] entries, each read in a short-lived
	/// read transaction and each retired or stamped in one write transaction of
	/// its own, so neither the pass's snapshot nor its queue update grows with
	/// the number of pending removals. Each prefix is then destroyed
	/// independently and idempotently — in one out-of-transaction call on a
	/// backend offering [`DestroyRange`], otherwise in bounded pages, each
	/// committed together with the resume cursor that accounts for it. A queue
	/// entry is retired only once its prefix is empty, so a crash or an
	/// exhausted budget mid-reclaim resumes from the committed cursor on a later
	/// pass instead of restarting the prefix.
	///
	/// One pass deletes at most [`RECLAIM_PASS_KEY_BUDGET`] data keys in total,
	/// so a prefix larger than that is finished over several ticks rather than
	/// monopolising the schedule this task shares. That budget is divided among
	/// the entries a batch still has to visit, floored at one page each: an
	/// entry naming an oversized prefix takes a page and yields to the next
	/// instead of spending the whole pass on itself, so each of the first
	/// `RECLAIM_PASS_KEY_BUDGET / RECLAIM_BATCH_SIZE` entries that still hold
	/// data makes progress on every pass.
	///
	/// Snapshot safety: a queued removal is only reclaimed once it has been
	/// *observed* by this task for at least `grace`. The reclaim task destroys
	/// data out-of-band, and on a backend offering out-of-transaction range
	/// destruction that destruction bypasses MVCC, so a read transaction whose
	/// snapshot predates the `REMOVE` must be given time to finish first. On
	/// first sight the task stamps the entry's
	/// [`ReclaimState::observed_ms`]; aging is measured from there, not from the
	/// key's `uid` (a UUIDv7 stamped while the `REMOVE` statement runs, which can
	/// be arbitrarily earlier than the commit inside a long `BEGIN`/`COMMIT`
	/// block). Because the task only ever reads committed entries, `observed_ms`
	/// is always at or after the removal's commit, so `age >= grace` implies the
	/// commit is at least `grace` old — equivalent to it having fallen behind the
	/// MVCC GC safepoint (`now - tikv_gc_lifetime`) when `grace >= tikv_gc_lifetime`,
	/// by which point any transaction that could still read the data has expired.
	/// A `grace` of zero reclaims immediately without stamping (used by tests and
	/// the language-test harness teardown, where there are no concurrent readers).
	///
	/// Returns `(iterations, errors)`.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(dbs, canceller)
		)
	)]
	pub async fn reclaim_tombstones(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		canceller: CancellationToken,
	) -> Result<(usize, usize)> {
		Self::reclaim_tombstones_bounded(dbs, interval, grace, RECLAIM_PASS_KEY_BUDGET, canceller)
			.await
	}

	/// [`Self::reclaim_tombstones`] with the per-pass key budget lowered, so a
	/// test can drive a pass that stops mid-prefix and inspect what it left
	/// behind.
	#[cfg(test)]
	#[cfg_attr(
		not(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv")),
		allow(dead_code)
	)]
	pub(crate) async fn reclaim_tombstones_with_budget(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		budget: u64,
		canceller: CancellationToken,
	) -> Result<(usize, usize)> {
		Self::reclaim_tombstones_bounded(dbs, interval, grace, budget, canceller).await
	}

	/// One reclaim pass, deleting at most `budget` data keys in total.
	///
	/// The pass walks the whole queue and spends its key budget on part of it,
	/// which are two different bounds and are enforced separately.
	///
	/// The walk is unconditional. Stamping an entry's first sighting and ageing it
	/// against the grace costs at most one write per entry, so that work scales
	/// with the number of pending removals — catalog scale — and not with the data
	/// they name. Gating it on the key budget would leave an entry beyond the
	/// budget's reach unstamped, and an unstamped entry has `observed_ms == 0`, so
	/// its grace has not started and it is not yet even a candidate for reclaim.
	/// The queue is walked in bounded batches, each batch's stamps committed in one
	/// transaction, so the walk's memory and write batches stay proportional to one
	/// batch however long the queue is.
	///
	/// The key budget is then spent on the [`RECLAIM_PASS_ENTRY_QUOTA`]
	/// longest-observed eligible entries, selected while walking so only that many
	/// are ever held. Oldest-first rather than queue order: key order makes the
	/// entries that happen to sort lowest the only ones ever served while they hold
	/// data, whereas the wait an operator cares about is how long a removal has
	/// gone unreclaimed. Spreading the budget across every eligible entry instead
	/// would shrink each share towards one key as the queue grows.
	async fn reclaim_tombstones_bounded(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		budget: u64,
		canceller: CancellationToken,
	) -> Result<(usize, usize)> {
		trace!(target: TARGET, "Attempting tombstone reclaim process");
		// Data keys this pass may still delete, shared out across the entries it
		// serves. Spent to zero ends the spend with every cursor committed.
		let mut budget = budget;
		// Coordinate across the cluster so only one node reclaims at a time.
		let lh = LeaseHandler::new_with_canceller(
			dbs.sequences.clone(),
			dbs.id,
			dbs.transaction_factory.clone(),
			TaskLeaseType::ReclaimTombstones,
			interval * 2,
			canceller.clone(),
		)?;
		let mut count_iteration = 0;
		let mut count_error = 0;
		// The queue range and a window within it, advanced past each batch the
		// walk reads. Reading the queue in bounded batches is what keeps both the
		// snapshot the pass holds and the write transaction that updates it
		// proportional to one batch rather than to the number of pending removals.
		let queue = ReclaimPrefix {}.range()?;
		let mut window = queue.clone();
		// The longest-observed eligible entries seen so far, at most
		// `RECLAIM_PASS_ENTRY_QUOTA` of them. A max-heap ordered oldest-first, so
		// the entry discarded when the quota is exceeded is the newest held.
		let mut candidates: BinaryHeap<ReclaimCandidate> = BinaryHeap::new();
		// Whether the walk finished holding the lease. Losing it mid-walk ends the
		// pass: the deletes below would otherwise run against a prefix the node
		// that holds the lease now is already working on.
		let mut lease_held = true;
		// Walk the queue: stamp, age, and select. No deletes, so no budget.
		loop {
			Self::ensure_not_cancelled(&canceller)?;
			// If we don't hold the lease, another node is handling this task.
			if !lh.has_lease().await? {
				lease_held = false;
				break;
			}
			Self::ensure_not_cancelled(&canceller)?;
			// Read one bounded batch of queue entries in a short-lived read
			// transaction, to avoid holding a write lock across the entire
			// reclaim cycle. Values are read as bytes so that an entry this node
			// cannot decode is skipped with a warning rather than failing the
			// whole pass.
			let items = {
				let txn = dbs.transaction(Read).await?;
				let res = txn.scan_raw(window.clone(), RECLAIM_BATCH_SIZE, 0, None).await;
				let _ = txn.cancel().await;
				res?
			};
			Self::ensure_not_cancelled(&canceller)?;
			let Some((last, _)) = items.last().cloned() else {
				// The window has run off the end of the queue: everything queued
				// has been walked.
				break;
			};
			count_iteration += 1;
			// Current wall-clock time (unix millis), used to age each entry
			// against `grace` and to stamp first observations.
			let now_ms = web_time::SystemTime::now()
				.duration_since(web_time::SystemTime::UNIX_EPOCH)
				.map(|d| d.as_millis() as u64)
				.unwrap_or(0);
			let grace_ms = grace.as_millis() as u64;
			// Queue states to initialise or repair. The raw value read with each
			// one is the compare-and-set guard, so a concurrent cursor advance or
			// cancellation cannot be overwritten by this batch.
			let mut to_stamp: Vec<(Vec<u8>, Vec<u8>, ReclaimState)> = Vec::new();
			for (k, v) in items.iter() {
				Self::ensure_not_cancelled(&canceller)?;
				if let Err(e) = ReclaimKey::decode_key(k) {
					count_error += 1;
					warn!(target: TARGET, "Failed to decode reclaim queue entry: {e}");
					continue;
				}
				// The state carries both the observation stamp the grace is
				// measured from and the resume cursor a partly-reclaimed prefix
				// continues at, so it is read before either is consulted.
				// A value this node cannot read still names a prefix whose catalog
				// entry is committed-removed, so the entry is restarted rather than
				// skipped: skipping it strands data that nothing else can reach,
				// because the queue entry is the only remaining name for it.
				// Restarting is safe in both fields — treating the entry as never
				// observed keeps the grace gate conservative, and discarding an
				// unreadable cursor only re-scans pages that may already be empty,
				// which a delete tolerates.
				let state = match ReclaimState::kv_decode_value(v, ()) {
					Ok(s) => s,
					Err(e) => {
						count_error += 1;
						warn!(
							target: TARGET,
							"Restarting a reclaim queue entry whose state could not be read: {e}",
						);
						// Repair the unreadable state conditionally before attempting
						// the reclaim. A typed cursor CAS cannot use unreadable bytes as
						// its expected value, and a blind repair could resurrect a claim
						// concurrently cancelled on another node.
						let repaired = ReclaimState {
							observed_ms: if grace.is_zero() {
								0
							} else {
								now_ms
							},
							cursor: None,
						};
						to_stamp.push((k.clone(), v.clone(), repaired.clone()));
						// Zero-grace callers may reclaim immediately after the
						// conditional repair commits. If that repair loses a race, the
						// page's own CAS sees that `repaired` is not current and stops.
						if grace.is_zero() {
							candidates.push(ReclaimCandidate {
								observed_ms: repaired.observed_ms,
								key: k.clone(),
								state: repaired,
							});
							if candidates.len() > RECLAIM_BATCH_SIZE as usize {
								candidates.pop();
							}
						}
						continue;
					}
				};
				// A zero grace reclaims immediately, skipping the
				// observe-then-age dance (tests / harness teardown — no
				// concurrent readers).
				if !grace.is_zero() {
					if state.observed_ms == 0 {
						// First sighting of this committed entry — record an
						// observation time (>= commit) and defer reclaim. Aging
						// is measured from here, never from the pre-commit
						// `uid`. Not a candidate this pass: its grace starts now.
						to_stamp.push((
							k.clone(),
							v.clone(),
							ReclaimState {
								observed_ms: now_ms,
								cursor: state.cursor.clone(),
							},
						));
						continue;
					}
					// Snapshot-safety gate: only destroy data once it has been
					// observed for at least `grace`, by which point any
					// transaction that could still read it is behind the MVCC
					// GC safepoint.
					if now_ms.saturating_sub(state.observed_ms) < grace_ms {
						continue;
					}
				}
				candidates.push(ReclaimCandidate {
					observed_ms: state.observed_ms,
					key: k.clone(),
					state,
				});
				if candidates.len() > RECLAIM_BATCH_SIZE as usize {
					// The newest held entry gives up its place: it has waited
					// least, so it loses to every other candidate. Held up to one
					// batch rather than up to the share divisor, because an entry
					// whose reclaim spends nothing — an already-empty prefix, or a
					// backend that drops the range in one call — costs no budget,
					// and the spend below walks through as many of those as it
					// finds. Capping the set at the divisor would stop that at the
					// divisor and throttle the backends where reclaim is cheapest.
					// One batch of candidates is the memory the pass already holds
					// for one batch of scanned entries.
					candidates.pop();
				}
			}
			// Persist this batch's stamps in one write transaction, bounded by the
			// batch as the paged data delete is bounded by the page. Done
			// separately from the read scan so we don't conflict with concurrent
			// enqueues.
			if !to_stamp.is_empty() {
				let txn = dbs.transaction(Write).await?;
				if let Err(e) = Self::ensure_not_cancelled(&canceller) {
					let _ = txn.cancel().await;
					return Err(e);
				}
				let mut condition_failed = false;
				for (k, expected, state) in &to_stamp {
					let value = match state.kv_encode_value() {
						Ok(value) => value,
						Err(e) => {
							let _ = txn.cancel().await;
							return Err(e);
						}
					};
					match txn.put_compare(Key::from(k.clone()), value, Some(expected.clone())).await
					{
						Ok(()) => {}
						Err(e) if super::is_conditional_write_conflict(&e) => {
							condition_failed = true;
							break;
						}
						Err(e) => {
							warn!(target: TARGET, "Failed to stamp reclaim queue entry: {e}");
						}
					}
				}
				if condition_failed {
					let _ = txn.cancel().await;
				} else {
					// A stamp that does not land leaves the entry unobserved, so the
					// next pass starts its grace instead. Counted rather than silent:
					// an uncounted failure here reports as a healthy pass while the
					// entry's clock never starts.
					if let Err(e) = txn.commit().await {
						let _ = txn.cancel().await;
						if !super::is_conditional_write_conflict(&e) {
							count_error += 1;
							warn!(target: TARGET, "Failed to commit reclaim queue stamps: {e}");
							return Ok((count_iteration, count_error));
						}
					}
				}
			}
			// Seek past the batch just walked; the window advancing is also what
			// terminates the walk, at the end of the queue.
			window = queue.clone().resume_after(&last, Direction::Forward);
		}
		if !lease_held {
			return Ok((count_iteration, count_error));
		}
		// Spend the key budget on the selected entries, longest-observed first.
		// `into_sorted_vec` is ascending in the candidate order, which is the
		// oldest observation first and the queue's key order between entries
		// observed in the same millisecond.
		let selected = candidates.into_sorted_vec();
		// Entries whose prefix this pass emptied, to delete from the queue. An
		// entry the pass only partly emptied is left queued for a later pass.
		let mut done: Vec<Vec<u8>> = Vec::with_capacity(selected.len());
		// How many of the selected entries are still to spend from the budget, so
		// each takes its slice of what is left rather than all of it.
		let mut remaining = selected.len() as u64;
		for candidate in selected {
			Self::ensure_not_cancelled(&canceller)?;
			// Stop at the entry the lease was lost on. The queue update below
			// still commits what this pass emptied, and whichever node holds
			// the lease now continues from the durable cursors. The answer has
			// to be read rather than discarded: a lease check is throttled to
			// one datastore read per maintenance period, so a call that drops
			// it consumes the read the page loop's own check depends on and
			// leaves that check answering affirmatively without asking.
			if !lh.try_maintain_lease().await? {
				break;
			}
			let ReclaimCandidate {
				key,
				state,
				..
			} = candidate;
			// Decoded during the walk that selected it, so this cannot fail; a
			// failure is still counted rather than panicking on it.
			let rc = match ReclaimKey::decode_key(&key) {
				Ok(rc) => rc,
				Err(e) => {
					count_error += 1;
					warn!(target: TARGET, "Failed to decode reclaim queue entry: {e}");
					remaining = remaining.saturating_sub(1);
					continue;
				}
			};
			// Keys this entry may delete before the pass moves on: its share of
			// what the budget has left, across the entries still to spend from
			// it, capped at the budget so the tail of a pass hands out only what
			// remains. An entry naming a prefix larger than its share therefore
			// cannot take the whole budget and leave its siblings untouched.
			// Floored at one key rather than one page: a share below a page
			// still deletes, since the page size caps a transaction rather than
			// setting a minimum.
			// The divisor is the entries still to spend, capped at the quota: a
			// pass holding a batch of candidates would otherwise cut every share
			// to a thousandth of the budget, so no entry would delete a useful
			// amount. Capped, each served entry gets about a page and the pass
			// stops when the budget runs out — which is what makes the
			// oldest-first order above decide who is served.
			let divisor = remaining.min(RECLAIM_PASS_ENTRY_QUOTA as u64);
			let share = (budget / divisor.max(1)).max(1).min(budget);
			remaining = remaining.saturating_sub(1);
			let mut left = share;
			// Whether the lease passed to another node mid-entry. Recorded
			// rather than broken on directly, so the budget below is charged
			// for the pages this entry did commit before the pass ends.
			let mut lease_lost = false;
			match dbs.reclaim_decoded(&rc, &state, &lh, &canceller, &mut left).await {
				// The prefix is empty, so the entry has nothing left to
				// name and can be retired.
				Ok(ReclaimOutcome::Complete) => done.push(key),
				// Progress is durable but keys remain: leaving the entry
				// queued is what lets a later pass resume it.
				Ok(ReclaimOutcome::Incomplete) => {}
				// The entry is already gone, so it must not be added to
				// `done`: that would delete whatever a concurrent writer
				// put at the same key.
				Ok(ReclaimOutcome::Cancelled) => {}
				// Whichever node holds the lease now resumes from the
				// cursors this pass committed.
				Ok(ReclaimOutcome::LeaseLost) => lease_lost = true,
				// Shutdown. Not a failure of this entry, so it is neither
				// counted nor logged; the queue update below refuses to
				// commit and the pass ends there.
				Err(e)
					if matches!(
						crate::err::engine_error(&e),
						Some(EngineError::QueryCancelled)
					) =>
				{
					break;
				}
				Err(e) => {
					count_error += 1;
					warn!(target: TARGET, "Tombstone reclaim failed for a queue entry: {e}");
				}
			}
			// Charge what the entry deleted, not what it was offered.
			budget = budget.saturating_sub(share.saturating_sub(left));
			if lease_lost {
				// The queue update below still commits what this pass
				// emptied; the node holding the lease now takes the rest.
				break;
			}
			if budget == 0 {
				// The pass has spent its key budget. Every cursor it
				// advanced is committed, so the next tick continues from
				// there rather than repeating this work.
				break;
			}
		}
		// Retire the entries this pass emptied, in one write transaction bounded
		// by the entry quota.
		if !done.is_empty() {
			let txn = dbs.transaction(Write).await?;
			if let Err(e) = Self::ensure_not_cancelled(&canceller) {
				let _ = txn.cancel().await;
				return Err(e);
			}
			// Retire only the value currently stored. This prevents a stale
			// completion from deleting a state advanced by another node, and
			// makes a concurrent cancellation win cleanly on TiKV.
			let mut condition_failed = false;
			for k in &done {
				let current = match txn.get(Key::from(k), None).await {
					Ok(Some(current)) => current,
					Ok(None) => continue,
					Err(e) => {
						warn!(target: TARGET, "Failed to read reclaim queue entry: {e}");
						continue;
					}
				};
				match txn.del_compare(Key::from(k), Some(&current)).await {
					Ok(()) => {}
					Err(e) if super::is_conditional_write_conflict(&e) => {
						condition_failed = true;
						break;
					}
					Err(e) => {
						warn!(target: TARGET, "Failed to delete reclaim queue entry: {e}");
					}
				}
			}
			if condition_failed {
				let _ = txn.cancel().await;
				return Ok((count_iteration, count_error));
			}
			// A pass whose queue update does not land has reclaimed data
			// without recording that it did, so it is counted: an uncounted
			// failure here reports as a healthy pass and leaves nothing for an
			// operator to see but the next tick repeating the same work.
			if let Err(e) = txn.commit().await {
				let _ = txn.cancel().await;
				if !super::is_conditional_write_conflict(&e) {
					count_error += 1;
					warn!(target: TARGET, "Failed to commit reclaim queue update: {e}");
				}
			}
		}
		Ok((count_iteration, count_error))
	}

	/// Whether the table a queue entry names still has a consumer of the
	/// shared doc-ID space: a doc-ID index, or the `graph_doc_ids` marker a
	/// numeric graph fold records (its blocks resolve through the space). A
	/// table with no definition has no consumer — its whole keyspace is
	/// reclaimed separately.
	async fn tb_has_doc_id_consumer(&self, rc: &ReclaimKey<'_>) -> Result<bool> {
		let txn = self.transaction(Read).await?;
		let res = async {
			let Some(tb) = txn.get_tb(rc.ns, rc.db, rc.tb.as_ref(), None).await? else {
				return Ok(false);
			};
			if tb.graph_doc_ids {
				return Ok(true);
			}
			let ixs = txn.all_tb_indexes(rc.ns, rc.db, rc.tb.as_ref(), None).await?;
			Ok(ixs.iter().any(|ix| ix.uses_shared_doc_ids()))
		}
		.await;
		let _ = txn.cancel().await;
		res
	}

	/// Destroy the data prefix a decoded reclaim queue entry names.
	///
	/// Resolves the entry's kind to the byte range beneath it and hands that
	/// range to [`Self::reclaim_range`]. All of these kinds name a subtree holding
	/// keys of more than one type, so the range is resolved as bytes.
	async fn reclaim_decoded(
		&self,
		rc: &ReclaimKey<'_>,
		state: &ReclaimState,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		budget: &mut u64,
	) -> Result<ReclaimOutcome> {
		let prefix = match rc.kind {
			ReclaimKind::Namespace => NsRoot {
				ns: rc.ns,
			}
			.raw_range()?,
			ReclaimKind::Database => DbRoot {
				ns: rc.ns,
				db: rc.db,
			}
			.raw_range()?,
			ReclaimKind::Index => IdxRoot {
				ns: rc.ns,
				db: rc.db,
				tb: Cow::Borrowed(rc.tb.as_ref()),
				ix: rc.ix,
			}
			.raw_range()?,
			// The bands of a table's shared doc-ID space, each its own entry so
			// each carries its own resume cursor. `rc.ix` is unused for these:
			// the space belongs to the table, not to any one index.
			//
			// The space is only orphaned while the table has no doc-ID consumer.
			// A consumer present here means one was defined after the entry was
			// queued without cancelling it — a node too old to know these kinds,
			// or too old to know the `graph_doc_ids` marker, is the case that
			// matters — so the reclaim obligation has lapsed and the entry is
			// retired with whatever the space still holds left in place.
			// Deleting from it would strip mappings the live consumer resolves
			// its entries through.
			//
			// The consumer check is resolved once, ahead of the prefix arms
			// below, because it opens a read transaction and scans the table's
			// index list.
			// Matched through the same predicate that keeps these kinds off the
			// out-of-transaction destroy, so the two cannot disagree about which
			// kinds hold a revocable claim.
			kind if kind.is_doc_id() && self.tb_has_doc_id_consumer(rc).await? => {
				// A cursor means this entry had already deleted part of the
				// space, so what the consumer adopted is torn: a `!dj` mapping
				// whose `!dd` twin is gone resolves to no record. Nothing here
				// can repair it — the keys are already deleted, and the reaper
				// must not delete further from a space a live index is reading —
				// so the tear is reported for an operator to rebuild the table's
				// doc-ID indexes.
				if state.cursor.is_some() {
					warn!(
						target: TARGET,
						"A doc-ID index was defined over a partly-reclaimed doc-ID space \
						 ({}:{}:{}): the space is torn and the table's doc-ID indexes \
						 need rebuilding",
						rc.ns,
						rc.db,
						rc.tb.as_ref(),
					);
				}
				return Ok(ReclaimOutcome::Complete);
			}
			ReclaimKind::DocKey => {
				DocKeyPrefix::new(rc.ns, rc.db, Cow::Borrowed(rc.tb.as_ref())).raw_range()?
			}
			ReclaimKind::DocLookup => {
				DocLookupPrefix::new(rc.ns, rc.db, Cow::Borrowed(rc.tb.as_ref())).raw_range()?
			}
			ReclaimKind::DocLookupIdentity => {
				DocLookupIdentityPrefix::new(rc.ns, rc.db, Cow::Borrowed(rc.tb.as_ref()))
					.raw_range()?
			}
			ReclaimKind::DocPending => {
				DocPendingPrefix::new(rc.ns, rc.db, Cow::Borrowed(rc.tb.as_ref())).raw_range()?
			}
		};
		self.reclaim_range(rc, state, prefix, lh, canceller, budget).await
	}

	/// Destroy every key beneath `prefix`, resuming from the queue entry's
	/// committed cursor and stopping once `budget` keys have gone.
	///
	/// `budget` is this entry's allowance out of the pass's, and is decremented by
	/// what the entry actually deletes. Returning [`ReclaimOutcome::Incomplete`]
	/// with it spent leaves a durable cursor for the next turn this entry gets.
	///
	/// A backend offering [`DestroyRange`] empties the whole prefix in one
	/// out-of-transaction call, and the pass is complete. Every other backend
	/// pages the delete: each round opens one write transaction, scans a bounded
	/// page of keys, deletes exactly those keys, writes the resume cursor, and
	/// commits. Memory is therefore O(page) whatever the prefix's cardinality.
	///
	/// The cursor and the page of deletions it accounts for commit in the same
	/// transaction, so every key at or before the cursor is durably gone and
	/// progress is monotonic: a restart, a lost lease or an exhausted budget
	/// resumes at the cursor instead of rescanning the prefix. Resuming is also
	/// what keeps each scan off the delete tombstones the retired pages left
	/// behind. [`ReclaimState::observed_ms`] is carried through every cursor
	/// write unchanged — the grace gate measures from the first observation, and
	/// restamping here would let a prefix too large for one pass reset its own
	/// grace and never age past it.
	///
	/// Giving up single-transaction atomicity is sound because the catalog entry
	/// naming this prefix is already committed-removed: a partly-destroyed
	/// prefix is unreachable garbage, not partial state any reader can observe.
	///
	/// Idempotent: a re-run on an already-empty prefix scans one empty page and
	/// reports [`ReclaimOutcome::Complete`].
	async fn reclaim_range(
		&self,
		rc: &ReclaimKey<'_>,
		state: &ReclaimState,
		prefix: RawRange,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		budget: &mut u64,
	) -> Result<ReclaimOutcome> {
		// A backend that can drop the range outside a transaction destroys
		// every version in it in one call, so it needs neither pages nor a
		// cursor and ignores `expunge` — the catalog entry is already gone, so
		// there is nothing to retain. A failure here is propagated rather than
		// retried through the paged path: the entry stays queued and the next
		// pass tries again, so a transient storage failure is never mistaken
		// for an absent capability.
		//
		// Not taken for a table's shared doc-ID prefixes, whose claim is
		// revocable: `DEFINE INDEX` cancels the reclaim to keep the mappings the
		// space still holds. An out-of-transaction destroy neither joins that
		// transaction's conflict domain nor re-reads the claim, so it would go on
		// to delete mappings the new index has already adopted, leaving entries
		// pointing at doc-IDs that resolve to no record. The paged path re-reads
		// the claim inside every page's transaction, which is what lets the
		// cancellation stop it; its cursor is also what tells the next
		// `DEFINE INDEX` that the space was left torn.
		if !rc.kind.is_doc_id()
			&& let Some(ops) = self.destroy_range_ops()
		{
			trace!(
				target: TARGET,
				mechanism = ops.mechanism(),
				"Destroying a reclaimed prefix out-of-transaction",
			);
			ops.destroy_range(prefix.into_key_range()).await?;
			return Ok(ReclaimOutcome::Complete);
		}
		// Resume past the last key whose deletion is durable, but only where the
		// cursor names a key this prefix covers. A cursor at or past the prefix's
		// end would leave an empty window, which the delete cannot tell from a
		// drained prefix: it would report the entry complete, retire the only
		// remaining name for the data, and strand whatever the prefix still holds.
		// Restarting from the head instead re-scans pages that may already be
		// empty, which a delete tolerates.
		let resumable = state
			.cursor
			.as_deref()
			.is_some_and(|c| c >= prefix.start().as_slice() && c < prefix.end().as_slice());
		let window = match &state.cursor {
			Some(cursor) if resumable => prefix.resume_after(cursor, Direction::Forward),
			Some(_) => {
				warn!(
					target: TARGET,
					"Restarting a reclaim from the head of its prefix: the resume cursor \
					 names a key outside it",
				);
				prefix
			}
			None => prefix,
		};
		let outcome = PagedDelete {
			window,
			companion: PageCompanion::ReclaimEntry {
				rc,
				state: state.clone(),
			},
			expunge: rc.expunge == Expunge::Expunge,
			page: RECLAIM_BATCH_SIZE,
		}
		.run(self, lh, canceller, budget)
		.await?;
		Ok(match outcome {
			PagedOutcome::Complete => ReclaimOutcome::Complete,
			PagedOutcome::Incomplete => ReclaimOutcome::Incomplete,
			PagedOutcome::Cancelled => ReclaimOutcome::Cancelled,
			PagedOutcome::LeaseLost => ReclaimOutcome::LeaseLost,
		})
	}

	/// Logs how a pass ended once its batch had already failed. A pass ended
	/// by the batch's own cancellation is expected and not logged.
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	fn log_index_compaction_pass_after_failure(
		ikb: &IndexKeyBase,
		report: CompactionPassReport,
		canceller: &CancellationToken,
	) {
		match report.result {
			Ok(_) => {}
			Err(e)
				if canceller.is_cancelled()
					&& matches!(
						crate::err::engine_error(&e),
						Some(EngineError::QueryCancelled)
					) => {}
			Err(e) => {
				warn!("Index compaction {ikb} fails after its batch failed: {e}");
			}
		}
	}

	/// Compacts each distinct index referenced by the given queue keys.
	///
	/// Folds queued graph adjacency scopes into packed blocks.
	///
	/// Modeled on [`Self::index_compaction`]: leased so one node drains the
	/// queue at a time, a rotating window so a hot table cannot starve
	/// later-sorting ones, bounded read batches, and queue cleanup in
	/// separate bounded write transactions. Before the leased drain, this
	/// node's read-path observations are flushed into the durable queue —
	/// they are held in memory because a read cannot write a queue entry.
	///
	/// The pass runs regardless of `graph_fold_threshold`: a zero threshold
	/// disables new read-path observations (fold initiation), never debt
	/// repayment — edge deletes on already-folded tables keep writing
	/// tombstones and queueing durable triggers whatever the threshold, and
	/// leaving those queued would grow the queue and the per-read tombstone
	/// subtraction without bound.
	pub async fn graph_fold(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<(usize, usize)> {
		trace!(target: TARGET, "Attempting graph fold process");
		Self::flush_graph_fold_observations(&dbs).await?;
		let lh = LeaseHandler::new_with_canceller(
			dbs.sequences.clone(),
			dbs.id,
			dbs.transaction_factory.clone(),
			TaskLeaseType::GraphFold,
			interval * 2,
			canceller.clone(),
		)?;
		let batch = dbs.config.idx.graph_fold_batch;
		let mut count_iteration = 0;
		let mut count_error = 0;
		let queue = GraphFoldPrefix {}.range()?;
		let mut window = queue.clone();
		// Whether the sweep since the last window wrap deleted a queue entry
		// or folded a scope. A wrap-around re-scan retries entries retained
		// by failed folds; without progress between two wraps every remaining
		// entry has just failed again, so the call yields to the next
		// interval instead of hot-looping on a poison scope.
		let mut progressed = false;
		'fold: loop {
			Self::ensure_not_cancelled(&canceller)?;
			// Without the lease, another node is draining the queue.
			if !lh.has_lease().await? {
				break 'fold;
			}
			Self::ensure_not_cancelled(&canceller)?;
			trace!(target: TARGET, "Running graph fold process");
			// One bounded, keys-only batch of queue entries per iteration,
			// in a short-lived read transaction.
			let keys = {
				let txn = dbs.transaction(Read).await?;
				let res =
					txn.keys(window.clone(), INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None).await;
				let _ = txn.cancel().await;
				res?
			};
			Self::ensure_not_cancelled(&canceller)?;
			if keys.is_empty() {
				if window.start() == queue.start() || !progressed {
					// Nothing left anywhere in the queue, or everything left
					// belongs to scopes that failed this sweep.
					break 'fold;
				}
				// End of the range: wrap to re-scan entries skipped when the
				// window seeked past a partially-drained scope, or retained
				// when a scope's fold failed.
				progressed = false;
				window = queue.clone();
				continue;
			}
			count_iteration += 1;
			// The distinct scopes this batch names, in queue order, and each
			// entry's scope for the cleanup below. Entries that fail to
			// decode name nothing foldable; the cleanup removes them.
			let mut scopes: Vec<GraphFoldScope> = Vec::new();
			let mut seen: HashSet<GraphFoldScope> = HashSet::new();
			let mut key_scopes: Vec<Option<GraphFoldScope>> = Vec::with_capacity(keys.len());
			for key in &keys {
				match GraphFoldKey::decode_key(key) {
					Ok(key) => {
						let scope = GraphFoldScope {
							ns: key.ns,
							db: key.db,
							tb: key.tb.into_owned(),
							id: key.id.into_owned(),
							dir: key.dir,
						};
						if seen.insert(scope.clone()) {
							scopes.push(scope.clone());
						}
						key_scopes.push(Some(scope));
					}
					Err(e) => {
						warn!(target: TARGET, "Skipping undecodable graph fold queue entry: {e}");
						key_scopes.push(None);
					}
				}
			}
			// The scopes whose drain completed; only their queue entries are
			// deleted below. A failed scope keeps its entries, so its fold
			// debt survives for the wrap-around re-scan and later calls.
			let mut folded: HashSet<GraphFoldScope> = HashSet::with_capacity(scopes.len());
			for scope in scopes {
				match Self::fold_queued_scope(&dbs, &scope, batch, &canceller).await {
					Ok(()) => {
						folded.insert(scope);
					}
					Err(e) => {
						if canceller.is_cancelled() {
							return Err(e);
						}
						count_error += 1;
						warn!(target: TARGET, "Graph fold failed for a queued scope: {e}");
					}
				}
			}
			// Seek the next batch past the last scope's run of queue entries,
			// so a scope that keeps re-enqueueing cannot pin the window.
			if let Some(last_key) = keys.last() {
				window = match GraphFoldKey::decode_key(last_key) {
					Ok(last) => {
						let covered = GraphFoldDirPrefix {
							ns: last.ns,
							db: last.db,
							tb: Cow::Owned(last.tb.into_owned()),
							id: Cow::Owned(last.id.into_owned()),
							dir: last.dir,
						}
						.skip_extensions()?;
						queue.clone().resume_after(&covered, Direction::Forward)
					}
					Err(_) => queue.clone().resume_after(last_key, Direction::Forward),
				};
			}
			// Delete this batch's consumed queue entries in a separate bounded
			// write transaction: entries whose scope folded, and undecodable
			// entries — which can never be folded — deleted as bytes so they
			// do not park at the head of the queue forever.
			let deletable: Vec<&Vec<u8>> = keys
				.iter()
				.zip(&key_scopes)
				.filter(|(_, scope)| scope.as_ref().is_none_or(|s| folded.contains(s)))
				.map(|(k, _)| k)
				.collect();
			if deletable.is_empty() {
				continue;
			}
			progressed = true;
			loop {
				let txn = dbs.transaction(Write).await?;
				if let Err(e) = Self::ensure_not_cancelled(&canceller) {
					let _ = txn.cancel().await;
					return Err(e);
				}
				for k in &deletable {
					if let Err(e) = txn.del(Key::from(k.as_slice())).await {
						warn!(target: TARGET, "Failed to delete graph fold queue entry: {e}");
					}
				}
				if let Err(e) = txn.commit().await {
					if Self::cancel_and_retry_index_operation_conflict(
						&txn,
						&e,
						"Retryable conflict committing graph fold queue cleanup, retrying",
					)
					.await
					{
						continue;
					}
					warn!(target: TARGET, "Failed to commit graph fold queue cleanup: {e}");
					break 'fold;
				}
				break;
			}
		}
		Ok((count_iteration, count_error))
	}

	/// Writes this node's read-path fold observations into the durable
	/// queue: blind puts of per-observation keys, so nothing contends.
	async fn flush_graph_fold_observations(dbs: &Arc<Datastore>) -> Result<()> {
		let observations = dbs.triggers.drain_graph_fold_observations();
		if observations.is_empty() {
			return Ok(());
		}
		let txn = dbs.transaction(Write).await?;
		for scope in &observations {
			let key = GraphFoldKey {
				ns: scope.ns,
				db: scope.db,
				tb: Cow::Borrowed(&scope.tb),
				id: Cow::Borrowed(&scope.id),
				dir: scope.dir,
				nid: dbs.id,
				uid: Uuid::now_v7(),
			};
			if let Err(e) = txn.put_key(&key, &()).await {
				let _ = txn.cancel().await;
				return Err(e);
			}
		}
		txn.commit().await
	}

	/// Drains one queued scope: bounded fold passes, each in its own
	/// transaction, retrying a bounded number of times when a concurrent
	/// edge write conflicts with a pass.
	async fn fold_queued_scope(
		dbs: &Arc<Datastore>,
		scope: &GraphFoldScope,
		batch: usize,
		canceller: &CancellationToken,
	) -> Result<()> {
		// The catalog write that marks the vertex table folded addresses the
		// namespace and database by name. A namespace or database dropped
		// since the entry was queued leaves nothing to fold.
		//
		// A namespace or database this build cannot read, because a newer
		// release wrote its definition, leaves nothing it can fold either. The
		// scope is left, with a warning, and a later observation queues it again.
		let (ns_name, db_name) = {
			let txn = dbs.transaction(Read).await?;
			let found = async {
				let nss = txn.all_readable(NamespacePrefix {}.range()?).await?;
				let ns = nss
					.values
					.iter()
					.find(|n| n.namespace_id == scope.ns)
					.map(|n| n.name.to_string());
				let dbs = txn
					.all_readable(
						DatabasePrefix {
							ns: scope.ns,
						}
						.range()?,
					)
					.await?;
				let db = dbs
					.values
					.iter()
					.find(|d| d.database_id == scope.db)
					.map(|d| d.name.to_string());
				Ok::<_, anyhow::Error>((ns, db, nss.refused + dbs.refused))
			}
			.await;
			let _ = txn.cancel().await;
			match found? {
				(Some(ns), Some(db), _) => (ns, db),
				(_, _, refused) => {
					if refused > 0 {
						warn!(
							target: TARGET,
							refused,
							"A graph fold scope was left because a catalog entry on its path needs \
							 a newer release than this build"
						);
					}
					return Ok(());
				}
			}
		};
		let vertex = crate::val::RecordId::new(scope.tb.clone(), scope.id.clone());
		let mut retries = 0;
		loop {
			Self::ensure_not_cancelled(canceller)?;
			let txn = Arc::new(dbs.transaction(Write).await?);
			// The environment the fold's doc-ID assignment reads: the
			// production `IndexEnv` is the query context, built the same way
			// the index-compaction processors build theirs.
			let mut env = dbs.setup_ctx()?;
			env.set_transaction(Arc::clone(&txn));
			let env = env.freeze();
			let outcome = match crate::idx::adjacency::fold_scope(
				&env, scope.ns, scope.db, &ns_name, &db_name, &vertex, scope.dir, batch,
			)
			.await
			{
				Ok(outcome) => outcome,
				Err(e) => {
					let _ = txn.cancel().await;
					return Err(e);
				}
			};
			match txn.commit().await {
				Ok(()) => {
					retries = 0;
					if !outcome.has_more {
						return Ok(());
					}
				}
				Err(e) => {
					// A concurrent RELATE or DELETE on the scope beat this
					// pass; take a fresh snapshot and try again, bounded so a
					// write-hot vertex cannot spin the task.
					if surrealdb_datastore::is_retryable_transaction_conflict(&e) && retries < 5 {
						retries += 1;
						continue;
					}
					return Err(e);
				}
			}
		}
	}

	/// Off the browser, the batch's distinct indexes, in queue order, are
	/// passed under one compaction ticket registered with the node's place
	/// pool: this task runs passes itself, and helpers run more only on places
	/// the ticket is granted ([`Self::run_compaction_passes`]). Failures are
	/// logged but do not abort the loop.
	///
	/// The caller has just confirmed the lease, so the batch starts under a
	/// lease it holds. It is renewed on a tick for as long as the run lasts —
	/// the passes this task runs, the helpers it awaits, and the helpers it
	/// still awaits once the batch has failed — since a pass can run past the
	/// lease's duration; a pass that finds the lease lost stops at its next
	/// plan boundary.
	///
	/// The first failure ends the batch: a cancellation, a pass that panicked
	/// or whose task was aborted, or a pass ended by cancellation. It is
	/// returned only once every pass the batch started has ended, so none runs
	/// on past the batch's cleanup or under a lease another node has taken.
	/// What a pass reports after the failure is only logged.
	///
	/// Returns what the passes leave the batch's cleanup to act on.
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	async fn index_compaction_loop(
		dbs: Arc<Datastore>,
		lease: &CompactionLease,
		indexes: &[IndexKeyBase],
		canceller: CancellationToken,
	) -> Result<CompactionBatchReport> {
		Self::ensure_not_cancelled(&canceller)?;
		let places = dbs.compaction_places();
		let pass = {
			let (dbs, canceller, lease, places) =
				(Arc::clone(&dbs), canceller.clone(), lease.clone(), places.clone());
			move |ikb: IndexKeyBase| {
				let (dbs, canceller, lease, places) =
					(Arc::clone(&dbs), canceller.clone(), lease.clone(), places.clone());
				async move {
					dbs.run_index_compaction_pass(&ikb, &canceller, Some(&lease), &places).await
				}
			}
		};
		let run = {
			let mut run = pin!(Self::run_compaction_passes(
				&places,
				indexes.iter().cloned(),
				&canceller,
				pass
			));
			// One ticker across the whole run, so the renewals keep their
			// cadence however the passes' completions fall between its ticks.
			// The lease was just confirmed, so the first renewal is due a full
			// tick from now rather than at once.
			let mut renewal = interval(INDEX_COMPACTION_LEASE_TICK);
			renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
			renewal.reset();
			loop {
				tokio::select! {
					biased;
					run = &mut run => break run,
					_ = renewal.tick() => {
						Self::maintain_index_compaction_lease(lease, &canceller).await;
					}
				}
			}
		};
		let mut failure = run.failure;
		let mut batch = CompactionBatchReport::default();
		for (ikb, report) in run.results {
			if failure.is_some() {
				Self::log_index_compaction_pass_after_failure(&ikb, report, &canceller);
				continue;
			}
			if let Err(e) = batch.record(ikb, report, &canceller) {
				failure = Some(e);
			}
		}
		match failure {
			Some(e) => Err(e),
			None => Ok(batch),
		}
	}

	/// Runs `pass` over every index of `indexes`, under the compaction ticket
	/// `places`, and returns what each pass reported.
	///
	/// This task claims indexes from a shared queue and runs their passes
	/// itself, holding no place. Each time it claims one, it spawns a helper
	/// for every place the ticket is granted while unclaimed indexes remain,
	/// each helper claiming its first index as it is spawned, so the queue holds
	/// only work nobody has claimed. A helper holds its place across the passes
	/// it runs, claims more from the same queue, and between two passes gives
	/// its place back when another registration waits. So a batch of one index
	/// takes no place, an index never waits behind a running pass while a
	/// place is free to start it, and the passes running at once are
	/// never more than the places the ticket holds plus this task. The pool's
	/// OLTP reserve, the ticket's cap and the pool's fairness bound them, and
	/// the work a pass runs in parallel takes its places through the same
	/// ticket. A refused ask's waiting mark is cleared at once, since this task
	/// asks again only once its own pass has ended. Once the registration has
	/// given a place way, by a helper or by the parallel work of any pass on
	/// the ticket, the ticket asks for none while another registration waits
	/// ([`crate::idx::places::Ticket::acquire`]), so the run never takes back
	/// what was given way, though it may have registered first and win the
	/// pool's tie.
	///
	/// The run fails on a cancellation, which stops every task before its next
	/// pass; on a pass this task ran that panicked; and on a helper that
	/// panicked or was aborted. A failed run claims nothing more, but awaits
	/// every helper it spawned before it returns, so no pass runs on past it,
	/// and an index claimed but not passed is never reported. A pass's report
	/// is returned as the pass made it, so telling a pass ended by
	/// cancellation apart is the caller's.
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	async fn run_compaction_passes<F, Fut, R>(
		places: &crate::idx::places::Ticket,
		indexes: impl IntoIterator<Item = IndexKeyBase>,
		canceller: &CancellationToken,
		pass: F,
	) -> CompactionRun<R>
	where
		F: Fn(IndexKeyBase) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = R> + Send + 'static,
		R: Send + 'static,
	{
		use futures::FutureExt as _;

		let queue: CompactionQueue =
			Arc::new(parking_lot::Mutex::new(indexes.into_iter().collect()));
		let pass = Arc::new(pass);
		// The loop is one user of the ticket; a refused ask stops it waiting at
		// once, since it asks again only once its own pass has ended.
		let asking = places.asking();
		let mut helpers: Vec<tokio::task::JoinHandle<CompactionRun<R>>> = Vec::new();
		let mut run = CompactionRun::default();
		loop {
			if let Err(e) = Self::ensure_not_cancelled(canceller) {
				run.failure = Some(e);
				break;
			}
			// Helpers that have ended hand back what they ran, and free their
			// slots for the places granted below.
			let mut at = 0;
			while at < helpers.len() && run.failure.is_none() {
				if !helpers[at].is_finished() {
					at += 1;
					continue;
				}
				let joined = helpers.swap_remove(at).await;
				run.absorb(joined);
			}
			if run.failure.is_some() {
				break;
			}
			let Some(ikb) = queue.lock().pop_front() else {
				break;
			};
			while !queue.lock().is_empty() {
				let Some(place) = asking.acquire() else {
					asking.stop_waiting();
					break;
				};
				// Taken by a helper while this task asked: the place goes back.
				let Some(first) = queue.lock().pop_front() else {
					break;
				};
				helpers.push(spawn(Self::compaction_helper(
					place,
					first,
					places.clone(),
					Arc::clone(&queue),
					Arc::clone(&pass),
					canceller.clone(),
				)));
			}
			// A panic in a pass this task runs is caught, so the run still
			// awaits its helpers, as it does for a panic on a helper's task.
			match std::panic::AssertUnwindSafe(pass(ikb.clone())).catch_unwind().await {
				Ok(report) => run.results.push((ikb, report)),
				Err(panic) => {
					run.failure = Some(anyhow::anyhow!(
						"Index compaction {ikb} panicked: {}",
						compaction_panic_message(panic.as_ref())
					));
					break;
				}
			}
		}
		for helper in helpers {
			let joined = helper.await;
			run.absorb(joined);
		}
		run
	}

	/// A helper of [`Self::run_compaction_passes`]: runs passes in `place`,
	/// over `first`, claimed for it as it was spawned, and then over each index
	/// it claims from `queue`, until none is left, the loop is cancelled, or
	/// another registration waits for a place once a pass has ended, when it
	/// gives its place way through the ticket. Returns what its passes
	/// reported, and the cancellation that stopped it before a pass it had
	/// claimed.
	#[cfg(not(all(target_family = "wasm", target_os = "unknown")))]
	async fn compaction_helper<F, Fut, R>(
		place: crate::idx::places::Place,
		first: IndexKeyBase,
		places: crate::idx::places::Ticket,
		queue: CompactionQueue,
		pass: Arc<F>,
		canceller: CancellationToken,
	) -> CompactionRun<R>
	where
		F: Fn(IndexKeyBase) -> Fut,
		Fut: Future<Output = R>,
	{
		let mut run = CompactionRun::default();
		let mut next = Some(first);
		loop {
			let Some(ikb) = next.take() else {
				// Nothing is left to claim, so the run will not ask again.
				places.stop_waiting();
				return run;
			};
			if let Err(e) = Self::ensure_not_cancelled(&canceller) {
				run.failure = Some(e);
				return run;
			}
			let report = pass(ikb.clone()).await;
			run.results.push((ikb, report));
			if places.others_wait() {
				places.give_way(place);
				return run;
			}
			next = queue.lock().pop_front();
		}
	}

	/// A compaction ticket: a new registration with the node's place pool,
	/// under this datastore's morsel policy, which the index compaction loop
	/// holds for its run.
	fn compaction_places(&self) -> crate::idx::places::Ticket {
		#[cfg(all(test, not(target_family = "wasm")))]
		if let Some(places) = test::compaction_places_for(self.id) {
			return places;
		}
		crate::ctx::IndexPlaces::node(crate::exec::planner::placement::Policy::from_config(
			&self.config.exec,
		))
	}

	/// Runs one pass per index in `indexes`, the batch's distinct indexes in
	/// queue order.
	///
	/// The browser executor has no join handle, so there the passes run
	/// sequentially, under a compaction ticket that is never granted a place.
	/// Failures are logged but do not abort the loop, matching
	/// the other targets so that a single transient failure does not prevent
	/// other indexes from being compacted.
	///
	/// The caller has just confirmed the lease, and it is renewed on a tick
	/// while each pass runs, as on the other targets.
	///
	/// Returns what the passes leave the batch's cleanup to act on.
	#[cfg(all(target_family = "wasm", target_os = "unknown"))]
	async fn index_compaction_loop(
		dbs: Arc<Datastore>,
		lease: &CompactionLease,
		indexes: &[IndexKeyBase],
		canceller: CancellationToken,
	) -> Result<CompactionBatchReport> {
		Self::ensure_not_cancelled(&canceller)?;
		let places = dbs.compaction_places();
		let mut batch = CompactionBatchReport::default();
		// The lease was just confirmed, so the first renewal is due a full tick
		// from now rather than at once.
		let mut renewal = interval(INDEX_COMPACTION_LEASE_TICK);
		renewal.set_missed_tick_behavior(MissedTickBehavior::Delay);
		renewal.reset();
		for ikb in indexes {
			Self::ensure_not_cancelled(&canceller)?;
			let report = {
				let mut pass =
					pin!(dbs.run_index_compaction_pass(ikb, &canceller, Some(lease), &places));
				loop {
					tokio::select! {
						biased;
						// As on the other targets, a cancelled batch still awaits
						// its pass rather than dropping it part-way through a
						// transaction: the pass observes the same canceller at its
						// next boundary. The batch then ends on the cancellation,
						// whatever the pass reported.
						_ = canceller.cancelled() => {
							if let Err(e) = (&mut pass).await.result
								&& !matches!(
									crate::err::engine_error(&e),
									Some(EngineError::QueryCancelled)
								)
							{
								warn!("Index compaction {ikb} fails while awaiting cancellation: {e}");
							}
							bail!(EngineError::QueryCancelled);
						}
						report = &mut pass => break report,
						_ = renewal.tick() => {
							Self::maintain_index_compaction_lease(lease, &canceller).await;
						}
					}
				}
			};
			batch.record(ikb.clone(), report, &canceller)?;
		}
		Ok(batch)
	}

	/// Renews the index-compaction lease while a batch's passes run.
	///
	/// A renewal that fails is logged rather than returned: the passes it would
	/// abandon are still running, and the next tick tries again.
	async fn maintain_index_compaction_lease(
		lease: &CompactionLease,
		canceller: &CancellationToken,
	) {
		if let Err(e) = lease.maintain().await
			&& !canceller.is_cancelled()
		{
			warn!(target: TARGET, "Failed to maintain the index compaction lease: {e}");
		}
	}

	/// Runs one pass over a single index outside any lease, under a
	/// compaction ticket of its own, as [`Self::run_index_compaction_pass`]
	/// does.
	#[cfg(test)]
	async fn process_index_compaction(
		&self,
		ikb: &IndexKeyBase,
		canceller: CancellationToken,
	) -> Result<CompactionPassOutcome> {
		self.run_index_compaction_pass(ikb, &canceller, None, &self.compaction_places())
			.await
			.result
	}

	/// Runs one compaction pass over a single index, under its materialise
	/// lock, and reports how it ended ([`Materialiser::run_pass_held`]).
	async fn run_index_compaction_pass(
		&self,
		ikb: &IndexKeyBase,
		canceller: &CancellationToken,
		lease: Option<&CompactionLease>,
		places: &crate::idx::places::Ticket,
	) -> CompactionPassReport {
		#[cfg(test)]
		compaction_pass_park::wait(self.id, ikb).await;
		let report = match self.materialiser() {
			Ok(materialiser) => {
				materialiser
					.run_pass(ikb, canceller, lease.map(|lease| lease as &dyn PassLease), places)
					.await
			}
			Err(e) => CompactionPassReport {
				result: Err(e),
				unqueued: false,
			},
		};
		#[cfg(test)]
		compaction_pass_park::end(self.id, ikb);
		report
	}

	/// The materialiser background compaction runs its passes through.
	fn materialiser(&self) -> Result<Materialiser> {
		Materialiser::new(&self.setup_ctx()?.freeze())
	}

	/// Runs [`Self::sweep_unmaterialised_indexes`] for `drain`, a drain of the
	/// compaction queue whose lease period is twice `interval`, and answers
	/// whether it queued a request for the drain to act on.
	///
	/// A drain sweeps at most once, and its sweep skips every index the drain
	/// ran a pass over ([`CompactionDrain::passed`]). A sweep that fails is
	/// logged, and queued nothing.
	async fn sweep_after_drain(
		dbs: &Datastore,
		interval: Duration,
		canceller: &CancellationToken,
		drain: &mut CompactionDrain,
	) -> bool {
		if std::mem::replace(&mut drain.swept, true) {
			return false;
		}
		match dbs.sweep_unmaterialised_indexes(interval * 2, canceller, &drain.passed).await {
			Ok(queued) => queued > 0,
			Err(e) => {
				warn!(target: TARGET, "Failed to sweep indexes for unmaterialised updates: {e}");
				false
			}
		}
	}

	/// Queues one compaction request for every defined index of a queued kind
	/// that holds updates waiting to be folded and no request in the queue, and
	/// answers how many it queued.
	///
	/// A request is how the compactor learns an index has work, and an index
	/// can hold work no request names. A full-text compactor that predates the
	/// residual family folds a term's deltas into its bitmap, leaves the
	/// residual beside it, and deletes the request that led to its pass; on an
	/// index nothing writes to, nothing queues another. Whichever way such work
	/// was stranded — before an upgrade, after a lease handover, or by a pass
	/// that outlived its lease — the next sweep on a node holding the lease
	/// finds it, through the same probe `INFO FOR INDEX` reports as
	/// `compacting` ([`index_has_unmaterialised_updates`]).
	///
	/// Throttled: a sweep runs at most once per `period`, measured from the
	/// last sweep that completed, so an idle store pays one existence probe per
	/// queued-kind index per period. A sweep that fails is not counted, and the
	/// next empty queue sweeps again.
	///
	/// # Arguments
	/// * `period` - the least time between two sweeps: the compaction lease's.
	/// * `canceller` - ends the sweep between indexes.
	/// * `passed` - indexes the sweep leaves alone: those the calling drain ran a pass over.
	///
	/// # Returns
	/// The number of requests queued, zero when the sweep was throttled.
	async fn sweep_unmaterialised_indexes(
		&self,
		period: Duration,
		canceller: &CancellationToken,
		passed: &HashSet<IndexKeyBase>,
	) -> Result<usize> {
		if self.compaction_swept_at.lock().is_some_and(|at| at.elapsed() < period) {
			return Ok(0);
		}
		let (defined, _) = self.defined_indexes().await?;
		let mut owed = Vec::new();
		{
			let txn = self.transaction(Read).await?;
			let res: Result<()> = async {
				for DefinedIndex {
					ns,
					db,
					ix,
					..
				} in defined.iter().filter(|d| d.ix.index.queues_compaction())
				{
					Self::ensure_not_cancelled(canceller)?;
					let ikb = IndexKeyBase::new(*ns, *db, ix.table_name.clone(), ix.index_id);
					if passed.contains(&ikb) {
						continue;
					}
					// An index with a request queued — one a write queued, a
					// pass requeued or a drain kept — is the queue's already.
					let requested = IndexCompactionIxPrefix {
						ns: *ns,
						db: *db,
						tb: Cow::Borrowed(&ix.table_name),
						ix: ix.index_id,
					}
					.range()?;
					if !txn.keys(requested, 1, 0, None).await?.is_empty() {
						continue;
					}
					if index_has_unmaterialised_updates(&txn, *ns, *db, ix).await? {
						owed.push(ikb);
					}
				}
				Ok(())
			}
			.await;
			let _ = txn.cancel().await;
			res?;
		}
		let materialiser = self.materialiser()?;
		for ikb in &owed {
			Self::ensure_not_cancelled(canceller)?;
			materialiser.requeue_index_compaction(ikb).await?;
		}
		*self.compaction_swept_at.lock() = Some(Instant::now());
		Ok(owed.len())
	}

	/// Forgets when the defined indexes were last swept, so the next empty
	/// queue sweeps them whatever the throttle.
	#[cfg(test)]
	pub(crate) fn expire_compaction_sweep(&self) {
		*self.compaction_swept_at.lock() = None;
	}

	/// Process queued async events using a distributed lease to coordinate batches.
	/// Once a batch starts it runs to completion even if the lease expires, so
	/// brief overlap is possible.
	///
	/// Returns as soon as `canceller` is tripped, at the next batch boundary. The
	/// events themselves are user-defined SurrealQL that nothing here bounds, so
	/// this bounds the wait to one batch rather than to the whole queue — which
	/// is what stops a busy queue from holding [`Self::shutdown`] open for as
	/// long as writes keep arriving.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, canceller)
		)
	)]
	pub(crate) async fn event_processing(
		&self,
		interval: Duration,
		canceller: &CancellationToken,
	) -> Result<()> {
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
		// We continue while there are keys, the lease, and no cancellation
		loop {
			// Stop between batches once shutdown starts. Cancellation is not a
			// failure: the queue is durable, so whatever is left is processed by
			// the next pass on this node or another.
			if canceller.is_cancelled() {
				return Ok(());
			}
			// Attempt to acquire a lease for the EventProcessing task
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok(());
			}
			// Output function invocation details to logs
			trace!(target: TARGET, "Running event processing process");
			if process_next_events_batch(self, Some(&lh)).await? == 0 {
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
	/// use surrealdb_core::kvs::{Datastore, TransactionType::*};
	/// use anyhow::Error;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(),Error> {
	///     let ds = Datastore::new("rocksdb://database.db").await?;
	///     let mut tx = ds.transaction(Write).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(&self, write: TransactionType) -> Result<Transaction> {
		self.transaction_factory.transaction(write, self.sequences.clone()).await
	}

	/// A write transaction that a read-only store still opens.
	///
	/// For this node's own membership rows and for deleting a torn-down session's durable
	/// copy, which must be written whatever the ledger says. Every other writer opens its
	/// transaction through [`Self::transaction`], which a read-only store refuses.
	pub(crate) async fn bookkeeping_transaction(&self) -> Result<Transaction> {
		self.transaction_factory.bookkeeping(self.sequences.clone()).await
	}

	/// Whether the store records features this build does not maintain, so that every write
	/// transaction other than bookkeeping is refused with
	/// [`DatastoreError::StoreReadOnly`].
	pub fn is_read_only(&self) -> bool {
		self.transaction_factory.is_read_only()
	}

	/// Opens the store read-only, logging it once. `features` names the ledger entries that this
	/// build does not maintain; it is reported by every refusal and holds ids and recorded names
	/// only. The mark is never cleared, and a store that is already read-only keeps its first
	/// `features`.
	pub(crate) fn open_read_only(&self, features: String) {
		if self.is_read_only() {
			return;
		}
		warn!(
			target: TARGET,
			features = %features,
			"The datastore records features this build does not maintain, so it is open read-only"
		);
		self.transaction_factory.set_read_only(features);
	}

	pub(crate) fn sequences(&self) -> &Sequences {
		&self.sequences
	}

	pub(crate) fn transaction_factory(&self) -> &TransactionFactory {
		&self.transaction_factory
	}

	/// Release the DiskANN writer leases this node holds on indexes it has
	/// committed nothing under for [`DISKANN_WRITER_LEASE_IDLE`], so other nodes'
	/// compaction can return those indexes' drained guards to `Empty`. A lease is
	/// only ever held on a store this datastore is not the sole writer of.
	pub(crate) async fn release_idle_diskann_writer_leases(&self) -> Result<()> {
		self.transaction_factory
			.release_idle_diskann_writer_leases(&self.sequences, DISKANN_WRITER_LEASE_IDLE)
			.await
			.map(|_| ())
	}

	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) fn index_builder(&self) -> &IndexBuilder {
		&self.index_builder
	}
	/// The post-commit wake-ups the background tasks wait on.
	pub fn commit_triggers(&self) -> &Arc<CommitTriggers> {
		&self.triggers
	}

	/// The live-query engine this datastore delivers notifications through.
	///
	/// Selected from the datastore configuration at construction and fixed for
	/// the datastore's lifetime. The engine's background tasks read it to decide
	/// whether to run the per-node live-query router at all: under
	/// [`LiveQueryEngine::Inline`] the router has nothing to deliver, so no task
	/// is spawned for it.
	pub fn live_query_engine(&self) -> LiveQueryEngine {
		self.config.datastore.live_query_engine
	}

	pub async fn health_check(&self) -> Result<()> {
		let tx = self.transaction(Read).await?;

		// Cancel the transaction
		trace!("Cancelling health check transaction");
		// Read a declared key rather than a probe byte: whether it is present says
		// nothing here, only that the store answered.
		match tx.get_key(&VersionKey {}, None).await {
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

	/// Returns how long ago the current node last refreshed its cluster
	/// heartbeat, or `None` when this node has no cluster membership row.
	///
	/// The node-membership refresh background task rewrites this node's
	/// heartbeat to the KV store every `node_membership_refresh_interval`. A
	/// small age therefore confirms, from work the node already performs, that
	/// the refresh task is running and that both the storage write path (the
	/// heartbeat was recently committed) and read path (it is readable here in
	/// a fresh transaction) are healthy. Readiness probes use this instead of a
	/// dedicated health check to avoid duplicating that work.
	///
	/// An absent row is `None` rather than an error, because it is an ordinary
	/// state of a live node and not a failure to read: `insert_node` writes the
	/// row during startup, a peer that archives this node deletes it again, and
	/// either way the next refresh tick writes it back. A caller can therefore
	/// tell "no heartbeat yet" from "storage did not answer", which only `Err`
	/// reports. `/ready` depends on the distinction.
	pub async fn node_heartbeat_age(&self) -> Result<Option<Duration>> {
		// Open a fresh read transaction so the lookup misses the per-transaction
		// cache and actually reads the node key from storage.
		let tx = self.transaction(Read).await?;
		let res = tx.get_node(self.id).await;
		// Always release the transaction, regardless of the lookup result.
		let _ = tx.cancel().await;
		let Some(node) = res? else {
			return Ok(None);
		};
		// Heartbeats are stored as milliseconds since the epoch; saturate to
		// avoid underflow from minor clock skew between writes and this read.
		let now = self.clock_now();
		Ok(Some(Duration::from_millis(now.value.saturating_sub(node.heartbeat.value))))
	}

	/// Run a query.
	///
	/// Every buffered query entry point that takes a whole query is this one:
	/// a [`QueryRequest`] describes what to run, who runs it, and the
	/// conditions — a caller-owned transaction, bound variables, a
	/// cancellation handle — independently of one another, so the
	/// combinations do not need a method each. Import is the exception: it
	/// consumes a byte stream and keeps its own preflight, whose session
	/// errors differ from this one's.
	///
	/// The source is split into statements before the session preflight, so a
	/// caller that submitted unparsable text learns that from the call
	/// whatever else is wrong with the request. SurrealQL statements stay in
	/// surface form; see [`Self::statements_for`].
	///
	/// See [`Self::run_streaming`] to receive results as they are produced
	/// rather than all at once.
	///
	/// ```rust,no_run
	/// use anyhow::Error;
	/// use surrealdb_core::kvs::{Datastore, QueryRequest};
	/// use surrealdb_core::dbs::Session;
	///
	/// #[tokio::main]
	/// async fn main() -> Result<(),Error> {
	///     let ds = Datastore::new("memory").await?;
	///     let ses = Session::owner();
	///     let sql = "USE NS test DB test; SELECT * FROM person;";
	///     let res = ds.run(QueryRequest::new(sql, &ses)).await?;
	///     Ok(())
	/// }
	/// ```
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn run(
		&self,
		req: QueryRequest<'_>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		self.run_sealed(req).await
	}

	#[inline(never)]
	fn run_sealed(
		&self,
		req: QueryRequest<'_>,
	) -> OutOfLine<impl Future<Output = std::result::Result<Vec<QueryResult>, TypesError>>> {
		OutOfLine::new(self.run_body(req))
	}

	async fn run_body(
		&self,
		req: QueryRequest<'_>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		let QueryRequest {
			source,
			session,
			variables,
			transaction,
			cancel,
			row_format: _,
			held_transactions: _,
		} = req;
		let statements = self.statements_for(source)?;
		let (opt, ctx) = self.setup_query(session, variables, transaction.as_ref(), cancel)?;
		match transaction {
			Some(_) => {
				Executor::execute_statements_with_transaction(self, ctx, opt, statements).await
			}
			None => Executor::execute_statements(self, ctx, opt, statements).await,
		}
		// `anyhow_to_types_error`, not a downcast to `err::Error`: that enum
		// holds a handful of residual variants, so a bare layer error
		// (`ExecError::DbEmpty` from `Options::ns_db()`, say) misses and
		// reaches the client as `internal` rather than as a validation
		// failure. The helper walks the whole registry.
		.map_err(crate::err::anyhow_to_types_error)
	}

	/// Run a query as the fragments of one stream: each statement is compiled
	/// when it arrives rather than with the whole request.
	///
	/// Sets up the query exactly as [`Self::run`] does, so session checks,
	/// variables, cancellation and parser settings are the same, and the
	/// statements have the same semantics. Intended for the equivalence checks of
	/// the language tests, which compares this against [`Self::run`]; it is
	/// compiled only under the `internal-fragments` feature. A request carrying
	/// a client transaction is refused: fragments run outside session
	/// transactions.
	#[cfg(feature = "internal-fragments")]
	#[doc(hidden)]
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn execute_fragments(
		&self,
		req: QueryRequest<'_>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		let QueryRequest {
			source,
			session,
			variables,
			transaction,
			cancel,
			row_format: _,
			held_transactions: _,
		} = req;
		if transaction.is_some() {
			return Err(TypesError::internal(
				"Fragments run outside session transactions, but the request carries one"
					.to_string(),
			));
		}
		let statements = self.statements_for(source)?;
		let (opt, ctx) = self.setup_query(session, variables, None, cancel)?;
		Executor::execute_fragments(self, ctx, opt, statements)
			.await
			.map_err(crate::err::anyhow_to_types_error)
	}

	/// Prepare a query to stream its results.
	///
	/// Where [`Self::run`] answers with every statement's result at once, this
	/// sends [`QueryStreamItem`]s into `items` as the executor produces them,
	/// so a caller can forward rows before the query finishes. See the
	/// [`stream`](crate::dbs::QueryStreamItem) module for what a consumer may
	/// assume about them — chiefly that rows are provisional until their
	/// statement's terminal item arrives.
	///
	/// Parsing and the session checks happen here, so their failures are
	/// reported instead of a job. Everything after that is reported by driving
	/// [`QueryStreamJob::run`].
	///
	/// A streaming caller wants [`QueryRequest::cancel`]: a consumer that stops
	/// reading is only noticed at the next send otherwise, so a query in a
	/// phase that emits nothing for a while — a sort, an aggregate — would run
	/// to completion after the client had gone.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub fn run_streaming(
		self: &Arc<Self>,
		req: QueryRequest<'_>,
		items: Sender<QueryStreamItem>,
	) -> std::result::Result<QueryStreamJob, TypesError> {
		let QueryRequest {
			source,
			session,
			variables,
			transaction,
			cancel,
			row_format,
			held_transactions,
		} = req;
		let statements = self.statements_for(source)?;
		let (opt, ctx) = self.setup_query(session, variables, transaction.as_ref(), cancel)?;
		let in_transaction = transaction.is_some();
		// The count is known now because parsing is done, which is what lets a
		// transport announce it before the first result exists.
		let statement_count = statements.len();
		let kvs = Arc::clone(self);
		Ok(QueryStreamJob {
			statement_count,
			run: Box::pin(async move {
				let executed = if in_transaction {
					Executor::execute_statements_streaming_with_transaction(
						&kvs, ctx, opt, statements, items, row_format,
					)
					.await
				} else {
					Executor::execute_statements_streaming(
						&kvs,
						ctx,
						opt,
						statements,
						items,
						row_format,
						held_transactions,
					)
					.await
				};
				executed.map_err(crate::err::anyhow_to_types_error)
			}),
		})
	}

	/// Split a [`QuerySource`] into the statements the executor runs.
	///
	/// Parsed SurrealQL stays in surface form: the executor converts each
	/// statement when it is about to run it, rather than converting the batch
	/// before the first one does. GQL has no SurrealQL surface form — it lowers
	/// straight to a plan — so it arrives at the executor as the plan it runs
	/// as, as does a plan a caller submits directly.
	fn statements_for(
		&self,
		source: QuerySource<'_>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		fn planned(plans: Vec<LogicalOp>) -> Vec<TopLevelStatement> {
			plans.into_iter().map(|op| TopLevelStatement::Plan(Box::new(op))).collect()
		}

		let ast = match source {
			QuerySource::Text {
				text,
				dialect: Dialect::SurrealQl,
			} => return self.parse_surrealql(text),
			QuerySource::Text {
				text,
				dialect: Dialect::Gql,
			} => return Ok(planned(self.parse_gql(text)?)),
			QuerySource::Text {
				text,
				dialect: Dialect::PgSql,
			} => return self.parse_pgsql(text),
			QuerySource::Text {
				text,
				dialect: Dialect::MySql,
			} => return self.parse_mysql(text),
			QuerySource::Ast(ast) => ast,
			QuerySource::Plan(plans) => return Ok(planned(plans)),
		};
		Ok(ast.expressions.into_iter().map(TopLevelStatement::Ast).collect())
	}

	/// Parse SurrealQL query text into the statements the executor runs, under the parser settings
	/// the datastore's capabilities and configuration derive. Reaches no other dialect's parser.
	pub(crate) fn parse_surrealql(
		&self,
		text: &str,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		let settings =
			syn::settings_from_capabilities_config(&self.capabilities.load(), &self.parser_config);
		let statements = compile_phase!(
			"parse",
			surrealdb_expr::parser::execution_query_with_settings(text, settings)
		)
		.map_err(|e| TypesError::validation(e.to_string(), None))?;
		Ok(statements
			.into_iter()
			.map(|statement| match statement {
				surrealdb_expr::parser::ExecutionStatement::Surface(stmt) => {
					TopLevelStatement::Ast(stmt)
				}
				surrealdb_expr::parser::ExecutionStatement::Logical(stmt) => {
					TopLevelStatement::Surreal(stmt)
				}
			})
			.collect())
	}

	/// Parse and lower GQL query text to the plans the executor runs.
	///
	/// The plan is returned bare rather than in its `PreparedGqlQuery`
	/// wrapper: that type comes from the `gql` crate, so naming it here would
	/// leave the signature unbuildable on a build without the feature.
	#[cfg(feature = "gql")]
	fn parse_gql(&self, text: &str) -> std::result::Result<Vec<LogicalOp>, TypesError> {
		crate::gql::parse_with_capabilities(text, &self.capabilities.load(), &self.parser_config)
			.map(|prepared| vec![prepared.0])
			.map_err(|e| TypesError::validation(e.to_string(), None))
	}

	/// The dialect exists on every build because it is chosen from what a
	/// client sent; the front end that would parse it does not.
	#[cfg(not(feature = "gql"))]
	fn parse_gql(&self, _text: &str) -> std::result::Result<Vec<LogicalOp>, TypesError> {
		Err(TypesError::configuration(
			"This build does not support GQL queries".to_string(),
			surrealdb_types::ConfigurationError::DialectNotSupported,
		))
	}

	/// Parse and lower PostgreSQL SQL query text to statements and plans.
	#[cfg(feature = "postgres")]
	fn parse_pgsql(&self, text: &str) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		crate::pgsql::parse_to_statements(text, &self.capabilities.load(), &self.parser_config)
			.map_err(|e| TypesError::validation(e.to_string(), None))
	}

	#[cfg(not(feature = "postgres"))]
	fn parse_pgsql(&self, _text: &str) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		Err(TypesError::configuration(
			"This build does not support PostgreSQL queries".to_string(),
			surrealdb_types::ConfigurationError::DialectNotSupported,
		))
	}

	/// Parse and lower MySQL SQL query text to statements and plans.
	#[cfg(feature = "mysql")]
	fn parse_mysql(&self, text: &str) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		crate::mysql::parse_to_statements(text, &self.capabilities.load(), &self.parser_config)
			.map_err(|e| TypesError::validation(e.to_string(), None))
	}

	#[cfg(not(feature = "mysql"))]
	fn parse_mysql(&self, _text: &str) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		Err(TypesError::configuration(
			"This build does not support MySQL queries".to_string(),
			surrealdb_types::ConfigurationError::DialectNotSupported,
		))
	}

	/// The preflight a query runs: the session checks, and the options and
	/// context the statements execute against.
	///
	/// [`Self::execute_import`] does not come through here. It repeats these
	/// checks inline and reports an expired session and an anonymous actor as
	/// `DatastoreError`/`PolicyError` where this reports `TypesError`, so a
	/// check added here does not reach the import path.
	///
	/// A caller-owned transaction is prepared here rather than by the executor.
	/// It carries the session's tenant identity so the emitted
	/// [`crate::observe::TransactionEvent`] names the active namespace,
	/// database and user; and the write-cardinality guard is armed on it
	/// because statements running inside a client-owned transaction (RPC
	/// `begin`, SDK-managed transactions) are subject to the same configured
	/// limit as executor-created statement transactions — wrapping a statement
	/// in an explicit transaction must not bypass the guard.
	fn setup_query(
		&self,
		sess: &Session,
		vars: Option<PublicVariables>,
		tx: Option<&Arc<Transaction>>,
		cancel: Option<CancelHandle>,
	) -> std::result::Result<(Options, FrozenContext), TypesError> {
		self.setup_query_with(&FULL, &self.compiled_units, sess, vars, tx, cancel)
	}

	/// [`Self::setup_query`] for an entry serving `surface`, whose function-body
	/// units are compiled into and served from `compiled_units`.
	fn setup_query_with(
		&self,
		surface: &'static Surface,
		compiled_units: &Arc<crate::exec::vm::units::CompiledUnits>,
		sess: &Session,
		vars: Option<PublicVariables>,
		tx: Option<&Arc<Transaction>>,
		cancel: Option<CancelHandle>,
	) -> std::result::Result<(Options, FrozenContext), TypesError> {
		// Check if the session has expired
		if sess.expired() {
			return Err(TypesError::not_allowed(
				"The session has expired".to_string(),
				AuthError::SessionExpired,
			));
		}

		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		if let Err(e) = self.check_anon(sess) {
			return Err(TypesError::not_allowed(
				format!("Anonymous access not allowed: {e}"),
				AuthError::NotAllowed {
					actor: "anonymous".to_owned(),
					action: "process".to_owned(),
					resource: "query".to_owned(),
				},
			));
		}

		// Create a new query options
		let opt = self.setup_options(sess);

		// Create a default context.
		//
		// `anyhow_to_types_error`, not a downcast to `err::Error`: that enum
		// holds a handful of residual variants, so a bare layer error
		// (`ExecError::DbEmpty` from `Options::ns_db()`, say) misses and
		// reaches the client as `internal` rather than as a validation
		// failure. The helper walks the whole registry.
		let mut ctx = self
			.setup_ctx_with(surface, compiled_units)
			.map_err(crate::err::anyhow_to_types_error)?;

		// Install the external cancellation handle if one was supplied. The
		// executor checks `Context::done` between statements and in iterator
		// hot loops, so flipping the flag mid-query causes the next yield to
		// return `EngineError::QueryCancelled` and the executor's error path
		// finalises the transaction cleanly. Bare-await sites (`SLEEP`)
		// `select!` against the handle's awaitable view. This is a flag rather
		// than the caller dropping the execution future because dropping the
		// future would drop an open transaction with it.
		if let Some(cancel) = cancel {
			ctx.set_cancellation(&cancel);
		}

		// Start an execution context
		ctx.attach_session(sess).map_err(crate::err::into_types_error)?;

		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_variables(vars.into()).map_err(crate::err::into_types_error)?;
		}

		// Prepare an externally-supplied transaction. See the method docs for
		// why the identity and the guard are applied here.
		if let Some(tx) = tx {
			if let Some(identity) = ctx.tenant_identity() {
				tx.set_tenant_identity(Arc::clone(identity));
			}
			tx.arm_write_keys_limit(self.transaction_max_write_keys());
			ctx.set_transaction(Arc::clone(tx));
		}

		Ok((opt, ctx.freeze()))
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
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn execute(
		&self,
		txt: &str,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		self.run(QueryRequest::new(txt, sess).with_variables(vars)).await
	}

	/// Execute a pre-parsed SQL query
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn process(
		&self,
		ast: impl Into<Ast>,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> std::result::Result<Vec<QueryResult>, TypesError> {
		self.run(QueryRequest::new(ast.into(), sess).with_variables(vars)).await
	}

	/// The parser settings an import is read under.
	///
	/// An import is this engine's own export read back, so its depth ceiling
	/// is derived from the deepest value the storage layer accepts
	/// ([`MAX_VALUE_DEPTH`]) rather than from the configured query limits:
	/// whatever was legal to write has to be legal to restore, on any node and
	/// under any configuration. A ceiling below that caps a restore short of
	/// what the writer accepts, and the shortfall shows up only as one failed
	/// statement inside an otherwise successful restore.
	///
	/// The grammar gates are derived from the caller's live capabilities:
	/// import text is a fresh statement at the query boundary, not text this
	/// engine rendered, so the experimental-syntax gates apply to
	/// it as they do to any other operator-supplied query.
	///
	/// The text is marked as an import, so the definition forms an export or
	/// a 3.x dump writes (an unnamed silo extension, `AS` for `FROM`, a
	/// definition without `UNSIGNED`) are read back; an ordinary query refuses
	/// the first two, and the executor refuses the third outside an import.
	fn import_parser_settings(capabilities: &Capabilities) -> ParserSettings {
		ParserSettings {
			files_enabled: capabilities.allows_experimental(&ExperimentalTarget::Files),
			surrealism_enabled: capabilities.allows_experimental(&ExperimentalTarget::Surrealism),
			import: true,
			..ParserSettings::for_value_depth(MAX_VALUE_DEPTH)
		}
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn execute_import<S>(
		&self,
		sess: &Session,
		vars: Option<PublicVariables>,
		query: S,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<Bytes>>,
	{
		self.execute_import_bounded(sess, vars, query, IMPORT_CARRY_BOUND).await
	}

	/// [`Datastore::execute_import`] with `carry` as the bound on what the
	/// import's top-level `LET`s retain between statements.
	pub(crate) async fn execute_import_bounded<S>(
		&self,
		sess: &Session,
		vars: Option<PublicVariables>,
		query: S,
		carry: CarryBound,
	) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<Bytes>>,
	{
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);

		// Check if anonymous actors can execute queries when auth is enabled
		// TODO(sgirones): Check this as part of the authorisation layer
		self.check_anon(sess).map_err(|_| {
			Error::from(PolicyError::NotAllowed {
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

		let parser_settings = Self::import_parser_settings(&ctx.get_capabilities());
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
							Poll::Ready(Some(Err(anyhow::Error::new(ParseError::InvalidQuery(e)))))
						}
						Ok(None) => Poll::Ready(None),
						Ok(Some(x)) => Poll::Ready(Some(Ok(x))),
					};
				}

				// otherwise try to parse a single statement.
				match statements_stream.parse_partial(&mut buffer) {
					Err(e) => {
						return Poll::Ready(Some(Err(anyhow::Error::new(
							ParseError::InvalidQuery(e),
						))));
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

		Executor::execute_statement_stream_bounded(
			self,
			Arc::new(ctx),
			opt,
			true,
			carry,
			stream.map(|x| x.map(TopLevelStatement::Ast)),
		)
		.await
	}

	/// Evaluates a SQL [`Value`] without checking authenticating config
	/// This is used in very specific cases, where we do not need to check
	/// whether authentication is enabled, or guest access is disabled.
	/// For example, this is used when processing a record access SIGNUP or
	/// SIGNIN clause, which still needs to work without guest access.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub(crate) async fn evaluate(
		&self,
		val: &Expr,
		sess: &Session,
		vars: Option<PublicVariables>,
	) -> Result<PublicValue> {
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);
		// Create a new query options
		let opt = self.setup_options(sess);
		// Create a default context
		let mut ctx = self.setup_ctx()?;
		// Set the global query timeout
		if let Some(timeout) = self.dynamic_configuration.get_query_timeout() {
			ctx.add_timeout(timeout)?;
		}

		let txn_type = if val.read_only() {
			TransactionType::Read
		} else {
			TransactionType::Write
		};
		// Start a new transaction. Tenant identity is attached up-front so the
		// emitted [`crate::observe::TransactionEvent`] carries the session's
		// namespace, database, user, etc. The write-cardinality guard is
		// armed like any other statement execution: this path evaluates
		// owner-defined record-access clauses (SIGNUP/SIGNIN/AUTHENTICATE)
		// on behalf of unauthenticated callers, so its fan-out must be
		// subject to the same configured limit.
		let txn = self
			.transaction(txn_type)
			.await?
			.with_tenant_identity(Some(Arc::new(crate::observe::TenantIdentity::from(sess))))
			.with_write_keys_limit(self.transaction_max_write_keys())
			.enclose();
		// Store the transaction
		ctx.set_transaction(Arc::clone(&txn));

		// Start an execution context
		ctx.attach_session(sess)?;
		// Store the query variables
		if let Some(vars) = vars {
			ctx.attach_public_variables(vars)?;
		}

		// Freeze the context
		let ctx = ctx.freeze();
		// Evaluate the clause. It has no statement behind it, so it is planned
		// as a statement of its own, in the frame every caller outside a plan
		// enters through, under the options built for this session above.
		let exec = crate::exec::entry::frame(&ctx, &opt, None).await;
		let res = crate::exec::entry::statement(&exec, val).await;
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

	/// Performs a database import from SQL
	///
	/// The text is parsed under [`Self::import_parser_settings`] rather than
	/// the datastore's configured query limits, so that this entry point and
	/// [`Self::execute_import`] restore the same exports. Everything after
	/// parsing is an ordinary query execution.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn import(&self, sql: &str, sess: &Session) -> Result<Vec<QueryResult>> {
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);
		// Parse the import under the import profile.
		//
		// Classified as the caller's mistake, as `Datastore::execute` classifies
		// the same failure: a truncated or hand-edited dump is a malformed
		// request, and retrying it unchanged cannot succeed. Left to rise as a
		// bare `ParseError` it reaches every transport as an unclassified
		// internal fault, which reads as a server problem worth retrying.
		let ast = surrealdb_expr::parser::query_with_settings(
			sql,
			Self::import_parser_settings(&self.capabilities.load()),
		)
		.map_err(|e| TypesError::validation(e.to_string(), None))?;
		// Execute the SQL import
		self.process(ast, sess, None).await.map_err(|e| anyhow::anyhow!(e))
	}

	/// Performs a database import from SQL
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn import_stream<S>(&self, sess: &Session, stream: S) -> Result<Vec<QueryResult>>
	where
		S: Stream<Item = Result<Bytes>>,
	{
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);
		// Execute the SQL import
		self.execute_import(sess, None, stream).await
	}

	/// Performs a full database export as SQL
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn export(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
	) -> Result<impl Future<Output = Result<()>>> {
		// Create a default export config
		let cfg = surrealdb_rpc::export::Config::default();
		self.export_with_config(sess, chn, cfg).await
	}

	/// Performs a full database export as SQL
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "debug", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn export_with_config(
		&self,
		sess: &Session,
		chn: Sender<Vec<u8>>,
		cfg: surrealdb_rpc::export::Config,
	) -> Result<impl Future<Output = Result<()>> + 'static> {
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);
		// A dump carries the current state of each record and has no grammar for a
		// record's history, so an export cannot answer a request for versions.
		//
		// Refused here rather than inside the returned job, because a transport that
		// streams the export has already sent a success status and opened the body by
		// the time that job runs: a refusal raised in there reaches the caller as an
		// empty dump that claims to have succeeded. Every caller awaits this function
		// before it awaits the job, so this is the last point common to all of them
		// that still precedes a response.
		//
		// Raised as a structured validation error rather than a bare message, so each
		// transport classifies it as the caller's mistake: the request named an option
		// the format has no representation for, and retrying it unchanged cannot
		// succeed.
		if cfg.versions {
			bail!(TypesError::validation(
				"Versioned export is not supported: a dump carries the current state of each \
				 record only"
					.to_owned(),
				surrealdb_types::ValidationError::InvalidRequest,
			));
		}
		// Retrieve the provided NS and DB
		let (ns, db) = crate::iam::check::check_ns_db(sess)?;
		// Create a new readonly transaction
		let txn = Arc::new(self.transaction(Read).await?);
		let batch_size = self.config.datastore.export_batch_size;
		// The frame the rows are read in: the export's own transaction, with
		// the authority of root, because a dump is the rows as stored and
		// permissions are judged by the check above.
		let mut ctx = self.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		let frame = super::export::RowFrame {
			ctx: ctx.freeze(),
			opt: crate::dbs::Options::new(&self.config.exec)
				.with_ns(Some(ns.as_str().into()))
				.with_db(Some(db.as_str().into()))
				.with_auth(Arc::new(crate::iam::Auth::for_root(crate::iam::Role::Owner))),
		};
		// Return an async export job
		Ok(async move {
			// Process the export
			let res = super::export::export(&txn, frame, &ns, &db, cfg, batch_size, chn).await;
			txn.cancel().await?;
			res
		})
	}

	/// Checks the required permissions level for this session
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, sess)
		)
	)]
	#[allow(clippy::needless_pass_by_value)] // Public API: ergonomic for callers passing `ResourceKind::X.on_db(ns, db)` inline.
	pub fn check(&self, sess: &Session, action: Action, resource: Resource) -> Result<()> {
		// Check if the session has expired
		ensure!(!sess.expired(), DatastoreError::ExpiredSession);
		// Skip auth for Anonymous users if auth is disabled
		let skip_auth = !self.is_auth_enabled() && sess.au.is_anon();
		if !skip_auth {
			sess.au.is_allowed(action, &resource).map_err(crate::err::Error::from)?;
		}
		// All ok
		Ok(())
	}

	pub fn setup_options(&self, sess: &Session) -> Options {
		Options::new(&self.config.exec)
			.with_ns(sess.ns())
			.with_db(sess.db())
			.with_auth(Arc::clone(&sess.au))
	}

	pub fn setup_ctx(&self) -> Result<Context> {
		self.setup_ctx_with(&FULL, &self.compiled_units)
	}

	/// The root context of a query served under `surface`, whose function-body
	/// units are compiled into and served from `compiled_units`.
	pub(crate) fn setup_ctx_with(
		&self,
		surface: &'static Surface,
		compiled_units: &Arc<crate::exec::vm::units::CompiledUnits>,
	) -> Result<Context> {
		let mut ctx = Context::from_ds(
			self.id,
			self.auth_enabled,
			self.dynamic_configuration.clone(),
			self.dynamic_configuration.get_query_timeout(),
			self.slow_log.clone(),
			self.capabilities.load_full(),
			self.index_stores.clone(),
			self.index_builder.clone(),
			self.sequences.clone(),
			Arc::clone(&self.cache),
			#[cfg(feature = "http")]
			self.http_client.load_full(),
			Arc::clone(&self.function_registry),
			surface,
			Arc::clone(&self.live_units),
			Arc::clone(compiled_units),
			#[cfg(storage)]
			self.temporary_directory.clone(),
			self.buckets.clone(),
			Arc::clone(&self.config),
			#[cfg(feature = "surrealism")]
			Arc::clone(&self.surrealism_cache),
		)?;
		ctx.set_scheduler(self.scheduler.clone());
		ctx.set_memory(Some(self.memory_host().query()));
		Ok(ctx)
	}

	/// check for disallowed anonymous users
	pub fn check_anon(&self, sess: &Session) -> Result<(), PolicyError> {
		if self.auth_enabled && sess.au.is_anon() && !self.capabilities.load().allows_guest_access()
		{
			Err(PolicyError::NotAllowed {
				actor: "anonymous".to_string(),
				action: String::new(),
				resource: String::new(),
			})
		} else {
			Ok(())
		}
	}

	/// SECURITY: `USE NS` implicitly creates the namespace when it does
	/// not exist. `DEFINE NAMESPACE` requires `Edit` on `Namespace`@`Root`,
	/// so the same authorization must gate the materialization step in
	/// `USE`. Returns `true` if the caller may proceed with
	/// `get_or_add_ns` — either because the namespace already exists
	/// (in which case the call is a no-op lookup) or because the caller
	/// has the necessary authorization. Returns `false` when the
	/// namespace does not exist *and* the caller lacks permission;
	/// callers that still want to set the session context for a later
	/// authenticated step (the typical pre-signin RPC `USE` pattern)
	/// can do so without triggering creation. See `SECURITY_GUIDE.md`
	/// section 3.
	pub(crate) async fn should_materialize_ns_on_use(
		&self,
		tx: &Transaction,
		auth: &Auth,
		ns: &str,
	) -> Result<bool> {
		if tx.get_ns_by_name(ns, None).await?.is_some() {
			return Ok(true);
		}
		if !self.auth_enabled && auth.is_anon() {
			return Ok(true);
		}
		Ok(auth.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root()).is_ok())
	}

	/// SECURITY: counterpart to [`Self::should_materialize_ns_on_use`]
	/// for the database half of `USE`. Implicit creation requires the
	/// same authorization as `DEFINE DATABASE` (`Edit` on `Database`@`Ns`),
	/// AND the parent namespace must already exist or the caller must
	/// also be authorized to create it — `ensure_ns_db` is
	/// `get_or_add_db_upwards(..., upwards = true)` and will silently
	/// `get_or_add_ns` the parent when it is missing
	/// (`kvs/tx.rs::get_or_add_db_upwards`). Without the second check a
	/// namespace-level Editor on a stale token (the namespace was
	/// dropped after the token was issued) could recreate the parent
	/// namespace as a side effect of `USE NS dropped DB anything`.
	pub async fn should_materialize_db_on_use(
		&self,
		tx: &Transaction,
		auth: &Auth,
		ns: &str,
		db: &str,
	) -> Result<bool> {
		if tx.get_db_by_name(ns, db, None).await?.is_some() {
			return Ok(true);
		}
		if !self.auth_enabled && auth.is_anon() {
			return Ok(true);
		}
		// Block the upwards-create side effect: if the parent namespace
		// is missing and the caller can't create namespaces, refuse.
		if tx.get_ns_by_name(ns, None).await?.is_none()
			&& auth.is_allowed(Action::Edit, &ResourceKind::Namespace.on_root()).is_err()
		{
			return Ok(false);
		}
		Ok(auth.is_allowed(Action::Edit, &ResourceKind::Database.on_ns(ns)).is_ok())
	}

	pub async fn process_use(
		&self,
		ctx: Option<&dyn crate::catalog::providers::CancellationProbe>,
		session: &mut Session,
		namespace: Option<String>,
		database: Option<String>,
	) -> std::result::Result<QueryResult, TypesError> {
		let new_tx = || async {
			self.transaction(Write).await.map_err(|err| TypesError::internal(err.to_string()))
		};
		let commit_tx = |txn: Transaction| async move {
			txn.commit().await.map_err(|err| TypesError::internal(err.to_string()))
		};

		let query_result = QueryResultBuilder::started_now();
		// SECURITY: `process_use` may be called before the caller has
		// authenticated (e.g. SDKs that call `use` before `signin`). To
		// preserve that pattern without re-opening the bypass that this
		// guard closes, we set the session context but only commit
		// implicit `DEFINE NAMESPACE` / `DEFINE DATABASE`-equivalent
		// creation when the caller has authorization for it. Callers
		// without permission end up with a session that targets a
		// resource that may not exist; downstream operations surface a
		// clean `NsNotFound` / `DbNotFound` rather than a silently
		// auto-created namespace they should not have been able to
		// create. See `SECURITY_GUIDE.md` section 3.
		let map_internal = |err: anyhow::Error| TypesError::internal(err.to_string());
		// SECURITY (GHSA-2v9j): tenant-boundary gate, mirroring the SurrealQL
		// `USE` statement handler (`dbs::executor`) and the RPC `use` handler
		// (`rpc::protocol::yuse`). This path (`process_use`) serves the embedded
		// SDK `Command::Use` and the PostgreSQL wire; the WS/HTTP/gRPC RPC `use`
		// method carries its own copy of this gate in `yuse`. An authenticated
		// Database-, Namespace- or Record-level principal must be verified as
		// authorized for the target namespace/database before the session is
		// pivoted, or it could switch into a foreign tenant and reach any table
		// there with satisfiable PERMISSIONS. The authenticated Auth level is the
		// source of truth; a switch that changes only one of ns/db keeps the
		// other from the current session. Root and anonymous (`Level::No`, e.g.
		// `use` before `signin`) principals are unrestricted by `can_access_ns_db`,
		// preserving the pre-signin `use` pattern.
		{
			let target_ns = namespace.as_deref().or(session.ns.as_deref());
			let target_db = database.as_deref().or(session.db.as_deref());
			if let Some(ns_name) = target_ns {
				let db_name = target_db.unwrap_or_default();
				if !session.au.can_access_ns_db(ns_name, db_name) {
					let err = if session.au.level().ns() == Some(ns_name) {
						ExecError::DbNotAllowed {
							db: db_name.to_owned(),
						}
					} else {
						ExecError::NsNotAllowed {
							ns: ns_name.to_owned(),
						}
					};
					return Err(TypesError::not_allowed(err.to_string(), None));
				}
			}
		}
		match (namespace, database) {
			(Some(ns), Some(db)) => {
				let tx = new_tx().await?;
				let create_ns = self
					.should_materialize_ns_on_use(&tx, &session.au, &ns)
					.await
					.map_err(map_internal)?;
				let create_db = create_ns
					&& self
						.should_materialize_db_on_use(&tx, &session.au, &ns, &db)
						.await
						.map_err(map_internal)?;
				if create_db {
					tx.ensure_ns_db(ctx, &ns, &db).await.map_err(map_internal)?;
					commit_tx(tx).await?;
				} else if create_ns {
					tx.get_or_add_ns(ctx, &ns).await.map_err(map_internal)?;
					commit_tx(tx).await?;
				} else {
					let _ = tx.cancel().await;
				}
				session.ns = Some(ns);
				session.db = Some(db);
			}
			(Some(ns), None) => {
				let tx = new_tx().await?;
				let create_ns = self
					.should_materialize_ns_on_use(&tx, &session.au, &ns)
					.await
					.map_err(map_internal)?;
				if create_ns {
					tx.get_or_add_ns(ctx, &ns).await.map_err(map_internal)?;
					commit_tx(tx).await?;
				} else {
					let _ = tx.cancel().await;
				}
				session.ns = Some(ns);
			}
			(None, Some(db)) => {
				let Some(ns) = session.ns.clone() else {
					return Err(TypesError::validation(
						"Cannot use database without namespace".to_string(),
						None,
					));
				};
				let tx = new_tx().await?;
				let create_db = self
					.should_materialize_db_on_use(&tx, &session.au, &ns, &db)
					.await
					.map_err(map_internal)?;
				if create_db {
					tx.ensure_ns_db(ctx, &ns, &db).await.map_err(map_internal)?;
					commit_tx(tx).await?;
				} else {
					let _ = tx.cancel().await;
				}
				session.db = Some(db);
			}
			// An empty `USE` asks for the configured default, not for the
			// session to be cleared. A session that has already selected a
			// namespace — from a token, say — keeps it, so this only ever fills
			// in what is missing.
			(None, None) if session.ns.is_none() => {
				let tx = new_tx().await?;
				let default = match tx.get_default_config().await {
					Ok(config) => config.map(|c| (c.namespace.clone(), c.database.clone())),
					Err(e) => {
						let _ = tx.cancel().await;
						return Err(map_internal(e));
					}
				};

				// The default names a namespace and database a fresh datastore
				// may not have materialised yet. Creating them is gated on the
				// same authorization an explicit `DEFINE NAMESPACE` /
				// `DEFINE DATABASE` needs, as the arms above are: a caller
				// without it still gets the selection, and a later operation
				// reports a clean `NsNotFound` rather than silently having
				// created what it could not have defined. See
				// `SECURITY_GUIDE.md` section 3.
				if let Some((Some(ns), db)) = default {
					// SECURITY (GHSA-2v9j): the default names a namespace/database
					// to a scoped principal just as an explicit `USE` would, so gate
					// the resolved default the same way before selecting or creating
					// it. Root and anonymous principals are unrestricted by
					// `can_access_ns_db`; a scoped principal is confined to its own
					// namespace/database. Mirrors the executor's `USE DEFAULT` path.
					if !session.au.can_access_ns_db(&ns, db.as_deref().unwrap_or_default()) {
						let _ = tx.cancel().await;
						let err = if session.au.level().ns() == Some(ns.as_str()) {
							ExecError::DbNotAllowed {
								db: db.clone().unwrap_or_default(),
							}
						} else {
							ExecError::NsNotAllowed {
								ns: ns.clone(),
							}
						};
						return Err(TypesError::not_allowed(err.to_string(), None));
					}
					let create_ns = self
						.should_materialize_ns_on_use(&tx, &session.au, &ns)
						.await
						.map_err(map_internal)?;
					if create_ns && let Err(e) = tx.get_or_add_ns(ctx, &ns).await {
						let _ = tx.cancel().await;
						return Err(map_internal(e));
					}
					if let Some(db) = db {
						let create_db = create_ns
							&& self
								.should_materialize_db_on_use(&tx, &session.au, &ns, &db)
								.await
								.map_err(map_internal)?;
						if create_db && let Err(e) = tx.ensure_ns_db(ctx, &ns, &db).await {
							let _ = tx.cancel().await;
							return Err(map_internal(e));
						}
						session.db = Some(db);
					}
					session.ns = Some(ns);
				}

				commit_tx(tx).await?;
			}
			(None, None) => {}
		}

		let value = PublicValue::from_t(object! {
			namespace: session.ns.clone(),
			database: session.db.clone(),
		});

		Ok(query_result.finish_with_result(Ok(value)))
	}

	/// Get a db model by name.
	///
	/// TODO: This should not be public, but it is used by callers outside the
	/// `surrealdb-core` crate (the SDK's local engine and the server's ML route).
	pub async fn get_db_model(
		&self,
		ns: &str,
		db: &str,
		model_name: &str,
		model_version: &str,
	) -> Result<Option<Arc<crate::catalog::StoredMlModelDefinition>>> {
		let tx = self.transaction(Read).await?;
		let db = tx.expect_db_by_name(ns, db).await?;
		let model = tx
			.get_db_model(db.namespace_id, db.database_id, model_name, model_version, None)
			.await?;
		tx.cancel().await?;
		Ok(model.map(|m| Arc::new(m.to_stored())))
	}

	/// Invoke an API handler.
	///
	/// TODO: This should not need to be public, but it is used by the server's
	/// HTTP API route (outside the `surrealdb-core` crate).
	pub async fn invoke_api_handler(
		&self,
		ns: &str,
		db: &str,
		path: &str,
		session: &Session,
		mut req: ApiRequest,
	) -> Result<ApiResponse> {
		// Enforce the tenant boundary before resolving or dispatching anything.
		// The namespace/database come from caller-controlled input (the
		// `/api/:ns/:db/:endpoint` URL path) and the HTTP route has already
		// overwritten the session's selected ns/db with them, so the
		// authenticated level is the only trustworthy scope. A principal
		// authenticated for one tenant must not be able to invoke another
		// tenant's custom API — whose handler runs with permissions disabled
		// (GHSA-848m-r628-vrxw).
		if !session.au.can_access_ns_db(ns, db) {
			debug!(
				request_id = %req.request_id,
				"Custom API request denied: URL namespace/database is outside the authenticated session scope"
			);
			return Ok(ApiResponse::from_error(ApiError::PermissionDenied, req.request_id.clone()));
		}

		let tx = Arc::new(self.transaction(TransactionType::Write).await?);
		// Custom API handlers evaluate owner-defined expressions on behalf of
		// external callers — statement execution in everything but name — so
		// the write-cardinality guard applies exactly as it does to executor
		// statements and explicit client-owned transactions.
		tx.arm_write_keys_limit(self.transaction_max_write_keys());

		// `tx` is open and writeable from here until the commit/cancel below, so
		// every fallible step in between cancels before returning. A bare `?`
		// would drop the transaction without committing or cancelling it,
		// tripping `Transactor::drop`'s "a transaction was dropped without being
		// committed or cancelled" error and leaving it to be reaped rather than
		// rolled back promptly. `catch!` is the cancel-then-propagate form.
		let db = catch!(tx, tx.ensure_ns_db(None, ns, db).await);

		let segments: Vec<&str> = path.split('/').filter(|x| !x.is_empty()).collect();

		// Routes on the stored definitions and compiles only the one that
		// handles this request: this transaction is opened per request, so the
		// compiled catalog cache is always cold, and compiling the whole list
		// would parse every handler body in the database to dispatch one.
		let routed = catch!(
			tx,
			tx.find_db_api(db.namespace_id, db.database_id, &segments, req.method).await
		);
		let res = match routed {
			Some((api, params)) => {
				let api = &api;
				debug!(
					request_id = %req.request_id,
					path = %path,
					"API definition found, dispatching to the API chain"
				);
				req.params = catch!(tx, params.try_into());

				let opt = self.setup_options(session);

				let mut ctx = catch!(tx, self.setup_ctx());
				ctx.set_transaction(Arc::clone(&tx));
				// Not `catch!`: like `try_into` above, this error needs
				// converting into the function's error type.
				if let Err(e) = ctx.attach_session(session) {
					let _ = tx.cancel().await;
					return Err(e.into());
				}
				let ctx = &ctx.freeze();

				crate::exec::api::process(ctx, &opt, api, req).await
			}
			None => {
				trace!(
					request_id = %req.request_id,
					path = %path,
					"No API definition found for path"
				);
				tx.cancel().await?;
				return Ok(ApiResponse::from_error(ApiError::NotFound, req.request_id.clone()));
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
		// Insert the model in to the database. The statement is built in the
		// form the parser produces, so it runs exactly as the same `DEFINE
		// MODEL` typed by a user would.
		let model = crate::expr::statements::DefineModelStatement {
			name: name.to_string().into(),
			version: version.to_string().into(),
			comment: crate::expr::Expr::Literal(crate::expr::Literal::String(description.into())),
			hash: hash.into(),
			kind: Default::default(),
			permissions: Default::default(),
		};
		let ast = surrealdb_expr::parser::Query::single_expr(crate::expr::Expr::Define(Box::new(
			crate::expr::statements::DefineStatement::Model(model),
		)));

		self.run(QueryRequest::new(ast, session)).await.map_err(|e| anyhow::anyhow!(e))?;

		Ok(())
	}

	/// The per-layer configuration this datastore was built with. Cheap to
	/// clone; the inner handle is an `Arc`. Crate-internal: these are operator
	/// knobs, not API — callers outside core take the specific settings they
	/// need, as the server does through [`Self::parser_config`].
	pub(crate) fn config(&self) -> Arc<RuntimeConfig> {
		Arc::clone(&self.config)
	}

	/// The parser depth limits this datastore parses query text with.
	///
	/// Transports that decode request bodies (RPC, WebSocket, export) apply the
	/// same limits, so they read them from here rather than from their own
	/// defaults. Cheap to clone; the inner handle is an `Arc`.
	pub fn parser_config(&self) -> Arc<ParserConfig> {
		Arc::clone(&self.parser_config)
	}

	/// Retrieve (or generate and cache) the GraphQL schema for the namespace
	/// and database selected on `session`. Backed by the datastore-wide schema
	/// cache so every transport (HTTP `/graphql`, WebSocket subscriptions, the
	/// `graphql` RPC method, the MCP `graphql` tool) shares one set of compiled
	/// schemas and a single DDL-driven invalidation path.
	#[cfg(all(feature = "graphql", not(target_family = "wasm")))]
	pub async fn graphql_schema(
		self: &Arc<Self>,
		session: &Session,
	) -> Result<async_graphql::dynamic::Schema, crate::graphql::GraphqlError> {
		self.graphql_schema_cache.get_schema(self, session).await
	}

	#[cfg(feature = "http")]
	pub fn http_client(&self) -> Arc<HttpClient> {
		self.http_client.load_full()
	}

	/// Builds a [`NodeEndpointResolver`] backed by this datastore's catalog. Used by the
	/// builder to hand a resolver to clustered message brokers after the datastore is
	/// fully constructed.
	///
	/// Only available on non-WASM targets: the underlying transaction types are not
	/// `Send + Sync` under the WASM single-threaded model, and cross-node delivery
	/// (the only consumer) doesn't apply to in-browser datastores.
	#[cfg(not(target_family = "wasm"))]
	pub(crate) fn endpoint_resolver(&self) -> Arc<dyn crate::dbs::NodeEndpointResolver> {
		Arc::new(CatalogNodeEndpointResolver {
			transaction_factory: self.transaction_factory.clone(),
			sequences: self.sequences.clone(),
		})
	}
}

/// Catalog-backed [`NodeEndpointResolver`] handed to clustered brokers post-construction.
///
/// Holds clones of the transaction factory and sequences (both cheap `Arc`-based clones) so
/// it can open read transactions independently of the [`Datastore`] struct, avoiding any
/// reference cycle between the broker and the datastore.
#[cfg(not(target_family = "wasm"))]
#[derive(Clone)]
struct CatalogNodeEndpointResolver {
	transaction_factory: TransactionFactory,
	sequences: Sequences,
}

#[cfg(not(target_family = "wasm"))]
impl std::fmt::Debug for CatalogNodeEndpointResolver {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CatalogNodeEndpointResolver").finish_non_exhaustive()
	}
}

#[cfg(not(target_family = "wasm"))]
impl crate::dbs::NodeEndpointResolver for CatalogNodeEndpointResolver {
	fn resolve(
		&self,
		target_node: [u8; 16],
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<String>> + Send + '_>> {
		Box::pin(async move {
			let uuid = Uuid::from_bytes(target_node);
			let txn =
				self.transaction_factory.transaction(Read, self.sequences.clone()).await.ok()?;
			let key = NodeKey {
				nd: uuid,
			};
			let node: Option<Node> = txn.get_key(&key, None).await.ok()?;
			let _ = txn.cancel().await;
			node.and_then(|n| n.http_endpoint)
		})
	}
}

pub(crate) fn define_user_statement_new_with_password(
	base: Base,
	user: String,
	pass: &str,
	role: String,
) -> DefineUserStatement {
	DefineUserStatement {
		kind: DefineKind::Default,
		base,
		name: Expr::Idiom(Idiom::field(user)),
		pending_password: None,
		hash_is_set: true,
		hash: crate::iam::hash_password(pass),
		code: Alphanumeric.sample_string(&mut rand::rng(), 128),
		roles: vec![role],
		duration: UserDuration::default(),
		comment: Expr::Literal(Literal::None),
		scram: surrealdb_expr::scram::derive(pass),
	}
}

#[cfg(test)]
pub(crate) mod test {
	use std::collections::BTreeMap;
	use std::future::pending;

	use chrono::Utc;
	use futures::FutureExt;
	#[cfg(diskann)]
	use surrealdb_datastore::values::diskann::{
		DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnPendingStateKind,
	};
	use surrealdb_kvs::consts::COUNT_BATCH_SIZE;
	use surrealdb_strand::TableName;

	use super::*;
	use crate::catalog::providers::{
		CatalogProvider, DatabaseProvider, NamespaceProvider, TableProvider, UserProvider,
	};
	use crate::iam::verify::verify_root_creds;
	use crate::idx::trees::hnsw::index::HNSW_COMPACTION_MAX_PENDING_KEYS;
	#[cfg(diskann)]
	use crate::key::schema::{DiskannPendingPrefix, DiskannRecordPendingShardShardPrefix};
	use crate::key::schema::{
		DocKeyPrefix, DocLookupIdentityPrefix, DocStatsBatchPrefix, IndexCountKey,
		IndexCountPrefix, TaskLeaseKey, TermChangeBatchPrefix,
	};
	use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
	use crate::kvs::testing::{
		NonRetryableErrorSite, RetryableConflictSite, inject_non_retryable_error,
		inject_non_retryable_errors, inject_retryable_conflict, inject_retryable_conflicts,
		retryable_conflict_count,
	};
	use crate::types::{PublicValue, PublicVariables};

	/// Work a test runs once a compaction pass of the node has compacted a batch
	/// and deleted its queue entries.
	type CompactionBatchHook =
		Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

	static AFTER_COMPACTION_BATCH: std::sync::LazyLock<
		std::sync::Mutex<HashMap<Uuid, CompactionBatchHook>>,
	> = std::sync::LazyLock::new(Default::default);

	/// The hook a test set for the node's next compacted batch, taken so it runs
	/// once.
	pub(super) fn take_after_compaction_batch(node: Uuid) -> Option<CompactionBatchHook> {
		AFTER_COMPACTION_BATCH.lock().unwrap().remove(&node)
	}

	/// Work a test runs in a resume scan of the node, before the scan takes up
	/// its next candidate.
	static BEFORE_RESUME_CANDIDATE: std::sync::LazyLock<
		std::sync::Mutex<HashMap<Uuid, CompactionBatchHook>>,
	> = std::sync::LazyLock::new(Default::default);

	/// The hook a test set for the node's next resume candidate, taken so it
	/// runs once.
	pub(super) fn take_before_resume_candidate(node: Uuid) -> Option<CompactionBatchHook> {
		BEFORE_RESUME_CANDIDATE.lock().unwrap().remove(&node)
	}

	/// A resume scan that loses its lease to another node before it takes up a
	/// candidate adopts no build: the stalled builds are the new holder's to
	/// recover, and two nodes must never adopt at once.
	#[tokio::test(flavor = "multi_thread")]
	async fn a_resume_scan_that_loses_its_lease_adopts_nothing() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE user SCHEMALESS;
			 CREATE user:one SET email = 'one@example.com' RETURN NONE;
			 DEFINE INDEX test ON user FIELDS email;",
		)
		.await?;
		let ikb = index_key_base(&ds, "user", "test").await?;
		// A build stranded by an owner whose lease has lapsed.
		let expired = Utc::now() - chrono::Duration::seconds(3_600);
		let stalled = crate::kvs::index::IndexBuildState {
			generation: 2,
			phase: crate::kvs::index::IndexBuildPhase::Building,
			owner: Some(Uuid::new_v4()),
			next_ticket: 0,
			initial_complete: false,
			updated_at: expired,
			owner_heartbeat_at: Some(expired),
			error: None,
			report_status: None,
			initial: Some(0),
			updated: None,
			pending: None,
			initial_cursor: None,
		};
		{
			let txn = ds.transaction(Write).await?;
			catch!(txn, txn.set_key(&ikb.new_bs_key(), &stalled).await);
			txn.commit().await?;
		}
		// Another node takes the resume lease once this one's scan has begun.
		let peer = Uuid::new_v4();
		let hook: CompactionBatchHook = {
			let ds = Arc::clone(&ds);
			Box::new(move || {
				Box::pin(async move {
					let key = TaskLeaseKey {
						task_id: TaskLeaseType::IndexBuildResume,
					};
					let lease = crate::catalog::TaskLease {
						owner: peer,
						expiration: Utc::now() + chrono::Duration::seconds(600),
					};
					let txn = ds.transaction(Write).await.expect("a transaction");
					txn.set_key(&key, &lease).await.expect("the lease is written");
					txn.commit().await.expect("the lease commits");
				})
			})
		};
		BEFORE_RESUME_CANDIDATE.lock().unwrap().insert(ds.id(), hook);

		let resumed = ds
			.resume_stalled_index_builds(Duration::from_secs(30), CancellationToken::new())
			.await?;
		assert_eq!(resumed, 0, "a scan that lost its lease adopts nothing");
		let txn = ds.transaction(Read).await?;
		let state = catch!(txn, txn.get_key(&ikb.new_bs_key(), None).await);
		txn.cancel().await?;
		assert_eq!(
			state.and_then(|state| state.owner),
			stalled.owner,
			"the stalled build keeps its lapsed owner for the lease holder to adopt"
		);
		Ok(())
	}

	/// Work a test runs in a count compaction pass of the node, between the
	/// pass reading a plan and opening the plan's write transaction.
	type BeforeCountApplyHook =
		Arc<dyn Fn() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

	/// Per node, how many more times the hook runs, and the hook.
	static BEFORE_COUNT_APPLY: std::sync::LazyLock<
		std::sync::Mutex<HashMap<Uuid, (usize, BeforeCountApplyHook)>>,
	> = std::sync::LazyLock::new(Default::default);

	/// Runs the hook a test set for the node's count compaction applies, while
	/// it has runs left.
	pub(crate) async fn before_count_compaction_apply(node: Uuid) {
		let hook = match BEFORE_COUNT_APPLY.lock().unwrap().get_mut(&node) {
			Some((runs, hook)) if *runs > 0 => {
				*runs -= 1;
				Some(Arc::clone(hook))
			}
			_ => None,
		};
		if let Some(hook) = hook {
			hook().await;
		}
	}

	/// Per node, the compaction ticket its compaction loop runs under in place
	/// of a registration with the node's pool.
	#[cfg(not(target_family = "wasm"))]
	static COMPACTION_PLACES: std::sync::LazyLock<
		std::sync::Mutex<HashMap<Uuid, crate::idx::places::Ticket>>,
	> = std::sync::LazyLock::new(Default::default);

	/// The compaction ticket a test gave the node, if any.
	#[cfg(not(target_family = "wasm"))]
	pub(super) fn compaction_places_for(node: Uuid) -> Option<crate::idx::places::Ticket> {
		COMPACTION_PLACES.lock().unwrap().get(&node).cloned()
	}

	/// Runs the node's compaction under a ticket on a pool of `places` places
	/// of its own, with no OLTP reserve, until the guard drops. A test whose
	/// passes must run side by side holds one, so it does not depend on how
	/// many places the node's pool has free, which varies with the host's
	/// cores and with the tests running beside it.
	#[cfg(not(target_family = "wasm"))]
	fn own_compaction_places(node: Uuid, places: usize) -> OwnCompactionPlaces {
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let ticket = crate::ctx::IndexPlaces::statement(
			Pool::new(places).register(),
			Policy::from_config(&ExecConfig {
				morsel_oltp_reserve: 0,
				..ExecConfig::default()
			}),
		);
		COMPACTION_PLACES.lock().unwrap().insert(node, ticket);
		OwnCompactionPlaces(node)
	}

	/// Restores the node's own compaction ticket when dropped.
	#[cfg(not(target_family = "wasm"))]
	struct OwnCompactionPlaces(Uuid);

	#[cfg(not(target_family = "wasm"))]
	impl Drop for OwnCompactionPlaces {
		fn drop(&mut self) {
			COMPACTION_PLACES.lock().unwrap().remove(&self.0);
		}
	}

	/// Makes the node's next `runs` count compaction applies lose their race:
	/// before each, another compactor advances the index's compaction
	/// generation past the one the pass's plan read, folding nothing.
	fn overtake_count_compaction_applies(ds: &Arc<Datastore>, ikb: &IndexKeyBase, runs: usize) {
		let node = ds.id();
		let (ds, ikb) = (Arc::clone(ds), ikb.clone());
		let hook: BeforeCountApplyHook = Arc::new(move || {
			let (ds, ikb) = (Arc::clone(&ds), ikb.clone());
			Box::pin(async move {
				advance_count_compaction_generation(&ds, &ikb)
					.await
					.expect("advancing the count compaction generation");
			})
		});
		BEFORE_COUNT_APPLY.lock().unwrap().insert(node, (runs, hook));
	}

	/// Advances a count index's compaction generation in a transaction of its
	/// own, as a compactor that applied a plan does.
	async fn advance_count_compaction_generation(ds: &Datastore, ikb: &IndexKeyBase) -> Result<()> {
		let key = crate::key::schema::IndexVersionKey {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		};
		let txn = ds.transaction(Write).await?;
		let current = catch!(txn, txn.get_key(&key, None).await);
		catch!(txn, txn.set_key(&key, &current.unwrap_or(0).saturating_add(1)).await);
		txn.commit().await
	}

	async fn new_index_compaction_test_ds() -> Result<(Arc<Datastore>, Session)> {
		new_index_compaction_test_ds_with_config(ConfigMap::empty()).await
	}

	/// [`new_index_compaction_test_ds`], with one configuration key set.
	async fn new_index_compaction_test_ds_with(
		key: &str,
		value: &str,
	) -> Result<(Arc<Datastore>, Session)> {
		new_index_compaction_test_ds_with_config(ConfigMap::empty().with_key_value(key, value))
			.await
	}

	async fn new_index_compaction_test_ds_with_config(
		config: ConfigMap,
	) -> Result<(Arc<Datastore>, Session)> {
		// These tests decide when compaction runs and observe the wake-up that
		// drives it. `CommitTriggers::index_compaction` is a `notify_one`, so a
		// background compactor would take the permit a test is waiting on.
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_config(config)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		Ok((ds, session))
	}

	async fn execute_all(ds: &Datastore, session: &Session, sql: &str) -> Result<()> {
		for result in ds.execute(sql, session, None).await? {
			result.result?;
		}
		Ok(())
	}

	#[tokio::test]
	async fn record_auth_is_scoped_to_its_ns_db() -> Result<()> {
		// A record-scoped principal is only authorized for the namespace and
		// database it authenticated against. If the session context drifts to
		// another namespace or database (via a USE statement or connection
		// headers), every document operation must be rejected — even when the
		// target table's PERMISSIONS clauses would otherwise allow the
		// operation for record users.
		let ds = Datastore::new("memory").await?;
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "first", "first").await?;
		txn.ensure_ns_db(None, "first", "other").await?;
		txn.ensure_ns_db(None, "second", "second").await?;
		txn.commit().await?;
		// Victim data in `second/second` with fully permissive permissions
		let owner = Session::owner().with_ns("second").with_db("second");
		execute_all(
			&ds,
			&owner,
			"DEFINE TABLE thing SCHEMALESS PERMISSIONS FULL; CREATE thing:1 SET secret = 'v';",
		)
		.await?;
		// A record principal authenticated against `first/first`, whose
		// session context has drifted to `second/second`
		let mut rec = Session::for_record(
			"first",
			"first",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "tester")),
		);
		rec.ns = Some("second".to_owned());
		rec.db = Some("second".to_owned());
		// Every write into the drifted scope must be rejected
		for query in [
			"CREATE thing:2 SET secret = 'w'",
			"UPDATE thing:1 SET secret = 'w'",
			"UPSERT thing:1 SET secret = 'w'",
			"DELETE thing:1",
			"INSERT INTO thing { id: thing:3, secret: 'w' }",
		] {
			let err = execute_all(&ds, &rec, query)
				.await
				.expect_err(&format!("record user expected to be denied: {query}"));
			assert!(
				err.to_string().contains("namespace"),
				"expected a namespace authorization error for {query}, got: {err}"
			);
		}
		// Reads from the drifted scope must be rejected as well
		let err = execute_all(&ds, &rec, "SELECT * FROM thing")
			.await
			.expect_err("record user expected to be denied: SELECT");
		assert!(
			err.to_string().contains("namespace"),
			"expected a namespace authorization error for SELECT, got: {err}"
		);
		// A drift within the same namespace but to a different database must
		// also be rejected
		let owner = Session::owner().with_ns("first").with_db("other");
		execute_all(
			&ds,
			&owner,
			"DEFINE TABLE thing SCHEMALESS PERMISSIONS FULL; CREATE thing:1 SET secret = 'v';",
		)
		.await?;
		let mut rec = Session::for_record(
			"first",
			"first",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "tester")),
		);
		rec.db = Some("other".to_owned());
		let err = execute_all(&ds, &rec, "UPDATE thing:1 SET secret = 'w'")
			.await
			.expect_err("record user expected to be denied in a sibling database");
		assert!(
			err.to_string().contains("database"),
			"expected a database authorization error, got: {err}"
		);
		// The victim data must be unchanged
		let owner = Session::owner().with_ns("second").with_db("second");
		let mut res = ds.execute("SELECT VALUE secret FROM thing", &owner, None).await?;
		let val = res.remove(0).result?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array!["v"]),
			"victim data was modified by an out-of-scope record user"
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_is_available_without_the_experimental_capability() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		// GQL is on by default: it must not be refused as an experimental
		// capability (any other error — e.g. missing namespace — is irrelevant).
		if let Err(err) =
			ds.run(QueryRequest::new(QuerySource::gql("MATCH (n:person) RETURN n"), &ses)).await
		{
			assert!(
				!err.to_string().contains("Experimental capability `gql` is not enabled"),
				"GQL was still gated: {err}"
			);
		}
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_text_parses_lowers_and_executes() -> Result<()> {
		use crate::dbs::capabilities::Targets;
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		execute_all(&ds, &ses, "CREATE person:tobie SET name = 'Tobie';").await?;
		// A valid GQL query parses, lowers, and executes through the
		// SurrealQL pipeline
		let mut res = ds
			.run(QueryRequest::new(
				QuerySource::gql("MATCH (n:person) RETURN n.name AS name"),
				&ses,
			))
			.await?;
		assert_eq!(res.len(), 1);
		let val = res.remove(0).result?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "Tobie" }]));
		// An invalid GQL query reports a parse error rather than a
		// capability error
		let err =
			ds.run(QueryRequest::new(QuerySource::gql("MATCH RETURN"), &ses)).await.unwrap_err();
		assert!(!err.to_string().contains("experimental"), "unexpected error: {err}");
		Ok(())
	}

	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn pgsql_text_parses_lowers_and_executes() -> Result<()> {
		use crate::dbs::capabilities::Targets;
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		execute_all(&ds, &ses, "CREATE person:tobie SET name = 'Tobie';").await?;

		let mut res = ds
			.run(QueryRequest::new(
				QuerySource::pgsql("SELECT name FROM person WHERE name = 'Tobie'"),
				&ses,
			))
			.await?;
		assert_eq!(res.len(), 1);
		let val = res.remove(0).result?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "Tobie" }]));

		let mut res =
			ds.run(QueryRequest::new(QuerySource::pgsql("SELECT 1 AS num;"), &ses)).await?;
		assert_eq!(res.len(), 1);
		let val = res.remove(0).result?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { num: 1 }]));
		Ok(())
	}

	#[cfg(feature = "mysql")]
	#[tokio::test]
	async fn mysql_text_parses_lowers_and_executes() -> Result<()> {
		use crate::dbs::capabilities::Targets;
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		execute_all(&ds, &ses, "CREATE person:tobie SET name = 'Tobie';").await?;

		let mut res = ds
			.run(QueryRequest::new(
				QuerySource::mysql("SELECT name FROM person WHERE name = 'Tobie'"),
				&ses,
			))
			.await?;
		assert_eq!(res.len(), 1);
		let val = res.remove(0).result?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "Tobie" }]));

		let mut res =
			ds.run(QueryRequest::new(QuerySource::mysql("SELECT 1 AS num;"), &ses)).await?;
		assert_eq!(res.len(), 1);
		let val = res.remove(0).result?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { num: 1 }]));
		Ok(())
	}

	/// `set_capabilities` must take effect on an already-built datastore without
	/// a rebuild: a capability-gated operation (scripting) flips from denied to
	/// allowed after a live swap, proving the executor observes the new
	/// `ArcSwap` snapshot.
	#[cfg(feature = "scripting")]
	#[tokio::test]
	async fn set_capabilities_swaps_live() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;

		// Default capabilities: scripting functions are denied.
		let mut before = ds.execute("RETURN function() { return 1; }", &ses, None).await?;
		assert!(
			before.remove(0).result.is_err(),
			"scripting should be denied under default capabilities"
		);

		// Swap in capabilities that enable scripting — no rebuild, same datastore.
		ds.set_capabilities(Capabilities::all())?;

		// The same datastore now permits scripting, proving the executor
		// observes the new `ArcSwap` snapshot.
		let mut after = ds.execute("RETURN function() { return 1; }", &ses, None).await?;
		assert!(
			after.remove(0).result.is_ok(),
			"scripting should be allowed after the capability swap"
		);
		Ok(())
	}

	/// A datastore with the `gql` capability and an initialised `test/test`
	/// namespace/database, for the GQL mutation tests.
	#[cfg(feature = "gql")]
	async fn gql_test_ds() -> Result<(Arc<Datastore>, Session)> {
		use crate::dbs::capabilities::Targets;
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().with_experimental(Targets::All))
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		Ok((ds, ses))
	}

	/// Run one GQL query, asserting a single statement result, and return it.
	#[cfg(feature = "gql")]
	async fn run_gql(ds: &Datastore, ses: &Session, query: &str) -> Result<PublicValue> {
		let mut res = ds.run(QueryRequest::new(QuerySource::gql(query), ses)).await?;
		assert_eq!(res.len(), 1, "expected one result for {query:?}");
		Ok(res.remove(0).result?)
	}

	/// Run one GQL query expecting failure (parse/lowering rejection or a
	/// per-statement execution error), returning the rendered error.
	#[cfg(feature = "gql")]
	async fn run_gql_err(ds: &Datastore, ses: &Session, query: &str) -> String {
		match ds.run(QueryRequest::new(QuerySource::gql(query), ses)).await {
			Ok(mut res) => match res.remove(0).result {
				Ok(value) => panic!("expected {query:?} to fail, got {value:?}"),
				Err(e) => e.to_string(),
			},
			Err(e) => e.to_string(),
		}
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_set_updates_and_returns_after_image() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A', age = 30;").await?;
		// SET returns the post-mutation value.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person) WHERE n.name = 'A' SET n.age = 31 RETURN n.age AS age",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { age: 31 }]));
		// And it persisted.
		let val = run_gql(&ds, &ses, "MATCH (n:person) RETURN n.age AS age").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { age: 31 }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_remove_unsets_field_and_returns_empty() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A', age = 30;").await?;
		// A mutation-only query returns an empty array.
		let val = run_gql(&ds, &ses, "MATCH (n:person) REMOVE n.age").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		// The field is gone: a filter on the old value does not match.
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) WHERE n.age = 30 RETURN n.name AS name").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		// The record itself survives.
		let val = run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "A" }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_delete_nodetach_errors_with_edges_then_detach_succeeds() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B'; \
			RELATE person:1->knows->person:2;",
		)
		.await?;
		// NODETACH (the default) on a node that still has edges errors.
		let err = run_gql_err(&ds, &ses, "MATCH (n:person) WHERE n.name = 'A' DELETE n").await;
		assert!(err.contains("connected edges"), "{err}");
		// The failed delete rolled back: A is still present.
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name ORDER BY name").await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A" },
				object! { name: "B" }
			])
		);
		// DETACH DELETE removes A and its edge.
		let val = run_gql(&ds, &ses, "MATCH (n:person) WHERE n.name = 'A' DETACH DELETE n").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name ORDER BY name").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "B" }]));
		// The knows edge is gone too.
		let val =
			run_gql(&ds, &ses, "MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_insert_node_and_edge() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		// A leading INSERT (no MATCH) creates a node exactly once.
		let val = run_gql(&ds, &ses, "INSERT (p:person {name: 'A'})").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		run_gql(&ds, &ses, "INSERT (p:person {name: 'B'})").await?;
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name ORDER BY name").await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A" },
				object! { name: "B" }
			])
		);
		// INSERT an edge between two MATCH-bound nodes.
		run_gql(
			&ds,
			&ses,
			"MATCH (a:person WHERE a.name = 'A') MATCH (b:person WHERE b.name = 'B') \
			INSERT (a)-[:knows]->(b)",
		)
		.await?;
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a, b.name AS b",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { a: "A", b: "B" }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_label_mutation_rejected() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A';").await?;
		let err = run_gql_err(&ds, &ses, "MATCH (n:person) SET n:Archived").await;
		assert!(err.contains("Label mutation is not supported"), "{err}");
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_set_all_properties_replaces_and_multi_item() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A', age = 30, city = 'L';").await?;
		// `SET n = {…}` replaces ALL user properties (age is dropped).
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person) SET n = {name: 'A2', city: 'X'} RETURN n.name AS name, n.city AS city",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![object! { name: "A2", city: "X" }])
		);
		// The dropped `age` does not match.
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) WHERE n.age = 30 RETURN n.name AS name").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		// Multi-item SET on distinct properties.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person) SET n.age = 5, n.city = 'Z' RETURN n.age AS age, n.city AS city",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { age: 5, city: "Z" }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_multi_statement_set_last_wins() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A', age = 1;").await?;
		// Two SET statements in one query: the second sees the first's write.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person) SET n.age = 5 SET n.age = n.age + 1 RETURN n.age AS age",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { age: 6 }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_edge_set_remove_delete() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B'; \
			RELATE person:1->knows->person:2 SET since = 2020;",
		)
		.await?;
		// SET a property on a bound edge.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:person)-[k:knows]->(b:person) SET k.since = 2099 RETURN k.since AS since",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { since: 2099 }]));
		// DELETE a bound edge (NODETACH default is fine: an edge has no sub-edges).
		let val = run_gql(&ds, &ses, "MATCH (a:person)-[k:knows]->(b:person) DELETE k").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		let val =
			run_gql(&ds, &ses, "MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a").await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_fanout_set_is_consistent_per_row() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A', age = 30; CREATE person:2 SET name = 'B'; \
			CREATE person:3 SET name = 'C'; RELATE person:1->knows->person:2; \
			RELATE person:1->knows->person:3;",
		)
		.await?;
		// `a` fans out to two rows; SET a.age = 7 must show a consistent AFTER image
		// on both rows (no stale pre-mutation binding is read on a duplicate).
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:person)-[:knows]->(b:person) WHERE a.name = 'A' SET a.age = 7 \
			RETURN a.age AS age, b.name AS b ORDER BY b",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { age: 7, b: "B" },
				object! { age: 7, b: "C" }
			])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_detach_delete_nulls_cascaded_edge_binding() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B'; \
			RELATE person:1->knows->person:2 SET since = 2020;",
		)
		.await?;
		// DETACH DELETE on `a` cascades the `k` edge; `k` is nulled in the
		// RETURN rather than surfacing the stale (deleted) edge.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:person)-[k:knows]->(b:person) WHERE a.name = 'A' DETACH DELETE a \
			RETURN a AS a, k AS k, b.name AS b",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![object! {
				a: PublicValue::Null,
				k: PublicValue::Null,
				b: "B"
			}])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_insert_left_edge_chain_and_repeated_anon() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B';")
			.await?;
		// A left-pointing INSERT edge relates b -> a.
		run_gql(
			&ds,
			&ses,
			"MATCH (a:person WHERE a.name = 'A') MATCH (b:person WHERE b.name = 'B') \
			INSERT (a)<-[:knows]-(b)",
		)
		.await?;
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (x:person)-[:knows]->(y:person) RETURN x.name AS x, y.name AS y",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { x: "B", y: "A" }]));
		// A chained INSERT creates three new nodes and two edges.
		run_gql(
			&ds,
			&ses,
			"INSERT (x:item {n: 1})-[:link]->(y:item {n: 2})-[:link]->(z:item {n: 3})",
		)
		.await?;
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:item)-[:link]->(b:item) RETURN a.n AS a, b.n AS b ORDER BY a",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { a: 1, b: 2 },
				object! { a: 2, b: 3 }
			])
		);
		// Repeated anonymous nodes in one INSERT create distinct records.
		run_gql(&ds, &ses, "INSERT (:tag {v: 1}), (:tag {v: 1})").await?;
		let val = run_gql(&ds, &ses, "MATCH (t:tag) RETURN t.v AS v ORDER BY v").await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![object! { v: 1 }, object! { v: 1 }])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_mutation_rejection_ledger() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		// Each lowers (or parses) to a precise rejection. No data needed — these
		// fail before execution.
		let cases: &[(&str, &str)] = &[
			// Mutating a variable not bound by the read body.
			("MATCH (n:person) SET x.age = 1", "Unknown variable"),
			// Mutating a path variable (a composite, not a record).
			("MATCH p = (a:person)-[:knows]->(b:person) SET p.x = 1", "group or path variable"),
			// `SET a = {…}` setting a reserved key.
			("MATCH (n:person) SET n = {id: 1}", "reserved `id` key"),
			// Per-property SET of a reserved edge endpoint (the native write path
			// would otherwise silently re-stamp `out`, so it is rejected up front).
			("MATCH (a:person)-[k:knows]->(b:person) SET k.out = 1", "reserved `out` key"),
			// Label mutation via REMOVE.
			("MATCH (n:person) REMOVE n:Foo", "Label mutation is not supported"),
			// INSERT re-declaring a MATCH-bound variable as a new (labelled) node.
			("MATCH (a:person) INSERT (a:thing)", "already bound"),
			// INSERT node that is neither labelled nor a bound-variable reference.
			("INSERT (x)", "must declare a label"),
			// Undirected INSERT edge.
			("INSERT (a:person)~[:knows]~(b:person)", "Undirected INSERT edges"),
			// DELETE of a non-variable expression.
			("MATCH (n:person) DELETE n.age", "bound variable"),
		];
		for (query, expected) in cases {
			let err = run_gql_err(&ds, &ses, query).await;
			assert!(err.contains(expected), "query {query:?}: expected {expected:?}, got: {err}");
		}
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_mutation_respects_record_permissions() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		// Owner: a table that denies create/update (but allows select), seeded
		// with one row.
		execute_all(
			&ds,
			&ses,
			"DEFINE TABLE person SCHEMALESS \
			PERMISSIONS FOR select FULL, FOR create NONE, FOR update NONE, FOR delete NONE; \
			CREATE person:1 SET name = 'A', age = 30;",
		)
		.await?;
		// A record-scoped session: table PERMISSIONS clauses apply (a role-based
		// system session would bypass them, so this is the meaningful test).
		let rec = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "tester")),
		);
		// Each write goes through the native document pipeline, so each is denied:
		// INSERT (create), SET (update), DELETE (delete) all leave the row intact.
		let _ = ds
			.run(QueryRequest::new(QuerySource::gql("INSERT (p:person {name: 'B'})"), &rec))
			.await;
		let _ = ds
			.run(QueryRequest::new(QuerySource::gql("MATCH (n:person) SET n.age = 99"), &rec))
			.await;
		let _ = ds
			.run(QueryRequest::new(QuerySource::gql("MATCH (n:person) DETACH DELETE n"), &rec))
			.await;
		// As owner: exactly the original row remains, unchanged.
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name, n.age AS age").await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![object! { name: "A", age: 30 }])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_nodetach_probe_ignores_select_permissions() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		// `person` is fully visible/deletable to a record session, but the `knows`
		// edge table hides SELECT from it. Seed a node with one connected edge.
		execute_all(
			&ds,
			&ses,
			"DEFINE TABLE person SCHEMALESS \
			PERMISSIONS FOR select FULL, FOR create FULL, FOR update FULL, FOR delete FULL; \
			DEFINE TABLE knows SCHEMALESS \
			PERMISSIONS FOR select NONE, FOR create FULL, FOR update FULL, FOR delete FULL; \
			CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B'; \
			RELATE person:1->knows->person:2;",
		)
		.await?;
		let rec = Session::for_record(
			"test",
			"test",
			"user",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "tester")),
		);
		// The NODETACH (default) connected-edge probe runs with permissions
		// DISABLED, so it sees the `knows` edge the record session cannot SELECT and
		// errors — rather than passing the guard and letting the native DELETE
		// silently cascade an edge the caller opted out of removing.
		let err = run_gql_err(&ds, &rec, "MATCH (n:person WHERE n.name = 'A') DELETE n").await;
		assert!(err.contains("connected edges"), "{err}");
		// The node (and its edge) survive the rejected delete.
		let val =
			run_gql(&ds, &ses, "MATCH (n:person) RETURN n.name AS name ORDER BY name").await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A" },
				object! { name: "B" }
			])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_match_after_set_rereads_live_state() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A', tier = 'bronze'; \
			CREATE person:2 SET name = 'B', tier = 'gold';",
		)
		.await?;
		// A read step after a write re-scans the live (post-write) state in the same
		// transaction: promoting A to gold and then MATCHing on `tier = 'gold'` finds
		// both A (just written) and B.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person WHERE n.name = 'A') SET n.tier = 'gold' \
			MATCH (m:person WHERE m.tier = 'gold') RETURN m.name AS name ORDER BY name",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A" },
				object! { name: "B" }
			])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_match_after_delete_rereads_live_state() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B';")
			.await?;
		// The trailing MATCH does not observe the row the DELETE removed.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person WHERE n.name = 'A') DETACH DELETE n \
			MATCH (m:person) RETURN m.name AS name ORDER BY name",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { name: "B" }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_match_after_insert_sees_new_node_and_anchors_on_binding() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A', age = 20;").await?;
		// A trailing MATCH re-scans and observes the just-inserted node, and can join
		// on the binding the INSERT created (`a.age` anchors the predicate on `b`).
		let val = run_gql(
			&ds,
			&ses,
			"INSERT (a:person {name: 'New', age: 20}) \
			MATCH (b:person WHERE b.age = a.age) RETURN b.name AS name ORDER BY name",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A" },
				object! { name: "New" }
			])
		);
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_optional_match_after_set_rereads_live_state() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A'; CREATE person:2 SET name = 'B';")
			.await?;
		// An OPTIONAL block after a write takes the general left-join path (the
		// mutating accumulator is the join's probe side, the OPTIONAL read is its
		// build side). The OPTIONAL must still observe the write: after promoting A
		// to gold, `OPTIONAL MATCH (m:person WHERE m.tier = 'gold')` matches A (just
		// written), so `m` is bound, NOT null-filled.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person WHERE n.name = 'A') SET n.tier = 'gold' \
					OPTIONAL MATCH (m:person WHERE m.tier = 'gold') \
					RETURN n.name AS n, m.name AS m",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { n: "A", m: "A" }]));
		Ok(())
	}

	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_optional_match_after_insert_sees_new_node() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(&ds, &ses, "CREATE person:1 SET name = 'A';").await?;
		// An OPTIONAL block after an INSERT must see the just-created node. The
		// INSERT runs once per matched `person`; the trailing OPTIONAL re-scans
		// `tag` and binds `m` to the new node rather than null-filling it.
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (a:person WHERE a.name = 'A') INSERT (t:tag {name: 'X'}) \
			OPTIONAL MATCH (m:tag) \
			RETURN m.name AS m",
		)
		.await?;
		assert_eq!(val, PublicValue::Array(surrealdb_types::array![object! { m: "X" }]));
		Ok(())
	}

	/// The writes of a MATCH mutation are compiled with the request that holds
	/// it: running one compiles nothing, and serves no table's stage bundle at
	/// run time, for a write whose target table the plan names.
	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_mutation_writes_compile_at_arrival() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		let mut setup = String::from(
			"DEFINE TABLE person SCHEMALESS; \
			DEFINE FIELD age ON person TYPE option<int> ASSERT $value = NONE OR $value > 0; \
			DEFINE INDEX person_name ON person FIELDS name UNIQUE; \
			DEFINE EVENT person_updated ON person WHEN $event = 'UPDATE' \
			THEN (CREATE audit SET who = $after.name);",
		);
		for i in 1..=50 {
			setup.push_str(&format!("CREATE person:{i} SET name = 'p{i}', age = {i};"));
		}
		execute_all(&ds, &ses, &setup).await?;
		for query in [
			"MATCH (n:person) SET n.age = 1",
			"MATCH (n:person WHERE n.name = 'p1') DETACH DELETE n",
			"INSERT (m:pet {name: 'x'})",
		] {
			let after = crate::exec::compiles::after_arrival();
			let faults = crate::exec::compiles::faults();
			run_gql(&ds, &ses, query).await?;
			assert_eq!(
				crate::exec::compiles::after_arrival() - after,
				0,
				"{query:?} compiled after it arrived"
			);
			assert_eq!(
				crate::exec::compiles::faults() - faults,
				0,
				"{query:?} served a compile at run time"
			);
		}
		Ok(())
	}

	/// The parameters a mutation's write reads its record and values from are
	/// the step's own: a request variable of the same name redirects nothing.
	#[cfg(feature = "gql")]
	#[tokio::test]
	async fn gql_mutation_params_are_not_client_settable() -> Result<()> {
		let (ds, ses) = gql_test_ds().await?;
		execute_all(
			&ds,
			&ses,
			"CREATE person:1 SET name = 'A', age = 1; CREATE person:2 SET name = 'B', age = 2;",
		)
		.await?;
		let vars = PublicVariables::from(BTreeMap::from_iter([(
			"__mutation_record".to_owned(),
			PublicValue::RecordId(surrealdb_types::RecordId::new("person", 2)),
		)]));
		let request = QueryRequest::new(
			QuerySource::gql("MATCH (n:person WHERE n.name = 'A') SET n.age = 7"),
			&ses,
		)
		.with_variables(Some(vars));
		for result in ds.run(request).await? {
			result.result?;
		}
		let val = run_gql(
			&ds,
			&ses,
			"MATCH (n:person) RETURN n.name AS name, n.age AS age ORDER BY name",
		)
		.await?;
		assert_eq!(
			val,
			PublicValue::Array(surrealdb_types::array![
				object! { name: "A", age: 7 },
				object! { name: "B", age: 2 }
			])
		);
		Ok(())
	}

	async fn index_key_base(ds: &Datastore, table: &str, index: &str) -> Result<IndexKeyBase> {
		let txn = ds.transaction(Read).await?;
		let ns = txn.get_ns_by_name("test", None).await?.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await?.unwrap();
		let table = TableName::from(table);
		let ix =
			txn.get_tb_index(ns.namespace_id, db.database_id, &table, index, None).await?.unwrap();
		txn.cancel().await?;
		Ok(IndexKeyBase::new(ns.namespace_id, db.database_id, table, ix.index_id))
	}

	async fn assert_index_compaction_commit_retry(
		site: RetryableConflictSite,
		table: &str,
		index: &str,
		sql: &str,
	) -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, sql).await?;
		let ikb = index_key_base(&ds, table, index).await?;
		let node_id = ds.id();
		let _guard = inject_retryable_conflict(site, node_id);

		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;

		assert_eq!(retryable_conflict_count(site, node_id), 0);
		Ok(())
	}

	const COUNT_COMPACTION_SQL: &str = "
		DEFINE TABLE user SCHEMALESS;
	DEFINE INDEX count_idx ON user COUNT;
	CREATE user:1 SET name = 'one' RETURN NONE;
	CREATE user:2 SET name = 'two' RETURN NONE;
	";

	const FULLTEXT_COMPACTION_SQL: &str = "
		DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
	DEFINE TABLE doc SCHEMALESS;
	DEFINE INDEX ft_idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
	CREATE doc:1 SET text = 'alpha beta' RETURN NONE;
	CREATE doc:2 SET text = 'beta gamma' RETURN NONE;
	";

	const HNSW_COMPACTION_SQL: &str = "
		DEFINE TABLE vec SCHEMALESS;
	DEFINE INDEX hnsw_idx ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
	CREATE vec:1 SET vector = [1, 2] RETURN NONE;
	CREATE vec:2 SET vector = [2, 3] RETURN NONE;
	";

	#[tokio::test]
	async fn count_index_compaction_retries_commit_conflict() -> Result<()> {
		assert_index_compaction_commit_retry(
			RetryableConflictSite::CountCompaction,
			"user",
			"count_idx",
			COUNT_COMPACTION_SQL,
		)
		.await
	}

	#[tokio::test]
	async fn fulltext_index_compaction_retries_commit_conflict() -> Result<()> {
		assert_index_compaction_commit_retry(
			RetryableConflictSite::FullTextCompaction,
			"doc",
			"ft_idx",
			FULLTEXT_COMPACTION_SQL,
		)
		.await
	}

	#[tokio::test]
	async fn hnsw_index_compaction_retries_commit_conflict() -> Result<()> {
		assert_index_compaction_commit_retry(
			RetryableConflictSite::HnswCompaction,
			"vec",
			"hnsw_idx",
			HNSW_COMPACTION_SQL,
		)
		.await
	}

	#[cfg(diskann)]
	const DISKANN_COMPACTION_SQL: &str = "
		DEFINE TABLE dvec SCHEMALESS;
	DEFINE INDEX diskann_idx ON dvec FIELDS vector DISKANN DIMENSION 2 DIST EUCLIDEAN TYPE F32;
	CREATE dvec:1 SET vector = [1, 2] RETURN NONE;
	CREATE dvec:2 SET vector = [2, 3] RETURN NONE;
	";

	/// The `!ic` compaction requests queued for one index.
	async fn index_compaction_queue_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let range = IndexCompactionIxPrefix {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?;
		let txn = ds.transaction(Read).await?;
		let res = txn.keys(range, u32::MAX, 0, None).await;
		txn.cancel().await?;
		Ok(res?.len())
	}

	/// The `!iu` deltas a count compaction still has to fold: every entry but the
	/// aggregate at the `None` bound.
	async fn count_delta_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let range = IndexCountPrefix {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?;
		let txn = ds.transaction(Read).await?;
		let res = txn.keys(range, u32::MAX, 0, None).await;
		txn.cancel().await?;
		let mut deltas = 0;
		for key in res? {
			if IndexCountKey::decode_key(&key)?.uid.is_some() {
				deltas += 1;
			}
		}
		Ok(deltas)
	}

	/// The `!tx` and `!dx` deltas a full-text compaction still has to fold.
	async fn fulltext_delta_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let terms = TermChangeBatchPrefix {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?;
		let stats = DocStatsBatchPrefix {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?;
		let txn = ds.transaction(Read).await?;
		let terms = txn.keys(terms, u32::MAX, 0, None).await;
		let stats = txn.keys(stats, u32::MAX, 0, None).await;
		txn.cancel().await?;
		Ok(terms?.len() + stats?.len())
	}

	/// The `!dw` pendings a DiskANN compaction still has to fold, across every
	/// shard.
	#[cfg(diskann)]
	async fn diskann_pending_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let mut pendings = 0;
		for shard in 0..DISKANN_PENDING_STATE_SHARDS {
			let range = DiskannRecordPendingShardShardPrefix {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
				shard,
			}
			.range()?;
			pendings += catch!(txn, txn.keys(range, u32::MAX, 0, None).await).len();
		}
		txn.cancel().await?;
		Ok(pendings)
	}

	/// The kind of every `!dy` guard a DiskANN index has written.
	#[cfg(diskann)]
	async fn diskann_guard_kinds(
		ds: &Datastore,
		ikb: &IndexKeyBase,
	) -> Result<Vec<DiskAnnPendingStateKind>> {
		let range =
			DiskannPendingPrefix::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()), ikb.index())
				.range()?;
		let txn = ds.transaction(Read).await?;
		let guards: Result<Vec<(Vec<u8>, DiskAnnPendingState)>> =
			txn.scan(range, DISKANN_PENDING_STATE_SHARDS.into(), 0, None).await;
		txn.cancel().await?;
		Ok(guards?.into_iter().map(|(_, state)| state.kind).collect())
	}

	/// Writes `deltas` count deltas of `+1` straight under an index, with one
	/// compaction request for them, as that many transactions' flushes would.
	async fn write_count_deltas(ds: &Datastore, ikb: &IndexKeyBase, deltas: usize) -> Result<()> {
		let txn = ds.transaction(Write).await?;
		for n in 0..deltas {
			let key = IndexCountKey {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
				uid: Some((ds.id(), Uuid::from_u128(n as u128 + 1))),
				pos: true,
				count: 1,
			};
			catch!(txn, txn.set_key(&key, &()).await);
		}
		catch!(txn, txn.set_key(&ikb.new_ic_key(ds.id()), &()).await);
		txn.commit().await
	}

	/// Runs the batch at the head of the queue through the passes and the
	/// cleanup [`Datastore::index_compaction`] runs every batch through,
	/// returning the batch's keys and the number of passes that failed.
	async fn run_one_index_compaction_batch(ds: &Arc<Datastore>) -> Result<(Vec<Vec<u8>>, usize)> {
		let batch = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		let lh = LeaseHandler::new(
			ds.sequences.clone(),
			ds.id(),
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(2),
		)?;
		let cleanup = Datastore::index_compaction_batch(
			Arc::clone(ds),
			&lh,
			&batch,
			&CompactionDrain::default(),
			&CancellationToken::new(),
		)
		.await?
		.expect("the batch must hold the lease");
		assert!(cleanup.deleted, "the batch's cleanup must commit");
		Ok((batch, cleanup.errors))
	}

	/// Whether every one of `keys` is still stored.
	async fn all_keys_remain(ds: &Datastore, keys: &[Vec<u8>]) -> Result<bool> {
		let txn = ds.transaction(Read).await?;
		for key in keys {
			if catch!(txn, txn.get(Key::from(key), None).await).is_none() {
				txn.cancel().await?;
				return Ok(false);
			}
		}
		txn.cancel().await?;
		Ok(true)
	}

	/// Whether any of `keys` is still stored.
	async fn any_key_remains(ds: &Datastore, keys: &[Vec<u8>]) -> Result<bool> {
		let txn = ds.transaction(Read).await?;
		for key in keys {
			if catch!(txn, txn.get(Key::from(key), None).await).is_some() {
				txn.cancel().await?;
				return Ok(true);
			}
		}
		txn.cancel().await?;
		Ok(false)
	}

	/// DiskANN counterpart of [`hnsw_index_compaction_retries_commit_conflict`].
	/// Its apply commits its own transaction, so the conflict is raised in place
	/// of the apply and arrives as the apply's own error.
	#[cfg(diskann)]
	#[tokio::test]
	async fn diskann_index_compaction_retries_commit_conflict() -> Result<()> {
		const INJECTED: usize = 3;
		const _: () = assert!(INJECTED <= INDEX_COMPACTION_PASS_MAX_RETRIES);
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, DISKANN_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "dvec", "diskann_idx").await?;
		assert_eq!(diskann_pending_len(&ds, &ikb).await?, 2, "each record leaves one pending");
		let site = RetryableConflictSite::DiskAnnCompaction;
		let node_id = ds.id();
		let _guard = inject_retryable_conflicts(site, node_id, INJECTED);

		let (_, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;

		assert_eq!(errors, 0, "the pass must retry its way past every injected conflict");
		assert_eq!(
			retryable_conflict_count(site, node_id),
			0,
			"the pass must have met all {INJECTED} injected conflicts"
		);
		assert_eq!(
			diskann_pending_len(&ds, &ikb).await?,
			0,
			"the retried apply folds every pending"
		);
		let guards = diskann_guard_kinds(&ds, &ikb).await?;
		assert!(!guards.is_empty(), "the writes must have raised their shards' guards");
		assert!(
			guards.iter().all(|kind| *kind != DiskAnnPendingStateKind::NonEmpty),
			"no shard may still report pendings to merge: {guards:?}"
		);
		Ok(())
	}

	/// A pass over more than one budget of HNSW pendings applies exactly one
	/// budget of plans, requeues its index once, and leaves the rest to the next
	/// pass, which picks up where it stopped.
	#[tokio::test(flavor = "multi_thread")]
	async fn hnsw_index_compaction_pass_requeues_the_rest_of_its_budget() -> Result<()> {
		let budget = INDEX_COMPACTION_PASS_PLAN_BUDGET * HNSW_COMPACTION_MAX_PENDING_KEYS;
		let backlog = budget + 1;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE TABLE vec SCHEMALESS;
				 DEFINE INDEX hnsw_idx ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
				 CREATE |vec:1..={backlog}| SET vector = [rand::float(), rand::float()] RETURN NONE;"
			),
		)
		.await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, backlog, "one pending per record");
		let queued = index_compaction_queue_len(&ds, &ikb).await?;

		let outcome = ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(outcome, CompactionPassOutcome::Budget);
		assert_eq!(
			hnsw_pending_len(&ds, &ikb).await?,
			backlog - budget,
			"the pass must fold exactly one budget of pendings"
		);
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			queued + 1,
			"the pass must leave exactly one fresh request for its index"
		);

		let outcome = ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(outcome, CompactionPassOutcome::Short);
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 0, "the next pass folds the remainder");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			queued + 1,
			"a pass that folds its whole backlog queues nothing"
		);
		Ok(())
	}

	/// A plan that fills its cap reports more work behind it even when there is
	/// none, so a pass over exactly one full plan of pendings applies it, finds
	/// the next plan empty, and ends short of its budget: the backlog is folded
	/// and nothing is requeued.
	#[tokio::test(flavor = "multi_thread")]
	async fn hnsw_index_compaction_pass_ending_on_an_empty_plan_is_short() -> Result<()> {
		let backlog = HNSW_COMPACTION_MAX_PENDING_KEYS;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE TABLE vec SCHEMALESS;
				 DEFINE INDEX hnsw_idx ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
				 CREATE |vec:1..={backlog}| SET vector = [rand::float(), rand::float()] RETURN NONE;"
			),
		)
		.await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, backlog);
		let queued = index_compaction_queue_len(&ds, &ikb).await?;

		let outcome = ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(outcome, CompactionPassOutcome::Short);
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 0);
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, queued, "nothing is requeued");
		Ok(())
	}

	/// [`hnsw_index_compaction_pass_requeues_the_rest_of_its_budget`] for the
	/// delta-log plan shape.
	#[tokio::test]
	async fn count_index_compaction_pass_requeues_the_rest_of_its_budget() -> Result<()> {
		let budget = INDEX_COMPACTION_PASS_PLAN_BUDGET * COUNT_BATCH_SIZE as usize;
		let backlog = budget + 1;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;",
		)
		.await?;
		let ikb = index_key_base(&ds, "item", "idx_total").await?;
		write_count_deltas(&ds, &ikb, backlog).await?;
		assert_eq!(count_delta_len(&ds, &ikb).await?, backlog);
		let queued = index_compaction_queue_len(&ds, &ikb).await?;

		let outcome = ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(outcome, CompactionPassOutcome::Budget);
		assert_eq!(
			count_delta_len(&ds, &ikb).await?,
			backlog - budget,
			"the pass must fold exactly one budget of deltas"
		);
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			queued + 1,
			"the pass must leave exactly one fresh request for its index"
		);

		let outcome = ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(outcome, CompactionPassOutcome::Short);
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0, "the next pass folds the remainder");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, queued + 1);
		let mut res = ds.execute("SELECT count() FROM item GROUP ALL", &session, None).await?;
		assert_eq!(
			res.remove(0).result?,
			PublicValue::Array(surrealdb_types::array![object! { count: backlog as i64 }]),
			"every delta must be folded exactly once"
		);
		Ok(())
	}

	/// A drain whose pass stops at its budget goes on with the request that
	/// pass queued, rather than ending as a drain of a short queue does, so one
	/// call folds a backlog several budgets deep, on a sole writer too.
	#[tokio::test]
	async fn index_compaction_drains_a_budgeted_backlog_in_one_call() -> Result<()> {
		let budget = INDEX_COMPACTION_PASS_PLAN_BUDGET * COUNT_BATCH_SIZE as usize;
		let backlog = 2 * budget + 1;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;",
		)
		.await?;
		let ikb = index_key_base(&ds, "item", "idx_total").await?;
		write_count_deltas(&ds, &ikb, backlog).await?;
		{
			let txn = ds.transaction(Read).await?;
			let sole_writer = txn.sole_writer();
			txn.cancel().await?;
			assert!(sole_writer, "the fixture must be a sole writer, where a short batch ends");
		}

		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0, "one call must fold the whole backlog");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 0, "and leave nothing queued");
		Ok(())
	}

	/// One queue batch names a large index and a small one. The large index's
	/// pass stops at its budget, so the batch ends without waiting for its whole
	/// backlog: the small index is folded, the large one is requeued once, and
	/// the batch's own keys are gone.
	#[tokio::test]
	async fn index_compaction_batch_serves_a_small_index_beside_a_budgeted_one() -> Result<()> {
		let budget = INDEX_COMPACTION_PASS_PLAN_BUDGET * COUNT_BATCH_SIZE as usize;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;
			 DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			 DEFINE TABLE doc SCHEMALESS;
			 DEFINE INDEX ft_idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25;
			 CREATE doc:1 SET text = 'alpha beta' RETURN NONE;
			 CREATE doc:2 SET text = 'beta gamma' RETURN NONE;",
		)
		.await?;
		let large = index_key_base(&ds, "item", "idx_total").await?;
		let small = index_key_base(&ds, "doc", "ft_idx").await?;
		write_count_deltas(&ds, &large, budget + 1).await?;
		assert!(fulltext_delta_len(&ds, &small).await? > 0, "the small index must have a backlog");

		let (batch, errors) = run_one_index_compaction_batch(&ds).await?;
		assert_eq!(errors, 0);
		let named: HashSet<IndexKeyBase> = batch
			.iter()
			.map(|key| {
				let ic = IndexCompactionKey::decode_key(key)?;
				Ok(IndexKeyBase::new(ic.ns, ic.db, ic.tb.into_owned(), ic.ix))
			})
			.collect::<Result<_>>()?;
		assert_eq!(named, HashSet::from([large.clone(), small.clone()]), "one batch names both");

		assert_eq!(fulltext_delta_len(&ds, &small).await?, 0, "the small index must be folded");
		assert_eq!(
			index_compaction_queue_len(&ds, &large).await?,
			1,
			"the large index must be requeued exactly once"
		);
		assert_eq!(index_compaction_queue_len(&ds, &small).await?, 0);
		assert!(!any_key_remains(&ds, &batch).await?, "the batch's own keys must be gone");
		Ok(())
	}

	/// The lease the compaction task drains the queue under outlasts a pass
	/// that runs longer than the lease itself, and no other node can take it
	/// meanwhile.
	///
	/// The pass is parked from before the lease it started under would have
	/// expired until after, and the test reads ownership, not time: a peer
	/// sharing the store asks for the lease throughout and never gets it, and
	/// the lease record still names this node when the pass resumes.
	#[tokio::test(flavor = "multi_thread")]
	async fn index_compaction_keeps_its_lease_while_a_pass_runs() -> Result<()> {
		async fn lease_record(ds: &Datastore) -> Result<crate::catalog::TaskLease> {
			let key = TaskLeaseKey {
				task_id: TaskLeaseType::IndexCompaction,
			};
			let txn = ds.transaction(Read).await?;
			let lease = txn.get_key(&key, None).await;
			txn.cancel().await?;
			Ok(lease?.expect("the compaction task holds a lease"))
		}

		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let interval = Duration::from_secs(1);
		let peer = ds.fork_for_test_with_node_id(Uuid::new_v4());
		let peer_lease = LeaseHandler::new(
			peer.sequences.clone(),
			peer.id(),
			peer.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			interval * 2,
		)?;

		let park = compaction_pass_park::park(ds.id());
		let compaction = tokio::spawn(Datastore::index_compaction(
			Arc::clone(&ds),
			interval,
			CancellationToken::new(),
		));
		tokio::time::timeout(Duration::from_secs(30), park.parked())
			.await
			.expect("the batch's pass must reach the park");
		let started_under = lease_record(&ds).await?;
		assert_eq!(started_under.owner, ds.id());

		// Past the expiry of the lease the pass started under, a lease nobody
		// renewed would be the peer's for the asking.
		while Utc::now() <= started_under.expiration + chrono::Duration::seconds(1) {
			assert!(
				!peer_lease.has_lease().await?,
				"a peer took the compaction lease while this node's pass was running"
			);
			tokio::time::sleep(Duration::from_millis(250)).await;
		}
		let resumed_under = lease_record(&ds).await?;
		assert_eq!(resumed_under.owner, ds.id(), "the lease must still name this node");
		assert!(resumed_under.expiration > Utc::now(), "the lease must still be live");

		park.release();
		let (_, errors) = compaction.await??;
		assert_eq!(errors, 0);
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0, "the resumed pass folds its backlog");
		Ok(())
	}

	/// A pass that finds its lease lost at a plan boundary stops there, folding
	/// nothing more, and requeues its index for whichever node holds the lease
	/// now.
	#[tokio::test]
	async fn index_compaction_pass_stops_at_a_plan_boundary_once_its_lease_is_lost() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let deltas = count_delta_len(&ds, &ikb).await?;
		assert!(deltas > 0, "the fixture must leave deltas for a pass to fold");
		let queued = index_compaction_queue_len(&ds, &ikb).await?;
		let lease = CompactionLease::new(&LeaseHandler::new(
			ds.sequences.clone(),
			ds.id(),
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(2),
		)?);
		lease.lost.store(true, std::sync::atomic::Ordering::Release);

		let report = ds
			.run_index_compaction_pass(
				&ikb,
				&CancellationToken::new(),
				Some(&lease),
				&ds.compaction_places(),
			)
			.await;
		assert_eq!(report.result?, CompactionPassOutcome::Budget);
		assert!(!report.unqueued, "the fresh request must have committed");
		assert_eq!(count_delta_len(&ds, &ikb).await?, deltas, "the pass must fold nothing more");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			queued + 1,
			"the pass must leave exactly one fresh request for its index"
		);
		Ok(())
	}

	/// A plan overtaken by another compactor is not the end of the backlog: the
	/// pass reads a fresh plan and folds what is left.
	#[tokio::test]
	async fn index_compaction_pass_replans_after_a_lost_race() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		assert!(count_delta_len(&ds, &ikb).await? > 0, "the fixture must leave deltas to fold");
		overtake_count_compaction_applies(&ds, &ikb, 1);

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Short
		);
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0, "the fresh plan folds the backlog");
		Ok(())
	}

	/// A pass that loses the race on every retry it has ends on the lost race
	/// with its backlog unfolded, so it queues a fresh request for its index,
	/// which the batch's cleanup leaves in place; a later pass folds the
	/// backlog.
	#[tokio::test]
	async fn index_compaction_requeues_an_index_whose_pass_keeps_losing_the_race() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let deltas = count_delta_len(&ds, &ikb).await?;
		assert!(deltas > 0, "the fixture must leave deltas to fold");
		assert!(index_compaction_queue_len(&ds, &ikb).await? > 0, "the fixture queues a request");
		overtake_count_compaction_applies(&ds, &ikb, INDEX_COMPACTION_PASS_MAX_RETRIES + 1);

		let (_, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(count_delta_len(&ds, &ikb).await?, deltas, "every apply lost its race");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			1,
			"the batch must leave exactly the fresh request its pass queued"
		);

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Short
		);
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0);
		Ok(())
	}

	/// A pass that stops with work left and whose fresh request fails to
	/// commit says so, for the batch that ran it to keep the requests that led
	/// to it.
	#[tokio::test]
	async fn index_compaction_pass_reports_a_request_it_could_not_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let queued = index_compaction_queue_len(&ds, &ikb).await?;
		let lease = CompactionLease::new(&LeaseHandler::new(
			ds.sequences.clone(),
			ds.id(),
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(2),
		)?);
		lease.lost.store(true, std::sync::atomic::Ordering::Release);
		let _guard =
			inject_non_retryable_error(NonRetryableErrorSite::IndexCompactionRequeue, ds.id());

		let report = ds
			.run_index_compaction_pass(
				&ikb,
				&CancellationToken::new(),
				Some(&lease),
				&ds.compaction_places(),
			)
			.await;
		assert_eq!(report.result?, CompactionPassOutcome::Budget);
		assert!(report.unqueued, "a request that did not commit must be reported");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, queued, "nothing was queued");
		Ok(())
	}

	/// A batch that finds the lease held by another node as it starts runs no
	/// pass, queues nothing and deletes nothing: its requests are left for the
	/// node that holds the lease.
	#[tokio::test]
	async fn index_compaction_batch_leaves_its_requests_to_the_lease_holder() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let indexes = [
			index_key_base(&ds, "user", "count_idx").await?,
			index_key_base(&ds, "vec", "hnsw_idx").await?,
		];
		let queued = || async {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res
		};
		let keys = queued().await?;
		for ikb in &indexes {
			assert!(index_compaction_queue_len(&ds, ikb).await? > 0, "{ikb} must be queued");
		}
		let lease = |node: Uuid| {
			LeaseHandler::new(
				ds.sequences.clone(),
				node,
				ds.transaction_factory.clone(),
				TaskLeaseType::IndexCompaction,
				Duration::from_secs(60),
			)
		};
		assert!(lease(Uuid::new_v4())?.has_lease().await?, "the peer must take the lease");

		let cleanup = Datastore::index_compaction_batch(
			Arc::clone(&ds),
			&lease(ds.id())?,
			&keys,
			&CompactionDrain::default(),
			&CancellationToken::new(),
		)
		.await?;
		assert!(cleanup.is_none(), "a batch without the lease must stop before its passes");
		for ikb in &indexes {
			assert_eq!(compaction_pass_park::passes(ds.id(), ikb), 0, "no pass may run: {ikb}");
		}
		assert_eq!(queued().await?, keys, "the queue must hold exactly the batch's requests");
		Ok(())
	}

	/// A pass the batch runs on its own task that panics fails the batch with
	/// the panic, as one on a helper does, rather than unwinding the compactor,
	/// and the batch's requests stay queued.
	#[tokio::test(flavor = "multi_thread")]
	async fn index_compaction_batch_fails_on_a_panic_in_a_pass_it_runs_itself() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		assert!(count_delta_len(&ds, &ikb).await? > 0, "the pass must have work");
		let node = ds.id();
		let hook: BeforeCountApplyHook =
			Arc::new(|| Box::pin(async { panic!("a compaction pass panics") }));
		BEFORE_COUNT_APPLY.lock().unwrap().insert(node, (1, hook));
		let keys = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		assert!(!keys.is_empty(), "the fixture must queue requests");
		let lh = LeaseHandler::new(
			ds.sequences.clone(),
			node,
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(60),
		)?;

		// One index, so its pass runs on the batch's own task.
		let res = Datastore::index_compaction_batch(
			Arc::clone(&ds),
			&lh,
			&keys,
			&CompactionDrain::default(),
			&CancellationToken::new(),
		)
		.await;
		let err = res.err().expect("the panicked pass must fail the batch");
		assert!(
			err.to_string().contains("a compaction pass panics"),
			"the batch must return the panic: {err}"
		);
		assert!(all_keys_remain(&ds, &keys).await?, "the batch's requests must stay queued");
		Ok(())
	}

	/// A batch whose pass panics still awaits every other pass it started
	/// before it returns the failure, so none of them runs on past the batch's
	/// cleanup and its lease.
	#[tokio::test(flavor = "multi_thread")]
	async fn index_compaction_batch_awaits_every_pass_when_one_panics() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE zz SCHEMALESS;
			 DEFINE INDEX cz ON zz COUNT;
			 CREATE zz:1 RETURN NONE;
			 CREATE zz:2 RETURN NONE;",
		)
		.await?;
		// The queue names `user` before `zz`, so the parked pass runs on the
		// batch's own task and the panicking one on a helper, granted a place
		// of the node's pool while the parked pass waits.
		let parked = index_key_base(&ds, "user", "count_idx").await?;
		assert!(count_delta_len(&ds, &parked).await? > 0, "the parked pass must have work");
		let node = ds.id();
		// The second pass runs on a helper, which needs a place to start on.
		let _places = own_compaction_places(node, 4);
		let panicked = Arc::new(tokio::sync::Notify::new());
		let hook: BeforeCountApplyHook = {
			let panicked = Arc::clone(&panicked);
			Arc::new(move || {
				let panicked = Arc::clone(&panicked);
				Box::pin(async move {
					panicked.notify_one();
					panic!("a compaction pass panics");
				})
			})
		};
		BEFORE_COUNT_APPLY.lock().unwrap().insert(node, (1, hook));
		let park = compaction_pass_park::park_index(node, parked.clone());

		let keys = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		let lh = LeaseHandler::new(
			ds.sequences.clone(),
			node,
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(60),
		)?;
		let batch_ds = Arc::clone(&ds);
		let mut batch = tokio::spawn(async move {
			Datastore::index_compaction_batch(
				batch_ds,
				&lh,
				&keys,
				&CompactionDrain::default(),
				&CancellationToken::new(),
			)
			.await
			.map(|cleanup| cleanup.map(|cleanup| cleanup.errors))
		});
		tokio::time::timeout(Duration::from_secs(30), park.parked())
			.await
			.expect("the parked pass must reach the park");
		tokio::time::timeout(Duration::from_secs(30), panicked.notified())
			.await
			.expect("the other pass must panic");
		assert!(
			tokio::time::timeout(Duration::from_millis(500), &mut batch).await.is_err(),
			"the batch must not return while a pass it started is still running"
		);

		park.release();
		let res = tokio::time::timeout(Duration::from_secs(30), batch)
			.await
			.expect("the batch must return once its passes have")?;
		assert!(res.is_err(), "the panicked pass must fail the batch");
		assert_eq!(
			count_delta_len(&ds, &parked).await?,
			0,
			"the parked pass must have run to its end before the batch returned"
		);
		Ok(())
	}

	/// A batch whose pass panics keeps renewing its lease while it awaits the
	/// passes still running, so a surviving pass that outlasts the lease never
	/// runs under a lease another node has taken. The panic fails the batch
	/// only once that pass has ended.
	///
	/// The surviving pass is parked from before the lease the batch started
	/// under would have expired until after, and the test reads ownership, not
	/// time: a peer sharing the store asks for the lease throughout and never
	/// gets it, and the lease record still names this node when the pass
	/// resumes.
	#[tokio::test(flavor = "multi_thread")]
	async fn index_compaction_batch_keeps_its_lease_while_it_drains_after_a_panic() -> Result<()> {
		async fn lease_record(ds: &Datastore) -> Result<crate::catalog::TaskLease> {
			let key = TaskLeaseKey {
				task_id: TaskLeaseType::IndexCompaction,
			};
			let txn = ds.transaction(Read).await?;
			let lease = txn.get_key(&key, None).await;
			txn.cancel().await?;
			Ok(lease?.expect("the compaction batch holds a lease"))
		}

		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE zz SCHEMALESS;
			 DEFINE INDEX cz ON zz COUNT;
			 CREATE zz:1 RETURN NONE;
			 CREATE zz:2 RETURN NONE;",
		)
		.await?;
		// The queue names `user` before `zz`, so the parked pass runs on the
		// batch's own task and the panicking one on a helper, granted a place
		// of the node's pool while the parked pass waits.
		let parked = index_key_base(&ds, "user", "count_idx").await?;
		assert!(count_delta_len(&ds, &parked).await? > 0, "the parked pass must have work");
		let node = ds.id();
		// The second pass runs on a helper, which needs a place to start on.
		let _places = own_compaction_places(node, 4);
		let panicked = Arc::new(tokio::sync::Notify::new());
		let hook: BeforeCountApplyHook = {
			let panicked = Arc::clone(&panicked);
			Arc::new(move || {
				let panicked = Arc::clone(&panicked);
				Box::pin(async move {
					panicked.notify_one();
					panic!("a compaction pass panics");
				})
			})
		};
		BEFORE_COUNT_APPLY.lock().unwrap().insert(node, (1, hook));
		let park = compaction_pass_park::park_index(node, parked.clone());

		let keys = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		let lease = |node| {
			LeaseHandler::new(
				ds.sequences.clone(),
				node,
				ds.transaction_factory.clone(),
				TaskLeaseType::IndexCompaction,
				Duration::from_secs(2),
			)
		};
		let lh = lease(node)?;
		let peer_lease = lease(Uuid::new_v4())?;
		let batch_ds = Arc::clone(&ds);
		let mut batch = tokio::spawn(async move {
			Datastore::index_compaction_batch(
				batch_ds,
				&lh,
				&keys,
				&CompactionDrain::default(),
				&CancellationToken::new(),
			)
			.await
			.map(|cleanup| cleanup.map(|cleanup| cleanup.errors))
		});
		tokio::time::timeout(Duration::from_secs(30), park.parked())
			.await
			.expect("the parked pass must reach the park");
		tokio::time::timeout(Duration::from_secs(30), panicked.notified())
			.await
			.expect("the other pass must panic");
		let started_under = lease_record(&ds).await?;
		assert_eq!(started_under.owner, node);

		// Past the expiry of the lease the batch started under, a lease nobody
		// renewed would be the peer's for the asking.
		while Utc::now() <= started_under.expiration + chrono::Duration::seconds(1) {
			assert!(
				!peer_lease.has_lease().await?,
				"a peer took the compaction lease while a pass of the failed batch was running"
			);
			assert!(
				!batch.is_finished(),
				"the batch must not return while a pass it started is still running"
			);
			tokio::time::sleep(Duration::from_millis(250)).await;
		}
		let resumed_under = lease_record(&ds).await?;
		assert_eq!(resumed_under.owner, node, "the lease must still name this node");
		assert!(resumed_under.expiration > Utc::now(), "the lease must still be live");

		park.release();
		let res = tokio::time::timeout(Duration::from_secs(30), &mut batch)
			.await
			.expect("the batch must return once its passes have")?;
		let err = res.expect_err("the panicked pass must fail the batch");
		assert!(
			err.to_string().contains("a compaction pass panics"),
			"the batch must return the panic: {err}"
		);
		assert_eq!(
			compaction_pass_park::ended(node, &parked),
			compaction_pass_park::passes(node, &parked),
			"the parked pass must have ended before the batch returned"
		);
		assert_eq!(
			count_delta_len(&ds, &parked).await?,
			0,
			"the parked pass, still under the lease, must have run to its end"
		);
		Ok(())
	}

	/// A batch cancelled while its passes run waits for every one of them to
	/// end before it returns the cancellation, and leaves every request of the
	/// batch queued, as no pass it ran finished its work.
	#[tokio::test(flavor = "multi_thread")]
	async fn index_compaction_batch_awaits_every_pass_on_cancellation() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let indexes = [
			index_key_base(&ds, "user", "count_idx").await?,
			index_key_base(&ds, "vec", "hnsw_idx").await?,
		];
		let node = ds.id();
		// The second pass runs on a helper, which needs a place to start on.
		let _places = own_compaction_places(node, 4);
		let keys = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		for ikb in &indexes {
			assert!(index_compaction_queue_len(&ds, ikb).await? > 0, "{ikb} must be queued");
		}
		let park = compaction_pass_park::park(node);
		let lh = LeaseHandler::new(
			ds.sequences.clone(),
			node,
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(60),
		)?;
		let canceller = CancellationToken::new();
		let (batch_ds, batch_keys, batch_canceller) =
			(Arc::clone(&ds), keys.clone(), canceller.clone());
		let mut batch = tokio::spawn(async move {
			Datastore::index_compaction_batch(
				batch_ds,
				&lh,
				&batch_keys,
				&CompactionDrain::default(),
				&batch_canceller,
			)
			.await
			.map(|cleanup| cleanup.map(|cleanup| cleanup.errors))
		});
		tokio::time::timeout(Duration::from_secs(30), async {
			while indexes.iter().any(|ikb| compaction_pass_park::passes(node, ikb) == 0) {
				park.parked().await;
			}
		})
		.await
		.expect("both passes must reach the park");

		canceller.cancel();
		assert!(
			tokio::time::timeout(Duration::from_millis(500), &mut batch).await.is_err(),
			"a cancelled batch must not return while a pass it started is still running"
		);
		park.release();
		let res = tokio::time::timeout(Duration::from_secs(30), batch)
			.await
			.expect("the batch must return once its passes have")?;
		let err = res.expect_err("a cancelled batch must fail");
		assert!(
			matches!(crate::err::engine_error(&err), Some(EngineError::QueryCancelled)),
			"the batch must return the cancellation: {err}"
		);
		for ikb in &indexes {
			assert_eq!(
				compaction_pass_park::ended(node, ikb),
				compaction_pass_park::passes(node, ikb),
				"every pass over {ikb} must have ended before the batch returned"
			);
		}
		assert!(all_keys_remain(&ds, &keys).await?, "the batch's requests must stay queued");
		Ok(())
	}

	/// A pass that stops with work left, and whose fresh request meets
	/// `conflicts` retryable conflicts as it commits. Returns whether the pass
	/// reported the request unqueued, and the requests queued for the index
	/// before and after it.
	async fn requeue_meeting_conflicts(conflicts: usize) -> Result<(bool, usize, usize)> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let queued = index_compaction_queue_len(&ds, &ikb).await?;
		let lease = CompactionLease::new(&LeaseHandler::new(
			ds.sequences.clone(),
			ds.id(),
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(2),
		)?);
		lease.lost.store(true, std::sync::atomic::Ordering::Release);
		let _guard = inject_retryable_conflicts(
			RetryableConflictSite::IndexCompactionRequeue,
			ds.id(),
			conflicts,
		);

		let report = ds
			.run_index_compaction_pass(
				&ikb,
				&CancellationToken::new(),
				Some(&lease),
				&ds.compaction_places(),
			)
			.await;
		assert_eq!(report.result?, CompactionPassOutcome::Budget);
		assert_eq!(
			retryable_conflict_count(RetryableConflictSite::IndexCompactionRequeue, ds.id()),
			0,
			"the requeue must meet every injected conflict"
		);
		Ok((report.unqueued, queued, index_compaction_queue_len(&ds, &ikb).await?))
	}

	/// A fresh request that meets retryable conflicts as it commits is retried,
	/// as the passes retry theirs, so a transient conflict does not leave the
	/// index unqueued.
	#[tokio::test]
	async fn index_compaction_requeue_retries_a_retryable_conflict() -> Result<()> {
		let (unqueued, before, after) =
			requeue_meeting_conflicts(INDEX_COMPACTION_PASS_MAX_RETRIES).await?;
		assert!(!unqueued, "a requeue whose conflicts it retried away must commit");
		assert_eq!(after, before + 1, "the pass must queue exactly one fresh request");
		Ok(())
	}

	/// A fresh request whose conflicts outlast the requeue's retries is
	/// reported unqueued, and queues nothing.
	#[tokio::test]
	async fn index_compaction_requeue_reports_conflicts_that_outlast_its_retries() -> Result<()> {
		let (unqueued, before, after) =
			requeue_meeting_conflicts(INDEX_COMPACTION_PASS_MAX_RETRIES + 1).await?;
		assert!(unqueued, "a requeue that ran out of retries must be reported");
		assert_eq!(after, before, "nothing was queued");
		Ok(())
	}

	/// A batch keeps the requests of an index whose pass owed it a fresh one
	/// and could not queue it, so the index's remaining work stays queued, and
	/// the next drain folds it.
	#[tokio::test]
	async fn index_compaction_batch_keeps_the_requests_of_an_index_it_could_not_requeue()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let node_id = ds.id();
		let conflicts = inject_retryable_conflicts(
			RetryableConflictSite::CountCompaction,
			node_id,
			INDEX_COMPACTION_PASS_MAX_RETRIES + 1,
		);
		let requeue =
			inject_non_retryable_error(NonRetryableErrorSite::IndexCompactionRequeue, node_id);

		let (batch, errors) = run_one_index_compaction_batch(&ds).await?;
		assert_eq!(
			retryable_conflict_count(RetryableConflictSite::CountCompaction, node_id),
			0,
			"the pass must have run out of retries"
		);
		assert_eq!(errors, 1);
		assert!(!batch.is_empty());
		assert!(all_keys_remain(&ds, &batch).await?, "the index's requests must be kept");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			batch.len(),
			"no fresh request committed, so the kept ones are all there is"
		);
		drop(conflicts);
		drop(requeue);

		let (_, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(count_delta_len(&ds, &ikb).await?, 0, "the kept requests must fold the backlog");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 0);
		Ok(())
	}

	/// The backlog one queued kind's compaction folds, as the requeue tests
	/// below count it.
	#[derive(Clone, Copy)]
	enum Backlog {
		Count,
		FullText,
		Hnsw,
		#[cfg(diskann)]
		DiskAnn,
	}

	impl Backlog {
		async fn len(self, ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
			match self {
				Self::Count => count_delta_len(ds, ikb).await,
				Self::FullText => fulltext_delta_len(ds, ikb).await,
				Self::Hnsw => hnsw_pending_len(ds, ikb).await,
				#[cfg(diskann)]
				Self::DiskAnn => diskann_pending_len(ds, ikb).await,
			}
		}
	}

	/// A pass that meets more retryable conflicts than it may retry fails, but
	/// leaves exactly one fresh request for its index once the batch's cleanup
	/// has run, so the next drain folds the backlog without waiting for another
	/// write.
	async fn assert_index_compaction_requeues_after_its_retries_run_out(
		site: RetryableConflictSite,
		table: &str,
		index: &str,
		sql: &str,
		backlog: Backlog,
	) -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, sql).await?;
		let ikb = index_key_base(&ds, table, index).await?;
		assert!(backlog.len(&ds, &ikb).await? > 0, "the fixture must leave a backlog");
		let node_id = ds.id();
		let guard =
			inject_retryable_conflicts(site, node_id, INDEX_COMPACTION_PASS_MAX_RETRIES + 1);

		let (batch, errors) = run_one_index_compaction_batch(&ds).await?;
		assert_eq!(
			retryable_conflict_count(site, node_id),
			0,
			"the pass must have met every injected conflict"
		);
		assert_eq!(errors, 1, "a pass that runs out of retries fails");
		assert!(!any_key_remains(&ds, &batch).await?, "the batch's own keys must be gone");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			1,
			"the failed pass must leave exactly one fresh request for its index"
		);
		drop(guard);

		let (_, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(backlog.len(&ds, &ikb).await?, 0, "the requeued request must fold the backlog");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 0);
		Ok(())
	}

	#[tokio::test]
	async fn count_index_compaction_requeues_after_its_retries_run_out() -> Result<()> {
		assert_index_compaction_requeues_after_its_retries_run_out(
			RetryableConflictSite::CountCompaction,
			"user",
			"count_idx",
			COUNT_COMPACTION_SQL,
			Backlog::Count,
		)
		.await
	}

	#[tokio::test]
	async fn fulltext_index_compaction_requeues_after_its_retries_run_out() -> Result<()> {
		assert_index_compaction_requeues_after_its_retries_run_out(
			RetryableConflictSite::FullTextCompaction,
			"doc",
			"ft_idx",
			FULLTEXT_COMPACTION_SQL,
			Backlog::FullText,
		)
		.await
	}

	#[tokio::test]
	async fn hnsw_index_compaction_requeues_after_its_retries_run_out() -> Result<()> {
		assert_index_compaction_requeues_after_its_retries_run_out(
			RetryableConflictSite::HnswCompaction,
			"vec",
			"hnsw_idx",
			HNSW_COMPACTION_SQL,
			Backlog::Hnsw,
		)
		.await
	}

	#[cfg(diskann)]
	#[tokio::test]
	async fn diskann_index_compaction_requeues_after_its_retries_run_out() -> Result<()> {
		assert_index_compaction_requeues_after_its_retries_run_out(
			RetryableConflictSite::DiskAnnCompaction,
			"dvec",
			"diskann_idx",
			DISKANN_COMPACTION_SQL,
			Backlog::DiskAnn,
		)
		.await
	}

	/// The definition a compaction plans and applies against.
	async fn index_definition(
		ds: &Datastore,
		ikb: &IndexKeyBase,
	) -> Result<Arc<crate::catalog::IndexDefinition>> {
		let txn = ds.transaction(Read).await?;
		let ix = txn.get_tb_index_by_id(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), None).await;
		txn.cancel().await?;
		Ok(ix?.expect("the index is defined"))
	}

	/// A context over `txn`, as the passes build one to plan and apply through.
	fn compaction_ctx(ds: &Datastore, txn: &Arc<Transaction>) -> Result<FrozenContext> {
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(Arc::clone(txn));
		Ok(ctx.freeze())
	}

	/// Of two count plans read from one state, the first applies and the second
	/// reports the lost race rather than an empty plan; with the backlog folded,
	/// a pass finds nothing.
	#[tokio::test]
	async fn count_index_compaction_reports_a_lost_race_apart_from_an_empty_plan() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "user", "count_idx").await?;
		let (first, second) = {
			let txn = ds.transaction(Read).await?;
			let first = IndexOperation::prepare_count_compaction(&ikb, &txn).await;
			let second = IndexOperation::prepare_count_compaction(&ikb, &txn).await;
			txn.cancel().await?;
			(first?, second?)
		};

		let txn = ds.transaction(Write).await?;
		let applied = catch!(txn, IndexOperation::apply_count_compaction(&ikb, &txn, first).await);
		assert_eq!(applied, CompactionApply::Applied);
		txn.commit().await?;
		let txn = ds.transaction(Write).await?;
		let applied = catch!(txn, IndexOperation::apply_count_compaction(&ikb, &txn, second).await);
		txn.cancel().await?;
		assert_eq!(applied, CompactionApply::LostRace);

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Empty
		);
		Ok(())
	}

	/// [`count_index_compaction_reports_a_lost_race_apart_from_an_empty_plan`]
	/// for a full-text index.
	#[tokio::test]
	async fn fulltext_index_compaction_reports_a_lost_race_apart_from_an_empty_plan() -> Result<()>
	{
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, FULLTEXT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "doc", "ft_idx").await?;
		let ix = index_definition(&ds, &ikb).await?;
		let Index::FullText(p) = &ix.index else {
			panic!("ft_idx is not a full-text index");
		};
		let allow_list = &ds.config.idx.file_allowlist;
		let (first, second) = {
			let txn = ds.transaction(Read).await?;
			let first = IndexOperation::prepare_fulltext_compaction(
				&ds.index_stores,
				&ikb,
				&txn,
				p,
				allow_list,
				ix.format_version,
			)
			.await;
			let second = IndexOperation::prepare_fulltext_compaction(
				&ds.index_stores,
				&ikb,
				&txn,
				p,
				allow_list,
				ix.format_version,
			)
			.await;
			txn.cancel().await?;
			(first?, second?)
		};

		for (plan, expected) in
			[(first, CompactionApply::Applied), (second, CompactionApply::LostRace)]
		{
			let txn = ds.transaction(Write).await?;
			let applied = catch!(
				txn,
				IndexOperation::apply_fulltext_compaction(
					&ds.index_stores,
					&ikb,
					&txn,
					p,
					allow_list,
					ix.format_version,
					plan,
				)
				.await
			);
			assert_eq!(applied, expected);
			if applied.applied() {
				txn.commit().await?;
			} else {
				txn.cancel().await?;
			}
		}

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Empty
		);
		Ok(())
	}

	/// [`count_index_compaction_reports_a_lost_race_apart_from_an_empty_plan`]
	/// for an HNSW index.
	#[tokio::test(flavor = "multi_thread")]
	async fn hnsw_index_compaction_reports_a_lost_race_apart_from_an_empty_plan() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		let ix = index_definition(&ds, &ikb).await?;
		let Index::Hnsw(p) = &ix.index else {
			panic!("hnsw_idx is not an HNSW index");
		};
		let (first, second) = {
			let txn = Arc::new(ds.transaction(Read).await?);
			let ctx = compaction_ctx(&ds, &txn)?;
			let first = IndexOperation::prepare_hnsw_compaction(&ctx, &ikb).await;
			let second = IndexOperation::prepare_hnsw_compaction(&ctx, &ikb).await;
			txn.cancel().await?;
			(first?, second?)
		};

		let txn = Arc::new(ds.transaction(Write).await?);
		let ctx = compaction_ctx(&ds, &txn)?;
		let applied = IndexOperation::apply_hnsw_compaction(
			&ctx,
			&ds.index_stores,
			&ikb,
			p,
			ix.format_version,
			first,
		)
		.await;
		let applied = catch!(txn, applied);
		assert!(applied.applied(), "the first plan must apply: {applied:?}");
		txn.commit().await?;
		let txn = Arc::new(ds.transaction(Write).await?);
		let ctx = compaction_ctx(&ds, &txn)?;
		let applied = IndexOperation::apply_hnsw_compaction(
			&ctx,
			&ds.index_stores,
			&ikb,
			p,
			ix.format_version,
			second,
		)
		.await;
		txn.cancel().await?;
		assert_eq!(applied?, HnswCompactionOutcome::LostRace);

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Empty
		);
		Ok(())
	}

	/// [`count_index_compaction_reports_a_lost_race_apart_from_an_empty_plan`]
	/// for a DiskANN index, whose apply commits its own transaction.
	///
	/// An apply that commits steps the guards of the shards it drained to
	/// `Empty` in a transaction of its own, so the pass after it finds nothing.
	#[cfg(diskann)]
	#[tokio::test]
	async fn diskann_index_compaction_reports_a_lost_race_apart_from_an_empty_plan() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, DISKANN_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "dvec", "diskann_idx").await?;
		let ix = index_definition(&ds, &ikb).await?;
		let Index::DiskAnn(p) = &ix.index else {
			panic!("diskann_idx is not a DiskANN index");
		};
		let (first, second) = {
			let txn = Arc::new(ds.transaction(Read).await?);
			let ctx = compaction_ctx(&ds, &txn)?;
			let first = IndexOperation::prepare_diskann_compaction(&ctx, &ikb).await;
			let second = IndexOperation::prepare_diskann_compaction(&ctx, &ikb).await;
			txn.cancel().await?;
			(first?, second?)
		};

		for (plan, expected) in
			[(first, CompactionApply::Applied), (second, CompactionApply::LostRace)]
		{
			let txn = Arc::new(ds.transaction(Write).await?);
			let ctx = compaction_ctx(&ds, &txn)?;
			let applied = IndexOperation::apply_diskann_compaction(
				&ctx,
				&ds.index_stores,
				&ikb,
				p,
				ix.format_version,
				plan,
			)
			.await;
			if !txn.closed() {
				txn.cancel().await?;
			}
			assert_eq!(applied?, expected);
		}
		assert_eq!(diskann_pending_len(&ds, &ikb).await?, 0);

		assert_eq!(
			ds.process_index_compaction(&ikb, CancellationToken::new()).await?,
			CompactionPassOutcome::Empty
		);
		Ok(())
	}

	/// Passing the compactor's write guard has to bind at commit, not just at
	/// the check.
	///
	/// A retirement that commits between the guard's read and the apply's commit
	/// has to take the apply with it; otherwise the compaction writes index data
	/// under a prefix the catalog no longer names, and only the queued reclaim
	/// would sweep it, if it had not already finished. The guard's reads are
	/// registered reads, so this needs a backend that validates them.
	#[cfg(feature = "kv-rocksdb")]
	async fn assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
		sql: &str,
		table: &str,
		index: &str,
	) -> Result<()> {
		let dir = temp_dir::TempDir::new()?;
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path(&format!("rocksdb://{}", dir.path().to_string_lossy()))
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		execute_all(&ds, &session, sql).await?;
		let ikb = index_key_base(&ds, table, index).await?;

		// The apply's transaction passes its guard while the index is live.
		let txn = ds.transaction(Write).await?;
		let guard = catch!(txn, compaction_write_guard(&txn, &ikb).await);
		assert!(
			matches!(guard, CompactionWriteGuard::Live(_)),
			"the guard must pass while the index is live"
		);

		// The retirement commits in between, as it may on any node.
		execute_all(&ds, &session, &format!("REMOVE INDEX {index} ON {table}")).await?;

		// The apply this guard stands in for. It writes index data, not a
		// guarded key: a write to one would conflict on the write alone and
		// prove nothing about whether the guard's reads are tracked.
		let mut entry = IdxRoot {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Borrowed(ikb.table()),
			ix: ikb.index(),
		}
		.range()?
		.start()
		.to_vec();
		entry.push(0x01);
		catch!(txn, txn.set(Key::from(entry), vec![0x01]).await);
		let err = txn.commit().await.expect_err("the retirement must take the apply with it");
		assert!(
			is_retryable_transaction_conflict(&err),
			"expected a retryable conflict, got: {err}"
		);
		let _ = txn.cancel().await;
		Ok(())
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread")]
	async fn fulltext_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it()
	-> Result<()> {
		assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
			FULLTEXT_COMPACTION_SQL,
			"doc",
			"ft_idx",
		)
		.await
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread")]
	async fn count_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it()
	-> Result<()> {
		assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
			COUNT_COMPACTION_SQL,
			"user",
			"count_idx",
		)
		.await
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test(flavor = "multi_thread")]
	async fn hnsw_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it()
	-> Result<()> {
		assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
			HNSW_COMPACTION_SQL,
			"vec",
			"hnsw_idx",
		)
		.await
	}

	#[cfg(all(feature = "kv-rocksdb", diskann))]
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it()
	-> Result<()> {
		assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
			DISKANN_COMPACTION_SQL,
			"dvec",
			"diskann_idx",
		)
		.await
	}

	/// The process-local HNSW wrapper for one index, resolved the way the query
	/// path resolves it, so a test holds the same instance the compaction and
	/// the executor reach for.
	async fn loaded_hnsw_index(
		ds: &Datastore,
		table: &str,
		index: &str,
	) -> Result<crate::idx::trees::store::hnsw::SharedHnswIndex> {
		let table = TableName::from(table);
		let txn = Arc::new(ds.transaction(Read).await?);
		let ns = txn.get_ns_by_name("test", None).await?.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await?.unwrap();
		let tb = txn.get_tb(ns.namespace_id, db.database_id, &table, None).await?.unwrap();
		let ix =
			txn.get_tb_index(ns.namespace_id, db.database_id, &table, index, None).await?.unwrap();
		let Index::Hnsw(p) = &ix.index else {
			panic!("{index} is not an HNSW index");
		};
		let ikb = IndexKeyBase::new(ns.namespace_id, db.database_id, table, ix.index_id);
		let mut ctx = ds.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		let ctx = ctx.freeze();
		let hi =
			ds.index_store().get_index_hnsw(&ctx, tb.table_id, &ikb, p, ix.format_version).await?;
		txn.cancel().await?;
		Ok(hi)
	}

	/// The entries an HNSW compaction still has to apply, across both pending
	/// families.
	async fn hnsw_pending_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let records = txn.getr(ikb.new_hr_range()?, None).await?.len();
		let appends = txn.getr_raw(ikb.new_hp_range()?, None).await?.len();
		txn.cancel().await?;
		Ok(records + appends)
	}

	/// The layer versions the store holds for one HNSW index, layer 0 first.
	async fn hnsw_layer_versions(ds: &Datastore, ikb: &IndexKeyBase) -> Result<Vec<u64>> {
		let txn = ds.transaction(Read).await?;
		let state: surrealdb_datastore::values::hnsw::HnswState =
			txn.get_key(&ikb.new_hs_key(), None).await?.unwrap_or_default();
		txn.cancel().await?;
		let mut versions = Vec::with_capacity(1 + state.layers.len());
		versions.push(state.layer0.version);
		versions.extend(state.layers.iter().map(|l| l.version));
		Ok(versions)
	}

	/// A filtered kNN search must not evaluate its residual condition against a
	/// legacy pending entry that carries no vectors to score.
	#[tokio::test]
	async fn hnsw_legacy_pending_delete_entry_is_not_where_evaluated() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE vec SCHEMALESS;
			 DEFINE INDEX hnsw_idx ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
			 CREATE vec:2 SET vector = [5, 5], bad = 5 RETURN NONE;
			 CREATE vec:1 SET bad = 'x' RETURN NONE;",
		)
		.await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;

		// Put vec:2 in the graph, then inject the shape of a pre-v3.1 legacy
		// delete that an upgraded node can still encounter while draining `!hp`.
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		{
			let txn = ds.transaction(Write).await?;
			let key = surrealdb_datastore::key::schema::HnswPendingLegacyKey::new(
				ikb.ns(),
				ikb.db(),
				Cow::Borrowed(ikb.table()),
				ikb.index(),
				0,
			);
			let pending = surrealdb_datastore::values::hnsw::VectorPendingUpdate {
				id: surrealdb_datastore::values::hnsw::VectorId::RecordKey(Arc::new(
					crate::val::RecordIdKey::Number(1),
				)),
				old_vectors: vec![],
				new_vectors: vec![],
			};
			txn.set_key(&key, &pending).await?;
			txn.commit().await?;
		}

		let mut response = ds
			.execute(
				"SELECT id FROM vec WHERE vector <|5,40|> [0, 0] AND type::int(bad) > 0;",
				&session,
				None,
			)
			.await?;
		let rows = match response.remove(0).result {
			Ok(PublicValue::Array(rows)) => rows,
			Ok(other) => panic!("expected an array result, got {other:?}"),
			Err(error) => panic!(
				"the excluded vec:1 legacy delete must not have its residual condition evaluated: {error}"
			),
		};
		assert_eq!(rows.len(), 1, "only vec:2 satisfies the query: {rows:?}");
		Ok(())
	}

	/// A compaction apply that reached the graph in place and then failed to
	/// commit must not leave that graph reachable.
	///
	/// The apply advances the in-memory version of every layer it writes, and
	/// that version is a plain per-layer counter: a peer applying the same
	/// batch from the same version lands on the same one. This fixture pins the
	/// arithmetic — one staged record advances layer 0 by exactly one — so the
	/// rolled-back copy and a peer's committed copy stand at the same version
	/// with topologies that need not match, and a state check comparing
	/// versions would keep the copy the store never took. The wrapper goes
	/// instead, and the next access rebuilds it from persisted state.
	///
	/// The index engines are configured serial, so the insertion is applied in
	/// place; see the staged counterpart below for the default. The injected
	/// conflict classifies as retryable, so the compaction rolls its write
	/// back, discards the wrapper, and applies again; the call returns once
	/// that attempt commits. That attempt drains the queue, so the records the
	/// index then answers over come from the graph alone.
	#[tokio::test]
	async fn hnsw_index_compaction_commit_failure_evicts_the_loaded_index() -> Result<()> {
		let (ds, session) =
			new_index_compaction_test_ds_with("hnsw_build_parallelism", "serial").await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		let site = RetryableConflictSite::HnswCompaction;
		let node_id = ds.id();

		// Hold the wrapper the query path uses, with its graph resident.
		let before = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		let versions = hnsw_layer_versions(&ds, &ikb).await?;

		// One more pending, and a commit conflict to roll its apply back.
		execute_all(&ds, &session, "CREATE vec:3 SET vector = [8, 9] RETURN NONE;").await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 1, "the record must be staged, not indexed");
		let _guard = inject_retryable_conflict(site, node_id);
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(retryable_conflict_count(site, node_id), 0, "the conflict must have fired");
		assert_eq!(
			hnsw_pending_len(&ds, &ikb).await?,
			0,
			"the attempt after the rollback must have applied the pending it rolled back"
		);

		// The advance the rolled-back apply staged in memory, and that the
		// attempt after it then committed: one batch of one record, one layer-0
		// version. A peer applying that same batch reproduces it exactly, which
		// is why the version cannot certify the rolled-back topology.
		let applied = hnsw_layer_versions(&ds, &ikb).await?;
		assert_eq!(
			applied[0],
			versions[0] + 1,
			"one staged record must advance layer 0 by exactly one: {versions:?} -> {applied:?}"
		);

		// The wrapper the rollback mutated is gone; the attempt after it ran on
		// one rebuilt from the store.
		let after = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		assert!(
			!Arc::ptr_eq(&before, &after),
			"a rolled-back compaction write must not leave its graph reachable"
		);

		// With the queue drained, every record answered for comes from the
		// graph, which the rebuilt wrapper took from the store.
		let mut response =
			ds.execute("SELECT id FROM vec WHERE vector <|3,40|> [1, 2];", &session, None).await?;
		let PublicValue::Array(rows) = response.remove(0).result? else {
			panic!("expected an array result");
		};
		assert_eq!(rows.len(), 3, "the recovered index must return every record: {rows:?}");
		Ok(())
	}

	/// A staged compaction whose commit fails leaves the loaded index in place.
	///
	/// A batch that only adds vectors builds its graph changes into its
	/// transaction and publishes them into the resident graph from the commit,
	/// so a commit that fails leaves the resident graph at the committed state:
	/// there is nothing to discard, and the retry runs on the same wrapper. The
	/// retry advances layer 0 by exactly one, as the in-place apply does, and
	/// once it has drained the queue the graph answers for every record.
	#[tokio::test]
	async fn hnsw_staged_compaction_commit_failure_keeps_the_loaded_index() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		let site = RetryableConflictSite::HnswCompaction;
		let node_id = ds.id();

		let before = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		let versions = hnsw_layer_versions(&ds, &ikb).await?;

		execute_all(&ds, &session, "CREATE vec:3 SET vector = [8, 9] RETURN NONE;").await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 1, "the record must be staged, not indexed");
		let _guard = inject_retryable_conflict(site, node_id);
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;
		assert_eq!(retryable_conflict_count(site, node_id), 0, "the conflict must have fired");
		assert_eq!(
			hnsw_pending_len(&ds, &ikb).await?,
			0,
			"the attempt after the rollback must have applied the pending it rolled back"
		);
		let applied = hnsw_layer_versions(&ds, &ikb).await?;
		assert_eq!(
			applied[0],
			versions[0] + 1,
			"one staged batch must advance layer 0 by exactly one: {versions:?} -> {applied:?}"
		);

		let after = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		assert!(
			Arc::ptr_eq(&before, &after),
			"a staged batch that did not commit must leave the loaded index in place"
		);
		let mut response =
			ds.execute("SELECT id FROM vec WHERE vector <|3,40|> [1, 2];", &session, None).await?;
		let PublicValue::Array(rows) = response.remove(0).result? else {
			panic!("expected an array result");
		};
		assert_eq!(rows.len(), 3, "the index must return every record: {rows:?}");
		Ok(())
	}

	/// An apply error can land after a layer mutates but before `save_nodes`
	/// advances its version, leaving a resident topology the store never took
	/// under the version the store already holds. That path must evict the
	/// wrapper too.
	#[tokio::test]
	async fn hnsw_index_compaction_apply_error_evicts_the_loaded_index() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;

		// Hold the query-path wrapper with the two-node graph resident. Element
		// ids follow the two separately keyed pending records: vec:1 is 0 and
		// vec:2 is 1.
		let before = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		let (table_id, format_version, entry_point) = {
			let txn = ds.transaction(Read).await?;
			let ns = txn.get_ns_by_name("test", None).await?.unwrap();
			let db = txn.get_db_by_name("test", "test", None).await?.unwrap();
			let table = TableName::from("vec");
			let tb = txn.get_tb(ns.namespace_id, db.database_id, &table, None).await?.unwrap();
			let ix = txn
				.get_tb_index(ns.namespace_id, db.database_id, &table, "hnsw_idx", None)
				.await?
				.unwrap();
			let state: surrealdb_datastore::values::hnsw::HnswState =
				txn.get_key(&ikb.new_hs_key(), None).await?.unwrap();
			txn.cancel().await?;
			(tb.table_id, ix.format_version, state.enter_point.unwrap())
		};
		assert!(entry_point <= 1, "the two-node fixture has an unexpected entry point");

		// Delete the non-entry-point record. Its layer-0 removal mutates the
		// resident graph before resolving the remaining neighbour's vector.
		let removed_element = 1 - entry_point;
		let delete = format!("DELETE vec:{} RETURN NONE;", removed_element + 1);
		execute_all(&ds, &session, &delete).await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 1);

		// Force that neighbour lookup to the store, then corrupt its encoded
		// vector. The apply errors after `remove_node_and_bidirectional_edges`
		// but before `save_nodes` can version the mutated layer.
		ds.index_store().invalidate_hnsw_index_caches(table_id, &ikb, format_version).await;
		let entry_vector_key = surrealdb_datastore::key::schema::HnswVectorKey::new(
			ikb.ns(),
			ikb.db(),
			Cow::Borrowed(ikb.table()),
			ikb.index(),
			entry_point,
		);
		let txn = ds.transaction(Write).await?;
		txn.set(entry_vector_key.encode_key()?, vec![0xff]).await?;
		txn.commit().await?;

		ds.process_index_compaction(&ikb, CancellationToken::new())
			.await
			.expect_err("the malformed neighbour vector must fail the graph apply");
		assert_eq!(
			hnsw_pending_len(&ds, &ikb).await?,
			1,
			"the failed apply must roll back its conditional pending delete"
		);

		let after = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		assert!(
			!Arc::ptr_eq(&before, &after),
			"an apply error before versioning must evict the mutated wrapper"
		);
		Ok(())
	}

	/// Runs one queue batch whose cleanup meets `conflicts` retryable
	/// conflicts as it commits. Returns whether the cleanup committed, and
	/// whether every key of the batch is still queued.
	async fn queue_cleanup_meeting_conflicts(conflicts: usize) -> Result<(bool, bool)> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let keys = {
			let txn = ds.transaction(Read).await?;
			let res = txn
				.keys(IndexCompactionPrefix {}.range()?, INDEX_COMPACTION_QUEUE_BATCH_SIZE, 0, None)
				.await;
			txn.cancel().await?;
			res?
		};
		assert!(!keys.is_empty(), "the fixture must queue requests");
		let site = RetryableConflictSite::IndexCompactionQueueCleanup;
		let _guard = inject_retryable_conflicts(site, ds.id(), conflicts);
		let lh = LeaseHandler::new(
			ds.sequences.clone(),
			ds.id(),
			ds.transaction_factory.clone(),
			TaskLeaseType::IndexCompaction,
			Duration::from_secs(60),
		)?;
		let cleanup = Datastore::index_compaction_batch(
			Arc::clone(&ds),
			&lh,
			&keys,
			&CompactionDrain::default(),
			&CancellationToken::new(),
		)
		.await?
		.expect("the batch must hold the lease");
		assert_eq!(
			retryable_conflict_count(site, ds.id()),
			0,
			"the cleanup must meet every injected conflict"
		);
		Ok((cleanup.deleted, all_keys_remain(&ds, &keys).await?))
	}

	/// A cleanup retries the retryable conflicts it meets, up to the bound the
	/// passes use.
	#[tokio::test]
	async fn index_compaction_queue_cleanup_retries_up_to_its_bound() -> Result<()> {
		let (deleted, remain) =
			queue_cleanup_meeting_conflicts(INDEX_COMPACTION_PASS_MAX_RETRIES).await?;
		assert!(deleted, "a cleanup whose conflicts it retried away must commit");
		assert!(!remain, "the committed cleanup must delete the batch's requests");
		Ok(())
	}

	/// A cleanup whose conflicts outlast its retries gives up and leaves every
	/// request of the batch queued.
	#[tokio::test]
	async fn index_compaction_queue_cleanup_gives_up_and_keeps_the_batch() -> Result<()> {
		let (deleted, remain) =
			queue_cleanup_meeting_conflicts(INDEX_COMPACTION_PASS_MAX_RETRIES + 1).await?;
		assert!(!deleted, "a cleanup that ran out of retries must not commit");
		assert!(remain, "a cleanup that gave up must leave every request queued");
		Ok(())
	}

	#[tokio::test]
	async fn index_compaction_retries_queue_cleanup_commit_conflict() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let site = RetryableConflictSite::IndexCompactionQueueCleanup;
		let node_id = ds.id();
		let _guard = inject_retryable_conflict(site, node_id);

		let (_, errors) =
			Datastore::index_compaction(ds, Duration::from_secs(1), CancellationToken::new())
				.await?;

		assert_eq!(errors, 0);
		assert_eq!(retryable_conflict_count(site, node_id), 0);
		Ok(())
	}

	/// Runs one compaction pass over a short queue, with two more entries queued
	/// once its first batch is compacted, as a write landing during the pass
	/// would, and returns the pass's batches and the entries left after it.
	async fn compaction_pass_with_entries_queued_during_it(
		shared_store: bool,
	) -> Result<(usize, usize)> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		if shared_store {
			ds.triggers.assume_shared_store();
		}
		execute_all(
			&ds,
			&session,
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			 DEFINE TABLE doc SCHEMALESS;
			 DEFINE INDEX ft_idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25;",
		)
		.await?;
		let ikb = index_key_base(&ds, "doc", "ft_idx").await?;
		let node_id = ds.id();
		let enqueue = move |ds: Arc<Datastore>, ikb: IndexKeyBase, n: usize| async move {
			let txn = ds.transaction(Write).await?;
			for _ in 0..n {
				txn.set_key(&ikb.new_ic_key(node_id), &()).await?;
			}
			txn.commit().await
		};
		enqueue(Arc::clone(&ds), ikb.clone(), 3).await?;
		let (hook_ds, hook_ikb) = (Arc::clone(&ds), ikb.clone());
		AFTER_COMPACTION_BATCH.lock().unwrap().insert(
			node_id,
			Box::new(move || {
				Box::pin(async move {
					enqueue(hook_ds, hook_ikb, 2).await.expect("queueing during the pass");
				})
			}),
		);
		let (batches, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert!(take_after_compaction_batch(node_id).is_none(), "the pass ran the hook");
		let remaining = {
			let txn = ds.transaction(Read).await?;
			let res = txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await;
			let _ = txn.cancel().await;
			res?
		};
		Ok((batches, remaining.len()))
	}

	/// Where the datastore is the store's sole writer, a pass ends once it has
	/// compacted a short batch holding the whole queue: entries queued during
	/// it are left for the next pass, which the commit that queued them wakes.
	#[tokio::test]
	async fn index_compaction_pass_on_a_sole_writer_leaves_entries_queued_during_it() -> Result<()>
	{
		assert_eq!(compaction_pass_with_entries_queued_during_it(false).await?, (1, 2));
		Ok(())
	}

	/// On a store other nodes write to, their commits do not wake this node's
	/// compactor, so a pass drains what was queued during it before it ends.
	#[tokio::test]
	async fn index_compaction_pass_on_a_shared_store_drains_entries_queued_during_it() -> Result<()>
	{
		assert_eq!(compaction_pass_with_entries_queued_during_it(true).await?, (2, 0));
		Ok(())
	}

	/// Queues `n` more compaction requests for `ikb`, each under its own key.
	async fn queue_compaction_requests(ds: &Datastore, ikb: &IndexKeyBase, n: usize) -> Result<()> {
		let txn = ds.transaction(Write).await?;
		for _ in 0..n {
			catch!(txn, txn.set_key(&ikb.new_ic_key(ds.id()), &()).await);
		}
		txn.commit().await
	}

	/// Writes `ikb`'s durable build state as an `Online` generation, owned by
	/// `owner` when one is given: a build that has published and holds its
	/// index until the statement that started it closes, or one that has
	/// released it. Writes no compaction request.
	async fn set_build_owner(
		ds: &Datastore,
		ikb: &IndexKeyBase,
		owner: Option<Uuid>,
	) -> Result<()> {
		let now = Utc::now();
		let state = crate::kvs::index::IndexBuildState {
			generation: 1,
			phase: crate::kvs::index::IndexBuildPhase::Online,
			owner,
			next_ticket: 0,
			initial_complete: true,
			updated_at: now,
			owner_heartbeat_at: owner.map(|_| now),
			error: None,
			report_status: None,
			initial: None,
			updated: None,
			pending: None,
			initial_cursor: None,
		};
		let txn = ds.transaction(Write).await?;
		catch!(txn, txn.set_key(&ikb.new_bs_key(), &state).await);
		txn.commit().await
	}

	/// Consumes a pending compaction wake, reporting whether there was one.
	fn take_compaction_wake(ds: &Datastore) -> bool {
		use futures::FutureExt as _;
		ds.triggers.index_compaction.notified().now_or_never().is_some()
	}

	/// A pass a live build owner fences off its vector index does not act on
	/// the index's requests, so the drain keeps one of them, and only one,
	/// however many the index had queued. The first drain after the fence
	/// comes down folds the backlog from it, with no request written by the
	/// release.
	#[tokio::test]
	async fn index_compaction_keeps_one_request_for_a_fenced_index() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		queue_compaction_requests(&ds, &ikb, 3).await?;
		let queued = index_compaction_queue_len(&ds, &ikb).await?;
		assert!(queued >= 3, "the fixture must queue several requests for the index");
		let pendings = hnsw_pending_len(&ds, &ikb).await?;
		assert!(pendings > 0, "the fixture must leave pendings to fold");
		set_build_owner(&ds, &ikb, Some(Uuid::now_v7())).await?;

		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, pendings, "the fence leaves the pendings");
		assert_eq!(
			index_compaction_queue_len(&ds, &ikb).await?,
			1,
			"the drain must keep exactly one of the index's requests"
		);

		set_build_owner(&ds, &ikb, None).await?;
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 1);
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 0, "the kept request folds the backlog");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 0);
		Ok(())
	}

	/// Requests for an index with no definition, under a live build owner, are
	/// the requests of an index whose defining statement has not committed: the
	/// drain keeps one.
	#[tokio::test]
	async fn index_compaction_keeps_one_request_for_an_index_defined_under_a_live_owner()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let defined = index_key_base(&ds, "user", "count_idx").await?;
		let ikb = IndexKeyBase::new(
			defined.ns(),
			defined.db(),
			defined.table().clone(),
			crate::catalog::IndexId(defined.index().0 + 100),
		);
		queue_compaction_requests(&ds, &ikb, 3).await?;
		set_build_owner(&ds, &ikb, Some(Uuid::now_v7())).await?;

		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 1);
		Ok(())
	}

	/// Requests for an index with no definition and no live build owner name an
	/// index that was removed or never committed, so the drain drops them all,
	/// whether the index left no build state or one its owner released.
	#[tokio::test]
	async fn index_compaction_drops_the_requests_of_an_index_with_no_definition_and_no_owner()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let defined = index_key_base(&ds, "user", "count_idx").await?;
		let gone = |offset: u32| {
			IndexKeyBase::new(
				defined.ns(),
				defined.db(),
				defined.table().clone(),
				crate::catalog::IndexId(defined.index().0 + offset),
			)
		};
		let (no_state, released) = (gone(100), gone(101));
		queue_compaction_requests(&ds, &no_state, 3).await?;
		queue_compaction_requests(&ds, &released, 3).await?;
		set_build_owner(&ds, &released, None).await?;

		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(index_compaction_queue_len(&ds, &no_state).await?, 0);
		assert_eq!(index_compaction_queue_len(&ds, &released).await?, 0);
		Ok(())
	}

	/// More requests than one queue batch holds, for a fenced index and for one
	/// whose definition is not yet readable, fold to one request each in one
	/// drain, with one pass per index: the batches after the one that kept an
	/// index's request delete that index's other requests without passing it
	/// again. The drain ends, and leaves no wake behind it.
	async fn assert_a_multi_batch_backlog_of_kept_requests_folds_to_one(
		shared_store: bool,
	) -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		if shared_store {
			ds.triggers.assume_shared_store();
		}
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let fenced = index_key_base(&ds, "vec", "hnsw_idx").await?;
		let defined = index_key_base(&ds, "user", "count_idx").await?;
		let not_visible = IndexKeyBase::new(
			defined.ns(),
			defined.db(),
			defined.table().clone(),
			crate::catalog::IndexId(defined.index().0 + 100),
		);
		let requests = INDEX_COMPACTION_QUEUE_BATCH_SIZE as usize + 50;
		for ikb in [&fenced, &not_visible] {
			let mut queued = index_compaction_queue_len(&ds, ikb).await?;
			while queued < requests {
				let n = (requests - queued).min(1000);
				queue_compaction_requests(&ds, ikb, n).await?;
				queued += n;
			}
			assert_eq!(index_compaction_queue_len(&ds, ikb).await?, requests);
			set_build_owner(&ds, ikb, Some(Uuid::now_v7())).await?;
		}
		take_compaction_wake(&ds);

		let (_, errors) = tokio::time::timeout(
			Duration::from_secs(120),
			Datastore::index_compaction(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
			),
		)
		.await
		.expect("a drain over kept requests must end")?;
		assert_eq!(errors, 0);
		for ikb in [&fenced, &not_visible] {
			assert_eq!(
				index_compaction_queue_len(&ds, ikb).await?,
				1,
				"one drain must fold the index's requests to one: {ikb}"
			);
			assert_eq!(
				compaction_pass_park::passes(ds.id(), ikb),
				1,
				"one drain must pass the index once: {ikb}"
			);
		}
		assert!(!take_compaction_wake(&ds), "keeping requests must not wake the compactor");
		Ok(())
	}

	/// A drain that sweeps leaves alone an index it already ran a pass over:
	/// a pass that failed on an error no retry cures leaves its index to its
	/// next write, and the sweep at the end of the same drain does not queue it
	/// again and run a second pass over it.
	#[tokio::test]
	async fn index_compaction_sweep_leaves_an_index_whose_pass_failed_to_its_next_write()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, FULLTEXT_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "doc", "ft_idx").await?;
		let deltas = fulltext_delta_len(&ds, &ikb).await?;
		assert!(deltas > 0, "the fixture must leave deltas to fold");
		ds.expire_compaction_sweep();
		let _fault = inject_non_retryable_error(NonRetryableErrorSite::FullTextCompaction, ds.id());

		let (_, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;

		assert_eq!(errors, 1, "the pass failed");
		assert_eq!(fulltext_delta_len(&ds, &ikb).await?, deltas, "no second pass ran");
		assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 0, "nothing was queued again");
		Ok(())
	}

	/// A drain that ends on a lap over nothing but the requests it kept still
	/// sweeps: a residual stranded on an index with no request is folded by
	/// the same call that kept a fenced index's request.
	#[tokio::test]
	async fn index_compaction_over_kept_requests_still_sweeps() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, FULLTEXT_COMPACTION_SQL).await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ft = index_key_base(&ds, "doc", "ft_idx").await?;
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(fulltext_delta_len(&ds, &ft).await?, 0);
		assert_eq!(index_compaction_queue_len(&ds, &ft).await?, 0);
		// A residual no delta and no request names.
		let residual = crate::key::schema::TermDocsResidualKey {
			ns: ft.ns(),
			db: ft.db(),
			tb: Cow::Borrowed(ft.table()),
			ix: ft.index(),
			term: Cow::Borrowed("zeta"),
		};
		let txn = ds.transaction(Write).await?;
		catch!(
			txn,
			txn.set_key(
				&residual,
				&surrealdb_datastore::values::fulltext::TermDocsResidual::new([(7, 1)])
			)
			.await
		);
		txn.commit().await?;
		// A fenced index with more requests than one batch reads, so the drain
		// is not a single batch that held the whole queue, and ends with the
		// one request it keeps.
		let fenced = index_key_base(&ds, "vec", "hnsw_idx").await?;
		let requests = INDEX_COMPACTION_QUEUE_BATCH_SIZE as usize + 50;
		let queued = index_compaction_queue_len(&ds, &fenced).await?;
		queue_compaction_requests(&ds, &fenced, requests - queued).await?;
		set_build_owner(&ds, &fenced, Some(Uuid::now_v7())).await?;
		ds.expire_compaction_sweep();
		// Other nodes write the store, so no batch ends the drain for holding
		// the whole queue: it ends on a lap over nothing but its kept request.
		ds.triggers.assume_shared_store();

		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;

		assert_eq!(
			index_compaction_queue_len(&ds, &fenced).await?,
			1,
			"the fenced request is kept"
		);
		let txn = ds.transaction(Read).await?;
		let left = catch!(txn, txn.get_key(&residual, None).await);
		txn.cancel().await?;
		assert!(left.is_none(), "the drain swept and folded the stranded residual");
		Ok(())
	}

	#[tokio::test]
	async fn index_compaction_folds_a_multi_batch_backlog_of_kept_requests_to_one() -> Result<()> {
		assert_a_multi_batch_backlog_of_kept_requests_folds_to_one(false).await
	}

	#[tokio::test]
	async fn index_compaction_folds_a_multi_batch_backlog_of_kept_requests_to_one_on_a_shared_store()
	-> Result<()> {
		assert_a_multi_batch_backlog_of_kept_requests_folds_to_one(true).await
	}

	/// A drain over nothing but requests it keeps passes each once and returns
	/// with them still queued, on a store other nodes write to, where a drain
	/// otherwise runs until the queue is empty. Keeping a request writes
	/// nothing, so the drain leaves no wake behind it, and the next drain
	/// passes the kept request once more.
	#[tokio::test]
	async fn index_compaction_over_kept_requests_ends_without_a_wake() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.triggers.assume_shared_store();
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		queue_compaction_requests(&ds, &ikb, 3).await?;
		set_build_owner(&ds, &ikb, Some(Uuid::now_v7())).await?;
		take_compaction_wake(&ds);

		for drain in 1..=2 {
			let (batches, errors) = tokio::time::timeout(
				Duration::from_secs(30),
				Datastore::index_compaction(
					Arc::clone(&ds),
					Duration::from_secs(60),
					CancellationToken::new(),
				),
			)
			.await
			.unwrap_or_else(|_| panic!("drain {drain} over kept requests must end"))?;
			assert_eq!(errors, 0);
			assert_eq!(batches, 1, "drain {drain} must pass the kept request once");
			assert_eq!(index_compaction_queue_len(&ds, &ikb).await?, 1);
			assert!(!take_compaction_wake(&ds), "drain {drain} must not wake the compactor");
		}
		Ok(())
	}

	/// The compaction-queue drain must never delete more than one batch of
	/// `/!ic` entries per write transaction: the queue grows by one entry per
	/// indexed record write, so an unbounded cleanup transaction would carry
	/// one write per accumulated entry — the per-key reservation profile that
	/// can starve distributed backends.
	#[tokio::test]
	async fn index_compaction_queue_cleanup_is_bounded() -> Result<()> {
		let batch = INDEX_COMPACTION_QUEUE_BATCH_SIZE as usize;
		// A backlog of 2.5 batches forces multiple drain iterations.
		let backlog = batch * 2 + batch / 2;
		let observer = Arc::new(WriteTxSizeObserver::default());
		// Background jobs are off so the only transactions observed are the
		// ones this test drives. The maintenance task runs this very function
		// on the same datastore, and its lease admits a second pass on the same
		// node, so one landing inside this drain both conflicts with the
		// cleanup commit — making it retry and re-observe its keys — and
		// commits batches of its own that the observer cannot distinguish from
		// this drain's.
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		{
			let txn = ds.transaction(Write).await?;
			txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
		}
		// A real index for the queue entries to reference. The table stays
		// empty (no record writes, which would each enqueue an entry of
		// their own), so compacting it performs no index writes and the
		// cycle's only large write transactions are the queue cleanups.
		execute_all(
			&ds,
			&session,
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			 DEFINE TABLE doc SCHEMALESS;
			 DEFINE INDEX ft_idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25;",
		)
		.await?;
		// The index's build queued a request of its own when it released
		// ownership. Drain it, so the queue holds exactly the backlog below.
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		let ikb = index_key_base(&ds, "doc", "ft_idx").await?;
		let node_id = ds.id();
		// Enqueue the backlog directly: each key is unique, exactly as the
		// per-record-write enqueue path produces them.
		{
			let txn = ds.transaction(Write).await?;
			for _ in 0..backlog {
				txn.set_key(&ikb.new_ic_key(node_id), &()).await?;
			}
			txn.commit().await?;
		}
		// Only observe the compaction cycle's own transactions.
		observer.clear();

		let (batches, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(batches, 3, "expected the backlog to drain in ceil(2.5) batches");

		// The queue is fully drained.
		let remaining = {
			let txn = ds.transaction(Read).await?;
			let res = txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await;
			let _ = txn.cancel().await;
			res?
		};
		assert!(remaining.is_empty(), "queue not drained: {} entries left", remaining.len());

		// No write transaction exceeded the batch bound, and the cleanups
		// were exactly [batch, batch, batch/2].
		let sizes = observer.sizes();
		assert!(
			sizes.iter().all(|&n| n <= batch as u64),
			"a write transaction exceeded the {batch}-key bound: {sizes:?}"
		);
		let cleanups = cleanup_sizes(&sizes);
		assert_eq!(
			cleanups,
			vec![batch as u64, batch as u64, (batch / 2) as u64],
			"unexpected cleanup transaction sizes"
		);
		Ok(())
	}

	/// Queue keys sort by index before their time-ordered UUID, so a
	/// start-anchored scan would keep serving an early-sorting index for as
	/// long as it has (or keeps receiving) entries. The drain must instead
	/// rotate: after a batch, the scan resumes past that batch's last index,
	/// reaching later-sorting indexes before returning for the remainder.
	#[tokio::test]
	async fn index_compaction_queue_drain_rotates_across_indexes() -> Result<()> {
		let batch = INDEX_COMPACTION_QUEUE_BATCH_SIZE as usize;
		let observer = Arc::new(WriteTxSizeObserver::default());
		// Background jobs are off, for the same reason as the bounded test
		// above: the maintenance task runs this very function, and a second
		// pass inside this drain would both conflict with the cleanup commits
		// and contribute batches of its own to the observed sequence.
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		{
			let txn = ds.transaction(Write).await?;
			txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
		}
		// Two empty indexed tables whose names order their queue entries:
		// `aaa` sorts before `zzz`.
		execute_all(
			&ds,
			&session,
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			 DEFINE TABLE aaa SCHEMALESS;
			 DEFINE TABLE zzz SCHEMALESS;
			 DEFINE INDEX ft_a ON aaa FIELDS text FULLTEXT ANALYZER simple BM25;
			 DEFINE INDEX ft_z ON zzz FIELDS text FULLTEXT ANALYZER simple BM25;",
		)
		.await?;
		// Each index's build queued a request of its own when it released
		// ownership. Drain them, so the queue holds exactly the backlog below.
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		let ikb_a = index_key_base(&ds, "aaa", "ft_a").await?;
		let ikb_z = index_key_base(&ds, "zzz", "ft_z").await?;
		let node_id = ds.id();
		// 1.5 batches for the early index, a handful for the late one.
		{
			let txn = ds.transaction(Write).await?;
			for _ in 0..(batch + batch / 2) {
				txn.set_key(&ikb_a.new_ic_key(node_id), &()).await?;
			}
			for _ in 0..10 {
				txn.set_key(&ikb_z.new_ic_key(node_id), &()).await?;
			}
			txn.commit().await?;
		}
		observer.clear();

		let (batches, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(batches, 3);

		// The queue is fully drained. An abandoned cleanup leaves entries
		// behind without moving `errors` or `batches`, so the sequence below
		// cannot stand in for this.
		let remaining = {
			let txn = ds.transaction(Read).await?;
			let res = txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await;
			let _ = txn.cancel().await;
			res?
		};
		assert!(remaining.is_empty(), "queue not drained: {} entries left", remaining.len());

		// Rotation signature: after the first full batch of the early index,
		// the late index is served before the early index's remainder.
		let sizes = observer.sizes();
		let cleanups = cleanup_sizes(&sizes);
		assert_eq!(
			cleanups,
			vec![batch as u64, 10, (batch / 2) as u64],
			"expected the late-sorting index to be drained between the early index's batches"
		);
		Ok(())
	}

	/// One statement writing many documents must leave exactly one `!iu`
	/// count-delta entry and one `/!ic` compaction request, not one of each per
	/// document.
	///
	/// Reads of a COUNT index sum every un-compacted `!iu` entry, so the
	/// per-document fan-out made `count()` cost scale with write volume and made
	/// the compaction queue grow at the same rate. Aggregating per transaction
	/// keeps the contention-free property the delta log exists for (the entry is
	/// still a blind write under a key no other transaction shares) while
	/// removing that scaling.
	#[tokio::test]
	async fn count_index_writes_one_delta_per_transaction() -> Result<()> {
		const ROWS: usize = 500;
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;",
		)
		.await?;
		// The index's build queued a request of its own when it released
		// ownership. Drain it, so the queue holds only what the write adds.
		Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(1),
			CancellationToken::new(),
		)
		.await?;
		let ikb = index_key_base(&ds, "item", "idx_total").await?;

		execute_all(&ds, &session, &format!("CREATE |item:1..={ROWS}| RETURN NONE")).await?;

		let (deltas, queued) = {
			let txn = ds.transaction(Read).await?;
			let iu = IndexCountPrefix {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
			}
			.range()?;
			let deltas = catch!(txn, txn.keys(iu, u32::MAX, 0, None).await);
			let queued =
				catch!(txn, txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await);
			let _ = txn.cancel().await;
			(deltas, queued)
		};

		assert_eq!(
			deltas.len(),
			1,
			"{ROWS} rows in one statement must leave one count-delta entry, found {}",
			deltas.len()
		);
		assert_eq!(
			queued.len(),
			1,
			"{ROWS} rows in one statement must enqueue one compaction request, found {}",
			queued.len()
		);

		// The single entry carries the net delta, so the count is still right.
		let sum = {
			let mut sum: i64 = 0;
			for key in &deltas {
				let iu = IndexCountKey::decode_key(key)?;
				let delta = i64::try_from(iu.count).expect("count delta out of range");
				sum += if iu.pos {
					delta
				} else {
					-delta
				};
			}
			sum
		};
		assert_eq!(sum, ROWS as i64, "the aggregated entry must carry the net delta");
		Ok(())
	}

	/// A commit that queues compaction work must wake the compactor, rather
	/// than leaving the queue to sit until the next tick.
	///
	/// The compaction interval and the write rate are otherwise unrelated, and
	/// that gap is what lets a count index's delta log — which every read of the
	/// index sums — grow without bound. The notification is what ties the two
	/// together.
	#[tokio::test]
	async fn commit_queuing_compaction_work_wakes_the_compactor() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;",
		)
		.await?;

		// Wait on the trigger the background compactor waits on. Registering
		// before the write is what makes this deterministic rather than a race:
		// `Notify` also stores one permit, so a notification that lands before
		// the await still satisfies it. For the same reason the permit the
		// index's own build stored, when it released ownership and queued a
		// compaction request, is taken first: left in place, it would satisfy
		// the waiter without the write.
		let triggers = Arc::clone(ds.commit_triggers());
		let _ = triggers.index_compaction.notified().now_or_never();
		let waiter = tokio::spawn(async move {
			triggers.index_compaction.notified().await;
		});

		execute_all(&ds, &session, "CREATE item:1 RETURN NONE").await?;

		tokio::time::timeout(Duration::from_secs(5), waiter)
			.await
			.expect("a commit that queues compaction work must notify the compactor")
			.expect("the waiter task must not panic");
		Ok(())
	}

	/// A commit that queues no compaction work must not wake the compactor, so
	/// unrelated write traffic does not keep the lease check spinning.
	#[tokio::test]
	async fn commit_without_compaction_work_does_not_wake_the_compactor() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// No COUNT (or other compaction-eligible) index, so nothing to queue.
		execute_all(&ds, &session, "DEFINE TABLE item SCHEMALESS;").await?;

		let triggers = Arc::clone(ds.commit_triggers());
		let waiter = tokio::spawn(async move {
			triggers.index_compaction.notified().await;
		});

		execute_all(&ds, &session, "CREATE item:1 RETURN NONE").await?;

		let woke = tokio::time::timeout(Duration::from_millis(300), waiter).await;
		assert!(woke.is_err(), "a write with no compaction work must not wake the compactor");
		Ok(())
	}

	/// Queues `count` async events and returns how many the event ran.
	async fn queue_async_events(ds: &Datastore, session: &Session, count: usize) -> Result<()> {
		execute_all(
			ds,
			session,
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE logged SCHEMALESS;
			 DEFINE EVENT log ON person ASYNC THEN (CREATE logged SET who = $after.id);",
		)
		.await?;
		execute_all(ds, session, &format!("CREATE |person:{count}| RETURN NONE;")).await
	}

	async fn logged_count(ds: &Datastore, session: &Session) -> Result<usize> {
		let mut res = ds.execute("SELECT * FROM logged", session, None).await?;
		match res.remove(0).result? {
			PublicValue::Array(a) => Ok(a.len()),
			other => panic!("expected an array, got {other:?}"),
		}
	}

	/// Defines an async event with the given `RETRY` budget and queues exactly
	/// one of it.
	async fn queue_one_async_event(ds: &Datastore, session: &Session, retry: u16) -> Result<()> {
		execute_all(
			ds,
			session,
			&format!(
				"DEFINE TABLE person SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT log ON person ASYNC RETRY {retry} \
				 THEN (CREATE logged SET who = $after.id);"
			),
		)
		.await?;
		execute_all(ds, session, "CREATE person:1 RETURN NONE;").await
	}

	/// Drains the queue the way [`Datastore::event_processing`] does, but with a
	/// bound, so a queue that stops draining fails the test instead of hanging
	/// it — which is the shape the failures under test take.
	async fn drain_event_queue(ds: &Datastore) -> Result<()> {
		for _ in 0..20 {
			if process_next_events_batch(ds, None).await? == 0 {
				return Ok(());
			}
		}
		panic!("the async event queue did not drain");
	}

	/// A retryable conflict on the commit that ends a queued event reports
	/// contention, not a failure of the event: the event still has to run, has
	/// to run exactly once, and must not spend the `RETRY` budget the definition
	/// reserved for a failing event. `RETRY 0` is what makes that last part
	/// observable — one charged attempt would drop the event instead of running
	/// it.
	///
	/// It pins the recovery path as well. `Transaction::commit` leaves the
	/// transaction finished whether or not it succeeds, so cancelling it
	/// afterwards reports `TransactionFinished` in place of the conflict; the
	/// entry is then neither re-run nor removed, and every later batch fetches
	/// and fails it again.
	#[test_log::test(tokio::test)]
	async fn a_queued_event_survives_commit_conflicts_without_spending_its_retries() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = RetryableConflictSite::AsyncEventCommit;
		// Fewer than the in-place bound, so every conflict is absorbed there.
		let _guard = inject_retryable_conflicts(site, ds.id(), 2);

		// Counting passes is what separates the two failures. Both conflicts are
		// absorbed within the pass that picked the entry up, so the first pass
		// finishes the event and the second finds the queue empty. An attempt
		// that ended by reporting `TransactionFinished` would leave the entry
		// for a later pass to fetch and fail again, and the second pass would
		// still find it.
		assert_eq!(
			process_next_events_batch(&ds, None).await?,
			1,
			"the queued event must be the one entry this pass picks up"
		);
		assert_eq!(
			process_next_events_batch(&ds, None).await?,
			0,
			"the entry must be gone: its conflicts belong to the pass that ran it"
		);

		assert_eq!(
			retryable_conflict_count(site, ds.id()),
			0,
			"both injected conflicts must have been raised, or the commit was never re-driven"
		);
		assert_eq!(
			logged_count(&ds, &session).await?,
			1,
			"the event must run exactly once; the conflicting attempts are rolled back"
		);
		Ok(())
	}

	/// Conflicts that outlast the in-place bound fall through to the
	/// definition's own `RETRY` budget, so an event that never wins the race
	/// still reaches a decision and its entry leaves the queue. Without that the
	/// drain loop does not return: it re-scans and re-fails the same entry for
	/// as long as this node holds the lease.
	#[test_log::test(tokio::test)]
	async fn a_queued_event_that_keeps_conflicting_leaves_the_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = RetryableConflictSite::AsyncEventCommit;
		// More than any run can consume, so no attempt ever commits.
		let _guard = inject_retryable_conflicts(site, ds.id(), usize::MAX);

		drain_event_queue(&ds).await?;

		assert_eq!(
			logged_count(&ds, &session).await?,
			0,
			"no attempt committed, so the event left nothing behind"
		);
		Ok(())
	}

	/// Reads the queue the way a batch does, without running anything.
	async fn fetch_event_queue(ds: &Datastore) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let tx = ds.transaction(Read).await?;
		let res =
			tx.scan_raw(crate::key::schema::EventQueuePrefix {}.range()?, 1000, 0, None).await;
		tx.cancel().await?;
		res
	}

	/// A batch that fetched an entry another consumer has since run must not
	/// run it again. Batches overlap once the event-processing lease lapses, so
	/// two nodes can hold the same entry; only the first to commit runs it.
	#[test_log::test(tokio::test)]
	async fn an_event_run_by_an_overlapping_batch_is_not_run_again() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 1).await?;

		let stale = fetch_event_queue(&ds).await?;
		assert_eq!(stale.len(), 1, "the queued event must be the one entry fetched");
		drain_event_queue(&ds).await?;
		assert_eq!(logged_count(&ds, &session).await?, 1, "the first batch runs the event");

		crate::doc::process_events_batch(&ds, stale, None).await?;

		assert_eq!(
			logged_count(&ds, &session).await?,
			1,
			"the overlapping batch must find the entry gone rather than run it again"
		);
		assert!(
			fetch_event_queue(&ds).await?.is_empty(),
			"the overlapping batch must not requeue an entry that already ran"
		);
		Ok(())
	}

	/// A commit that applied but reported an unknown outcome has run the event
	/// and removed its entry. Requeueing on that report would run the event
	/// twice: `RETRY 1` is what makes that observable, since a charged attempt
	/// would put the entry back for a second pass to run.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_applied_unknown_is_not_run_again() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 1).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitAppliedUnknown;
		let _guard = inject_non_retryable_error(site, ds.id());

		assert_eq!(process_next_events_batch(&ds, None).await?, 1);
		assert_eq!(
			process_next_events_batch(&ds, None).await?,
			0,
			"the applied commit removed the entry, so nothing may have put it back"
		);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event must run exactly once");
		Ok(())
	}

	/// A commit that reported an unknown outcome without applying has neither
	/// run the event nor removed its entry. Deleting the entry on that report
	/// would lose the event: `RETRY 0` is what makes that observable, since a
	/// charged attempt would exhaust the budget and delete it.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_was_discarded_unknown_still_runs() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown;
		let _guard = inject_non_retryable_error(site, ds.id());

		assert_eq!(process_next_events_batch(&ds, None).await?, 1);
		assert_eq!(
			process_next_events_batch(&ds, None).await?,
			0,
			"the unknown outcome belongs to the pass that ran the event"
		);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event must still run, once");
		Ok(())
	}

	/// An outcome that stays unknown past the in-place bound is never charged
	/// to the `RETRY` budget: the entry is left as the last commit left it, and
	/// a later pass runs it or finds it gone. `RETRY 0` is what makes that
	/// observable, since a charged attempt would delete the entry.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_outcome_stays_unknown_is_left_queued() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown;
		// Every attempt of the first pass: the first run and each in-place re-run.
		let _guard = inject_non_retryable_errors(site, ds.id(), 4);

		assert_eq!(process_next_events_batch(&ds, None).await?, 1);
		assert_eq!(logged_count(&ds, &session).await?, 0, "no attempt of the first pass applied");
		assert_eq!(
			fetch_event_queue(&ds).await?.len(),
			1,
			"an unknown outcome must leave the entry queued rather than delete it"
		);

		drain_event_queue(&ds).await?;
		assert_eq!(logged_count(&ds, &session).await?, 1, "a later pass runs the event");
		Ok(())
	}

	/// A cancelled pass must return at the next batch boundary rather than drain
	/// the queue. Async events run user-defined SurrealQL of unbounded duration,
	/// so a pass that ignored cancellation would hold [`Datastore::shutdown`]
	/// open for as long as the queue kept refilling.
	#[test_log::test(tokio::test)]
	async fn event_processing_returns_immediately_once_cancelled() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 50).await?;

		let canceller = CancellationToken::new();
		canceller.cancel();
		// Cancellation is not an error: the queue is durable, so what is left is
		// picked up by the next pass here or on another node.
		ds.event_processing(Duration::from_secs(1), &canceller).await?;
		assert_eq!(
			logged_count(&ds, &session).await?,
			0,
			"a cancelled pass must not process the queue"
		);
		Ok(())
	}

	/// The control for the test above: with a live token the same queue drains,
	/// so the early return is cancellation and not a broken pass.
	#[test_log::test(tokio::test)]
	async fn event_processing_drains_the_queue_when_not_cancelled() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 50).await?;

		ds.event_processing(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(
			logged_count(&ds, &session).await?,
			50,
			"an uncancelled pass must drain the queue"
		);
		Ok(())
	}

	/// Two owners of one datastore may both call `shutdown`, which the sequence
	/// serialises so a second caller cannot close the storage engine while the
	/// first is still waiting on a maintenance pass.
	///
	/// This covers the liveness half of that: both callers complete and neither
	/// blocks on the other's guard. The exclusion itself is a property of holding
	/// the guard across the whole sequence, not something observable from
	/// outside without a test hook in the shutdown path.
	#[test_log::test(tokio::test)]
	async fn concurrent_shutdown_calls_both_complete() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let (first, second) = tokio::time::timeout(Duration::from_secs(30), async {
			tokio::join!(ds.shutdown(), ds.shutdown())
		})
		.await
		.map_err(|_| anyhow::anyhow!("concurrent shutdown deadlocked"))?;
		first?;
		second?;
		Ok(())
	}

	/// Mutations that cancel out within one transaction write no entry at all,
	/// and a read inside that transaction still sees its own uncommitted work.
	#[tokio::test]
	async fn count_index_nets_out_within_a_transaction() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_total ON item COUNT;",
		)
		.await?;
		let ikb = index_key_base(&ds, "item", "idx_total").await?;

		// A create and a delete of the same rows inside one transaction net to
		// zero, so there is no delta worth recording.
		execute_all(
			&ds,
			&session,
			"BEGIN;
			 CREATE |item:1..=50| RETURN NONE;
			 DELETE item:1..=50 RETURN NONE;
			 COMMIT;",
		)
		.await?;

		let deltas = {
			let txn = ds.transaction(Read).await?;
			let iu = IndexCountPrefix {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
			}
			.range()?;
			let res = catch!(txn, txn.keys(iu, u32::MAX, 0, None).await);
			let _ = txn.cancel().await;
			res
		};
		assert!(
			deltas.is_empty(),
			"a net-zero transaction must write no delta, found {}",
			deltas.len()
		);

		// A read later in the same transaction as its writes must observe them,
		// even though the aggregate is not flushed until commit.
		let res = ds
			.execute(
				"BEGIN;
				 CREATE |item:100..=109| RETURN NONE;
				 SELECT count() FROM item GROUP ALL;
				 COMMIT;",
				&session,
				None,
			)
			.await?;
		// Locate the SELECT's result by content rather than by position, so the
		// assertion does not depend on how many results the surrounding
		// BEGIN/COMMIT contribute.
		let mut counted = None;
		for r in res {
			let rendered = format!("{:?}", r.result?);
			if rendered.contains("count") {
				counted = Some(rendered);
			}
		}
		let counted = counted.expect("the SELECT must produce a result");
		assert!(
			counted.contains("Int(10)"),
			"a read must see its own transaction's buffered deltas, got {counted}"
		);
		Ok(())
	}

	/// An undecodable `/!ic` entry must not wedge the drain: it names no
	/// index to compact, so the cycle skips it with a warning, deletes it
	/// with its batch, and continues serving the decodable entries.
	#[tokio::test]
	async fn index_compaction_quarantines_undecodable_queue_keys() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		let ds = ds;
		execute_all(
			&ds,
			&session,
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
			 DEFINE TABLE doc SCHEMALESS;
			 DEFINE INDEX ft_idx ON doc FIELDS text FULLTEXT ANALYZER simple BM25;",
		)
		.await?;
		let ikb = index_key_base(&ds, "doc", "ft_idx").await?;
		let node_id = ds.id();
		{
			let txn = ds.transaction(Write).await?;
			// The corrupt entry's 0xff lead byte sorts it after every valid
			// entry, so it lands last in the batch and exercises the
			// cursor-seek decode fallback as well as the dedupe-loop skip.
			for _ in 0..5 {
				txn.set_key(&ikb.new_ic_key(node_id), &()).await?;
			}
			txn.set(Key::from(b"/!ic\xff-not-a-valid-entry".to_vec()), vec![]).await?;
			txn.commit().await?;
		}

		let (batches, errors) = Datastore::index_compaction(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert!(batches >= 1);
		assert_eq!(errors, 0, "a corrupt queue entry must not count as a compaction failure");

		// The queue is fully drained, corrupt entry included.
		let remaining = {
			let txn = ds.transaction(Read).await?;
			let res = txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await;
			let _ = txn.cancel().await;
			res?
		};
		assert!(remaining.is_empty(), "queue not drained: {} entries left", remaining.len());
		Ok(())
	}

	#[tokio::test]
	async fn archive_node_for_shutdown_reports_success() {
		let outcome = archive_node_for_shutdown(Duration::from_secs(60), Ok(()));

		assert_eq!(outcome, ShutdownNodeDeleteOutcome::Archived);
	}

	#[tokio::test]
	async fn archive_node_for_shutdown_reports_failure() {
		let outcome = archive_node_for_shutdown(
			Duration::from_secs(60),
			Err(anyhow::anyhow!("delete failed")),
		);

		assert_eq!(outcome, ShutdownNodeDeleteOutcome::Failed);
	}

	#[tokio::test]
	async fn archive_node_for_shutdown_reports_timeout() {
		let outcome = archive_node_for_shutdown(
			Duration::from_millis(1),
			Err(anyhow::Error::new(EngineError::QueryTimedout(Duration::from_millis(1)))),
		);

		assert_eq!(outcome, ShutdownNodeDeleteOutcome::TimedOut);
	}

	#[tokio::test]
	async fn node_tx_step_cancels_after_timeout() {
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(Write).await.unwrap();
		let timeout_duration = Duration::from_millis(10);

		let err = await_node_tx_step(
			&txn,
			Instant::now() + timeout_duration,
			timeout_duration,
			None,
			pending::<Result<()>>(),
		)
		.await
		.unwrap_err();

		assert!(matches!(crate::err::engine_error(&err), Some(EngineError::QueryTimedout(_))));
		assert!(txn.closed());
	}

	#[tokio::test]
	async fn node_tx_step_cancels_after_cancellation() {
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(Write).await.unwrap();
		let canceller = CancellationToken::new();
		canceller.cancel();

		let err = await_node_tx_step(
			&txn,
			Instant::now() + Duration::from_secs(60),
			Duration::from_secs(60),
			Some(&canceller),
			pending::<Result<()>>(),
		)
		.await
		.unwrap_err();

		assert!(matches!(crate::err::engine_error(&err), Some(EngineError::QueryCancelled)));
		assert!(txn.closed());
	}

	#[tokio::test]
	async fn node_tx_step_cancels_after_error() {
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(Write).await.unwrap();

		let err = await_node_tx_step(
			&txn,
			Instant::now() + Duration::from_secs(60),
			Duration::from_secs(60),
			None,
			async { Err::<(), _>(anyhow::anyhow!("step failed")) },
		)
		.await
		.unwrap_err();

		assert_eq!(err.to_string(), "step failed");
		assert!(txn.closed());
	}

	#[tokio::test]
	async fn node_tx_step_success_leaves_transaction_open() {
		let ds = Datastore::new("memory").await.unwrap();
		let txn = ds.transaction(Write).await.unwrap();

		await_node_tx_step(
			&txn,
			Instant::now() + Duration::from_secs(60),
			Duration::from_secs(60),
			None,
			async { Ok::<_, anyhow::Error>(()) },
		)
		.await
		.unwrap();

		assert!(!txn.closed());
		txn.commit().await.unwrap();
		assert!(txn.closed());
	}

	#[tokio::test]
	async fn node_heartbeat_age_is_small_after_insert() {
		let ds = Datastore::new("memory").await.unwrap();
		// `insert_node` writes the current node's heartbeat at `clock_now()`.
		ds.insert_node().await.unwrap();
		let age = ds.node_heartbeat_age().await.unwrap().expect("the node row was just written");
		assert!(age < Duration::from_secs(5), "heartbeat should be fresh, got {age:?}");
	}

	/// A node with no membership row reports no heartbeat, rather than the read
	/// failure `get_node` raises for the same state. Readiness depends on the
	/// distinction: before `insert_node` runs, and after a peer archives and
	/// reaps this node, the row is legitimately absent.
	#[tokio::test]
	async fn node_heartbeat_age_is_absent_without_a_node_row() {
		let ds = Datastore::new("memory").await.unwrap();
		assert!(
			ds.node_heartbeat_age().await.unwrap().is_none(),
			"an unregistered node reported a heartbeat"
		);
	}

	#[tokio::test]
	async fn node_heartbeat_age_reflects_a_stale_heartbeat() {
		let ds = Datastore::new("memory").await.unwrap();
		// Write this node's registration with a heartbeat 60s in the past.
		let now = ds.clock_now().value;
		let stale = Node::new(
			ds.id(),
			Timestamp {
				value: now.saturating_sub(60_000),
			},
			false,
		);
		let key = NodeKey {
			nd: ds.id(),
		};
		let txn = ds.transaction(Write).await.unwrap();
		txn.set_key(&key, &stale).await.unwrap();
		txn.commit().await.unwrap();
		// The reported age should reflect the stale heartbeat.
		let age = ds.node_heartbeat_age().await.unwrap().expect("the node row was just written");
		assert!(age >= Duration::from_secs(59), "heartbeat should be stale, got {age:?}");
	}

	#[tokio::test]
	async fn test_setup_superuser() {
		let ds = Datastore::new("memory").await.unwrap();
		let username = "root";
		let password = "root";

		// Setup the initial user if there are no root users
		{
			let txn = ds.transaction(Read).await.unwrap();
			assert_eq!(txn.all_root_users(None).await.unwrap().len(), 0);
			txn.cancel().await.unwrap();
		}
		ds.initialise_credentials(username, password).await.unwrap();
		{
			let txn = ds.transaction(Read).await.unwrap();
			assert_eq!(txn.all_root_users(None).await.unwrap().len(), 1);
			txn.cancel().await.unwrap();
		}
		verify_root_creds(&ds, username, password).await.unwrap();

		// Do not setup the initial root user if there are root users:
		// Test the scenario by making sure the custom password doesn't change.
		let sql = "DEFINE USER root ON ROOT PASSWORD 'test' ROLES OWNER";
		let sess = Session::owner();
		ds.execute(sql, &sess, None).await.unwrap();
		let pass_hash = {
			let txn = ds.transaction(Read).await.unwrap();
			let res = txn.expect_root_user(username).await.unwrap().hash.clone();
			txn.cancel().await.unwrap();
			res
		};

		ds.initialise_credentials(username, password).await.unwrap();
		{
			let txn = ds.transaction(Read).await.unwrap();
			assert_eq!(pass_hash, txn.expect_root_user(username).await.unwrap().hash.clone());
			txn.cancel().await.unwrap();
		}
	}

	#[tokio::test]
	async fn cross_transaction_caching_uuids_updated() -> Result<()> {
		let (send, _recv) = crate::channel::bounded(surrealdb_cnf::NOTIFICATIONS_CHANNEL_SIZE);
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.with_notify(send)
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);

		let db = {
			let txn = ds.transaction(TransactionType::Write).await?;
			let db = txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
			db
		};

		// Define the table, set the initial uuids
		let initial = {
			let sql = r"DEFINE TABLE test;".to_owned();
			let res = &mut ds.execute(&sql, &ses, None).await?;
			assert_eq!(res.len(), 1);
			res.remove(0).result.unwrap();
			// Obtain the initial uuids
			let txn = ds.transaction(TransactionType::Read).await?;
			let tb = TableName::from("test");
			let initial = txn.get_tb(db.namespace_id, db.database_id, &tb, None).await?.unwrap();
			txn.cancel().await?;
			initial
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
		let after_define = {
			let txn = ds.transaction(TransactionType::Read).await?;
			let tb = TableName::from("test");
			let after_define =
				txn.get_tb(db.namespace_id, db.database_id, &tb, None).await?.unwrap();
			txn.cancel().await?;
			// Compare uuids after definitions
			assert_ne!(initial.cache_indexes_ts, after_define.cache_indexes_ts);
			assert_ne!(initial.cache_tables_ts, after_define.cache_tables_ts);
			assert_ne!(initial.cache_events_ts, after_define.cache_events_ts);
			assert_ne!(initial.cache_fields_ts, after_define.cache_fields_ts);
			// LIVE bumped the committed live-query cache timestamp.
			assert_ne!(initial.cache_lives_ts, after_define.cache_lives_ts);
			after_define
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
			let vars =
				PublicVariables::from(BTreeMap::from_iter(map! { "lqid".to_string() => lqid }));
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
			let txn = ds.transaction(TransactionType::Read).await?;
			let tb = TableName::from("test");
			let after_remove =
				txn.get_tb(db.namespace_id, db.database_id, &tb, None).await?.unwrap();
			txn.cancel().await?;
			// Compare uuids after definitions
			assert_ne!(after_define.cache_fields_ts, after_remove.cache_fields_ts);
			assert_ne!(after_define.cache_events_ts, after_remove.cache_events_ts);
			assert_ne!(after_define.cache_tables_ts, after_remove.cache_tables_ts);
			assert_ne!(after_define.cache_indexes_ts, after_remove.cache_indexes_ts);
			// KILL bumped the committed live-query cache timestamp.
			assert_ne!(after_define.cache_lives_ts, after_remove.cache_lives_ts);
		}
		//
		Ok(())
	}

	/// Every statement that defines, alters or removes a database-scoped
	/// object moves that object's catalog version to one it never held, and a
	/// removal leaves a version behind.
	#[tokio::test]
	async fn ddl_moves_each_objects_catalog_version() -> Result<()> {
		use surrealdb_datastore::values::catalog_version::CatalogObject;

		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test");
		let db = {
			let txn = ds.transaction(TransactionType::Write).await?;
			let db = txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
			db
		};
		let cases: [(CatalogObject<'_>, [&str; 3]); 6] = [
			(
				CatalogObject::Function("f"),
				[
					"DEFINE FUNCTION fn::f() { RETURN 1; }",
					"DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2; }",
					"REMOVE FUNCTION fn::f",
				],
			),
			(
				CatalogObject::Param("p"),
				["DEFINE PARAM $p VALUE 1", "ALTER PARAM $p VALUE 2", "REMOVE PARAM $p"],
			),
			(
				CatalogObject::Analyzer("az"),
				[
					"DEFINE ANALYZER az TOKENIZERS blank",
					"DEFINE ANALYZER OVERWRITE az TOKENIZERS class",
					"REMOVE ANALYZER az",
				],
			),
			(
				CatalogObject::Sequence("s"),
				["DEFINE SEQUENCE s", "ALTER SEQUENCE s TIMEOUT 5s", "REMOVE SEQUENCE s"],
			),
			(
				CatalogObject::Access("a"),
				[
					"DEFINE ACCESS a ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'one'",
					"DEFINE ACCESS OVERWRITE a ON DATABASE TYPE JWT ALGORITHM HS256 KEY 'two'",
					"REMOVE ACCESS a ON DATABASE",
				],
			),
			(
				CatalogObject::Api("/x"),
				["DEFINE API \"/x\"", "ALTER API \"/x\" COMMENT \"altered\"", "REMOVE API \"/x\""],
			),
		];
		for (object, statements) in cases {
			let mut seen = vec![None];
			for sql in statements {
				let res = ds.execute(sql, &ses, None).await?;
				for response in res {
					response.result.map_err(|e| anyhow::anyhow!("{sql}: {e}"))?;
				}
				let txn = ds.transaction(TransactionType::Read).await?;
				let version =
					txn.catalog_object_version(db.namespace_id, db.database_id, object).await?;
				txn.cancel().await?;
				assert!(version.is_some(), "`{sql}` stamps {object:?}");
				assert!(!seen.contains(&version), "`{sql}` mints a version {object:?} never held");
				seen.push(version);
			}
		}
		Ok(())
	}

	/// Every statement that changes how reads or writes of a table behave —
	/// its definition, permissions and changefeed, its fields and references,
	/// its indexes, its events, and the views that read it — moves the
	/// table's catalog version; registering a live query does not.
	#[tokio::test]
	async fn ddl_under_a_table_moves_its_catalog_version() -> Result<()> {
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await?;
		let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
		let db = {
			let txn = ds.transaction(TransactionType::Write).await?;
			let db = txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
			db
		};
		let tb = TableName::from("t");
		let version = || async {
			let txn = ds.transaction(TransactionType::Read).await?;
			let version = txn.catalog_version(db.namespace_id, db.database_id, &tb).await?;
			txn.cancel().await?;
			Result::<_>::Ok(version)
		};
		ds.execute("DEFINE TABLE other", &ses, None).await?;
		let mut seen = vec![None];
		for sql in [
			"DEFINE TABLE t SCHEMALESS",
			"DEFINE TABLE OVERWRITE t SCHEMALESS PERMISSIONS FOR select NONE",
			"ALTER TABLE t COMMENT 'altered'",
			"DEFINE TABLE OVERWRITE t SCHEMALESS CHANGEFEED 1h",
			"DEFINE FIELD a ON t TYPE int",
			"DEFINE FIELD OVERWRITE a ON t TYPE option<int>",
			"DEFINE FIELD r ON t TYPE option<record<other>> REFERENCE",
			"ALTER FIELD a ON t COMMENT 'altered'",
			"DEFINE INDEX ia ON t FIELDS a",
			"REBUILD INDEX ia ON t",
			"REMOVE INDEX ia ON t",
			"DEFINE EVENT e ON t WHEN true THEN {}",
			"ALTER EVENT e ON t COMMENT 'altered'",
			"REMOVE EVENT e ON t",
			"DEFINE TABLE v AS SELECT * FROM t",
			"REMOVE TABLE v",
			"REMOVE FIELD a ON t",
			"REMOVE TABLE t",
		] {
			let res = ds.execute(sql, &ses, None).await?;
			for response in res {
				response.result.map_err(|e| anyhow::anyhow!("{sql}: {e}"))?;
			}
			let version = version().await?;
			assert!(version.is_some(), "`{sql}` stamps the table");
			assert!(!seen.contains(&version), "`{sql}` mints a version the table never held");
			seen.push(version);
		}
		ds.execute("DEFINE TABLE t", &ses, None).await?;
		let before = version().await?;
		let res = ds.execute("LIVE SELECT * FROM t", &ses, None).await?;
		for response in res {
			response.result?;
		}
		assert_eq!(version().await?, before, "a live query is not a catalog change");
		Ok(())
	}

	/// Exact-count accounting for the durable commits an indexed write incurs
	/// beyond its own statement transaction.
	///
	/// Groups every observed transaction completion by outcome, type and
	/// `keys_written`, so a per-record commit is distinguishable from a
	/// per-record cancel: a cancel costs nothing on a remote backend, a commit
	/// is a full durability round trip.
	#[derive(Default)]
	struct TxOutcomeObserver(std::sync::Mutex<Vec<(&'static str, bool, u64)>>);

	impl ExecutionObserver for TxOutcomeObserver {
		fn on_transaction_complete(&self, event: &crate::observe::TransactionEvent) {
			self.0.lock().unwrap().push((
				event.safe.outcome.as_label(),
				event.safe.write,
				event.safe.metrics.keys_written,
			));
		}
	}

	impl TxOutcomeObserver {
		fn take(&self) -> Vec<(&'static str, bool, u64)> {
			let mut g = self.0.lock().unwrap();
			std::mem::take(&mut *g)
		}
	}

	#[tokio::test]
	async fn doc_id_allocation_commits_per_record() -> Result<()> {
		// Windowed allocation makes the commit count flat in the row count. The
		// doc-ID window is `table_doc_ids_batch_size` wide (1000), so none of
		// these sizes exhausts one, and the builder commits per batch of
		// `INDEXING_BATCH_SIZE`. Persisting per allocation instead put this at
		// one commit per row, which is what the bounds below rule out: they sit
		// far under the smallest row count, so a per-row commit breaches them at
		// every size.
		const ROWS: [usize; 3] = [100, 200, 400];
		const INSERT_MAX: usize = 16;
		const BUILD_MAX: usize = 48;

		let mut observed = Vec::with_capacity(ROWS.len());
		for rows in ROWS {
			let (insert, build) = measure_doc_id_commits(rows).await?;
			assert!(
				insert <= INSERT_MAX,
				"{rows} indexed inserts took {insert} durable write commits, over the {INSERT_MAX} \
				 a windowed allocator needs"
			);
			assert!(
				build <= BUILD_MAX,
				"a DEFINE INDEX over {rows} rows took {build} durable write commits, over the \
				 {BUILD_MAX} a windowed allocator needs"
			);
			observed.push((insert, build));
		}

		// Flat in the row count, not merely bounded: quadrupling the rows must
		// not multiply the commits. A per-row commit shows up here as growth
		// proportional to `ROWS`, even were the caps above ever loosened.
		let (first_insert, first_build) = observed[0];
		let (last_insert, last_build) = observed[ROWS.len() - 1];
		let (first_rows, last_rows) = (ROWS[0], ROWS[ROWS.len() - 1]);
		assert!(
			last_insert <= first_insert * 2,
			"inserting {last_rows} rows took {last_insert} commits against {first_insert} for \
			 {first_rows}: the cost is tracking the row count"
		);
		assert!(
			last_build <= first_build * 2,
			"building over {last_rows} rows took {last_build} commits against {first_build} for \
			 {first_rows}: the cost is tracking the row count"
		);

		Ok(())
	}

	async fn measure_doc_id_commits(rows: usize) -> Result<(usize, usize)> {
		let observer = Arc::new(TxOutcomeObserver::default());
		let ds = Datastore::builder()
			.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		{
			let txn = ds.transaction(Write).await?;
			txn.ensure_ns_db(None, "test", "test").await?;
			txn.commit().await?;
		}

		let report = |label: &str, obs: Vec<(&'static str, bool, u64)>| -> usize {
			let mut h: BTreeMap<(&'static str, bool, u64), usize> = BTreeMap::new();
			for e in &obs {
				*h.entry(*e).or_default() += 1;
			}
			// A rollback reports `Outcome::Cancelled`, so a `success` write
			// completion is exactly a durable commit. Of those, the single-key
			// ones are the sequence state writes this test is about: nothing
			// else on this path commits exactly one key.
			let durable_writes = obs.iter().filter(|(o, w, _)| *o == "success" && *w).count();
			let seq_state = obs.iter().filter(|(o, w, k)| *o == "success" && *w && *k == 1).count();
			let per_row = durable_writes as f64 / rows as f64;
			println!(
				"  {label}: {durable_writes} durable write commits ({per_row:.3}/row), of which {seq_state} single-key sequence-state; {} completions total",
				obs.len()
			);
			for ((outcome, write, keys), n) in h {
				let kind = if write {
					"write"
				} else {
					"read "
				};
				println!("      {n:>4} x [{outcome:>9} {kind} keys_written={keys}]");
			}
			durable_writes
		};

		// Case A: inserts into a table that already carries a b-tree index.
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_name ON item FIELDS name;",
		)
		.await?;
		let _ = observer.take();
		execute_all(&ds, &session, &format!("CREATE |item:1..={rows}| SET name = 'x' RETURN NONE"))
			.await?;
		let insert_obs = observer.take();

		// Case B: DEFINE INDEX over rows that already exist.
		execute_all(&ds, &session, "DEFINE TABLE other SCHEMALESS;").await?;
		execute_all(
			&ds,
			&session,
			&format!("CREATE |other:1..={rows}| SET name = 'x' RETURN NONE"),
		)
		.await?;
		let _ = observer.take();
		execute_all(&ds, &session, "DEFINE INDEX idx_other ON other FIELDS name;").await?;
		let build_obs = observer.take();

		println!("\n  rows={rows}");
		let insert = report("insert", insert_obs);
		let build = report("build ", build_obs);

		Ok((insert, build))
	}

	/// A doc-ID is issued once. The forward map (record -> id) and the reverse
	/// map (id -> record) therefore hold one entry per record: reissuing an id
	/// overwrites a reverse entry, which leaves the reverse map short while the
	/// forward map stays whole.
	///
	/// The restart lands mid-window — the doc-ID batch is 1000 and far fewer
	/// records are written — which is the case a persisted per-allocation
	/// cursor must cover, as must the window's committed end. A
	/// restart keeps the node id, which is what makes the persisted allocator
	/// state reachable at all: it is keyed per node.
	#[tokio::test]
	async fn doc_ids_stay_unique_across_mid_batch_restart() -> Result<()> {
		const BEFORE: usize = 30;
		const AFTER: usize = 30;

		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE item SCHEMALESS;
			 DEFINE INDEX idx_tag ON item FIELDS tag;",
		)
		.await?;
		execute_all(
			&ds,
			&session,
			&format!("CREATE |item:1..={BEFORE}| SET tag = 'x' RETURN NONE"),
		)
		.await?;

		let ds = Arc::new(Arc::into_inner(ds).expect("sole owner of the datastore").restart());
		let lo = BEFORE + 1;
		let hi = BEFORE + AFTER;
		execute_all(&ds, &session, &format!("CREATE |item:{lo}..={hi}| SET tag = 'x' RETURN NONE"))
			.await?;

		let (forward, reverse) = doc_id_map_sizes(&ds, "item", "idx_tag").await?;

		assert_eq!(forward, BEFORE + AFTER, "one forward doc-ID mapping per record");
		assert_eq!(
			reverse,
			BEFORE + AFTER,
			"a reissued doc-ID overwrites a reverse mapping, leaving this map short"
		);

		Ok(())
	}

	/// The sizes of a table's doc-ID maps: the forward map keyed by record and
	/// the reverse map keyed by id. They hold one entry per record while every
	/// id is distinct; a reissued id overwrites a reverse entry, so the reverse
	/// map is where a collision becomes visible.
	async fn doc_id_map_sizes(ds: &Datastore, tb: &str, ix: &str) -> Result<(usize, usize)> {
		let ikb = index_key_base(ds, tb, ix).await?;
		let txn = ds.transaction(Read).await?;
		let forward = catch!(
			txn,
			txn.keys(
				DocLookupIdentityPrefix::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table()))
					.range()?,
				u32::MAX,
				0,
				None
			)
			.await
		);
		let reverse = catch!(
			txn,
			txn.keys(
				DocKeyPrefix::new(ikb.ns(), ikb.db(), Cow::Borrowed(ikb.table())).range()?,
				u32::MAX,
				0,
				None
			)
			.await
		);
		let _ = txn.cancel().await;
		Ok((forward.len(), reverse.len()))
	}

	/// A record nested to [`MAX_VALUE_DEPTH`], the deepest the storage layer
	/// accepts.
	///
	/// It is grown one level per statement so no statement in the source is
	/// itself deeply nested: the depth reaches storage without ever passing
	/// through the query parser, which is what a restore has to read back.
	async fn seed_deepest_storable_record(ds: &Datastore, session: &Session) -> Result<()> {
		execute_all(ds, session, "CREATE deep:1 SET data = 'leaf' RETURN NONE;").await?;
		// The record object is the first level and the leaf the last, so the
		// wrappers between them carry the rest.
		for _ in 0..MAX_VALUE_DEPTH - 2 {
			execute_all(ds, session, "UPDATE deep:1 SET data = { l: data } RETURN NONE;").await?;
		}
		Ok(())
	}

	/// The SurrealQL an export renders a database as.
	async fn export_sql(ds: &Datastore, session: &Session) -> Result<String> {
		let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
		let task = ds.export(session, tx).await?;
		let collector = tokio::spawn(async move {
			let mut out = Vec::new();
			while let Ok(chunk) = rx.recv().await {
				out.extend_from_slice(&chunk);
			}
			out
		});
		task.await?;
		Ok(String::from_utf8(collector.await?)?)
	}

	async fn empty_datastore(session: &Session) -> Result<Arc<Datastore>> {
		let ds = Datastore::new("memory").await?;
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, session.ns().unwrap().as_ref(), session.db().unwrap().as_ref())
			.await?;
		txn.commit().await?;
		Ok(ds)
	}

	/// A record at the deepest depth the storage layer accepts survives an
	/// export and a restore through both import entry points: what the writer
	/// accepted, an import can read back.
	///
	/// The comparison is between exports rather than query results: a
	/// statement result nests the record one level further than storage does,
	/// which the public-value conversion rejects, so a `SELECT` cannot observe
	/// a record at this depth at all.
	#[tokio::test]
	async fn deepest_storable_record_survives_the_round_trip() -> Result<()> {
		let session = Session::owner().with_ns("test").with_db("test");
		let source = empty_datastore(&session).await?;
		seed_deepest_storable_record(&source, &session).await?;
		let sql = export_sql(&source, &session).await?;

		let text_target = empty_datastore(&session).await?;
		for result in text_target.import(&sql, &session).await? {
			result.result?;
		}
		assert_eq!(export_sql(&text_target, &session).await?, sql, "restored from text");

		let stream_target = empty_datastore(&session).await?;
		let bytes = Bytes::from(sql.clone().into_bytes());
		let stream = futures::stream::once(async move { Ok(bytes) });
		for result in stream_target.import_stream(&session, stream).await? {
			result.result?;
		}
		assert_eq!(export_sql(&stream_target, &session).await?, sql, "restored from a stream");

		Ok(())
	}

	/// An import of `lets` distinct top-level `LET`s, then a record that shows
	/// whether the stream got past them.
	fn import_binding(lets: usize) -> String {
		let mut sql = String::from("OPTION IMPORT;\n");
		for i in 0..lets {
			sql.push_str(&format!("LET $p{i} = {i};\n"));
		}
		sql.push_str("CREATE marker:1;\n");
		sql
	}

	/// An import that binds [`IMPORT_CARRY_BOUND`] names runs to its end, and
	/// one that binds a name more fails the `LET` that crosses the bound and
	/// runs nothing after it, through both import entry points.
	#[tokio::test]
	async fn import_stops_at_the_let_that_crosses_the_carry_bound() -> Result<()> {
		let session = Session::owner().with_ns("test").with_db("test");
		let limit = IMPORT_CARRY_BOUND.names.expect("an import bounds the names it carries");

		let at_limit = empty_datastore(&session).await?;
		let sql = Bytes::from(import_binding(limit).into_bytes());
		let results = at_limit
			.execute_import(&session, None, futures::stream::once(async move { Ok(sql) }))
			.await?;
		assert!(results.is_empty(), "an import at the bound reports only failures: {results:?}");
		let rows = at_limit.execute("SELECT VALUE id FROM marker", &session, None).await?;
		assert_eq!(rows[0].result.as_ref().unwrap().as_array().unwrap().len(), 1);

		let past_limit = empty_datastore(&session).await?;
		let sql = Bytes::from(import_binding(limit + 1).into_bytes());
		let results = past_limit
			.import_stream(&session, futures::stream::once(async move { Ok(sql) }))
			.await?;
		assert_eq!(results.len(), 1, "only the crossing LET reports");
		let err = results[0].result.as_ref().unwrap_err();
		assert!(err.message().contains(&format!("limit of {limit}")), "got: {err:?}");
		// Nothing after the crossing ran, so no statement created the table.
		let rows = past_limit.execute("SELECT VALUE id FROM marker", &session, None).await?;
		let err = rows[0].result.as_ref().unwrap_err();
		assert!(err.message().contains("'marker' does not exist"), "got: {err:?}");

		Ok(())
	}

	/// Every depth budget an import is read under stays above the deepest
	/// value the storage layer accepts. Raising [`MAX_VALUE_DEPTH`] without
	/// the parser profile, or narrowing the profile, would re-open the gap
	/// where a record can be written but the export of it cannot be restored.
	#[test]
	fn import_profile_clears_the_storable_value_depth() {
		let settings = Datastore::import_parser_settings(&Capabilities::default());
		for (budget, limit) in [
			("object", settings.object_recursion_limit),
			("query", settings.query_recursion_limit),
			("expression", settings.expr_recursion_limit),
		] {
			assert!(
				limit > MAX_VALUE_DEPTH,
				"the {budget} budget ({limit}) must clear the storable value depth \
				 ({MAX_VALUE_DEPTH})"
			);
		}
	}

	/// The deepest literal the import profile accepts is formatted, lowered
	/// and dropped within the call stack a worker thread running statements is
	/// guaranteed.
	///
	/// Parsing keeps its nesting on the heap, so depth alone costs nothing
	/// there; the lowering of the flat tree into the `sql` AST, the `sql ->
	/// expr` lowering that follows, `ToSql` and the tree's own `Drop` recurse
	/// on the call stack, where exhaustion aborts the process rather than
	/// returning an error. A ceiling raised past what those survive fails here
	/// instead.
	///
	/// The budget is a fraction of `SURREAL_RUNTIME_STACK_SIZE`, which the
	/// server sizes well above this in both profiles precisely because these
	/// walks recurse. It cannot be set to the smallest stack an embedded
	/// caller might supply: frames are several times larger without
	/// optimisation, and this test runs unoptimised.
	///
	/// So what this bounds is the server. `SURREAL_RUNTIME_STACK_SIZE` is a
	/// server-only setting, and [`Datastore::import`] is also the embedded
	/// path — `EmbeddedEngine::import`, and `SurrealWasmEngine::import` under
	/// it. A wasm build takes the linker's default shadow stack, which this
	/// repository does not raise, and there a stack overflow is an unrecoverable
	/// trap rather than an abort a supervisor can restart. An embedded caller
	/// that accepts untrusted dumps is not covered by what is asserted here.
	#[test]
	fn import_profile_ceiling_is_stack_safe() {
		std::thread::Builder::new()
			.stack_size(8 * 1024 * 1024)
			.spawn(|| {
				let settings = Datastore::import_parser_settings(&Capabilities::default());
				let parse = |depth: usize| {
					let source = format!("{}'leaf'{}", "{ l: ".repeat(depth), " }".repeat(depth));
					surrealdb_expr::parser::expr_with_settings(&source, settings, false)
				};

				// The deepest literal the profile accepts, so the tree the
				// recursive consumers below are handed is its worst case.
				let mut depth = settings.expr_recursion_limit;
				while parse(depth).is_err() {
					depth -= 1;
					assert!(
						depth > MAX_VALUE_DEPTH,
						"the profile accepts a literal at the storable value depth"
					);
				}
				assert!(parse(depth + 1).is_err(), "the search stopped at the ceiling");

				let expr = parse(depth).expect("the deepest accepted literal parses");
				// `ToSql` recurses once per node.
				assert!(surrealdb_types::ToSql::to_sql(&expr).contains("leaf"));
				// Dropping the runtime tree recurses once per node.
				drop(expr);
			})
			.unwrap()
			.join()
			.unwrap();
	}

	/// Runs `pass` over `indexes` under `places` as the compaction loop does,
	/// and counts the passes that failed. A run that failed as a whole fails.
	#[cfg(not(target_family = "wasm"))]
	async fn failed_compaction_passes<F, Fut>(
		places: &crate::idx::places::Ticket,
		indexes: impl IntoIterator<Item = IndexKeyBase>,
		pass: F,
	) -> Result<usize>
	where
		F: Fn(IndexKeyBase) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<()>> + Send + 'static,
	{
		let run =
			Datastore::run_compaction_passes(places, indexes, &CancellationToken::new(), pass)
				.await;
		if let Some(e) = run.failure {
			return Err(e);
		}
		Ok(run.results.iter().filter(|(_, result)| result.is_err()).count())
	}

	/// A background run over more indexes than the pool has places holds at
	/// most `min(P − R, C)` places at once through its one compaction ticket,
	/// counting the places its passes' own parallel work takes, and runs at
	/// most one pass beyond them, on its own task. With a cap of at least the
	/// pool, that is exactly `P − R`, the pool's OLTP reserve left free; with
	/// a cap below `P − R`, exactly the cap.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn compaction_passes_are_bounded_by_the_places_their_ticket_holds() -> Result<()> {
		use std::sync::atomic::{AtomicUsize, Ordering};

		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		/// What the passes saw: the passes started, those running, and the
		/// most passes running and places held at once.
		#[derive(Default)]
		struct Seen {
			started: AtomicUsize,
			running: AtomicUsize,
			peak_running: AtomicUsize,
			peak_held: AtomicUsize,
		}

		// (places P, reserve R, cap C, the places held at most)
		for (pool_places, reserve, cap, ceiling) in [(6, 2, usize::MAX, 4), (6, 1, 2, 2)] {
			let pool = Pool::new(pool_places);
			let statement = pool.register();
			let places = crate::ctx::IndexPlaces::statement(
				statement.clone(),
				Policy::from_config(&ExecConfig {
					morsel_places: cap,
					morsel_oltp_reserve: reserve,
					..ExecConfig::default()
				}),
			);
			let indexes: Vec<IndexKeyBase> = (0..pool_places as u32 + 3)
				.map(|ix| IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(ix)))
				.collect();
			let total = indexes.len();
			let seen = Arc::new(Seen::default());
			// Helpers' passes wait until a pass runs on the loop's own task,
			// which it starts once an ask for a place has been refused, or until
			// every pass has started; so every place granted before that refusal
			// is still held then.
			let (open, opened) = tokio::sync::watch::channel(false);
			let open = Arc::new(open);
			let pass = {
				let (seen, statement, open, opened, places) = (
					Arc::clone(&seen),
					statement.clone(),
					Arc::clone(&open),
					opened.clone(),
					places.clone(),
				);
				move |_: IndexKeyBase| {
					let (seen, statement, open, mut opened, places) = (
						Arc::clone(&seen),
						statement.clone(),
						Arc::clone(&open),
						opened.clone(),
						places.clone(),
					);
					async move {
						// A pass's own parallel work takes its places through the
						// same ticket.
						let _inner = places.acquire_places(2);
						let started = seen.started.fetch_add(1, Ordering::SeqCst) + 1;
						let running = seen.running.fetch_add(1, Ordering::SeqCst) + 1;
						seen.peak_running.fetch_max(running, Ordering::SeqCst);
						seen.peak_held.fetch_max(statement.held(), Ordering::SeqCst);
						if tokio::task::try_id().is_none() || started == total {
							open.send_replace(true);
						} else {
							opened
								.wait_for(|open| *open)
								.await
								.expect("the sender outlives the passes");
						}
						tokio::task::yield_now().await;
						seen.running.fetch_sub(1, Ordering::SeqCst);
						Ok(())
					}
				}
			};
			let failed = failed_compaction_passes(&places, indexes, pass).await?;
			assert_eq!(failed, 0);
			assert_eq!(seen.started.load(Ordering::SeqCst), total, "every index was compacted");
			assert_eq!(
				seen.peak_held.load(Ordering::SeqCst),
				ceiling,
				"P = {pool_places}, R = {reserve}, C = {cap}: the places held at most"
			);
			assert!(
				seen.peak_running.load(Ordering::SeqCst) <= ceiling + 1,
				"no more passes ran at once than the places held plus the loop's own task"
			);
			assert_eq!(statement.held(), 0, "every place went back");
			assert!(!statement.waiting(), "the ticket is left asking for nothing");
			assert_eq!(pool.free(), pool_places);
		}
		Ok(())
	}

	/// A batch of one index runs its pass on the loop's own task and takes no
	/// place, so the pass's own parallel work has every place the ticket can be
	/// granted.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_single_index_compaction_pass_takes_no_place() -> Result<()> {
		use std::sync::atomic::{AtomicUsize, Ordering};

		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let pool = Pool::new(4);
		let statement = pool.register();
		let places = crate::ctx::IndexPlaces::statement(
			statement.clone(),
			Policy::from_config(&ExecConfig {
				morsel_oltp_reserve: 0,
				..ExecConfig::default()
			}),
		);
		let held = Arc::new(AtomicUsize::new(usize::MAX));
		let on_loop = Arc::new(AtomicUsize::new(0));
		let pass = {
			let (held, on_loop, statement) =
				(Arc::clone(&held), Arc::clone(&on_loop), statement.clone());
			move |_: IndexKeyBase| {
				let (held, on_loop, statement) =
					(Arc::clone(&held), Arc::clone(&on_loop), statement.clone());
				async move {
					held.store(statement.held(), Ordering::SeqCst);
					if tokio::task::try_id().is_none() {
						on_loop.fetch_add(1, Ordering::SeqCst);
					}
					Ok(())
				}
			}
		};
		let index = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(0));
		let failed = failed_compaction_passes(&places, [index], pass).await?;
		assert_eq!(failed, 0);
		assert_eq!(held.load(Ordering::SeqCst), 0, "the one pass ran without a place");
		assert_eq!(on_loop.load(Ordering::SeqCst), 1, "on the loop's own task");
		assert_eq!(pool.free(), 4);
		Ok(())
	}

	/// While the loop's own task runs a pass, a pass behind it is taken by a
	/// helper holding a granted place: the inline pass here completes only
	/// once both other indexes have been compacted elsewhere.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn an_index_behind_an_inline_pass_is_taken_by_a_helper() -> Result<()> {
		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let pool = Pool::new(1);
		let statement = pool.register();
		let places = crate::ctx::IndexPlaces::statement(
			statement.clone(),
			Policy::from_config(&ExecConfig {
				morsel_oltp_reserve: 0,
				..ExecConfig::default()
			}),
		);
		let (done, watched) = tokio::sync::watch::channel(0usize);
		let done = Arc::new(done);
		let pass = {
			let (done, watched) = (Arc::clone(&done), watched.clone());
			move |_: IndexKeyBase| {
				let (done, mut watched) = (Arc::clone(&done), watched.clone());
				async move {
					if tokio::task::try_id().is_none() {
						// The timeout only turns a pass nobody takes into a
						// failure rather than a hang.
						tokio::time::timeout(
							std::time::Duration::from_secs(30),
							watched.wait_for(|done| *done >= 2),
						)
						.await
						.expect("the indexes behind the inline pass are compacted meanwhile")
						.expect("the sender outlives the passes");
					}
					done.send_modify(|done| *done += 1);
					Ok(())
				}
			}
		};
		let indexes: Vec<IndexKeyBase> = (0..3)
			.map(|ix| IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(ix)))
			.collect();
		let failed = failed_compaction_passes(&places, indexes, pass).await?;
		assert_eq!(failed, 0);
		assert_eq!(*watched.borrow(), 3, "every index was compacted");
		assert_eq!(statement.held(), 0);
		Ok(())
	}

	/// A place freed while a helper is busy goes to the next unclaimed index
	/// at once. Index 0 runs on the loop's own task and frees a place another
	/// registration held; indexes 1 and 2 run until index 3 has started,
	/// so index 3 can only start on a new helper given the freed place.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_freed_place_starts_a_helper_for_the_next_unclaimed_index() -> Result<()> {
		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let policy = Policy::from_config(&ExecConfig {
			morsel_oltp_reserve: 0,
			..ExecConfig::default()
		});
		let pool = Pool::new(2);
		let holder = pool.register();
		let held = Arc::new(parking_lot::Mutex::new(holder.acquire(policy)));
		assert!(held.lock().is_some());
		let statement = pool.register();
		let places = crate::ctx::IndexPlaces::statement(statement.clone(), policy);
		let (started, watched) = tokio::sync::watch::channel(false);
		let started = Arc::new(started);
		let pass = {
			let (held, started, watched) =
				(Arc::clone(&held), Arc::clone(&started), watched.clone());
			move |ikb: IndexKeyBase| {
				let (held, started, mut watched) =
					(Arc::clone(&held), Arc::clone(&started), watched.clone());
				async move {
					match ikb.index().0 {
						0 => drop(held.lock().take()),
						3 => {
							started.send_replace(true);
						}
						_ => {
							// The timeout only turns an index nobody starts into
							// a failure rather than a hang.
							tokio::time::timeout(
								std::time::Duration::from_secs(30),
								watched.wait_for(|started| *started),
							)
							.await
							.expect("index 3 starts while indexes 1 and 2 run")
							.expect("the sender outlives the passes");
						}
					}
					Ok(())
				}
			}
		};
		let indexes: Vec<IndexKeyBase> = (0..4)
			.map(|ix| IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(ix)))
			.collect();
		let failed = failed_compaction_passes(&places, indexes, pass).await?;
		assert_eq!(failed, 0);
		assert!(*watched.borrow());
		assert_eq!(statement.held(), 0);
		Ok(())
	}

	/// A compaction run whose helper gave its place way to a waiting
	/// registration does not take the place back while that registration still
	/// waits, though it registered first and would win the pool's tie: the
	/// indexes left after the helper gave way run on the loop's own task.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_compaction_run_does_not_take_back_a_place_its_helper_gave_way() -> Result<()> {
		use std::sync::atomic::{AtomicBool, Ordering};

		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let policy = Policy::from_config(&ExecConfig {
			morsel_oltp_reserve: 0,
			..ExecConfig::default()
		});
		let pool = Pool::new(1);
		let statement = pool.register();
		let waiter = pool.register();
		let places = crate::ctx::IndexPlaces::statement(statement.clone(), policy);
		let (step, stepped) = tokio::sync::watch::channel(0u8);
		let step = Arc::new(step);
		let last_on_helper = Arc::new(AtomicBool::new(false));
		let pass = {
			let (statement, waiter, step, stepped, last_on_helper) = (
				statement.clone(),
				waiter.clone(),
				Arc::clone(&step),
				stepped.clone(),
				Arc::clone(&last_on_helper),
			);
			move |ikb: IndexKeyBase| {
				let (statement, waiter, step, mut stepped, last_on_helper) = (
					statement.clone(),
					waiter.clone(),
					Arc::clone(&step),
					stepped.clone(),
					Arc::clone(&last_on_helper),
				);
				async move {
					// The timeouts only turn a stall into a failure rather than a
					// hang.
					let guard = std::time::Duration::from_secs(30);
					match ikb.index().0 {
						// On the loop's own task, once the helper runs index 1:
						// the waiter is refused, so it waits, and the helper then
						// gives its place way at the end of index 1.
						0 => {
							tokio::time::timeout(guard, stepped.wait_for(|s| *s >= 1))
								.await
								.expect("index 1 starts on a helper")
								.expect("the sender outlives the passes");
							assert!(waiter.acquire(policy).is_none(), "the waiter is refused");
							step.send_replace(2);
							tokio::time::timeout(guard, async {
								while statement.held() > 0 {
									tokio::task::yield_now().await;
								}
							})
							.await
							.expect("the helper gives its place way");
						}
						1 => {
							step.send_replace(1);
							tokio::time::timeout(guard, stepped.wait_for(|s| *s >= 2))
								.await
								.expect("the waiter is refused")
								.expect("the sender outlives the passes");
						}
						3 => {
							last_on_helper.store(tokio::task::try_id().is_some(), Ordering::SeqCst)
						}
						_ => {}
					}
					Ok(())
				}
			}
		};
		let indexes: Vec<IndexKeyBase> = (0..4)
			.map(|ix| IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(ix)))
			.collect();
		let failed = failed_compaction_passes(&places, indexes, pass).await?;
		assert_eq!(failed, 0);
		assert!(
			!last_on_helper.load(Ordering::SeqCst),
			"no helper took the place back for index 3 while the waiter waited"
		);
		assert!(waiter.waiting(), "the waiter still waits");
		assert_eq!(pool.free(), 1, "the place is free for the waiter");
		Ok(())
	}

	/// A place the parallel work of an inline pass gave way, through the
	/// compaction ticket, to a waiting registration is not taken back for the
	/// next queued index while that registration still waits: no helper is
	/// started, and the remaining indexes run on the loop's own task.
	#[cfg(not(target_family = "wasm"))]
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	async fn a_place_an_inline_pass_gave_way_is_not_taken_back() -> Result<()> {
		use std::sync::atomic::{AtomicUsize, Ordering};

		use crate::catalog::{DatabaseId, IndexId, NamespaceId};
		use crate::exec::config::ExecConfig;
		use crate::exec::planner::placement::Policy;
		use crate::exec::vm::morsel::places::Pool;

		let policy = Policy::from_config(&ExecConfig {
			morsel_oltp_reserve: 0,
			..ExecConfig::default()
		});
		let pool = Pool::new(1);
		let holder = pool.register();
		let held = Arc::new(parking_lot::Mutex::new(holder.acquire(policy)));
		let statement = pool.register();
		let waiter = pool.register();
		let places = crate::ctx::IndexPlaces::statement(statement.clone(), policy);
		let on_helpers = Arc::new(AtomicUsize::new(0));
		let pass = {
			let (held, waiter, places, on_helpers) =
				(Arc::clone(&held), waiter.clone(), places.clone(), Arc::clone(&on_helpers));
			move |ikb: IndexKeyBase| {
				let (held, waiter, places, on_helpers) =
					(Arc::clone(&held), waiter.clone(), places.clone(), Arc::clone(&on_helpers));
				async move {
					if tokio::task::try_id().is_some() {
						on_helpers.fetch_add(1, Ordering::SeqCst);
					}
					if ikb.index().0 == 0 {
						// The pass's own parallel work takes the place freed
						// here, then gives it way to the waiter, as a worker of
						// its staged batch does at a quantum boundary.
						drop(held.lock().take());
						let worker = places.acquire().expect("the freed place");
						assert!(waiter.acquire(policy).is_none(), "the waiter is refused");
						assert!(places.others_wait());
						places.give_way(worker);
					}
					Ok(())
				}
			}
		};
		let indexes: Vec<IndexKeyBase> = (0..3)
			.map(|ix| IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "t".into(), IndexId(ix)))
			.collect();
		let failed = failed_compaction_passes(&places, indexes, pass).await?;
		assert_eq!(failed, 0);
		assert_eq!(on_helpers.load(Ordering::SeqCst), 0, "no helper took the place back");
		assert!(waiter.waiting(), "the waiter still waits");
		assert_eq!(pool.free(), 1, "the place is free for the waiter");
		Ok(())
	}
}
