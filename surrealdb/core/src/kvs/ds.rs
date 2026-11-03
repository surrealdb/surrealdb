use std::any::TypeId;
use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt::{self, Display};
use std::ops::Bound;
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
use surrealdb_datastore::values::event_queue::AsyncEventRecord;
use surrealdb_datastore::values::graph::GraphFoldScope;
use surrealdb_datastore::{FeatureLevelCell, FeatureLevelState, ReadRequest};
use surrealdb_expr::parser::Query as Ast;
use surrealdb_kvs::TransactionType::*;
use surrealdb_kvs::value::EncodeContext;
use surrealdb_kvs::{
	DestroyRange, DestroyRangeHandle, FeatureLevel, Isolation, Maintenance, MaintenanceHandle,
	Release, SnapshotInfo, TransactionType,
};
use surrealdb_observe::TaskLeaseState;
use surrealdb_types::{AuthError, Error as TypesError, SurrealValue, object};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, trace, warn};
use uuid::Uuid;

use super::tx::Transaction;
use super::version::{MajorVersion, MigrationRecord, StorageVersion, VersionHistoryEntry};
use super::{
	EVENT_PROCESSING_SLICE_BATCHES, GRAPH_FOLD_SLICE_BATCHES, GRAPH_FOLD_SLICE_PASSES_PER_SCOPE,
	INDEX_COMPACTION_QUEUE_BATCH_SIZE, INDEX_COMPACTION_SLICE_BATCHES, INDEX_RESUME_PAGE_SIZE,
	QUEUE_CLEANUP_CONFLICT_RETRIES, migration,
};
use crate::api::err::ApiError;
use crate::api::request::ApiRequest;
use crate::api::response::ApiResponse;
use crate::buc::manager::BucketsManager;
#[cfg(test)]
use crate::catalog::Index;
use crate::catalog::providers::{
	CatalogProvider, DatabaseProvider, NamespaceProvider, NodeProvider, RootProvider, TableProvider,
};
use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId, NodeLiveQuery, StoredFeatureLevel};
use crate::cf::gc::{ChangefeedSweep, Discovered, SweepReads};
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
	Capabilities, Dispatch, DurableSession, Executor, MessageBroker, Options, OrderedBroker,
	QueryResult, QueryResultBuilder, QueryStreamItem, QueryStreamJob, Session, StoredRpcSession,
	TopLevelStatement, durable_session, restore_session,
};
use crate::doc::{EventBatch, FailedBatch, RunningEvents, process_next_events_batch};
use crate::err::{EngineError, Error};
use crate::exec::Error as ExecError;
use crate::exec::config::ExecConfig;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::timers::compile_phase;
use crate::exec::surface::{FULL, Surface};
use crate::expr::logical::LogicalOp;
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
	BootstrapKey, DbRoot, DocKeyPrefix, DocLookupIdentityPrefix, DocLookupPrefix, DocPendingPrefix,
	EventQueuePrefix, FeatureLevelKey, GraphFoldDirPrefix, GraphFoldKey, GraphFoldPrefix, IdxRoot,
	IndexCompactionIxPrefix, IndexCompactionKey, IndexCompactionPrefix, MigrationKey,
	MigrationPrefix, NodeDrainHeartbeatKey, NodeEventQueueKey, NodeEventQueuePrefix, NodeKey,
	NodeLiveQueryKey, NodeLiveQueryPrefix, NodePrefix, NodeReleaseKey, NodeReleasePrefix,
	NodeStatePrefix, NodeStateRoot, NsRoot, ReclaimKey, ReclaimPrefix, RootUserPrefix, SessionKey,
	SessionPrefix, StorageVersionKey, SubscriptionKey, VersionHistoryPrefix, VersionKey,
};
use crate::key::{
	AnyRange, KVKey, KVKeyDecode, KVSubspace, KVValue, Key, KeyRange, RawRange, Resumable,
	TypedRange,
};
use crate::kvs::cache::ds::DatastoreCache;
use crate::kvs::clock::SystemClock;
pub(crate) use crate::kvs::index::materialise::INDEX_COMPACTION_PASS_MAX_RETRIES;
#[cfg(test)]
use crate::kvs::index::materialise::INDEX_COMPACTION_PASS_PLAN_BUDGET;
use crate::kvs::index::materialise::{
	CompactionPassOutcome, CompactionPassReport, Materialiser, PassLease,
};
#[cfg(all(test, feature = "kv-surrealkv"))]
use crate::kvs::index::materialise::{CompactionWriteGuard, compaction_write_guard};
use crate::kvs::index::{BuildPass, IndexBuilder, ResumeScope};
use crate::kvs::paging::{PageCompanion, PagedDelete, PagedOutcome};
use crate::kvs::request::{Dialect, QueryRequest, QuerySource, ReadOnlyOptions};
use crate::kvs::sequences::Sequences;
use crate::kvs::slowlog::SlowLog;
use crate::kvs::tasklease::{LeaseCheckFailed, LeaseHandler, TaskLeaseType};
use crate::kvs::tasks::{HostMaintenance, MaintenanceSlice, Pass, Progress};
#[cfg(test)]
use crate::kvs::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::kvs::walk::{IndexWalk, Step, WalkedIndex};
use crate::kvs::{
	CHANGEFEED_GC_PASS_KEY_BUDGET, CHANGEFEED_GC_SLICE_STEPS, DatastoreError, Direction,
	EVENT_ADOPTION_PAGE_SIZE, NODE_CLEANUP_SLICE_BATCHES, NODE_EXPIRE_SLICE_BATCHES,
	NORMAL_BATCH_SIZE, RECLAIM_BATCH_SIZE, RECLAIM_PASS_ENTRY_QUOTA, RECLAIM_PASS_KEY_BUDGET,
	RECLAIM_PASS_WALK_BATCHES, RPC_SESSION_GC_SLICE_BATCHES, TransactionFactory,
	is_retryable_transaction_conflict,
};
use crate::lq::LiveQueryRouter;
use crate::lq::external::ChangeFeedSource;
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
mod doc_ids_reclaim;
mod edge;
#[cfg(all(test, feature = "kv-mem"))]
mod edge_tests;
#[cfg(all(test, feature = "kv-mem", any(feature = "postgres", feature = "mysql")))]
mod sql_declared_tests;
#[cfg(all(test, feature = "kv-mem", any(feature = "postgres", feature = "mysql")))]
mod sql_function_cases;
#[cfg(all(test, feature = "kv-mem", any(feature = "postgres", feature = "mysql")))]
mod sql_function_tests;
#[cfg(any(feature = "postgres", feature = "mysql"))]
mod sql_links;
#[cfg(all(test, feature = "kv-mem", any(feature = "postgres", feature = "mysql")))]
mod sql_window_tests;
mod table_migration;

pub use builder::Builder;
pub use config::LiveQueryEngine;
pub(crate) use doc_ids_reclaim::{DocIdsReclaimPending, DocIdsReclaimStep};

const TARGET: &str = "surrealdb::core::kvs::ds";

/// How long a node commits nothing under its writer leases on an index
/// before it releases them ([`Datastore::release_idle_writer_leases`]).
/// Until then no node returns those shards' guards to `Empty`, so lookups keep
/// scanning them; past it, a write takes the lease again.
const WRITER_LEASE_IDLE: Duration = Duration::from_secs(10);

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

/// How many transactions a durable RPC session create or delete spends before
/// a conflicting concurrent writer is reported as an error.
const RPC_SESSION_WRITE_ATTEMPTS: u32 = 3;

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

/// What a drain did with one queued graph-fold scope; see
/// [`Datastore::fold_queued_scope`].
#[derive(Debug, PartialEq, Eq)]
enum FoldOutcome {
	/// The scope is drained, and its queue entries are consumed.
	Folded,
	/// The passes ran out with keys left to examine: the entries stay queued
	/// as the slice's backlog, which the next slice reaches again.
	Unfinished,
	/// The table's definition needs a newer release than this build, so the
	/// scope cannot be folded here. The entries stay queued, for a build that
	/// can, and the rotation does not count the scope as progress: a slice
	/// that finds nothing else to do yields to its interval rather than run
	/// again at once on a scope it cannot fold.
	Refused,
	/// Another node took the lease before a pass, so the scope was left
	/// where its last committed pass stopped. The entries stay queued, and the
	/// drain leaves its batch to the holder.
	LeaseLost,
}

/// What one drain of the graph fold queue did; see [`Datastore::graph_fold_drain`].
struct FoldDrain {
	/// The batches of queue entries read.
	batches: usize,
	/// The scopes whose fold failed.
	failed: usize,
	/// The scopes whose queue entries the drain consumed.
	folded: usize,
	/// The pass report.
	pass: Pass,
}

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

/// How a reclaim runs: under the cluster's reclaim task lease, or as a pass a
/// host drives itself.
#[derive(Clone, Copy)]
enum ReclaimMode<'a> {
	/// Under `lease`, checked before every page, destroying a prefix in one
	/// out-of-transaction call where the backend offers [`DestroyRange`].
	Leased(&'a LeaseHandler),
	/// With no lease, every prefix deleted in pages that are ordinary
	/// transactions. The host runs one pass at a time, and each page's
	/// compare-and-set on its queue entry keeps a concurrent statement
	/// finishing the same reclaim from deleting twice.
	Hosted,
}

impl<'a> ReclaimMode<'a> {
	/// The lease checked before every page, if any.
	fn lease(self) -> Option<&'a LeaseHandler> {
		match self {
			Self::Leased(lh) => Some(lh),
			Self::Hosted => None,
		}
	}

	/// Whether the lease is still held: always, for a hosted pass.
	async fn held(self) -> Result<bool> {
		match self {
			Self::Leased(lh) => lh.try_maintain_lease().await,
			Self::Hosted => Ok(true),
		}
	}
}

/// What one bounded reclaim pass did: see [`Datastore::run_reclaim`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReclaimRun {
	/// The data keys the pass deleted.
	pub keys: u64,
	/// Whether work is left that a pass run at once continues: the pass spent its key budget,
	/// left an entry it served holding data, or walked its share of the queue with entries still
	/// to walk.
	pub more: bool,
	/// Where no such work is left but an entry is still inside its grace, how long until the
	/// first of them ages past it, when a pass deletes it.
	pub retry_after: Option<Duration>,
}

/// What [`Datastore::reclaim_pass`] did.
struct ReclaimPass {
	/// The queue batches the walk read.
	iterations: usize,
	/// The entries the pass could not decode, stamp, reclaim or retire.
	errors: usize,
	pass: Pass,
	/// Whether an entry the pass served still holds data.
	incomplete: bool,
	/// The earliest instant, in milliseconds on the datastore's wall clock, at which an entry
	/// the walk saw inside its grace ages past it.
	next_due_ms: Option<u64>,
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

/// Where the graph fold drain's rotation stands between slices.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FoldCursor {
	/// The queue position the last slice covered, or `None` at the head.
	resume_at: Option<Vec<u8>>,
	/// Whether the rotation since it last began at the head has deleted a
	/// queue entry or folded a scope.
	progressed: bool,
	/// For each scope a pass has left with keys still to examine, where its
	/// next pass continues, so the passes of later slices cross a scope
	/// whose unpackable keys outnumber a slice's budget rather than examine
	/// the same keys each time. A scope's entry goes when its fold ends or
	/// the scope is dropped, and every entry goes when the queue is found
	/// empty.
	scans: HashMap<GraphFoldScope, ScopeScan>,
}

/// Where a scope's next fold pass continues, and the queue entries that
/// continuation answers for.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ScopeScan {
	/// The key the next pass scans after: every key at or before it was
	/// folded or could not be packed by the passes that led here.
	after: Vec<u8>,
	/// The queue entries whose writes those passes saw: the entries naming
	/// the scope in the batch whose fold began at the scope's start. A write
	/// since may have put a key before `after` — a tombstone over a packed
	/// edge, or an edge whose key sorts early — and queued an entry outside
	/// this set, so a batch holding such an entry folds the scope from its
	/// start: continuing would declare the scope folded with that key
	/// unexamined, and the cleanup would delete the entry that named it.
	entries: HashSet<Vec<u8>>,
}

/// One async event queue, as the drains name it: the one every node drains
/// for itself, the ones the adoption sweep drains for nodes that are gone, and
/// the position each drain keeps in it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EventQueue {
	/// The root-level queue that nodes queueing at the root still write.
	Root,
	/// The per-node queue of the node with this id.
	Node(Uuid),
}

/// Where a drain of one event queue stands between passes: one lap over the
/// queue as it stood when the lap began at the head.
#[derive(Clone, Debug, PartialEq, Eq)]
struct EventLap {
	/// The key of the last entry fetched, which the next fetch resumes after;
	/// `None` at the head.
	after: Option<Vec<u8>>,
	/// The last key the queue held when the lap began. Once the scan has
	/// passed it, what lies beyond was queued since, and the next lap starts
	/// from the head so that the entries there — requeued for another attempt,
	/// or queued since for an earlier-sorting event — get their turn.
	end: Vec<u8>,
	/// Whether an attempt in this lap failed and was requeued. A requeued
	/// entry sits behind `after`, so when the lap ends at the queue's end it
	/// is what the next pass owes a run; kept with the lap so a requeue in the
	/// batch that spent the last of one pass's budget is still known to the
	/// pass that finds the end.
	requeued: bool,
	/// How many commits had queued async events on this datastore when the
	/// lap began; meaningful for the local queue alone. An event runs on the
	/// node that drains it and queues what it queues under that node's own
	/// queue, after the fetch that would have found the entry and, when its
	/// table sorts earlier, behind the scan's position too; a concurrent
	/// write on this node queues the same way. So a lap that finds the
	/// queue's end at a higher count than it began with ends at the head
	/// rather than at rest, and the drain ends with a lap during which
	/// nothing was queued. Kept with the lap for the same reason as
	/// `requeued`.
	queued: u64,
}

/// One leg of a rotation of the adoption sweep over the queues: the
/// root-level queue, or a stretch of the per-node queues in id order.
enum AdoptionLeg {
	/// The root-level queue.
	Root,
	/// The per-node queues of the nodes with state in `window`, in id order,
	/// up to and including `until`'s when given. The window advances as the
	/// walk steps over nodes, so a leg left unfinished resumes where it
	/// stopped.
	Nodes {
		window: RawRange,
		until: Option<Uuid>,
	},
}

/// A scan of a key range that a paged pass stopped in, kept for the pass that
/// continues it; see [`Datastore::purge_expired_rpc_sessions`],
/// [`Datastore::expire_nodes`] and [`Datastore::remove_nodes`].
#[derive(Clone, Debug)]
struct KeyScan {
	/// The last key of the page the previous pass read, which the next read
	/// resumes after.
	after: Vec<u8>,
	/// The last key there was when the scan began. The page that reaches it
	/// ends the scan, so keys written past it while the scan runs are the
	/// next scan's, and the scan ends however fast they are written.
	end: Vec<u8>,
}

/// Where a node cleanup pass stopped, kept for the pass that continues it;
/// see [`Datastore::remove_nodes`].
#[derive(Clone, Debug)]
enum NodeCleanupScan {
	/// In the scan of the node rows.
	Rows(KeyScan),
	/// In the sweep of the release records, which follows the row scan: the
	/// last key of the page read last, `None` before the first, and the last
	/// key there was when the sweep began.
	Releases {
		after: Option<Vec<u8>>,
		end: Vec<u8>,
	},
}

/// A scan of the reclaim queue that a reclaim pass stopped in, kept for the
/// pass that continues it; see [`Datastore::reclaim_tombstones`].
#[derive(Clone, Debug)]
struct ReclaimScan {
	/// The last key of the batch the previous pass read, which the next read
	/// resumes after.
	after: Vec<u8>,
	/// The last key there was when the scan began. The batch that reaches it
	/// ends the scan, so entries queued past it while the scan runs are the
	/// next scan's, and the scan ends however fast removals are queued.
	end: Vec<u8>,
}

/// A rotation of the adoption sweep over the queues between passes: the legs
/// still to walk, and the queues drained so far. A rotation covers the root
/// queue and every node once; a pass walks a page of it, and the rotation is
/// kept while a pass ends at its page with legs left, so a cluster with more
/// owners than one pass steps over is covered a page per pass and the
/// rotation still ends. A pass that spends its fetch budget ends the rotation
/// early, and the next begins one after the queue it was spent in and ending
/// with it, so a queue refilled as fast as it drains cannot hold the sweep on
/// itself.
struct AdoptionRotation {
	legs: VecDeque<AdoptionLeg>,
	/// The queues drained so far in this rotation. When the rotation ends,
	/// the lap of any other queue is stale.
	swept: HashSet<EventQueue>,
}

impl AdoptionRotation {
	/// The rotation from the ring's start: the root queue, then every node.
	fn fresh() -> Result<Self> {
		Ok(Self {
			legs: VecDeque::from([
				AdoptionLeg::Root,
				AdoptionLeg::Nodes {
					window: NodeStatePrefix {}.raw_range()?,
					until: None,
				},
			]),
			swept: HashSet::new(),
		})
	}

	/// The rotation starting after `queue` and ending with it.
	fn after(queue: EventQueue) -> Result<Self> {
		let nodes = NodeStatePrefix {}.raw_range()?;
		let legs = match queue {
			EventQueue::Root => VecDeque::from([
				AdoptionLeg::Nodes {
					window: nodes,
					until: None,
				},
				AdoptionLeg::Root,
			]),
			EventQueue::Node(nd) => {
				let subtree = NodeStateRoot {
					nd,
				}
				.raw_range()?;
				VecDeque::from([
					AdoptionLeg::Nodes {
						window: nodes.clone().resume_after(subtree.end(), Direction::Forward),
						until: None,
					},
					AdoptionLeg::Root,
					AdoptionLeg::Nodes {
						window: nodes,
						until: Some(nd),
					},
				])
			}
		};
		Ok(Self {
			legs,
			swept: HashSet::new(),
		})
	}
}

/// Why a pass stopped before the end of its rotation.
enum SweepStop {
	/// The fetch budget was spent in this queue.
	Budget(EventQueue),
	/// The page of nodes was walked.
	Page,
	/// The lease passed to another node, which sweeps from here.
	LeaseLost,
}

/// Where [`Datastore::drain_event_range`] left a queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Drained {
	/// The next pass owes the queue nothing.
	Clear,
	/// The queue holds entries the next pass owes a run.
	Backlog,
	/// The lease the drain ran under passed to another node, which drains
	/// the queue from here.
	LeaseLost,
}

impl Drained {
	/// [`Self::Backlog`] when the next pass owes the queue a run, else
	/// [`Self::Clear`].
	const fn owed(backlog: bool) -> Self {
		if backlog {
			Self::Backlog
		} else {
			Self::Clear
		}
	}
}

/// One adoption pass's running state, shared by the legs of its rotation.
struct AdoptionSweep<'a> {
	lh: &'a LeaseHandler,
	canceller: &'a CancellationToken,
	/// Fetches that find an entry the pass may still make.
	budget: usize,
	/// Nodes with state the pass may still step over.
	steps: usize,
	pass: Pass,
	/// The queues drained so far.
	swept: HashSet<EventQueue>,
	/// The queue whose drain failed the pass, when one did.
	failed: Option<EventQueue>,
}

/// The first node with state at or after a walk's position.
struct NodeState {
	nd: Uuid,
	/// Whether that state includes an event queue.
	queued: bool,
	/// Whether the queue has an owner that drains it: the node has a row that
	/// is not archived, and the row's heartbeat is the one its owner wrote
	/// beside it, which only a release that drains per-node queues does. A
	/// row kept fresh by a release that queues at the root — the node rolled
	/// back with its id kept — is live, but its owner cannot see this queue.
	owned: bool,
}

/// The node a key beneath the node-state prefix belongs to: the level puts
/// the node id, sixteen raw bytes, right after its prefix and before anything
/// a key beneath the node adds. `None` for bytes too short to hold one.
fn node_of_state_key(key: &[u8]) -> Result<Option<Uuid>> {
	let prefix = NodeStatePrefix {}.encode_bound()?;
	let Some(rest) = key.strip_prefix(prefix.as_ref()) else {
		return Ok(None);
	};
	Ok(rest.get(..16).and_then(|id| Uuid::from_slice(id).ok()))
}

/// What drives a datastore's maintenance jobs.
///
/// Whichever of [`Datastore::start_maintenance_tasks`] and
/// [`Datastore::run_maintenance_slice`] is called first chooses, and the choice
/// holds for the life of the datastore: two drivers would run every job twice,
/// and the jobs that take no lease — the live-query router among them — would
/// then deliver the same work twice. Recorded under one lock so the two entry
/// points cannot both find the datastore undriven.
enum MaintenanceDriver {
	/// Neither entry point has been called.
	Undriven,
	/// The datastore's own tasks, started by the builder or by
	/// [`Datastore::start_maintenance_tasks`].
	Tasks(crate::kvs::tasks::Tasks),
	/// A host calling [`Datastore::run_maintenance_slice`] on its own schedule.
	Host,
}

impl MaintenanceDriver {
	/// Takes the task handles, leaving the datastore undriven. A host-driven
	/// or undriven datastore is left as it is and has none to give.
	fn take_tasks(&mut self) -> Option<crate::kvs::tasks::Tasks> {
		match std::mem::replace(self, Self::Undriven) {
			Self::Tasks(tasks) => Some(tasks),
			driver @ (Self::Undriven | Self::Host) => {
				*self = driver;
				None
			}
		}
	}
}

/// How a conditional write to a durable RPC session ended.
enum ConditionalWrite {
	/// The write committed.
	Committed,
	/// The stored value did not meet the write's guard.
	Superseded,
	/// The transaction conflicted with a concurrent writer before the guard was
	/// settled, so a fresh transaction re-evaluates it.
	Conflicted(anyhow::Error),
}

/// Commits `txn` after the conditional write `write`, classifying how it ended.
///
/// Any failure cancels the transaction. An unmet guard and a transaction
/// conflict are outcomes the caller decides on; every other failure is an
/// error. On last-writer-wins backends (TiKV) the guard is validated at commit,
/// so an unmet guard can surface from either the write or the commit.
async fn commit_conditional_write(
	txn: &Transaction,
	write: Result<()>,
) -> Result<ConditionalWrite> {
	let result = match write {
		Ok(()) => txn.commit().await,
		Err(e) => Err(e),
	};
	let Err(e) = result else {
		return Ok(ConditionalWrite::Committed);
	};
	let _ = txn.cancel().await;
	if super::is_conditional_write_conflict(&e) {
		Ok(ConditionalWrite::Superseded)
	} else if is_retryable_transaction_conflict(&e) {
		Ok(ConditionalWrite::Conflicted(e))
	} else {
		Err(e)
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

/// The SQL dialects whose text is lowered with the links of the tables it names.
#[derive(Clone, Copy)]
enum LinkedSql {
	PgSql,
	MySql,
}

/// The text and dialect of `source` when it is SQL text that is lowered with links.
fn linked_sql_text<'a>(source: &QuerySource<'a>) -> Option<(&'a str, LinkedSql)> {
	match source {
		QuerySource::Text {
			text,
			dialect: Dialect::PgSql,
		} => Some((text, LinkedSql::PgSql)),
		QuerySource::Text {
			text,
			dialect: Dialect::MySql,
		} => Some((text, LinkedSql::MySql)),
		QuerySource::Text {
			dialect: Dialect::SurrealQl | Dialect::Gql,
			..
		}
		| QuerySource::Ast(_)
		| QuerySource::Plan(_) => None,
	}
}

/// The underlying datastore instance which stores the dataset.
pub struct Datastore {
	transaction_factory: TransactionFactory,
	/// Cancellation for this datastore's own background work, tripped by
	/// [`Self::shutdown`].
	pub(super) shutdown: CancellationToken,
	/// What drives this datastore's maintenance jobs, and the task handles
	/// when it is the datastore's own tasks, so [`Self::shutdown`] can wait
	/// for them to stop.
	///
	/// A datastore is not a working database without a driver — index
	/// compaction, tombstone reclaim, changefeed GC, index-build recovery and
	/// the cluster heartbeat all live there — so the builder starts the tasks
	/// rather than leaving it to the embedder. A datastore built with
	/// [`Builder::without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks)
	/// is undriven until [`Self::start_maintenance_tasks`] or
	/// [`Self::run_maintenance_slice`] is called on it.
	maintenance: parking_lot::Mutex<MaintenanceDriver>,
	/// Whether this datastore answers write barriers and keeps its write fence armed
	/// ([`Self::begin_answering_write_barriers`]), and so records that it does
	/// ([`Self::advertises_write_barrier`]). Only the datastore's own tasks answer them: a
	/// datastore a host drives answers none.
	pub(super) answers_write_barriers: std::sync::atomic::AtomicBool,
	/// The schedule a host drives through [`Self::run_maintenance_slice`].
	/// Built on the first slice; `None` until then, for every datastore whose
	/// tasks run themselves, and again after [`Self::shutdown`], which drops
	/// it once the slice in flight has ended: its runner holds this
	/// datastore's claim on the process metrics refresh, which another
	/// datastore in the process can take only once the runner is gone. Held
	/// for the length of a slice, so shutdown can wait for one in flight.
	host_maintenance: tokio::sync::Mutex<Option<HostMaintenance>>,
	/// The walk the stalled-index-build sweep in progress is making over the
	/// catalog's indexes; `None` between sweeps. This node's alone: the sweep
	/// runs under a lease, and a node the lease moves to begins a sweep of
	/// its own. See [`IndexWalk`].
	resume_walk: parking_lot::Mutex<Option<IndexWalk>>,
	/// Where the next [`Self::run_index_builds`] resumes its walk over the catalog's indexes.
	build_walk: parking_lot::Mutex<Option<IndexWalk>>,
	/// The scan of the durable RPC sessions a purge pass stopped in when it
	/// spent its slice with pages still to read; `None` between scans. This
	/// node's alone: the purge runs under a lease, and a node the lease
	/// moves to scans from the head, which purges nothing twice. See
	/// [`KeyScan`].
	rpc_session_cursor: parking_lot::Mutex<Option<KeyScan>>,
	/// The scan of the node rows an expiry pass stopped in when it spent its
	/// slice with pages still to read; `None` between scans. This node's
	/// alone: every node runs expiry, and archiving is conditional on the
	/// heartbeat the scan saw, so a scan another node runs alongside archives
	/// nothing twice. See [`KeyScan`].
	node_expire_cursor: parking_lot::Mutex<Option<KeyScan>>,
	/// The scan of the node rows a cleanup pass stopped in when it spent its
	/// slice with pages still to read; `None` between scans. This node's
	/// alone: every node runs cleanup, and a node is re-read as archived in
	/// the transaction that cleans it, so a scan another node runs alongside
	/// cleans nothing twice. See [`KeyScan`].
	node_cleanup_cursor: parking_lot::Mutex<Option<NodeCleanupScan>>,
	/// The scan of the reclaim queue a reclaim pass stopped in when it walked
	/// its share of batches with entries still to walk; `None` between scans.
	/// This node's alone: a leased pass runs on the node holding the lease, and
	/// a node the lease moves to scans from the head; a hosted pass
	/// ([`Datastore::run_reclaim`]) runs on the one node its host drives. See
	/// [`ReclaimScan`].
	reclaim_cursor: parking_lot::Mutex<Option<ReclaimScan>>,
	/// When this node's reclaim task first saw each doc-ID queue entry that
	/// carries no stamp, in unix milliseconds, keyed by the entry's key. Such an
	/// entry ages from here and is not written until its first page, which
	/// records this time with its cursor: a `DEFINE INDEX` may compare-delete it
	/// throughout its grace, and a stamp would refuse that claim. This node's
	/// alone, lost on restart and forgotten when the lease is found elsewhere,
	/// each of which only restarts a grace: a node the lease moves to sees the
	/// entry anew. Dropped once the entry carries a record of its own. See
	/// [`ReclaimKind::is_doc_id`].
	reclaim_sightings: parking_lot::Mutex<BTreeMap<Vec<u8>, u64>>,
	/// Since when, in unix milliseconds, [`Self::reclaim_sightings`] has been
	/// kept without a break: set by the first walk that holds the lease, and
	/// reset to `0` (unset) when a pass finds the lease elsewhere and the
	/// sightings are forgotten. An entry queued after it that holds no sighting
	/// has not been seen yet, rather than seen and forgotten, so the backstop
	/// that stamps a forgotten one leaves it to be sighted.
	reclaim_sightings_since: std::sync::atomic::AtomicU64,
	/// The changefeed-collection sweep in progress, or one not begun between
	/// sweeps. This node's alone: the sweep runs under a lease, and a node the
	/// lease moves to begins a sweep of its own. See [`ChangefeedSweep`].
	changefeed_sweep: parking_lot::Mutex<ChangefeedSweep>,
	/// As `changefeed_sweep`, for the live-query event collector, which sweeps
	/// the catalog on its own cadence.
	live_event_sweep: parking_lot::Mutex<ChangefeedSweep>,
	/// Where the index-compaction drain stands between slices: where its
	/// rotating window resumes, and what it has kept queued so far. Kept across
	/// slices so a slice that stops at its budget continues the drain rather
	/// than restarting it, which is what keeps a later-sorting index from
	/// starving behind ones that keep refilling. See [`CompactionCursor`].
	compaction_cursor: parking_lot::Mutex<CompactionCursor>,
	/// As `compaction_cursor`, for the graph fold drain, together with whether
	/// the rotation since it last began at the head has made progress. A
	/// rotation over scopes that all fail spans as many slices as its budget
	/// dictates, and that flag is what lets the slice reaching the end tell a
	/// failed rotation, which yields, from one that has merely not finished.
	fold_cursor: parking_lot::Mutex<FoldCursor>,
	/// The event-adoption sweep's rotation in progress, or `None` when the
	/// last pass ended one: the next then begins from the ring's start. See
	/// [`AdoptionRotation`].
	adoption_rotation: parking_lot::Mutex<Option<AdoptionRotation>>,
	/// Where each event queue's drain stands: the lap in progress over it,
	/// kept while a pass ends at its budget or is cancelled with the queue
	/// still full, and dropped once a pass reaches the queue's end. A pass
	/// that continues past the entries it could not run reaches the ones
	/// behind them without reading the same prefix again.
	event_laps: parking_lot::Mutex<HashMap<EventQueue, EventLap>>,
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
	/// The queued async events a worker of this process is running, by the
	/// entry's key, from its claim to its settlement. A second worker of this
	/// process is refused an entry marked here, and a claim this node wrote
	/// on an entry not marked here is void; see
	/// [`crate::doc::process_next_events_batch`]. A restart or a fork starts
	/// with none marked, so the claims of the process before are void to it.
	running_events: RunningEvents,
	/// Whether this process created the datastore's storage.
	///
	/// Latched by [`Self::check_version`], because the underlying signal is
	/// one-shot: it comes from writing the version key, and a caller that
	/// retries after a later failure would otherwise see the key already there
	/// and be told the datastore is pre-existing.
	created_here: std::sync::atomic::AtomicBool,
	/// Whether this datastore's data migrations have run, which every
	/// maintenance job but the cluster heartbeat waits for before its first
	/// pass.
	///
	/// A migration rewrites persisted data under a lease, and a pass that ran
	/// alongside it would read that data mid-rewrite: act on half of it, or
	/// write back the shape the migration is retiring. The heartbeat is exempt
	/// because the migration's lease is this node's, and holding it depends on
	/// the node's membership row staying fresh. Raised by
	/// [`Self::check_version`] once the migrations have run, and by the builder
	/// for a datastore this process created or found already migrated, so a
	/// datastore that owes nothing runs its maintenance without that call. A
	/// `watch` channel, so a task waits on it without polling.
	migrated: tokio::sync::watch::Sender<bool>,
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
	live_query_broker: Option<Arc<OrderedBroker>>,
	/// Host-provided source `SHOW CHANGES` reads from instead of the change-feed keyspace.
	changefeed_source: Option<Arc<dyn ChangeFeedSource>>,
	/// Per-node live-query router state (the tail cursor over the `lqe`
	/// keyspace). Only used when `config.datastore.live_query_engine` is
	/// `Router`, where the router is the sole notification delivery path. See
	/// [`crate::lq`].
	live_query_router: Arc<LiveQueryRouter>,
	/// How a running program gives up its thread once it has spent its
	/// quantum; the default scheduler when `None`.
	scheduler: Option<Arc<dyn crate::exec::scheduler::Scheduler>>,
	/// The host's places a morsel pipeline's morsels may run at besides the node's own; see
	/// [`Self::with_morsel_places`].
	morsel_places: Option<Arc<dyn crate::exec::places::MorselPlaces>>,
	/// The most async events an unleased batch runs at once on the task that runs it; see
	/// [`Builder::with_joined_event_workers`].
	joined_event_workers: Option<usize>,
	/// The entries a batch of async events fetches; see [`Builder::with_event_batch_size`].
	event_batch_size: u32,
	/// The clock the reclaim grace is judged against; see [`Builder::with_wall_clock`].
	wall_clock: Arc<dyn surrealdb_datastore::clock::WallClock>,
	/// The node whose async event queue this datastore's writes queue under, when it is not this
	/// datastore's own; see [`Builder::with_event_queue_node`].
	event_queue_node: Option<Uuid>,
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
	/// Where the table migration job's walk of its queues stands between
	/// passes; see [`table_migration::TableMigrationLap`].
	table_migration_lap: parking_lot::Mutex<table_migration::TableMigrationLap>,
	/// When the table migration job first saw each removal hold it has found standing; see
	/// [`table_migration::RemovalHoldsSeen`].
	removal_holds_seen: table_migration::RemovalHoldsSeen,
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
/// "surrealkv:...") and which clock to use. This lets the CLI and
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
	/// When the reclaim task first observed the entry, by the clock of the node
	/// holding the lease: its stamp, or for an unstamped doc-ID entry its
	/// sighting in [`Datastore::reclaim_sightings`]. Zero only where the grace is
	/// disabled, in which case every candidate ties and key order decides.
	observed_ms: u64,
	/// The queue key, as the scan returned it.
	key: Vec<u8>,
	/// The queue key, decoded during the walk.
	rc: ReclaimKey<'static>,
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
	/// Whether a cleanup that did not commit gave up on retryable conflicts
	/// that outlasted its retries, rather than failing on anything else.
	contended: bool,
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

/// How [`Datastore::delete_index_compaction_batch`] ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompactionCleanupEnd {
	/// The cleanup committed.
	Committed,
	/// Retryable conflicts outlasted the cleanup's retries.
	Contended,
	/// The cleanup failed on anything else.
	Failed,
}

/// What one bounded pass of index builds did: see [`Datastore::run_index_builds`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexBuildsRun {
	/// The build batches the pass ran.
	pub batches: usize,
	/// Whether a pending build is left that a pass may claim now: one this pass handed back
	/// with its budget spent, or one it did not reach.
	pub more: bool,
	/// Where a pending build is held by an owner other than this process's builders, the
	/// longest a pass waits before it may claim one: the soonest any such owner's lease lapses.
	/// An owner that is running passes hands its build back sooner, and wakes
	/// [`CommitTriggers::index_build`] when a statement hands one back.
	pub retry_after: Option<Duration>,
}

/// What one bounded pass of the graph fold job did: see [`Datastore::run_graph_fold`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct GraphFoldRun {
	/// The batches of queue entries the pass read.
	pub batches: usize,
	/// The scopes whose queued work the pass finished, their queue entries deleted.
	pub scopes: usize,
	/// The scopes whose fold failed: their entries stay queued, for a later pass to retry.
	pub failed: usize,
	/// Whether work is left that a pass run at once continues: the budget ran out with entries
	/// left, a scope's fold outlasted its passes, or a batch's cleanup kept losing conflicts.
	pub more: bool,
}

/// What one bounded pass of index compaction did: see [`Datastore::run_index_compaction`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct IndexCompactionRun {
	/// The compaction passes the call ran, one per index each queue batch it acted on named,
	/// each a bounded number of plans.
	pub passes: usize,
	/// Whether work is left that a call run at once continues: queue batches the call did not
	/// read, catalog its sweep did not reach, a request a pass queued for the work its budget
	/// left, or a batch whose cleanup gave up on conflicts and stays queued.
	pub more: bool,
	/// Where the call stopped on a batch whose cleanup failed on anything but a conflict, which
	/// stays queued and no commit announces again, how long a host waits before it calls again.
	pub retry_after: Option<Duration>,
}

/// What one call of [`Datastore::release_idle_writer_leases`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriterLeaseRelease {
	/// The shard leases the call released.
	pub released: usize,
	/// How long until a lease this datastore still holds goes idle, when a host calls again:
	/// `None` while it holds none, zero for one whose release failed and is to be retried.
	pub next_idle: Option<Duration>,
}

/// What one call of [`Datastore::index_compaction_drain`] did.
struct CompactionDrained {
	/// The queue batches the call acted on.
	batches: usize,
	/// The indexes that failed to compact.
	errors: usize,
	/// The compaction passes the call ran.
	passes: usize,
	/// Whether the call stopped on a batch whose cleanup failed on anything but
	/// a conflict, which stays queued.
	stalled: bool,
	/// The pass report.
	pass: Pass,
}

/// What one drain of the compaction queue has left queued so far, which its
/// later batches do not pass again, so a drain passes each index's kept
/// requests at most once.
#[derive(Clone, Default)]
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
	/// How far this drain has swept the defined indexes, which it does at most
	/// once whatever the sweep's throttle allows.
	sweep: DrainSweep,
}

/// How far a compaction drain's sweep of the defined indexes has got.
///
/// The sweep walks the catalog a page of [`INDEX_COMPACTION_SWEEP_STEPS`] steps
/// at a time, each page charged to the slice as a queue batch is, so a slice
/// reads a bounded part of the catalog however many indexes it holds, and the
/// walk keeps its place between the slices of a drain.
#[derive(Clone, Default)]
enum DrainSweep {
	/// The drain has not yet reached a queue drained as far as it can be.
	#[default]
	Unbegun,
	/// The walk, as far as the pages so far have taken it.
	Walking(Box<IndexWalk>),
	/// The walk reached the end of the catalog, failed, or was skipped by the
	/// throttle.
	Done,
}

/// What a drain's sweep left the drain to do.
enum Swept {
	/// A page queued requests, which the drain acts on.
	Queued,
	/// The slice's budget was spent with catalog still to walk.
	Spent,
	/// The sweep is over and queued nothing on its last page.
	Done,
	/// Another node took the lease before a page; the walk stays where it was.
	LeaseLost,
}

/// How long a host waits before it runs [`Datastore::run_index_compaction`]
/// again after a call stopped on a batch whose cleanup failed on anything but a
/// conflict: the batch stays queued, and no commit announces it again.
const INDEX_COMPACTION_CLEANUP_RETRY: Duration = Duration::from_secs(1);

/// Catalog steps one page of a compaction drain's sweep takes; see
/// [`INDEX_RESUME_PAGE_SIZE`], which bounds the stalled-build scan's pages the
/// same way. Every index a page reaches costs up to two probes besides.
const INDEX_COMPACTION_SWEEP_STEPS: usize = 256;

impl CompactionDrain {
	/// Whether a batch passes the index `key` names: an undecodable key never
	/// passes, nor does a key this drain kept, nor one of an index it kept a
	/// request for.
	fn passes(&self, key: &[u8], ikb: Option<&IndexKeyBase>) -> bool {
		ikb.is_some_and(|ikb| !self.kept.contains(key) && !self.retained.contains(ikb))
	}
}

/// Where the index-compaction drain stands between the calls that run it.
///
/// A drain is one run of the queue's rotation until it ends: on a lap that
/// acted on nothing, or once a sole writer's pass has covered the whole queue,
/// in either case with a sweep of the defined indexes that queued nothing.
/// A call that stops before then — at its batch budget, at a cleanup that did
/// not commit, or at a lease another node took — leaves the drain here for the
/// next call to continue; the call that ends it resets this, so the next call
/// begins a drain of its own from the head of the queue.
#[derive(Clone, Default)]
struct CompactionCursor {
	/// The queue position the last batch covered, or `None` at the head.
	resume_at: Option<Vec<u8>>,
	/// What the drain has kept, passed and swept so far.
	drain: CompactionDrain,
	/// Whether the current lap of the queue acted on a request: passed it, or
	/// deleted it.
	lap_passed: bool,
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
			maintenance: parking_lot::Mutex::new(MaintenanceDriver::Undriven),
			answers_write_barriers: std::sync::atomic::AtomicBool::new(false),
			host_maintenance: tokio::sync::Mutex::new(None),
			resume_walk: parking_lot::Mutex::new(None),
			build_walk: parking_lot::Mutex::new(None),
			rpc_session_cursor: parking_lot::Mutex::new(None),
			node_expire_cursor: parking_lot::Mutex::new(None),
			node_cleanup_cursor: parking_lot::Mutex::new(None),
			reclaim_cursor: parking_lot::Mutex::new(None),
			reclaim_sightings: parking_lot::Mutex::new(BTreeMap::new()),
			reclaim_sightings_since: std::sync::atomic::AtomicU64::new(0),
			changefeed_sweep: parking_lot::Mutex::new(ChangefeedSweep::default()),
			live_event_sweep: parking_lot::Mutex::new(ChangefeedSweep::default()),
			compaction_cursor: parking_lot::Mutex::new(CompactionCursor::default()),
			fold_cursor: parking_lot::Mutex::new(FoldCursor::default()),
			adoption_rotation: parking_lot::Mutex::new(None),
			event_laps: parking_lot::Mutex::new(HashMap::new()),
			engine_options: self.engine_options,
			shutdown_lock: tokio::sync::Mutex::new(()),
			id: self.id,
			running_events: RunningEvents::default(),
			auth_enabled: self.auth_enabled,
			dynamic_configuration: DynamicConfiguration::default(),
			slow_log: self.slow_log,
			transaction_timeout: self.transaction_timeout,
			capabilities: ArcSwap::new(self.capabilities.load_full()),
			live_query_broker: self.live_query_broker,
			changefeed_source: self.changefeed_source,
			// Fresh router cursor: a restarted node re-establishes its baseline.
			live_query_router: Arc::new(LiveQueryRouter::new()),
			scheduler: self.scheduler,
			morsel_places: self.morsel_places,
			joined_event_workers: self.joined_event_workers,
			event_batch_size: self.event_batch_size,
			wall_clock: Arc::clone(&self.wall_clock),
			event_queue_node: self.event_queue_node,
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
				self.config.idx.fulltext_cache_size,
				self.config.idx.segment_dictionary_path.clone(),
			),
			index_builder: IndexBuilder::new(
				self.transaction_factory.clone(),
				self.index_builder.spawner(),
				self.index_builder.inline_batches(),
			),
			compaction_swept_at: parking_lot::Mutex::new(None),
			table_migration_lap: parking_lot::Mutex::new(Default::default()),
			removal_holds_seen: Default::default(),
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
			migrated: self.migrated,
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
			maintenance: parking_lot::Mutex::new(MaintenanceDriver::Undriven),
			answers_write_barriers: std::sync::atomic::AtomicBool::new(false),
			host_maintenance: tokio::sync::Mutex::new(None),
			resume_walk: parking_lot::Mutex::new(None),
			build_walk: parking_lot::Mutex::new(None),
			rpc_session_cursor: parking_lot::Mutex::new(None),
			node_expire_cursor: parking_lot::Mutex::new(None),
			node_cleanup_cursor: parking_lot::Mutex::new(None),
			reclaim_cursor: parking_lot::Mutex::new(None),
			reclaim_sightings: parking_lot::Mutex::new(BTreeMap::new()),
			reclaim_sightings_since: std::sync::atomic::AtomicU64::new(0),
			changefeed_sweep: parking_lot::Mutex::new(ChangefeedSweep::default()),
			live_event_sweep: parking_lot::Mutex::new(ChangefeedSweep::default()),
			compaction_cursor: parking_lot::Mutex::new(CompactionCursor::default()),
			fold_cursor: parking_lot::Mutex::new(FoldCursor::default()),
			adoption_rotation: parking_lot::Mutex::new(None),
			event_laps: parking_lot::Mutex::new(HashMap::new()),
			engine_options: self.engine_options,
			shutdown_lock: tokio::sync::Mutex::new(()),
			id,
			running_events: RunningEvents::default(),
			auth_enabled: self.auth_enabled,
			dynamic_configuration: self.dynamic_configuration.clone(),
			slow_log: self.slow_log.clone(),
			transaction_timeout: self.transaction_timeout,
			capabilities: ArcSwap::new(self.capabilities.load_full()),
			live_query_broker: self.live_query_broker.clone(),
			changefeed_source: self.changefeed_source.clone(),
			// A fork models a separate node, so it gets its own router cursor.
			live_query_router: Arc::new(LiveQueryRouter::new()),
			scheduler: self.scheduler.clone(),
			morsel_places: self.morsel_places.clone(),
			joined_event_workers: self.joined_event_workers,
			event_batch_size: self.event_batch_size,
			wall_clock: Arc::clone(&self.wall_clock),
			event_queue_node: self.event_queue_node,
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
				self.config.idx.fulltext_cache_size,
				self.config.idx.segment_dictionary_path.clone(),
			),
			index_builder: IndexBuilder::new(
				transaction_factory.clone(),
				self.index_builder.spawner(),
				self.index_builder.inline_batches(),
			),
			compaction_swept_at: parking_lot::Mutex::new(None),
			table_migration_lap: parking_lot::Mutex::new(Default::default()),
			removal_holds_seen: Default::default(),
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
			migrated: tokio::sync::watch::channel(self.is_migrated()).0,
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

	/// A fork as another process writing the same store would be: its own node id, and its own
	/// commit triggers, so it counts its own transactions; both datastores then take the
	/// protocols of a store other processes write too.
	#[cfg(test)]
	pub(crate) fn fork_for_test_as_another_process(&self, id: Uuid) -> Self {
		let triggers = Arc::new(CommitTriggers::new());
		triggers.assume_shared_store();
		self.triggers.assume_shared_store();
		let transaction_factory = self.transaction_factory.with_triggers(Arc::clone(&triggers));
		let mut fork = self.fork_for_test_with_node_id(id);
		fork.index_builder = IndexBuilder::new(
			transaction_factory.clone(),
			self.index_builder.spawner(),
			self.index_builder.inline_batches(),
		);
		fork.sequences = Sequences::new(transaction_factory.clone(), id);
		fork.transaction_factory = transaction_factory;
		fork.triggers = triggers;
		fork
	}

	/// Leave this datastore's writes unfenced against later catalog changes, as on a store whose
	/// locked reads conflict with one another, so its builds ask every other live node to answer
	/// their write barriers as builds on such a store do
	/// ([`surrealdb_datastore::tx::Transaction::fences_catalog_changes`]).
	#[cfg(test)]
	pub(crate) fn assume_unfenced_catalog_for_test(&self) {
		self.triggers.assume_unfenced_catalog();
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
	pub(crate) fn live_query_broker(&self) -> Option<Arc<OrderedBroker>> {
		self.live_query_broker.clone()
	}

	/// This node's live-query router; see [`crate::lq`].
	#[cfg(test)]
	pub(crate) fn live_query_router(&self) -> &LiveQueryRouter {
		&self.live_query_router
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

	/// Set the places a host runs the morsels of a morsel pipeline at, for a host that fans a
	/// scan out beyond the node ([`MorselPlaces`](crate::exec::places::MorselPlaces)). Without
	/// one, morsels run on the node's own places.
	pub fn with_morsel_places(
		mut self,
		places: Arc<dyn crate::exec::places::MorselPlaces>,
	) -> Self {
		self.morsel_places = Some(places);
		self
	}

	/// Run `morsel` of the morsel unit `unit` as a host's leaf does, reading the morsel's range
	/// from `reader`, and answer its frames ([`crate::exec::places`]). The leaf's own engine reads
	/// nothing: the run reads only the morsel's range, from `reader`, which answers what the read
	/// view of the transaction that handed the morsel out answers. A unit or morsel this build
	/// does not run is answered as a failure of the place.
	pub async fn run_morsel(
		&self,
		unit: &[u8],
		morsel: &crate::exec::places::Morsel,
		reader: Arc<dyn surrealdb_kvs::Snapshot>,
	) -> crate::exec::places::MorselFrames {
		match self.leaf_context().await {
			Ok(ctx) => crate::exec::vm::morsel::run_leaf(unit, morsel, reader, ctx),
			Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
		}
	}

	/// The root context a leaf runs a morsel in: this engine's, over a read transaction the run
	/// never reads through, under the leaf's surface ([`LEAF`](crate::exec::surface::LEAF)): a unit
	/// holds no statement and reads no catalog entry, so a leaf reaches no statement family and no
	/// catalog decoder through it.
	async fn leaf_context(&self) -> Result<crate::exec::ExecutionContext> {
		use crate::exec::context::RootContext;
		let txn = self.transaction(TransactionType::Read).await?;
		let mut ctx = self.setup_ctx_with(&crate::exec::surface::LEAF, &self.compiled_units)?;
		ctx.set_transaction(Arc::new(txn));
		Ok(crate::exec::ExecutionContext::Root(RootContext {
			ctx: ctx.freeze(),
			options: None,
			datastore: None,
			cancellation: Arc::new(tokio_util::sync::CancellationToken::new()),
			auth: Arc::new(crate::iam::Auth::default()),
			session: None,
			current_value: None,
			current_row: None,
			forbidden_row: None,
			inherited_knn: None,
			skip_fetch_perms: false,
			perms_suppressed: false,
			caller_perms_suppressed: false,
			caller_view_checked: false,
			computing_record: None,
			no_write: None,
			version_stamp: None,
			guard_verdicts: Default::default(),
			writing: false,
			nested_runs: Default::default(),
			session_effects: None,
		}))
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
			("morsel_needs_view", exec.morsel_needs_view.to_string()),
			("morsel_split_pass_ppm", exec.morsel_split_pass_ppm.to_string()),
			("morsel_target_bytes", exec.morsel_target_bytes.to_string()),
			("morsel_places", exec.morsel_places.to_string()),
			("morsel_loopback_places", exec.morsel_loopback_places.to_string()),
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

	pub(crate) fn clock_now(&self) -> Timestamp {
		SystemClock::new().now()
	}

	/// The current time on the datastore's wall clock ([`Builder::with_wall_clock`]), in unix
	/// milliseconds; zero for an instant before the epoch.
	fn wall_clock_ms(&self) -> u64 {
		u64::try_from(self.wall_clock.now().timestamp_millis()).unwrap_or(0)
	}

	/// When this datastore was opened, on the clock [`Self::clock_now`] reads.
	pub(crate) fn running_events(&self) -> &RunningEvents {
		&self.running_events
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
		// Refused before any migration, so a refused store is left as it was found.
		self.refuse_stored_models().await?;

		// `get_version` reports a datastore as new only on the start that wrote
		// `!v`. Everything below can fail transiently and be retried by the
		// caller, and that retry finds the key present — so the flag is latched on
		// the datastore rather than re-derived, or a caller keying default
		// namespace creation off it would silently skip that on the second pass.
		let is_new =
			self.created_here.fetch_or(is_new, std::sync::atomic::Ordering::SeqCst) || is_new;
		// Refused before any migration, so a refused store is left as it was found.
		self.refuse_upgrade_from_3x().await?;
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
		// Every maintenance job but the heartbeat waits for this.
		self.mark_migrated();
		// Everything ok
		Ok((version, is_new))
	}

	/// Refuses a datastore a 3.x release last wrote, unless the
	/// `experimental_upgrade_from_3x` configuration key opts in to upgrading it
	/// in place, in which case it logs a warning and allows it.
	///
	/// Judged on the version stamp at `/!vs`. A store stamped below 4.0.0 was
	/// last advanced by a 3.x release. So was a store with no stamp at
	/// [`FeatureLevel::Legacy`], which predates the stamp. A store with no stamp
	/// at a stored feature level was created by this line and has yet to be
	/// stamped. A store this process created is never refused.
	///
	/// Reads only, so a refused store is left as it was found. Call it after the
	/// feature level is settled and [`Self::created_here`] is latched, and
	/// before anything writes to the store.
	pub(crate) async fn refuse_upgrade_from_3x(&self) -> Result<()> {
		if self.created_here.load(std::sync::atomic::Ordering::SeqCst) {
			return Ok(());
		}
		let stored = match self.storage_version().await? {
			Some(stored) if stored.triple().0 >= 4 => return Ok(()),
			Some(stored) => stored.to_string(),
			None if !self.feature_cell().level().is_legacy() => return Ok(()),
			None => "3.x (earlier than 3.3.0)".to_owned(),
		};
		let running = StorageVersion::current().to_string();
		if !self.config.datastore.experimental_upgrade_from_3x {
			bail!(DatastoreError::UpgradeFrom3xNotEnabled {
				stored,
				running,
			});
		}
		warn!(
			target: TARGET,
			%stored,
			%running,
			"Upgrading a datastore last written by a 3.x release in place. This upgrade is \
			 experimental and was enabled by SURREAL_EXPERIMENTAL_UPGRADE_FROM_3X"
		);
		Ok(())
	}

	/// Refuses a datastore that still records a `DEFINE MODEL` definition: this build
	/// does not run models, so it cannot serve a database that holds one.
	async fn refuse_stored_models(&self) -> Result<()> {
		let txn = self.transaction(Read).await?;
		let found = catch!(txn, Self::first_stored_model(&txn).await);
		txn.cancel().await?;
		if let Some(error) = found {
			bail!(error);
		}
		Ok(())
	}

	/// Finds the first `DEFINE MODEL` definition in the datastore, as the error that refuses it.
	async fn first_stored_model(txn: &Transaction) -> Result<Option<DatastoreError>> {
		use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};

		for ns in txn.all_ns(None).await?.iter() {
			for db in txn.all_db(ns.namespace_id, None).await?.iter() {
				let models = txn.all_db_models(ns.namespace_id, db.database_id, None).await?;
				if let Some(model) = models.first() {
					return Ok(Some(DatastoreError::StoredModels {
						ns: ns.name.to_string(),
						db: db.name.to_string(),
						model: model.name.to_string(),
						version: model.version.to_string(),
					}));
				}
			}
		}
		Ok(None)
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
		// Refused before the datastore is attached, as `check_version` refuses it.
		self.refuse_stored_models().await?;
		let is_new =
			self.created_here.fetch_or(is_new, std::sync::atomic::Ordering::SeqCst) || is_new;
		if let migration::LedgerVerdict::ReadOnly {
			features,
		} = verdict
		{
			self.open_read_only(features);
		} else {
			migration::attach(self, is_new).await?;
		}
		// The store is attached only once it owes this build no migration, or
		// opened read-only where none could run, so the jobs a host drives through
		// `run_maintenance_slice` are released as `check_version` releases them.
		self.mark_migrated();
		Ok(())
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

	/// Records that the datastore's data migrations have run, releasing the
	/// maintenance jobs waiting for them. See [`Self::is_migrated`].
	pub(crate) fn mark_migrated(&self) {
		self.migrated.send_replace(true);
	}

	/// Whether the datastore's data migrations have run.
	///
	/// Every maintenance job but the cluster heartbeat waits for this before
	/// its first pass, and a host-driven slice is refused until then. Set by
	/// [`Self::check_version`], and already set on a datastore this process
	/// created or opened to find fully migrated.
	pub(crate) fn is_migrated(&self) -> bool {
		*self.migrated.borrow()
	}

	/// A receiver that resolves once [`Self::is_migrated`] does, for a task to
	/// wait on.
	pub(crate) fn subscribe_migrated(&self) -> tokio::sync::watch::Receiver<bool> {
		self.migrated.subscribe()
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
	pub(crate) fn feature_cell(&self) -> &Arc<FeatureLevelCell> {
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
		ensure!(
			!user.trim().is_empty() && !pass.trim().is_empty(),
			"Initial root username and password must not be empty"
		);
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
	/// Every job but the heartbeat also waits for that call to have run the
	/// datastore's migrations, so a datastore that owes them runs no other
	/// maintenance until it returns, however early the tasks are started.
	/// A datastore that is open read-only ([`Self::is_read_only`]) starts only the
	/// node-membership refresh, because every other task writes state it refuses.
	///
	/// Idempotent: a datastore that already carries its tasks is left alone.
	///
	/// It does not check the node membership refresh interval against the write fence these
	/// tasks arm on a store other nodes write; [`Self::start_maintenance_tasks_checked`] does.
	///
	/// Refused once a host has driven a slice through
	/// [`Self::run_maintenance_slice`]: the tasks would run every job a second
	/// time alongside it.
	pub fn start_maintenance_tasks(self: &Arc<Self>) -> Result<()> {
		let mut driver = self.maintenance.lock();
		match &*driver {
			MaintenanceDriver::Tasks(_) => Ok(()),
			MaintenanceDriver::Host => bail!(
				"A host drives this datastore's maintenance; its own tasks would run every job twice"
			),
			MaintenanceDriver::Undriven => {
				let tasks =
					crate::kvs::tasks::init(self, self.shutdown.clone(), &self.engine_options);
				// A build without `maintenance-tasks` started none, and records
				// none, so a host may still drive the datastore's maintenance.
				if cfg!(feature = "maintenance-tasks") {
					*driver = MaintenanceDriver::Tasks(tasks);
				}
				Ok(())
			}
		}
	}

	/// [`Self::start_maintenance_tasks`], refusing first, as a datastore that starts them at
	/// construction does, a node membership refresh interval the write fence they arm cannot run
	/// with (see [`crate::kvs::write_barrier`]).
	///
	/// A node registered before this ([`Self::insert_node`]) records, before it returns, that it
	/// answers write barriers ([`Self::publish_write_barrier_support`]), so it serves no write a
	/// build elsewhere would take for one from a node without the barrier.
	///
	/// Refused, before anything is recorded, once a host drives the datastore's maintenance, as
	/// [`Self::start_maintenance_tasks`] is. A datastore whose heartbeat is disabled records
	/// nothing either: its tasks answer no barriers, since the write fence they would arm lapses
	/// without a heartbeat.
	pub async fn start_maintenance_tasks_checked(self: &Arc<Self>) -> Result<()> {
		self.check_heartbeat_interval(self.engine_options.node_membership_refresh_interval).await?;
		let answers = !self.engine_options.node_membership_refresh_interval.is_zero()
			&& !self.is_read_only()
			&& cfg!(feature = "maintenance-tasks");
		match &*self.maintenance.lock() {
			MaintenanceDriver::Tasks(_) => return Ok(()),
			MaintenanceDriver::Host => bail!(
				"A host drives this datastore's maintenance; its own tasks would run every job twice"
			),
			// Begun under the lock a host's first slice takes, so that slice is refused rather
			// than taking over a datastore that has begun recording that it answers barriers.
			MaintenanceDriver::Undriven if answers => self.begin_answering_write_barriers(),
			MaintenanceDriver::Undriven => {}
		}
		if answers {
			self.publish_write_barrier_support().await?;
		}
		self.start_maintenance_tasks()
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
		matches!(&*self.maintenance.lock(), MaintenanceDriver::Tasks(_))
	}

	/// Runs one bounded slice of this node's background maintenance, for a host
	/// that drives it itself instead of running the datastore's own tasks.
	///
	/// The datastore's tasks are the normal driver, and this is refused once
	/// they run: two drivers would run every job twice and contend for their
	/// own leases. The first slice makes the host the driver for the life of
	/// the datastore, and [`Self::start_maintenance_tasks`] is refused from
	/// then on for the same reason. It exists for a host built with
	/// [`Builder::without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks)
	/// that cannot run free-running tasks — a runtime with no timers between
	/// requests — and calls this from its own schedule: on a request, on a host
	/// timer, or as soon as the returned `next_due` allows. Each call runs at
	/// most one job pass, bounded by that job's slice budget, and never waits
	/// for a job that is not yet due. A result of [`Progress::More`] means the
	/// job stopped at its budget with work still queued; call again at once.
	///
	/// Every job the tasks would run is on this schedule at the same cadence,
	/// the cluster heartbeat included, so a host calling this as often as
	/// `next_due` asks keeps its membership row fresh. Answering write
	/// barriers is not ([`crate::kvs::write_barrier`]): it arms a fence that
	/// refuses this node's writes once its heartbeat lapses, which a host's
	/// schedule does not promise to prevent. A host-driven node answers none,
	/// and records none, so builds elsewhere take it for a node that predates
	/// the barrier and do not wait for its writes. Refused once a checked start
	/// of the datastore's own tasks has begun answering them
	/// ([`Self::start_maintenance_tasks_checked`]).
	///
	/// Refused until [`Self::check_version`] has run the datastore's
	/// migrations: every job but the heartbeat waits for them, and a host
	/// drives its jobs synchronously, so there is nothing to park a slice on
	/// until then.
	///
	/// After [`Self::shutdown`] nothing is scheduled: a slice then runs no job
	/// and reports no `next_due`, which is the host's cue to stop calling. A
	/// slice that shutdown began during ends the same way once its job has
	/// run.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn run_maintenance_slice(self: &Arc<Self>) -> Result<MaintenanceSlice> {
		// Checked before the driver is taken, so a host refused here can still
		// start the datastore's own tasks instead.
		if !self.is_migrated() {
			bail!(
				"The datastore's migrations have not run; call `check_version` before driving its maintenance"
			);
		}
		{
			let mut driver = self.maintenance.lock();
			match &*driver {
				MaintenanceDriver::Tasks(_) => bail!(
					"The datastore runs its own maintenance tasks; a host-driven slice would run every job twice"
				),
				MaintenanceDriver::Host => {}
				// A checked start of the datastore's own tasks has begun answering write
				// barriers: a host driving the jobs instead would leave them unanswered, and
				// every build elsewhere waiting for this node.
				MaintenanceDriver::Undriven
					if self.answers_write_barriers.load(std::sync::atomic::Ordering::SeqCst) =>
				{
					bail!(
						"The datastore is starting its own maintenance tasks; a host-driven slice would run every job twice"
					)
				}
				MaintenanceDriver::Undriven => *driver = MaintenanceDriver::Host,
			}
		}
		let mut host = self.host_maintenance.lock().await;
		if self.shutdown.is_cancelled() {
			// Checked under the lock, so a slice that arrives once shutdown has
			// dropped the schedule does not build it again.
			return Ok(MaintenanceSlice {
				job: None,
				progress: Progress::Done,
				next_due: None,
			});
		}
		let slice = host
			.get_or_insert_with(|| {
				HostMaintenance::new(self, self.shutdown.clone(), &self.engine_options)
			})
			.step(self)
			.await;
		if self.shutdown.is_cancelled() {
			// Shutdown began during this slice, and may have stopped waiting
			// for it: the schedule goes with the slice either way, so the claim
			// its runner holds on the process metrics refresh is released now
			// rather than with the datastore, and nothing is scheduled again.
			*host = None;
			return Ok(MaintenanceSlice {
				job: slice.job,
				progress: Progress::Done,
				next_due: None,
			});
		}
		Ok(slice)
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
		let tasks = self.maintenance.lock().take_tasks();
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
		// A host driving maintenance through `run_maintenance_slice` holds this
		// lock for the length of one pass, so taking it waits for a pass in
		// flight to end — for the same reason, and with the same bound, as the
		// tasks above. The cancellation above ends the pass at its next
		// boundary. The schedule goes with the pass: its runner holds this
		// datastore's claim on the process metrics refresh, and a datastore
		// that stays referenced after shutting down must not keep that claim
		// from the others in the process. A pass that outlives the wait fails
		// at its next transaction, as the index builds below do, and drops the
		// schedule itself when it returns.
		match timeout(MAINTENANCE_SHUTDOWN_TIMEOUT, self.host_maintenance.lock()).await {
			Ok(mut host) => *host = None,
			Err(_) => warn!(
				target: TARGET,
				"A host-driven maintenance pass did not stop within {MAINTENANCE_SHUTDOWN_TIMEOUT:?}; \
				 continuing shutdown without it"
			),
		}
		// Local index builder tasks are deliberately left running: the storage
		// engine shutdown below closes commit admission first, after which
		// every commit is refused before it applies (see the engine
		// `Transaction::commit` gate). An in-flight build therefore makes no
		// durable change during shutdown and stays at its last committed
		// checkpoint, so the periodic resume scan continues it after restart —
		// exactly as it would after a crash. Its owner lease is lapsed first,
		// while commits are still accepted, so that scan continues it on its
		// next pass rather than once the lease runs out: until then a write to
		// a table whose build stopped in `Closing` would wait for it.
		self.index_builder.lapse_local_build_leases().await;
		// Once this node is archived, no build elsewhere waits for its writes (see
		// `crate::kvs::write_barrier`), so no write commits after that: they are refused from
		// here on, and those already committing are waited for, within the archive's own bound.
		// On a store this datastore writes alone the fence takes no part, and no other node builds.
		self.transaction_factory.write_fence().close();
		let shared = match self.transaction(TransactionType::Read).await {
			Ok(txn) => {
				let shared = !txn.sole_writer();
				let _ = txn.cancel().await;
				shared
			}
			Err(_) => false,
		};
		if shared {
			let live = self.transaction_factory.live_writes();
			let since = live.advance();
			let deadline = Instant::now() + NODE_DELETE_TIMEOUT;
			while !live.ended_since(since) && Instant::now() < deadline {
				sleep(Duration::from_millis(10)).await;
			}
			// Once every write has ended, this node answers every barrier, so no build waits out
			// the liveness window for a node that left cleanly.
			if live.ended_since(since)
				&& let Err(err) = self.retire_write_barrier_answer().await
			{
				warn!(target: TARGET, "Could not answer the write barrier on shutdown: {err}");
			}
		}
		// Archive this datastore in the cluster, but don't let a blocked
		// metadata transaction prevent storage engine shutdown.
		let _ = archive_node_for_shutdown(
			NODE_DELETE_TIMEOUT,
			self.delete_node_with_timeout(NODE_DELETE_TIMEOUT).await,
		);
		if let Err(err) = self.hand_back_event_queues_if_last_upgraded().await {
			warn!(target: TARGET, "Could not hand back per-node event queues on shutdown: {err}");
		}
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
	/// Resolved by capability through [`TransactionBuilder::extension`]; no-op
	/// on backends that publish no [`Maintenance`]. Background tasks call this
	/// on `EngineOptions::mvcc_gc_interval`.
	pub(crate) async fn run_mvcc_gc(&self, lifetime: Duration) -> Result<()> {
		match self.maintenance_ops() {
			Some(ops) => ops.collect_garbage(lifetime).await.map_err(Into::into),
			None => Ok(()),
		}
	}

	/// Resolve stale transactional locks left by crashed clients.
	///
	/// Resolved by capability through [`TransactionBuilder::extension`]; no-op
	/// on backends that publish no [`Maintenance`]. Background tasks call this
	/// on `EngineOptions::lock_cleanup_interval`.
	pub(crate) async fn run_lock_cleanup(&self, lifetime: Duration) -> Result<()> {
		match self.maintenance_ops() {
			Some(ops) => ops.cleanup_locks(lifetime).await.map_err(Into::into),
			None => Ok(()),
		}
	}

	/// Number of in-flight transactions tracked by the backend, when
	/// available. `None` indicates the backend does not track this.
	pub fn in_flight_transaction_count(&self) -> Option<usize> {
		self.maintenance_ops().and_then(|ops| ops.in_flight_transactions())
	}

	/// The backend's maintenance capability, or `None` when it publishes none.
	/// Resolved by capability rather than by backend name, like
	/// [`Self::destroy_range_ops`].
	fn maintenance_ops(&self) -> Option<Arc<dyn Maintenance>> {
		let ext = self.transaction_factory.extension(TypeId::of::<MaintenanceHandle>())?;
		let handle = ext.downcast::<MaintenanceHandle>().ok()?;
		Some(Arc::clone(&handle.0))
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
		// A node whose heartbeat has lapsed refuses its own writes until a heartbeat commits.
		txn.exempt_from_write_fence();
		if replace {
			txn.replace_key(key, node).await?;
		} else {
			txn.set_key(key, node).await?;
		}
		// Marks this heartbeat as one from a node that answers the write barrier
		// (`crate::kvs::write_barrier`). A node that does not leaves it unmarked, and builds take
		// it for one that predates the barrier.
		if self.advertises_write_barrier() {
			txn.set_key(
				&crate::key::schema::WriteBarrierHeartbeatKey {
					nd: key.nd,
				},
				&node.heartbeat.value,
			)
			.await?;
			// Marks it too as one from a node whose writes the store fences against a later
			// change to their table's catalog, which lets a build leave this node out of its
			// barrier ([`crate::kvs::write_barrier::WriteBarrier::start`]).
			if txn.fences_catalog_changes() {
				txn.set_key(
					&crate::key::schema::CatalogFenceHeartbeatKey {
						nd: key.nd,
					},
					&node.heartbeat.value,
				)
				.await?;
			}
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
		catch!(txn, self.write_node(&txn, &key, &node, false).await);
		// Registered before this node opens a transaction of its own, so it answers every write
		// barrier asked so far (see `crate::kvs::write_barrier`). A node that answers none records
		// no answer, and builds take it for one that predates the barrier.
		if self.advertises_write_barrier() {
			catch!(txn, self.register_write_barrier_answer(&txn).await);
		}
		run!(txn, self.stamp_drain_heartbeat(&txn, now).await)
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
		catch!(txn, self.write_node(&txn, &key, &node, true).await);
		run!(txn, self.stamp_drain_heartbeat(&txn, now).await)
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
		await_node_tx_step(
			&txn,
			deadline,
			timeout_duration,
			Some(canceller),
			self.stamp_drain_heartbeat(&txn, now),
		)
		.await?;
		await_node_tx_step(&txn, deadline, timeout_duration, Some(canceller), txn.commit()).await
	}

	/// Writes the drain heartbeat beside this node's row, in the transaction
	/// that writes the row with the same `heartbeat`. Every write of the row
	/// by this release goes through here, so the two read equal for as long
	/// as this release keeps the row fresh; see [`NodeDrainHeartbeatKey`].
	async fn stamp_drain_heartbeat(&self, txn: &Transaction, heartbeat: Timestamp) -> Result<()> {
		let key = NodeDrainHeartbeatKey {
			nd: self.id,
		};
		txn.set_key(&key, &heartbeat.value).await
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

	/// Expires nodes which have timed out from the cluster.
	///
	/// This function should be run periodically at an interval.
	///
	/// This function marks the node as archived, ready for garbage collection.
	/// Later on when garbage collection is running the live queries assigned
	/// to this node will be removed, along with the node itself.
	///
	/// Runs [`Self::expire_nodes_slice`] until a pass reports
	/// [`Progress::Done`], so every expired node is archived however many rows
	/// the cluster holds: the passes are bounded, the call is not.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn expire_nodes(&self) -> Result<()> {
		while self.expire_nodes_slice().await?.progress == Progress::More {}
		Ok(())
	}

	/// One pass of [`Self::expire_nodes`] as a job pass.
	///
	/// Bounded: a pass scans at most [`NODE_EXPIRE_SLICE_BATCHES`] pages of
	/// node rows, archives the expired nodes on each page in a commit of its
	/// own, and one that stops with pages still to scan keeps its position and
	/// reports [`Progress::More`], so the next pass continues after the page
	/// it stopped at. A scan ends at the last row there was when it began:
	/// rows registered past that key while it runs are the next scan's, so a
	/// scan ends however fast nodes register.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub(crate) async fn expire_nodes_slice(&self) -> Result<Pass> {
		self.expire_nodes_bounded(NODE_EXPIRE_SLICE_BATCHES).await
	}

	/// [`Self::expire_nodes_slice`] with the per-pass page budget lowered, so
	/// a test can drive a pass that stops mid-scan and inspect what it left.
	#[cfg(test)]
	pub(crate) async fn expire_nodes_with_budget(&self, pages: usize) -> Result<Pass> {
		self.expire_nodes_bounded(pages).await
	}

	/// One node expiry pass, scanning at most `pages` pages of node rows.
	async fn expire_nodes_bounded(&self, pages: usize) -> Result<Pass> {
		// Log when this method is run
		trace!(target: TARGET, "Archiving expired nodes in the cluster");
		let mut budget = pages;
		let full = NodePrefix {}.range()?;
		// Continue the scan the previous pass stopped in, or begin one over
		// the rows there are now.
		let resumed = self.node_expire_cursor.lock().take();
		let (mut after, end) = match resumed {
			Some(scan) => (Some(scan.after), scan.end),
			None => match self.range_end(full.clone()).await? {
				Some(end) => (None, end),
				// No node is registered.
				None => return Ok(Pass::done(TaskLeaseState::NotLeased)),
			},
		};
		// The nodes this pass archived.
		let mut items = 0;
		loop {
			if budget == 0 {
				// Spent with a page still to scan: the next pass resumes here.
				if let Some(after) = after {
					*self.node_expire_cursor.lock() = Some(KeyScan {
						after,
						end,
					});
				}
				return Ok(Pass {
					progress: Progress::More,
					items,
					oldest_age: None,
					held: 0,
					ran: items,
					lease: TaskLeaseState::NotLeased,
				});
			}
			budget -= 1;
			let range = match &after {
				Some(after) => full.clone().resume_after(after, Direction::Forward),
				None => full.clone(),
			};
			// One page of node rows, read in a short-lived read transaction.
			let batch = {
				let txn = self.transaction(Read).await?;
				let res = txn.batch_keys_vals(range, NORMAL_BATCH_SIZE, None).await;
				let _ = txn.cancel().await;
				res?
			};
			let now = self.clock_now();
			// The expired nodes on the page: active, and silent for longer
			// than the heartbeat allows.
			let mut inactive = Vec::new();
			for (_, v) in batch.result.iter() {
				let node: Node = KVValue::kv_decode_value(v, ())?;
				if node.is_active() && node.heartbeat < now - Duration::from_secs(30) {
					inactive.push(node);
				}
			}
			// Check if there are inactive nodes
			if !inactive.is_empty() {
				// Open a writeable transaction
				let txn = self.transaction(Write).await?;
				// How many candidates still qualified once re-read below.
				let mut archived = 0;
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
					archived += 1;
				}
				// Commit the changes, or release the transaction when every candidate
				// turned out to be live after all
				match archived {
					0 => catch!(txn, txn.cancel().await),
					_ => catch!(txn, txn.commit().await),
				}
				items += archived;
			}
			// A full page carries a continuation: the next read resumes after
			// the last key this page returned. The page that reaches the end
			// the scan began with ends it: what lies past that key was
			// registered since, and is the next scan's.
			let more = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) if *k < end => {
					after = Some(k.clone());
					true
				}
				_ => false,
			};
			if !more {
				break;
			}
		}
		// Everything was successful
		Ok(Pass {
			progress: Progress::Done,
			items,
			oldest_age: None,
			held: 0,
			ran: items,
			lease: TaskLeaseState::NotLeased,
		})
	}

	/// Removes and cleans up nodes which are not in this cluster any more.
	///
	/// This function should be run periodically at an interval.
	///
	/// Every node marked archived has its live queries deleted, both the
	/// node's registrations and the tables' entries for them, and then its
	/// row, with the drain heartbeat, release record and write barrier keys
	/// written beside it, once its heartbeat is
	/// [`exclude_after`](crate::kvs::write_barrier::exclude_after)
	/// old: until then a build may wait for its write barrier answer, and finds
	/// the node only through its row. A
	/// node's queued async events are left in place: the event adoption sweep
	/// ([`Self::event_processing_adopt`]) finds a queue by its own prefix,
	/// whether or not a row for its owner remains, and drains it. The release
	/// records of nodes whose row is gone are swept once the rows are scanned.
	///
	/// Runs [`Self::remove_nodes_slice`] until a pass reports
	/// [`Progress::Done`], so every archived node is cleaned up however many
	/// live queries it left behind, and every orphaned record swept: the
	/// passes are bounded, the call is not.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn remove_nodes(&self) -> Result<()> {
		while self.remove_nodes_slice().await?.progress == Progress::More {}
		Ok(())
	}

	/// One pass of [`Self::remove_nodes`] as a job pass.
	///
	/// Bounded: a pass reads at most [`NODE_CLEANUP_SLICE_BATCHES`] pages —
	/// of node rows, then of the release records whose node row is gone,
	/// which it sweeps with the pages left once the rows are scanned — and
	/// deletes at most as many pages of keys, the archived nodes' live
	/// queries a page per commit and then their rows, and the orphaned
	/// records a page per commit. A pass that stops with rows or records
	/// still to read, or a node still to clean, keeps its position and
	/// reports [`Progress::More`]. What a pass deleted is committed, so the
	/// next pass finds what remains of a node, or of a page of records, at
	/// the head of what it reads. A scan ends at the last key there was when
	/// it began: rows and records written past it while the scan runs are
	/// the next scan's, so a scan ends however fast nodes register.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub(crate) async fn remove_nodes_slice(&self) -> Result<Pass> {
		self.remove_nodes_bounded(
			NODE_CLEANUP_SLICE_BATCHES,
			NODE_CLEANUP_SLICE_BATCHES as u64 * u64::from(NORMAL_BATCH_SIZE),
		)
		.await
	}

	/// [`Self::remove_nodes_slice`] with the per-pass page and key budgets
	/// lowered, so a test can drive a pass that stops mid-scan or mid-node and
	/// inspect what it left behind.
	#[cfg(test)]
	pub(crate) async fn remove_nodes_with_budget(&self, pages: usize, budget: u64) -> Result<Pass> {
		self.remove_nodes_bounded(pages, budget).await
	}

	/// One node cleanup pass, scanning at most `pages` pages of node rows and
	/// deleting at most `budget` keys: an archived node's live queries a page
	/// per commit, then its row.
	async fn remove_nodes_bounded(&self, pages: usize, budget: u64) -> Result<Pass> {
		// Log when this method is run
		trace!(target: TARGET, "Cleaning up archived nodes in the cluster");
		let mut pages = pages;
		// Keys this pass may still delete, and the keys it has deleted.
		let mut budget = budget;
		let mut items = 0;
		let full = NodePrefix {}.range()?;
		// Continue the scan the previous pass stopped in, or begin one over
		// the rows there are now.
		let resumed = self.node_cleanup_cursor.lock().take();
		let (mut after, end) = match resumed {
			Some(NodeCleanupScan::Rows(scan)) => (Some(scan.after), scan.end),
			// The rows were scanned by an earlier pass: the sweep continues.
			Some(NodeCleanupScan::Releases {
				after,
				end,
			}) => return self.sweep_node_releases(pages, budget, 0, Some((after, end))).await,
			None => match self.range_end(full.clone()).await? {
				Some(end) => (None, end),
				// No node is registered: only release records can be left.
				None => return self.sweep_node_releases(pages, budget, 0, None).await,
			},
		};
		loop {
			if pages == 0 || budget == 0 {
				// Spent with a page still to scan: the next pass resumes here.
				return Ok(self.node_cleanup_stopped(after, end, items));
			}
			pages -= 1;
			// Where this page is read from. A pass that spends its keys among
			// the page's archived nodes resumes here: a cleaned node's row is
			// gone, so the page read again holds only what is left to clean.
			let page_after = after.clone();
			let range = match &after {
				Some(after) => full.clone().resume_after(after, Direction::Forward),
				None => full.clone(),
			};
			// One page of node rows, read in a short-lived read transaction.
			let batch = {
				let txn = self.transaction(Read).await?;
				let res = txn.batch_keys_vals(range, NORMAL_BATCH_SIZE, None).await;
				let _ = txn.cancel().await;
				res?
			};
			// The archived nodes on the page
			let mut archived = Vec::new();
			for (_, v) in batch.result.iter() {
				let node: Node = KVValue::kv_decode_value(v, ())?;
				if let Some(id) = node.archived() {
					archived.push(id);
				}
			}
			// Loop over the archived nodes
			'nodes: for id in archived.iter() {
				let range = NodeLiveQueryPrefix {
					nd: *id,
				}
				.range()?;
				// Delete the node's live queries a page per commit, then its row.
				loop {
					if budget == 0 {
						// Spent with this node still to clean. Every page deleted
						// so far is committed, so the next pass finds what remains
						// at the head of the node's range, reading this page of
						// rows again.
						return Ok(self.node_cleanup_stopped(page_after, end, items));
					}
					// Open a writeable transaction
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
						let Some(node) = node.filter(|nd| nd.is_archived()) else {
							trace!(target: TARGET, id = %id, "Skipping node that is no longer archived");
							catch!(txn, txn.cancel().await);
							continue 'nodes;
						};
						// A build waits for an archived node's write barrier answer until its
						// heartbeat is `exclude_after` old, and finds the node only
						// through its row, so the row stays until then.
						let excluded_before = self.clock_now()
							- crate::kvs::write_barrier::exclude_after(txn.write_commit_bound());
						if node.heartbeat >= excluded_before {
							trace!(target: TARGET, id = %id, "Keeping archived node a build may still wait for");
							catch!(txn, txn.cancel().await);
							continue 'nodes;
						}
					}
					// Log the live query scanning
					trace!(target: TARGET, id = %id, "Deleting live queries for node");
					// One page of the node's live queries, bounded by what the budget
					// has left. What an earlier page deleted is committed and gone, so
					// every page is read from the head of the range.
					let limit = budget.min(u64::from(NORMAL_BATCH_SIZE)) as u32;
					let res = catch!(txn, txn.batch_keys_vals(range.clone(), limit, None).await);
					for (k, v) in res.result.iter() {
						// Decode the data for this live query
						let val: NodeLiveQuery = catch!(txn, KVValue::kv_decode_value(v, ()));
						// Get the key for this node live query
						let nlq = catch!(txn, NodeLiveQueryKey::decode_key(k));
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
					let deleted = res.result.len() as u64;
					budget -= deleted;
					items += deleted;
					if res.next.is_some() {
						// A full page carries a continuation: commit it, and read the
						// next from the head of what remains.
						catch!(txn, txn.commit().await);
						// Pause and yield execution
						yield_now!();
						continue;
					}
					// Log the node deletion
					trace!(target: TARGET, id = %id, "Deleting node from the cluster");
					// Get the key for the node entry
					let key = NodeKey {
						nd: *id,
					};
					// Delete the cluster node entry. The node's queued async events, if
					// any, are not gated on this: the adoption sweep finds a queue by its
					// own prefix, whether or not a row for its owner remains.
					catch!(txn, txn.clr_key(&key).await);
					// The drain heartbeat and the release record were written beside
					// the row and go with it.
					let stamp = NodeDrainHeartbeatKey {
						nd: *id,
					};
					catch!(txn, txn.clr_key(&stamp).await);
					let release = NodeReleaseKey {
						nd: *id,
					};
					catch!(txn, txn.clr_key(&release).await);
					// So were its write barrier answer and heartbeat marks.
					catch!(
						txn,
						txn.clr_key(&crate::key::schema::WriteBarrierAckKey {
							nd: *id,
						})
						.await
					);
					catch!(
						txn,
						txn.clr_key(&crate::key::schema::WriteBarrierHeartbeatKey {
							nd: *id,
						})
						.await
					);
					catch!(
						txn,
						txn.clr_key(&crate::key::schema::CatalogFenceHeartbeatKey {
							nd: *id,
						})
						.await
					);
					// Commit the changes
					catch!(txn, txn.commit().await);
					// The row is a key of the budget too, so a pass over archived nodes
					// that left no live queries behind is bounded as well.
					budget = budget.saturating_sub(1);
					items += 1;
					break;
				}
			}
			// A full page carries a continuation: the next read resumes after
			// the last key this page returned. The page that reaches the end
			// the scan began with ends it: what lies past that key was
			// registered since, and is the next scan's.
			let more = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) if *k < end => {
					after = Some(k.clone());
					true
				}
				_ => false,
			};
			if !more {
				break;
			}
		}
		// The rows are scanned: the release records whose node row is gone
		// are swept with what the pass has left.
		self.sweep_node_releases(pages, budget, items, None).await
	}

	/// The pass a node cleanup reports when it stops with rows still to scan,
	/// its scan kept at `after` for the next pass to resume; a scan stopped at
	/// its head is begun again, over the rows there are then.
	fn node_cleanup_stopped(&self, after: Option<Vec<u8>>, end: Vec<u8>, items: u64) -> Pass {
		if let Some(after) = after {
			*self.node_cleanup_cursor.lock() = Some(NodeCleanupScan::Rows(KeyScan {
				after,
				end,
			}));
		}
		Self::node_cleanup_more(items)
	}

	/// The pass a node cleanup reports when it stops with work still to do,
	/// having deleted `items` keys.
	fn node_cleanup_more(items: u64) -> Pass {
		Pass {
			progress: Progress::More,
			items,
			oldest_age: None,
			held: 0,
			ran: items,
			lease: TaskLeaseState::NotLeased,
		}
	}

	/// The pass a node cleanup reports when it has scanned every row and
	/// swept every record, having deleted `items` keys.
	fn node_cleanup_done(items: u64) -> Pass {
		Pass {
			progress: Progress::Done,
			items,
			oldest_age: None,
			held: 0,
			ran: items,
			lease: TaskLeaseState::NotLeased,
		}
	}

	/// The release-record sweep of a node cleanup pass, after its row scan:
	/// deletes the release records whose node row does not exist, a page of
	/// records per step, with the `pages` and `budget` the pass has left and
	/// `items` the keys it has deleted. `scan` is where the previous pass
	/// stopped in the sweep, or `None` to begin one over the records there
	/// are now.
	///
	/// A release that does not know the records removes a node's row without
	/// removing its record, so the record outlives the node. A page of records
	/// is read without a write transaction, and its orphans are deleted in a
	/// write transaction that checks each again, so a node that registers in
	/// between keeps its record. A sweep that stops with pages still to read,
	/// or orphans on a page beyond the keys left, keeps its position and the
	/// pass reports [`Progress::More`]; what it deleted is committed, so the
	/// page read again holds only what is left. A sweep ends at the last
	/// record there was when it began, so records written past that key while
	/// it runs are the next sweep's.
	async fn sweep_node_releases(
		&self,
		mut pages: usize,
		mut budget: u64,
		mut items: u64,
		scan: Option<(Option<Vec<u8>>, Vec<u8>)>,
	) -> Result<Pass> {
		let full = NodeReleasePrefix {}.range()?;
		let (mut after, end) = match scan {
			Some(scan) => scan,
			None => match self.range_end(full.clone()).await? {
				Some(end) => (None, end),
				// No record is left.
				None => return Ok(Self::node_cleanup_done(items)),
			},
		};
		loop {
			if pages == 0 || budget == 0 {
				// Spent with a page still to read: the next pass resumes here.
				*self.node_cleanup_cursor.lock() = Some(NodeCleanupScan::Releases {
					after,
					end,
				});
				return Ok(Self::node_cleanup_more(items));
			}
			pages -= 1;
			let range = match &after {
				Some(after) => full.clone().resume_after(after, Direction::Forward),
				None => full.clone(),
			};
			// One page of records, read in a short-lived read transaction, and
			// the nodes among them without a row.
			let (batch, candidates) = {
				let txn = self.transaction(Read).await?;
				let res = catch!(txn, txn.batch_keys_raw(range, NORMAL_BATCH_SIZE, None).await);
				// A key that does not spell a node id holds no record of a node, and is left alone
				let nodes: Vec<Uuid> = res
					.result
					.iter()
					.filter_map(|k| NodeReleaseKey::decode_key(k).ok())
					.map(|k| k.nd)
					.collect();
				let orphans = catch!(txn, Self::nodes_without_row(&txn, nodes).await);
				catch!(txn, txn.cancel().await);
				(res, orphans)
			};
			if !candidates.is_empty() {
				let txn = self.transaction(Write).await?;
				let orphans = catch!(txn, Self::nodes_without_row(&txn, candidates).await);
				// As many of the page's orphans as the budget has left.
				let within =
					usize::try_from(budget).map_or(orphans.len(), |b| b.min(orphans.len()));
				for nd in &orphans[..within] {
					trace!(target: TARGET, id = %nd, "Deleting release record of a node that is gone");
					catch!(
						txn,
						txn.clr_key(&NodeReleaseKey {
							nd: *nd,
						})
						.await
					);
				}
				catch!(txn, txn.commit().await);
				budget -= within as u64;
				items += within as u64;
				if within < orphans.len() {
					// Spent with orphans left on the page: the next pass reads
					// it again, and finds only what is left.
					*self.node_cleanup_cursor.lock() = Some(NodeCleanupScan::Releases {
						after,
						end,
					});
					return Ok(Self::node_cleanup_more(items));
				}
			}
			// A full page carries a continuation: the next read resumes after
			// the last key this page returned. The page that reaches the end
			// the sweep began with ends it: what lies past that key was
			// written since, and is the next sweep's.
			let more = match (&batch.next, batch.result.last()) {
				(Some(_), Some(k)) if *k < end => {
					after = Some(k.clone());
					true
				}
				_ => false,
			};
			if !more {
				return Ok(Self::node_cleanup_done(items));
			}
		}
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
	/// Nothing on the request path uses this: a session is created by
	/// [`create_rpc_session`](Self::create_rpc_session) (insert-if-absent) and
	/// changed only by the conditional writes
	/// [`replace_rpc_session`](Self::replace_rpc_session) and
	/// [`refresh_rpc_session`](Self::refresh_rpc_session), so no node resurrects
	/// or clobbers a session another node changed. This blind upsert is a
	/// primitive for callers that own the id exclusively, and for test setup.
	///
	/// Runs through the bookkeeping transaction, so a store open read-only still
	/// serves attached sessions (see [`Self::bookkeeping_transaction`]).
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
		let txn = self.bookkeeping_transaction().await?;
		run!(txn, txn.set_key(&key, &value).await)
	}

	/// Creates the durable copy of a client-attached RPC session, expiring `ttl`
	/// from now, but only if no live session with this id is stored. Returns
	/// the stored copy, or `None` if a live session with this id exists.
	///
	/// An expired entry the purge task has not yet removed does not hold the
	/// id: it is replaced. The write is conditional on the entry read — absent
	/// (`put_compare_key` with no expected value) or that expired value — which
	/// is atomic even on last-writer-wins backends (TiKV) where a blind `set`
	/// is not, so two concurrent creates for one id on different nodes cannot
	/// both succeed: one wins and the other reports the session as existing.
	///
	/// Runs through the bookkeeping transaction, so a store open read-only still
	/// serves attached sessions (see [`Self::bookkeeping_transaction`]).
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
	) -> Result<Option<StoredRpcSession>> {
		trace!(target: TARGET, id = %id, "Creating durable RPC session");
		let key = SessionKey {
			id,
		};
		let mut attempt = 1;
		loop {
			let now = self.clock_now().value;
			let txn = self.bookkeeping_transaction().await?;
			let current = catch!(txn, txn.get_key(&key, None).await);
			if current.as_ref().is_some_and(|current| current.expires_at > now) {
				catch!(txn, txn.cancel().await);
				return Ok(None);
			}
			let value = durable_session(session, now + ttl.as_millis() as u64);
			let write = txn.put_compare_key(&key, &value, current.as_ref()).await;
			let lost = match commit_conditional_write(&txn, write).await? {
				ConditionalWrite::Committed => return Ok(Some(StoredRpcSession::new(value))),
				// The entry changed after it was read, and neither that nor a
				// conflict decides whether the id is taken now, so both are
				// re-evaluated in a fresh transaction.
				ConditionalWrite::Superseded => {
					anyhow::Error::new(super::Error::TransactionConditionNotMet)
				}
				ConditionalWrite::Conflicted(e) => e,
			};
			if attempt == RPC_SESSION_WRITE_ATTEMPTS {
				return Err(lost);
			}
			attempt += 1;
		}
	}

	/// Loads the stored copy of a client-attached RPC session — the
	/// authoritative state every node serves the session from — returning the
	/// restored session with the stored copy it came from, or `None` if the
	/// session is gone or expired.
	///
	/// This is a pure read plus lazy expiry: an entry past its `expires_at` is
	/// deleted and reported as a miss, but a live entry is returned **without**
	/// being rewritten. It never extends a session's life, so it is safe to call
	/// before the caller is authorized. The TTL is slid only after
	/// authorization, by [`refresh_rpc_session`](Self::refresh_rpc_session) or
	/// [`replace_rpc_session`](Self::replace_rpc_session).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn load_rpc_session(&self, id: Uuid) -> Result<Option<(Session, StoredRpcSession)>> {
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
		// last-writer-wins on TiKV). When the delete loses to a concurrent
		// writer the entry is left in place, and this request still reports the
		// miss it read: the next one reads whatever that writer committed.
		if durable.expires_at <= now {
			trace!(target: TARGET, id = %id, "Durable RPC session has expired");
			let txn = self.bookkeeping_transaction().await?;
			let write = txn.del_compare_key(&key, Some(&durable)).await;
			if let ConditionalWrite::Conflicted(e) = commit_conditional_write(&txn, write).await? {
				trace!(target: TARGET, id = %id, "Expired RPC session changed concurrently: {e}");
			}
			return Ok(None);
		}
		let session = restore_session(durable.clone())?;
		Ok(Some((session, StoredRpcSession::new(durable))))
	}

	/// Replaces the stored copy of a client-attached RPC session with
	/// `session`, expiring `ttl` from now — but only while the stored copy is
	/// still `expected`. Returns the new stored copy, or `None` if the stored
	/// copy is no longer `expected`: the session was changed, removed, or
	/// expired since `expected` was read.
	///
	/// The write is conditional (`put_compare_key` guarded by `expected`), so a
	/// change committed on another node after `expected` was read is never
	/// overwritten, and a session removed elsewhere is never recreated. On
	/// `None` the caller reloads the session to learn which of those happened.
	///
	/// Runs through the bookkeeping transaction, so a store open read-only still
	/// serves attached sessions (see [`Self::bookkeeping_transaction`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all, fields(id = %id))
	)]
	pub async fn replace_rpc_session(
		&self,
		id: Uuid,
		expected: &StoredRpcSession,
		session: &Session,
		ttl: Duration,
	) -> Result<Option<StoredRpcSession>> {
		trace!(target: TARGET, id = %id, "Replacing durable RPC session");
		let value = durable_session(session, self.clock_now().value + ttl.as_millis() as u64);
		self.write_rpc_session(id, expected, value).await
	}

	/// Extends the stored copy of a client-attached RPC session to expire `ttl`
	/// from now, leaving its contents as they are — but only while the stored
	/// copy is still `expected`. Returns the new stored copy, or `None` if the
	/// stored copy is no longer `expected`.
	///
	/// This is how an actively used session's idle TTL slides. It writes the
	/// stored contents back rather than a node's copy of them, so extending a
	/// session never carries a node's view of it over a change made elsewhere.
	///
	/// Runs through the bookkeeping transaction, so a store open read-only still
	/// serves attached sessions (see [`Self::bookkeeping_transaction`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all, fields(id = %id))
	)]
	pub async fn refresh_rpc_session(
		&self,
		id: Uuid,
		expected: &StoredRpcSession,
		ttl: Duration,
	) -> Result<Option<StoredRpcSession>> {
		trace!(target: TARGET, id = %id, "Refreshing durable RPC session");
		let value = DurableSession {
			expires_at: self.clock_now().value + ttl.as_millis() as u64,
			..expected.durable().clone()
		};
		self.write_rpc_session(id, expected, value).await
	}

	/// Writes `value` over the stored copy of session `id` while that copy is
	/// still `expected`, the guard shared by
	/// [`replace_rpc_session`](Self::replace_rpc_session) and
	/// [`refresh_rpc_session`](Self::refresh_rpc_session).
	///
	/// A transaction conflict is reported like an unmet guard: both mean
	/// another writer touched the entry, and the caller's reload settles what
	/// it now holds.
	async fn write_rpc_session(
		&self,
		id: Uuid,
		expected: &StoredRpcSession,
		value: DurableSession,
	) -> Result<Option<StoredRpcSession>> {
		let key = SessionKey {
			id,
		};
		let txn = self.bookkeeping_transaction().await?;
		let write = txn.put_compare_key(&key, &value, Some(expected.durable())).await;
		Ok(match commit_conditional_write(&txn, write).await? {
			ConditionalWrite::Committed => Some(StoredRpcSession::new(value)),
			ConditionalWrite::Superseded | ConditionalWrite::Conflicted(_) => None,
		})
	}

	/// Deletes the durable copy of a client-attached RPC session.
	///
	/// Run on detach/teardown so the session cannot be rehydrated. Each attempt
	/// reads the current value and deletes only that (`del_compare_key`) rather
	/// than clearing the key blindly, because a blind `clr` is last-writer-wins
	/// on TiKV. When another node changes the session between the read and the
	/// delete, the attempt is repeated against the new value: the teardown is
	/// the client's explicit intent and outranks a concurrent change. An entry
	/// already absent is a no-op.
	///
	/// When every attempt loses to a concurrent writer the error is returned,
	/// not swallowed: the durable copy was *not* removed, so reporting success
	/// would tell a detaching client its session is gone while it remains
	/// loadable. Callers fail the teardown on that error, keeping detach
	/// fail-closed.
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
		let mut attempt = 1;
		loop {
			let txn = self.bookkeeping_transaction().await?;
			// Read the value we intend to remove; if it is already gone, done.
			let Some(current) = catch!(txn, txn.get_key(&key, None).await) else {
				let _ = txn.cancel().await;
				return Ok(());
			};
			let write = txn.del_compare_key(&key, Some(&current)).await;
			let lost = match commit_conditional_write(&txn, write).await? {
				ConditionalWrite::Committed => return Ok(()),
				ConditionalWrite::Superseded => {
					anyhow::Error::new(super::Error::TransactionConditionNotMet)
				}
				ConditionalWrite::Conflicted(e) => e,
			};
			if attempt == RPC_SESSION_WRITE_ATTEMPTS {
				return Err(lost);
			}
			attempt += 1;
		}
	}

	/// Reports, for each of `ids`, whether a live (stored and unexpired)
	/// client-attached RPC session exists under it. A pure read: expired
	/// entries are reported as gone but left for lazy expiry and the purge
	/// task to remove.
	///
	/// An entry that cannot be decoded is reported as live, and does not fail
	/// the others: it may have been written by a newer node during a rolling
	/// upgrade, and this node cannot tell when it expires.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip_all)
	)]
	pub async fn live_rpc_sessions(&self, ids: &[Uuid]) -> Result<Vec<bool>> {
		let now = self.clock_now().value;
		let keys: Vec<SessionKey> = ids
			.iter()
			.map(|&id| SessionKey {
				id,
			})
			.collect();
		let txn = self.transaction(Read).await?;
		let stored = catch!(txn, txn.getm_raw(keys, None).await);
		catch!(txn, txn.cancel().await);
		Ok(stored
			.into_iter()
			.map(|bytes| {
				bytes.is_some_and(|bytes| {
					match <DurableSession as KVValue>::kv_decode_value(&bytes, ()) {
						Ok(stored) => stored.expires_at > now,
						Err(e) => {
							warn!(target: TARGET, "Undecodable durable RPC session entry: {e}");
							true
						}
					}
				})
			})
			.collect())
	}

	/// Purges expired durable RPC sessions.
	///
	/// This function should be run periodically at a regular interval.
	///
	/// It uses a distributed task lease so only one node in the cluster
	/// performs the purge per interval. A store open read-only refuses the
	/// lease row, so there every node purges; the conditional deletes make
	/// concurrent purges safe. The deletes run through the bookkeeping
	/// transaction, so a read-only store, which still accepts attached
	/// sessions, still purges them. Entries are scanned in batches, and only
	/// entries whose expiry has passed are deleted; an entry that fails to
	/// decode (for example one written by a newer node during a rolling
	/// upgrade) is skipped, never deleted.
	///
	/// Bounded: a pass scans at most [`RPC_SESSION_GC_SLICE_BATCHES`] pages,
	/// and one that stops with pages still to scan keeps its position and
	/// reports [`Progress::More`], so the next pass — run at once by the
	/// datastore's own task, or by a host calling for another slice —
	/// continues after the page it stopped at rather than from the head, and
	/// the scan ends however many pages the sessions span. A scan ends at
	/// the last session there was when it began: sessions created past that
	/// key while it runs are the next scan's, so a scan ends however fast
	/// sessions are created, and the sessions behind its position — scanned
	/// while live, expired since — are reached by the scan after it rather
	/// than left behind a suffix that keeps growing.
	///
	/// Reports the number of sessions purged as the pass's items. The lease
	/// is checked before each entry: a pass that finds it passed to another
	/// node stops there; see [`Self::purge_rpc_session_slice_with`].
	///
	/// # Arguments
	/// * `interval` - The interval between purge runs, to calculate the lease duration
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(err, level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub(crate) async fn purge_expired_rpc_sessions(&self, interval: &Duration) -> Result<Pass> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting expired RPC session purge");
		let lh = if self.is_read_only() {
			None
		} else {
			// Create a new lease handler
			let lh = self.lease_handler(TaskLeaseType::RpcSessionCleanup, *interval * 2, None)?;
			// If we don't get the lease, another node is handling this task
			if !lh.has_lease().await? {
				return Ok(Pass::elsewhere());
			}
			Some(lh)
		};
		// Output function invocation details to logs
		trace!(target: TARGET, "Purging expired RPC sessions");
		self.purge_rpc_session_slice_with(lh.as_ref()).await
	}

	/// One pass of [`Self::purge_expired_rpc_sessions`], under the lease `lh`.
	///
	/// The lease is checked before each entry. A check that finds it passed
	/// to another node stops the pass there without failing it: the new
	/// holder purges from its own scan, this node's scan keeps its place
	/// after the last entry handled for its next pass should it hold the
	/// lease again, and the pass reports [`TaskLeaseState::Elsewhere`] with
	/// the sessions purged until then, due at the interval rather than at
	/// once. The verdict is read rather than discarded because the check is
	/// throttled to one datastore read per maintenance period: a check that
	/// dropped it would leave the next answering from the throttle.
	#[cfg(test)]
	pub(crate) async fn purge_rpc_session_slice(&self, lh: &LeaseHandler) -> Result<Pass> {
		self.purge_rpc_session_slice_with(Some(lh)).await
	}

	async fn purge_rpc_session_slice_with(&self, lh: Option<&LeaseHandler>) -> Result<Pass> {
		let now = self.clock_now().value;
		let mut purged: u64 = 0;
		let held = if lh.is_some() {
			TaskLeaseState::Held
		} else {
			TaskLeaseState::NotLeased
		};
		let report = |progress, purged, lease| Pass {
			progress,
			items: purged,
			oldest_age: None,
			held: 0,
			ran: purged,
			lease,
		};
		let mut budget = RPC_SESSION_GC_SLICE_BATCHES;
		let full = SessionPrefix {}.range()?;
		// Continue the scan the previous pass stopped in, or begin one over
		// the sessions there are now.
		let resumed = self.rpc_session_cursor.lock().take();
		let (mut after, end) = match resumed {
			Some(scan) => (Some(scan.after), scan.end),
			None => match self.range_end(full.clone()).await? {
				Some(end) => (None, end),
				// Nothing stored.
				None => return Ok(report(Progress::Done, 0, held)),
			},
		};
		// Scan the durable session entries in batches under read transactions,
		// deleting each expired entry in its own conditional-delete write
		// transaction. The delete is guarded by the value read
		// (`del_compare_key`), so a request that refreshed the session after
		// the scan read the old expired value is not clobbered — a blind `clr`
		// is last-writer-wins on TiKV. A per-entry transaction keeps one such
		// conflict from aborting the whole purge.
		loop {
			if budget == 0 {
				// Spent with a page still to scan: the next pass resumes here.
				if let Some(after) = after {
					*self.rpc_session_cursor.lock() = Some(KeyScan {
						after,
						end,
					});
				}
				return Ok(report(Progress::More, purged, held));
			}
			let rng = match &after {
				Some(k) => full.clone().resume_after(k, Direction::Forward),
				None => full.clone(),
			};
			// Fetch the next batch of keys and values under a read transaction.
			let batch = {
				let txn = self.transaction(Read).await?;
				let res = catch!(txn, txn.batch_keys_vals(rng, NORMAL_BATCH_SIZE, None).await);
				catch!(txn, txn.cancel().await);
				res
			};
			budget -= 1;
			// The key the page resumed after, which a lease lost on its first
			// entry leaves the scan after.
			let start = after.clone();
			// A full page carries a continuation: the next read resumes after
			// the last key this page returned. The page that reaches the end
			// the scan began with ends it: what lies past that key was
			// created since, and is the next scan's.
			let more = match (&batch.next, batch.result.last()) {
				(Some(_), Some((k, _))) if *k < end => {
					after = Some(k.clone());
					true
				}
				_ => false,
			};
			for (i, (k, v)) in batch.result.iter().enumerate() {
				if let Some(lh) = lh
					&& !lh.try_maintain_lease().await?
				{
					// Another node holds the lease: the scan keeps its place
					// after the last entry handled.
					let handled = match i {
						0 => start,
						i => Some(batch.result[i - 1].0.clone()),
					};
					if let Some(after) = handled {
						*self.rpc_session_cursor.lock() = Some(KeyScan {
							after,
							end,
						});
					}
					return Ok(report(Progress::Done, purged, TaskLeaseState::Elsewhere));
				}
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
					let txn = self.bookkeeping_transaction().await?;
					match txn.del_compare_key(&key, Some(&val)).await {
						Ok(()) => match txn.commit().await {
							Ok(()) => purged += 1,
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
			if !more {
				break;
			}
			// Pause and yield execution
			yield_now!();
		}
		Ok(report(Progress::Done, purged, held))
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
	/// 2. Cleaning up old changefeed data from the databases one page of steps through the catalog
	///    reaches
	///
	/// A sweep visits every database of the catalog at most
	/// [`CHANGEFEED_GC_SLICE_STEPS`] steps per pass — a namespace entered, a
	/// database reached or a page of a database's tables read — keeping its
	/// place between passes, so a pass reads the pages of the catalog its
	/// steps move through rather than the whole catalog. The next sweep begins
	/// once it has visited them all.
	///
	/// Reports the entries deleted as the pass's items, and [`Progress::More`]
	/// when the sweep has databases still to visit or a key budget ran out
	/// before the backlog did.
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
	) -> Result<Pass> {
		self.changefeed_process_bounded(
			interval,
			CHANGEFEED_GC_PASS_KEY_BUDGET,
			CHANGEFEED_GC_SLICE_STEPS,
			canceller,
		)
		.await
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
	) -> Result<Pass> {
		self.changefeed_process_bounded(interval, budget, CHANGEFEED_GC_SLICE_STEPS, canceller)
			.await
	}

	/// [`Self::changefeed_process`] with the page of steps a pass takes
	/// through the catalog lowered, so a test can drive a sweep that spans
	/// several passes.
	#[cfg(test)]
	#[cfg_attr(not(feature = "kv-mem"), allow(dead_code))]
	pub(crate) async fn changefeed_gc_with_page(
		&self,
		interval: &Duration,
		steps: usize,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		self.changefeed_process_bounded(interval, CHANGEFEED_GC_PASS_KEY_BUDGET, steps, canceller)
			.await
	}

	/// One changefeed-collection pass, taking at most `steps` steps through
	/// the catalog — a namespace entered, a database reached or a page of a
	/// database's tables read — and deleting at most `budget` entries.
	///
	/// The changefeed backlog is sized by write throughput rather than by the
	/// catalog, so it is not deleted in one transaction: it is paged behind
	/// committed transactions of at most one page (see [`crate::kvs::paging`]).
	/// A pass therefore costs a bounded amount of work however far the backlog
	/// has run ahead, and the next tick resumes at the head of what survives.
	async fn changefeed_process_bounded(
		&self,
		interval: &Duration,
		budget: u64,
		steps: usize,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		// An engine that writes no change feed has nothing to collect.
		if !self.config.datastore.live_query_engine.writes_changefeed() {
			return Ok(Pass::done(TaskLeaseState::NotLeased));
		}
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting changefeed garbage collection");
		// Create a new lease handler
		let lh = self.lease_handler(TaskLeaseType::ChangeFeedCleanup, *interval * 2, None)?;
		// If we don't get the lease, another node is handling this task. The
		// sweep this node was making is dropped with it: the one it begins
		// when the lease returns lists the catalog as it then stands.
		if !lh.has_lease().await? {
			*self.changefeed_sweep.lock() = ChangefeedSweep::default();
			return Ok(Pass::elsewhere());
		}
		// Output function invocation details to logs
		trace!(target: TARGET, "Running changefeed garbage collection");
		let mut pass = Pass::done(TaskLeaseState::Held);
		// The databases this pass collects: those the next page of steps of
		// the sweep the previous pass left, or of one begun now, discovers.
		// The sweep is taken from the datastore for the pass, and put back
		// only if it has catalog left, so a pass that ends early drops it.
		let mut sweep = std::mem::take(&mut *self.changefeed_sweep.lock());
		let page = self.changefeed_page(&mut sweep, steps, SweepReads::Retention).await?;
		// The databases the sweep reported by id have no changefeed retention
		// this build can read, so their entries are retained. Only the
		// live-event collector, whose window is the datastore's, collects them.
		sweep.take_unknown();
		let skipped = sweep.take_skipped();
		if skipped > 0 {
			warn!(
				target: TARGET,
				skipped,
				"Changefeed garbage collection skipped catalog entries that need a newer release \
				 than this build; their changefeed entries are retained"
			);
		}
		// Entries this pass may delete, shared out across the databases it
		// visits.
		let mut left = budget;
		let kept_lease = crate::cf::gc_databases(self, &lh, canceller, &mut left, &page).await?;
		pass.items += budget - left;
		if !kept_lease {
			// Another node holds the lease now: the rest of the pass is its,
			// and this one is reported under its standing.
			pass.lease = TaskLeaseState::Elsewhere;
			return Ok(pass);
		}
		if left == 0 {
			pass.progress = Progress::More;
		}
		// A sweep with databases still to visit keeps its place for the next
		// pass, which is due at once; one that has visited them all is dropped,
		// and the next pass begins a sweep over the catalog as it then stands.
		if sweep.has_more() {
			*self.changefeed_sweep.lock() = sweep;
			pass.progress = Progress::More;
		}
		Ok(pass)
	}

	/// Garbage-collects the dedicated live-query event keyspace as a
	/// background task, deleting the events older than the datastore's
	/// `live_query_retention` so reconnecting or lagging subscribers can
	/// still replay the window and no more.
	///
	/// Scheduled on its own interval, independently of
	/// [`Self::changefeed_process`], so the window holds whether or not
	/// changefeed collection runs. It runs whichever engine delivers live
	/// queries: the events the Router engine captured before the datastore was
	/// restarted under another engine age out all the same, and a database
	/// that never captured one scans an empty range. A zero retention collects
	/// nothing, and the pass takes no lease.
	///
	/// Leased under [`TaskLeaseType::LiveQueryCleanup`], a lease of its own:
	/// this job and changefeed collection each run on a cadence a node may
	/// disable, so a lease the two shared would let a node running one of
	/// them keep every other node from running the other. A node on a release
	/// that collects live events within its changefeed pass, under the
	/// changefeed lease, may collect them alongside this job while the cluster
	/// is upgraded. Each deletes only the events past its own retention, so
	/// the overlap costs pages that conflict, which the next pass retries.
	///
	/// Its own sweep visits every database of the catalog at most
	/// [`CHANGEFEED_GC_SLICE_STEPS`] steps per pass, keeping its place between
	/// passes and reading no table definition, and collects as well the
	/// database subtrees its probe of the keyspace reached where a namespace
	/// or database definition it cannot read hid their ids. The window is the
	/// datastore's, so no catalog definition bears on their events.
	///
	/// Reports the entries deleted as the pass's items, and [`Progress::More`]
	/// when the sweep has databases still to visit or the key budget ran out
	/// before the backlog did.
	///
	/// # Arguments
	/// * `interval` - The interval between passes, to calculate the lease duration
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn live_query_gc_process(
		&self,
		interval: &Duration,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		self.live_query_gc_process_bounded(
			interval,
			CHANGEFEED_GC_PASS_KEY_BUDGET,
			CHANGEFEED_GC_SLICE_STEPS,
			canceller,
		)
		.await
	}

	/// One live-event collection pass, taking at most `steps` steps through
	/// the catalog and deleting at most `budget` events.
	///
	/// The backlog is sized by write throughput rather than by the catalog, so
	/// it is paged behind committed transactions of at most one page (see
	/// [`crate::kvs::paging`]), and the next tick resumes at the head of what
	/// survives.
	async fn live_query_gc_process_bounded(
		&self,
		interval: &Duration,
		budget: u64,
		steps: usize,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		trace!(target: TARGET, "Attempting live-query event garbage collection");
		let retention = self.config.datastore.live_query_retention;
		if retention.is_zero() {
			return Ok(Pass::done(TaskLeaseState::NotLeased));
		}
		let lh = self.lease_handler(TaskLeaseType::LiveQueryCleanup, *interval * 2, None)?;
		// Without the lease, the sweep this node was making is dropped, as the
		// changefeed collector's is.
		if !lh.has_lease().await? {
			*self.live_event_sweep.lock() = ChangefeedSweep::default();
			return Ok(Pass::elsewhere());
		}
		trace!(target: TARGET, "Running live-query event garbage collection");
		let mut pass = Pass::done(TaskLeaseState::Held);
		let mut sweep = std::mem::take(&mut *self.live_event_sweep.lock());
		let page = self.changefeed_page(&mut sweep, steps, SweepReads::Database).await?;
		let ids: Vec<(NamespaceId, DatabaseId)> =
			page.iter().map(|db| (db.ns, db.db)).chain(sweep.take_unknown()).collect();
		let mut left = budget;
		let kept_lease =
			crate::lq::gc::gc_databases(self, &lh, retention, canceller, &mut left, &ids).await?;
		pass.items += budget - left;
		if !kept_lease {
			pass.lease = TaskLeaseState::Elsewhere;
			return Ok(pass);
		}
		if left == 0 {
			pass.progress = Progress::More;
		}
		if sweep.has_more() {
			*self.live_event_sweep.lock() = sweep;
			pass.progress = Progress::More;
		}
		Ok(pass)
	}

	/// One page of a changefeed-collection sweep: the databases the next
	/// `steps` steps of `sweep` through the catalog discover, in catalog
	/// order, each with its retention.
	///
	/// Read in one short transaction, and only the pages of the catalog the
	/// steps move through; see [`ChangefeedSweep`]. An empty page with
	/// nothing more to sweep means the sweep has visited every database.
	async fn changefeed_page(
		&self,
		sweep: &mut ChangefeedSweep,
		steps: usize,
		reads: SweepReads,
	) -> Result<Vec<Discovered>> {
		let txn = self.transaction(Read).await?;
		let res = sweep.page(&txn, steps, reads).await;
		let _ = txn.cancel().await;
		res
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
	///
	/// Bounded: a pass delivers at most a slice of the captured entries, and
	/// one that stops with entries still to deliver keeps its lap and reports
	/// [`Progress::More`], so the next pass continues it.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::lq", skip(self))
	)]
	pub(crate) async fn live_query_router_process(&self) -> Result<Pass> {
		// Only the Router engine delivers via the router.
		if self.config.datastore.live_query_engine != LiveQueryEngine::Router {
			return Ok(Pass::done(TaskLeaseState::NotLeased));
		}
		crate::lq::router::process(self, &self.live_query_router).await
	}

	/// [`Self::live_query_router_process`] with the per-pass event budget
	/// lowered, so a test can drive a pass that stops mid-lap and inspect what
	/// it left behind.
	#[cfg(test)]
	pub(crate) async fn live_query_router_process_with_budget(&self, budget: u64) -> Result<Pass> {
		if self.config.datastore.live_query_engine != LiveQueryEngine::Router {
			return Ok(Pass::done(TaskLeaseState::NotLeased));
		}
		crate::lq::router::process_bounded(self, &self.live_query_router, budget).await
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

	/// A handler for the cluster-wide lease on `task`, held by this node for
	/// `duration` after each acquisition and stopping its retries when
	/// `canceller` trips.
	pub(crate) fn lease_handler(
		&self,
		task: TaskLeaseType,
		duration: Duration,
		canceller: Option<&CancellationToken>,
	) -> Result<LeaseHandler> {
		match canceller {
			Some(canceller) => LeaseHandler::new_with_canceller(
				self.sequences.clone(),
				self.id,
				self.transaction_factory.clone(),
				task,
				duration,
				canceller.clone(),
			),
			None => LeaseHandler::new(
				self.sequences.clone(),
				self.id,
				self.transaction_factory.clone(),
				task,
				duration,
			),
		}
	}

	/// Age of the oldest of the given queue entries, each stamped with a
	/// time-ordered UUID when it was enqueued. `None` when there are none, or
	/// none carries a timestamp.
	fn oldest_queued_age(uids: impl Iterator<Item = Uuid>) -> Option<Duration> {
		let now =
			web_time::SystemTime::now().duration_since(web_time::SystemTime::UNIX_EPOCH).ok()?;
		uids.filter_map(|uid| {
			let (secs, nanos) = uid.get_timestamp()?.to_unix();
			Some(now.saturating_sub(Duration::new(secs, nanos)))
		})
		.max()
	}

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
	/// depend on the lease. The scan is paged: a pass takes at most
	/// [`INDEX_RESUME_PAGE_SIZE`] steps through the catalog in catalog order,
	/// checking at most an index per step, continuing the walk where the
	/// previous pass stopped and beginning afresh once one reaches the end,
	/// and reads only the pages of the catalog its steps move through, so one
	/// pass costs a bounded amount of work however many indexes, tables,
	/// databases and namespaces the datastore holds. A candidate is checked
	/// against the catalog as it stands immediately before its takeover: the
	/// page is read in a transaction of its own, and a candidate given back
	/// to the walk is reached again a pass or more later, so the index a
	/// candidate names may since have been removed, defined again under the
	/// same names with new ids, or marked for removal; such a candidate is
	/// dropped, and a takeover is given the definition the catalog holds now.
	/// Operators who prefer to recover stalled builds manually (with
	/// `REBUILD INDEX`) can disable the scan by setting its interval to zero.
	///
	/// Recovery is serialized: a pass adopts at most one stalled build, and
	/// while a local builder task is still running a pass adopts only a
	/// stalled `Closing` build, which has no scan left to run.
	/// A restart with several stranded builds (for example multiple full-text
	/// indexes on a small instance) therefore rebuilds them one at a time
	/// instead of multiplying the node's memory and CPU footprint; every
	/// stalled build is still recovered, one scan pass after the previous one
	/// finishes. User-initiated `REBUILD INDEX` is not throttled by this.
	///
	/// Reports the stalled builds adopted as the pass's items, and
	/// [`Progress::More`] when the walk has catalog left beyond the steps this
	/// pass took. A pass that finds the lease passed to another node stops at
	/// the candidate it found so, gives that candidate back to the walk with
	/// the rest unchecked, and reports [`TaskLeaseState::Elsewhere`]: the new
	/// holder adopts from here, and this node's walk is kept for its next
	/// pass, due at the interval.
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
	) -> Result<Pass> {
		Self::ensure_not_cancelled(&canceller)?;
		// Single-node-per-cluster guard. The lease lasts two intervals so an
		// in-flight scan isn't preempted between ticks.
		let lh =
			self.lease_handler(TaskLeaseType::IndexBuildResume, interval * 2, Some(&canceller))?;
		if !lh.has_lease().await? {
			return Ok(Pass::elsewhere());
		}
		let mut pass = Pass::done(TaskLeaseState::Held);
		// Serialize recovery: while any local builder task is still running
		// (an already adopted build or a user-started one), defer adopting
		// builds that still scan to a later pass. Without this, a restart with
		// several stranded builds starts them all at once, and the concurrent
		// initial scans can overwhelm a small instance's memory and CPU. A
		// stranded `Closing` build is adopted regardless: it only drains its
		// queues, and until it does every write that changes its index's
		// fields waits in admission.
		let scope = if self.index_builder.has_unfinished_build().await {
			trace!(
				target: TARGET,
				"Adopting only stalled Closing index builds; a local index build is still running"
			);
			ResumeScope::ClosingOnly
		} else {
			ResumeScope::Any
		};
		// The page this pass checks: the indexes the next page of steps of
		// the walk reaches. The walk is taken from the datastore for the pass
		// and put back only with catalog left, so a pass that reaches the end
		// drops it and the next begins a walk over the catalog as it then
		// stands.
		let mut walk = self.resume_walk.lock().take().unwrap_or_default();
		let page = self.resume_page(&mut walk).await?;
		// An index whose definition, or whose table's, database's or
		// namespace's, a newer release wrote is one this build cannot read,
		// and so does not adopt: the walk passes it, and the pass reports the
		// count once.
		let skipped = walk.take_refused();
		if skipped > 0 {
			warn!(
				target: TARGET,
				skipped,
				"Index build adoption skipped catalog entries that need a newer release than \
				 this build"
			);
		}
		self.adopt_stalled_build(&lh, &canceller, page, scope, &mut walk, &mut pass).await?;
		// A walk with catalog left keeps its place for the next pass, due at
		// once while this node holds the lease and at the interval once
		// another does; one that has reached the end is dropped.
		if walk.has_more() {
			*self.resume_walk.lock() = Some(walk);
			if pass.lease == TaskLeaseState::Held {
				pass.progress = Progress::More;
			}
		}
		Ok(pass)
	}

	/// Attempts a takeover, limited to the builds `scope` admits, for each
	/// candidate of `page`, under the lease `lh`, until one is adopted, which
	/// `pass` counts, and gives the candidates it did not check back to
	/// `walk`, which reaches them again first. A lease found passed to another
	/// node stops the attempts at the candidate it was found on, which goes
	/// back unchecked, and is reported in `pass` as
	/// [`TaskLeaseState::Elsewhere`].
	async fn adopt_stalled_build(
		&self,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		page: Vec<WalkedIndex>,
		scope: ResumeScope,
		walk: &mut IndexWalk,
		pass: &mut Pass,
	) -> Result<()> {
		// Attempt a takeover for each candidate on the page. `resume_stalled`
		// is a cheap no-op for healthy/online/live builds, so this is safe to
		// call for every index. The page may be stale by the time a candidate
		// is taken up; `resume_stalled` adopts a build only while the catalog
		// still holds the candidate's table and definition.
		let index_builder = &self.index_builder;
		let mut candidates = page.into_iter();
		// The candidate the lease was found lost on, unchecked.
		let mut lost_on = None;
		for candidate in candidates.by_ref() {
			Self::ensure_not_cancelled(canceller)?;
			#[cfg(test)]
			if let Some(hook) = test::take_before_resume_candidate(self.id) {
				hook().await;
			}
			// The new holder adopts from here. The verdict has to be read
			// rather than discarded: the check is throttled to one datastore
			// read per maintenance period, so the next one answers from the
			// throttle.
			if !lh.try_maintain_lease().await? {
				trace!(target: TARGET, "Index build resume lease lost, stopping the scan");
				lost_on = Some(candidate);
				pass.lease = TaskLeaseState::Elsewhere;
				break;
			}
			// The index as the catalog holds it now, which the takeover keeps; a
			// candidate that no longer names the index the walk reached is
			// dropped. See [`Self::resume_candidate_as_defined`].
			let Some(ix) = self.resume_candidate_as_defined(&candidate).await? else {
				trace!(
					target: TARGET,
					"Dropping stalled index build candidate '{}' on table '{}': not the index the walk reached",
					candidate.index.name,
					candidate.index.table_name
				);
				continue;
			};
			let ctx = self.setup_ctx()?.freeze();
			let opt = self.setup_options(
				&Session::owner().with_ns(&candidate.ns_name).with_db(&candidate.db_name),
			);
			match index_builder
				.resume_stalled(
					&ctx,
					opt,
					candidate.ns,
					candidate.db,
					candidate.tb,
					Arc::clone(&ix),
					scope,
				)
				.await
			{
				Ok(true) => {
					pass.items += 1;
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
		// The candidates an adoption or the lease left unchecked are the
		// walk's to reach again first, so the next pass checks them before it
		// walks on.
		walk.give_back(lost_on.into_iter().chain(candidates));
		Ok(())
	}

	/// One page of the stalled-build scan: the indexes the next
	/// [`INDEX_RESUME_PAGE_SIZE`] steps of `walk` reach, in catalog order.
	///
	/// Read in one short transaction, and only the pages of the catalog the
	/// steps move through; see [`IndexWalk`]. The per-index build-state
	/// checks run after it, with no catalog transaction open, and the
	/// definitions on the page are the only ones kept.
	async fn resume_page(&self, walk: &mut IndexWalk) -> Result<Vec<WalkedIndex>> {
		let txn = self.transaction(Read).await?;
		let res: Result<Vec<WalkedIndex>> = async {
			let mut page = Vec::new();
			for _ in 0..INDEX_RESUME_PAGE_SIZE {
				match walk.next(&txn).await? {
					Some(Step::Reached(index)) => page.push(index),
					Some(Step::Passed) => {}
					None => break,
				}
			}
			Ok(page)
		}
		.await;
		let _ = txn.cancel().await;
		res
	}

	/// The index `candidate` names, as the catalog holds it when the takeover
	/// is attempted; `None` when it is no longer the index the walk reached.
	///
	/// A candidate carries the names and ids the walk read, in the page's
	/// transaction or, for one given back to the walk, a pass or more
	/// earlier. A namespace, database, table or index removed since and
	/// defined again under the same names has new ids, so the candidate's
	/// ids are checked against the ones its names resolve to now, and an
	/// index marked for removal since is dropped as the walk passes one it
	/// reaches so marked. The definition returned is the current one, which
	/// the takeover builds to.
	async fn resume_candidate_as_defined(
		&self,
		candidate: &WalkedIndex,
	) -> Result<Option<Arc<IndexDefinition>>> {
		let txn = self.transaction(Read).await?;
		let res: Result<Option<Arc<IndexDefinition>>> = async {
			let (ns_name, db_name) = (&*candidate.ns_name, &*candidate.db_name);
			let table = &candidate.index.table_name;
			let Some(ns) = txn.get_ns_by_name(ns_name, None).await? else {
				return Ok(None);
			};
			let Some(db) = txn.get_db_by_name(ns_name, db_name, None).await? else {
				return Ok(None);
			};
			let Some(tb) = txn.get_tb_by_name(ns_name, db_name, table, None).await? else {
				return Ok(None);
			};
			let Some(ix) = txn
				.get_tb_index(ns.namespace_id, db.database_id, table, &candidate.index.name, None)
				.await?
			else {
				return Ok(None);
			};
			let same = ns.namespace_id == candidate.ns
				&& db.database_id == candidate.db
				&& tb.table_id == candidate.tb
				&& ix.index_id == candidate.index.index_id;
			Ok((same && !ix.prepare_remove).then_some(ix))
		}
		.await;
		let _ = txn.cancel().await;
		match res {
			// A definition a newer release wrote since the walk read the
			// candidate is not this build's to adopt under, as the walk skips
			// one it finds so written.
			Err(e) if surrealdb_datastore::tx::refused_by_release(&e) => Ok(None),
			res => res,
		}
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
	/// most once per lease period and a page of the catalog at a time (see
	/// [`Self::sweep_page`]), and drains what the sweep queued.
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
		let (iterations, errors, _) =
			Self::index_compaction_bounded(dbs, interval, canceller, None).await?;
		Ok((iterations, errors))
	}

	/// One slice of [`Self::index_compaction`]: at most
	/// [`INDEX_COMPACTION_SLICE_BATCHES`] queue batches and pages of the drain's
	/// sweep together, each of whose passes applies at most
	/// [`INDEX_COMPACTION_PASS_PLAN_BUDGET`] plans, reporting
	/// [`Progress::More`] when the drain has batches left to read or catalog
	/// left to sweep, or when a
	/// batch's cleanup gave up on conflicts that outlasted its retries and left
	/// the batch queued for the drain to reach again. The keys of the batches
	/// the slice acted on are the pass's items, and the oldest entry among them
	/// its queue age.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(dbs, canceller)
		)
	)]
	pub(crate) async fn index_compaction_slice(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<Pass> {
		let (_, _, pass) = Self::index_compaction_bounded(
			dbs,
			interval,
			canceller,
			Some(INDEX_COMPACTION_SLICE_BATCHES),
		)
		.await?;
		Ok(pass)
	}

	/// The drain behind [`Self::index_compaction`] and
	/// [`Self::index_compaction_slice`], stopping after `max_batches` queue
	/// batches when given one, whether or not they held a request to act on.
	/// The call continues the drain [`CompactionCursor`] holds, and leaves it
	/// there unless the call ends it. Returns the batches acted on, the indexes
	/// that failed to compact, and the pass report.
	async fn index_compaction_bounded(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
		max_batches: Option<usize>,
	) -> Result<(usize, usize, Pass)> {
		let lh =
			dbs.lease_handler(TaskLeaseType::IndexCompaction, interval * 2, Some(&canceller))?;
		let drained =
			Self::index_compaction_drain(dbs, Some(&lh), interval * 2, canceller, max_batches)
				.await?;
		Ok((drained.batches, drained.errors, drained.pass))
	}

	/// The drain of the compaction queue, under the lease `lh` when given one
	/// and otherwise outside any lease, sweeping the defined indexes at most
	/// once per `sweep_period` (see [`Self::sweep_after_drain`]).
	///
	/// Under a lease, each batch's passes run as [`Self::index_compaction_loop`]
	/// runs them, and the drain stops where it finds the lease taken by another
	/// node. Outside one, they run one after another on the calling task
	/// ([`Self::index_compaction_serial`]), and the pass reports
	/// [`TaskLeaseState::NotLeased`].
	async fn index_compaction_drain(
		dbs: Arc<Datastore>,
		lh: Option<&LeaseHandler>,
		sweep_period: Duration,
		canceller: CancellationToken,
		max_batches: Option<usize>,
	) -> Result<CompactionDrained> {
		// Output function invocation details to logs
		trace!(target: TARGET, "Attempting index compaction process");
		let mut count_iteration = 0;
		let mut count_error = 0;
		let mut count_passes = 0;
		let mut stalled = false;
		// The batches this call has read, which its budget counts.
		let mut read = 0;
		let mut pass = match lh {
			Some(_) => Pass::elsewhere(),
			None => Pass::done(TaskLeaseState::NotLeased),
		};
		// The queue range and a rotating window within it. After each batch
		// the window resumes past the last index that batch covered, so
		// indexes later in the keyspace are reached even while an
		// earlier-sorting index keeps enqueueing new entries (queue keys
		// sort by ns/db/tb/ix before their time-ordered UUID, so a hot
		// index would otherwise pin a start-anchored scan to itself). When
		// the scan reaches the end of the range it wraps to the start, and
		// a wrap that finds nothing means the queue is drained.
		let queue = IndexCompactionPrefix {}.range()?;
		// The drain continues from where the previous call stopped, so a
		// budget-bounded slice is one stretch of the same rotation rather than a
		// fresh scan from the head that would never reach the later indexes, and
		// passes none of the requests the drain has kept so far again (see
		// `CompactionDrain`). A call that fails leaves the drain where the
		// previous call did.
		let mut cursor = dbs.compaction_cursor.lock().clone();
		let mut window = match &cursor.resume_at {
			Some(after) => queue.clone().resume_after(after, Direction::Forward),
			None => queue.clone(),
		};
		// We continue without interruptions while there are keys and the lease
		'compaction: loop {
			Self::ensure_not_cancelled(&canceller)?;
			// The slice budget: a pass that has read its share of batches
			// reports the rest as backlog and yields, so a driver running the
			// drain a slice at a time can interleave other work.
			if max_batches.is_some_and(|max| read >= max) {
				pass.progress = Progress::More;
				break 'compaction;
			}
			// Attempt to acquire a lease for the IndexCompaction task
			// If we don't get the lease, another node is handling this task
			if let Some(lh) = lh {
				if !lh.has_lease().await? {
					pass.lease = TaskLeaseState::Elsewhere;
					break 'compaction;
				}
				pass.lease = TaskLeaseState::Held;
			}
			Self::ensure_not_cancelled(&canceller)?;
			// Output function invocation details to logs
			trace!(target: TARGET, "Running index compaction process");
			// Read one bounded batch of queue keys in a short-lived read
			// transaction. The queue holds one entry per index each write
			// transaction touched since it last drained, so an unbounded
			// snapshot would make the cleanup transaction below arbitrarily
			// large — every key it deletes is a per-replica write reservation
			// on distributed backends. Queue values carry no payload, so a
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
				if window.start() == queue.start() || !cursor.lap_passed {
					// Nothing left anywhere in the queue, or nothing but the
					// requests this drain already kept: a queue drained as far as
					// this drain can. An index can still hold work no request
					// names, so the defined indexes are swept for it, and a sweep
					// that queued requests is drained by this same drain.
					match Self::sweep_after_drain(
						&dbs,
						sweep_period,
						lh,
						&canceller,
						&mut cursor.drain,
						&mut read,
						max_batches,
					)
					.await?
					{
						Swept::Queued => {
							cursor.resume_at = None;
							cursor.lap_passed = false;
							window = queue.clone();
							continue;
						}
						// The next slice continues the sweep, after reading the
						// queue again for what was queued meanwhile.
						Swept::Spent => {
							cursor.resume_at = None;
							cursor.lap_passed = false;
							pass.progress = Progress::More;
							break 'compaction;
						}
						Swept::Done => {
							cursor = CompactionCursor::default();
							break 'compaction;
						}
						// The holder sweeps for itself; this node's drain stays
						// where it stopped.
						Swept::LeaseLost => {
							pass.lease = TaskLeaseState::Elsewhere;
							break 'compaction;
						}
					}
				}
				// End of the range: wrap to re-scan entries that were
				// skipped when the window seeked past a partially-drained
				// index.
				cursor.resume_at = None;
				cursor.lap_passed = false;
				window = queue.clone();
				continue;
			}
			read += 1;
			// Every key the drain has not kept is either passed or deleted.
			let acts = keys.iter().any(|k| !cursor.drain.kept.contains(k));
			if acts {
				pass.items += keys.len() as u64;
				if let Some(age) = Self::oldest_queued_age(
					keys.iter()
						.filter_map(|k| IndexCompactionKey::decode_key(k).ok().map(|k| k.uid)),
				) {
					pass.observe_age(age);
				}
				#[cfg(test)]
				if let Some(hook) = test::take_compaction_batch_hook(dbs.id, test::BatchStage::Read)
				{
					hook().await;
				}
				let Some(cleanup) = Self::index_compaction_batch(
					Arc::clone(&dbs),
					lh,
					&keys,
					&cursor.drain,
					&canceller,
				)
				.await?
				else {
					// Another node holds the lease now. The batch's requests are
					// its to act on, and this node's rotation stays where the
					// batch began.
					pass.lease = TaskLeaseState::Elsewhere;
					break 'compaction;
				};
				count_iteration += 1;
				count_passes += cleanup.passed.len();
				cursor.lap_passed = true;
				count_error += cleanup.errors;
				if !cleanup.deleted {
					// The batch stays queued, and the rotation stays where it
					// began. Conflicts that outlasted the cleanup's retries leave
					// a backlog the next slice reaches again at once; any other
					// failure waits for the next drain.
					if cleanup.contended {
						pass.progress = Progress::More;
					} else {
						stalled = true;
					}
					break 'compaction;
				}
				whole_queue &= !cleanup.budgeted;
				cursor.drain.kept.extend(cleanup.kept);
				cursor.drain.retained.extend(cleanup.retained);
				cursor.drain.passed.extend(cleanup.passed);
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
				let after = match IndexCompactionKey::decode_key(last_key) {
					Ok(last) => {
						// The first key ordering after the whole index's run of
						// queue entries.
						IndexCompactionIxPrefix {
							ns: last.ns,
							db: last.db,
							tb: Cow::Owned(last.tb.into_owned()),
							ix: last.ix,
						}
						.skip_extensions()?
						.to_vec()
					}
					Err(e) => {
						warn!(target: TARGET, "Skipping undecodable index compaction queue entry: {e}");
						last_key.clone()
					}
				};
				window = queue.clone().resume_after(&after, Direction::Forward);
				cursor.resume_at = Some(after);
			}
			#[cfg(test)]
			if acts
				&& let Some(hook) =
					test::take_compaction_batch_hook(dbs.id, test::BatchStage::Cleared)
			{
				hook().await;
			}
			if whole_queue {
				// The batch held the whole queue, so the queue this drain
				// leaves is empty but for what it kept and what was queued
				// meanwhile: swept, as a drain that reads it empty sweeps it.
				match Self::sweep_after_drain(
					&dbs,
					sweep_period,
					lh,
					&canceller,
					&mut cursor.drain,
					&mut read,
					max_batches,
				)
				.await?
				{
					Swept::Queued => {
						cursor.resume_at = None;
						cursor.lap_passed = false;
						window = queue.clone();
						continue;
					}
					Swept::Spent => {
						cursor.resume_at = None;
						cursor.lap_passed = false;
						pass.progress = Progress::More;
						break;
					}
					// The sweep queued nothing, which ends the drain.
					Swept::Done => {
						cursor = CompactionCursor::default();
						break;
					}
					Swept::LeaseLost => {
						pass.lease = TaskLeaseState::Elsewhere;
						break;
					}
				}
			}
		}
		*dbs.compaction_cursor.lock() = cursor;
		Ok(CompactionDrained {
			batches: count_iteration,
			errors: count_error,
			passes: count_passes,
			stalled,
			pass,
		})
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
	/// Under the lease `lh`, the batch first confirms it. One that finds another
	/// node holds it runs no pass, queues nothing and deletes nothing, leaving
	/// its requests to that node, and returns `None`. Without a lease, the
	/// batch's passes run on the calling task ([`Self::index_compaction_serial`])
	/// and it always returns `Some`.
	///
	/// Returns what the committed cleanup left queued, for the drain to record.
	async fn index_compaction_batch(
		dbs: Arc<Datastore>,
		lh: Option<&LeaseHandler>,
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
		Self::ensure_not_cancelled(canceller)?;
		let report = match lh {
			Some(lh) => {
				let lease = CompactionLease::new(lh);
				if !lease.maintain().await? {
					trace!(target: TARGET, "Index compaction lease lost, leaving the batch to its holder");
					return Ok(None);
				}
				Self::index_compaction_loop(Arc::clone(&dbs), &lease, &indexes, canceller.clone())
					.await?
			}
			None => dbs.index_compaction_serial(&indexes, canceller).await?,
		};
		let (deletes, still_queued) =
			Self::partition_index_compaction_batch(keys, &decoded, drain, &report);
		let end = Self::delete_index_compaction_batch(&dbs, &deletes, canceller).await?;
		Ok(Some(if end == CompactionCleanupEnd::Committed {
			CompactionBatchCleanup {
				errors: report.errors,
				deleted: true,
				contended: false,
				kept: still_queued,
				retained: report.retained,
				budgeted: report.budgeted,
				passed: indexes,
			}
		} else {
			CompactionBatchCleanup {
				errors: report.errors,
				deleted: false,
				contended: end == CompactionCleanupEnd::Contended,
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
	/// Returns how the cleanup ended. A retryable conflict on a delete or the
	/// commit is retried, at most [`INDEX_COMPACTION_PASS_MAX_RETRIES`] times
	/// and after the same wait as a pass's retries. A conflict that outlasts
	/// the retries ends it [`CompactionCleanupEnd::Contended`], and a failure
	/// on anything else [`CompactionCleanupEnd::Failed`]; either is logged and
	/// leaves every entry queued, as the cleanup's transaction is discarded:
	/// the cleanup never loses a request, and the drain that ran it stops.
	async fn delete_index_compaction_batch(
		dbs: &Datastore,
		deletes: &[&[u8]],
		canceller: &CancellationToken,
	) -> Result<CompactionCleanupEnd> {
		if deletes.is_empty() {
			Self::ensure_not_cancelled(canceller)?;
			return Ok(CompactionCleanupEnd::Committed);
		}
		let mut retries = 0;
		loop {
			match Self::try_delete_index_compaction_batch(dbs, deletes, canceller).await? {
				Ok(()) => return Ok(CompactionCleanupEnd::Committed),
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
					return Ok(if is_retryable_transaction_conflict(&e) {
						CompactionCleanupEnd::Contended
					} else {
						CompactionCleanupEnd::Failed
					});
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
	/// the number of pending removals. A pass walks at most
	/// [`RECLAIM_PASS_WALK_BATCHES`] of them, so a queue that has grown long
	/// is walked over several passes rather than whole in one: a pass that
	/// stops with entries still to walk keeps its position for the next, and
	/// the scan they make up ends at the last entry there was when it began,
	/// so entries queued while it runs are the next scan's. Each prefix is
	/// then destroyed independently and idempotently — in one
	/// out-of-transaction call on a backend offering [`DestroyRange`],
	/// otherwise in bounded pages, each committed together with the resume
	/// cursor that accounts for it. A queue entry is retired only once its
	/// prefix is empty, so a crash or an exhausted budget mid-reclaim resumes
	/// from the committed cursor on a later pass instead of restarting the
	/// prefix.
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
	/// MVCC GC safepoint (`now - mvcc_gc_lifetime`) when `grace >= mvcc_gc_lifetime`,
	/// by which point any transaction that could still read the data has expired.
	/// A `grace` of zero reclaims immediately without stamping, which is sound where
	/// nothing reads the prefix out-of-band: no concurrent reader, as for tests and
	/// the language-test harness teardown, or no out-of-transaction destroy, as for a
	/// host's own passes ([`Self::run_reclaim`]).
	///
	/// A doc-ID entry ([`ReclaimKind::is_doc_id`]) is observed without being
	/// written: its first sighting is held in [`Self::reclaim_sightings`], and the
	/// first page records it with the cursor. Until the entry has been queued
	/// for twice the grace, this task writes it only to repair a state it cannot
	/// read, so a `DEFINE INDEX` claiming the space back meets no conflict from
	/// it. A restart or a lease move forgets the sightings and restarts the grace
	/// of an entry no page has touched; an entry a page has touched ages from the
	/// time that page recorded. So that sightings forgotten faster than the grace
	/// cannot defer the reclaim indefinitely, an entry this node holds no
	/// sighting of, queued by its `uid` both twice the grace ago and before this
	/// node's sightings began ([`Self::reclaim_sightings_since`]), is stamped, as
	/// every other kind is, and ages from the stamp; so is one whose `uid` is
	/// twice the grace ahead of this node's clock, which cannot date it. An
	/// entry queued after the sightings began, however long its `REMOVE` stayed
	/// open, is new to this node and is sighted. The `uid` only decides when to
	/// stamp, never how long the grace runs. Comparing it with this node's clock
	/// assumes node clocks agree to well within the backstop, as the task's
	/// lease already does to within its far shorter duration: a node whose clock
	/// trails by more than the backstop has its fresh entries stamped as though
	/// forgotten, which costs a claim its conflict-free window, never the grace.
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
		let (iterations, errors, _) = Self::reclaim_tombstones_bounded(
			dbs,
			interval,
			grace,
			RECLAIM_PASS_KEY_BUDGET,
			canceller,
		)
		.await?;
		Ok((iterations, errors))
	}

	/// One pass of [`Self::reclaim_tombstones`] as a job pass: the keys
	/// deleted are its items, and a spent key budget, or a walk stopped with
	/// entries still to walk, reports [`Progress::More`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(dbs, canceller)
		)
	)]
	pub(crate) async fn reclaim_tombstones_slice(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		canceller: CancellationToken,
	) -> Result<Pass> {
		let (_, _, pass) = Self::reclaim_tombstones_bounded(
			dbs,
			interval,
			grace,
			RECLAIM_PASS_KEY_BUDGET,
			canceller,
		)
		.await?;
		Ok(pass)
	}

	/// [`Self::reclaim_tombstones`] with the per-pass key budget lowered, so a
	/// test can drive a pass that stops mid-prefix and inspect what it left
	/// behind, and what it reported.
	#[cfg(test)]
	#[cfg_attr(not(any(feature = "kv-mem", feature = "kv-surrealkv")), allow(dead_code))]
	pub(crate) async fn reclaim_tombstones_with_budget(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		budget: u64,
		canceller: CancellationToken,
	) -> Result<(usize, usize, Pass)> {
		Self::reclaim_tombstones_bounded(dbs, interval, grace, budget, canceller).await
	}

	/// One bounded pass of the reclaim queue on the calling task, for a host that drives the
	/// datastore's background work itself
	/// ([`without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks)): it
	/// deletes the data of namespaces, databases and indexes a committed `REMOVE` left queued
	/// ([`ReclaimKey`]), and of the shared doc-ID spaces removing a table's last doc-ID index
	/// left queued.
	///
	/// At most `budget` data keys are deleted, capped at [`RECLAIM_PASS_KEY_BUDGET`], in pages of
	/// at most [`RECLAIM_BATCH_SIZE`] keys, each served entry's share of the budget rounded down
	/// to whole pages (at least one), so a pass commits at most `budget / RECLAIM_BATCH_SIZE`
	/// (rounded up) data pages however many entries hold data, and one write transaction that
	/// retires the entries it emptied; an entry found already empty costs a read and no commit.
	/// Every page is an ordinary write transaction that deletes its keys and advances its entry's
	/// resume cursor together, so a pass stopped anywhere loses no committed page and the next
	/// pass resumes at the cursor. An entry is retired once its prefix is empty. The queue is
	/// walked as [`Self::reclaim_tombstones`] walks it, at most [`RECLAIM_PASS_WALK_BATCHES`]
	/// batches a pass, and the budget is spent on the longest-waiting entries first.
	///
	/// Nothing is spawned and no lease is taken: the pass runs unleased, so the host runs one
	/// pass at a time, and each page's compare-and-set on its entry keeps a `DEFINE INDEX`
	/// finishing the same doc-ID reclaim from deleting a page twice. No prefix is
	/// destroyed outside a transaction, whatever the backend offers, so the data stays visible
	/// to every snapshot that predates a page.
	///
	/// An entry is reclaimed only once it has been observed for `grace`, measured on the
	/// datastore's wall clock ([`Builder::with_wall_clock`]) from the pass that first saw it,
	/// which stamps it, or, for an untouched doc-ID entry, holds the sighting in memory
	/// ([`Self::reclaim_tombstones`]). A `grace` of zero reclaims every entry on the first pass
	/// that reaches it and writes no stamp; it is sound on a backend whose reads hold their
	/// snapshot for as long as they run, since every page is an ordinary transaction. A host whose
	/// readers need the data kept for some time after the `REMOVE` commits passes that time.
	///
	/// A commit that queues an entry puts a key [`Self::is_reclaim_key`] matches and notifies
	/// [`CommitTriggers::reclaim`](surrealdb_datastore::triggers::CommitTriggers::reclaim). A
	/// pass's pages and first stamps put such keys too, without notifying; retiring an entry
	/// deletes its key, and a pass that finds nothing to do commits nothing. A host runs a pass
	/// on such a commit, again at once while [`ReclaimRun::more`] is set, and after
	/// [`ReclaimRun::retry_after`].
	pub async fn run_reclaim(&self, budget: u64, grace: Duration) -> Result<ReclaimRun> {
		let budget = budget.min(RECLAIM_PASS_KEY_BUDGET);
		let run = self
			.reclaim_pass(ReclaimMode::Hosted, grace, budget, &CancellationToken::new())
			.await?;
		let more = run.pass.progress == Progress::More || run.incomplete;
		let retry_after = match run.next_due_ms {
			Some(due) if !more => {
				Some(Duration::from_millis(due.saturating_sub(self.wall_clock_ms())))
			}
			_ => None,
		};
		Ok(ReclaimRun {
			keys: run.pass.items,
			more,
			retry_after,
		})
	}

	/// Whether `key` is an entry of the reclaim queue, which a commit that removes a namespace,
	/// database or index, or a table's last doc-ID index, puts. A commit that put no such key
	/// queued no reclaim.
	pub fn is_reclaim_key(key: &[u8]) -> bool {
		ReclaimKey::decode_key(key).is_ok()
	}

	/// The key bounds `[start, end)` of the reclaim queue: every key [`Self::is_reclaim_key`]
	/// matches lies within them.
	pub fn reclaim_queue_bounds() -> Result<(Vec<u8>, Vec<u8>)> {
		let range = ReclaimPrefix {}.range()?;
		Ok((range.start().as_slice().to_vec(), range.end().as_slice().to_vec()))
	}

	/// One reclaim pass, deleting at most `budget` data keys in total.
	///
	/// A scan walks the whole queue, in passes of at most
	/// [`RECLAIM_PASS_WALK_BATCHES`] batches, and each pass spends its key budget
	/// on part of what it walked. The walk and the spend are two different bounds
	/// and are enforced separately.
	///
	/// The walk is not gated on the key budget. Stamping an entry's first sighting
	/// and ageing it against the grace costs at most one write per entry, so that
	/// work scales with the number of pending removals — catalog scale — and not
	/// with the data they name. Gating it on the key budget would leave an entry
	/// beyond the budget's reach unstamped, and an unstamped entry has
	/// `observed_ms == 0`, so its grace has not started and it is not yet even a
	/// candidate for reclaim. The queue is walked in bounded batches, each batch's
	/// stamps committed in one transaction, so the walk's memory and write batches
	/// stay proportional to one batch however long the queue is; and a pass walks
	/// at most its share of batches, so what it reads and stamps is bounded too. A
	/// pass that stops with entries still to walk keeps the scan's position for
	/// the next pass, and a scan ends at the last entry there was when it began,
	/// so entries queued past that while it runs are the next scan's and a scan
	/// ends however fast removals are queued.
	///
	/// The key budget is then spent on the [`RECLAIM_PASS_ENTRY_QUOTA`]
	/// longest-observed eligible entries the pass walked, selected while walking
	/// so only that many are ever held. Oldest-first rather than queue order: key
	/// order makes the entries that happen to sort lowest the only ones ever
	/// served while they hold data, whereas the wait an operator cares about is
	/// how long a removal has gone unreclaimed. Spreading the budget across every
	/// eligible entry instead would shrink each share towards one key as the
	/// queue grows.
	async fn reclaim_tombstones_bounded(
		dbs: Arc<Datastore>,
		interval: Duration,
		grace: Duration,
		budget: u64,
		canceller: CancellationToken,
	) -> Result<(usize, usize, Pass)> {
		// Coordinate across the cluster so only one node reclaims at a time.
		let lh =
			dbs.lease_handler(TaskLeaseType::ReclaimTombstones, interval * 2, Some(&canceller))?;
		let run = dbs.reclaim_pass(ReclaimMode::Leased(&lh), grace, budget, &canceller).await?;
		Ok((run.iterations, run.errors, run.pass))
	}

	/// One reclaim pass in `mode`, deleting at most `budget` data keys in total;
	/// see [`Self::reclaim_tombstones_bounded`] for how the walk and the spend
	/// are bounded. Entry ages are read from the datastore's wall clock
	/// ([`Builder::with_wall_clock`]).
	async fn reclaim_pass(
		&self,
		mode: ReclaimMode<'_>,
		grace: Duration,
		budget: u64,
		canceller: &CancellationToken,
	) -> Result<ReclaimPass> {
		trace!(target: TARGET, "Attempting tombstone reclaim process");
		let dbs = self;
		// Data keys this pass may still delete, shared out across the entries it
		// serves. Spent to zero ends the spend with every cursor committed.
		let limit = budget;
		let mut budget = budget;
		// The earliest instant an entry this pass saw inside its grace ages past
		// it, so a host that drives the passes knows when to run the next.
		let mut next_due_ms: Option<u64> = None;
		// Whether an entry this pass served still holds data, which a pass run
		// at once continues even where this one ends with budget unspent.
		let mut incomplete = false;
		let mut count_iteration = 0;
		let mut count_error = 0;
		// The queue range and a window within it, advanced past each batch the
		// walk reads. Reading the queue in bounded batches is what keeps both the
		// snapshot the pass holds and the write transaction that updates it
		// proportional to one batch rather than to the number of pending removals.
		let queue = ReclaimPrefix {}.range()?;
		// The scan this pass continues, or `None` to begin one over the entries
		// there are now, once the lease is confirmed held.
		let mut scan = dbs.reclaim_cursor.lock().take();
		// Batches this pass may still walk. Spent with entries still to walk,
		// the pass keeps the scan for the next one and reports `More`.
		let mut walk = RECLAIM_PASS_WALK_BATCHES;
		let mut walk_stopped = false;
		// The longest-observed eligible entries seen so far, at most
		// `RECLAIM_PASS_ENTRY_QUOTA` of them. A max-heap ordered oldest-first, so
		// the entry discarded when the quota is exceeded is the newest held.
		let mut candidates: BinaryHeap<ReclaimCandidate> = BinaryHeap::new();
		// Whether the walk finished holding the lease. Losing it mid-walk ends the
		// pass: the deletes below would otherwise run against a prefix the node
		// that holds the lease now is already working on.
		let mut lease_held = true;
		'pass: {
			// Walk the queue: stamp, age, and select. No deletes, so no budget.
			loop {
				Self::ensure_not_cancelled(canceller)?;
				// If we don't hold the lease, another node is handling this task.
				let held = match mode {
					ReclaimMode::Leased(lh) => lh.has_lease().await?,
					ReclaimMode::Hosted => true,
				};
				if !held {
					lease_held = false;
					break;
				}
				// The sightings are kept from here until the lease is found
				// elsewhere ([`Self::reclaim_sightings_since`]).
				if dbs.reclaim_sightings_since.load(std::sync::atomic::Ordering::Relaxed) == 0 {
					let now_ms = dbs.wall_clock_ms().max(1);
					dbs.reclaim_sightings_since.store(now_ms, std::sync::atomic::Ordering::Relaxed);
				}
				Self::ensure_not_cancelled(canceller)?;
				// The window to read: after the batch the scan last read, or from
				// the head of a scan begun now, which ends at the last entry there
				// is. Nothing queued at all ends the walk.
				let walked_after = scan.as_ref().map(|scan| scan.after.clone());
				let (window, end) = match scan.take() {
					Some(scan) => {
						(queue.clone().resume_after(&scan.after, Direction::Forward), scan.end)
					}
					None => match dbs.range_end(queue.clone()).await? {
						Some(end) => (queue.clone(), end),
						None => {
							// Nothing is queued, so no sighting names a live entry.
							dbs.reclaim_sightings.lock().clear();
							break;
						}
					},
				};
				// Read one bounded batch of queue entries in a short-lived read
				// transaction, to avoid holding a write lock across the entire
				// reclaim cycle. Values are read as bytes so that an entry this node
				// cannot decode is skipped with a warning rather than failing the
				// whole pass.
				let items = {
					let txn = dbs.transaction(Read).await?;
					let res = txn.scan_raw(window, RECLAIM_BATCH_SIZE, 0, None).await;
					let _ = txn.cancel().await;
					res?
				};
				Self::ensure_not_cancelled(canceller)?;
				let Some((last, _)) = items.last().cloned() else {
					// The window has run off the end of the queue: everything queued
					// has been walked, and nothing past the scan's position is held.
					dbs.prune_reclaim_sightings(walked_after.as_deref(), None, &items);
					break;
				};
				count_iteration += 1;
				// The batch that completes the scan holds the last entry there was
				// when the scan began, or is short because nothing lies past it now
				// (the last entry may have been claimed since the scan began). Either
				// way nothing past it was sighted live.
				let exhausted = items.len() < RECLAIM_BATCH_SIZE as usize || last >= end;
				let through = (!exhausted).then_some(last.as_slice());
				dbs.prune_reclaim_sightings(walked_after.as_deref(), through, &items);
				// Current wall-clock time (unix millis), used to age each entry
				// against `grace` and to stamp first observations.
				let now_ms = dbs.wall_clock_ms();
				let grace_ms = grace.as_millis() as u64;
				// How long an untouched doc-ID entry may stay queued before it is
				// stamped rather than sighted ([`Self::reclaim_tombstones`]).
				let sighting_backstop_ms = grace_ms.saturating_mul(2);
				let mut due = |at: u64| {
					next_due_ms = Some(next_due_ms.map_or(at, |d| d.min(at)));
				};
				// Queue states to initialise or repair. The raw value read with each
				// one is the compare-and-set guard, so a concurrent cursor advance or
				// cancellation cannot be overwritten by this batch.
				let mut to_stamp: Vec<(Vec<u8>, Vec<u8>, ReclaimState)> = Vec::new();
				for (k, v) in items.iter() {
					Self::ensure_not_cancelled(canceller)?;
					let rc = match ReclaimKey::decode_key(k) {
						Ok(rc) => rc,
						Err(e) => {
							count_error += 1;
							warn!(target: TARGET, "Failed to decode reclaim queue entry: {e}");
							continue;
						}
					};
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
							// A doc-ID entry's lost state may have held a cursor, so
							// the space it names may be torn. It is repaired with an
							// empty cursor, which reads as started to a claim
							// ([`ReclaimKind::is_doc_id`]) and restarts the reclaim
							// from the head of its prefix.
							let repaired = ReclaimState {
								observed_ms: if grace.is_zero() {
									0
								} else {
									now_ms
								},
								cursor: rc.kind.is_doc_id().then(Vec::new),
							};
							to_stamp.push((k.clone(), v.clone(), repaired.clone()));
							dbs.reclaim_sightings.lock().remove(k.as_slice());
							// Zero-grace callers may reclaim immediately after the
							// conditional repair commits. If that repair loses a race, the
							// page's own CAS sees that `repaired` is not current and stops.
							if grace.is_zero() {
								candidates.push(ReclaimCandidate {
									observed_ms: repaired.observed_ms,
									key: k.clone(),
									rc: rc.into_owned(),
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
					let mut observed_ms = state.observed_ms;
					// Only an untouched doc-ID entry is aged from a sighting. One that
					// carries a stamp or a cursor has a durable record of its own, and
					// its sighting is no longer needed.
					let untouched_doc_id =
						rc.kind.is_doc_id() && observed_ms == 0 && state.cursor.is_none();
					if rc.kind.is_doc_id() && !untouched_doc_id {
						dbs.reclaim_sightings.lock().remove(k.as_slice());
					}
					if !grace.is_zero() {
						if untouched_doc_id {
							// First sighting of an unstamped doc-ID entry: held in
							// memory rather than written, so a claim on it meets no
							// stamp ([`Self::reclaim_sightings`]). Not a candidate this
							// pass: its grace starts now. One queued past the backstop
							// with no sighting held is stamped instead, so a sighting
							// forgotten on every restart or lease move cannot hold it
							// back for good. Past it means queued at least twice the
							// grace before now, by its `uid`, and before this node's
							// sightings began ([`Self::reclaim_sightings_since`]): an
							// entry queued since then that holds no sighting is new to
							// this node, however long its `REMOVE` stayed open before
							// it committed. A `uid` with no timestamp is past it, and
							// so is one further ahead of this node's clock than the
							// backstop: a `uid` that far ahead came from a clock that
							// cannot date the entry, and would otherwise hold the
							// backstop off until that clock's time.
							let sightings_since = dbs
								.reclaim_sightings_since
								.load(std::sync::atomic::Ordering::Relaxed);
							let past_backstop = rc.uid.get_timestamp().is_none_or(|t| {
								let (secs, nanos) = t.to_unix();
								let queued_ms = secs
									.saturating_mul(1000)
									.saturating_add(u64::from(nanos / 1_000_000));
								match queued_ms.checked_sub(now_ms) {
									Some(ahead) => ahead >= sighting_backstop_ms,
									None => {
										now_ms - queued_ms >= sighting_backstop_ms
											&& queued_ms < sightings_since
									}
								}
							});
							match dbs.reclaim_sightings.lock().entry(k.clone()) {
								std::collections::btree_map::Entry::Occupied(seen) => {
									observed_ms = *seen.get()
								}
								std::collections::btree_map::Entry::Vacant(slot) => {
									if past_backstop {
										to_stamp.push((
											k.clone(),
											v.clone(),
											ReclaimState {
												observed_ms: now_ms,
												cursor: None,
											},
										));
									} else {
										slot.insert(now_ms);
									}
									due(now_ms.saturating_add(grace_ms));
									continue;
								}
							}
						} else if observed_ms == 0 {
							// First sighting of this committed entry — record an
							// observation time (>= commit) and defer reclaim. Aging
							// is measured from here, never from the pre-commit
							// `uid`. Not a candidate this pass: its grace starts now.
							due(now_ms.saturating_add(grace_ms));
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
						if now_ms.saturating_sub(observed_ms) < grace_ms {
							due(observed_ms.saturating_add(grace_ms));
							continue;
						}
					}
					candidates.push(ReclaimCandidate {
						observed_ms,
						key: k.clone(),
						rc: rc.into_owned(),
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
					if let Err(e) = Self::ensure_not_cancelled(canceller) {
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
						match txn
							.put_compare(Key::from(k.clone()), value, Some(expected.clone()))
							.await
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
								break 'pass;
							}
						}
					}
				}
				// The batch that reaches the end the scan began with ends it: what
				// lies past that key was queued since, and is the next scan's. A short
				// batch ends it too, having found nothing past its last entry.
				if exhausted {
					break;
				}
				// Seek past the batch just walked.
				scan = Some(ReclaimScan {
					after: last,
					end,
				});
				walk -= 1;
				if walk == 0 {
					// Spent with entries still to walk: the next pass resumes here.
					*dbs.reclaim_cursor.lock() = scan.take();
					walk_stopped = true;
					break;
				}
			}
			if !lease_held {
				break 'pass;
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
				Self::ensure_not_cancelled(canceller)?;
				// Stop at the entry the lease was lost on, and report the pass
				// as run under another node's lease, as the walk does. The queue
				// update below still commits what this pass emptied, and
				// whichever node holds the lease now continues from the durable
				// cursors. The answer has to be read rather than discarded: a
				// lease check is throttled to one datastore read per maintenance
				// period, so a call that drops it consumes the read the page
				// loop's own check depends on and leaves that check answering
				// affirmatively without asking.
				if !mode.held().await? {
					lease_held = false;
					break;
				}
				let ReclaimCandidate {
					observed_ms,
					key,
					rc,
					state,
				} = candidate;
				// The candidate carries the sighting to the page that records it. It
				// is forgotten once the entry has a record of its own or is retired,
				// and kept through a failure that left the entry untouched, so a retry
				// on the next pass does not start its grace again.
				let forget = |key: &[u8]| {
					if rc.kind.is_doc_id() {
						dbs.reclaim_sightings.lock().remove(key);
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
				// A hosted pass instead rounds each share down to whole pages, at
				// least one, so the pages it commits add up to at most one per
				// `RECLAIM_BATCH_SIZE` keys of budget however many entries hold
				// data, where unrounded shares would end the pass with ever smaller
				// pages.
				let share = match mode {
					ReclaimMode::Leased(_) => {
						let divisor = remaining.min(RECLAIM_PASS_ENTRY_QUOTA as u64);
						(budget / divisor.max(1)).max(1).min(budget)
					}
					ReclaimMode::Hosted => {
						let page = RECLAIM_BATCH_SIZE as u64;
						let even = budget / remaining.max(1);
						(even - even % page).max(page).min(budget)
					}
				};
				remaining = remaining.saturating_sub(1);
				let mut left = share;
				// Whether the lease passed to another node mid-entry. Recorded
				// rather than broken on directly, so the budget below is charged
				// for the pages this entry did commit before the pass ends.
				let mut lease_lost = false;
				match dbs
					.reclaim_decoded(&rc, &state, observed_ms, mode, canceller, &mut left)
					.await
				{
					// The prefix is empty, so the entry has nothing left to
					// name and can be retired.
					// Its sighting is forgotten once the retirement below commits.
					Ok(ReclaimOutcome::Complete) => done.push(key),
					// Progress is durable but keys remain: leaving the entry
					// queued is what lets a later pass resume it.
					Ok(ReclaimOutcome::Incomplete) => {
						forget(&key);
						incomplete = true;
					}
					// The entry is already gone, so it must not be added to
					// `done`: that would delete whatever a concurrent writer
					// put at the same key.
					Ok(ReclaimOutcome::Cancelled) => forget(&key),
					// Whichever node holds the lease now resumes from the
					// cursors this pass committed, and the pass reports the
					// lease elsewhere, as it does when the check before the
					// entry loses it.
					Ok(ReclaimOutcome::LeaseLost) => {
						lease_lost = true;
						lease_held = false;
					}
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
				if let Err(e) = Self::ensure_not_cancelled(canceller) {
					let _ = txn.cancel().await;
					return Err(e);
				}
				// Retire only the value currently stored. This prevents a stale
				// completion from deleting a state advanced by another node, and
				// makes a concurrent cancellation win cleanly on TiKV.
				let mut condition_failed = false;
				// The entries this transaction retires, or found already gone: only
				// their sightings are forgotten once it commits. One whose read or
				// delete failed stays queued, and keeps its sighting.
				let mut retired: Vec<&[u8]> = Vec::with_capacity(done.len());
				for k in &done {
					let current = match txn.get(Key::from(k), None).await {
						Ok(Some(current)) => current,
						Ok(None) => {
							retired.push(k);
							continue;
						}
						Err(e) => {
							warn!(target: TARGET, "Failed to read reclaim queue entry: {e}");
							continue;
						}
					};
					let deleted = async {
						#[cfg(test)]
						maybe_inject_non_retryable_error(
							NonRetryableErrorSite::ReclaimQueueRetireEntry,
							dbs.id,
						)?;
						txn.del_compare(Key::from(k), Some(&current)).await
					}
					.await;
					match deleted {
						Ok(()) => retired.push(k),
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
					break 'pass;
				}
				// A pass whose queue update does not land has reclaimed data
				// without recording that it did, so it is counted: an uncounted
				// failure here reports as a healthy pass and leaves nothing for an
				// operator to see but the next tick repeating the same work.
				let committed = async {
					#[cfg(test)]
					maybe_inject_retryable_conflict(
						RetryableConflictSite::ReclaimQueueRetire,
						dbs.id,
					)?;
					txn.commit().await
				}
				.await;
				match committed {
					// Retired, so their sightings name nothing. A retirement that does
					// not commit keeps them all, and the next pass retires them at once.
					Ok(()) => {
						let mut sightings = dbs.reclaim_sightings.lock();
						for k in retired {
							sightings.remove(k);
						}
					}
					Err(e) => {
						let _ = txn.cancel().await;
						if !super::is_conditional_write_conflict(&e) {
							count_error += 1;
							warn!(target: TARGET, "Failed to commit reclaim queue update: {e}");
						}
					}
				}
			}
		}
		// A node without the lease forgets the walk it held: the node holding
		// the lease now walks from its own head, and a sighting kept across a
		// lease that moved away and back would age an entry from before another
		// node held it.
		if !lease_held {
			*dbs.reclaim_cursor.lock() = None;
			dbs.reclaim_sightings.lock().clear();
			dbs.reclaim_sightings_since.store(0, std::sync::atomic::Ordering::Relaxed);
		}
		// A spent key budget, or a walk stopped with entries still to walk, are
		// what end a pass with work still queued: every other exit has either
		// walked its scan to the end or handed the rest to the node that holds
		// the lease now.
		let pass = Pass {
			progress: if budget == 0 || walk_stopped {
				Progress::More
			} else {
				Progress::Done
			},
			items: limit - budget,
			oldest_age: None,
			held: 0,
			ran: limit - budget,
			lease: if lease_held {
				TaskLeaseState::Held
			} else {
				TaskLeaseState::Elsewhere
			},
		};
		Ok(ReclaimPass {
			iterations: count_iteration,
			errors: count_error,
			pass,
			next_due_ms,
			incomplete,
		})
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

	/// Forget the sightings ([`Self::reclaim_sightings`]) of entries after `after`
	/// and through `through` that `held`, the batch read over that span in key
	/// order, does not hold: entries claimed or retired since they were sighted,
	/// whose keys are never queued again. `None` leaves that side unbounded.
	fn prune_reclaim_sightings<V>(
		&self,
		after: Option<&[u8]>,
		through: Option<&[u8]>,
		held: &[(Vec<u8>, V)],
	) {
		let mut sightings = self.reclaim_sightings.lock();
		if sightings.is_empty() {
			return;
		}
		let lower = after.map_or(Bound::Unbounded, Bound::Excluded);
		let upper = through.map_or(Bound::Unbounded, Bound::Included);
		// `held` is a forward scan, so it is sorted by key.
		let gone: Vec<Vec<u8>> = sightings
			.range::<[u8], _>((lower, upper))
			.filter(|(k, _)| held.binary_search_by(|(probe, _)| probe.as_slice().cmp(k)).is_err())
			.map(|(k, _)| k.clone())
			.collect();
		for k in gone {
			sightings.remove(&k);
		}
	}

	/// How many sightings this node holds ([`Self::reclaim_sightings`]).
	#[cfg(test)]
	pub(crate) fn reclaim_sightings_len(&self) -> usize {
		self.reclaim_sightings.lock().len()
	}

	/// Destroy the data prefix a decoded reclaim queue entry names.
	///
	/// Resolves the entry's kind to the byte range beneath it and hands that
	/// range to [`Self::reclaim_range`]. All of these kinds name a subtree holding
	/// keys of more than one type, so the range is resolved as bytes.
	/// `observed_ms` is when the entry was first observed, which its first page
	/// records if `state` carries no stamp.
	async fn reclaim_decoded(
		&self,
		rc: &ReclaimKey<'_>,
		state: &ReclaimState,
		observed_ms: u64,
		mode: ReclaimMode<'_>,
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
		self.reclaim_range(rc, state, observed_ms, prefix, mode, canceller, budget).await
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
	/// behind. Every cursor write carries [`ReclaimState::observed_ms`] through
	/// unchanged, or `observed_ms` where the entry carries no stamp — an
	/// unstamped doc-ID entry, whose first sighting is otherwise held only in
	/// memory. The grace gate measures from the first observation, and
	/// restamping here would let a prefix too large for one pass reset its own
	/// grace and never age past it. An empty cursor marks a doc-ID entry whose
	/// unreadable state was repaired: started, with nothing durably deleted
	/// past the head of the prefix.
	///
	/// Giving up single-transaction atomicity is sound because the catalog entry
	/// naming this prefix is already committed-removed: a partly-destroyed
	/// prefix is unreachable garbage, not partial state any reader can observe.
	///
	/// Idempotent: a re-run on an already-empty prefix scans one empty page and
	/// reports [`ReclaimOutcome::Complete`].
	#[expect(clippy::too_many_arguments)]
	async fn reclaim_range(
		&self,
		rc: &ReclaimKey<'_>,
		state: &ReclaimState,
		observed_ms: u64,
		prefix: RawRange,
		mode: ReclaimMode<'_>,
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
			&& matches!(mode, ReclaimMode::Leased(_))
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
			Some(cursor) if cursor.is_empty() => prefix,
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
				observed_ms,
			},
			expunge: rc.expunge == Expunge::Expunge,
			page: RECLAIM_BATCH_SIZE,
		}
		.run(self, mode.lease(), canceller, budget)
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
		let (iterations, errors, _) =
			Self::graph_fold_bounded(dbs, interval, canceller, None, None).await?;
		Ok((iterations, errors))
	}

	/// One slice of [`Self::graph_fold`]: at most [`GRAPH_FOLD_SLICE_BATCHES`]
	/// batches, running at most [`GRAPH_FOLD_SLICE_PASSES_PER_SCOPE`] fold
	/// passes on any scope a batch names, and reporting [`Progress::More`]
	/// when the queue may hold more — or when a scope's fold outlasted its
	/// passes, or a batch's cleanup its [`QUEUE_CLEANUP_CONFLICT_RETRIES`],
	/// and the slice kept its entries for the rotation to reach again. The
	/// keys read are the pass's items, and the oldest entry read its queue
	/// age.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(dbs, canceller)
		)
	)]
	pub(crate) async fn graph_fold_slice(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<Pass> {
		let (_, _, pass) = Self::graph_fold_bounded(
			dbs,
			interval,
			canceller,
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(GRAPH_FOLD_SLICE_PASSES_PER_SCOPE),
		)
		.await?;
		Ok(pass)
	}

	/// The drain behind [`Self::graph_fold`] and [`Self::graph_fold_slice`],
	/// stopping after `max_batches` batches when given one, and running at
	/// most `passes_per_scope` fold passes on any scope a batch names when
	/// given one: a scope whose fold outlasts them keeps its queue entries,
	/// for the rotation to reach again once the later scopes have had their
	/// passes, and the drain stops at that batch to report them as its
	/// backlog. A batch whose cleanup commit keeps losing conflicts keeps its
	/// entries the same way once [`QUEUE_CLEANUP_CONFLICT_RETRIES`] retries
	/// are spent.
	///
	/// A batch of many scopes can outlast the lease, so the lease is renewed
	/// before each scope's fold and before each of its passes. A drain that
	/// finds another node holding it stops at that scope: it deletes nothing of
	/// the batch, which is the holder's to drain, leaves its rotation where the
	/// batch began, and reports the lease as held elsewhere. A renewal that
	/// reaches no verdict fails the drain.
	///
	/// Returns the batches processed, the scopes that failed to fold, and the
	/// pass report.
	pub(crate) async fn graph_fold_bounded(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
		max_batches: Option<usize>,
		passes_per_scope: Option<usize>,
	) -> Result<(usize, usize, Pass)> {
		trace!(target: TARGET, "Attempting graph fold process");
		let lh = dbs.lease_handler(TaskLeaseType::GraphFold, interval * 2, Some(&canceller))?;
		let drain =
			Self::graph_fold_drain(dbs, Some(&lh), canceller, max_batches, passes_per_scope, None)
				.await?;
		Ok((drain.batches, drain.failed, drain.pass))
	}

	/// The drain behind [`Self::graph_fold_bounded`] and [`Self::run_graph_fold`]: this node's
	/// read-path observations flushed into the queue, then the queue's batches folded under
	/// `lh` when given one, or with no lease when the caller is the only one driving the queue.
	///
	/// With `max_passes`, the drain runs at most that many fold passes in all: a scope reached
	/// once they are spent is left with its entries, as one whose fold outlasted its own passes
	/// is, and the drain stops at that batch to report them as its backlog.
	async fn graph_fold_drain(
		dbs: Arc<Datastore>,
		lh: Option<&LeaseHandler>,
		canceller: CancellationToken,
		max_batches: Option<usize>,
		passes_per_scope: Option<usize>,
		max_passes: Option<usize>,
	) -> Result<FoldDrain> {
		dbs.flush_graph_fold_observations().await?;
		let mut count_iteration = 0;
		let mut count_error = 0;
		let mut count_folded = 0;
		let mut spent = 0;
		let mut pass = match lh {
			Some(_) => Pass::elsewhere(),
			None => Pass::done(TaskLeaseState::NotLeased),
		};
		let queue = GraphFoldPrefix {}.range()?;
		// The rotation continues from where the previous slice stopped, with
		// what it had achieved so far; see `index_compaction_bounded`.
		// `progressed` is whether the rotation since it last began at the
		// head has deleted a queue entry or folded a scope. A wrap-around
		// re-scan retries entries retained by failed folds; a rotation that
		// reaches the end of the range without progress has just failed every
		// remaining entry again, so the call yields to the next interval
		// instead of hot-looping on a poison scope — whether the rotation ran
		// in one call or, slice by slice, across several.
		let (mut resume_at, mut progressed) = {
			let cursor = dbs.fold_cursor.lock();
			(cursor.resume_at.clone(), cursor.progressed)
		};
		let mut window = match &resume_at {
			Some(after) => queue.clone().resume_after(after, Direction::Forward),
			None => queue.clone(),
		};
		'fold: loop {
			Self::ensure_not_cancelled(&canceller)?;
			// The slice budget; see `index_compaction_bounded`.
			if max_batches.is_some_and(|max| count_iteration >= max) {
				pass.progress = Progress::More;
				break 'fold;
			}
			// Without the lease, another node is draining the queue.
			if let Some(lh) = lh {
				if !lh.has_lease().await? {
					pass.lease = TaskLeaseState::Elsewhere;
					break 'fold;
				}
				pass.lease = TaskLeaseState::Held;
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
				if window.start() == queue.start() {
					// Nothing left anywhere in the queue, so no scope has a
					// pass to continue.
					progressed = false;
					dbs.fold_cursor.lock().scans.clear();
					break 'fold;
				}
				if !progressed {
					// Everything left belongs to scopes that failed this
					// rotation. The next interval starts a fresh rotation from
					// the head; resuming here would only read nothing again.
					resume_at = None;
					break 'fold;
				}
				// End of the range: wrap to re-scan entries skipped when the
				// window seeked past a partially-drained scope, or retained
				// when a scope's fold failed.
				progressed = false;
				resume_at = None;
				window = queue.clone();
				continue;
			}
			count_iteration += 1;
			pass.items += keys.len() as u64;
			if let Some(age) = Self::oldest_queued_age(
				keys.iter().filter_map(|k| GraphFoldKey::decode_key(k).ok().map(|k| k.uid)),
			) {
				pass.observe_age(age);
			}
			// The distinct scopes this batch names, in queue order, each with
			// the entries naming it, and each entry's scope for the cleanup
			// below. Entries that fail to decode name nothing foldable; the
			// cleanup removes them.
			let mut scopes: Vec<(GraphFoldScope, Vec<&[u8]>)> = Vec::new();
			let mut seen: HashMap<GraphFoldScope, usize> = HashMap::new();
			let mut key_scopes: Vec<Option<GraphFoldScope>> = Vec::with_capacity(keys.len());
			for key in &keys {
				match GraphFoldKey::decode_key(key) {
					Ok(decoded) => {
						let scope = GraphFoldScope {
							ns: decoded.ns,
							db: decoded.db,
							tb: decoded.tb.into_owned(),
							id: decoded.id.into_owned(),
							dir: decoded.dir,
						};
						let index = *seen.entry(scope.clone()).or_insert_with(|| {
							scopes.push((scope.clone(), Vec::new()));
							scopes.len() - 1
						});
						scopes[index].1.push(key.as_slice());
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
			// debt survives for the wrap-around re-scan and later calls, and
			// so do a scope whose fold outlasted its passes and one whose
			// table definition this build cannot read — the latter without
			// counting as progress, so a rotation left with nothing but such
			// scopes ends at the range like one left with failed ones.
			let mut folded: HashSet<GraphFoldScope> = HashSet::with_capacity(scopes.len());
			let mut unfinished = false;
			for (scope, entries) in scopes {
				let passes = match max_passes {
					Some(max) => {
						let left = max.saturating_sub(spent);
						if left == 0 {
							unfinished = true;
							continue;
						}
						Some(passes_per_scope.map_or(left, |per_scope| per_scope.min(left)))
					}
					None => passes_per_scope,
				};
				#[cfg(test)]
				if let Some(hook) = test::take_before_fold_scope(dbs.id) {
					hook().await;
				}
				match Self::fold_queued_scope(
					&dbs, lh, &scope, &entries, passes, &mut spent, &canceller,
				)
				.await
				{
					Ok(FoldOutcome::Folded) => {
						folded.insert(scope);
					}
					Ok(FoldOutcome::Unfinished) => unfinished = true,
					Ok(FoldOutcome::Refused) => {}
					Ok(FoldOutcome::LeaseLost) => {
						trace!(target: TARGET, "Graph fold lease lost, leaving the batch to its holder");
						pass.lease = TaskLeaseState::Elsewhere;
						break 'fold;
					}
					Err(e) => {
						if canceller.is_cancelled()
							|| e.downcast_ref::<LeaseCheckFailed>().is_some()
						{
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
				let after = match GraphFoldKey::decode_key(last_key) {
					Ok(last) => GraphFoldDirPrefix {
						ns: last.ns,
						db: last.db,
						tb: Cow::Owned(last.tb.into_owned()),
						id: Cow::Owned(last.id.into_owned()),
						dir: last.dir,
					}
					.skip_extensions()?
					.to_vec(),
					Err(_) => last_key.clone(),
				};
				window = queue.clone().resume_after(&after, Direction::Forward);
				resume_at = Some(after);
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
			if !deletable.is_empty() {
				progressed = true;
				let mut retries = 0;
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
					let committed = async {
						#[cfg(test)]
						maybe_inject_retryable_conflict(
							RetryableConflictSite::GraphFoldQueueCleanup,
							dbs.id,
						)?;
						txn.commit().await
					}
					.await;
					if let Err(e) = committed {
						if retries < QUEUE_CLEANUP_CONFLICT_RETRIES
							&& Self::cancel_and_retry_index_operation_conflict(
								&txn,
								&e,
								"Retryable conflict committing graph fold queue cleanup, retrying",
							)
							.await
						{
							retries += 1;
							continue;
						}
						let _ = txn.cancel().await;
						if is_retryable_transaction_conflict(&e) {
							// See the index compaction queue cleanup: the entries
							// stay queued as the slice's backlog.
							warn!(
								target: TARGET,
								"Graph fold queue cleanup kept losing conflicts, leaving its entries queued: {e}"
							);
							pass.progress = Progress::More;
						} else {
							warn!(target: TARGET, "Failed to commit graph fold queue cleanup: {e}");
						}
						break 'fold;
					}
					break;
				}
			}
			count_folded += folded.len();
			// A scope whose fold outlasted its passes is progress the rotation
			// made, and the slice ends at its batch: the entries it kept are
			// its backlog, and the next slice reaches them after the scopes
			// the rotation has yet to visit.
			if unfinished {
				progressed = true;
				pass.progress = Progress::More;
				break 'fold;
			}
		}
		{
			let mut cursor = dbs.fold_cursor.lock();
			cursor.resume_at = resume_at;
			cursor.progressed = progressed;
		}
		Ok(FoldDrain {
			batches: count_iteration,
			failed: count_error,
			folded: count_folded,
			pass,
		})
	}

	/// Writes this node's read-path fold observations into the durable
	/// queue in one transaction, and answers how many scopes it queued.
	///
	/// A read that finds a (vertex, direction) scope carrying at least
	/// `graph_fold_threshold` unfolded per-edge keys records the scope in this
	/// datastore's memory and notifies
	/// [`CommitTriggers::graph_fold`](surrealdb_datastore::triggers::CommitTriggers::graph_fold)
	/// on [`Self::commit_triggers`]; reads write nothing. Every fold drain
	/// calls this first, so a node that drains the queue queues its own
	/// observations. A host whose reads are served by datastores that never
	/// drain the queue, or that may be dropped before their next drain, calls
	/// this on each of them: observations not yet flushed are lost with the
	/// datastore, and a scope still worth folding is observed again by its
	/// next read.
	///
	/// Each observed scope is a blind put of a key of its own, so the
	/// transaction contends with nothing, and every key it writes is one
	/// [`Self::is_graph_fold_queue_key`] matches. With no observation recorded
	/// since the last flush it opens no transaction and answers zero. A scope
	/// read hot again after its flush is recorded again, so a host bounds how
	/// often it calls this, not how often reads observe.
	pub async fn flush_graph_fold_observations(&self) -> Result<usize> {
		let observations = self.triggers.drain_graph_fold_observations();
		if observations.is_empty() {
			return Ok(0);
		}
		let dbs = self;
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
		txn.commit().await?;
		Ok(observations.len())
	}

	/// Drains one queued scope: bounded fold passes, each in its own
	/// transaction, retrying a bounded number of times when a concurrent
	/// edge write conflicts with a pass. Runs at most `passes` of them when a
	/// bound is given, adding each committed pass to `spent`, and reports
	/// whether the scope is drained, unfinished
	/// — the passes ran out with keys left to examine — or refused, its
	/// table's definition needing a newer release; see [`FoldOutcome`]. Each
	/// pass continues where the last committed one stopped, within this call
	/// and across calls through the fold cursor, so the passes cross the
	/// scope's keys once however many of them cannot be packed. `entries`
	/// are the queue entries naming the scope in the batch being drained,
	/// which the drain consumes once the scope is folded: a continuation
	/// taken before one of them was queued is dropped and the scope scanned
	/// from its start, so that no entry is consumed without the write it
	/// names having been examined; see [`ScopeScan`].
	///
	/// The lease `lh`, when given, is renewed before the scope is examined and
	/// before each pass; a scope reached after another node took it reports
	/// [`FoldOutcome::LeaseLost`] with no further pass run.
	async fn fold_queued_scope(
		dbs: &Arc<Datastore>,
		lh: Option<&LeaseHandler>,
		scope: &GraphFoldScope,
		entries: &[&[u8]],
		passes: Option<usize>,
		spent: &mut usize,
		canceller: &CancellationToken,
	) -> Result<FoldOutcome> {
		if let Some(lh) = lh
			&& !lh.try_maintain_lease().await?
		{
			return Ok(FoldOutcome::LeaseLost);
		}
		// A table, database or namespace removed since the entry was queued
		// leaves nothing to fold: the removal deletes the table's definition
		// and the scope's pointer keys with it. Both are read by id, so the
		// check costs two keys however large the catalog is. Pointer keys
		// under no definition are fold debt, taxing every read of the scope
		// until the table is defined or they are removed, so such a scope is
		// folded, and the fold's failure keeps its entries.
		{
			let txn = dbs.transaction(Read).await?;
			let foldable = async {
				match txn.get_tb(scope.ns, scope.db, &scope.tb, None).await {
					Ok(Some(_)) => return Ok(Some(true)),
					Ok(None) => {}
					// A table definition a newer release wrote is one this build
					// cannot fold under: the scope is left queued, with a
					// warning, for a build that can.
					Err(e) if surrealdb_datastore::tx::refused_by_release(&e) => {
						warn!(
							target: TARGET,
							"A graph fold scope was left because its table's definition needs a \
							 newer release than this build"
						);
						return Ok(None);
					}
					Err(e) => return Err(e),
				}
				let range = crate::key::schema::GraphDirPrefix {
					ns: scope.ns,
					db: scope.db,
					tb: Cow::Borrowed(&scope.tb),
					id: Cow::Borrowed(&scope.id),
					dir: scope.dir,
				}
				.range()?;
				anyhow::Ok(Some(!txn.keys_raw(range, 1, 0, None).await?.is_empty()))
			}
			.await;
			let _ = txn.cancel().await;
			match foldable? {
				Some(true) => {}
				Some(false) => {
					dbs.fold_cursor.lock().scans.remove(scope);
					return Ok(FoldOutcome::Folded);
				}
				None => return Ok(FoldOutcome::Refused),
			}
		}
		let vertex = crate::val::RecordId::new(scope.tb.clone(), scope.id.clone());
		let batch = dbs.config.idx.graph_fold_batch;
		let mut retries = 0;
		let mut done = 0;
		// The key the next pass scans after: where the last committed pass
		// stopped, in this call or an earlier one, unless an entry of this
		// batch was queued since that position was taken, in which case the
		// write it names may precede the position and the scan starts over.
		// A pass that fails to commit is retried from the same key.
		let mut after: Option<Vec<u8>> = {
			let mut cursor = dbs.fold_cursor.lock();
			match cursor.scans.get(scope) {
				Some(scan) if entries.iter().all(|entry| scan.entries.contains(*entry)) => {
					Some(scan.after.clone())
				}
				Some(_) => {
					cursor.scans.remove(scope);
					None
				}
				None => None,
			}
		};
		loop {
			Self::ensure_not_cancelled(canceller)?;
			if let Some(lh) = lh
				&& !lh.try_maintain_lease().await?
			{
				return Ok(FoldOutcome::LeaseLost);
			}
			let txn = Arc::new(dbs.transaction(Write).await?);
			// The environment the fold's doc-ID assignment reads: the
			// production `IndexEnv` is the query context, built the same way
			// the index-compaction processors build theirs.
			let mut env = dbs.setup_ctx()?;
			env.set_transaction(Arc::clone(&txn));
			let env = env.freeze();
			let outcome = match crate::idx::adjacency::fold_scope_from(
				&env,
				scope.ns,
				scope.db,
				&vertex,
				scope.dir,
				batch,
				after.as_deref(),
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
					*spent += 1;
					if !outcome.has_more {
						dbs.fold_cursor.lock().scans.remove(scope);
						return Ok(FoldOutcome::Folded);
					}
					after = outcome.resume;
					{
						let mut cursor = dbs.fold_cursor.lock();
						match &after {
							// A continuation answers for the entries it already
							// did; a scan begun at the scope's start answers for
							// this batch's.
							Some(key) => match cursor.scans.get_mut(scope) {
								Some(scan) => scan.after.clone_from(key),
								None => {
									cursor.scans.insert(
										scope.clone(),
										ScopeScan {
											after: key.clone(),
											entries: entries
												.iter()
												.map(|entry| entry.to_vec())
												.collect(),
										},
									);
								}
							},
							None => {
								cursor.scans.remove(scope);
							}
						}
					}
					done += 1;
					if passes.is_some_and(|passes| done >= passes) {
						return Ok(FoldOutcome::Unfinished);
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

	/// Runs one pass per index in `indexes`, the batch's distinct indexes in
	/// queue order, one after another on the calling task: outside any lease,
	/// under a compaction ticket that is never granted a place, so the passes
	/// spawn nothing and their parallel work runs on this task too, and with no
	/// timer. A pass that fails is logged and the batch goes on with the next
	/// index; a pass ended by cancellation ends the batch with its error.
	///
	/// On a store other datastores write to, a shard this datastore wrote into
	/// is covered by its writer lease, and no pass steps a guard under a lease
	/// that holds, so a drained shard's guard would read `NonEmpty` — and the
	/// index `compacting` — until the lease went idle and was released. So a
	/// pass that folded its index's backlog is followed by the release of this
	/// datastore's writer leases on the index and, where it released any, by
	/// one more pass, which steps the guards. A lease another datastore holds
	/// is stepped past by the first pass to reach the index once it expires.
	/// Elsewhere this datastore holds no lease, and the release finds none.
	///
	/// Returns what the passes leave the batch's cleanup to act on.
	async fn index_compaction_serial(
		&self,
		indexes: &[IndexKeyBase],
		canceller: &CancellationToken,
	) -> Result<CompactionBatchReport> {
		let places = crate::idx::places::Ticket::serial();
		let mut batch = CompactionBatchReport::default();
		for ikb in indexes {
			Self::ensure_not_cancelled(canceller)?;
			let mut report = self.run_index_compaction_pass(ikb, canceller, None, &places).await;
			let folded = matches!(
				report.result,
				Ok(CompactionPassOutcome::Empty | CompactionPassOutcome::Short)
			);
			if folded && self.release_writer_leases_on(ikb).await? > 0 {
				Self::ensure_not_cancelled(canceller)?;
				report = self.run_index_compaction_pass(ikb, canceller, None, &places).await;
			}
			batch.record(ikb.clone(), report, canceller)?;
		}
		Ok(batch)
	}

	/// Releases every writer lease this datastore holds on the index `ikb`, in
	/// every guard family, and answers how many it released. The release queues
	/// a compaction request for the index in its own transaction.
	async fn release_writer_leases_on(&self, ikb: &IndexKeyBase) -> Result<usize> {
		let index = ikb.guarded(surrealdb_datastore::pending_guard::PendingGuardFamily::Hnsw).index;
		self.transaction_factory.release_writer_leases_of(&self.sequences, &index).await
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

	/// Continues `drain`'s sweep of the defined indexes, a page at a time
	/// until a page queues a request for the drain to act on, the sweep ends,
	/// `max_batches` is reached, or `lh`, where the drain holds a lease, finds
	/// it taken by another node, each page counting one of `read`. The lease is
	/// renewed before every page, so a sweep that spans pages never queues
	/// requests once another node holds it.
	///
	/// A drain sweeps at most once, and its sweep skips every index the drain
	/// ran a pass over ([`CompactionDrain::passed`]). A sweep begins only once
	/// `period` has passed since the end of the last sweep that completed, or
	/// when none has ([`Self::sweep_page`]). A sweep that fails is logged,
	/// ends, and is not counted, so the next drain sweeps again. A lease check
	/// that fails is returned.
	async fn sweep_after_drain(
		dbs: &Datastore,
		period: Duration,
		lh: Option<&LeaseHandler>,
		canceller: &CancellationToken,
		drain: &mut CompactionDrain,
		read: &mut usize,
		max_batches: Option<usize>,
	) -> Result<Swept> {
		if matches!(drain.sweep, DrainSweep::Unbegun) {
			if dbs.compaction_swept_at.lock().is_some_and(|at| at.elapsed() < period) {
				drain.sweep = DrainSweep::Done;
				return Ok(Swept::Done);
			}
			drain.sweep = DrainSweep::Walking(Box::default());
		}
		let DrainSweep::Walking(walk) = &mut drain.sweep else {
			return Ok(Swept::Done);
		};
		loop {
			if max_batches.is_some_and(|max| *read >= max) {
				return Ok(Swept::Spent);
			}
			if let Some(lh) = lh
				&& !lh.try_maintain_lease().await?
			{
				return Ok(Swept::LeaseLost);
			}
			*read += 1;
			match dbs.sweep_page(walk, canceller, &drain.passed).await {
				Ok(queued) => {
					if !walk.has_more() {
						drain.sweep = DrainSweep::Done;
						*dbs.compaction_swept_at.lock() = Some(Instant::now());
						return Ok(if queued > 0 {
							Swept::Queued
						} else {
							Swept::Done
						});
					}
					if queued > 0 {
						return Ok(Swept::Queued);
					}
				}
				Err(e) => {
					warn!(target: TARGET, "Failed to sweep indexes for unmaterialised updates: {e}");
					drain.sweep = DrainSweep::Done;
					return Ok(Swept::Done);
				}
			}
		}
	}

	/// One page of a compaction drain's sweep: queues one compaction request
	/// for every index the next [`INDEX_COMPACTION_SWEEP_STEPS`] steps of `walk`
	/// reach that is of a queued kind, holds updates waiting to be folded, and
	/// has no request in the queue, and answers how many it queued.
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
	/// Throttled by [`Self::sweep_after_drain`]: a sweep begins at most once per
	/// the drain's sweep period (the lease period, for a leased drain), measured
	/// from the end of the last sweep that completed, so an idle store pays one
	/// existence probe per queued-kind index per period.
	/// An index a newer release wrote, or whose table, database or namespace
	/// one wrote, cannot be read by this build and is passed.
	///
	/// # Arguments
	/// * `walk` - the sweep's walk of the catalog, moved on by the page.
	/// * `canceller` - ends the page between indexes.
	/// * `passed` - indexes the sweep leaves alone: those the calling drain ran a pass over.
	async fn sweep_page(
		&self,
		walk: &mut IndexWalk,
		canceller: &CancellationToken,
		passed: &HashSet<IndexKeyBase>,
	) -> Result<usize> {
		let mut owed = Vec::new();
		{
			let txn = self.transaction(Read).await?;
			let res: Result<()> = async {
				for _ in 0..INDEX_COMPACTION_SWEEP_STEPS {
					Self::ensure_not_cancelled(canceller)?;
					let WalkedIndex {
						ns,
						db,
						index: ix,
						..
					} = match walk.next(&txn).await? {
						Some(Step::Reached(index)) => index,
						Some(Step::Passed) => continue,
						None => break,
					};
					if !ix.index.queues_compaction() {
						continue;
					}
					let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
					if passed.contains(&ikb) {
						continue;
					}
					// An index with a request queued — one a write queued, a
					// pass requeued or a drain kept — is the queue's already.
					let requested = IndexCompactionIxPrefix {
						ns,
						db,
						tb: Cow::Borrowed(&ix.table_name),
						ix: ix.index_id,
					}
					.range()?;
					if !txn.keys(requested, 1, 0, None).await?.is_empty() {
						continue;
					}
					if index_has_unmaterialised_updates(&txn, ns, db, &ix).await? {
						owed.push(ikb);
					}
				}
				Ok(())
			}
			.await;
			let _ = txn.cancel().await;
			res?;
		}
		walk.take_refused();
		let materialiser = self.materialiser()?;
		for ikb in &owed {
			Self::ensure_not_cancelled(canceller)?;
			materialiser.requeue_index_compaction(ikb).await?;
		}
		Ok(owed.len())
	}

	/// Forgets when the defined indexes were last swept, so the next empty
	/// queue sweeps them whatever the throttle.
	#[cfg(test)]
	pub(crate) fn expire_compaction_sweep(&self) {
		*self.compaction_swept_at.lock() = None;
	}

	/// The most async events an unleased batch runs at once on the task that runs it, where the
	/// datastore was built with [`Builder::with_joined_event_workers`].
	pub(crate) fn joined_event_workers(&self) -> Option<usize> {
		self.joined_event_workers
	}

	/// The entries a batch of async events fetches.
	pub(crate) fn event_batch_size(&self) -> u32 {
		self.event_batch_size
	}

	/// The node whose async event queue this datastore's writes queue under and its local drain
	/// runs: this datastore's own unless [`Builder::with_event_queue_node`] named another.
	pub(crate) fn event_queue_node(&self) -> Uuid {
		self.event_queue_node.unwrap_or(self.id)
	}

	/// The pass of [`Job::EventProcessing`](crate::kvs::tasks::Job::EventProcessing):
	/// [`Self::event_processing_local`] on a node that owns the queue its writes join, and nothing
	/// on a node that only writes to another's ([`Builder::with_event_queue_node`]), whose owner
	/// drains it.
	pub(crate) async fn event_processing_job(&self, canceller: &CancellationToken) -> Result<Pass> {
		if self.event_queue_node() != self.id {
			return Ok(Pass::done(TaskLeaseState::NotLeased));
		}
		self.event_processing_local(canceller).await
	}

	/// One bounded pass over the async events this node has queued, for a host that drives the
	/// queue itself: [`Self::event_processing_local`] without a canceller, which runs at most
	/// [`EVENT_PROCESSING_SLICE_BATCHES`] batches of [`Self::event_batch_size`] entries. The pass
	/// reports [`Progress::More`] while entries remain that a pass run at once owes a run, and the
	/// host calls again; [`Pass::ran`] counts the entries it ran.
	///
	/// [`CommitTriggers::async_event`](surrealdb_datastore::triggers::CommitTriggers::async_event)
	/// is notified after every commit that queued an event, and every key such a commit writes to
	/// queue it lies within [`Self::async_event_queue_bounds`].
	pub async fn process_async_events(&self) -> Result<Pass> {
		self.event_processing_local(&CancellationToken::new()).await
	}

	/// One bounded pass over the root-level async event queue, which a release that preceded
	/// per-node queues wrote and a host that has since moved to them still owes a run: entries
	/// queued before the move and not yet run. [`Self::process_async_events`] reads only a node's
	/// queue, so a host calls this beside it until the root queue is empty.
	///
	/// No lease is taken. Each entry is claimed by the worker about to run it, and an entry whose
	/// writer is a node that is still live is left to it, as in the adoption sweep, so the pass
	/// reports it as held and does not run it. Bounded as [`Self::process_async_events`] is, by
	/// four batches of [`Builder::with_event_batch_size`] entries, and reports [`Progress::More`]
	/// while entries remain that a pass run at once owes a run.
	pub async fn process_legacy_async_events(&self) -> Result<Pass> {
		let range = EventQueuePrefix {}.range()?;
		let mut pass = Pass::done(TaskLeaseState::NotLeased);
		let mut budget = EVENT_PROCESSING_SLICE_BATCHES;
		let canceller = CancellationToken::new();
		match self
			.drain_event_range(EventQueue::Root, &range, None, &mut budget, &canceller, &mut pass)
			.await
		{
			Ok(Drained::Backlog) => pass.progress = Progress::More,
			Ok(Drained::Clear | Drained::LeaseLost) => {}
			Err(e) => return Err(pass.failed(e)),
		}
		Ok(pass)
	}

	/// The key bounds `[start, end)` of this node's async event queue: a commit of this node that
	/// queues an `ASYNC` event writes a key within them.
	pub fn async_event_queue_bounds(&self) -> Result<(Vec<u8>, Vec<u8>)> {
		let range = NodeEventQueuePrefix {
			nd: self.event_queue_node(),
		}
		.range()?;
		Ok((range.start().as_slice().to_vec(), range.end().as_slice().to_vec()))
	}

	/// Runs at most `budget` batches of pending index builds and reports what is left.
	///
	/// A build is pending from the statement that starts it until it is published, fails or is
	/// aborted. Every step of it is a transaction that commits its own progress in the build's
	/// state, so the pass is the build resumed: it claims a pending build that no live owner holds,
	/// runs its batches from the last one committed, and hands it back when the budget is spent,
	/// for the next pass to continue at once. A pass stopped anywhere — its future dropped, its
	/// process gone — loses at most the batch it was in, and leaves the build to its owner lease,
	/// after which a pass claims it again.
	///
	/// A call walks one page of the catalog's indexes ([`IndexWalk`]) in catalog order, and
	/// continues the walk where the previous call stopped, so a call reads a bounded part of the
	/// catalog however many indexes it holds; the walk begins afresh once a call reaches its end.
	/// A call that has not reached the end reports [`IndexBuildsRun::more`], so a host that calls
	/// again while it is set has visited every index.
	///
	/// Nothing is spawned and no lease is taken: the batches run on the calling task, and passes
	/// that overlap claim each build once, the losing claim reading the build as held. Each batch
	/// is bounded as a spawned build's is, by its record count and bytes, and so is each page of
	/// the wipe that starts a restarted scan. A `COUNT` index's tail runs whole in the pass that
	/// reaches it, one transaction growing with the records written past the scan's end while it
	/// ran, and is not counted against `budget` (see `Building::spend_batch`). A pass with a
	/// budget of one still advances its build by a batch.
	///
	/// A host that builds the datastore
	/// [`without_index_build_tasks`](self::builder::Builder::without_index_build_tasks) runs this
	/// itself: when
	/// [`CommitTriggers::index_build`](surrealdb_datastore::triggers::CommitTriggers::index_build)
	/// on [`Self::commit_triggers`] is notified, when a commit wrote a key
	/// [`Self::is_index_build_state_key`] matches, again while [`IndexBuildsRun::more`] is set, and
	/// after [`IndexBuildsRun::retry_after`].
	pub async fn run_index_builds(&self, budget: usize) -> Result<IndexBuildsRun> {
		let mut walk = self.build_walk.lock().take().unwrap_or_default();
		let run = self.run_index_builds_page(&mut walk, budget).await;
		let unwalked = walk.has_more();
		if unwalked {
			*self.build_walk.lock() = Some(walk);
		}
		let mut run = run?;
		run.more |= unwalked;
		Ok(run)
	}

	/// The page of `walk` that [`Self::run_index_builds`] runs: the candidates it does not reach,
	/// because a build is left that a pass may claim, are given back to `walk`.
	async fn run_index_builds_page(
		&self,
		walk: &mut IndexWalk,
		budget: usize,
	) -> Result<IndexBuildsRun> {
		let page = self.resume_page(walk).await?;
		let skipped = walk.take_refused();
		if skipped > 0 {
			warn!(
				target: TARGET,
				skipped,
				"A pass of index builds skipped catalog entries that need a newer release than \
				 this build"
			);
		}
		let mut left = budget;
		let mut run = IndexBuildsRun {
			batches: 0,
			more: false,
			retry_after: None,
		};
		let mut candidates = page.into_iter();
		while let Some(candidate) = candidates.next() {
			// The index as the catalog holds it now; a candidate that no longer names the index
			// the walk reached is dropped.
			let Some(ix) = self.resume_candidate_as_defined(&candidate).await? else {
				continue;
			};
			let ctx = self.setup_ctx()?.freeze();
			let opt = self.setup_options(
				&Session::owner().with_ns(&candidate.ns_name).with_db(&candidate.db_name),
			);
			let pass = match self
				.index_builder
				.run_pass(
					&ctx,
					opt,
					candidate.ns,
					candidate.db,
					candidate.tb,
					Arc::clone(&ix),
					left,
				)
				.await
			{
				Ok(pass) => pass,
				Err(err) if crate::kvs::is_shutdown_error(&err) => {
					walk.give_back(std::iter::once(candidate).chain(candidates));
					return Err(err);
				}
				Err(err) => {
					warn!(
						target: TARGET,
						"Failed to run a pass of index build '{}' on table '{}': {err}",
						ix.name,
						ix.table_name
					);
					continue;
				}
			};
			match pass {
				BuildPass::Idle | BuildPass::Held(None) => {}
				BuildPass::Pending => {
					run.more = true;
					walk.give_back(std::iter::once(candidate).chain(candidates));
					break;
				}
				BuildPass::Held(Some(after)) => {
					run.retry_after = Some(run.retry_after.map_or(after, |at| at.min(after)));
				}
				BuildPass::Ran {
					batches,
					finished,
				} => {
					run.batches += batches;
					left = left.saturating_sub(batches);
					run.more |= !finished;
				}
			}
		}
		Ok(run)
	}

	/// Runs at most `budget` steps of the index compaction drain on the calling task, and reports
	/// what is left.
	///
	/// Full-text, `COUNT` and HNSW writes leave a backlog beside the read-optimised structure, and
	/// queue a request naming the index (a key [`Self::is_index_compaction_key`] matches). The
	/// drain reads the queue a batch of [`INDEX_COMPACTION_QUEUE_BATCH_SIZE`] requests at a time,
	/// runs one compaction pass over each index a batch names, and then deletes the batch's
	/// requests the passes acted on in a transaction of its own. A step is one such batch, or one
	/// page of the sweep below; a call with a budget of one still reads a batch. Every plan a pass
	/// applies is a transaction conditional on the index's compaction generation, so a call stopped
	/// anywhere — its future dropped, its process gone — loses nothing: a request is deleted only
	/// after its index was compacted at or after it was written, and a plan that lost its race is
	/// read again. Reads merge the backlog whatever a pass has folded, so a pass changes what a
	/// read costs and what `INFO FOR INDEX` reports as `compacting`, never what a read answers.
	/// read again. Reads merge the backlog whatever a pass has folded, so a pass changes what a
	/// read costs and what `INFO FOR INDEX` reports as `compacting`, never what a read answers.
	///
	/// Nothing is spawned, no lease is taken and no timer runs: the passes run one after another on
	/// the calling task, and their parallel work runs there too. The call continues the drain where
	/// the previous one stopped, so successive calls rotate through every queued index. A drain
	/// that reads the queue empty sweeps the defined indexes for backlogs no request names, which
	/// queues a request for each: the first such drain after the datastore starts, and then at most
	/// once per `sweep_period`, measured from the end of the last sweep that completed, each page
	/// of the sweep a step. A drain ends once its sweep queues nothing, and the next call begins
	/// a drain of its own.
	///
	/// A host that builds the datastore
	/// [`without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks) runs this
	/// itself: when
	/// [`CommitTriggers::index_compaction`](surrealdb_datastore::triggers::CommitTriggers::index_compaction)
	/// on [`Self::commit_triggers`] is notified, which follows every commit that queued a request,
	/// when a commit wrote a key [`Self::is_index_compaction_key`] matches, again while
	/// [`IndexCompactionRun::more`] is set, and after [`IndexCompactionRun::retry_after`]. A
	/// request a pass could not act on — an HNSW index a live build holds, or an index whose
	/// defining statement has not committed — stays queued, and the build or definition that holds
	/// it writes the commit that announces it again. A pass that fails on anything else is logged,
	/// and its index waits for its next write or the next sweep.
	pub async fn run_index_compaction(
		self: &Arc<Self>,
		budget: usize,
		sweep_period: Duration,
	) -> Result<IndexCompactionRun> {
		let drained = Self::index_compaction_drain(
			Arc::clone(self),
			None,
			sweep_period,
			CancellationToken::new(),
			Some(budget.max(1)),
		)
		.await?;
		Ok(IndexCompactionRun {
			passes: drained.passes,
			more: drained.pass.progress == Progress::More,
			retry_after: drained.stalled.then_some(INDEX_COMPACTION_CLEANUP_RETRY),
		})
	}

	/// Whether `key` is a request of the index compaction queue (`/!ic`), which a commit that
	/// writes to a full-text, `COUNT` or HNSW index puts, as does a compaction pass that stops with
	/// work left and a sweep that finds a backlog no request names. A commit that wrote no such
	/// key queued no compaction work.
	pub fn is_index_compaction_key(key: &[u8]) -> bool {
		IndexCompactionKey::decode_key(key).is_ok()
	}

	/// Whether `key` is the key of an index's build state, which every statement that starts an
	/// index build writes, as does every batch of the build. A commit that wrote no such key
	/// started no build.
	pub fn is_index_build_state_key(key: &[u8]) -> bool {
		crate::key::schema::BuildStateKey::decode_key(key).is_ok()
	}

	/// Runs at most `budget` fold passes over the graph fold queue and reports what is left.
	///
	/// The queue holds one entry per (vertex, direction) scope whose per-edge adjacency keys are
	/// worth packing into blocks: a read that found a scope over `graph_fold_threshold` unfolded
	/// keys, once its observation is flushed ([`Self::flush_graph_fold_observations`], which the
	/// pass runs first for this datastore's own), and an edge delete on a table already folded,
	/// whose tombstone every read of the scope subtracts until a fold consumes it. A zero
	/// threshold stops the first and never the second, so a host runs this pass whatever the
	/// threshold.
	///
	/// A fold pass is one transaction over at most `graph_fold_batch` of a scope's keys that
	/// commits its own progress, and a call runs at most `budget` of them, at most
	/// [`GRAPH_FOLD_SLICE_PASSES_PER_SCOPE`] on any one scope, reading at most `budget` batches of
	/// queue entries. A scope whose fold outlasts its passes, or is reached with the budget spent,
	/// keeps its entries and the call ends at its batch; the next call continues where the last
	/// committed pass stopped, after the scopes the rotation has yet to reach. A call is cancelled
	/// by dropping its future: stopped anywhere — its future dropped, its process gone — it loses
	/// at most the fold pass it was in. The entries of the scopes a batch folded are deleted in a
	/// bounded write transaction of its own. [`GraphFoldRun::more`] is set when the budget ran out
	/// with entries left, a scope's fold outlasted its passes, or a batch's cleanup kept losing
	/// conflicts; a host calls again while it is set. A scope whose fold fails keeps its entries
	/// and is counted in [`GraphFoldRun::failed`] without setting `more`: the next pass any wake
	/// starts retries it, and a pass run at once would fail it again.
	///
	/// Nothing is spawned and no lease is taken: the caller is the only one driving the queue, as
	/// a host that builds the datastore
	/// [`without_maintenance_tasks`](self::builder::Builder::without_maintenance_tasks) and runs
	/// these passes on one node is. It runs a pass when
	/// [`CommitTriggers::graph_fold`](surrealdb_datastore::triggers::CommitTriggers::graph_fold) on
	/// [`Self::commit_triggers`] is notified, when a commit wrote a key
	/// [`Self::is_graph_fold_queue_key`] matches, and again while [`GraphFoldRun::more`] is set. A
	/// pass with nothing queued and nothing observed reads one empty batch and writes nothing.
	pub async fn run_graph_fold(self: &Arc<Self>, budget: usize) -> Result<GraphFoldRun> {
		let budget = budget.max(1);
		let drain = Self::graph_fold_drain(
			Arc::clone(self),
			None,
			CancellationToken::new(),
			Some(budget),
			Some(GRAPH_FOLD_SLICE_PASSES_PER_SCOPE),
			Some(budget),
		)
		.await?;
		Ok(GraphFoldRun {
			batches: drain.batches,
			scopes: drain.folded,
			failed: drain.failed,
			more: drain.pass.progress == Progress::More,
		})
	}

	/// Whether `key` is an entry of the graph fold queue. Every write that gives
	/// [`Self::run_graph_fold`] work puts such a key: an observation's flush, and an edge delete
	/// on a folded table. A pass deletes the entries it consumes.
	pub fn is_graph_fold_queue_key(key: &[u8]) -> bool {
		GraphFoldKey::decode_key(key).is_ok()
	}

	/// The key bounds `[start, end)` of the graph fold queue: every key
	/// [`Self::is_graph_fold_queue_key`] matches lies within them.
	pub fn graph_fold_queue_bounds() -> Result<(Vec<u8>, Vec<u8>)> {
		let range = GraphFoldPrefix {}.range()?;
		Ok((range.start().as_slice().to_vec(), range.end().as_slice().to_vec()))
	}

	/// Runs the async events this node has queued, without a lease.
	///
	/// The queue under this node's id is written only by this node's own
	/// commits, so nothing has to be coordinated to drain it: every node drains
	/// its own queue, woken by the commit that filled it, and the cluster's
	/// event throughput scales with its node count rather than serialising
	/// through one lease holder. Entries of a node that dies are adopted by
	/// [`Self::event_processing_adopt`] once the node is no longer live.
	///
	/// One pass runs at most [`EVENT_PROCESSING_SLICE_BATCHES`] full batches,
	/// resuming where the previous pass over the queue stopped, and reports
	/// [`Progress::More`] when the last of them was full. Returns as
	/// soon as `canceller` is tripped, at the next batch boundary: the events
	/// themselves are user-defined SurrealQL that nothing here bounds, so this
	/// bounds the wait to one batch rather than to the whole queue — which is
	/// what stops a busy queue from holding [`Self::shutdown`] open for as long
	/// as writes keep arriving. Cancellation is not a failure: the queue is
	/// durable, so whatever is left is processed by the next pass on this node,
	/// or adopted. A pass that fails — on a batch whose entry could not be
	/// claimed or settled, see [`crate::doc::process_next_events_batch`] —
	/// attaches what it did to its error with [`Pass::failed`]: the batches
	/// settled before, which stand, and a backlog, since the entry left in
	/// place is still queued.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, canceller)
		)
	)]
	pub(crate) async fn event_processing_local(
		&self,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		trace!(target: TARGET, "Running event processing for this node's queue");
		let range = NodeEventQueuePrefix {
			nd: self.event_queue_node(),
		}
		.range()?;
		let mut pass = Pass::done(TaskLeaseState::NotLeased);
		let mut budget = EVENT_PROCESSING_SLICE_BATCHES;
		let queue = EventQueue::Node(self.event_queue_node());
		// Unleased, so the lease cannot pass elsewhere.
		match self.drain_event_range(queue, &range, None, &mut budget, canceller, &mut pass).await {
			Ok(Drained::Backlog) => pass.progress = Progress::More,
			Ok(Drained::Clear | Drained::LeaseLost) => {}
			Err(e) => return Err(pass.failed(e)),
		}
		Ok(pass)
	}

	/// Runs the async events left by nodes that can no longer run them, under
	/// the [`TaskLeaseType::EventProcessing`] lease.
	///
	/// Two queues qualify: the per-node queue of every node whose owner does
	/// not drain it — archived, with no row at all, or with a row kept live by
	/// a release that queues at the root, as a node rolled back to one with its
	/// id kept is (see [`NodeState::owned`]) — and the root-level queue that
	/// nodes on such a release write. Owners are found from the queues
	/// themselves, not from node rows, so a queue outlives its owner's row
	/// however that row went. One node in the cluster sweeps them per
	/// interval.
	///
	/// Each entry is claimed by the worker about to run it, immediately before
	/// its body is evaluated, and deleted in the same transaction as its
	/// event's writes, only while it still reads as claimed, so an owner still
	/// alive after being archived and the adopter that read its queue neither
	/// both evaluate an event nor both land its writes; see
	/// [`crate::doc::process_next_events_batch`] for how long a claim holds.
	///
	/// Bounded like [`Self::event_processing_local`]: at most
	/// [`EVENT_PROCESSING_SLICE_BATCHES`] fetches that find an entry per pass
	/// across every queue swept, reporting [`Progress::More`] when the budget
	/// ran out or a queue still owes an entry a run. The sweep walks the
	/// per-node state in rotations that cover the root queue and every node
	/// once, adopting queues as it finds them, and a pass stops once that
	/// budget is spent or it has stepped over [`EVENT_ADOPTION_PAGE_SIZE`]
	/// nodes, so a pass reads in proportion to the queues it drains and a page
	/// of the nodes it steps over, not to every owner in the cluster: a backlog
	/// of many short queues costs one walk over them in total, not one per
	/// pass. A rotation left unfinished at a page continues on the next pass,
	/// run at once, and the sweep goes idle when the rotation ends, so a large
	/// live cluster costs one walk per interval however many pages it spans.
	/// A pass that spends its budget ends the rotation, and the next begins one
	/// after the queue it was spent in and ending with it, which keeps a queue
	/// refilled as fast as it drains from holding the sweep on itself; each
	/// queue is drained from where the last pass over it stopped. That next
	/// pass is due at once only if this one got somewhere: an entry ran, or a
	/// queue still owes one a run. A budget spent on short fetches that ran
	/// nothing — entries a newer release wrote, or another processor holds,
	/// in as many queues as the budget has fetches — would be spent the same
	/// way by a pass run at once, so the sweep goes idle until its interval
	/// and resumes the rotation there. A pass that fails attaches what it did
	/// to its error with [`Pass::failed`], as the local drain does, under the
	/// lease it held: the queues drained before the failure are settled. Its
	/// rotation is kept, past the queue it failed in, so the next pass
	/// continues with the queues after it and a queue whose drain keeps
	/// failing cannot hold the sweep at itself.
	///
	/// A pass that finds the lease passed to another node stops there, at
	/// the batch that found it so, which hands out no further entry, and
	/// reports [`TaskLeaseState::Elsewhere`] with what it settled until then:
	/// the new holder sweeps from here, so the two nodes do not both adopt
	/// queues. The rotation is kept for this node's next pass to continue,
	/// due at the interval rather than at once.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, canceller)
		)
	)]
	pub(crate) async fn event_processing_adopt(
		&self,
		interval: Duration,
		canceller: &CancellationToken,
	) -> Result<Pass> {
		trace!(target: TARGET, "Attempting event adoption");
		let lh =
			self.lease_handler(TaskLeaseType::EventProcessing, interval * 2, Some(canceller))?;
		if !lh.has_lease().await? {
			return Ok(Pass::elsewhere());
		}
		trace!(target: TARGET, "Running event adoption");
		let rotation = match self.adoption_rotation.lock().take() {
			Some(rotation) => rotation,
			None => AdoptionRotation::fresh()?,
		};
		let AdoptionRotation {
			mut legs,
			swept,
		} = rotation;
		let mut sweep = AdoptionSweep {
			lh: &lh,
			canceller,
			budget: EVENT_PROCESSING_SLICE_BATCHES,
			steps: EVENT_ADOPTION_PAGE_SIZE,
			pass: Pass::done(TaskLeaseState::Held),
			swept,
			failed: None,
		};
		let root = EventQueuePrefix {}.range()?;
		let mut stop = None;
		while let Some(leg) = legs.front_mut() {
			let res = match leg {
				AdoptionLeg::Root => self.adopt_queue(EventQueue::Root, &root, &mut sweep).await,
				AdoptionLeg::Nodes {
					window,
					until,
				} => self.adopt_node_queues(window, *until, &mut sweep).await,
			};
			stop = match res {
				Ok(stop) => stop,
				Err(e) => {
					// The rotation outlives the failure, past the queue it
					// failed in: the node walk has already stepped past that
					// queue's node, and a root leg that failed is dropped. The
					// next pass continues with the queues after it, and the next
					// rotation comes back to it with its lap kept, so a queue
					// whose drain keeps failing is retried once a rotation
					// rather than holding the sweep at itself.
					if sweep.failed == Some(EventQueue::Root) {
						legs.pop_front();
					}
					*self.adoption_rotation.lock() = Some(AdoptionRotation {
						legs,
						swept: sweep.swept,
					});
					return Err(sweep.pass.failed(e));
				}
			};
			if stop.is_some() || canceller.is_cancelled() {
				break;
			}
			legs.pop_front();
		}
		let next = match stop {
			Some(SweepStop::Budget(queue)) => {
				// Spent: the next pass begins a rotation after this queue and
				// ending with it, and resumes it where this one stopped when
				// the rotation comes back round. It is due at once if this
				// pass ran anything; a queue that owes an entry a run has
				// reported so already, and a budget spent on nothing that ran
				// would be spent so again.
				if sweep.pass.ran > 0 {
					sweep.pass.progress = Progress::More;
				}
				Some(AdoptionRotation::after(queue).map_err(|e| sweep.pass.failed(e))?)
			}
			Some(SweepStop::Page) => {
				// The page is walked and the rotation is not: the next pass
				// continues it at once, so the rotation ends however many
				// pages it spans, and the sweep goes idle when it does.
				sweep.pass.progress = Progress::More;
				Some(AdoptionRotation {
					legs,
					swept: sweep.swept,
				})
			}
			Some(SweepStop::LeaseLost) => {
				// The new holder sweeps from here. A pass run at once would
				// find the lease elsewhere, so the next is the interval's.
				sweep.pass.lease = TaskLeaseState::Elsewhere;
				sweep.pass.progress = Progress::Done;
				Some(AdoptionRotation {
					legs,
					swept: sweep.swept,
				})
			}
			None if canceller.is_cancelled() => Some(AdoptionRotation {
				legs,
				swept: sweep.swept,
			}),
			None => {
				// The rotation visited every queue there is, so a lap for any
				// other queue — its owner live again, or its entries gone — is
				// stale. The local queue's lap belongs to the local drain.
				self.event_laps.lock().retain(|queue, _| {
					*queue == EventQueue::Node(self.event_queue_node())
						|| sweep.swept.contains(queue)
				});
				None
			}
		};
		*self.adoption_rotation.lock() = next;
		Ok(sweep.pass)
	}

	/// Drains `queue` for the sweep. Returns why the sweep stops in it: the
	/// lease found passed to another node, or the sweep's budget spent on it.
	async fn adopt_queue(
		&self,
		queue: EventQueue,
		range: &TypedRange<AsyncEventRecord>,
		sweep: &mut AdoptionSweep<'_>,
	) -> Result<Option<SweepStop>> {
		sweep.swept.insert(queue);
		let drained = self
			.drain_event_range(
				queue,
				range,
				Some(sweep.lh),
				&mut sweep.budget,
				sweep.canceller,
				&mut sweep.pass,
			)
			.await;
		match drained.inspect_err(|_| sweep.failed = Some(queue))? {
			Drained::LeaseLost => return Ok(Some(SweepStop::LeaseLost)),
			// Entries remain in this queue that the next pass owes a run.
			Drained::Backlog => sweep.pass.progress = Progress::More,
			Drained::Clear => {}
		}
		Ok((sweep.budget == 0).then_some(SweepStop::Budget(queue)))
	}

	/// Walks the per-node state from `window`'s start, one node at a time and
	/// in id order, up to and including node `until`'s when given, and drains
	/// the queue of every node found whose owner does not drain it, until the
	/// sweep's fetch budget is spent, its page of nodes is walked, the lease
	/// passes to another node, or the pass is cancelled.
	/// Returns why the pass stopped when it did not finish the leg, whose
	/// window is then advanced past the last node stepped over, or, for a
	/// lease found lost in a node's queue, left at that node.
	///
	/// The queue sorts first within a node's state, so the first key under a
	/// node is a queue entry exactly when the node has a queue; the walk then
	/// resumes past the node's whole subtree. One read per node with any state,
	/// one more for the row of a node with a queue, and one more for the drain
	/// heartbeat beside a row that is not archived. The reads share a
	/// transaction until a drain, which runs user-defined events of unbounded
	/// duration; the step after it opens a fresh one, so no read transaction
	/// outlives the events.
	async fn adopt_node_queues(
		&self,
		window: &mut RawRange,
		until: Option<Uuid>,
		sweep: &mut AdoptionSweep<'_>,
	) -> Result<Option<SweepStop>> {
		let mut txn: Option<Transaction> = None;
		loop {
			if sweep.canceller.is_cancelled() {
				break;
			}
			let t = match txn.take() {
				Some(t) => t,
				None => self.transaction(Read).await?,
			};
			// The window as it stood before this node, which a pass that finds
			// the lease lost in the node's queue is left at, so the rotation
			// resumes at the queue it stopped in.
			let at = window.clone();
			let Some(state) = catch!(t, self.next_node_state(&t, window).await) else {
				let _ = t.cancel().await;
				break;
			};
			if until.is_some_and(|until| state.nd > until) {
				let _ = t.cancel().await;
				break;
			}
			sweep.steps = sweep.steps.saturating_sub(1);
			let queue = EventQueue::Node(state.nd);
			// The local queue is the local drain's, lap included, so the local
			// node costs a step and nothing more.
			let local = state.nd == self.event_queue_node();
			if !local && state.queued && state.owned {
				// An owned queue is its owner's to drain, so a lap this sweep
				// left on it is stale.
				self.event_laps.lock().remove(&queue);
			}
			if !local && state.queued && !state.owned {
				let _ = t.cancel().await;
				let range = NodeEventQueuePrefix {
					nd: state.nd,
				}
				.range()?;
				match self.adopt_queue(queue, &range, sweep).await? {
					Some(SweepStop::LeaseLost) => {
						*window = at;
						return Ok(Some(SweepStop::LeaseLost));
					}
					Some(stop) => return Ok(Some(stop)),
					None => {}
				}
			} else {
				txn = Some(t);
			}
			if sweep.steps == 0 {
				// The page is walked: the next pass resumes after this node.
				if let Some(t) = txn.take() {
					let _ = t.cancel().await;
				}
				return Ok(Some(SweepStop::Page));
			}
		}
		if let Some(t) = txn {
			let _ = t.cancel().await;
		}
		Ok(None)
	}

	/// The first node with state in `window`, which is then moved past that
	/// node's whole subtree; `None` once the window holds no node.
	///
	/// A key beneath a node that this release cannot decode — a newer
	/// release's state, left by a node since rolled back — still names its
	/// node, which the level puts before anything beneath it. Such a key is
	/// placed against the node's queue: before it, the walk resumes at the
	/// queue, so a queue behind such keys is still found; in it, the node has
	/// a queue, whose entries the drain reads for itself; past it, the node
	/// has none. A node therefore costs at most two reads however many such
	/// keys it holds, and the page bound holds. A key too short to name a
	/// node is outside what the level describes and is stepped over alone.
	async fn next_node_state(
		&self,
		txn: &Transaction,
		window: &mut RawRange,
	) -> Result<Option<NodeState>> {
		loop {
			let keys = txn.keys_raw(window.clone(), 1, 0, None).await?;
			let Some(key) = keys.first() else {
				return Ok(None);
			};
			let (nd, queued) = match NodeEventQueueKey::decode_key(key) {
				Ok(k) => (k.nd, true),
				Err(_) => match NodeLiveQueryKey::decode_key(key) {
					Ok(k) => (k.nd, false),
					Err(_) => {
						let Some(nd) = node_of_state_key(key)? else {
							*window = window.clone().resume_after(key, Direction::Forward);
							continue;
						};
						let queue = NodeEventQueuePrefix {
							nd,
						}
						.range()?;
						if key.as_slice() < queue.start().as_ref() {
							*window =
								window.clone().resume_after(queue.start(), Direction::Forward);
							continue;
						}
						(nd, key.as_slice() < queue.end().as_ref())
					}
				},
			};
			let subtree = NodeStateRoot {
				nd,
			}
			.raw_range()?;
			*window = window.clone().resume_after(subtree.end(), Direction::Forward);
			let owned = queued && self.queue_owned(txn, nd).await?;
			return Ok(Some(NodeState {
				nd,
				queued,
				owned,
			}));
		}
	}

	/// Whether node `nd`'s queue has an owner that drains it; see
	/// [`NodeState::owned`]. One read for the row, and one for the drain
	/// heartbeat beside a row that is not archived.
	async fn queue_owned(&self, txn: &Transaction, nd: Uuid) -> Result<bool> {
		let Some(node) = txn.get_node(nd).await?.filter(|node| !node.is_archived()) else {
			return Ok(false);
		};
		let stamp = NodeDrainHeartbeatKey {
			nd,
		};
		Ok(txn.get_key(&stamp, None).await?.is_some_and(|beat| beat >= node.heartbeat.value))
	}

	/// When the last node on a release that drains per-node event queues shuts
	/// down, moves any remaining [`NodeEventQueueKey`] entries back to the
	/// root-level [`EventQueueKey`] range with `claim: None` (encoded at
	/// revision 1), so a cluster rolled back to an older release can still find
	/// and drain them.
	async fn hand_back_event_queues_if_last_upgraded(&self) -> Result<()> {
		use crate::key::schema::EventQueueKey;

		let txn = self.transaction(Read).await?;
		let queued_nodes: Result<Vec<Uuid>> = async {
			let mut window = NodeStatePrefix {}.raw_range()?;
			let mut nodes = Vec::new();
			while let Some(state) = self.next_node_state(&txn, &mut window).await? {
				if state.queued {
					if state.owned && state.nd != self.id {
						return Ok(Vec::new());
					}
					nodes.push(state.nd);
				}
			}
			if nodes.is_empty() {
				return Ok(Vec::new());
			}
			for node in txn.all_nodes().await?.iter() {
				if node.id == self.id || node.is_archived() {
					continue;
				}
				let stamp = NodeDrainHeartbeatKey {
					nd: node.id,
				};
				if txn.get_key(&stamp, None).await?.is_some_and(|beat| beat >= node.heartbeat.value)
				{
					return Ok(Vec::new());
				}
			}
			Ok(nodes)
		}
		.await;
		let _ = txn.cancel().await;
		let queued_nodes = queued_nodes?;
		for nd in queued_nodes {
			let range = NodeEventQueuePrefix {
				nd,
			}
			.range()?;
			loop {
				let txn = self.bookkeeping_transaction().await?;
				let entries =
					catch!(txn, txn.scan_raw(range.clone(), NORMAL_BATCH_SIZE, 0, None).await);
				if entries.is_empty() {
					let _ = txn.cancel().await;
					break;
				}
				let full = entries.len() >= NORMAL_BATCH_SIZE as usize;
				for (k, v) in entries {
					let Ok(key) = NodeEventQueueKey::decode_key(&k) else {
						continue;
					};
					let Ok(mut record) = <AsyncEventRecord as KVValue>::kv_decode_value(&v, ())
					else {
						continue;
					};
					record.claim = None;
					let legacy = EventQueueKey {
						ns: key.ns,
						db: key.db,
						tb: Cow::Owned(key.tb.into_owned()),
						ev: Cow::Owned(key.ev.into_owned()),
						ts: key.ts,
						node_id: key.nd,
					};
					catch!(txn, txn.put_key(&legacy, &record).await);
					catch!(txn, txn.del(Key::from(k.as_slice())).await);
				}
				catch!(txn, txn.commit().await);
				if !full {
					break;
				}
			}
		}
		Ok(())
	}

	/// Runs batches of `range`, which is `queue`'s, until one is short, the
	/// pass is cancelled, `budget` is spent, or the lease `lh` passes to
	/// another node. Returns [`Drained::Backlog`] when the queue still holds
	/// entries the next pass owes a run: the budget ran out with entries
	/// beyond the last fetched, or an attempt in this lap failed and was
	/// requeued for another. Returns [`Drained::LeaseLost`] when a batch found
	/// the lease passed to another node: the lap is kept where that batch
	/// began, so the entries it fetched but did not hand out are not stepped
	/// over should this node hold the lease again. Retries are immediate, without backoff, so a
	/// requeue is a backlog however short the batch it came from, and the
	/// entry gets its next attempt on the pass run at once rather than at the
	/// next tick. The lap remembers the requeue across the passes it spans, so
	/// one in the batch that spent the last of a pass's budget is still a
	/// backlog when a later pass finds the queue's end.
	///
	/// The drain continues the lap the previous pass over the queue left, and
	/// each fetch resumes after the last entry of the one before, so entries
	/// this binary cannot run — left in place for a node that can — are
	/// stepped over once per lap rather than read again on every pass, and the
	/// entries behind them are reached. Every fetch that finds an entry spends
	/// one unit of budget, full or not, which is what bounds a pass however
	/// many entries it cannot run and however many short queues it sweeps; a
	/// queue that turns out empty costs nothing. A short batch is the queue's
	/// end and ends the lap. So does passing the last key the queue held when
	/// the lap began: the next lap starts from the head, so entries requeued
	/// for another attempt and entries queued since for an earlier-sorting
	/// event get their turn however fast the queue's tail grows. A lap of the
	/// local queue during which this node queued an event ends the same way,
	/// at the head, since the entry can sit behind the scan's position (see
	/// [`EventLap::queued`]); the drain ends with a lap during which nothing
	/// was queued, so a chain of events runs to its end within one drain
	/// rather than one hop per pass, and entries this binary cannot run are
	/// read again only when something was queued behind them.
	///
	/// A batch that fails ends the drain with its error, after the pass has
	/// accounted the batch's report — the entries it ran are settled — so a
	/// pass that fails still reports what it did; see
	/// [`crate::doc::FailedBatch`].
	async fn drain_event_range(
		&self,
		queue: EventQueue,
		range: &TypedRange<AsyncEventRecord>,
		lh: Option<&LeaseHandler>,
		budget: &mut usize,
		canceller: &CancellationToken,
		pass: &mut Pass,
	) -> Result<Drained> {
		// Only the local drain can be handed entries by the events it runs, or
		// by a concurrent write on this node; see [`EventLap::queued`]. The
		// count is read before the queue, so an entry queued between the two
		// reads as queued during the lap.
		let own = queue == EventQueue::Node(self.event_queue_node());
		let queued = || self.commit_triggers().async_events_queued();
		let resumed = self.event_laps.lock().get(&queue).cloned();
		let mut lap = match resumed {
			Some(lap) => lap,
			None => {
				let queued = queued();
				match self.queue_end(range).await? {
					Some(end) => EventLap {
						after: None,
						end,
						requeued: false,
						queued,
					},
					// Nothing queued.
					None => return Ok(Drained::Clear),
				}
			}
		};
		loop {
			if canceller.is_cancelled() || *budget == 0 {
				self.event_laps.lock().insert(queue, lap);
				return Ok(Drained::owed(*budget == 0));
			}
			if lap.after.as_ref().is_some_and(|after| *after > lap.end) {
				// Past the end the lap began with: the head's turn.
				let queued = queued();
				let Some(end) = self.queue_end(range).await? else {
					self.event_laps.lock().remove(&queue);
					return Ok(Drained::Clear);
				};
				lap = EventLap {
					after: None,
					end,
					requeued: false,
					queued,
				};
			}
			let window = match &lap.after {
				Some(after) => range.clone().resume_after(after, Direction::Forward),
				None => range.clone(),
			};
			let batch = match process_next_events_batch(self, &window, lh).await {
				Ok(batch) => batch,
				Err(e) => {
					if let Some(failed) = e.downcast_ref::<FailedBatch>() {
						Self::account_event_batch(pass, &failed.0);
					}
					return Err(e);
				}
			};
			Self::account_event_batch(pass, &batch);
			if batch.fetched > 0 {
				*budget = budget.saturating_sub(1);
			}
			lap.requeued |= batch.requeued > 0;
			if batch.lease_lost {
				self.event_laps.lock().insert(queue, lap);
				return Ok(Drained::LeaseLost);
			}
			let full = batch.is_full(self.event_batch_size());
			let Some(last) = batch.last_key.filter(|_| full) else {
				let queued = queued();
				if !own || queued == lap.queued {
					self.event_laps.lock().remove(&queue);
					return Ok(Drained::owed(lap.requeued));
				}
				// This node queued an entry during the lap, possibly behind
				// the scan: the head's turn, and the drain ends with a lap
				// during which nothing was queued.
				let Some(end) = self.queue_end(range).await? else {
					self.event_laps.lock().remove(&queue);
					return Ok(Drained::Clear);
				};
				lap = EventLap {
					after: None,
					end,
					requeued: false,
					queued,
				};
				continue;
			};
			lap.after = Some(last);
		}
	}

	/// The last key `range` holds, or `None` when it holds nothing.
	async fn queue_end(&self, range: &TypedRange<AsyncEventRecord>) -> Result<Option<Vec<u8>>> {
		self.range_end(range.clone()).await
	}

	/// The last key `range` holds, or `None` when it holds nothing.
	async fn range_end<V>(&self, range: TypedRange<V>) -> Result<Option<Vec<u8>>> {
		let txn = self.transaction(Read).await?;
		let res = txn.keysr(range, 1, 0, None).await;
		let _ = txn.cancel().await;
		Ok(res?.into_iter().next())
	}

	/// How long [`Self::drain_async_events`] waits before looking again at
	/// entries another processor holds.
	const EVENT_DRAIN_HELD_WAIT: Duration = Duration::from_millis(50);

	/// Runs every async event this node has queued, to empty, and returns how
	/// many entries it fetched that were this drain's to run.
	///
	/// The event task does this on its own after every commit that queues an
	/// event; this is for an embedder or a test that needs the queue drained
	/// now rather than on that task's schedule. Events queued by other nodes
	/// are not run: those belong to their owners, or to the leased adoption
	/// sweep once their owners are archived.
	///
	/// Entries a concurrent processor holds — the event task's workers, or
	/// another caller of this method — are that processor's to settle, and
	/// their bodies may still be running, so this waits for them to be
	/// settled or their claims to expire, and looks again: it returns only
	/// once a lap over the whole queue found nothing held, which is what
	/// makes it a flush of the queue rather than of this drain's own share.
	/// A lap can span passes — each pass is budgeted, and a pass that spends
	/// its budget leaves the lap's cursor for the next — so what a pass found
	/// held is carried across the passes of its lap rather than read from the
	/// pass that finds the queue's end, which sees only what lies past the
	/// cursor. And a lap in progress when this begins, left by a pass of the
	/// event task or of an earlier caller, is finished from where it stood:
	/// what its earlier passes found held is not this drain's to know, so it
	/// does not end the drain, whatever the passes that finish it find.
	/// Entries this binary cannot decode are not waited for; they are a
	/// newer release's to run. An entry left in place by an error — its
	/// claim's or its settlement's transaction failing — fails the pass and
	/// so this, rather than being counted and passed over, so `Ok` means the
	/// queue was flushed.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::ds", skip(self))
	)]
	pub async fn drain_async_events(&self) -> Result<u64> {
		let canceller = CancellationToken::new();
		let queue = EventQueue::Node(self.event_queue_node());
		let mut items = 0;
		loop {
			// The passes up to the next rest finish the lap in progress, if
			// there is one, and otherwise run one from the head.
			let from_head = !self.event_laps.lock().contains_key(&queue);
			let mut held = 0;
			loop {
				let pass = self.event_processing_local(&canceller).await?;
				items += pass.items.saturating_sub(pass.held);
				held += pass.held;
				if pass.progress != Progress::More {
					break;
				}
			}
			if held > 0 {
				sleep(Self::EVENT_DRAIN_HELD_WAIT).await;
			} else if from_head {
				return Ok(items);
			}
		}
	}

	/// Adds one fetched batch to a pass's report.
	fn account_event_batch(pass: &mut Pass, batch: &EventBatch) {
		pass.items += batch.fetched as u64;
		pass.held += batch.held as u64;
		pass.ran += batch.ran as u64;
		if let Some(age) = batch.oldest_age {
			pass.observe_age(age);
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
	///     let ds = Datastore::new("surrealkv://database.db").await?;
	///     let mut tx = ds.transaction(Write).await?;
	///     tx.cancel().await?;
	///     Ok(())
	/// }
	/// ```
	pub async fn transaction(&self, write: TransactionType) -> Result<Transaction> {
		self.transaction_factory.transaction(write, self.sequences.clone()).await
	}

	/// Opens the transaction a `BEGIN` asks for: a write transaction, or a
	/// read-only one when `read_only` is given, at `isolation` (default
	/// `SNAPSHOT`).
	///
	/// A read-only request with a bound of zero reads the current state at the
	/// requested level. A bound above zero reads a snapshot, which is always
	/// `SNAPSHOT`: combining it with `SERIALIZABLE` is refused, because a
	/// serializable read must not come from a snapshot older than the data it
	/// has to conflict with. The snapshot description is `None` for a write
	/// transaction.
	pub async fn begin_transaction(
		&self,
		isolation: Option<Isolation>,
		read_only: Option<ReadOnlyOptions>,
	) -> Result<(Transaction, Option<SnapshotInfo>)> {
		let isolation = isolation.unwrap_or(Isolation::Snapshot);
		let Some(read_only) = read_only else {
			return Ok((self.transaction_at(TransactionType::Write, isolation).await?, None));
		};
		if read_only.max_staleness.is_zero() {
			let txn = self.transaction_at(TransactionType::Read, isolation).await?;
			return Ok((txn, Some(SnapshotInfo::current())));
		}
		if isolation == Isolation::Serializable {
			return Err(surrealdb_datastore::error::DatastoreError::SerializableStaleRead.into());
		}
		let (txn, info) = self
			.read_transaction(ReadRequest {
				max_staleness: read_only.max_staleness,
				strict: read_only.strict,
			})
			.await?;
		Ok((txn, Some(info)))
	}

	/// Opens a transaction at `isolation`. See
	/// [`TransactionFactory::transaction_at`].
	pub async fn transaction_at(
		&self,
		write: TransactionType,
		isolation: Isolation,
	) -> Result<Transaction> {
		self.transaction_factory.transaction_at(write, isolation, self.sequences.clone()).await
	}

	/// Opens a read-only transaction over a snapshot no older than the
	/// request's staleness bound, with the description of the snapshot it
	/// reads.
	///
	/// A datastore whose backend has no stale tier reads its current state,
	/// which meets every bound. See [`TransactionFactory::read_transaction`].
	pub async fn read_transaction(
		&self,
		request: ReadRequest,
	) -> Result<(Transaction, SnapshotInfo)> {
		self.transaction_factory.read_transaction(request, self.sequences.clone()).await
	}

	/// A write transaction that a read-only store still opens.
	///
	/// For this node's own membership rows and for the durable copies of attached RPC
	/// sessions (written, and purged once expired), which must be written whatever the
	/// ledger says: they are a connection's state, not data the ledger's features govern,
	/// and a store open read-only still serves its data over HTTP, which keeps every
	/// attached session only in storage.
	/// Every other writer opens its transaction through [`Self::transaction`], which a
	/// read-only store refuses.
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

	/// Release the writer leases this datastore holds on indexes it has
	/// committed nothing under for [`WRITER_LEASE_IDLE`], so compaction can
	/// return those indexes' drained guards to `Empty`: no compaction pass steps
	/// a guard under a live lease. A lease is only ever held on a store this
	/// datastore is not the sole writer of.
	///
	/// Each index's release commits, in the same transaction, a compaction
	/// request for the index, so whichever datastore drains the compaction
	/// queue passes it again. A host that builds the datastore without its
	/// maintenance tasks calls this itself, next after
	/// [`WriterLeaseRelease::next_idle`] or [`Self::next_idle_writer_lease`].
	pub async fn release_idle_writer_leases(&self) -> Result<WriterLeaseRelease> {
		self.release_writer_leases_idle_for(WRITER_LEASE_IDLE).await
	}

	/// [`Self::release_idle_writer_leases`] for leases no commit has relied on
	/// for `idle` rather than [`WRITER_LEASE_IDLE`]. A shorter `idle` costs a
	/// writer a lease taken again on its next commit into the index.
	pub async fn release_writer_leases_idle_for(
		&self,
		idle: Duration,
	) -> Result<WriterLeaseRelease> {
		let released =
			self.transaction_factory.release_idle_writer_leases(&self.sequences, idle).await?;
		Ok(WriterLeaseRelease {
			released,
			next_idle: self.transaction_factory.next_idle_writer_lease(idle),
		})
	}

	/// How long until a writer lease this datastore holds goes idle and
	/// [`Self::release_idle_writer_leases`] releases it: `None` while it holds
	/// none, as on a store it is the sole writer of. Reads only this
	/// datastore's memory, so a host may ask after every request.
	pub fn next_idle_writer_lease(&self) -> Option<Duration> {
		self.transaction_factory.next_idle_writer_lease(WRITER_LEASE_IDLE)
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
	/// The PostgreSQL and MySQL front ends also read the catalog, for the link
	/// columns of the tables their text names. That read comes after the
	/// session checks of [`Self::check_session`], so a session the preflight
	/// refuses is refused without a read of storage.
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
			read_only,
			held_transactions: _,
		} = req;
		let statements = match linked_sql_text(&source) {
			Some((text, LinkedSql::PgSql)) => {
				let statements = self.pgsql_statements(text, session, transaction.as_ref()).await?;
				Self::read_only_block(statements, read_only, transaction.is_some())?
			}
			Some((text, LinkedSql::MySql)) => {
				let statements = self.mysql_statements(text, session, transaction.as_ref()).await?;
				Self::read_only_block(statements, read_only, transaction.is_some())?
			}
			None => self.request_statements(source, read_only, transaction.is_some())?,
		};
		let (opt, ctx) = self.setup_query(session, variables, transaction.as_ref(), cancel)?;
		match transaction {
			// Boxed: held inline, the run inside a caller's transaction would make this
			// future, which every request's caller awaits, its size.
			Some(_) => {
				Box::pin(Executor::execute_statements_with_transaction(self, ctx, opt, statements))
					.await
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
			read_only: _,
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
			read_only,
			held_transactions,
		} = req;
		// SQL text is lowered here without links, which reports what it refuses and
		// fixes the statement count, and again when the job starts, once the
		// catalog can be read (see [`Self::lower_linked`]).
		let relowered = linked_sql_text(&source).map(|(text, dialect)| {
			(text.to_string(), dialect, session.clone(), transaction.clone())
		});
		let statements = self.request_statements(source, read_only, transaction.is_some())?;
		let (opt, ctx) = self.setup_query(session, variables, transaction.as_ref(), cancel)?;
		let in_transaction = transaction.is_some();
		// The count is known now because parsing is done, which is what lets a
		// transport announce it before the first result exists.
		let statement_count = statements.len();
		let kvs = Arc::clone(self);
		Ok(QueryStreamJob {
			statement_count,
			run: Box::pin(async move {
				let statements = match relowered {
					Some((text, dialect, session, transaction)) => {
						let lowered = match dialect {
							LinkedSql::MySql => {
								kvs.mysql_statements(&text, &session, transaction.as_ref()).await
							}
							LinkedSql::PgSql => {
								kvs.pgsql_statements(&text, &session, transaction.as_ref()).await
							}
						}?;
						Self::read_only_block(lowered, read_only, in_transaction)?
					}
					None => statements,
				};
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

	/// The statements of a request: those of its source, wrapped in a read-only
	/// block when the request asks for one. See [`ReadOnlyOptions`].
	fn request_statements(
		&self,
		source: QuerySource<'_>,
		read_only: Option<ReadOnlyOptions>,
		in_caller_transaction: bool,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		let statements = self.statements_for(source)?;
		Self::read_only_block(statements, read_only, in_caller_transaction)
	}

	/// `statements`, wrapped in a read-only block when the request asks for one.
	fn read_only_block(
		statements: Vec<TopLevelStatement>,
		read_only: Option<ReadOnlyOptions>,
		in_caller_transaction: bool,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		let Some(read_only) = read_only else {
			return Ok(statements);
		};
		if in_caller_transaction {
			return Err(TypesError::validation(
				"A read-only request cannot run inside a caller-owned transaction".to_string(),
				None,
			));
		}
		// The wrapping block ends at the first transaction-control statement, so
		// any later statement would run outside it, as a write. A request that
		// asks for a read-only block therefore carries none of its own.
		if statements.iter().any(|statement| {
			matches!(statement.dispatch(), Dispatch::Begin(_) | Dispatch::Commit | Dispatch::Cancel)
		}) {
			return Err(TypesError::validation(
				"A read-only request cannot contain BEGIN, COMMIT or CANCEL statements".to_string(),
				None,
			));
		}
		use surrealdb_expr::parser::{BeginOptions, ReadOnly, Staleness, TopLevelExpr};
		let begin = TopLevelExpr::Begin(BeginOptions {
			isolation: None,
			read_only: Some(ReadOnly {
				max_staleness: Some(Staleness::Duration(read_only.max_staleness)),
				strict: read_only.strict,
			}),
		});
		let mut wrapped = Vec::with_capacity(statements.len() + 2);
		wrapped.push(TopLevelStatement::Ast(begin));
		wrapped.extend(statements);
		wrapped.push(TopLevelStatement::Ast(TopLevelExpr::Commit));
		Ok(wrapped)
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
			} => {
				self.check_sql_dialect(Dialect::PgSql)?;
				return self.parse_pgsql(text);
			}
			QuerySource::Text {
				text,
				dialect: Dialect::MySql,
			} => {
				self.check_sql_dialect(Dialect::MySql)?;
				return self.parse_mysql(text);
			}
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

	/// Parse and lower PostgreSQL query text to the statements the executor
	/// runs, telling the lowering which columns of the tables it names hold
	/// record links. See [`Self::lower_linked`].
	#[cfg(feature = "postgres")]
	async fn pgsql_statements(
		&self,
		text: &str,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		self.check_sql_dialect(Dialect::PgSql)?;
		let stmts =
			crate::pgsql::parse(text).map_err(|e| TypesError::validation(e.to_string(), None))?;
		let capabilities = self.capabilities.load();
		let columns_stay = sql_links::pgsql_keeps_columns(&stmts);
		self.lower_linked(session, None, transaction, columns_stay, |catalog| {
			crate::pgsql::lower_statements(&stmts, catalog, &capabilities, &self.parser_config)
		})
		.await
	}

	#[cfg(not(feature = "postgres"))]
	async fn pgsql_statements(
		&self,
		text: &str,
		_session: &Session,
		_transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		self.parse_pgsql(text)
	}

	/// Parse and lower MySQL query text to the statements the executor runs,
	/// telling the lowering which columns of the tables it names hold record
	/// links. See [`Self::lower_linked`].
	#[cfg(feature = "mysql")]
	async fn mysql_statements(
		&self,
		text: &str,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		use surrealdb_mysql::ast::stmt::Statement;

		self.check_sql_dialect(Dialect::MySql)?;
		let stmts =
			crate::mysql::parse(text).map_err(|e| TypesError::validation(e.to_string(), None))?;
		let capabilities = self.capabilities.load();
		let columns_stay = sql_links::mysql_keeps_columns(&stmts);
		// A `USE` changes the database the statements after it run in, so the
		// links of their tables are those of that database: each run of statements
		// up to and including a `USE` is lowered against the database in force
		// before it.
		let mut statements = Vec::new();
		let mut database: Option<&str> = None;
		let mut start = 0;
		for (end, stmt) in stmts.iter().enumerate() {
			let Statement::Use(use_stmt) = stmt else {
				continue;
			};
			statements.extend(
				self.lower_mysql_run(
					session,
					database,
					transaction,
					columns_stay,
					&stmts[start..=end],
					&capabilities,
				)
				.await?,
			);
			database = Some(use_stmt.database.as_str());
			start = end + 1;
		}
		if start < stmts.len() {
			statements.extend(
				self.lower_mysql_run(
					session,
					database,
					transaction,
					columns_stay,
					&stmts[start..],
					&capabilities,
				)
				.await?,
			);
		}
		Ok(statements)
	}

	/// Lower the MySQL statements `stmts`, which all run in `database` (the
	/// session's own when it is `None`), with the links of the tables they name.
	#[cfg(feature = "mysql")]
	async fn lower_mysql_run(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
		columns_stay: bool,
		stmts: &[surrealdb_mysql::ast::stmt::Statement],
		capabilities: &Capabilities,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		self.lower_linked(session, database, transaction, columns_stay, |catalog| {
			crate::mysql::lower_statements(stmts, catalog, capabilities, &self.parser_config)
		})
		.await
	}

	#[cfg(not(feature = "mysql"))]
	async fn mysql_statements(
		&self,
		text: &str,
		_session: &Session,
		_transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		self.parse_mysql(text)
	}

	/// Lower with a catalog that names the link columns of the tables the
	/// lowering asks about; see [`sql_links`] for the two steps.
	///
	/// The first lowering reads nothing. When it succeeds, or names no table, its result
	/// stands unless the catalog says otherwise, and its errors, like those of parsing, are
	/// reported whatever the session. When it is refused after naming a table, the refusal is
	/// settled by the catalog: it is lifted if it rested on what the catalog holds, and
	/// reported otherwise, but a session the catalog cannot be read for is told as much instead.
	/// The catalog is read only as [`Self::link_columns`] allows.
	///
	/// The catalog is read through `transaction` when there is one, so a link
	/// the caller's own transaction has defined is seen; otherwise through a
	/// read transaction of its own that ends before the statements run. It is
	/// read for the session's namespace and for `database`, or the session's
	/// own database when that is `None`.
	///
	/// The catalog also describes the tables that declare their columns ([`Self::read_tables`])
	/// only when `columns_stay`. The statements are all lowered before any runs, so for a request
	/// that alters, drops or creates a table those columns would be the ones the table had before
	/// the request; it is lowered against the links and the vector columns alone, with no index.
	///
	/// A statement over a system view that lists the user's tables is lowered against a catalog
	/// that also lists them, as [`Self::listed_tables`] allows.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn lower_linked(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
		columns_stay: bool,
		lower: impl Fn(&dyn surrealdb_sqlplan::BinderCatalog) -> anyhow::Result<Vec<TopLevelStatement>>,
	) -> std::result::Result<Vec<TopLevelStatement>, TypesError> {
		let invalid = |e: anyhow::Error| TypesError::validation(e.to_string(), None);
		let active_db = database.or(session.db.as_deref());
		let recorder = sql_links::RecordingCatalog::with_database(active_db);
		// A statement may be refused for what only the catalog tells (the dimensions of a column
		// an index covers), so a refusal is settled once the tables it names have been read.
		let first = lower(&recorder);
		let listing = recorder.asked_table_listing();
		let deferred = recorder.deferred_schema_checks();
		let enums_asked = recorder.asked_user_types();
		let tables = recorder.into_tables();
		if tables.is_empty() && !listing && !deferred && !enums_asked {
			return first.map_err(invalid);
		}
		let mut catalog = None;
		if !tables.is_empty() {
			if columns_stay {
				let purpose = sql_links::Purpose::Lowering;
				let described = self
					.read_tables(session, database, transaction, &tables, purpose, true)
					.await?;
				catalog = described
					.filter(|described| described.shapes_lowering || listing || deferred)
					.map(|described| described.catalog);
			} else {
				// The column shapes are read only for a lowering that asked for them, which is
				// one that deferred a refusal to the schema.
				let declared =
					self.link_columns(session, database, transaction, &tables, deferred).await?;
				catalog = sql_links::link_catalog(&declared, active_db);
				// The vector columns do not change in a request that holds DDL, but an index
				// may, so none is described.
				let purpose = sql_links::Purpose::Lowering;
				let described = self
					.read_tables(session, database, transaction, &tables, purpose, false)
					.await?;
				if let Some(described) = described
					&& described.shapes_lowering
				{
					catalog = Some(catalog.unwrap_or_default().with_vectors_of(&described.catalog));
				}
			}
		}
		if listing {
			let listed = self.listed_tables(session, database, transaction).await?;
			if !listed.is_empty() {
				let mut listing_catalog =
					catalog.take().unwrap_or_default().with_listed_tables(listed);
				listing_catalog.set_database(active_db.map(str::to_owned));
				catalog = Some(listing_catalog);
			}
		}
		if enums_asked {
			let types = self.user_types_of(session, database, transaction).await?;
			if !types.enums.is_empty() || !types.domains.is_empty() || !types.composites.is_empty()
			{
				let mut types_catalog = catalog
					.take()
					.unwrap_or_default()
					.with_enums(types.enums)
					.with_domains(types.domains)
					.with_composites(types.composites);
				types_catalog.set_database(active_db.map(str::to_owned));
				catalog = Some(types_catalog);
			}
		}
		match catalog {
			Some(catalog) => lower(&catalog).map_err(invalid),
			// The schema names no link and lists no table, which is an answer to a lowering that
			// left its refusals to the schema.
			None if deferred => lower(&surrealdb_sqlplan::MemoryCatalog::new()).map_err(invalid),
			None => first.map_err(invalid),
		}
	}

	/// The types of the selected database, as the enum, domain and composite types PostgreSQL
	/// knows them by, read as [`Self::listed_tables`] reads the tables and for the same sessions.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn user_types_of(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<surrealdb_sqlplan::UserTypes, TypesError> {
		self.check_session(session)?;
		let (Some(ns), Some(db)) = (session.ns.as_deref(), database.or(session.db.as_deref()))
		else {
			return Ok(Default::default());
		};
		if !session.au.can_access_ns_db(ns, db)
			|| self.check(session, Action::View, ResourceKind::Any.on_db(ns, db)).is_err()
		{
			return Ok(Default::default());
		}
		let types = match transaction {
			Some(txn) => sql_links::user_types(txn, ns, db).await,
			None => {
				let txn =
					self.transaction(Read).await.map_err(crate::err::anyhow_to_types_error)?;
				let types = sql_links::user_types(&txn, ns, db).await;
				let _ = txn.cancel().await;
				types
			}
		};
		types.map_err(crate::err::anyhow_to_types_error)
	}

	/// Every table of the selected database with its columns, for the system views that list them
	/// (`information_schema.tables`, `pg_class`, ...), read through `transaction` when there is
	/// one and otherwise through a read transaction of its own that ends before the statements
	/// run.
	///
	/// Table and column definitions are listed only to a session that could read them with
	/// `INFO FOR DB`: `Action::View` on `ResourceKind::Any` at the selected database, checked as
	/// [`Self::check`] does through the session's [`Auth`], so a record-access session, or a
	/// session whose roles do not reach the database, gets no tables and nothing is read. As for
	/// [`Self::link_columns`]:
	///
	/// - A session [`Self::check_session`] refuses gets that refusal, with nothing read.
	/// - A session with no namespace selected, or with no database selected and none given as
	///   `database`, has no tables.
	/// - A session whose authenticated level cannot reach the selected namespace and database
	///   ([`Auth::can_access_ns_db`]) has no tables.
	///
	/// The definitions are read with the datastore's own authority; the check above is what
	/// entitles the session to them, and table and field `PERMISSIONS` do not narrow the list, as
	/// they do not narrow `INFO FOR DB`.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn listed_tables(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
	) -> std::result::Result<Vec<surrealdb_sqlplan::TableSchema>, TypesError> {
		self.check_session(session)?;
		let (Some(ns), Some(db)) = (session.ns.as_deref(), database.or(session.db.as_deref()))
		else {
			return Ok(Vec::new());
		};
		if !session.au.can_access_ns_db(ns, db)
			|| self.check(session, Action::View, ResourceKind::Any.on_db(ns, db)).is_err()
		{
			return Ok(Vec::new());
		}
		let tables = match transaction {
			Some(txn) => sql_links::database_tables(txn, ns, db).await,
			None => {
				let txn =
					self.transaction(Read).await.map_err(crate::err::anyhow_to_types_error)?;
				let tables = sql_links::database_tables(&txn, ns, db).await;
				let _ = txn.cancel().await;
				tables
			}
		};
		tables.map_err(crate::err::anyhow_to_types_error)
	}

	/// The columns of `tables` that hold record links, as `(table, column,
	/// target)`, read through `transaction` when there is one and otherwise
	/// through a read transaction of its own.
	///
	/// This is the one place the front ends' lookup reads storage, and it reads
	/// for a session only when a query of that session would be served:
	///
	/// - A session [`Self::check_session`] refuses gets that refusal, as a query does, with nothing
	///   read.
	/// - A session with no namespace selected, or with no database selected and none given as
	///   `database`, has no links, and nothing is read.
	/// - A session whose authenticated level cannot reach the selected namespace and database
	///   ([`Auth::can_access_ns_db`]) has no links, and nothing is read: the executor refuses its
	///   statements with `NsNotAllowed` or `DbNotAllowed`, so it learns nothing of another tenant's
	///   field definitions on the way.
	///
	/// The field definitions are read with the datastore's own authority, not
	/// the session's permissions. They decide only how a comparison or an
	/// assignment converts the operand of a link column; none is returned to the
	/// session, and the statement then runs under the session's table and field
	/// permissions as any statement does. `shapes` also reads how each column is declared, for a
	/// statement that gives a column a new type.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn link_columns(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
		tables: &[String],
		shapes: bool,
	) -> std::result::Result<sql_links::Declared, TypesError> {
		let Some((ns, db)) = self.catalog_scope(session, database, tables)? else {
			return Ok(sql_links::Declared::default());
		};
		let links = match transaction {
			Some(txn) => sql_links::link_columns(txn, ns, db, tables, shapes).await,
			None => {
				let txn =
					self.transaction(Read).await.map_err(crate::err::anyhow_to_types_error)?;
				let links = sql_links::link_columns(&txn, ns, db, tables, shapes).await;
				let _ = txn.cancel().await;
				links
			}
		};
		links.map_err(crate::err::anyhow_to_types_error)
	}

	/// The namespace and database whose catalog a lookup of `tables` for `session` reads, or `None`
	/// when it reads nothing; see [`Self::link_columns`] for each case.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn catalog_scope<'a>(
		&self,
		session: &'a Session,
		database: Option<&'a str>,
		tables: &[String],
	) -> std::result::Result<Option<(&'a str, &'a str)>, TypesError> {
		self.check_session(session)?;
		let (Some(ns), Some(db)) = (session.ns.as_deref(), database.or(session.db.as_deref()))
		else {
			return Ok(None);
		};
		if tables.is_empty() || !session.au.can_access_ns_db(ns, db) {
			return Ok(None);
		}
		Ok(Some((ns, db)))
	}

	/// What the catalog says of `tables`, read as [`Self::link_columns`] reads the links and with
	/// the same refusals, or `None` when nothing is read.
	///
	/// Besides the links, the columns the tables declare are described when `declared_columns`, but
	/// only to a session that may view the definitions of tables (`Action::View` on the database's
	/// tables, which `INFO FOR DB` requires). A statement over a table that declares its columns
	/// then fails on a name outside them and expands `*` to them, which a session that may not see
	/// them could use to learn their names; it binds as it does against a schemaless table.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn read_tables(
		&self,
		session: &Session,
		database: Option<&str>,
		transaction: Option<&Arc<Transaction>>,
		tables: &[String],
		purpose: sql_links::Purpose,
		declared_columns: bool,
	) -> std::result::Result<Option<sql_links::Described>, TypesError> {
		let Some((ns, db)) = self.catalog_scope(session, database, tables)? else {
			return Ok(None);
		};
		let columns = declared_columns
			&& session.au.is_allowed(Action::View, &ResourceKind::Table.on_db(ns, db)).is_ok();
		let types = purpose.column_types();
		let described = match transaction {
			Some(txn) => sql_links::describe_tables(txn, ns, db, tables, columns, types).await,
			None => {
				let txn =
					self.transaction(Read).await.map_err(crate::err::anyhow_to_types_error)?;
				let described =
					sql_links::describe_tables(&txn, ns, db, tables, columns, types).await;
				// The transaction wrote nothing, so one that cannot be ended costs the request
				// nothing and is only noted.
				if let Err(error) = txn.cancel().await {
					debug!(target: TARGET, "A catalog read transaction was not cancelled: {error}");
				}
				described
			}
		};
		described.map(Some).map_err(crate::err::anyhow_to_types_error)
	}

	/// The columns of `table` declared as a link to one table, as `(column,
	/// target, admits NONE)`, for a front end that writes rows without going through
	/// the SQL lowering.
	///
	/// A session that [`Self::check_session`] refuses is an error, the one a
	/// query gets, and is not served from the catalog. The result is empty when
	/// the session has no namespace or database selected, or when its level
	/// cannot reach them; see [`Self::link_columns`].
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	pub async fn record_link_columns(
		&self,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
		table: &str,
	) -> std::result::Result<Vec<(String, String, bool)>, TypesError> {
		let declared =
			self.link_columns(session, None, transaction, &[table.to_owned()], false).await?;
		Ok(declared
			.links
			.into_iter()
			.map(|(_, column, target, admits_none)| (column, target, admits_none))
			.collect())
	}

	/// The catalog a PostgreSQL SELECT is bound against to describe its result, read through the
	/// caller's transaction when present: the types of the columns it reads and, for a session
	/// that may view table definitions, the columns each table declares. It is the catalog the
	/// statement is lowered against when it runs (see [`Self::lower_linked`]), so a client is
	/// told the columns the statement answers with. Catalog preflight follows the same session and
	/// tenant checks as link lowering.
	#[cfg(feature = "postgres")]
	#[tracing::instrument(level = "trace", skip_all)]
	pub async fn pgsql_catalog(
		&self,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
		select: &surrealdb_pgsql::ast::stmt::SelectStmt,
	) -> std::result::Result<surrealdb_sqlplan::MemoryCatalog, TypesError> {
		let recorder = sql_links::RecordingCatalog::default();
		let tables = surrealdb_sqlplan::binder::pgsql::bind_select(
			select,
			&recorder,
			&surrealdb_sqlplan::DialectPolicy::postgres(),
			None,
		)
		.is_ok()
		.then(|| recorder.into_tables());
		self.wire_catalog(session, transaction, tables).await
	}

	/// The catalog a MySQL SELECT is bound against to describe its result; see
	/// [`Self::pgsql_catalog`].
	#[cfg(feature = "mysql")]
	#[tracing::instrument(level = "trace", skip_all)]
	pub async fn mysql_catalog(
		&self,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
		select: &surrealdb_mysql::ast::stmt::SelectStmt,
	) -> std::result::Result<surrealdb_sqlplan::MemoryCatalog, TypesError> {
		let recorder = sql_links::RecordingCatalog::default();
		let tables = surrealdb_sqlplan::binder::mysql::bind_select(
			select,
			&recorder,
			&surrealdb_sqlplan::DialectPolicy::mysql(),
			None,
		)
		.is_ok()
		.then(|| recorder.into_tables());
		self.wire_catalog(session, transaction, tables).await
	}

	/// The catalog a wire front end describes a SELECT by, given the `tables` it names, or `None`
	/// when it could not be bound to find them: the caller binds the statement and reports its
	/// analysis errors with their codes, so a failed discovery needs no schema read.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn wire_catalog(
		&self,
		session: &Session,
		transaction: Option<&Arc<Transaction>>,
		tables: Option<Vec<String>>,
	) -> std::result::Result<surrealdb_sqlplan::MemoryCatalog, TypesError> {
		let described = match tables {
			Some(tables) => {
				let purpose = sql_links::Purpose::Describing;
				self.read_tables(session, None, transaction, &tables, purpose, true).await?
			}
			None => None,
		};
		let mut catalog = match described {
			Some(described) => described.catalog,
			None => surrealdb_sqlplan::MemoryCatalog::new(),
		};
		catalog.set_database(session.db.clone());
		Ok(catalog)
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

	/// Refuse PostgreSQL or MySQL query text when the datastore's capabilities
	/// deny the `postgres` or `mysql` route target. Evaluated on every query, so
	/// a capability swap applies to connections that already chose the dialect.
	pub(crate) fn check_sql_dialect(
		&self,
		dialect: Dialect,
	) -> std::result::Result<(), TypesError> {
		let (target, name, capability) = match dialect {
			Dialect::PgSql => (RouteTarget::Postgres, "PostgreSQL", "postgres"),
			Dialect::MySql => (RouteTarget::MySql, "MySQL", "mysql"),
			Dialect::SurrealQl | Dialect::Gql => return Ok(()),
		};
		if self.capabilities.load().allows_http_route(&target) {
			return Ok(());
		}
		Err(TypesError::not_allowed(
			format!(
				"The {name} dialect is not allowed on this server: the `{capability}` capability is denied"
			),
			None::<surrealdb_types::NotAllowedError>,
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

	/// The session checks of the preflight: an expired session, and an
	/// anonymous actor on a datastore that refuses them, are not served.
	///
	/// They are a function of the session and the datastore's settings alone and
	/// read no storage, so work that reads storage on a request's behalf before
	/// [`Self::setup_query`] runs (the catalog lookup of the SQL front ends)
	/// calls this first, and a refused session costs no I/O. The request's text
	/// is parsed before them, so unparsable text is reported as that whatever
	/// the session. [`Self::execute_import`] repeats these checks inline; see
	/// [`Self::setup_query`].
	pub(crate) fn check_session(&self, sess: &Session) -> std::result::Result<(), TypesError> {
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

		// A pinned session runs only within its pin. `USE` and the RPC methods
		// refuse to leave it, but a caller may set the selection directly (the
		// MCP tools take a namespace and database per call), so every query
		// checks it here as well.
		if let Some(pin) = &sess.pin {
			pin.check_session(sess).map_err(crate::err::anyhow_to_types_error)?;
		}
		Ok(())
	}

	/// The preflight a query runs: the session checks
	/// ([`Self::check_session`]), and the options and context the statements
	/// execute against.
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
		self.check_session(sess)?;

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
		let mut preamble_checked = false;

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

				// The start of the input is looked at once, when the fill has stopped, so a
				// byte order mark is never split across chunks. A UTF-8 mark is not part of
				// the statements and is dropped. A UTF-16 mark means the whole input is
				// UTF-16 text, which no statement of it could be read from.
				if !preamble_checked {
					preamble_checked = true;
					if buffer.starts_with(&[0xEF, 0xBB, 0xBF]) {
						let _ = buffer.split_to(3);
					} else if buffer.starts_with(&[0xFF, 0xFE]) || buffer.starts_with(&[0xFE, 0xFF])
					{
						return Poll::Ready(Some(Err(anyhow::Error::new(TypesError::validation(
							"The import is UTF-16 text, and SurrealQL is read as UTF-8. Save it as \
							 UTF-8; an export is written as UTF-8 by `surreal export <file>`, but \
							 not by redirecting standard output in Windows PowerShell"
								.to_owned(),
							surrealdb_types::ValidationError::InvalidRequest,
						)))));
					}
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
		// The dump is read with the authority of root and applies no table or
		// field SELECT permission (see the frame below), so a caller whose reads
		// are permission checked on `/sql` must not receive an export through it:
		// the level-only `View` check the transport makes admits a roleless
		// database- or namespace-level token, which `/sql` would still filter.
		// Only a principal that bypasses record permissions at this level - a
		// role at or above the database - may export (GHSA-qh3v-rr48-x3cq).
		if crate::exec::permission::action_is_checked(
			self.auth_enabled,
			&sess.au,
			Action::View,
			Some(ns.as_str()),
			Some(db.as_str()),
		) {
			return Err(Error::from(PolicyError::NotAllowed {
				actor: sess.au.id().to_string(),
				action: Action::View.to_string(),
				resource: format!("the `{ns}`/`{db}` database export"),
			})
			.into());
		}
		// Create a new readonly transaction
		let txn = Arc::new(self.transaction(Read).await?);
		let batch_size = self.config.datastore.export_batch_size;
		// The frame the rows are read in: the export's own transaction, with
		// the authority of root, because a dump is the rows as stored and
		// permissions are judged by the check above. User password hashes and
		// SCRAM verifiers are included only when the caller holds `Edit` on
		// `Actor` in the database (`Owner` at or above the database level, or
		// an anonymous session when authentication is disabled).
		let include_user_secrets = (!self.auth_enabled && sess.au.is_anon())
			|| sess
				.au
				.is_allowed(
					Action::Edit,
					&crate::iam::ResourceKind::Actor.on_db(ns.as_str(), db.as_str()),
				)
				.is_ok();
		let mut ctx = self.setup_ctx()?;
		ctx.set_transaction(Arc::clone(&txn));
		let frame = super::export::RowFrame {
			ctx: ctx.freeze(),
			opt: crate::dbs::Options::new(&self.config.exec)
				.with_ns(Some(ns.as_str().into()))
				.with_db(Some(db.as_str().into()))
				.with_auth(Arc::new(crate::iam::Auth::for_root(crate::iam::Role::Owner))),
			include_user_secrets,
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
			.with_pin(sess.pin.clone().map(Arc::new))
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
		ctx.set_morsel_places(self.morsel_places.clone());
		ctx.set_event_queue_node(self.event_queue_node);
		ctx.set_changefeed_source(self.changefeed_source.clone());
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
		// A pinned session keeps its selection within the pin. An empty `USE`
		// changes nothing on a pinned session, which always has a namespace.
		if let Some(pin) = &session.pin
			&& (namespace.is_some() || database.is_some())
		{
			let target_ns = namespace.as_deref().or(session.ns.as_deref());
			let target_db = database.as_deref().or(session.db.as_deref());
			pin.check_selection(target_ns, target_db)
				.map_err(|err| TypesError::not_allowed(err.to_string(), None))?;
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
		// A pinned session reaches only the schema of its pin. Every GraphQL
		// transport takes its schema from here, and the `signIn`/`signUp`
		// resolvers act on the schema's namespace and database, so this keeps
		// GraphQL within the pin as the query preflight keeps SurrealQL.
		if let Some(pin) = &session.pin {
			pin.check_session(session)
				.map_err(|err| crate::graphql::GraphqlError::AuthError(err.to_string()))?;
		}
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
	use surrealdb_datastore::values::event_queue::EventClaim;
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
	use crate::kvs::EVENT_CLAIM_TTL;
	use crate::kvs::tasks::PassFailure;
	use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
	use crate::kvs::testing::{
		NonRetryableErrorSite, PanicSite, RetryableConflictSite, inject_non_retryable_error,
		inject_non_retryable_errors, inject_panic, inject_retryable_conflict,
		inject_retryable_conflicts, retryable_conflict_count,
	};
	use crate::types::{PublicValue, PublicVariables};

	/// Work a test runs at a stage of a compaction pass's batch.
	type CompactionBatchHook =
		Box<dyn FnOnce() -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

	/// The stage of a compaction pass's batch a test hook runs at.
	#[derive(Clone, Copy, PartialEq, Eq, Hash)]
	pub(super) enum BatchStage {
		/// The batch is read and the lease held, and no compaction has started.
		Read,
		/// The batch is compacted and its queue entries deleted.
		Cleared,
	}

	static COMPACTION_BATCH_HOOKS: std::sync::LazyLock<
		std::sync::Mutex<HashMap<(Uuid, BatchStage), CompactionBatchHook>>,
	> = std::sync::LazyLock::new(Default::default);

	/// The hook a test set for the stage of the node's next batch, taken so it
	/// runs once.
	pub(super) fn take_compaction_batch_hook(
		node: Uuid,
		stage: BatchStage,
	) -> Option<CompactionBatchHook> {
		COMPACTION_BATCH_HOOKS.lock().unwrap().remove(&(node, stage))
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

	/// Work a test runs in a graph fold drain of the node, before the drain
	/// folds its next queued scope.
	static BEFORE_FOLD_SCOPE: std::sync::LazyLock<
		std::sync::Mutex<HashMap<Uuid, CompactionBatchHook>>,
	> = std::sync::LazyLock::new(Default::default);

	/// The hook a test set for the node's next queued scope, taken so it runs
	/// once.
	pub(super) fn take_before_fold_scope(node: Uuid) -> Option<CompactionBatchHook> {
		BEFORE_FOLD_SCOPE.lock().unwrap().remove(&node)
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
			basis: None,
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
		assert_eq!(resumed.items, 0, "a scan that lost its lease adopts nothing");
		assert_eq!(
			resumed.lease,
			TaskLeaseState::Elsewhere,
			"the scan reports the lease elsewhere"
		);
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

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn denied_sql_dialects_are_refused_per_query() -> Result<()> {
		use crate::dbs::capabilities::Targets;
		let ses = Session::owner().with_ns("test").with_db("test");
		let ds = Datastore::builder()
			.with_capabilities(Capabilities::all().without_http_routes(Targets::Some(
				[RouteTarget::Postgres, RouteTarget::MySql].into(),
			)))
			.build_with_path("memory")
			.await?;
		let sources = [
			#[cfg(feature = "postgres")]
			QuerySource::pgsql("SELECT 1"),
			#[cfg(feature = "mysql")]
			QuerySource::mysql("SELECT 1"),
		];
		for source in sources {
			let err = ds.run(QueryRequest::new(source, &ses)).await.unwrap_err();
			assert!(err.to_string().contains("not allowed"), "unexpected error: {err}");
		}
		let mut res = ds.run(QueryRequest::new(QuerySource::text("RETURN 1"), &ses)).await?;
		assert_eq!(res.remove(0).result?, PublicValue::Number(surrealdb_types::Number::Int(1)));
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

	/// The SQL front ends under test: the label and the constructor of their
	/// query source.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[allow(clippy::type_complexity, clippy::vec_init_then_push)]
	fn sql_dialects() -> Vec<(&'static str, for<'a> fn(&'a str) -> QuerySource<'a>)> {
		#[cfg(feature = "postgres")]
		fn pgsql(text: &str) -> QuerySource<'_> {
			QuerySource::pgsql(text)
		}
		#[cfg(feature = "mysql")]
		fn mysql(text: &str) -> QuerySource<'_> {
			QuerySource::mysql(text)
		}
		let mut dialects: Vec<(&'static str, for<'a> fn(&'a str) -> QuerySource<'a>)> = Vec::new();
		#[cfg(feature = "postgres")]
		dialects.push(("pgsql", pgsql));
		#[cfg(feature = "mysql")]
		dialects.push(("mysql", mysql));
		dialects
	}

	/// A datastore whose `test`/`test` database has the table `emp` with a
	/// column `dept` declared a link to the table `dept`, with `auth` and
	/// `guests` as its authentication settings. The observer has seen every
	/// transaction the setup took, and is cleared.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn sql_link_ds(
		auth: bool,
		guests: bool,
	) -> Result<(Arc<Datastore>, Arc<TxOutcomeObserver>)> {
		use crate::dbs::capabilities::Targets;
		let observer = Arc::new(TxOutcomeObserver::default());
		let capabilities =
			Capabilities::all().with_experimental(Targets::All).with_guest_access(guests);
		let ds = Datastore::builder()
			.with_capabilities(capabilities)
			.with_auth(auth)
			.with_observer(Arc::clone(&observer) as Arc<dyn ExecutionObserver>)
			.without_maintenance_tasks()
			.build_with_path("memory")
			.await?;
		let owner = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		execute_all(
			&ds,
			&owner,
			"DEFINE TABLE dept; DEFINE TABLE emp;
			 DEFINE FIELD dept ON emp TYPE record<dept>;
			 CREATE dept:1; CREATE dept:2;
			 CREATE emp:1 SET dept = dept:1;
			 CREATE emp:2 SET dept = dept:2;",
		)
		.await?;
		let _ = observer.take();
		Ok((ds, observer))
	}

	/// What a request for `sql` as `session` answers: the error of the request
	/// itself, or of its single statement, rendered.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn sql_error(
		ds: &Datastore,
		session: &Session,
		source: QuerySource<'_>,
		transaction: Option<&Arc<Transaction>>,
	) -> String {
		let req =
			QueryRequest::new(source, session).with_optional_transaction(transaction.cloned());
		match ds.run(req).await {
			Err(err) => err.to_string(),
			Ok(mut results) => match results.remove(0).result {
				Err(err) => err.to_string(),
				Ok(value) => panic!("expected the request to fail, got {value:?}"),
			},
		}
	}

	/// A session that [`Datastore::setup_query`] refuses because it expired a
	/// minute ago.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	fn expired_session() -> Session {
		let mut session = Session::owner().with_ns("test").with_db("test");
		session.exp = Some(Utc::now().timestamp() - 60);
		session
	}

	/// Asserts that SQL text as `session` is refused with the error the same
	/// request gets as SurrealQL text, and that doing so read no catalog entry:
	/// not through a caller-owned transaction, not through a transaction the
	/// request would open for itself, and not when the request is streamed. The
	/// text names both tables, so a lookup of their links is what would read.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	async fn assert_refused_before_the_catalog(
		ds: &Arc<Datastore>,
		observer: &TxOutcomeObserver,
		session: &Session,
		refusal: &str,
	) -> Result<()> {
		let surql =
			sql_error(ds, session, QuerySource::text("SELECT * FROM emp, dept"), None).await;
		assert!(surql.contains(refusal), "SurrealQL answers `{surql}`, not `{refusal}`");
		assert!(observer.take().is_empty(), "the refused SurrealQL request opened a transaction");
		for (label, source) in sql_dialects() {
			let text = "SELECT * FROM emp, dept WHERE emp.dept = 1";
			let err = sql_error(ds, session, source(text), None).await;
			assert_eq!(err, surql, "{label}: the refusal is not the one SurrealQL gets");
			let opened = observer.take();
			assert!(opened.is_empty(), "{label}: a refused request opened {opened:?}");

			let txn = Arc::new(ds.transaction(Read).await?);
			let recording = txn.record_catalog_reads();
			let err = sql_error(ds, session, source(text), Some(&txn)).await;
			let reads = recording.finish();
			txn.cancel().await?;
			let _ = observer.take();
			assert_eq!(err, surql, "{label}: the refusal in a caller's transaction differs");
			assert_eq!(
				reads,
				Default::default(),
				"{label}: a refused request read the catalog of the caller's transaction"
			);

			let (items, _) = async_channel::unbounded();
			match ds.run_streaming(QueryRequest::new(source(text), session), items) {
				Err(err) => {
					assert_eq!(err.to_string(), surql, "{label}: the streamed refusal differs")
				}
				Ok(_) => panic!("{label}: a refused request was prepared to stream"),
			}
			let opened = observer.take();
			assert!(opened.is_empty(), "{label}: a refused streamed request opened {opened:?}");
		}
		Ok(())
	}

	/// A valid session's request reads the catalog through a caller-owned
	/// transaction and, without one, opens a transaction of its own. These are
	/// the controls for the refusal tests, whose observations would otherwise
	/// be empty for want of any way to see a read. The recording also notes the
	/// executor's own reads, so a refusal test, where the executor never starts,
	/// is what attributes a read to the lookup.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn a_valid_session_reads_the_catalog_for_sql_links() -> Result<()> {
		let (ds, observer) = sql_link_ds(false, true).await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for (label, source) in sql_dialects() {
			let text = "SELECT * FROM emp, dept WHERE emp.dept = 1";
			let txn = Arc::new(ds.transaction(Read).await?);
			let recording = txn.record_catalog_reads();
			let req = QueryRequest::new(source(text), &session).with_transaction(Arc::clone(&txn));
			let results = ds.run(req).await?;
			let reads = recording.finish();
			txn.cancel().await?;
			for result in results {
				result.result?;
			}
			assert!(
				reads.databases.contains(&("test".to_owned(), "test".to_owned())),
				"{label}: the lookup read no database: {reads:?}"
			);

			let _ = observer.take();
			ds.run(QueryRequest::new(source(text), &session)).await?;
			assert!(
				!observer.take().is_empty(),
				"{label}: the observer saw no transaction of an accepted request"
			);
		}
		Ok(())
	}

	/// A link column named in SQL text is converted for a valid session: the
	/// key `1` finds the record that links to `dept:1`.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn a_valid_session_gets_sql_links_converted() -> Result<()> {
		let (ds, _) = sql_link_ds(false, true).await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for (label, source) in sql_dialects() {
			let mut res = ds
				.run(QueryRequest::new(source("SELECT id FROM emp WHERE dept = 1"), &session))
				.await?;
			let val = res.remove(0).result?;
			assert_eq!(
				val,
				PublicValue::Array(surrealdb_types::array![object! { id: 1 }]),
				"{label}: the key did not find the linked record"
			);
		}
		Ok(())
	}

	/// SQL text run as a result stream is lowered with the links of its tables, as
	/// it is when it is run to completion: a bare key written to a link column
	/// is stored as the link, and a key finds the record that links to it.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn streamed_sql_text_is_lowered_with_links() -> Result<()> {
		use crate::dbs::QueryStreamItem;

		let (ds, _) = sql_link_ds(false, true).await?;
		let session = Session::owner().with_ns("test").with_db("test");
		for (label, source) in sql_dialects() {
			let (tx, rx) = async_channel::unbounded();
			let sql = "INSERT INTO emp (id, dept) VALUES (9, 2); SELECT id FROM emp WHERE dept = 2";
			let job = ds.run_streaming(QueryRequest::new(source(sql), &session), tx)?;
			assert_eq!(job.statement_count, 2, "{label}");
			job.run.await?;
			let mut finished = 0;
			let mut ids = Vec::new();
			while let Ok(item) = rx.try_recv() {
				match item {
					QueryStreamItem::Finished {
						error,
						..
					} => {
						assert!(error.is_none(), "{label}: {error:?}");
						finished += 1;
					}
					QueryStreamItem::Rows {
						values,
						..
					} => ids.extend(values),
					QueryStreamItem::Frame {
						rows,
						..
					} => ids.extend(rows.to_values()?),
					QueryStreamItem::Value {
						..
					} => {}
				}
			}
			assert_eq!(finished, 2, "{label}");
			assert!(!ids.is_empty(), "{label}: the key did not find the linked record");
			execute_all(&ds, &session, "DELETE emp:9").await?;
		}
		Ok(())
	}

	/// A `USE` in MySQL text moves the catalog lookup with it: the statements
	/// after it are lowered with the links of the database it selects, and those
	/// before it with the links of the database in force before.
	#[cfg(feature = "mysql")]
	#[tokio::test]
	async fn a_use_in_mysql_text_moves_the_link_lookup_to_the_database_it_selects() -> Result<()> {
		let (ds, _) = sql_link_ds(false, true).await?;
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "other").await?;
		txn.commit().await?;
		let other = Session::owner().with_ns("test").with_db("other");
		execute_all(&ds, &other, "DEFINE TABLE emp; DEFINE FIELD dept ON emp TYPE int;").await?;

		// The session is in `other`, where `dept` is an integer, and the text
		// writes `emp:3` in `test`, where `dept` is a link, between two writes
		// to `other`.
		let text = "INSERT INTO emp (id, dept) VALUES (1, 7); \
		            USE test; INSERT INTO emp (id, dept) VALUES (3, 2); \
		            USE other; INSERT INTO emp (id, dept) VALUES (3, 8)";
		for result in ds.run(QueryRequest::new(QuerySource::mysql(text), &other)).await? {
			result.result?;
		}

		let read = |db: &'static str, id: i64| {
			let ds = &ds;
			async move {
				use surrealdb_types::ToSql;
				let session = Session::owner().with_ns("test").with_db(db);
				let sql = format!("RETURN (SELECT VALUE dept FROM ONLY emp:{id})");
				let mut res = ds.execute(&sql, &session, None).await?;
				Ok::<_, anyhow::Error>(res.remove(0).result?.to_sql())
			}
		};
		assert_eq!(read("other", 1).await?, "7");
		assert_eq!(read("test", 3).await?, "dept:2");
		assert_eq!(read("other", 3).await?, "8");
		Ok(())
	}

	/// An expired session is refused before the front end reads the catalog for
	/// the tables its text names.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn an_expired_session_reads_no_catalog_for_sql_text() -> Result<()> {
		let (ds, observer) = sql_link_ds(false, true).await?;
		assert_refused_before_the_catalog(
			&ds,
			&observer,
			&expired_session(),
			"The session has expired",
		)
		.await
	}

	/// An anonymous session of a datastore that refuses guests is refused
	/// before the front end reads the catalog for the tables its text names.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn an_anonymous_session_reads_no_catalog_for_sql_text() -> Result<()> {
		let (ds, observer) = sql_link_ds(true, false).await?;
		let session = Session::default().with_ns("test").with_db("test");
		assert_refused_before_the_catalog(&ds, &observer, &session, "Anonymous access not allowed")
			.await
	}

	/// Text that does not parse is reported as the parse error it is, whatever
	/// the session: the parse comes before the session checks.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn a_parse_error_wins_over_an_expired_session() -> Result<()> {
		let (ds, observer) = sql_link_ds(false, true).await?;
		let valid = Session::owner().with_ns("test").with_db("test");
		for (label, source) in sql_dialects() {
			let text = "SELECT FROM FROM emp WHERE";
			let parse_error = sql_error(&ds, &valid, source(text), None).await;
			assert!(
				!parse_error.contains("expired"),
				"{label}: the valid session's parse error `{parse_error}`"
			);
			let _ = observer.take();
			let err = sql_error(&ds, &expired_session(), source(text), None).await;
			assert_eq!(err, parse_error, "{label}: the expired session hides the parse error");
			assert!(observer.take().is_empty(), "{label}: unparsable text opened a transaction");
		}
		Ok(())
	}

	/// Text that parses but that the lowering refuses without any catalog is
	/// reported as that refusal, whatever the session: the lowering that notes
	/// the tables a statement names reads nothing, and comes before the session
	/// checks.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn a_lowering_error_wins_over_an_expired_session() -> Result<()> {
		let (ds, observer) = sql_link_ds(false, true).await?;
		let valid = Session::owner().with_ns("test").with_db("test");
		for (label, source) in sql_dialects() {
			let text = "WITH RECURSIVE t AS (SELECT n FROM t) SELECT * FROM t, emp";
			let lowering_error = sql_error(&ds, &valid, source(text), None).await;
			assert!(
				lowering_error.contains("Recursive CTE self-reference"),
				"{label}: the valid session's error is `{lowering_error}`"
			);
			let _ = observer.take();
			let err = sql_error(&ds, &expired_session(), source(text), None).await;
			assert_eq!(err, lowering_error, "{label}: the expired session hides the error");
			assert!(observer.take().is_empty(), "{label}: a refused lowering opened a transaction");
		}
		Ok(())
	}

	/// A session whose level cannot reach the database it selected is served no
	/// links, and the executor refuses its statement as it refuses the same
	/// statement in SurrealQL: skipping the lookup does not change the answer.
	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn a_session_outside_its_scope_gets_the_executors_refusal_for_sql_text() -> Result<()> {
		use crate::iam::Level;
		let (ds, _) = sql_link_ds(true, false).await?;
		let mut elsewhere = Session::for_level(
			Level::Database("other".to_owned(), "other".to_owned()),
			Role::Owner,
		);
		elsewhere.ns = Some("test".to_owned());
		elsewhere.db = Some("test".to_owned());
		let surql = sql_error(&ds, &elsewhere, QuerySource::text("SELECT * FROM emp"), None).await;
		assert!(
			surql.contains("permission to change to the test namespace"),
			"SurrealQL: `{surql}`"
		);
		for (label, source) in sql_dialects() {
			let err =
				sql_error(&ds, &elsewhere, source("SELECT * FROM emp WHERE dept = 1"), None).await;
			assert_eq!(err, surql, "{label}: the refusal is not the one SurrealQL gets");
		}
		Ok(())
	}

	/// What a front end that writes rows itself, as pgwire's `COPY` does, is told
	/// of the link columns of a table: the links of a valid session, an error
	/// for a session a query would refuse, and nothing for a session that names
	/// no database or one its level cannot reach. None of the last three reads
	/// the catalog.
	/// Table and column definitions are listed through the system views only to a session that
	/// could read them with `INFO FOR DB`; every other session sees the views without user tables.
	#[cfg(feature = "postgres")]
	#[tokio::test]
	async fn system_views_list_tables_only_to_a_session_that_may_view_them() -> Result<()> {
		use surrealdb_types::ToSql;

		use crate::iam::Level;
		let (ds, _observer) = sql_link_ds(true, false).await?;
		let in_db = |level: Level, role: Role| {
			let mut session = Session::for_level(level, role);
			session.ns = Some("test".to_owned());
			session.db = Some("test".to_owned());
			session
		};
		let record = Session::for_record(
			"test",
			"test",
			"account",
			surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", 1)),
		);
		let sessions = [
			("owner", Session::owner().with_ns("test").with_db("test"), true),
			("viewer", Session::viewer().with_ns("test").with_db("test"), true),
			(
				"database user",
				in_db(Level::Database("test".into(), "test".into()), Role::Viewer),
				true,
			),
			("namespace user", in_db(Level::Namespace("test".into()), Role::Editor), true),
			("record", record, false),
			(
				"another tenant",
				in_db(Level::Database("other".into(), "other".into()), Role::Owner),
				false,
			),
		];
		for (label, source) in sql_dialects().into_iter().filter(|(label, _)| *label == "pgsql") {
			let views = [
				"SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
				"SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = 'public'",
			];
			for (what, session, may) in &sessions {
				for text in views {
					let results = ds.run(QueryRequest::new(source(text), session)).await?;
					let mut shown = String::new();
					for result in results {
						if let Ok(value) = result.result {
							shown.push_str(&value.to_sql());
						}
					}
					let lists = shown.contains("emp") && shown.contains("dept");
					assert_eq!(lists, *may, "{label} {what} {text}: {shown}");
				}
			}
		}
		Ok(())
	}

	#[cfg(any(feature = "postgres", feature = "mysql"))]
	#[tokio::test]
	async fn record_link_columns_reads_the_catalog_only_for_a_session_that_may() -> Result<()> {
		use crate::iam::Level;
		let (ds, observer) = sql_link_ds(true, false).await?;

		async fn links(
			ds: &Datastore,
			session: &Session,
		) -> (
			std::result::Result<Vec<(String, String, bool)>, TypesError>,
			crate::kvs::catalog_reads::CatalogReads,
		) {
			let txn = Arc::new(ds.transaction(Read).await.expect("a read transaction opens"));
			let recording = txn.record_catalog_reads();
			let links = ds.record_link_columns(session, Some(&txn), "emp").await;
			let reads = recording.finish();
			txn.cancel().await.expect("the read transaction cancels");
			(links, reads)
		}

		let owner = Session::owner().with_ns("test").with_db("test");
		let (found, reads) = links(&ds, &owner).await;
		assert_eq!(found?, vec![("dept".to_owned(), "dept".to_owned(), false)]);
		assert!(!reads.databases.is_empty(), "the lookup of a valid session read nothing");

		let mut elsewhere = Session::for_level(
			Level::Database("other".to_owned(), "other".to_owned()),
			Role::Owner,
		);
		elsewhere.ns = Some("test".to_owned());
		elsewhere.db = Some("test".to_owned());
		let mut no_db = Session::owner();
		no_db.ns = Some("test".to_owned());
		for (what, session) in [("no database", no_db), ("another tenant", elsewhere)] {
			let (found, reads) = links(&ds, &session).await;
			assert_eq!(found?, Vec::new(), "{what} has links");
			assert_eq!(reads, Default::default(), "{what}: the lookup read the catalog");
		}

		let anonymous = Session::default().with_ns("test").with_db("test");
		for (what, session) in [("expired", expired_session()), ("anonymous", anonymous)] {
			let (found, reads) = links(&ds, &session).await;
			assert!(found.is_err(), "{what}: the lookup answered {found:?}");
			assert_eq!(reads, Default::default(), "{what}: the lookup read the catalog");
		}
		let _ = observer.take();
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
			Some(&lh),
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
			Some(&lease(ds.id())?),
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
			Some(&lh),
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
				Some(&lh),
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
				Some(&lh),
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
				Some(&lh),
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
			let first =
				IndexOperation::prepare_hnsw_compaction(&ctx, &ikb, ix.format_version).await;
			let second =
				IndexOperation::prepare_hnsw_compaction(&ctx, &ikb, ix.format_version).await;
			txn.cancel().await?;
			(first?, second?)
		};
		// The pass steps the guards of the shards a plan drained once its
		// apply has committed.
		let step = first.guard_step(&ikb);

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
		let applied = catch!(txn, applied.map_err(|e| e.error));
		assert!(applied.applied(), "the first plan must apply: {applied:?}");
		txn.commit().await?;
		assert!(step.run(&txn).await?, "the first plan drained its shards");
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
	#[cfg(feature = "kv-surrealkv")]
	async fn assert_compaction_write_guard_conflicts_with_a_retirement_that_commits_after_it(
		sql: &str,
		table: &str,
		index: &str,
	) -> Result<()> {
		let dir = temp_dir::TempDir::new()?;
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.build_with_path(&format!("surrealkv://{}", dir.path().to_string_lossy()))
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

	#[cfg(feature = "kv-surrealkv")]
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

	#[cfg(feature = "kv-surrealkv")]
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

	#[cfg(feature = "kv-surrealkv")]
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

	#[cfg(all(feature = "kv-surrealkv", diskann))]
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

	/// An index that keeps losing its commit to conflicts ends its pass once
	/// the pass's retries are spent, and the pass queues a fresh request for
	/// the index: the slice returns rather than retrying until a commit wins,
	/// and the next slice compacts the index from that request once its commit
	/// goes through.
	async fn assert_index_compaction_slice_yields_under_sustained_conflict(
		site: RetryableConflictSite,
		sql: &str,
	) -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, sql).await?;
		assert!(compaction_queue_len(&ds).await? > 0, "the writes queued a compaction");
		let node_id = ds.id();
		// More conflicts than any slice can consume, so no attempt commits.
		let guard = inject_retryable_conflicts(site, node_id, usize::MAX);

		let (batches, errors, first) = tokio::time::timeout(
			Duration::from_secs(30),
			Datastore::index_compaction_bounded(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
				Some(INDEX_COMPACTION_SLICE_BATCHES),
			),
		)
		.await
		.expect("a slice under sustained conflicts must return")?;
		assert_eq!(batches, 1);
		assert_eq!(errors, 1, "a pass that runs out of retries fails");
		assert_eq!(
			first.progress,
			Progress::Done,
			"the fresh request's commit wakes the next slice"
		);
		assert_eq!(
			retryable_conflict_count(site, node_id),
			usize::MAX - (INDEX_COMPACTION_PASS_MAX_RETRIES + 1),
			"the pass makes its attempt and its retries, and the slice no more"
		);
		assert_eq!(compaction_queue_len(&ds).await?, 1, "the pass queued one fresh request");

		// With the contention gone, the next slice compacts the index from the
		// request the pass queued.
		drop(guard);
		let (_, errors, second) = Datastore::index_compaction_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(INDEX_COMPACTION_SLICE_BATCHES),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(compaction_queue_len(&ds).await?, 0, "the queue is drained");
		Ok(())
	}

	#[tokio::test]
	async fn count_index_compaction_slice_yields_under_sustained_conflict() -> Result<()> {
		assert_index_compaction_slice_yields_under_sustained_conflict(
			RetryableConflictSite::CountCompaction,
			COUNT_COMPACTION_SQL,
		)
		.await
	}

	#[tokio::test]
	async fn fulltext_index_compaction_slice_yields_under_sustained_conflict() -> Result<()> {
		assert_index_compaction_slice_yields_under_sustained_conflict(
			RetryableConflictSite::FullTextCompaction,
			FULLTEXT_COMPACTION_SQL,
		)
		.await
	}

	#[tokio::test]
	async fn hnsw_index_compaction_slice_yields_under_sustained_conflict() -> Result<()> {
		assert_index_compaction_slice_yields_under_sustained_conflict(
			RetryableConflictSite::HnswCompaction,
			HNSW_COMPACTION_SQL,
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

	/// The entries an HNSW compaction still has to apply, across every pending
	/// layout.
	async fn hnsw_pending_len(ds: &Datastore, ikb: &IndexKeyBase) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let records = txn.getr(ikb.new_hr_range()?, None).await?.len()
			+ txn.getr(ikb.new_hw_range()?, None).await?.len();
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
	///
	/// Only an index with no stored format version (one an earlier release defined
	/// and nothing rebuilt) can hold the legacy append-keyed range, so the index is
	/// unstamped before any record is written to it.
	#[tokio::test]
	async fn hnsw_legacy_pending_delete_entry_is_not_where_evaluated() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE vec SCHEMALESS;
			 DEFINE INDEX hnsw_idx ON vec FIELDS vector HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;",
		)
		.await?;
		{
			let txn = ds.transaction(Write).await?;
			let ns = txn.get_ns_by_name("test", None).await?.expect("namespace should exist");
			let db =
				txn.get_db_by_name("test", "test", None).await?.expect("database should exist");
			let tb = TableName::from("vec");
			let ix = txn
				.get_tb_index(ns.namespace_id, db.database_id, &tb, "hnsw_idx", None)
				.await?
				.expect("index should exist");
			let mut unstamped = (*ix).clone();
			unstamped.format_version = 0;
			txn.put_tb_index(ns.namespace_id, db.database_id, &tb, &unstamped).await?;
			// Bump the table definition so cached index lists are refreshed.
			let tb_def = txn.expect_tb(ns.namespace_id, db.database_id, &tb).await?;
			txn.put_tb("test", "test", &tb_def).await?;
			txn.commit().await?;
		}
		execute_all(
			&ds,
			&session,
			"CREATE vec:2 SET vector = [5, 5], bad = 5 RETURN NONE;
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

	/// A staged add-only batch links against the committed graph under the
	/// graph read lock and publishes into the resident graph only on commit.
	/// When `apply_staged` fails before commit (for example because a vector
	/// read from the store is malformed), the resident graph has not been
	/// mutated and the loaded `HnswIndex` wrapper must stay cached rather than
	/// being evicted.
	#[tokio::test]
	async fn hnsw_staged_compaction_apply_error_keeps_the_loaded_index() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HNSW_COMPACTION_SQL).await?;
		let ikb = index_key_base(&ds, "vec", "hnsw_idx").await?;
		ds.process_index_compaction(&ikb, CancellationToken::new()).await?;

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

		// Queue an add-only pending operation so compaction takes the staged path.
		execute_all(&ds, &session, "CREATE vec:3 SET vector = [8, 9] RETURN NONE;").await?;
		assert_eq!(hnsw_pending_len(&ds, &ikb).await?, 1);

		// Evict the vector cache and corrupt the entry-point vector in the store
		// so `batch::build` inside `apply_staged` fails while reading the base graph.
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
			.expect_err("the malformed entry-point vector must fail the staged build");
		assert_eq!(
			hnsw_pending_len(&ds, &ikb).await?,
			1,
			"the failed staged apply must roll back its conditional pending delete"
		);

		let after = loaded_hnsw_index(&ds, "vec", "hnsw_idx").await?;
		assert!(
			Arc::ptr_eq(&before, &after),
			"a staged apply error leaves the resident graph untouched and must not evict the wrapper"
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
			Some(&lh),
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

	/// A batch whose queue cleanup keeps losing its commit to conflicts is
	/// left where it is once the cleanup's retries are spent: the slice keeps
	/// the batch's entries, reports them as its backlog and returns, rather
	/// than retrying until a commit wins. The next slice reaches the entries
	/// again, finds their index with nothing pending, and deletes them once
	/// its commit goes through.
	#[tokio::test]
	async fn index_compaction_slice_leaves_a_batch_whose_cleanup_keeps_conflicting() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, COUNT_COMPACTION_SQL).await?;
		let queued = compaction_queue_len(&ds).await?;
		assert!(queued > 0, "the writes queued a compaction");
		let site = RetryableConflictSite::IndexCompactionQueueCleanup;
		let node_id = ds.id();
		// More conflicts than any slice can consume, so no cleanup commits.
		let guard = inject_retryable_conflicts(site, node_id, usize::MAX);

		let (batches, errors, first) = tokio::time::timeout(
			Duration::from_secs(30),
			Datastore::index_compaction_bounded(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
				Some(INDEX_COMPACTION_SLICE_BATCHES),
			),
		)
		.await
		.expect("a slice whose cleanup keeps conflicting must return")?;
		assert_eq!(errors, 0, "the index compacted; only the cleanup was lost");
		assert_eq!(batches, 1, "the slice ends at the batch that kept entries");
		assert_eq!(first.progress, Progress::More);
		assert_eq!(
			retryable_conflict_count(site, node_id),
			usize::MAX - (INDEX_COMPACTION_PASS_MAX_RETRIES + 1),
			"the cleanup is attempted once and retried its bound"
		);
		assert_eq!(compaction_queue_len(&ds).await?, queued, "the batch keeps its entries");

		drop(guard);
		let (_, errors, second) = Datastore::index_compaction_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(INDEX_COMPACTION_SLICE_BATCHES),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(compaction_queue_len(&ds).await?, 0, "the queue is drained");
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
		COMPACTION_BATCH_HOOKS.lock().unwrap().insert(
			(node_id, BatchStage::Cleared),
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
		assert!(
			take_compaction_batch_hook(node_id, BatchStage::Cleared).is_none(),
			"the pass ran the hook"
		);
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
			basis: None,
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

	/// The sweep a drain runs on an empty queue walks the catalog a page at a
	/// time, each page charged to the slice as a queue batch is: a catalog of
	/// more steps than the slice's batches leaves the slice reporting more,
	/// with the walk kept, and the next slice continues the walk to its end
	/// rather than reading the catalog from its start.
	#[tokio::test]
	async fn a_compaction_sweep_walks_the_catalog_a_page_per_batch() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// Every table entered is a step of the walk.
		let mut sql = String::new();
		for n in 0..INDEX_COMPACTION_SWEEP_STEPS {
			sql.push_str(&format!("DEFINE TABLE t{n:04}; "));
		}
		execute_all(&ds, &session, &sql).await?;
		ds.expire_compaction_sweep();

		let (_, _, first) = Datastore::index_compaction_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(1),
		)
		.await?;
		assert_eq!(first.progress, Progress::More, "the walk outlasts the slice's one page");
		assert!(
			matches!(ds.compaction_cursor.lock().drain.sweep, DrainSweep::Walking(_)),
			"the walk is kept for the next slice"
		);

		let (_, _, second) = Datastore::index_compaction_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(INDEX_COMPACTION_SLICE_BATCHES),
		)
		.await?;
		assert_eq!(second.progress, Progress::Done, "the next slice finishes the walk");
		assert!(
			matches!(ds.compaction_cursor.lock().drain.sweep, DrainSweep::Unbegun),
			"and the drain ends with it"
		);
		assert!(ds.compaction_swept_at.lock().is_some(), "the completed sweep is throttled");
		Ok(())
	}

	/// A full-text, a `COUNT` and an HNSW index over one table, and the writes
	/// that leave each a backlog.
	const HOST_COMPACTION_SQL: &str = "
		DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
		DEFINE TABLE t SCHEMALESS;
		DEFINE INDEX ft ON t FIELDS text FULLTEXT ANALYZER simple BM25;
		DEFINE INDEX cx ON t COUNT;
		DEFINE INDEX hx ON t FIELDS vec HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 16 M 4;
	";

	/// The indexes of [`HOST_COMPACTION_SQL`].
	const HOST_COMPACTION_INDEXES: [&str; 3] = ["ft", "cx", "hx"];

	/// Queries each index of [`HOST_COMPACTION_SQL`] answers.
	const HOST_COMPACTION_QUERIES: [&str; 3] = [
		"SELECT text FROM t WHERE text @@ 'beta' ORDER BY text",
		"SELECT count() FROM t GROUP ALL",
		"SELECT vec FROM t WHERE vec <|3,40|> [1.0, 1.0] ORDER BY vec",
	];

	/// Writes `n` new records, each in a transaction of its own, so each queues
	/// a compaction request per index.
	async fn host_compaction_writes(ds: &Datastore, session: &Session, n: usize) -> Result<()> {
		for i in 0..n {
			execute_all(
				ds,
				session,
				&format!(
					"CREATE t SET text = 'alpha beta{i} beta', vec = [{i}.0, 1.0] RETURN NONE;"
				),
			)
			.await?;
		}
		Ok(())
	}

	/// What `INFO FOR INDEX` reports as `compacting` for `index` on `t`.
	async fn info_compacting(ds: &Datastore, session: &Session, index: &str) -> Result<bool> {
		let mut results =
			ds.execute(&format!("INFO FOR INDEX {index} ON t"), session, None).await?;
		let json = results.remove(0).result?.into_json_value();
		json.get("building")
			.and_then(|b| b.get("compacting"))
			.and_then(|c| c.as_bool())
			.ok_or_else(|| anyhow::anyhow!("INFO FOR INDEX reported no `compacting`: {json}"))
	}

	/// The answers of [`HOST_COMPACTION_QUERIES`].
	async fn host_compaction_answers(ds: &Datastore, session: &Session) -> Result<Vec<String>> {
		let mut answers = Vec::new();
		for sql in HOST_COMPACTION_QUERIES {
			let value = ds.execute(sql, session, None).await?.remove(0).result?;
			answers.push(value.into_json_value().to_string());
		}
		Ok(answers)
	}

	/// Calls [`Datastore::run_index_compaction`] as a host does, with a budget
	/// of one step, until a call reports no work left, and answers how many
	/// calls it took.
	async fn run_index_compaction_until_done(
		ds: &Arc<Datastore>,
		period: Duration,
	) -> Result<usize> {
		for calls in 1..=200 {
			let run = ds.run_index_compaction(1, period).await?;
			assert_eq!(run.retry_after, None, "{run:?}");
			if !run.more {
				return Ok(calls);
			}
		}
		anyhow::bail!("the host-driven drain did not end")
	}

	/// Deletes every request of the compaction queue, stranding the backlogs
	/// they named.
	async fn strand_compaction_backlogs(ds: &Datastore) -> Result<()> {
		let txn = ds.transaction(Write).await?;
		let keys =
			catch!(txn, txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await);
		for k in &keys {
			catch!(txn, txn.del(Key::from(k.as_slice())).await);
		}
		txn.commit().await
	}

	/// A host that drives compaction itself, a call at a time with a budget of
	/// one step and nothing else running, drains a full-text, `COUNT` and HNSW
	/// backlog that no read or statement folds: each index stops reporting
	/// `compacting`, the queue is empty, and every query answers as it did.
	#[tokio::test]
	async fn host_driven_compaction_drains_each_queued_kind() -> Result<()> {
		assert_host_driven_compaction_drains_each_queued_kind(false).await
	}

	/// [`host_driven_compaction_drains_each_queued_kind`] on a store other
	/// datastores write to, where the HNSW writes leave their shards' guards
	/// under this datastore's writer leases: the drain releases them, so the
	/// guards are stepped and the index stops reporting `compacting`.
	#[tokio::test]
	async fn host_driven_compaction_drains_each_queued_kind_on_a_shared_store() -> Result<()> {
		assert_host_driven_compaction_drains_each_queued_kind(true).await
	}

	async fn assert_host_driven_compaction_drains_each_queued_kind(
		shared_store: bool,
	) -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		if shared_store {
			ds.triggers.assume_shared_store();
		}
		execute_all(&ds, &session, HOST_COMPACTION_SQL).await?;
		host_compaction_writes(&ds, &session, 24).await?;
		assert!(compaction_queue_len(&ds).await? > 0, "the writes queued requests");
		for ix in HOST_COMPACTION_INDEXES {
			assert!(info_compacting(&ds, &session, ix).await?, "{ix} holds a backlog");
		}
		let before = host_compaction_answers(&ds, &session).await?;

		let first = ds.run_index_compaction(1, Duration::from_secs(60)).await?;
		assert!(first.passes > 0, "the first call passes the queued indexes: {first:?}");
		run_index_compaction_until_done(&ds, Duration::from_secs(60)).await?;

		assert_eq!(compaction_queue_len(&ds).await?, 0, "the queue is drained");
		for ix in HOST_COMPACTION_INDEXES {
			assert!(!info_compacting(&ds, &session, ix).await?, "{ix} is folded");
		}
		assert_eq!(host_compaction_answers(&ds, &session).await?, before);
		let idle = ds.run_index_compaction(1, Duration::from_secs(60)).await?;
		assert_eq!((idle.passes, idle.more), (0, false), "a drained queue runs no pass");
		Ok(())
	}

	/// On a store two datastores write, the HNSW guards one datastore's writes
	/// leave under its writer leases are not stepped by the other's compaction
	/// until the writer releases them: the writer reports when its leases go
	/// idle, its release reports what it released and that it holds none, and
	/// the release queues the request that lets the runner's next drain fold the
	/// index.
	#[tokio::test]
	async fn a_writers_lease_release_lets_the_runners_compaction_fold_its_guards() -> Result<()> {
		let (runner, session) = new_index_compaction_test_ds().await?;
		let writer = runner.fork_for_test_as_another_process(Uuid::now_v7());
		execute_all(&runner, &session, HOST_COMPACTION_SQL).await?;
		assert_eq!(writer.next_idle_writer_lease(), None, "no write has taken a lease");
		host_compaction_writes(&writer, &session, 8).await?;
		let next = writer.next_idle_writer_lease().expect("the HNSW writes took leases");
		assert!(next <= WRITER_LEASE_IDLE, "{next:?}");
		assert_eq!(runner.next_idle_writer_lease(), None, "the runner wrote nothing");

		run_index_compaction_until_done(&runner, Duration::from_secs(60)).await?;
		assert!(
			info_compacting(&runner, &session, "hx").await?,
			"the writer's leases cover its guards"
		);
		let held = writer.release_idle_writer_leases().await?;
		assert_eq!(held.released, 0, "no lease has been idle for {WRITER_LEASE_IDLE:?}");
		assert!(held.next_idle.is_some(), "{held:?}");

		let release = writer.release_writer_leases_idle_for(Duration::ZERO).await?;
		assert!(release.released > 0, "{release:?}");
		assert_eq!(release.next_idle, None, "the writer holds no lease");
		assert_eq!(writer.next_idle_writer_lease(), None);
		assert!(compaction_queue_len(&runner).await? > 0, "the release queued the index");

		run_index_compaction_until_done(&runner, Duration::from_secs(60)).await?;
		assert!(!info_compacting(&runner, &session, "hx").await?, "the runner folded the index");
		Ok(())
	}

	/// A commit that writes to each index queues a request naming it, notifies
	/// `index_compaction`, and every request is a key
	/// [`Datastore::is_index_compaction_key`] matches; the table's own keys —
	/// the record and the indexes' backlogs — do not match.
	#[tokio::test]
	async fn a_queued_compaction_is_signalled_and_its_request_key_recognised() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HOST_COMPACTION_SQL).await?;
		run_index_compaction_until_done(&ds, Duration::from_secs(60)).await?;
		assert_eq!(compaction_queue_len(&ds).await?, 0);
		let notified = ds.commit_triggers().index_compaction.notified();
		host_compaction_writes(&ds, &session, 1).await?;
		tokio::time::timeout(Duration::from_secs(5), notified)
			.await
			.map_err(|_| anyhow::anyhow!("the write did not wake index compaction"))?;
		let txn = ds.transaction(Read).await?;
		let requests =
			catch!(txn, txn.keys(IndexCompactionPrefix {}.range()?, u32::MAX, 0, None).await);
		let db = catch!(txn, txn.get_db_by_name("test", "test", None).await).expect("the database");
		let table = TableName::from("t");
		let under_table = crate::key::schema::TblRoot {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&table),
		}
		.range_subtree()?;
		let table_keys = catch!(txn, txn.keys_raw(under_table, u32::MAX, 0, None).await);
		txn.cancel().await?;
		let named: HashSet<_> = requests
			.iter()
			.map(|k| IndexCompactionKey::decode_key(k).map(|k| k.ix))
			.collect::<Result<_>>()?;
		assert_eq!(named.len(), HOST_COMPACTION_INDEXES.len(), "a request names each index");
		assert!(requests.iter().all(|k| Datastore::is_index_compaction_key(k)));
		assert!(!table_keys.is_empty());
		assert!(table_keys.iter().all(|k| !Datastore::is_index_compaction_key(k)));
		Ok(())
	}

	/// The host-driven drain sweeps for backlogs no request names on the first
	/// drain after the datastore starts, and after that at most once per its
	/// sweep period.
	#[tokio::test]
	async fn host_driven_compaction_sweeps_first_and_then_once_per_period() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, HOST_COMPACTION_SQL).await?;
		host_compaction_writes(&ds, &session, 4).await?;
		strand_compaction_backlogs(&ds).await?;
		assert!(info_compacting(&ds, &session, "ft").await?, "a backlog no request names");

		let period = Duration::from_secs(60);
		run_index_compaction_until_done(&ds, period).await?;
		for ix in HOST_COMPACTION_INDEXES {
			assert!(!info_compacting(&ds, &session, ix).await?, "the first drain swept {ix}");
		}

		host_compaction_writes(&ds, &session, 2).await?;
		strand_compaction_backlogs(&ds).await?;
		run_index_compaction_until_done(&ds, period).await?;
		assert!(
			info_compacting(&ds, &session, "ft").await?,
			"a drain within the period does not sweep again"
		);

		run_index_compaction_until_done(&ds, Duration::ZERO).await?;
		for ix in HOST_COMPACTION_INDEXES {
			assert!(!info_compacting(&ds, &session, ix).await?, "the period passed: {ix} swept");
		}
		Ok(())
	}

	/// A host-driven call dropped at a batch boundary — its batch read and
	/// nothing compacted, or its batch compacted and cleaned up — loses
	/// nothing: the calls that follow drain every backlog, and every query
	/// answers as it did.
	#[tokio::test]
	async fn a_dropped_host_driven_compaction_call_loses_nothing() -> Result<()> {
		for stage in [BatchStage::Read, BatchStage::Cleared] {
			let (ds, session) = new_index_compaction_test_ds().await?;
			execute_all(&ds, &session, HOST_COMPACTION_SQL).await?;
			host_compaction_writes(&ds, &session, 8).await?;
			let before = host_compaction_answers(&ds, &session).await?;
			COMPACTION_BATCH_HOOKS
				.lock()
				.unwrap()
				.insert((ds.id(), stage), Box::new(|| Box::pin(std::future::pending())));
			let dropped = tokio::time::timeout(
				Duration::from_millis(200),
				ds.run_index_compaction(1, Duration::from_secs(60)),
			)
			.await;
			assert!(dropped.is_err(), "the call waits at the hook until it is dropped");

			run_index_compaction_until_done(&ds, Duration::from_secs(60)).await?;
			assert_eq!(compaction_queue_len(&ds).await?, 0);
			for ix in HOST_COMPACTION_INDEXES {
				assert!(!info_compacting(&ds, &session, ix).await?, "{ix} is folded");
			}
			assert_eq!(host_compaction_answers(&ds, &session).await?, before);
		}
		Ok(())
	}

	/// A sweep that finds its lease taken by another node before a page stops
	/// there, reading no page, so the former holder probes and queues nothing
	/// once the lease is the new holder's; its walk stays where it stopped.
	#[tokio::test]
	async fn a_compaction_sweep_stops_at_a_lease_another_node_took() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(&ds, &session, "DEFINE TABLE t; DEFINE INDEX i ON t FIELDS f;").await?;
		ds.expire_compaction_sweep();
		let interval = Duration::from_secs(60);
		let lease =
			|ds: &Datastore| ds.lease_handler(TaskLeaseType::IndexCompaction, interval * 2, None);
		// The sweep's handler, whose first check reads the lease rather than
		// answering from its throttle, and another node that takes the lease.
		let lh = lease(&ds)?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		assert!(lease(&other)?.has_lease().await?, "the other node takes the lease");

		let mut drain = CompactionDrain::default();
		let mut read = 0;
		let swept = Datastore::sweep_after_drain(
			&ds,
			interval * 2,
			Some(&lh),
			&CancellationToken::new(),
			&mut drain,
			&mut read,
			None,
		)
		.await?;
		assert!(matches!(swept, Swept::LeaseLost), "the sweep reports the lease lost");
		assert_eq!(read, 0, "no page was read");
		assert!(
			matches!(&drain.sweep, DrainSweep::Walking(walk) if walk.has_more()),
			"the walk stays where it stopped"
		);
		assert!(ds.compaction_swept_at.lock().is_none(), "an unfinished sweep is not counted");
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

	/// A pass whose batch finds the lease passed to another node before its
	/// compactions start reports the lease elsewhere and leaves the batch's
	/// entries queued for the new holder: deleting them would drop requests
	/// for indexes the pass never compacted.
	#[tokio::test]
	async fn a_compaction_pass_keeps_its_batch_at_a_lease_another_node_took() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
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
			for _ in 0..3 {
				txn.set_key(&ikb.new_ic_key(node_id), &()).await?;
			}
			txn.commit().await?;
		}
		let queued = compaction_queue_len(&ds).await?;
		assert!(queued >= 3);
		// Another node takes the lease once the pass has read its batch.
		let hook_ds = Arc::clone(&ds);
		COMPACTION_BATCH_HOOKS.lock().unwrap().insert(
			(node_id, BatchStage::Read),
			Box::new(move || {
				Box::pin(async move {
					let lease = crate::catalog::TaskLease {
						owner: Uuid::from_u128(2),
						expiration: chrono::Utc::now() + chrono::Duration::seconds(120),
					};
					let key = crate::key::schema::TaskLeaseKey {
						task_id: TaskLeaseType::IndexCompaction,
					};
					let txn = hook_ds.transaction(Write).await.expect("a write transaction");
					txn.set_key(&key, &lease).await.expect("planting the lease");
					txn.commit().await.expect("committing the lease");
				})
			}),
		);

		let (batches, errors, pass) = Datastore::index_compaction_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(INDEX_COMPACTION_SLICE_BATCHES),
		)
		.await?;
		assert!(take_compaction_batch_hook(node_id, BatchStage::Read).is_none());
		assert_eq!((batches, errors), (0, 0), "the batch is left to the lease holder");
		assert_eq!(pass.lease, TaskLeaseState::Elsewhere);
		assert_eq!(pass.progress, Progress::Done);
		assert_eq!(compaction_queue_len(&ds).await?, queued, "the batch's entries stay queued");
		assert!(
			ds.compaction_cursor.lock().resume_at.is_none(),
			"the rotation stays where the batch began"
		);
		Ok(())
	}

	/// The compaction-queue drain must never delete more than one batch of
	/// `/!ic` entries per write transaction: the queue grows by one entry per
	/// index each committed write transaction touched, so an unbounded cleanup
	/// transaction would carry one write per accumulated entry — the per-key
	/// reservation profile that can starve distributed backends.
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
	/// Reads of a COUNT index sum every un-compacted `!iu` entry, so a
	/// per-document fan-out would make `count()` cost scale with write volume
	/// and the compaction queue grow at the same rate. Aggregating per
	/// transaction keeps the contention-free property the delta log exists for
	/// (the entry is still a blind write under a key no other transaction
	/// shares) without that scaling.
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
		let PublicValue::Array(a) = res.remove(0).result? else {
			panic!("expected an array");
		};
		Ok(a.len())
	}

	/// Fills this node's own queue with `count` entries this binary cannot
	/// decode, as a node on a newer release would leave behind. Their
	/// timestamps are the lowest possible, so they sort ahead of every event
	/// queued for the same table.
	async fn queue_undecodable_events(ds: &Datastore, count: u64) -> Result<()> {
		let txn = ds.transaction(Write).await?;
		let ns = txn.get_ns_by_name("test", None).await?.expect("the test namespace exists");
		let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database exists");
		for ts in 0..count {
			let key = NodeEventQueueKey {
				nd: ds.id(),
				ns: ns.namespace_id,
				db: db.database_id,
				tb: Cow::Owned("person".into()),
				ev: Cow::Owned("log".into()),
				ts,
			};
			txn.set(key.encode_key()?, b"not-an-async-event-record".to_vec()).await?;
		}
		txn.commit().await
	}

	/// Every key in the graph fold queue, in queue order.
	async fn fold_queue_keys(ds: &Datastore) -> Result<Vec<Vec<u8>>> {
		let txn = ds.transaction(Read).await?;
		let res = txn.keys(GraphFoldPrefix {}.range()?, u32::MAX, 0, None).await;
		let _ = txn.cancel().await;
		res
	}

	/// How many entries the index compaction queue holds.
	async fn compaction_queue_len(ds: &Datastore) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let res = txn.count(IndexCompactionPrefix {}.range()?, None).await;
		let _ = txn.cancel().await;
		res
	}

	/// How many unfolded adjacency keys one vertex's outgoing scope holds.
	async fn pointer_keys(ds: &Datastore, vertex: &crate::val::RecordId) -> Result<usize> {
		use crate::key::schema::GraphDirPrefix;
		let txn = ds.transaction(Read).await?;
		let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
		let range = GraphDirPrefix {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: crate::expr::Dir::Out,
		}
		.range()?;
		let res = txn.count(range, None).await;
		let _ = txn.cancel().await;
		res
	}

	/// How many entries node `nd`'s event queue holds, up to two.
	async fn queued_events(ds: &Datastore, nd: Uuid) -> Result<usize> {
		let range = NodeEventQueuePrefix {
			nd,
		}
		.range()?;
		let txn = ds.transaction(Read).await?;
		let n = txn.keys_raw(range, 2, 0, None).await?.len();
		txn.cancel().await?;
		Ok(n)
	}

	/// Whether the cluster still carries a row for `id`.
	async fn node_exists(ds: &Datastore, id: Uuid) -> Result<bool> {
		let txn = ds.transaction(Read).await?;
		let node = txn
			.get_key(
				&NodeKey {
					nd: id,
				},
				None,
			)
			.await?;
		txn.cancel().await?;
		Ok(node.is_some())
	}

	/// A heartbeat old enough that node cleanup removes an archived node's row
	/// rather than keeping it for a build that may still wait for the node's
	/// write barrier answer.
	fn lapsed_heartbeat(ds: &Datastore) -> Timestamp {
		ds.clock_now()
			- crate::kvs::write_barrier::exclude_after(
				surrealdb_datastore::triggers::WRITE_COMMIT_BOUND,
			) - Duration::from_secs(1)
	}

	/// Archives `id`, as the membership scan does once its heartbeat has gone
	/// stale, with its heartbeat past the write barrier's window.
	async fn archive_node(ds: &Datastore, id: Uuid) -> Result<()> {
		let key = NodeKey {
			nd: id,
		};
		let txn = ds.transaction(Write).await?;
		let node = txn.get_key(&key, None).await?.expect("the node is registered");
		let mut archived = node.archive();
		archived.heartbeat = lapsed_heartbeat(ds);
		txn.replace_key(&key, &archived).await?;
		txn.commit().await
	}

	/// Register `count` live queries for node `nd`, on a table that need not
	/// exist: cleanup deletes registrations, not subscriptions it looks up.
	async fn plant_node_live_queries(ds: &Datastore, nd: Uuid, count: usize) -> Result<()> {
		let txn = ds.transaction(Write).await?;
		for n in 0..count {
			let key = NodeLiveQueryKey {
				nd,
				lq: Uuid::from_u128(n as u128 + 1),
			};
			txn.set_key(
				&key,
				&NodeLiveQuery {
					ns: NamespaceId(1),
					db: DatabaseId(1),
					tb: TableName::from("doc"),
				},
			)
			.await?;
		}
		txn.commit().await
	}

	/// How many live queries node `nd` has registered.
	async fn node_live_query_count(ds: &Datastore, nd: Uuid) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let count = txn
			.count(
				NodeLiveQueryPrefix {
					nd,
				}
				.range()?,
				None,
			)
			.await?;
		txn.cancel().await?;
		Ok(count)
	}

	/// The heartbeat node `id`'s row carries, if it has one.
	async fn node_heartbeat(ds: &Datastore, id: Uuid) -> Result<Option<u64>> {
		let txn = ds.transaction(Read).await?;
		let node = txn.get_node(id).await?;
		txn.cancel().await?;
		Ok(node.map(|node| node.heartbeat.value))
	}

	/// The drain heartbeat written beside node `id`'s row, if any.
	async fn drain_heartbeat(ds: &Datastore, id: Uuid) -> Result<Option<u64>> {
		let key = NodeDrainHeartbeatKey {
			nd: id,
		};
		let txn = ds.transaction(Read).await?;
		let stamp = txn.get_key(&key, None).await?;
		txn.cancel().await?;
		Ok(stamp)
	}

	/// Refreshes this node's row the way a release that queues at the root
	/// does: a later heartbeat, and nothing written beside it.
	async fn heartbeat_without_drain_stamp(ds: &Datastore) -> Result<()> {
		let key = NodeKey {
			nd: ds.id(),
		};
		let txn = ds.transaction(Write).await?;
		let node = txn.get_key(&key, None).await?.expect("the node is registered");
		let later = Node::new(node.id, node.heartbeat + Duration::from_millis(1), false);
		txn.replace_key(&key, &later).await?;
		txn.commit().await
	}

	/// Rewrites this node's queued events under the root-level layout, as a
	/// node running the layout that preceded per-node queues would have written
	/// them.
	async fn relocate_queue_to_root(ds: &Datastore) -> Result<usize> {
		use crate::key::schema::EventQueueKey;

		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let txn = ds.transaction(Write).await?;
		let entries = txn.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await?;
		for (k, v) in &entries {
			let key = NodeEventQueueKey::decode_key(k)?;
			let record: AsyncEventRecord = KVValue::kv_decode_value(v, ())?;
			let legacy = EventQueueKey {
				ns: key.ns,
				db: key.db,
				tb: Cow::Owned(key.tb.into_owned()),
				ev: Cow::Owned(key.ev.into_owned()),
				ts: key.ts,
				node_id: key.nd,
			};
			txn.put_key(&legacy, &record).await?;
			txn.del(Key::from(k.as_slice())).await?;
		}
		txn.commit().await?;
		Ok(entries.len())
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

	/// Drains this node's queue the way the event task does, but with a bound,
	/// so a queue that stops draining fails the test instead of hanging it —
	/// which is the shape the failures under test take.
	async fn drain_event_queue(ds: &Datastore) -> Result<()> {
		for _ in 0..20 {
			if ds.event_processing_local(&CancellationToken::new()).await?.items == 0 {
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
			ds.event_processing_local(&CancellationToken::new()).await?.items,
			1,
			"the queued event must be the one entry this pass picks up"
		);
		assert_eq!(
			ds.event_processing_local(&CancellationToken::new()).await?.items,
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

	/// Reads this node's queue the way a batch does, without running anything.
	async fn fetch_event_queue(ds: &Datastore) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let tx = ds.transaction(Read).await?;
		let res = tx.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await;
		tx.cancel().await?;
		res
	}

	/// A batch that fetched an entry another consumer has since run must not
	/// run it again. Two processors can hold one entry — an adopter and an
	/// owner archived while still running, or the event task and a caller of
	/// `drain_async_events` — and only the one whose claim lands runs it.
	#[test_log::test(tokio::test)]
	async fn an_event_run_by_an_overlapping_batch_is_not_run_again() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 1).await?;

		let stale = fetch_event_queue(&ds).await?;
		assert_eq!(stale.len(), 1, "the queued event must be the one entry fetched");
		drain_event_queue(&ds).await?;
		assert_eq!(logged_count(&ds, &session).await?, 1, "the first batch runs the event");

		let batch = crate::doc::process_events_batch(&ds, stale, None).await?;
		assert_eq!(batch.held, 1, "the overlapping batch finds the entry gone rather than run it");
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event ran once");
		assert!(
			fetch_event_queue(&ds).await?.is_empty(),
			"the overlapping batch must not requeue an entry that already ran"
		);
		Ok(())
	}

	/// A commit that applied but reported an unknown outcome has run the event
	/// and removed its entry. Requeueing on that report would run the event
	/// twice: `RETRY 1` is what makes that observable, since a charged attempt
	/// would put the entry back for a second pass to run. The re-run that
	/// resolves the outcome finds the entry gone before evaluating the body.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_applied_unknown_is_not_run_again() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 1).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitAppliedUnknown;
		let _guard = inject_non_retryable_error(site, ds.id());

		assert_eq!(ds.event_processing_local(&CancellationToken::new()).await?.items, 1);
		assert_eq!(
			ds.event_processing_local(&CancellationToken::new()).await?.items,
			0,
			"the applied commit removed the entry, so nothing may have put it back"
		);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event must run exactly once");
		Ok(())
	}

	/// A re-run whose previous attempt applied finds the entry gone and ends
	/// without evaluating the body. The event's own writes cannot show the
	/// difference: a re-run that evaluated the body would lose the conditional
	/// delete and discard them. An effect the body has outside its
	/// transaction can. A sequence draw is one — the allocator advances
	/// whether or not the transaction that drew commits — so the draw after
	/// the pass counts the evaluations before it.
	#[test_log::test(tokio::test)]
	async fn a_re_run_that_finds_its_entry_gone_does_not_evaluate_the_body() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE logged SCHEMALESS;
			 DEFINE SEQUENCE draws START 0;
			 DEFINE EVENT log ON person ASYNC RETRY 0 \
			 THEN (CREATE logged SET draw = sequence::nextval('draws'));",
		)
		.await?;
		execute_all(&ds, &session, "CREATE person:1 RETURN NONE;").await?;

		let site = NonRetryableErrorSite::AsyncEventCommitAppliedUnknown;
		let _guard = inject_non_retryable_error(site, ds.id());

		assert_eq!(ds.event_processing_local(&CancellationToken::new()).await?.items, 1);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event's writes landed once");
		let mut res = ds.execute("RETURN sequence::nextval('draws');", &session, None).await?;
		assert_eq!(
			res.remove(0).result?,
			PublicValue::Number(surrealdb_types::Number::Int(1)),
			"the body drew once, so the re-run did not evaluate it"
		);
		Ok(())
	}

	/// A commit that reported an unknown outcome without applying has neither
	/// run the event nor removed its entry. Deleting the entry on that report
	/// would lose the event: `RETRY 0` is what makes that observable, since a
	/// charged attempt would exhaust the budget and delete it. The re-run that
	/// resolves the outcome finds the entry still as claimed and runs it.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_was_discarded_unknown_still_runs() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown;
		let _guard = inject_non_retryable_error(site, ds.id());

		assert_eq!(ds.event_processing_local(&CancellationToken::new()).await?.items, 1);
		assert_eq!(
			ds.event_processing_local(&CancellationToken::new()).await?.items,
			0,
			"the unknown outcome belongs to the pass that ran the event"
		);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event must still run, once");
		Ok(())
	}

	/// An outcome that stays unknown past the in-place bound is never charged
	/// to the `RETRY` budget: the entry is left as the last commit left it,
	/// with its claim released, and the pass fails with the unknown outcome
	/// rather than report the entry drained; a later pass runs it or finds it
	/// gone. `RETRY 0` is what makes that observable, since a charged attempt
	/// would delete the entry.
	#[test_log::test(tokio::test)]
	async fn an_event_whose_commit_outcome_stays_unknown_is_left_queued() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;

		let site = NonRetryableErrorSite::AsyncEventCommitDiscardedUnknown;
		// Every attempt of the first pass: the first run and each in-place re-run.
		let _guard = inject_non_retryable_errors(site, ds.id(), 4);

		assert!(
			ds.event_processing_local(&CancellationToken::new()).await.is_err(),
			"the pass fails with the unknown outcome"
		);
		assert_eq!(logged_count(&ds, &session).await?, 0, "no attempt of the first pass applied");
		let left = fetch_event_queue(&ds).await?;
		assert_eq!(left.len(), 1, "an unknown outcome must leave the entry queued, not delete it");
		let record = <AsyncEventRecord as KVValue>::kv_decode_value(&left[0].1, ())?;
		assert_eq!(record.claim, None, "with its claim released for the next pass");

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
		ds.event_processing_local(&canceller).await?;
		assert_eq!(
			logged_count(&ds, &session).await?,
			0,
			"a cancelled pass must not process the queue"
		);
		Ok(())
	}

	/// The control for the test above: with a live token the same queue drains,
	/// so the early return is cancellation and not a broken pass. This node's
	/// own queue needs no lease.
	#[test_log::test(tokio::test)]
	async fn event_processing_drains_the_queue_when_not_cancelled() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 50).await?;

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(
			logged_count(&ds, &session).await?,
			50,
			"an uncancelled pass must drain the queue"
		);
		assert_eq!(pass.items, 50, "every fetched entry is reported");
		assert_eq!(pass.progress, Progress::Done);
		assert_eq!(pass.lease, TaskLeaseState::NotLeased);
		Ok(())
	}

	/// A host that drives the queue calls the public pass until it reports no more, and every
	/// queued event runs once.
	#[test_log::test(tokio::test)]
	async fn process_async_events_drains_this_nodes_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 10).await?;
		let mut ran = 0;
		let mut passes = 0;
		loop {
			let pass = ds.process_async_events().await?;
			ran += pass.ran;
			passes += 1;
			assert!(passes < 10, "the queue did not drain");
			if pass.progress == Progress::Done {
				break;
			}
		}
		assert_eq!(ran, 10, "every queued event ran once");
		assert_eq!(logged_count(&ds, &session).await?, 10);
		let last = ds.process_async_events().await?;
		assert_eq!((last.ran, last.progress), (0, Progress::Done), "an empty queue reads as empty");
		Ok(())
	}

	/// Events of one batch run several at once on the calling task when the datastore asks for
	/// joined workers, and each runs once.
	#[test_log::test(tokio::test)]
	async fn joined_event_workers_run_a_batch_on_the_calling_task() -> Result<()> {
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_joined_event_workers(8)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		queue_async_events(&ds, &session, 20).await?;
		let pass = ds.process_async_events().await?;
		assert_eq!((pass.ran, pass.progress), (20, Progress::Done));
		assert_eq!(logged_count(&ds, &session).await?, 20, "every event ran once");
		Ok(())
	}

	/// A pass runs at most four batches of the size the datastore was built with, and reports
	/// that entries remain until a caller that repeats it has drained the queue.
	#[test_log::test(tokio::test)]
	async fn process_async_events_is_bounded_by_the_event_batch_size() -> Result<()> {
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_event_batch_size(2)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		queue_async_events(&ds, &session, 10).await?;
		let first = ds.process_async_events().await?;
		assert_eq!((first.ran, first.progress), (8, Progress::More), "{first:?}");
		assert_eq!(logged_count(&ds, &session).await?, 8, "a pass runs at most its batches");
		let second = ds.process_async_events().await?;
		assert_eq!((second.ran, second.progress), (2, Progress::Done), "{second:?}");
		assert_eq!(logged_count(&ds, &session).await?, 10, "every event runs once");
		Ok(())
	}

	/// A datastore told to queue under another node's queue writes its events there, names that
	/// queue's bounds, and drains it as its own.
	#[test_log::test(tokio::test)]
	async fn a_datastore_queues_and_drains_under_the_node_it_was_given() -> Result<()> {
		let own = Uuid::from_u128(1);
		let queue_node = Uuid::from_u128(2);
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_id(own)
			.with_event_queue_node(queue_node)
			.build_with_path("memory")
			.await?;
		let session = Session::owner().with_ns("test").with_db("test");
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		queue_async_events(&ds, &session, 3).await?;

		let queued_under = |nd: Uuid| {
			let ds = &ds;
			async move {
				let range = NodeEventQueuePrefix {
					nd,
				}
				.range()?;
				let tx = ds.transaction(Read).await?;
				let keys = tx.keys(range, 10, 0, None).await;
				tx.cancel().await?;
				anyhow::Ok(keys?.len())
			}
		};
		assert_eq!(queued_under(queue_node).await?, 3, "the events join the named node's queue");
		assert_eq!(queued_under(own).await?, 0, "none joins the datastore's own");
		let (start, _) = ds.async_event_queue_bounds()?;
		let named = NodeEventQueuePrefix {
			nd: queue_node,
		}
		.range()?;
		assert_eq!(start, named.start().as_slice().to_vec(), "the bounds are the named queue's");
		let idle = ds.event_processing_job(&CancellationToken::new()).await?;
		assert_eq!(idle.items, 0, "the background job drains only a queue the node owns");
		assert_eq!(queued_under(queue_node).await?, 3, "so it leaves the named queue to its owner");
		let pass = ds.process_async_events().await?;
		assert_eq!((pass.ran, pass.progress), (3, Progress::Done));
		assert_eq!(logged_count(&ds, &session).await?, 3, "the datastore drains that queue");
		Ok(())
	}

	/// Entries a release that queued at the root left are run by the legacy pass, which a node's
	/// own pass does not reach.
	#[test_log::test(tokio::test)]
	async fn the_legacy_pass_runs_the_entries_queued_at_the_root() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 3).await?;
		assert_eq!(relocate_queue_to_root(&ds).await?, 3);

		let own = ds.process_async_events().await?;
		assert_eq!((own.ran, own.progress), (0, Progress::Done), "the node's queue is empty");
		assert_eq!(logged_count(&ds, &session).await?, 0, "the root queue is not the node's");
		let legacy = ds.process_legacy_async_events().await?;
		assert_eq!((legacy.ran, legacy.progress), (3, Progress::Done), "{legacy:?}");
		assert_eq!(logged_count(&ds, &session).await?, 3, "each entry runs once");
		let again = ds.process_legacy_async_events().await?;
		assert_eq!((again.items, again.progress), (0, Progress::Done), "the root queue is empty");
		Ok(())
	}

	/// A commit that queues an event writes a key within the bounds of this node's queue.
	#[test_log::test(tokio::test)]
	async fn a_queued_event_lies_within_the_queue_bounds() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 1).await?;
		let (start, end) = ds.async_event_queue_bounds()?;
		let range = NodeEventQueuePrefix {
			nd: ds.id,
		}
		.range()?;
		let tx = ds.transaction(Read).await?;
		let keys = tx.keys(range, 10, 0, None).await;
		tx.cancel().await?;
		let keys = keys?;
		assert_eq!(keys.len(), 1, "one event is queued");
		assert!(keys.iter().all(|k| *k >= start && *k < end), "{keys:?} lies outside the bounds");
		Ok(())
	}

	/// Entries this binary cannot run stay in place, so a pass that ends at a
	/// full batch of them and reports a backlog must not have the drain task,
	/// or [`Datastore::drain_async_events`], fetch the same batch without end:
	/// the pass moves past them to the queue's end, and reports it drained.
	#[test_log::test(tokio::test)]
	async fn a_full_batch_of_unrunnable_entries_is_not_a_backlog() -> Result<()> {
		let (ds, _session) = new_index_compaction_test_ds().await?;
		queue_undecodable_events(&ds, NORMAL_BATCH_SIZE as u64).await?;

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, NORMAL_BATCH_SIZE as u64, "the entries were fetched");
		assert_eq!(pass.progress, Progress::Done, "but nothing ran, so there is no backlog");
		assert_eq!(ds.drain_async_events().await?, NORMAL_BATCH_SIZE as u64, "and a drain returns");
		Ok(())
	}

	/// Entries this binary cannot run stay in place for a node that can, so a
	/// full batch of them reads the same on every fetch. The drain must resume
	/// past such a batch rather than start over, or the events queued behind
	/// it — as an older node adopting a newer node's queue finds them — would
	/// never run.
	#[test_log::test(tokio::test)]
	async fn event_processing_reaches_events_behind_a_full_undecodable_batch() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 0).await?;
		queue_undecodable_events(&ds, NORMAL_BATCH_SIZE as u64).await?;
		execute_all(&ds, &session, "CREATE person RETURN NONE;").await?;
		// The runnable event sorts behind the full batch.
		let entries = {
			let txn = ds.transaction(Read).await?;
			let range = NodeEventQueuePrefix {
				nd: ds.id(),
			}
			.range()?;
			let res = txn.scan_raw(range, NORMAL_BATCH_SIZE + 1, 0, None).await;
			txn.cancel().await?;
			res?
		};
		assert_eq!(entries.len(), NORMAL_BATCH_SIZE as usize + 1);
		let decodes = |v: &[u8]| <AsyncEventRecord as KVValue>::kv_decode_value(v, ()).is_ok();
		assert!(!decodes(&entries[0].1), "the batch ahead must be undecodable");
		assert!(decodes(&entries[NORMAL_BATCH_SIZE as usize].1), "the event behind it decodes");

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, NORMAL_BATCH_SIZE as u64 + 1, "both batches were fetched");
		assert_eq!(pass.progress, Progress::Done, "a batch nothing ran in is no backlog");
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event behind the batch ran");
		Ok(())
	}

	/// A pass over entries this binary cannot run is bounded like any other:
	/// every full batch spends budget, so more than a budget's worth of them
	/// ends the pass with a backlog. The next pass then resumes where that one
	/// stopped rather than at the head, so it reaches the event behind them
	/// instead of reading the same prefix again.
	#[test_log::test(tokio::test)]
	async fn a_pass_over_unrunnable_entries_is_bounded_and_resumed() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 0).await?;
		let budget = (EVENT_PROCESSING_SLICE_BATCHES * NORMAL_BATCH_SIZE as usize) as u64;
		queue_undecodable_events(&ds, budget + 1).await?;
		execute_all(&ds, &session, "CREATE person RETURN NONE;").await?;

		let first = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(first.items, budget, "the pass stopped at its budget");
		assert_eq!(first.progress, Progress::More, "with the queue still full");
		assert_eq!(logged_count(&ds, &session).await?, 0);

		let second = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(second.items, 2, "the next pass resumed past the entries already read");
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(logged_count(&ds, &session).await?, 1, "and ran the event behind them");
		assert!(ds.event_laps.lock().is_empty(), "reaching the end ends the lap");
		Ok(())
	}

	/// An event that fails with retries left is written back in place, and its
	/// next attempt is owed at once: the pass that requeued it reports a
	/// backlog, so the drain runs another pass now rather than at the next
	/// tick, and keeps doing so until the entry runs or its retries are spent.
	#[test_log::test(tokio::test)]
	async fn a_requeued_event_is_run_again_on_the_next_pass() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE EVENT fail ON person ASYNC RETRY 2 THEN { THROW 'every time' };
			 CREATE person:1 RETURN NONE;",
		)
		.await?;

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, 1);
		assert_eq!(pass.progress, Progress::More, "the requeued entry is owed another attempt");
		assert_eq!(queued_events(&ds, ds.id()).await?, 1, "the entry was written back in place");

		// The two attempts left: one more requeue, then the retries are spent
		// and the entry leaves the queue.
		assert_eq!(ds.drain_async_events().await?, 2);
		assert_eq!(queued_events(&ds, ds.id()).await?, 0);
		assert!(ds.event_laps.lock().is_empty());
		Ok(())
	}

	/// A requeue in the batch that spends the last of a pass's budget, at the
	/// end of the lap, is still owed its next attempt: the lap carries the
	/// requeue to the next pass, which finds the queue's end, reports the
	/// backlog, and the pass after that starts from the head and runs it.
	#[test_log::test(tokio::test)]
	async fn a_requeue_is_remembered_across_the_passes_of_a_lap() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// Exactly a budget's worth of full batches, the last of them failing
		// events: keys sort by table name, so the `b` events come last.
		let ok = (EVENT_PROCESSING_SLICE_BATCHES - 1) * NORMAL_BATCH_SIZE as usize;
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE TABLE a SCHEMALESS;
				 DEFINE TABLE b SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT log ON a ASYNC THEN (CREATE logged SET who = $after.id);
				 DEFINE EVENT fail ON b ASYNC RETRY 1 THEN {{ THROW 'every time' }};
				 CREATE |a:{ok}| RETURN NONE;
				 CREATE |b:{}| RETURN NONE;",
				NORMAL_BATCH_SIZE
			),
		)
		.await?;

		let first = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(first.items, (ok + NORMAL_BATCH_SIZE as usize) as u64, "every batch fetched");
		assert_eq!(first.progress, Progress::More, "the budget is spent");
		assert_eq!(logged_count(&ds, &session).await?, ok);

		let second = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(second.items, 0, "the lap's end holds nothing new");
		assert_eq!(second.progress, Progress::More, "but the lap's requeues are owed a run");

		let third = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(third.items, NORMAL_BATCH_SIZE as u64, "the requeued entries, from the head");
		assert_eq!(third.progress, Progress::Done, "their retries are spent");
		assert_eq!(queued_events(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A requeue in a full batch is owed its next attempt even when the short
	/// batch that ends the pass requeued nothing: the backlog is the pass's,
	/// not the last batch's.
	#[test_log::test(tokio::test)]
	async fn a_requeue_behind_a_full_batch_is_a_backlog() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// Queue keys sort by table name, so a full batch of failing `a` events
		// is fetched before the one `b` event that runs.
		execute_all(
			&ds,
			&session,
			&format!(
				"DEFINE TABLE a SCHEMALESS;
				 DEFINE TABLE b SCHEMALESS;
				 DEFINE TABLE logged SCHEMALESS;
				 DEFINE EVENT fail ON a ASYNC RETRY 1 THEN {{ THROW 'every time' }};
				 DEFINE EVENT log ON b ASYNC THEN (CREATE logged SET who = $after.id);
				 CREATE |a:{}| RETURN NONE;
				 CREATE b:1 RETURN NONE;",
				NORMAL_BATCH_SIZE
			),
		)
		.await?;

		let first = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(first.items, NORMAL_BATCH_SIZE as u64 + 1, "both batches were fetched");
		assert_eq!(logged_count(&ds, &session).await?, 1, "the short batch's event ran");
		assert_eq!(first.progress, Progress::More, "the full batch's requeues are a backlog");

		// The next pass starts a lap from the head, spends the failing events'
		// last retry, and finds the queue at rest.
		let second = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(second.items, NORMAL_BATCH_SIZE as u64);
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(queued_events(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// An event runs on the node that drains it and queues what it queues
	/// under that node's own queue: after the fetch that would have found the
	/// entry and, when its table sorts earlier, behind the scan's position
	/// too. A lap during which this node queued an entry therefore ends at
	/// the head rather than at rest, so one pass runs a chain of events to
	/// its end and `drain_async_events` returns with the queue empty.
	#[test_log::test(tokio::test)]
	async fn a_drain_runs_the_events_its_events_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// Each hop queues an event on a table that sorts before its own.
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE x SCHEMALESS;
			 DEFINE TABLE y SCHEMALESS;
			 DEFINE TABLE z SCHEMALESS;
			 DEFINE TABLE logged SCHEMALESS;
			 DEFINE EVENT hop ON z ASYNC MAXDEPTH 3 THEN (CREATE y:1);
			 DEFINE EVENT hop ON y ASYNC MAXDEPTH 3 THEN (CREATE x:1);
			 DEFINE EVENT hop ON x ASYNC MAXDEPTH 3 THEN (CREATE logged SET who = $after.id);
			 CREATE z:1 RETURN NONE;",
		)
		.await?;

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, 3, "each hop's entry, found at the head once the last settled");
		assert_eq!(pass.progress, Progress::Done);
		assert_eq!(logged_count(&ds, &session).await?, 1, "the chain ran to its end");
		assert_eq!(queued_events(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A queue whose tail grows at least as fast as the drain reads it never
	/// yields a short batch, so the lap's end is what gives the head its turn:
	/// once the scan has passed the last key the queue held when the lap
	/// began, the next lap starts from the head, where entries requeued for
	/// another attempt and entries queued since for an earlier-sorting event
	/// wait.
	#[test_log::test(tokio::test)]
	async fn an_event_lap_wraps_to_the_head_once_past_the_end_it_began_with() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 0).await?;
		// A full batch at the head this binary cannot run, then two full
		// batches of events behind it.
		queue_undecodable_events(&ds, NORMAL_BATCH_SIZE as u64).await?;
		execute_all(
			&ds,
			&session,
			&format!("CREATE |person:{}| RETURN NONE;", 2 * NORMAL_BATCH_SIZE),
		)
		.await?;
		let keys = {
			let txn = ds.transaction(Read).await?;
			let range = NodeEventQueuePrefix {
				nd: ds.id(),
			}
			.range()?;
			let res = txn.keys(range, u32::MAX, 0, None).await;
			txn.cancel().await?;
			res?
		};
		let batch = NORMAL_BATCH_SIZE as usize;
		assert_eq!(keys.len(), 3 * batch);
		// The lap began when the queue held the head and the first batch of
		// events, and the previous pass read the head; the second batch of
		// events arrived since.
		ds.event_laps.lock().insert(
			EventQueue::Node(ds.id()),
			EventLap {
				after: Some(keys[batch - 1].clone()),
				end: keys[2 * batch - 1].clone(),
				requeued: false,
				queued: 0,
			},
		);

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(logged_count(&ds, &session).await?, 2 * batch, "every event ran");
		assert_eq!(
			pass.items,
			3 * batch as u64,
			"the two batches of events, then the head again once the lap's end was passed"
		);
		assert_eq!(pass.progress, Progress::Done);
		assert!(ds.event_laps.lock().is_empty(), "the queue's end ends the lap");
		Ok(())
	}

	/// The stalled-build scan pages through the catalog in catalog order
	/// across passes: a pass stops at its page of steps with the rest left for
	/// the next, which continues the walk where it stopped, and the pass that
	/// reaches the end drops the walk for the one after to begin afresh.
	#[test_log::test(tokio::test)]
	async fn stalled_build_scan_pages_through_the_catalog() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		// A page and a few more, split unevenly across two tables so the page
		// boundary falls inside the second: the namespace, the database and
		// each table entered are steps of the page too.
		let per_table = INDEX_RESUME_PAGE_SIZE / 2 + 2;
		let mut sql = String::from("DEFINE TABLE aaa SCHEMALESS; DEFINE TABLE zzz SCHEMALESS;");
		for table in ["aaa", "zzz"] {
			for n in 0..per_table {
				sql.push_str(&format!(" DEFINE INDEX i{n} ON {table} FIELDS f{n};"));
			}
		}
		execute_all(&ds, &session, &sql).await?;

		let scan =
			|| ds.resume_stalled_index_builds(Duration::from_secs(60), CancellationToken::new());
		let first = scan().await?;
		assert_eq!(first.progress, Progress::More, "a page was left for the next pass");
		assert!(
			ds.resume_walk.lock().as_ref().is_some_and(IndexWalk::has_more),
			"the walk is kept for the sweep's next page"
		);
		let second = scan().await?;
		assert_eq!(second.progress, Progress::Done, "the rest fits in one page");
		assert!(ds.resume_walk.lock().is_none(), "reaching the end drops the walk");
		let third = scan().await?;
		assert_eq!(third.progress, Progress::More, "the scan starts over");
		assert!(ds.resume_walk.lock().is_some());
		Ok(())
	}

	/// A resume pass that finds its lease passed to another node stops at the
	/// candidate it found so and reports the lease elsewhere, so the former
	/// holder does not adopt builds alongside the new one; the candidates it
	/// did not check go back to its walk, which reaches them first.
	#[test_log::test(tokio::test)]
	async fn a_resume_pass_stops_at_a_lease_another_node_took() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE t SCHEMALESS; DEFINE INDEX a ON t FIELDS a; DEFINE INDEX b ON t FIELDS b;",
		)
		.await?;
		let mut walk = IndexWalk::default();
		let page = ds.resume_page(&mut walk).await?;
		let names = |page: &[WalkedIndex]| -> Vec<String> {
			page.iter().map(|candidate| candidate.index.name.to_string()).collect()
		};
		assert_eq!(names(&page), ["a", "b"]);

		let lease = |ds: &Datastore| {
			ds.lease_handler(TaskLeaseType::IndexBuildResume, Duration::from_secs(60), None)
		};
		// The pass's handler, whose first check reads the lease rather than
		// answering from its throttle, and another node that takes the lease.
		let lh = lease(&ds)?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		assert!(lease(&other)?.has_lease().await?, "the other node takes the lease");

		let mut pass = Pass::done(TaskLeaseState::Held);
		ds.adopt_stalled_build(
			&lh,
			&CancellationToken::new(),
			page,
			ResumeScope::Any,
			&mut walk,
			&mut pass,
		)
		.await?;
		assert_eq!(pass.lease, TaskLeaseState::Elsewhere, "the pass reports the lease lost");
		assert_eq!(pass.items, 0);
		assert_eq!(
			names(&ds.resume_page(&mut walk).await?),
			["a", "b"],
			"the unchecked candidates go back to the walk"
		);
		Ok(())
	}

	/// A live node's queue is its own to drain: the leased adoption sweep on
	/// another node leaves it untouched, so a cluster's event throughput does
	/// not serialise through one lease holder.
	#[test_log::test(tokio::test)]
	async fn event_adoption_leaves_a_live_nodes_queue_alone() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.lease, TaskLeaseState::Held);
		assert_eq!(pass.items, 0, "a live node's queue is not adopted");
		assert_eq!(logged_count(&ds, &session).await?, 0);
		// The owner drains it.
		ds.drain_async_events().await?;
		assert_eq!(logged_count(&ds, &session).await?, 5);
		Ok(())
	}

	/// Once a node is archived, the sweep adopts its queue.
	#[test_log::test(tokio::test)]
	async fn event_adoption_drains_an_archived_nodes_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;
		archive_node(&ds, ds.id()).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.lease, TaskLeaseState::Held);
		assert_eq!(pass.items, 5);
		assert_eq!(logged_count(&other, &session).await?, 5);
		Ok(())
	}

	/// The sweep finds a queue by its own prefix, not through its owner's row,
	/// so a queue whose owner's row is already gone — deleted by cleanup on
	/// this release, or by a node on a release that knows nothing of per-node
	/// queues — is still adopted rather than orphaned.
	#[test_log::test(tokio::test)]
	async fn event_adoption_drains_the_queue_of_a_node_whose_row_is_gone() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;
		archive_node(&ds, ds.id()).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		other.remove_nodes().await?;
		assert!(!node_exists(&other, ds.id()).await?, "cleanup does not wait for the queue");

		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.lease, TaskLeaseState::Held);
		assert_eq!(pass.items, 5);
		assert_eq!(logged_count(&other, &session).await?, 5);
		Ok(())
	}

	/// Node cleanup deletes at most a slice of an archived node's live queries
	/// per pass, a page per commit, and a pass that stops with a node still to
	/// clean reports `More`.
	///
	/// A departed node can leave any number of live queries behind, and
	/// deleting them all in one transaction per node makes one pass as long,
	/// and as large, as the node's registrations, in a slice a host driving
	/// maintenance itself waits on. What a pass deleted is committed, so the
	/// next pass continues from the head of what remains, and the node's row
	/// goes only with its last live queries.
	#[test_log::test(tokio::test)]
	async fn node_cleanup_is_paged_across_passes() -> Result<()> {
		let (ds, _) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		let page = NORMAL_BATCH_SIZE as usize;
		// One live query more than a page.
		plant_node_live_queries(&ds, ds.id(), page + 1).await?;
		archive_node(&ds, ds.id()).await?;

		// A budget of one page: the pass stops with a live query, and the row,
		// still to delete.
		let first = ds.remove_nodes_with_budget(NODE_CLEANUP_SLICE_BATCHES, page as u64).await?;
		assert_eq!(first.progress, Progress::More, "the pass stopped with the node still to clean");
		assert_eq!(first.items, page as u64);
		assert_eq!(node_live_query_count(&ds, ds.id()).await?, 1);
		assert!(node_exists(&ds, ds.id()).await?, "a node with live queries left keeps its row");

		let second = ds.remove_nodes_with_budget(NODE_CLEANUP_SLICE_BATCHES, page as u64).await?;
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(second.items, 2, "the last live query, and the row");
		assert_eq!(node_live_query_count(&ds, ds.id()).await?, 0);
		assert!(!node_exists(&ds, ds.id()).await?);
		Ok(())
	}

	/// Register `count` archived nodes with no live queries, their heartbeats
	/// past the write barrier's window. Their ids sort before any node id
	/// drawn from the clock, so they fill the head of the node range.
	async fn plant_archived_nodes(ds: &Datastore, count: usize) -> Result<()> {
		let now = lapsed_heartbeat(ds);
		let txn = ds.transaction(Write).await?;
		for n in 0..count {
			let id = Uuid::from_u128(n as u128 + 1);
			let key = NodeKey {
				nd: id,
			};
			txn.set_key(&key, &Node::new(id, now, false).archive()).await?;
		}
		txn.commit().await
	}

	/// How many node rows the cluster carries.
	async fn node_row_count(ds: &Datastore) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let nodes = txn.all_nodes().await?;
		txn.cancel().await?;
		Ok(nodes.len())
	}

	/// Node cleanup scans at most a slice of node rows per pass, so a pass
	/// over archived nodes that left nothing behind is bounded by the rows it
	/// reads as well as by the keys it deletes, and a pass that stops with
	/// rows still to scan reports `More`.
	///
	/// The archived nodes were listed from every row of the cluster on every
	/// pass, so the listing alone was as long as the membership, in a slice a
	/// host driving maintenance itself waits on. A pass now keeps its
	/// position between passes, and a row registered while the scan runs is
	/// the next scan's.
	#[test_log::test(tokio::test)]
	async fn node_cleanup_discovery_is_paged_across_passes() -> Result<()> {
		let (ds, _) = new_index_compaction_test_ds().await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let rows = node_row_count(&ds).await?;
		// One archived node more than a page, none with a live query.
		plant_archived_nodes(&ds, page + 1).await?;

		// One page of rows, and keys enough for every node: the pass stops
		// with a page still to scan.
		let first = ds.remove_nodes_with_budget(1, u64::MAX).await?;
		assert_eq!(first.progress, Progress::More, "the pass stopped with a page still to scan");
		assert_eq!(first.items, page as u64, "the archived nodes on the page are cleaned");
		assert_eq!(node_row_count(&ds).await?, rows + 1);

		let second = ds.remove_nodes_with_budget(1, u64::MAX).await?;
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(second.items, 1);
		assert_eq!(node_row_count(&ds).await?, rows);
		Ok(())
	}

	/// Write `count` release records for nodes that have no row. Their ids
	/// sort before any node id drawn from the clock, so they fill the head of
	/// the record range.
	async fn plant_orphaned_node_releases(ds: &Datastore, count: usize) -> Result<()> {
		let now = ds.clock_now();
		let txn = ds.transaction(Write).await?;
		for n in 0..count {
			let key = NodeReleaseKey {
				nd: Uuid::from_u128(n as u128 + 1),
			};
			let record = NodeRelease {
				release: Release::CURRENT,
				heartbeat: now,
			};
			txn.set_key(&key, &record).await?;
		}
		txn.commit().await
	}

	/// How many node release records the cluster carries.
	async fn node_release_count(ds: &Datastore) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let mut count = 0;
		let mut range = Some(NodeReleasePrefix {}.range()?);
		while let Some(rng) = range {
			let res = txn.batch_keys_raw(rng.clone(), NORMAL_BATCH_SIZE, None).await?;
			count += res.result.len();
			range = match (&res.next, res.result.last()) {
				(Some(_), Some(last)) => Some(rng.resume_after(last, Direction::Forward)),
				_ => None,
			};
		}
		txn.cancel().await?;
		Ok(count)
	}

	/// The sweep of release records whose node row is gone reads at most the
	/// pages a node cleanup pass has left once its rows are scanned, so a
	/// pass over a cluster that left many records behind is bounded by the
	/// pages it reads, and a pass that stops with records still to read
	/// reports `More` and continues the sweep, not the row scan, next.
	///
	/// The sweep ran whole at the end of the row scan, so a pass bounded in
	/// the rows it read was as long as the record range, in a slice a host
	/// driving maintenance itself waits on.
	#[test_log::test(tokio::test)]
	async fn node_cleanup_release_sweep_is_paged_across_passes() -> Result<()> {
		let (ds, _) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let records = node_release_count(&ds).await?;
		// One orphaned record more than a page.
		plant_orphaned_node_releases(&ds, page + 1).await?;

		// One page a pass: the first reads the node rows and has none left
		// for the records.
		let first = ds.remove_nodes_with_budget(1, u64::MAX).await?;
		assert_eq!(first.progress, Progress::More, "the pass stopped with the records to sweep");
		assert_eq!(first.items, 0);
		assert_eq!(node_release_count(&ds).await?, records + page + 1);

		let second = ds.remove_nodes_with_budget(1, u64::MAX).await?;
		assert_eq!(second.progress, Progress::More, "the pass stopped with a page still to read");
		assert_eq!(second.items, page as u64, "the orphaned records on the page are deleted");
		assert_eq!(node_release_count(&ds).await?, records + 1);

		let third = ds.remove_nodes_with_budget(1, u64::MAX).await?;
		assert_eq!(third.progress, Progress::Done);
		assert_eq!(third.items, 1);
		assert_eq!(node_release_count(&ds).await?, records, "a live node keeps its record");
		Ok(())
	}

	/// The sweep deletes no more orphaned records than the keys a pass has
	/// left, and a pass that stops with orphans left on a page reads the page
	/// again next, finding only what is left.
	#[test_log::test(tokio::test)]
	async fn node_cleanup_release_sweep_is_bounded_by_keys() -> Result<()> {
		let (ds, _) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		let records = node_release_count(&ds).await?;
		plant_orphaned_node_releases(&ds, 3).await?;

		// Keys for two records a pass.
		let first = ds.remove_nodes_with_budget(NODE_CLEANUP_SLICE_BATCHES, 2).await?;
		assert_eq!(first.progress, Progress::More, "the pass stopped with a record left");
		assert_eq!(first.items, 2);
		assert_eq!(node_release_count(&ds).await?, records + 1);

		let second = ds.remove_nodes_with_budget(NODE_CLEANUP_SLICE_BATCHES, 2).await?;
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(second.items, 1);
		assert_eq!(node_release_count(&ds).await?, records);
		Ok(())
	}

	/// Register `count` nodes whose heartbeats are a minute old, so every one
	/// of them is expired. Their ids sort before any node id drawn from the
	/// clock, so they fill the head of the node range.
	async fn plant_expired_nodes(ds: &Datastore, count: usize) -> Result<()> {
		let stale = Timestamp {
			value: ds.clock_now().value.saturating_sub(60_000),
		};
		let txn = ds.transaction(Write).await?;
		for n in 0..count {
			let id = Uuid::from_u128(n as u128 + 1);
			let key = NodeKey {
				nd: id,
			};
			txn.set_key(&key, &Node::new(id, stale, false)).await?;
		}
		txn.commit().await
	}

	/// How many registered nodes are active.
	async fn active_node_count(ds: &Datastore) -> Result<usize> {
		let txn = ds.transaction(Read).await?;
		let nodes = txn.all_nodes().await?;
		txn.cancel().await?;
		Ok(nodes.iter().filter(|n| n.is_active()).count())
	}

	/// Node expiry scans at most a slice of node rows per pass, archiving the
	/// expired nodes on each page in a commit of its own, and a pass that
	/// stops with pages still to scan reports `More`.
	///
	/// Every registered node keeps a row until cleanup removes it, and expiry
	/// read them all and archived every expired one in one transaction, so a
	/// pass was as long as the membership, in a slice a host driving
	/// maintenance itself waits on. A pass now keeps its position between
	/// passes, and a row registered while the scan runs is the next scan's.
	#[test_log::test(tokio::test)]
	async fn node_expiry_is_paged_across_passes() -> Result<()> {
		let (ds, _) = new_index_compaction_test_ds().await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let live = active_node_count(&ds).await?;
		// One expired node more than a page.
		plant_expired_nodes(&ds, page + 1).await?;

		// A budget of one page: the pass stops with a page still to scan.
		let first = ds.expire_nodes_with_budget(1).await?;
		assert_eq!(first.progress, Progress::More, "the pass stopped with a page still to scan");
		assert_eq!(first.items, page as u64, "the expired nodes on the page are archived");
		assert_eq!(active_node_count(&ds).await?, live + 1);

		let second = ds.expire_nodes_with_budget(1).await?;
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(second.items, 1);
		assert_eq!(active_node_count(&ds).await?, live);
		Ok(())
	}

	/// The node-state level puts the node id right after its prefix, whatever
	/// key lies beneath the node, which is what lets the sweep name the node
	/// of a key it cannot decode.
	#[test]
	fn a_node_state_key_names_its_node_first() -> Result<()> {
		let nd = Uuid::from_u128(7);
		let queue = NodeEventQueueKey {
			nd,
			ns: NamespaceId(1),
			db: DatabaseId(2),
			tb: Cow::Owned("t".into()),
			ev: Cow::Owned("e".into()),
			ts: 3,
		}
		.encode_key()?;
		assert_eq!(node_of_state_key(&queue)?, Some(nd));
		let live = NodeLiveQueryKey {
			nd,
			lq: Uuid::from_u128(9),
		}
		.encode_key()?;
		assert_eq!(node_of_state_key(&live)?, Some(nd));
		let mut unknown = NodeStateRoot {
			nd,
		}
		.encode_bound()?
		.to_vec();
		unknown.extend_from_slice(b"!zz");
		assert_eq!(node_of_state_key(&unknown)?, Some(nd));
		let mut short = NodeStatePrefix {}.encode_bound()?.to_vec();
		short.push(b'!');
		assert_eq!(node_of_state_key(&short)?, None, "too short to name a node");
		Ok(())
	}

	/// Wide enough to read a queue whose lap spans passes in one scan.
	const QUEUE_SCAN_LIMIT: u32 =
		2 * crate::kvs::EVENT_PROCESSING_SLICE_BATCHES as u32 * NORMAL_BATCH_SIZE;
	/// Claims every entry of `nd`'s queue for `holder` as of now, as the
	/// workers about to run them would, and returns how many. When the holder
	/// is `ds` itself the entries are marked running as well, which is what
	/// keeps this node's own claims: they stand until `settle_queue`, as a
	/// worker's do until it settles.
	async fn hold_queue(ds: &Datastore, nd: Uuid, holder: Uuid) -> Result<usize> {
		let range = NodeEventQueuePrefix {
			nd,
		}
		.range()?;
		let now = ds.clock_now().value;
		let txn = ds.transaction(Write).await?;
		let mut held = 0;
		for (k, v) in txn.scan_raw(range, QUEUE_SCAN_LIMIT, 0, None).await? {
			let mut ev = <AsyncEventRecord as KVValue>::kv_decode_value(&v, ())?;
			ev.claim = Some(EventClaim {
				node: holder,
				at: now,
			});
			if holder == ds.id() {
				ds.running_events().lock().insert(k.clone());
			}
			txn.set(Key::from(k.as_slice()), ev.kv_encode_value()?).await?;
			held += 1;
		}
		txn.commit().await?;
		Ok(held)
	}

	/// Removes every entry of `nd`'s queue, as their holder settling them
	/// would, and no longer marks them running here once the removal has
	/// committed. The order is the worker's, whose mark outlives its
	/// settlement's commit: an entry unmarked while still stored reads, to
	/// a drain of this node, as a void claim of its own, and its takeover
	/// conflicts with the removal.
	async fn settle_queue(ds: &Datastore, nd: Uuid) -> Result<()> {
		let range = NodeEventQueuePrefix {
			nd,
		}
		.range()?;
		let txn = ds.transaction(Write).await?;
		let mut settled = Vec::new();
		for (k, _) in txn.scan_raw(range, QUEUE_SCAN_LIMIT, 0, None).await? {
			txn.del(Key::from(k.as_slice())).await?;
			settled.push(k);
		}
		txn.commit().await?;
		let mut running = ds.running_events().lock();
		for k in settled {
			running.remove(&k);
		}
		Ok(())
	}

	/// Backdates every claim on `nd`'s queue by `by`, as the passage of that
	/// much time would.
	async fn age_claims(ds: &Datastore, nd: Uuid, by: Duration) -> Result<()> {
		let range = NodeEventQueuePrefix {
			nd,
		}
		.range()?;
		let delta = by.as_millis() as u64;
		let txn = ds.transaction(Write).await?;
		for (k, v) in txn.scan_raw(range, NORMAL_BATCH_SIZE, 0, None).await? {
			let mut ev = <AsyncEventRecord as KVValue>::kv_decode_value(&v, ())?;
			let claim = ev.claim.as_mut().expect("every entry is claimed");
			claim.at = claim.at.saturating_sub(delta);
			txn.set(Key::from(k.as_slice()), ev.kv_encode_value()?).await?;
		}
		let stamp = NodeDrainHeartbeatKey {
			nd,
		};
		if let Some(beat) = txn.get_key(&stamp, None).await? {
			txn.set_key(&stamp, &beat.saturating_sub(delta)).await?;
		}
		txn.commit().await
	}

	/// How many entries `nd`'s queue holds, up to a batch.
	async fn queue_len(ds: &Datastore, nd: Uuid) -> Result<usize> {
		let range = NodeEventQueuePrefix {
			nd,
		}
		.range()?;
		let txn = ds.transaction(Read).await?;
		let n = txn.keys_raw(range, QUEUE_SCAN_LIMIT, 0, None).await?.len();
		txn.cancel().await?;
		Ok(n)
	}

	/// An adopter that finds entries a departed owner had claimed leaves them
	/// to it for [`EVENT_CLAIM_TTL`]: the owner may be paused rather than
	/// gone, still inside those bodies, whose effects outside their
	/// transactions cannot be taken back. Once the window has passed the
	/// owner is taken for dead with its claims, and the entries are taken
	/// over and run.
	#[test_log::test(tokio::test)]
	async fn an_adopter_waits_out_a_departed_owners_claims() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 3).await?;
		// The owner's workers claim their entries and stop before running them.
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 3);
		archive_node(&ds, ds.id()).await?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;

		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 3, "the held entries are fetched");
		assert_eq!(pass.held, 3, "and reported as held");
		assert_eq!(logged_count(&other, &session).await?, 0, "but left to the owner holding them");
		assert_eq!(queue_len(&ds, ds.id()).await?, 3, "and stay queued");

		age_claims(&ds, ds.id(), EVENT_CLAIM_TTL).await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 3);
		assert_eq!(logged_count(&other, &session).await?, 3, "taken over once the window passed");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// This node's own claims hold against a concurrent drain of its queue
	/// for as long as its workers run the entries, and no longer than the
	/// process that made them: a restarted process is running nothing, so a
	/// node restarted with its id kept runs the batch it was in the middle
	/// of at once rather than waiting out a window on it.
	#[test_log::test(tokio::test)]
	async fn a_restarted_node_runs_the_entries_it_had_claimed_at_once() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 3).await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 3);

		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, 3, "the held entries are fetched");
		assert_eq!(pass.held, 3, "and reported as held");
		assert_eq!(logged_count(&ds, &session).await?, 0, "but left to the drain holding them");

		let ds = Arc::new(Arc::into_inner(ds).expect("sole owner of the datastore").restart());
		let pass = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(pass.items, 3);
		assert_eq!(logged_count(&ds, &session).await?, 3, "the restarted node runs them at once");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// `drain_async_events` is a flush: it returns once nothing this node
	/// queued is left to run, including the entries a concurrent processor —
	/// the event task's workers — holds. Those are their holder's to settle
	/// and their bodies may still be running, so the drain waits for that
	/// rather than reporting the queue drained of its own share only.
	#[test_log::test(tokio::test)]
	async fn an_explicit_drain_waits_for_the_entries_another_processor_holds() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 2).await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 2);
		// The holder settles its entries a little later.
		let holder = {
			let ds = Arc::clone(&ds);
			spawn(async move {
				sleep(Duration::from_millis(300)).await;
				settle_queue(&ds, ds.id()).await
			})
		};

		let started = Instant::now();
		let drained = ds.drain_async_events().await?;
		assert!(
			started.elapsed() >= Duration::from_millis(300),
			"the drain waited for the holder to settle its entries"
		);
		holder.await??;
		assert_eq!(drained, 0, "the held entries were the holder's to run, not this drain's");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A body this node's worker has been running for longer than the claim
	/// window is still that worker's: the window bounds another node's
	/// claims, whose workers cannot be asked, while this node knows which
	/// entries its own workers are running. A drain of the same node finds the
	/// claims older than the window and still waits, rather than take the
	/// entries over and evaluate the bodies a second time.
	#[test_log::test(tokio::test)]
	async fn an_explicit_drain_waits_for_a_body_this_node_has_run_past_the_window() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 2).await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 2);
		age_claims(&ds, ds.id(), EVENT_CLAIM_TTL).await?;
		let holder = {
			let ds = Arc::clone(&ds);
			spawn(async move {
				sleep(Duration::from_millis(300)).await;
				settle_queue(&ds, ds.id()).await
			})
		};

		let started = Instant::now();
		let drained = ds.drain_async_events().await?;
		assert!(
			started.elapsed() >= Duration::from_millis(300),
			"the drain waited for this node's worker however old its claim"
		);
		holder.await??;
		assert_eq!(drained, 0, "the entries were the worker's to run, not this drain's");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A lap spans passes when the queue holds `EVENT_PROCESSING_SLICE_BATCHES`
	/// full batches: the first pass spends its budget with the lap's cursor
	/// at the queue's end, and the next finds the end from there, seeing
	/// nothing. What the first pass found held is still held, so the drain
	/// carries the count across the lap's passes rather than trust the one
	/// that found the end.
	#[test_log::test(tokio::test)]
	async fn an_explicit_drain_carries_held_entries_across_the_passes_of_a_lap() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		let count = crate::kvs::EVENT_PROCESSING_SLICE_BATCHES * NORMAL_BATCH_SIZE as usize;
		queue_async_events(&ds, &session, count).await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, count);
		let holder = {
			let ds = Arc::clone(&ds);
			spawn(async move {
				sleep(Duration::from_millis(300)).await;
				settle_queue(&ds, ds.id()).await
			})
		};

		let started = Instant::now();
		let drained = ds.drain_async_events().await?;
		assert!(
			started.elapsed() >= Duration::from_millis(300),
			"the drain waited for the entries the lap's first pass found held"
		);
		holder.await??;
		assert_eq!(drained, 0, "the held entries were the holder's to run, not this drain's");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A drain that begins while a lap is in progress — left by a pass that
	/// spent its budget, the event task's or an earlier caller's — finishes
	/// that lap from its cursor, and what the passes before it found held is
	/// not this drain's to know. The drain ends only with a lap it began at
	/// the head.
	#[test_log::test(tokio::test)]
	async fn an_explicit_drain_ends_only_with_a_lap_it_began_at_the_head() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 2).await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 2);
		// A lap whose cursor stands at the queue's end, the held entries
		// behind it.
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let end = ds.queue_end(&range).await?.expect("the queue holds entries");
		ds.event_laps.lock().insert(
			EventQueue::Node(ds.id()),
			EventLap {
				after: Some(end.clone()),
				end,
				requeued: false,
				queued: ds.commit_triggers().async_events_queued(),
			},
		);
		let holder = {
			let ds = Arc::clone(&ds);
			spawn(async move {
				sleep(Duration::from_millis(300)).await;
				settle_queue(&ds, ds.id()).await
			})
		};

		let started = Instant::now();
		let drained = ds.drain_async_events().await?;
		assert!(
			started.elapsed() >= Duration::from_millis(300),
			"the drain did not end with the lap it resumed"
		);
		holder.await??;
		assert_eq!(drained, 0, "the held entries were the holder's to run, not this drain's");
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// An entry an explicit drain could not claim — the claim's transaction
	/// failing, not losing to another processor — fails the drain rather than
	/// being counted and passed over, so `Ok` means the queue was flushed.
	#[test_log::test(tokio::test)]
	async fn an_explicit_drain_fails_rather_than_pass_an_entry_it_could_not_claim() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 1).await?;
		let fault = crate::kvs::testing::inject_non_retryable_error(
			crate::kvs::testing::NonRetryableErrorSite::AsyncEventClaim,
			ds.id(),
		);

		assert!(ds.drain_async_events().await.is_err(), "the drain fails with the claim's error");
		assert_eq!(logged_count(&ds, &session).await?, 0, "the body did not run");
		assert_eq!(queue_len(&ds, ds.id()).await?, 1, "and the entry is still queued");

		drop(fault);
		assert_eq!(ds.drain_async_events().await?, 1, "the next drain runs it");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		Ok(())
	}

	/// A worker that panics — a bug in what it runs — settles nothing: its
	/// entry stays claimed, and the mark that made the claim hold went with
	/// the worker, so the next pass takes the entry over. The batch the
	/// worker was part of fails, and with it the pass and the drain, rather
	/// than report the queue flushed past the entry.
	#[test_log::test(tokio::test)]
	async fn a_worker_that_panics_fails_the_pass_rather_than_pass_its_entry() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_one_async_event(&ds, &session, 0).await?;
		{
			let _panic = inject_panic(PanicSite::AsyncEventWorker, ds.id());
			assert!(ds.drain_async_events().await.is_err(), "the drain fails with the worker");
		}
		assert_eq!(logged_count(&ds, &session).await?, 0, "the event did not run");
		assert_eq!(queue_len(&ds, ds.id()).await?, 1, "and its entry stays queued");
		assert_eq!(ds.drain_async_events().await?, 1, "the next drain takes it over and runs it");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// A pass that fails after settling some entries — one worker's claim
	/// failing while another's held — attaches what it did to its error: the
	/// entries fetched and run, the queue's age, and a backlog, since the
	/// entry the failure left in place is still queued. The observer reports
	/// those rather than a pass that did nothing.
	#[test_log::test(tokio::test)]
	async fn a_pass_that_fails_reports_what_it_settled_before_failing() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 2).await?;
		let _fault = inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, ds.id());

		let e = ds
			.event_processing_local(&CancellationToken::new())
			.await
			.expect_err("the claim that fails fails the pass");
		let PassFailure(pass) = e.downcast_ref::<PassFailure>().expect("the pass is attached");
		assert_eq!(pass.items, 2, "both entries were fetched");
		assert_eq!(pass.ran, 1, "the entry whose claim held ran");
		assert!(pass.oldest_age.is_some(), "the queue's age is reported");
		assert_eq!(pass.progress, Progress::More, "the entry left in place is a backlog");
		assert_eq!(pass.lease, TaskLeaseState::NotLeased);
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(queue_len(&ds, ds.id()).await?, 1, "the entry whose claim failed stays queued");
		Ok(())
	}

	/// The adoption sweep attaches the same under the lease it held: the
	/// entries it settled before the failure stand.
	#[test_log::test(tokio::test)]
	async fn an_adoption_pass_that_fails_reports_what_it_settled_before_failing() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 2).await?;
		archive_node(&ds, ds.id()).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let _fault = inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, other.id());

		let e = other
			.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new())
			.await
			.expect_err("the claim that fails fails the pass");
		let PassFailure(pass) = e.downcast_ref::<PassFailure>().expect("the pass is attached");
		assert_eq!(pass.lease, TaskLeaseState::Held, "under the lease the sweep held");
		assert_eq!((pass.items, pass.ran), (2, 1));
		assert_eq!(pass.progress, Progress::More);
		assert_eq!(logged_count(&other, &session).await?, 1);
		assert_eq!(
			queue_len(&other, ds.id()).await?,
			1,
			"the entry whose claim failed stays queued"
		);
		Ok(())
	}

	/// A pass that fails in a queue keeps its rotation, past that queue: the
	/// next pass continues with the queues after it, and the next rotation
	/// retries it, so a queue whose drain keeps failing does not starve the
	/// queues behind it.
	#[test_log::test(tokio::test)]
	async fn an_adoption_pass_that_fails_in_a_queue_continues_past_it() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 1).await?;
		assert_eq!(relocate_queue_to_root(&ds).await?, 1);
		// Two departed nodes, one event each, in id order.
		let ids: Vec<Uuid> = (10..12u128).map(Uuid::from_u128).collect();
		for &id in &ids {
			let node = ds.fork_for_test_with_node_id(id);
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
			archive_node(&node, node.id()).await?;
		}
		let root_queued = || async {
			let txn = ds.transaction(Read).await?;
			let n = txn.count(EventQueuePrefix {}.range()?, None).await;
			txn.cancel().await?;
			n
		};
		let canceller = CancellationToken::new();
		let adopt = || ds.event_processing_adopt(Duration::from_secs(1), &canceller);
		// The first claim of a pass fails it.
		let failing =
			|| inject_non_retryable_error(NonRetryableErrorSite::AsyncEventClaim, ds.id());

		// The rotation begins with the root queue, whose claim fails.
		{
			let _fault = failing();
			adopt().await.expect_err("the root queue's claim fails the pass");
		}
		assert_eq!(root_queued().await?, 1);
		// The next pass continues with the nodes, and fails in the first.
		{
			let _fault = failing();
			adopt().await.expect_err("the first node's claim fails the pass");
		}
		assert_eq!(queued_events(&ds, ids[0]).await?, 1);
		// The next continues past that node, and ends the rotation.
		let third = adopt().await?;
		assert_eq!(third.items, 1);
		assert_eq!(queued_events(&ds, ids[1]).await?, 0, "the second node was swept");
		assert_eq!(root_queued().await?, 1, "the root queue waits for the next rotation");
		assert_eq!(queued_events(&ds, ids[0]).await?, 1, "so does the first node's");
		assert!(ds.adoption_rotation.lock().is_none(), "the rotation ended");
		// The next rotation retries the two.
		let fourth = adopt().await?;
		assert_eq!(fourth.items, 2);
		assert_eq!(root_queued().await?, 0);
		assert_eq!(queued_events(&ds, ids[0]).await?, 0);
		assert_eq!(logged_count(&ds, &session).await?, 3);
		Ok(())
	}

	/// An adoption sweep that finds the lease passed to another node stops in
	/// the queue it found so, adopting nothing further, so the former holder
	/// does not sweep alongside the new one; the queue's lap stays where the
	/// batch began.
	#[test_log::test(tokio::test)]
	async fn an_adoption_sweep_stops_at_a_lease_another_node_took() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;
		archive_node(&ds, ds.id()).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let lease = |ds: &Datastore| {
			ds.lease_handler(TaskLeaseType::EventProcessing, Duration::from_secs(60), None)
		};
		// The sweep's handler, whose first check reads the lease rather than
		// answering from its throttle, and a third node that takes the lease.
		let lh = lease(&other)?;
		let third = ds.fork_for_test_with_node_id(Uuid::from_u128(3));
		assert!(lease(&third)?.has_lease().await?, "the third node takes the lease");

		let canceller = CancellationToken::new();
		let mut sweep = AdoptionSweep {
			lh: &lh,
			canceller: &canceller,
			budget: EVENT_PROCESSING_SLICE_BATCHES,
			steps: EVENT_ADOPTION_PAGE_SIZE,
			pass: Pass::done(TaskLeaseState::Held),
			swept: HashSet::new(),
			failed: None,
		};
		let queue = EventQueue::Node(ds.id());
		let range = NodeEventQueuePrefix {
			nd: ds.id(),
		}
		.range()?;
		let stop = other.adopt_queue(queue, &range, &mut sweep).await?;
		assert!(matches!(stop, Some(SweepStop::LeaseLost)), "the sweep stops in the queue");
		assert_eq!(sweep.pass.items, 0, "nothing is adopted without the lease");
		assert_eq!(logged_count(&other, &session).await?, 0);
		assert_eq!(queue_len(&other, ds.id()).await?, 5);
		assert!(
			other.event_laps.lock().get(&queue).is_some_and(|lap| lap.after.is_none()),
			"the lap stays at the queue's head"
		);
		Ok(())
	}

	/// A sweep that finds the lease lost in a node's queue leaves its walk at
	/// that node, so the rotation it keeps resumes at the queue it stopped in
	/// rather than past it.
	#[test_log::test(tokio::test)]
	async fn a_lease_lost_in_a_nodes_queue_leaves_the_walk_at_that_node() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;
		archive_node(&ds, ds.id()).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let lease = |ds: &Datastore| {
			ds.lease_handler(TaskLeaseType::EventProcessing, Duration::from_secs(60), None)
		};
		// The sweep's handler, whose first check reads the lease rather than
		// answering from its throttle, and a third node that takes the lease.
		let lh = lease(&other)?;
		let third = ds.fork_for_test_with_node_id(Uuid::from_u128(3));
		assert!(lease(&third)?.has_lease().await?, "the third node takes the lease");

		let canceller = CancellationToken::new();
		let mut sweep = AdoptionSweep {
			lh: &lh,
			canceller: &canceller,
			budget: EVENT_PROCESSING_SLICE_BATCHES,
			steps: EVENT_ADOPTION_PAGE_SIZE,
			pass: Pass::done(TaskLeaseState::Held),
			swept: HashSet::new(),
			failed: None,
		};
		let mut window = NodeStatePrefix {}.raw_range()?;
		let stop = other.adopt_node_queues(&mut window, None, &mut sweep).await?;
		assert!(matches!(stop, Some(SweepStop::LeaseLost)), "the sweep stops in the queue");
		let txn = other.transaction(Read).await?;
		let next = other.next_node_state(&txn, &mut window).await?;
		txn.cancel().await?;
		assert_eq!(next.map(|state| state.nd), Some(ds.id()), "the walk resumes at its queue");
		Ok(())
	}

	/// The local queue is the local drain's, but the local node is a step of
	/// the walk like any other: a page whose last step is the local node ends
	/// there, rather than stepping over a node more than the page.
	#[test_log::test(tokio::test)]
	async fn a_page_whose_last_step_is_the_local_node_ends_there() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		queue_async_events(&ds, &session, 0).await?;
		// A page of nodes with a queued event each: live owners, then the
		// local node last, and a departed node beyond the page.
		let page = EVENT_ADOPTION_PAGE_SIZE as u128;
		for n in 0..page - 1 {
			let node = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000 + n));
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
		}
		let local = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000 + page - 1));
		local.insert_node().await?;
		execute_all(&local, &session, "CREATE person RETURN NONE;").await?;
		let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(9_000));
		departed.insert_node().await?;
		execute_all(&departed, &session, "CREATE person RETURN NONE;").await?;
		archive_node(&departed, departed.id()).await?;

		let first =
			local.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.items, 0, "the page ends at the local node");
		assert_eq!(first.progress, Progress::More, "the walk stopped at its page");
		assert_eq!(queued_events(&local, departed.id()).await?, 1, "not reached yet");

		let second =
			local.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.items, 1, "the departed node beyond the page is reached");
		Ok(())
	}

	/// Counts the scans of every transaction completed on the datastore.
	#[derive(Default)]
	struct ScanCounter(std::sync::atomic::AtomicU32);

	impl ExecutionObserver for ScanCounter {
		fn on_transaction_complete(&self, event: &crate::observe::TransactionEvent) {
			self.0.fetch_add(event.safe.metrics.ops_scan, std::sync::atomic::Ordering::Relaxed);
		}
	}

	/// Keys beneath a node that this release cannot decode — a newer
	/// release's state, left by a node since rolled back — do not cost the
	/// sweep a read each: the walk names their node from the key and places
	/// them against its queue, so the queue behind them is still adopted and
	/// a node costs a fixed few reads however many such keys it holds.
	#[test_log::test(tokio::test)]
	async fn event_adoption_reads_past_a_nodes_unknown_state_in_bounded_reads() -> Result<()> {
		let observer = Arc::new(ScanCounter::default());
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
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 1).await?;
		// As many unknown keys before the node's queue as after it, each side
		// well over the reads a pass makes.
		let unknown: u32 = 100;
		{
			let root = NodeStateRoot {
				nd: ds.id(),
			}
			.encode_bound()?;
			let txn = ds.transaction(Write).await?;
			for i in 0..unknown {
				for tag in [b"!aa", b"!zz"] {
					let mut key = root.to_vec();
					key.extend_from_slice(tag);
					key.extend_from_slice(&i.to_be_bytes());
					txn.set(Key::from(key.as_slice()), b"state this release cannot read".to_vec())
						.await?;
				}
			}
			txn.commit().await?;
		}
		archive_node(&ds, ds.id()).await?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;

		observer.0.store(0, std::sync::atomic::Ordering::Relaxed);
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 1, "the queue behind the unknown keys is adopted");
		assert_eq!(logged_count(&other, &session).await?, 1);
		let scans = observer.0.load(std::sync::atomic::Ordering::Relaxed);
		assert!(scans > 0, "the pass's transactions are observed");
		assert!(
			scans < unknown,
			"the pass scanned {scans} times over {unknown} unknown keys a side"
		);
		Ok(())
	}

	/// A node that keeps its id across a rollback to a release that queues at
	/// the root keeps heartbeating, so its row stays live, but nothing on it
	/// drains the per-node queue it left. Such a release refreshes the row
	/// without the drain heartbeat this one writes beside it, and a row that
	/// has moved ahead of that stamp tells the sweep the owner cannot run its
	/// queue: the queue is adopted, as it is once the owner is archived.
	#[test_log::test(tokio::test)]
	async fn event_adoption_drains_the_queue_of_a_live_owner_on_an_older_release() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 5).await?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;

		// Heartbeating on this release, the queue is the owner's.
		ds.update_node().await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.lease, TaskLeaseState::Held);
		assert_eq!(pass.items, 0, "a queue its live owner drains is not adopted");

		// Heartbeating on the older release, it is nobody's.
		heartbeat_without_drain_stamp(&ds).await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 5);
		assert_eq!(logged_count(&other, &session).await?, 5);
		assert_eq!(queued_events(&other, ds.id()).await?, 0);
		Ok(())
	}

	/// Every write of the node row by this release stamps the drain heartbeat
	/// beside it with the same value, so the two read equal for as long as
	/// this release keeps the row fresh, and cleanup of an archived node takes
	/// the stamp with the row.
	#[test_log::test(tokio::test)]
	async fn the_drain_heartbeat_follows_the_node_row() -> Result<()> {
		let (ds, _session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		let registered = node_heartbeat(&ds, ds.id()).await?.expect("registered");
		assert_eq!(drain_heartbeat(&ds, ds.id()).await?, Some(registered));

		ds.update_node().await?;
		let refreshed = node_heartbeat(&ds, ds.id()).await?.expect("still registered");
		assert_eq!(drain_heartbeat(&ds, ds.id()).await?, Some(refreshed));

		ds.update_node_with_timeout(Duration::from_secs(5), &CancellationToken::new()).await?;
		let refreshed = node_heartbeat(&ds, ds.id()).await?.expect("still registered");
		assert_eq!(drain_heartbeat(&ds, ds.id()).await?, Some(refreshed));

		heartbeat_without_drain_stamp(&ds).await?;
		let moved = node_heartbeat(&ds, ds.id()).await?.expect("still registered");
		assert_ne!(drain_heartbeat(&ds, ds.id()).await?, Some(moved), "the stamp stays behind");

		archive_node(&ds, ds.id()).await?;
		ds.remove_nodes().await?;
		assert!(!node_exists(&ds, ds.id()).await?);
		assert_eq!(drain_heartbeat(&ds, ds.id()).await?, None, "gone with the row");
		Ok(())
	}

	/// Every queue with an entry costs the sweep one unit of budget, however
	/// short, so a cluster with more departed nodes than the budget covers is
	/// swept a budget's worth of queues per pass, each pass continuing where
	/// the one before stopped. The empty root-level queue costs nothing, which
	/// is what lets a full budget's worth of node queues fit in the first pass.
	#[test_log::test(tokio::test)]
	async fn event_adoption_over_many_short_queues_is_bounded_and_continues() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		// Two more departed nodes than the budget covers, one event each.
		let departed = EVENT_PROCESSING_SLICE_BATCHES + 2;
		for n in 0..departed as u128 {
			let node = ds.fork_for_test_with_node_id(Uuid::from_u128(10 + n));
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
			archive_node(&node, node.id()).await?;
		}

		let first =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.items, EVENT_PROCESSING_SLICE_BATCHES as u64, "one queue per unit");
		assert_eq!(first.progress, Progress::More, "queues are left for the next pass");
		let second =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.items, 2, "the next pass starts at the first queue left");
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(logged_count(&ds, &session).await?, departed);
		Ok(())
	}

	/// Entries under the root-level queue — written by a node running the layout
	/// that preceded per-node queues — are not this node's own, and are adopted
	/// by the sweep.
	#[test_log::test(tokio::test)]
	async fn event_adoption_drains_the_root_level_queue() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 3).await?;
		assert_eq!(relocate_queue_to_root(&ds).await?, 3);

		let local = ds.event_processing_local(&CancellationToken::new()).await?;
		assert_eq!(local.items, 0, "the root-level queue is not this node's own");

		let pass =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 3);
		assert_eq!(logged_count(&ds, &session).await?, 3);
		Ok(())
	}

	/// Root-level `!eq` entries queued by a live node on an older release —
	/// whose `!nd` row is active and ahead of [`NodeDrainHeartbeatKey`] — are
	/// left to that node to drain, and adopted only once the node is archived
	/// or upgraded.
	#[test_log::test(tokio::test)]
	async fn event_adoption_leaves_root_queue_entries_of_a_live_legacy_owner_alone() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 3).await?;
		assert_eq!(relocate_queue_to_root(&ds).await?, 3);
		heartbeat_without_drain_stamp(&ds).await?;

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;

		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 3, "the legacy entries are fetched");
		assert_eq!(pass.held, 3, "and reported as held by their live legacy owner");
		assert_eq!(logged_count(&other, &session).await?, 0);

		archive_node(&ds, ds.id()).await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 3);
		assert_eq!(pass.ran, 3, "adopted once the legacy owner is archived");
		assert_eq!(logged_count(&other, &session).await?, 3);
		Ok(())
	}

	/// A worker running an event body past [`EVENT_CLAIM_TTL`] after its node
	/// was archived keeps renewing [`NodeDrainHeartbeatKey`], so an adopter
	/// leaves the entry to the worker rather than re-running the body.
	#[test_log::test(tokio::test)]
	async fn an_adopter_respects_a_running_workers_renewed_drain_heartbeat_past_claim_ttl()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE logged SCHEMALESS;
			 DEFINE EVENT log ON person ASYNC THEN {
			 	sleep(250ms);
			 	CREATE logged SET who = $after.id;
			 };
			 CREATE person:1 RETURN NONE;",
		)
		.await?;
		let stamp = NodeDrainHeartbeatKey {
			nd: ds.id(),
		};
		let read_beat = || async {
			let txn = ds.transaction(Read).await?;
			let beat = txn.get_key(&stamp, None).await?;
			txn.cancel().await?;
			Ok::<_, anyhow::Error>(beat)
		};

		// 1. Verify that a running worker actively renews `NodeDrainHeartbeatKey`
		// while the event body is executing, without conflicting with settlement.
		let owner = {
			let ds = Arc::clone(&ds);
			spawn(async move { ds.event_processing_local(&CancellationToken::new()).await })
		};
		while ds.running_events().lock().is_empty() {
			sleep(Duration::from_millis(10)).await;
		}
		let initial_beat = read_beat().await?.expect("drain heartbeat recorded");
		sleep(Duration::from_millis(150)).await;
		let renewed_beat = read_beat().await?.expect("drain heartbeat renewed");
		assert!(
			renewed_beat > initial_beat,
			"the running worker's ClaimRenewalGuard advances NodeDrainHeartbeatKey ({renewed_beat} > {initial_beat})"
		);
		let owner_pass = owner.await??;
		assert_eq!(owner_pass.ran, 1, "the worker settles the entry without write conflict");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);

		// 2. Verify that an adopter respects a renewed `NodeDrainHeartbeatKey`
		// even after the owner is archived and the entry's `EventClaim::at` is
		// older than `EVENT_CLAIM_TTL`.
		execute_all(&ds, &session, "CREATE person:2 RETURN NONE;").await?;
		assert_eq!(hold_queue(&ds, ds.id(), ds.id()).await?, 1);
		archive_node(&ds, ds.id()).await?;
		age_claims(&ds, ds.id(), EVENT_CLAIM_TTL).await?;
		// Simulate the running worker's renewal of `NodeDrainHeartbeatKey`.
		{
			let txn = ds.transaction(Write).await?;
			ds.stamp_drain_heartbeat(&txn, ds.clock_now()).await?;
			txn.commit().await?;
		}

		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 1, "the entry is fetched");
		assert_eq!(pass.held, 1, "and held by the worker's renewed drain heartbeat");
		assert_eq!(pass.ran, 0, "the adopter does not evaluate the body a second time");
		assert_eq!(logged_count(&ds, &session).await?, 1);

		// Once the renewed heartbeat also ages past `EVENT_CLAIM_TTL`, the
		// adopter takes over the entry and runs it.
		age_claims(&ds, ds.id(), EVENT_CLAIM_TTL).await?;
		let pass =
			other.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 1);
		assert_eq!(pass.ran, 1, "taken over once the drain heartbeat expires too");
		assert_eq!(logged_count(&ds, &session).await?, 2);
		assert_eq!(queue_len(&ds, ds.id()).await?, 0);
		Ok(())
	}

	/// Shutting down the last upgraded node moves any remaining per-node queue
	/// entries back to the root `!eq` queue encoded at revision 1, so a cluster
	/// rolled back to an older release can decode and drain them.
	#[test_log::test(tokio::test)]
	async fn shutting_down_the_last_upgraded_node_hands_event_queues_back_to_root_at_revision_1()
	-> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 2).await?;
		// Claim one entry (revision 2) so shutdown must clear the claim too.
		assert_eq!(hold_queue(&ds, ds.id(), Uuid::from_u128(99)).await?, 2);

		let peer = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		peer.insert_node().await?;

		// While `peer` is an active upgraded node, `ds` shutting down leaves
		// its per-node queue in place for `peer` to adopt.
		ds.shutdown().await?;
		assert_eq!(queue_len(&peer, ds.id()).await?, 2, "kept while an upgraded peer is active");

		// When the last upgraded node (`peer`) shuts down, the entries are
		// handed back to the root queue at revision 1.
		peer.shutdown().await?;
		let inspect = peer.fork_for_test_with_node_id(Uuid::from_u128(3));
		assert_eq!(queue_len(&inspect, ds.id()).await?, 0, "per-node queue emptied");
		let txn = inspect.transaction(Read).await?;
		let root = txn.scan_raw(EventQueuePrefix {}.range()?, NORMAL_BATCH_SIZE, 0, None).await?;
		txn.cancel().await?;
		assert_eq!(root.len(), 2, "moved to the root `!eq` queue");
		for (_, bytes) in &root {
			assert_eq!(bytes[0], 1, "encoded at revision 1 for older releases");
			let decoded = <AsyncEventRecord as KVValue>::kv_decode_value(bytes, ())?;
			assert_eq!(decoded.claim, None);
		}
		Ok(())
	}

	/// A live owner costs the sweep a step — the read of the first key under
	/// its node and one of its row — but no fetch budget, so the walk is
	/// bounded by a page of nodes as well: a pass steps over at most
	/// [`EVENT_ADOPTION_PAGE_SIZE`] of them before it yields with its position,
	/// and the departed node beyond the page is reached by the next pass rather
	/// than every live owner being walked on every pass.
	#[test_log::test(tokio::test)]
	async fn event_adoption_walks_a_page_of_owners_per_pass() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		// Two more live nodes than a page, each with a queued event of its
		// own, then a departed node beyond them all.
		let live = EVENT_ADOPTION_PAGE_SIZE + 2;
		for n in 0..live as u128 {
			let node = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000 + n));
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
		}
		let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(9_000));
		departed.insert_node().await?;
		execute_all(&departed, &session, "CREATE person RETURN NONE;").await?;
		archive_node(&departed, departed.id()).await?;

		let first =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.items, 0, "a page of live owners holds nothing to adopt");
		assert_eq!(first.progress, Progress::More, "the walk stopped at its page");
		assert!(ds.adoption_rotation.lock().is_some(), "the rotation is kept for the next pass");
		assert_eq!(queued_events(&ds, departed.id()).await?, 1, "not reached yet");

		let second =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.items, 1, "the departed node beyond the page is reached");
		assert_eq!(second.progress, Progress::Done, "and the rotation ends at the last node");
		assert!(ds.adoption_rotation.lock().is_none());
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(queued_events(&ds, Uuid::from_u128(1_000)).await?, 1, "live queues are left");
		Ok(())
	}

	/// A budget spent on fetches that ran nothing is not a backlog: as many
	/// departed nodes as the budget has fetches, each with an entry this
	/// binary cannot run, would have every pass run at once spend its budget
	/// on the same entries. The sweep goes idle until its interval instead,
	/// keeping its place in the rotation, and a pass that does run something
	/// is followed at once.
	#[test_log::test(tokio::test)]
	async fn a_sweep_whose_budget_ran_nothing_goes_idle_until_its_interval() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		for n in 0..EVENT_PROCESSING_SLICE_BATCHES as u128 {
			let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000 + n));
			departed.insert_node().await?;
			queue_undecodable_events(&departed, 1).await?;
			archive_node(&departed, departed.id()).await?;
		}

		let pass =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items as usize, EVENT_PROCESSING_SLICE_BATCHES, "the budget was spent");
		assert_eq!(pass.ran, 0, "on entries that could not be run");
		assert_eq!(pass.progress, Progress::Done, "so a pass run at once would find no more");
		assert!(ds.adoption_rotation.lock().is_some(), "the rotation is kept for the next pass");

		// A departed node beyond them with an entry that runs: the next pass
		// reaches it, and having run something, is followed at once.
		let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(9_000));
		departed.insert_node().await?;
		execute_all(&departed, &session, "CREATE person RETURN NONE;").await?;
		archive_node(&departed, departed.id()).await?;
		let pass =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.ran, 1, "the entry ran");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(pass.progress, Progress::More, "and the pass is followed at once");
		Ok(())
	}

	/// A budget spent on full batches that ran nothing is a backlog all the
	/// same: the batches were full, so the queue holds entries beyond them,
	/// and the lap keeps its place after them, so the pass run at once reads
	/// on from there rather than the same entries again. It reaches the event
	/// behind them, and the queue's end ends the lap, so the sweep goes idle.
	#[test_log::test(tokio::test)]
	async fn a_sweep_whose_budget_ran_nothing_past_full_batches_continues_after_them() -> Result<()>
	{
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		// A budget's worth of full batches this binary cannot run, one more
		// such entry, then an event that runs, in a departed node's queue.
		let budget = (EVENT_PROCESSING_SLICE_BATCHES * NORMAL_BATCH_SIZE as usize) as u64;
		let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000));
		departed.insert_node().await?;
		queue_undecodable_events(&departed, budget + 1).await?;
		execute_all(&departed, &session, "CREATE person RETURN NONE;").await?;
		archive_node(&departed, departed.id()).await?;
		let queue = EventQueue::Node(departed.id());

		let first =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.items, budget, "the budget was spent on full batches");
		assert_eq!(first.ran, 0, "none of which could be run");
		assert_eq!(first.progress, Progress::More, "which the queue holds entries beyond");
		assert!(ds.event_laps.lock().contains_key(&queue), "the lap keeps its place in the queue");
		assert_eq!(logged_count(&ds, &session).await?, 0);

		let second =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.items, 2, "the next pass read on from where the lap stopped");
		assert_eq!(second.ran, 1, "and ran the event behind the entries it cannot run");
		assert_eq!(logged_count(&ds, &session).await?, 1);
		assert_eq!(second.progress, Progress::Done, "the queue's end ends the lap");
		assert!(!ds.event_laps.lock().contains_key(&queue));
		Ok(())
	}

	/// The local queue's lap belongs to the local drain: the sweep, which
	/// steps over this node as any other live one, must leave it in place,
	/// or a host interleaving the sweep between local slices would start
	/// each slice from the head again and never reach the entries behind the
	/// ones it cannot run. A lap left on another live node's queue is stale
	/// and dropped.
	#[test_log::test(tokio::test)]
	async fn event_adoption_leaves_the_local_queues_lap_alone() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 1).await?;
		let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
		other.insert_node().await?;
		execute_all(&other, &session, "CREATE person RETURN NONE;").await?;
		let lap = || EventLap {
			after: None,
			end: vec![0],
			requeued: false,
			queued: 0,
		};
		ds.event_laps.lock().insert(EventQueue::Node(ds.id()), lap());
		ds.event_laps.lock().insert(EventQueue::Node(other.id()), lap());

		let pass =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(pass.items, 0, "both queues belong to live nodes");
		let laps = ds.event_laps.lock();
		assert!(laps.contains_key(&EventQueue::Node(ds.id())), "the local lap is kept");
		assert!(!laps.contains_key(&EventQueue::Node(other.id())), "the other node's is stale");
		Ok(())
	}

	/// A rotation that finds nothing to adopt still ends: with more live owners
	/// than a page, the first pass yields at its page and the next completes
	/// the rotation and reports it done, so the sweep waits for its interval
	/// rather than walking the cluster without end. The pass after that begins
	/// a rotation of its own.
	#[test_log::test(tokio::test)]
	async fn a_page_only_rotation_goes_idle_at_its_end() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		for n in 0..(EVENT_ADOPTION_PAGE_SIZE + 1) as u128 {
			let node = ds.fork_for_test_with_node_id(Uuid::from_u128(1_000 + n));
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
		}

		let canceller = CancellationToken::new();
		let adopt = || ds.event_processing_adopt(Duration::from_secs(1), &canceller);
		assert_eq!(adopt().await?.progress, Progress::More, "a page walked, one node left");
		assert_eq!(adopt().await?.progress, Progress::Done, "the rotation ends");
		assert!(ds.adoption_rotation.lock().is_none());
		assert_eq!(adopt().await?.progress, Progress::More, "the next rotation begins");
		assert_eq!(logged_count(&ds, &session).await?, 0, "nothing was adopted");
		Ok(())
	}

	/// A rotation begun after the queue a pass spent its budget in wraps from
	/// the last node to the root and on through the first nodes, and a pass
	/// stops walking once its budget is spent: the queues it has not reached
	/// wait for the next pass, which begins after the queue it stopped in.
	#[test_log::test(tokio::test)]
	async fn event_adoption_resumes_after_the_queue_the_last_pass_stopped_in() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		queue_async_events(&ds, &session, 0).await?;
		// Six departed nodes, one event each, in id order.
		let ids: Vec<Uuid> = (10..16u128).map(Uuid::from_u128).collect();
		for &id in &ids {
			let node = ds.fork_for_test_with_node_id(id);
			node.insert_node().await?;
			execute_all(&node, &session, "CREATE person RETURN NONE;").await?;
			archive_node(&node, node.id()).await?;
		}
		// The previous pass spent its budget in the third.
		*ds.adoption_rotation.lock() = Some(AdoptionRotation::after(EventQueue::Node(ids[2]))?);

		let first =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.items, EVENT_PROCESSING_SLICE_BATCHES as u64);
		assert_eq!(first.progress, Progress::More);
		// The three after it, the root, then the first from the head.
		for &id in &[ids[3], ids[4], ids[5], ids[0]] {
			assert_eq!(queued_events(&ds, id).await?, 0, "{id} was swept");
		}
		for &id in &[ids[1], ids[2]] {
			assert_eq!(queued_events(&ds, id).await?, 1, "{id} was not reached");
		}

		// The next rotation begins after the first node, where the budget went.
		let second =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.items, 2);
		assert_eq!(second.progress, Progress::Done);
		assert!(ds.adoption_rotation.lock().is_none(), "the rotation ended");
		assert_eq!(logged_count(&ds, &session).await?, 6);
		Ok(())
	}

	/// A queue batch bounds the entries a slice reads, not the folding they
	/// call for: a scope can hold more adjacency keys than a slice should
	/// fold. The slice runs its passes on the scope, keeps the scope's entry
	/// and reports the backlog; the next slice reaches it again and folds the
	/// rest.
	#[tokio::test]
	async fn graph_fold_slice_bounds_the_passes_on_one_scope() -> Result<()> {
		use crate::expr::Dir;
		use crate::val::RecordId;

		// Two keys per fold pass, and a scope of six.
		let (ds, session) = new_index_compaction_test_ds_with("graph_fold_batch", "2").await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1, person:p2, person:p3, person:p4, person:p5, person:p6;
			 RELATE person:p0->likes->person:p1;
			 RELATE person:p0->likes->person:p2;
			 RELATE person:p0->likes->person:p3;
			 RELATE person:p0->likes->person:p4;
			 RELATE person:p0->likes->person:p5;
			 RELATE person:p0->likes->person:p6;",
		)
		.await?;
		let vertex = RecordId::new("person".into(), "p0".to_string());
		assert_eq!(pointer_keys(&ds, &vertex).await?, 6);
		{
			let txn = ds.transaction(Write).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			let key = GraphFoldKey {
				ns: db.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
				nid: ds.id(),
				uid: Uuid::now_v7(),
			};
			txn.put_key(&key, &()).await?;
			txn.commit().await?;
		}

		// Two passes per scope: the first slice folds four keys and keeps the
		// scope's entry.
		let (batches, errors, first) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(2),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(batches, 1, "the slice ends at the batch that kept entries");
		assert_eq!(first.progress, Progress::More);
		assert_eq!(pointer_keys(&ds, &vertex).await?, 2, "two passes of two keys");
		assert_eq!(fold_queue_keys(&ds).await?.len(), 1, "an unfinished scope keeps its entry");

		// The next slice reaches the entry again and finishes the scope.
		let (_, errors, second) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(2),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(second.progress, Progress::Done);
		assert_eq!(pointer_keys(&ds, &vertex).await?, 0, "the scope is folded");
		assert!(fold_queue_keys(&ds).await?.is_empty(), "the queue is drained");
		Ok(())
	}

	/// A batch of many scopes can outlast the graph fold lease. A drain that
	/// finds another node took the lease after it read its batch folds no
	/// further scope and deletes nothing of the batch: the scopes and their
	/// entries are the holder's to drain, and two nodes must not fold the same
	/// scopes at once.
	#[tokio::test]
	async fn a_graph_fold_that_loses_its_lease_leaves_its_batch_to_the_holder() -> Result<()> {
		use crate::expr::Dir;
		use crate::val::RecordId;

		let (ds, session) = new_index_compaction_test_ds_with("graph_fold_batch", "2").await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1, person:p2, person:p3;
			 RELATE person:p0->likes->person:p2;
			 RELATE person:p0->likes->person:p3;
			 RELATE person:p1->likes->person:p2;
			 RELATE person:p1->likes->person:p3;",
		)
		.await?;
		let vertices = [
			RecordId::new("person".into(), "p0".to_string()),
			RecordId::new("person".into(), "p1".to_string()),
		];
		{
			let txn = ds.transaction(Write).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			for vertex in &vertices {
				let key = GraphFoldKey {
					ns: db.namespace_id,
					db: db.database_id,
					tb: Cow::Borrowed(&vertex.table),
					id: Cow::Borrowed(&vertex.key),
					dir: Dir::Out,
					nid: ds.id(),
					uid: Uuid::now_v7(),
				};
				txn.put_key(&key, &()).await?;
			}
			txn.commit().await?;
		}
		for vertex in &vertices {
			assert_eq!(pointer_keys(&ds, vertex).await?, 2, "{vertex:?} must hold fold debt");
		}
		let queued = fold_queue_keys(&ds).await?;
		assert_eq!(queued.len(), 2);
		// Another node takes the lease once the drain has read its batch.
		let node_id = ds.id();
		let hook_ds = Arc::clone(&ds);
		BEFORE_FOLD_SCOPE.lock().unwrap().insert(
			node_id,
			Box::new(move || {
				Box::pin(async move {
					let lease = crate::catalog::TaskLease {
						owner: Uuid::from_u128(2),
						expiration: chrono::Utc::now() + chrono::Duration::seconds(120),
					};
					let key = crate::key::schema::TaskLeaseKey {
						task_id: TaskLeaseType::GraphFold,
					};
					let txn = hook_ds.transaction(Write).await.expect("a write transaction");
					txn.set_key(&key, &lease).await.expect("planting the lease");
					txn.commit().await.expect("committing the lease");
				})
			}),
		);

		let (_, errors, pass) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(GRAPH_FOLD_SLICE_PASSES_PER_SCOPE),
		)
		.await?;
		assert!(take_before_fold_scope(node_id).is_none(), "the hook must have run");
		assert_eq!(errors, 0);
		assert_eq!(pass.lease, TaskLeaseState::Elsewhere);
		assert_eq!(pass.progress, Progress::Done);
		for vertex in &vertices {
			assert_eq!(pointer_keys(&ds, vertex).await?, 2, "{vertex:?} must be left unfolded");
		}
		assert_eq!(fold_queue_keys(&ds).await?, queued, "the batch's entries stay queued");
		assert!(
			ds.fold_cursor.lock().resume_at.is_none(),
			"the rotation stays where the batch began"
		);
		Ok(())
	}

	/// A scope's continuation answers only for the queue entries that were
	/// there when its passes began at the scope's start. A write to the scope
	/// after a slice left its position can put a key before that position —
	/// here a tombstone over an edge the slice packed — and queue an entry for
	/// it. The slice that reaches the scope again must fold it from the start:
	/// continuing would declare the scope folded with the tombstone unexamined,
	/// and the cleanup would delete the entry that named it, leaving the
	/// tombstone in the delta layer with nothing queued to fold it.
	#[tokio::test]
	async fn graph_fold_restarts_a_scope_whose_entry_was_queued_after_its_position() -> Result<()> {
		use crate::expr::Dir;
		use crate::val::RecordId;

		// Two keys per fold pass, and a scope of four edges in key order.
		let (ds, session) = new_index_compaction_test_ds_with("graph_fold_batch", "2").await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1, person:p2, person:p3, person:p4;
			 RELATE person:p0->likes:m0->person:p1;
			 RELATE person:p0->likes:m1->person:p2;
			 RELATE person:p0->likes:m2->person:p3;
			 RELATE person:p0->likes:m3->person:p4;",
		)
		.await?;
		let vertex = RecordId::new("person".into(), "p0".to_string());
		assert_eq!(pointer_keys(&ds, &vertex).await?, 4);
		{
			let txn = ds.transaction(Write).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			let key = GraphFoldKey {
				ns: db.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
				nid: ds.id(),
				uid: Uuid::now_v7(),
			};
			txn.put_key(&key, &()).await?;
			txn.commit().await?;
		}

		// One pass per slice: the first packs the two earliest edges and
		// leaves its position after the second.
		let (_, errors, first) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(1),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(first.progress, Progress::More);
		assert_eq!(pointer_keys(&ds, &vertex).await?, 2, "the two earliest edges are packed");
		assert_eq!(fold_queue_keys(&ds).await?.len(), 1, "an unfinished scope keeps its entry");

		// Deleting a packed edge writes a tombstone at its pointer key, before
		// the position, and queues an entry for each of its vertices.
		execute_all(&ds, &session, "DELETE likes:m0;").await?;
		assert_eq!(pointer_keys(&ds, &vertex).await?, 3, "the tombstone joins the delta layer");
		assert_eq!(fold_queue_keys(&ds).await?.len(), 3, "both vertices are queued");

		// The slices that follow drain the queue, and the tombstone with it.
		let mut slices = 0;
		loop {
			let (_, errors, pass) = Datastore::graph_fold_bounded(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
				Some(GRAPH_FOLD_SLICE_BATCHES),
				Some(1),
			)
			.await?;
			slices += 1;
			assert_eq!(errors, 0, "slice {slices}");
			if pass.progress == Progress::Done {
				break;
			}
			assert!(slices < 10, "the drain must end");
		}
		assert!(fold_queue_keys(&ds).await?.is_empty(), "the queue is drained");
		assert_eq!(
			pointer_keys(&ds, &vertex).await?,
			0,
			"the tombstone is folded, not left behind"
		);
		assert!(ds.fold_cursor.lock().scans.is_empty(), "a drained scope keeps no position");
		Ok(())
	}

	/// The graph-fold counterpart of
	/// [`index_compaction_slice_leaves_a_batch_whose_cleanup_keeps_conflicting`]:
	/// a batch whose queue cleanup keeps losing its commit is left where it is
	/// once the cleanup's retries are spent, and the next slice deletes its
	/// entry once the commit goes through.
	#[tokio::test]
	async fn graph_fold_slice_leaves_a_batch_whose_cleanup_keeps_conflicting() -> Result<()> {
		use crate::expr::Dir;
		use crate::val::RecordId;

		let (ds, session) = new_index_compaction_test_ds().await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1;
			 RELATE person:p0->likes->person:p1;",
		)
		.await?;
		let vertex = RecordId::new("person".into(), "p0".to_string());
		assert_eq!(pointer_keys(&ds, &vertex).await?, 1);
		{
			let txn = ds.transaction(Write).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			let key = GraphFoldKey {
				ns: db.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
				nid: ds.id(),
				uid: Uuid::now_v7(),
			};
			txn.put_key(&key, &()).await?;
			txn.commit().await?;
		}
		let site = RetryableConflictSite::GraphFoldQueueCleanup;
		let node_id = ds.id();
		// More conflicts than any slice can consume, so no cleanup commits.
		let guard = inject_retryable_conflicts(site, node_id, usize::MAX);

		let (batches, errors, first) = tokio::time::timeout(
			Duration::from_secs(30),
			Datastore::graph_fold_bounded(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
				Some(GRAPH_FOLD_SLICE_BATCHES),
				Some(GRAPH_FOLD_SLICE_PASSES_PER_SCOPE),
			),
		)
		.await
		.expect("a slice whose cleanup keeps conflicting must return")?;
		assert_eq!(errors, 0, "the scope folded; only the cleanup was lost");
		assert_eq!(batches, 1, "the slice ends at the batch that kept entries");
		assert_eq!(first.progress, Progress::More);
		assert_eq!(pointer_keys(&ds, &vertex).await?, 0, "the scope is folded");
		assert_eq!(
			retryable_conflict_count(site, node_id),
			usize::MAX - (QUEUE_CLEANUP_CONFLICT_RETRIES + 1),
			"the cleanup is attempted once and retried its bound"
		);
		assert_eq!(fold_queue_keys(&ds).await?.len(), 1, "the batch keeps its entry");

		drop(guard);
		let (_, errors, second) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(GRAPH_FOLD_SLICE_PASSES_PER_SCOPE),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(second.progress, Progress::Done);
		assert!(fold_queue_keys(&ds).await?.is_empty(), "the queue is drained");
		Ok(())
	}

	/// A pass charges the keys it cannot pack as well as those it folds, and
	/// the slices continue a scope where its last pass stopped: a scope whose
	/// legacy keys outnumber a slice's budget is crossed a slice at a time,
	/// and the packable key behind them is folded by the slice whose budget
	/// reaches it. Slices that each started the scope over would examine the
	/// same legacy keys every time and never reach it.
	#[tokio::test]
	async fn graph_fold_slices_continue_a_scope_past_the_keys_they_cannot_pack() -> Result<()> {
		use crate::expr::Dir;
		use crate::key::schema::GraphKey;
		use crate::val::RecordId;

		// Two keys per fold pass, one pass per slice.
		let (ds, session) = new_index_compaction_test_ds_with("graph_fold_batch", "2").await?;
		execute_all(
			&ds,
			&session,
			"DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 CREATE person:p0, person:p1;
			 RELATE person:p0->likes:z->person:p1;",
		)
		.await?;
		let vertex = RecordId::new("person".into(), "p0".to_string());
		let (ns, db) = {
			let txn = ds.transaction(Read).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			let ids = (db.namespace_id, db.database_id);
			txn.cancel().await?;
			ids
		};
		// Five legacy-format keys, which cannot be packed, sorting before the
		// packable edge, and the scope's queue entry.
		{
			let txn = ds.transaction(Write).await?;
			for i in 0..5 {
				let edge = RecordId::new("likes".into(), format!("a{i}"));
				let key = GraphKey {
					ns,
					db,
					tb: Cow::Borrowed(&vertex.table),
					id: Cow::Borrowed(&vertex.key),
					dir: Dir::Out,
					foreign_table: Cow::Borrowed(&edge.table),
					foreign_key: Cow::Borrowed(&edge.key),
				};
				txn.set_key(&key, &()).await?;
			}
			let key = GraphFoldKey {
				ns,
				db,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
				nid: ds.id(),
				uid: Uuid::now_v7(),
			};
			txn.put_key(&key, &()).await?;
			txn.commit().await?;
		}
		assert_eq!(pointer_keys(&ds, &vertex).await?, 6);

		// Two slices of one pass each examine two legacy keys apiece and
		// fold nothing; each keeps the entry and reports the backlog.
		for slice in 1..=2 {
			let (_, errors, pass) = Datastore::graph_fold_bounded(
				Arc::clone(&ds),
				Duration::from_secs(60),
				CancellationToken::new(),
				Some(GRAPH_FOLD_SLICE_BATCHES),
				Some(1),
			)
			.await?;
			assert_eq!(errors, 0, "slice {slice}");
			assert_eq!(pass.progress, Progress::More, "slice {slice}");
			assert_eq!(pointer_keys(&ds, &vertex).await?, 6, "slice {slice} packs nothing");
			assert_eq!(fold_queue_keys(&ds).await?.len(), 1, "slice {slice} keeps the entry");
		}
		// The third slice's pass reaches the last legacy key and the edge.
		let (_, errors, third) = Datastore::graph_fold_bounded(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
			Some(GRAPH_FOLD_SLICE_BATCHES),
			Some(1),
		)
		.await?;
		assert_eq!(errors, 0);
		assert_eq!(third.progress, Progress::Done);
		assert_eq!(pointer_keys(&ds, &vertex).await?, 5, "the edge is packed; legacy keys stay");
		assert!(fold_queue_keys(&ds).await?.is_empty(), "the queue is drained");
		assert!(ds.fold_cursor.lock().scans.is_empty(), "a drained scope keeps no position");
		Ok(())
	}

	/// A slice can run out of budget with its window past the last queue
	/// entry. The next call resumes there and reads nothing, and must wrap to
	/// the head rather than end: the entries before the cursor — retained by
	/// failed folds, or queued since — are its work, and an end-of-range cursor
	/// left in place would hide them from every later call.
	#[test_log::test(tokio::test)]
	async fn graph_fold_resumed_past_the_last_entry_wraps_to_the_head() -> Result<()> {
		use crate::expr::Dir;
		use crate::val::RecordId;

		let (ds, _session) = new_index_compaction_test_ds().await?;
		// Entries for a namespace nothing has defined: they leave nothing to
		// fold, so the drain's only work is reaching them.
		{
			let txn = ds.transaction(Write).await?;
			for n in 0..3 {
				let vertex = RecordId::new("person".into(), format!("p{n}"));
				let key = GraphFoldKey {
					ns: NamespaceId(999),
					db: DatabaseId(999),
					tb: Cow::Borrowed(&vertex.table),
					id: Cow::Borrowed(&vertex.key),
					dir: Dir::Out,
					nid: ds.id(),
					uid: Uuid::now_v7(),
				};
				txn.put_key(&key, &()).await?;
			}
			txn.commit().await?;
		}
		let keys = fold_queue_keys(&ds).await?;
		assert_eq!(keys.len(), 3);
		// The previous slice folded scopes and stopped after the last entry.
		*ds.fold_cursor.lock() = FoldCursor {
			resume_at: keys.last().cloned(),
			progressed: true,
			scans: HashMap::new(),
		};

		let pass = Datastore::graph_fold_slice(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(pass.items, 3, "the entries before the cursor must be reached");
		assert!(fold_queue_keys(&ds).await?.is_empty(), "the queue must be drained");
		assert_eq!(
			*ds.fold_cursor.lock(),
			FoldCursor::default(),
			"a drained queue leaves no cursor"
		);
		Ok(())
	}

	/// A rotation over scopes that all fail can span several slices, each
	/// ending at its budget with a backlog. The slice that reaches the end of
	/// the range must know the whole rotation made no progress — not only its
	/// own share of it — and yield with the cursor cleared, or the drain would
	/// wrap and report a backlog for as long as the scopes keep failing.
	#[test_log::test(tokio::test)]
	async fn graph_fold_rotation_without_progress_yields_at_the_range_end() -> Result<()> {
		use surrealdb_datastore::values::graph::AdjacencyValue;

		use crate::expr::Dir;
		use crate::key::schema::GraphPointerKey;
		use crate::val::RecordId;

		let (ds, _session) = new_index_compaction_test_ds().await?;
		let (ns, db) = {
			let txn = ds.transaction(Read).await?;
			let db = txn.get_db_by_name("test", "test", None).await?.expect("the test database");
			txn.cancel().await?;
			(db.namespace_id, db.database_id)
		};
		// Three scopes whose folds fail: a pointer key under a table nothing
		// has defined, and a queue entry naming its scope, so the fold reads
		// the catalog for the missing vertex table and errors.
		{
			let txn = ds.transaction(Write).await?;
			for n in 0..3 {
				let ghost = RecordId::new("ghost".into(), format!("g{n}"));
				let edge = RecordId::new("likes".into(), format!("e{n}"));
				let target = RecordId::new("person".into(), format!("p{n}"));
				let pointer = GraphPointerKey {
					ns,
					db,
					tb: Cow::Borrowed(&ghost.table),
					id: Cow::Borrowed(&ghost.key),
					dir: Dir::Out,
					foreign_table: Cow::Borrowed(&edge.table),
					foreign_key: Cow::Borrowed(&edge.key),
					target_table: Cow::Borrowed(&target.table),
					target_key: Cow::Borrowed(&target.key),
				};
				txn.set_key(&pointer, &AdjacencyValue::live()).await?;
				let queued = GraphFoldKey {
					ns,
					db,
					tb: Cow::Borrowed(&ghost.table),
					id: Cow::Borrowed(&ghost.key),
					dir: Dir::Out,
					nid: ds.id(),
					uid: Uuid::now_v7(),
				};
				txn.put_key(&queued, &()).await?;
			}
			txn.commit().await?;
		}
		let keys = fold_queue_keys(&ds).await?;
		assert_eq!(keys.len(), 3);
		// The previous slice of this rotation failed the first scope and
		// stopped at its budget with nothing to show for it.
		*ds.fold_cursor.lock() = FoldCursor {
			resume_at: Some(keys[0].clone()),
			progressed: false,
			scans: HashMap::new(),
		};

		let pass = Datastore::graph_fold_slice(
			Arc::clone(&ds),
			Duration::from_secs(60),
			CancellationToken::new(),
		)
		.await?;
		assert_eq!(pass.items, 2, "only the entries after the cursor are read; no wrap");
		assert_eq!(pass.progress, Progress::Done, "a failed rotation is no backlog");
		assert_eq!(fold_queue_keys(&ds).await?.len(), 3, "failed scopes keep their entries");
		assert_eq!(
			*ds.fold_cursor.lock(),
			FoldCursor::default(),
			"the next interval starts a fresh rotation from the head"
		);
		Ok(())
	}

	/// A queue that refills at least as fast as one pass drains it must not pin
	/// the sweep: the pass after one that ran out of budget in the root queue
	/// starts at the next queue, so a departed node's events still run.
	#[test_log::test(tokio::test)]
	async fn event_adoption_rotates_past_a_queue_that_exhausts_its_budget() -> Result<()> {
		let (ds, session) = new_index_compaction_test_ds().await?;
		ds.insert_node().await?;
		// Two passes' worth of full batches in the root queue, and one more.
		let root = 2 * EVENT_PROCESSING_SLICE_BATCHES * NORMAL_BATCH_SIZE as usize + 1;
		queue_async_events(&ds, &session, root).await?;
		while relocate_queue_to_root(&ds).await? > 0 {}
		let departed = ds.fork_for_test_with_node_id(Uuid::from_u128(10));
		departed.insert_node().await?;
		execute_all(&departed, &session, "CREATE person RETURN NONE;").await?;
		archive_node(&departed, departed.id()).await?;

		let first =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(first.progress, Progress::More, "the root queue outlasts the budget");
		assert_eq!(
			queued_events(&ds, departed.id()).await?,
			1,
			"the first pass stayed in the root"
		);

		let second =
			ds.event_processing_adopt(Duration::from_secs(1), &CancellationToken::new()).await?;
		assert_eq!(second.progress, Progress::More, "the root queue is still full");
		assert_eq!(queued_events(&ds, departed.id()).await?, 0, "the second pass moved on");
		Ok(())
	}

	/// A host's schedule owns this datastore's claim on the process metrics
	/// refresh once the refresh has run under it. Shutdown drops the schedule
	/// once the slice in flight has ended, which releases the claim to any
	/// other datastore in the process, and a slice driven after that reports
	/// nothing scheduled rather than building the schedule again.
	#[test_log::test(tokio::test)]
	async fn shutdown_drops_a_hosts_schedule_and_its_metrics_claim() -> Result<()> {
		let ds = Datastore::builder().without_maintenance_tasks().build_with_path("memory").await?;
		ds.check_version().await?;
		let first = ds.run_maintenance_slice().await?;
		assert_eq!(first.job, Some(crate::kvs::tasks::Job::SystemMetricsRefresh));
		assert!(ds.host_maintenance.lock().await.is_some(), "built on the first slice");

		ds.shutdown().await?;
		assert!(ds.host_maintenance.lock().await.is_none(), "dropped, and its claim with it");

		let after = ds.run_maintenance_slice().await?;
		assert_eq!(after.job, None);
		assert_eq!(after.next_due, None, "nothing is scheduled after shutdown");
		assert!(ds.host_maintenance.lock().await.is_none(), "and it is not built again");
		Ok(())
	}

	/// A slice that shutdown begins during drops the schedule itself when its
	/// job returns, whether or not shutdown was still waiting for it, so the
	/// claim the schedule's runner holds is released with the slice rather
	/// than with the datastore, and the host is told nothing is scheduled.
	#[test_log::test(tokio::test)]
	async fn a_slice_shutdown_begins_during_drops_the_schedule_when_it_returns() -> Result<()> {
		let opts = crate::options::EngineOptions::default()
			.with_event_processing_interval(Duration::from_millis(1));
		let ds = Datastore::builder()
			.without_maintenance_tasks()
			.with_engine_options(opts)
			.build_with_path("memory")
			.await?;
		ds.check_version().await?;
		let session = Session::owner().with_ns("test").with_db("test");
		execute_all(
			&ds,
			&session,
			"DEFINE NAMESPACE test; DEFINE DATABASE test;
			 DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE logged SCHEMALESS;
			 DEFINE EVENT log ON person ASYNC THEN {
			 	sleep(500ms);
			 	CREATE logged SET who = $after.id;
			 };
			 CREATE person:1 RETURN NONE;",
		)
		.await?;
		// The metrics refresh is due at registration; the event drain is due
		// next, and its slice runs the sleeping event.
		let first = ds.run_maintenance_slice().await?;
		assert_eq!(first.job, Some(crate::kvs::tasks::Job::SystemMetricsRefresh));
		tokio::time::sleep(Duration::from_millis(5)).await;

		// Shutdown begins, without waiting, while the event is running.
		let (slice, ()) = tokio::join!(ds.run_maintenance_slice(), async {
			tokio::time::sleep(Duration::from_millis(100)).await;
			ds.shutdown.cancel();
		});
		let slice = slice?;
		assert_eq!(slice.job, Some(crate::kvs::tasks::Job::EventProcessing));
		assert_eq!(logged_count(&ds, &session).await?, 1, "the event ran to its end");
		assert_eq!(slice.next_due, None, "nothing is scheduled after shutdown");
		assert!(ds.host_maintenance.lock().await.is_none(), "the schedule went with the slice");
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
	async fn initial_credentials_that_are_empty_or_whitespace_are_refused() {
		let ds = Datastore::new("memory").await.unwrap();
		for (user, pass) in [("", "root"), ("root", ""), ("   ", "root"), ("root", " \t\n")] {
			assert!(
				ds.initialise_credentials(user, pass).await.is_err(),
				"credentials {user:?} / {pass:?} should be refused"
			);
		}
		let txn = ds.transaction(Read).await.unwrap();
		assert!(txn.all_root_users(None).await.unwrap().is_empty());
		txn.cancel().await.unwrap();
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
