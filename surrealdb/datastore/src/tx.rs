//! Transaction implementation and cache coordination.
//!
//! Cache paths use `Entry::Any(val.clone())` for concrete `Arc<T>` values that must coerce to
//! `Arc<dyn Any + Send + Sync>`; `Arc::clone(&val)` does not perform that unsized coercion.
#![allow(clippy::clone_on_ref_ptr)]
// `Transaction`'s pub methods take `K: KVKey` / `K::Value: KVValue`.
// Both traits are `pub` (their `pub` declarations are gated by
// `pub use` re-exports in `kvs/mod.rs`), so the lint flags every
// such method. The visibility is intentional — silence at module scope.
#![allow(private_bounds, private_interfaces)]

use std::any::Any;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZeroU64;
use std::ops::{ControlFlow, Deref};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
use common::EngineError;
use common::time::sleep;
use roaring::RoaringTreemap;
use surrealdb_catalog::node::{Node, NodeRelease, Timestamp};
use surrealdb_catalog::providers::{
	ApiProvider, AuthorisationProvider, BoxProviderFut, BucketProvider, CachePolicy,
	CancellationProbe, CatalogProvider, DatabaseProvider, NamespaceProvider, NodeProvider,
	RootProvider, TableProvider, UserProvider,
};
use surrealdb_catalog::stored::{CATALOG_MARKER, EntryError};
use surrealdb_catalog::{
	self, Conformance, DatabaseCompatibility, DatabaseDefinition, DatabaseId, DefaultConfig,
	Error as CatalogError, FieldCandidate, FieldClaim, FromStored, Generation, GenerationLayout,
	GenerationMark, GenerationRing, IndexId, LATEST_EDGE_VARIANT, LayoutDescriptor, LayoutRefusal,
	Metadata, Migration, MigrationRun, NamespaceDefinition, NamespaceId, Record, RecordFormat,
	RecordLayoutError, RecordType, RecordWriter, StoredConfigDefinition, StoredFeatureLevel,
	StoredFieldDefinition, StoredRecord, StoredTableDefinition, TableConformance, TableDefinition,
	TableId, TableLayouts, TableMigrations, TableShape, TableType, birth_claims_of,
	decode_generation, definition_digest, derive_layout,
};
use surrealdb_expr::val::stored::schema::Unrevisioned;
use surrealdb_expr::val::{RecordId, RecordIdKey, RecordIdentity, TableName, Value};
use surrealdb_kvs::api::{
	Batch, KeyVisitor, KeysBatch, RangeKeyVisitor, ScanChunkStats, ScanCursorKeys, ScanCursorVals,
	ValVisitor, ValsBatch,
};
use surrealdb_kvs::key::{
	AnyRange, KVKey, KVKeyDecode, KVSubspace, RawRange, Resumable, TypedRange,
};
use surrealdb_kvs::snapshot::ViewCapabilities;
use surrealdb_kvs::timestamp::{BoxTimeStamp, BoxTimeStampImpl};
use surrealdb_kvs::value::{EncodeContext, Encoded, KVValue};
use surrealdb_kvs::{Estimate, FeatureLevel, Key, KeyRange, Release, Snapshot, SplitTarget};
use surrealdb_observe::{
	ExecutionObserver, Outcome, TenantIdentity, TransactionEvent, TransactionEventSafe,
	TransactionMetrics,
};
use tokio::sync::Mutex;
use uuid::Uuid;
use web_time::Instant;

use crate::cache::tx::TransactionCache;
// The transaction holds the queue because only it knows when the outcome is
// settled; what to run belongs to the layer that registered it.
use crate::close::{
	AfterCommitQueue, AfterCommitWait, AfterCommitWaits, CommitAction, RollbackAction,
};
use crate::feature::{FeatureLevelCell, FeatureLevelState, LevelledMode, Readable};
use crate::index_shapes::{IndexShapes, ShapeRecorder, TableIndexShapes, ValueShape};
use crate::key::reclaim::{ALL_DOC_ID_KINDS, Expunge, ReclaimState};
use crate::key::schema::{
	AdjacencyEdgeTablePrefix, AdjacencyKey, AnalyzerKey, AnalyzerPrefix, ApiKey, ApiPrefix,
	BucketKey, BucketPrefix, BuildAppendTicketPrefix, BuildReservationKey, BuildStateKey,
	CatalogVersionKey, CatalogVersionPrefix, ChangeFeedKey, ChangefeedFenceKey, DatabaseKey,
	DatabasePrefix, DbAccessMethodKey, DbAccessMethodPrefix, DbAccessRoot, DbConfigKey,
	DbConfigPrefix, DbGrantKey, DbGrantPrefix, DbUserKey, DbUserPrefix, DocLookupIdentityKey,
	DocStatsBatchKey, EventKey, EventPrefix, FeatureLevelKey, FieldKey, FieldPrefix,
	ForeignTablePrefix, FunctionKey, FunctionPrefix, GenerationMarkKey, GraphFoldKey,
	GraphPointerKey, IdxRoot, IndexBuildShapesKey, IndexCompactionKey, IndexCountKey, IndexDefKey,
	IndexDefPrefix, IndexNameKey, IndexShapesKey, IndexShapesTrackedKey, LayoutKey, LayoutPrefix,
	LiveEventsKey, MigrationKey, MigrationRunKey, MigrationRunPrefix, MlModelKey, MlModelPrefix,
	ModuleKey, ModulePrefix, NamespaceKey, NamespacePrefix, NodeKey, NodePrefix, NodeReleaseKey,
	NsAccessMethodKey, NsAccessMethodPrefix, NsAccessRoot, NsGrantKey, NsGrantPrefix, NsUserKey,
	NsUserPrefix, ObjectVersionKey, ParamKey, ParamPrefix, ReclaimKey, ReclaimTbPrefix, RecordKey,
	RecordPrefix, RootAccessMethodKey, RootAccessMethodPrefix, RootAccessRoot, RootConfigKey,
	RootGrantKey, RootGrantPrefix, RootUserKey, RootUserPrefix, SequenceKey, SequencePrefix,
	StoreDefaultsKey, SubscriptionPrefix, TableConformanceKey, TableKey, TableMigrationKey,
	TableMigrationRequestKey, TableMigrationRequestTbPrefix, TableMigrationsKey, TablePrefix,
	TermChangeBatchKey, VersionKey,
};
#[cfg(diskann)]
use crate::key::schema::{
	DiskannPendingKey, DiskannPendingPrefix, DiskannRecordPendingShardShardPrefix,
	DiskannWriterLeaseKey, DiskannWriterLeaseShardPrefix,
};
use crate::key::table_migration::{TableMigrationRequest, TableMigrationState, VerifiedField};
use crate::layouts::LayoutSet;
use crate::obs::Instrument;
use crate::out_of_line::OutOfLine;
use crate::sequences::{Sequence, Sequences};
#[cfg(any(test, feature = "test-hooks"))]
use crate::testing::{
	NonRetryableErrorSite, RetryableConflictSite, maybe_inject_non_retryable_error,
	maybe_inject_retryable_conflict,
};
use crate::triggers::CommitTriggers;
use crate::values::catalog_version::{CatalogObject, CatalogObjectKind, CatalogVersion};
use crate::values::changefeed::Changefeed;
#[cfg(diskann)]
use crate::values::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnPendingStateKind, DiskAnnWriterLease,
};
use crate::values::fulltext::DocLengthAndCount;
use crate::values::graph::{AdjacencyBlock, BlockEntries, GraphFoldScope};
use crate::values::ids::DocId;
use crate::values::index_build::{
	BuildGeneration, BuildTicket, BuildTicketMutationSeq, IndexBuildPhase, IndexBuildReportStatus,
	IndexBuildState,
};
use crate::values::index_delta::{BufferedIndex, BufferedTerm, IndexDeltaBuffer};
use crate::values::live_query::LiveEventBuffer;
use crate::version::ledger::{RECORD_FORMAT_V1, RECORD_FORMAT_V1_NAME, RECORD_FORMAT_V1_VERSION};
use crate::version::{MajorVersion, MigrationRecord, StoreDefaults, unix_millis_now};
use crate::{
	DatastoreError, Direction, Error as KvsError, IntoBytes, NORMAL_BATCH_SIZE, TransactionConfig,
	TransactionFactory, TransactionType, Transactor, Val, cache, catalog,
	is_retryable_transaction_conflict, storage_error, util,
};

/// How many times a commit covers its DiskANN pending-state guards before it
/// reports the conflict. Covering conflicts only with another writer covering
/// the same guard, or on a shared store with a guard step, and a retry then
/// finds the guard covered, so a second attempt usually settles the race; the
/// bound only keeps a misbehaving store from looping. Each retry backs off
/// first ([`diskann_guard_arm_backoff`]).
#[cfg(diskann)]
const DISKANN_GUARD_ARM_ATTEMPTS: u32 = 8;

/// The wait before retrying a DiskANN guard covering that lost a conflict on
/// attempt number `attempt`: 1 ms doubling per attempt up to 16 ms, each drawn
/// from half to one and a half times that.
#[cfg(diskann)]
fn diskann_guard_arm_backoff(attempt: u32) -> Duration {
	let base = Duration::from_millis(1) * 2u32.pow(attempt.saturating_sub(1).min(4));
	base.mul_f64(rand::random_range(0.5..=1.5))
}

/// How long a DiskANN writer lease holds once taken or renewed
/// (`DiskAnnWriterLease::expires_at`). Past it, any node's guard step retires
/// it. A commit relying on a lease with less than
/// [`DISKANN_WRITER_LEASE_RENEWAL`] left renews it, and a lease nothing has
/// relied on is released well within it, so a lease expires only once its
/// datastore has stopped, restarted or stalled. Nodes' clocks must agree to
/// within the renewal margin.
#[cfg(diskann)]
const DISKANN_WRITER_LEASE_TTL: chrono::Duration = chrono::Duration::seconds(30);

/// The time left on a DiskANN writer lease below which a commit relying on it
/// renews it.
#[cfg(diskann)]
const DISKANN_WRITER_LEASE_RENEWAL: chrono::Duration = chrono::Duration::seconds(15);

/// The writer lease (`!dz`) node `nid` takes on the shard whose guard is `key`.
#[cfg(diskann)]
fn diskann_writer_lease_key<'a>(
	key: &'a DiskannPendingKey<'_>,
	nid: Uuid,
) -> DiskannWriterLeaseKey<'a> {
	DiskannWriterLeaseKey {
		ns: key.ns,
		db: key.db,
		tb: Cow::Borrowed(key.tb.as_ref()),
		ix: key.ix,
		shard: key.shard,
		nid,
	}
}

/// Release the DiskANN writer leases (`!dz`) this datastore holds on indexes
/// it has committed nothing under for `idle`, and return how many it released.
///
/// Each index's leases are released holding its guard fence exclusively, so no
/// commit of this datastore relies on them while they go, and in one
/// transaction that also queues a compaction pass for the index: no node steps
/// a guard under a lease, so a guard left `NonEmpty` over a drained shard is
/// stepped by that pass. A lease another process of this node took is left for
/// the step that recognises it ([`Transaction::step_diskann_pending_guards`]).
#[cfg(diskann)]
pub(crate) async fn release_idle_diskann_writer_leases(
	tf: &TransactionFactory,
	triggers: &CommitTriggers,
	sequences: &Sequences,
	idle: Duration,
) -> Result<usize> {
	let fenced = &triggers.pending_guard_fences;
	let (node, process) = (sequences.node_id(), fenced.process());
	let mut released = 0;
	for (prefix, index) in fenced.idle_lease_fences(idle) {
		let fence = fenced.exclusive(&prefix, || index.clone()).await;
		// A commit may have relied on the leases while the fence was awaited.
		let shards = fence.idle_leases(idle);
		if shards == 0 {
			continue;
		}
		let tx = tf.transaction(TransactionType::Write, sequences.clone()).await?;
		let result: Result<()> = async {
			// Read in one batch: the exclusive fence holds the index's writers
			// back meanwhile.
			let keys: Vec<_> = (0..u64::BITS as u16)
				.filter(|shard| shards & (1 << shard) != 0)
				.map(|shard| DiskannWriterLeaseKey {
					ns: index.ns,
					db: index.db,
					tb: Cow::Borrowed(&index.tb),
					ix: index.ix,
					shard,
					nid: node,
				})
				.collect();
			let leases = tx.get_many_key(keys.clone(), None).await?;
			for (key, lease) in keys.iter().zip(leases) {
				if lease.is_some_and(|lease| lease.process == process) {
					tx.del_key(key).await?;
				}
			}
			tx.buffer_compaction_trigger(index.ns, index.db, &index.tb, index.ix, node);
			tx.commit().await
		}
		.await;
		match result {
			Ok(()) => {
				fence.release_leases(shards);
				released += shards.count_ones() as usize;
			}
			Err(e) => {
				if !tx.closed() {
					let _ = tx.cancel().await;
				}
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"releasing idle DiskANN writer leases failed, to be retried: {e}"
				);
			}
		}
	}
	Ok(released)
}

/// Without DiskANN no writer lease is ever taken, so none is released.
#[cfg(not(diskann))]
pub(crate) async fn release_idle_diskann_writer_leases(
	_tf: &TransactionFactory,
	_triggers: &CommitTriggers,
	_sequences: &Sequences,
	_idle: Duration,
) -> Result<usize> {
	Ok(0)
}

/// The DiskANN guard fences a commit holds across its storage commit
/// ([`Transaction::arm_diskann_pending_guards`]).
#[derive(Default)]
struct DiskAnnGuardFences {
	/// Held shared, for indexes the transaction wrote into.
	#[cfg(diskann)]
	shared: Vec<crate::triggers::SharedGuardFence>,
	/// Held exclusively, for indexes whose data the transaction wiped.
	#[cfg(diskann)]
	wiped: Vec<crate::triggers::ExclusiveGuardFence>,
	/// On a store other datastores write to, the earliest expiry among the
	/// writer leases the commit relies on, and the guards it covered, to cover
	/// again if the commit lands too close to that expiry
	/// ([`Transaction::recover_diskann_leases`]).
	#[cfg(diskann)]
	#[allow(clippy::type_complexity)]
	leases_relied:
		Option<(chrono::DateTime<Utc>, Vec<(Vec<DiskannPendingKey<'static>>, usize, u64)>)>,
}

impl DiskAnnGuardFences {
	/// Release the fences once the commit has returned, first forgetting what
	/// was known of a wiped index's guards and leases, whatever the commit's
	/// outcome.
	fn finish(self) {
		#[cfg(diskann)]
		for fence in &self.wiped {
			fence.forget_all();
		}
	}
}

/// Counts a write or a save-point rollback in
/// [`Transaction::state_changes`] when dropped: held across the change, so
/// the count grows once the change has taken effect.
#[must_use = "the change is counted when the guard is dropped"]
struct StateChange<'a>(&'a AtomicU64);

impl Drop for StateChange<'_> {
	fn drop(&mut self) {
		self.0.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
	}
}

/// The catalog epoch of a transaction that names none; see
/// [`Transaction::catalog_epoch`].
pub const NO_CATALOG_EPOCH: u64 = u64::MAX;

/// The slot a transaction holds for a sequence it defined: the allocator it
/// owns for that sequence, empty until something draws from it.
///
/// Its lock is what makes the first draw single-flight. Building the allocator
/// claims a window and writes it through the transaction, so two draws that
/// both found the slot empty would both claim — and the second claim would
/// collide on a row the first created. Whoever takes the lock builds; whoever
/// waits finds it built.
pub(crate) type ProvisionalSequence = Arc<Mutex<Option<Sequence>>>;

/// A transaction's provisional slots, with what each held, at one moment. See
/// `Transaction::provisional_save_points`.
type ProvisionalSnapshot = Vec<((NamespaceId, DatabaseId, String), Option<Sequence>)>;

/// What a levelled write read at its key, in the form [`Transaction::changed_definition`]
/// takes: `None` when the write read nothing.
fn held(prior: &Option<Option<Val>>) -> Option<Option<&[u8]>> {
	prior.as_ref().map(Option::as_deref)
}

/// The store operation an unconditional encoded write performs.
#[derive(Clone, Copy)]
enum WriteOp {
	Set,
	Put,
	Replace,
}

/// The answer to admitting the floor an encoding carries: the bytes stand, or
/// they are encoded again at the level the store has been found to hold.
enum Admission {
	Accept,
	RetryAt(FeatureLevel),
}

/// Whether a failure to decode a catalog entry is a refusal because a newer
/// release wrote it ([`EntryError::NeedsRelease`]), found anywhere in the
/// failure's source chain.
pub(crate) fn refused_by_release(err: &anyhow::Error) -> bool {
	err.chain()
		.any(|cause| matches!(EntryError::of_cause(cause), Some(EntryError::NeedsRelease { .. })))
}

/// The compatibility level a store's defaults name, refused when the stored number is no level.
fn compatibility_of(defaults: &StoreDefaults) -> Result<DatabaseCompatibility> {
	DatabaseCompatibility::new(defaults.compatibility).ok_or_else(|| {
		anyhow::Error::new(EngineError::Internal(format!(
			"the store's default compatibility level is not 3 or 4: {}",
			defaults.compatibility
		)))
	})
}

/// The refusal of `layouts`' table, when the table's generation ring is one this build cannot
/// read: nothing is written to the table's rows or minted for it on a guess.
fn ring_refusal_error(layouts: &TableLayouts) -> Option<anyhow::Error> {
	layouts.ring_refusal().map(|cause| {
		CatalogError::Layout(RecordLayoutError::RingRefused {
			table: layouts.table().clone(),
			cause: cause.clone(),
		})
		.into()
	})
}

/// Whether a failure is [`DatastoreError::FeatureNotEnabled`], found anywhere in the failure's
/// source chain.
fn refused_by_level(err: &anyhow::Error) -> bool {
	err.chain().any(|cause| {
		matches!(
			cause.downcast_ref::<DatastoreError>(),
			Some(DatastoreError::FeatureNotEnabled { .. })
		)
	})
}

/// Brings the datastore's feature level cell up to a record this transaction wrote, once the
/// transaction has committed.
struct ObserveFeatureLevel {
	cell: Arc<FeatureLevelCell>,
	record: StoredFeatureLevel,
}

impl CommitAction for ObserveFeatureLevel {
	fn run(self: Box<Self>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
		Box::pin(async move { self.cell.observe(Some(&self.record)) })
	}
}

/// Takes the stored-format ledger entry as committed in the datastore's feature level cell, once
/// the transaction that wrote or saw the entry has committed.
struct ObserveStoredFormat {
	cell: Arc<FeatureLevelCell>,
}

impl CommitAction for ObserveStoredFormat {
	fn run(self: Box<Self>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
		Box::pin(async move { self.cell.observe_stored_format() })
	}
}

/// A transaction's view of the layouts of the tables it has read or written, by namespace and
/// database and then by table, so a lookup borrows the table's name instead of cloning it.
type LayoutMemo = HashMap<(NamespaceId, DatabaseId), HashMap<TableName, Arc<TableLayouts>>>;

/// The keys a range delete must not remove unnoticed from a table whose layouts this transaction
/// holds: `[start, end)` covers the table's `!fc`, `!lg` and `!mi` keys
/// ([`Transaction::hold_span`]).
#[derive(Clone, Debug)]
enum HeldSpan {
	/// The keys from `start` up to, and excluding, `end`.
	Bounded {
		start: Key<'static>,
		end: Key<'static>,
	},
	/// A span whose keys could not be encoded, which overlaps every range.
	Unbounded,
}

impl HeldSpan {
	/// Whether the delete of `range` may remove a key of the span, comparing the borrowed bytes.
	fn overlaps(&self, range: &KeyRange<'_>) -> bool {
		match self {
			Self::Bounded {
				start,
				end,
			} => range.start.as_slice() < end.as_slice() && start.as_slice() < range.end.as_slice(),
			Self::Unbounded => true,
		}
	}
}

/// A transaction's held spans, by namespace and database and then by table.
type HeldSpans = HashMap<(NamespaceId, DatabaseId), HashMap<TableName, HeldSpan>>;

/// The tables whose definition a transaction created, and the born tables whose conformance the
/// table's next stamp settles.
///
/// A birth is the one moment a table has no writer that has seen it, which is what a floor that is
/// known at birth needs ([`Transaction::mint_with`]). A birth is keyed by the id the creating write
/// gave the table, so it can never apply to another incarnation of the name: a save point rollback
/// that restores an older definition leaves a birth that names an id the definition does not
/// carry. Save points therefore do not restore it, and a speculation starts from a copy.
#[derive(Clone, Debug, Default)]
struct Births {
	/// The tables whose definition key this transaction wrote while it held nothing.
	born: HashMap<(NamespaceId, DatabaseId, TableName), Birth>,
	/// The born tables whose conformance the table's next stamp settles.
	unsettled: HashSet<(NamespaceId, DatabaseId, TableName)>,
	/// The field definitions of a table as the last settle or derivation compiled them. A
	/// compiled definition is a function of the definition's bytes ([`CompiledField`]), so an
	/// entry stays right whatever else the transaction does, and the next read compiles only the
	/// definitions whose bytes changed since the last one. The creation of a table clears its
	/// entry.
	compiled: HashMap<(NamespaceId, DatabaseId, TableName), Arc<CompiledFields>>,
}

/// The compiled field definitions of a table, by the `fd` component of the field's key.
type CompiledFields = HashMap<String, CompiledField>;

/// A field definition compiled for the birth claim rules, with the stored bytes it was compiled
/// from.
#[derive(Clone, Debug)]
struct CompiledField {
	/// The stored definition `candidate` was compiled from.
	bytes: Arc<[u8]>,
	/// The definition, compiled.
	candidate: Arc<FieldCandidate>,
}

/// A table this transaction created ([`Births`]).
#[derive(Clone, Copy, Debug)]
struct Birth {
	/// The id the creating write gave the table.
	table_id: TableId,
	/// Whether this transaction minted the table's first generation after creating it, so the
	/// ring the table holds is one its birth started.
	minted_ring: bool,
}

/// What a transaction holds of a table's conformance record `!fc`
/// ([`Transaction::held_conformance`]).
#[derive(Clone, Debug)]
enum HeldConformance {
	/// The record was read and is not there.
	Absent,
	/// This transaction removed the record with a delete conditional on the bytes it read, which
	/// its write set holds ([`Transaction::forfeit_conformance`]).
	Forfeited,
	/// The record, as this transaction reads or wrote it.
	Claims(Arc<TableConformance>),
	/// The record is there and this build does not decode it, so it states no claims.
	Unreadable,
}

/// A transaction's view of the conformance records of the tables it has read or written, by
/// namespace and database and then by table.
type ConformanceMemo = HashMap<(NamespaceId, DatabaseId), HashMap<TableName, HeldConformance>>;

/// What a save point records when it opens, so that rolling it back undoes exactly what its scope
/// did to the transaction's own bookkeeping.
struct SavePointMark {
	/// How many commit actions were queued; the rollback drops those queued after it
	/// ([`Transaction::commit_actions`]).
	actions: usize,
	/// The layout epoch; a rollback finding it moved forgets the layouts the transaction holds
	/// ([`Transaction::layout_epoch`]).
	layout_epoch: u64,
	/// The count of range deletes; a rollback finding it moved forgets the layouts the
	/// transaction holds, because the delete it undoes may have removed layouts.
	range_deletes: u64,
	/// Whether the transaction already held the stored-format ledger entry; a rollback of a scope
	/// that took it discards the observation ([`Transaction::admit_stored_format`]).
	stored_format_admitted: bool,
}

/// A record as a read answers it, with the generation its stored bytes carry: `None` for a
/// `revision` record, stamp 0, a lightweight edge and an id that names no record.
pub type StampedRecord = (Arc<Record>, Option<Generation>);

/// What admitting a stored record found ([`Transaction::admit_record_bytes`]).
struct Admitted {
	/// The generation the record's bytes carry: `None` for a `revision` record and for stamp 0.
	generation: Option<Generation>,
	/// The layouts that admitted the record, for a packed row only: its decode reads its
	/// descriptor from them. A document and a `revision` row clone no layouts, because their
	/// decode needs none.
	layouts: Option<Arc<TableLayouts>>,
}

impl Admitted {
	/// A `revision` record, or a record of stamp 0: no generation, and no layouts to decode by.
	const UNSTAMPED: Self = Self {
		generation: None,
		layouts: None,
	};
}

/// The envelope of the stored record `bytes`, for a record its table's layouts must admit: the
/// generation its stamp names (`Err(stamp)` for a stamp that names none) and whether the row is
/// packed. `None` for a `revision` record and for stamp 0, which no layouts judge.
fn stamped_envelope(bytes: &[u8]) -> Result<Option<(std::result::Result<Generation, u64>, bool)>> {
	let Some(stored) = StoredRecord::parse(bytes)? else {
		return Ok(None);
	};
	Ok(match stored.generation() {
		Ok(None) => None,
		Ok(Some(generation)) => Some((Ok(generation), stored.is_packed())),
		Err(stamp) => Some((Err(stamp), stored.is_packed())),
	})
}

/// Admits a record of `tb` whose envelope [`stamped_envelope`] read against `layouts`, in the one
/// order every reader checks: the layouts are `tb`'s, the stamp names a generation, and the
/// layouts list that generation with a descriptor this build reads. A packed row keeps the
/// layouts it is decoded by.
fn admit_with_layouts(
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	generation: std::result::Result<Generation, u64>,
	packed: bool,
	layouts: Arc<TableLayouts>,
) -> std::result::Result<Admitted, RecordLayoutError> {
	if !layouts.belongs_to(ns, db, tb) {
		return Err(RecordLayoutError::ForeignLayouts {
			layouts: layouts.table().clone(),
			table: tb.clone(),
		});
	}
	let generation = generation.map_err(|stamp| layouts.stamp_refusal(stamp))?;
	layouts.admit(generation)?;
	Ok(Admitted {
		generation: Some(generation),
		layouts: packed.then_some(layouts),
	})
}

/// What [`Transaction::getm_records_stored`] found for one record id.
#[derive(Debug)]
pub enum StoredSlot {
	/// The record the transaction's cache holds, and the generation its stored bytes carry.
	Cached(Arc<Record>, Option<Generation>),
	/// The record's stored bytes: the entry of the multi-get's answer at this position.
	Stored(usize),
	/// Nothing the caller may use: the multi-get found no bytes (the record is missing, or a
	/// lightweight edge with no stored value), or the cache holds an entry that is not a record
	/// with data. The caller reads the id for itself.
	Absent,
}

/// The buffers of the keys [`Transaction::getm_records_stored`] reads, kept across calls.
#[derive(Default)]
pub struct RecordKeys {
	/// The encoded keys, back to back.
	bytes: Vec<u8>,
	/// The span of each key in [`Self::bytes`].
	spans: Vec<(usize, usize)>,
	/// The vector the borrowed keys of one call are built in, empty between calls.
	keys: Vec<Key<'static>>,
}

pub struct Transaction {
	/// Is this is a local datastore transaction?
	local: bool,
	/// The wall-clock instant the transaction was opened. Used to compute
	/// transaction lifetime when emitting the terminal
	/// [`surrealdb_observe::TransactionEvent`].
	started_at: Instant,
	/// Observability hook fired on commit/cancel. Defaults to
	/// [`surrealdb_observe::NoopObserver`] and is otherwise supplied by the
	/// datastore builder.
	observer: Arc<dyn ExecutionObserver>,
	/// Per-transaction KV operation counters, updated by the wrapper methods
	/// on `Transaction` (`get`, `set`, `scan`, ...). Snapshotted once when
	/// the transaction finishes to populate the emitted event.
	metrics: TransactionMetrics,
	/// Pre-resolved tenant identity for this transaction. Sourced from the
	/// originating session via [`crate::ctx::Context::tenant_identity`] and
	/// surfaced as the `*Ctx` half of the emitted [`TransactionEvent`].
	///
	/// `OnceLock` so callers that wrap the [`Transaction`] in an `Arc` before
	/// the session is known (a client-owned transaction handed to
	/// `Datastore::run`) can still attach identity after the fact via
	/// [`Self::set_tenant_identity`].
	tenant_identity: OnceLock<Arc<TenantIdentity>>,
	/// The underlying transactor.
	pub(crate) tr: Transactor,
	/// The query cache for this store
	cache: TransactionCache,
	/// The caches speculations read through, each with the speculative state
	/// its entries describe and the [`Self::state_changes`] reading of this
	/// transaction they were read over: a speculation starting from the same
	/// overlay reads through its cache again while this transaction has not
	/// changed since.
	speculative_caches: Mutex<Vec<(crate::speculation::Overlay, u64, Arc<cache::tx::Cache>)>>,
	/// The sequences for this store
	sequences: Sequences,
	/// The feature level the store was last read at, shared with every transaction of the store
	/// ([`Self::write_level`]).
	feature: Arc<FeatureLevelCell>,
	/// Whether the engine this transaction runs on admits no other writer of the store
	/// ([`Self::sole_writer`]), as the engine answered when the transaction opened.
	sole_writer: bool,
	/// This transaction's view of each table's layouts, filled by a read of the table's `!lg`
	/// keys or adopted from a shared snapshot ([`Self::table_layouts`]).
	///
	/// Cleared by a range delete that spans a held table's layouts and by a save point rollback
	/// that undoes a mint, a floor move, a range delete or the stored-format ledger entry, the
	/// events that can change which generations the transaction sees, and each of them moves the
	/// layout epoch. A generation number names one descriptor at a time, but a save point
	/// rollback can undo a mint after which the transaction mints the same number with other
	/// bytes, so a missed invalidation could leave a number naming another descriptor, and a
	/// packed row read through it would decode to wrong values. That is why every holder of
	/// layouts (a row gate, a layout set, a write context's format slot) re-resolves when the
	/// layout epoch moves.
	layout_memo: parking_lot::Mutex<LayoutMemo>,
	/// This transaction's view of each table's conformance record `!fc`, filled by a read of the
	/// key or by this transaction's own write or delete of it ([`Self::held_conformance`]).
	///
	/// Cleared with the layout memo ([`Self::invalidate_layouts`]); never shared with another
	/// transaction, because the record changes without a catalog version stamp.
	conformance_memo: parking_lot::Mutex<ConformanceMemo>,
	/// The span of keys of every table either memo holds, computed once when the table enters a
	/// memo ([`Self::hold_span`]) and cleared with them, so that a range delete is compared with
	/// each span by borrowed bytes ([`Self::begin_range_delete`]).
	held_spans: parking_lot::Mutex<HeldSpans>,
	/// The tables whose definition this transaction created, with the id it gave each, and the
	/// born tables the next stamp settles ([`Births`]).
	births: parking_lot::Mutex<Births>,
	/// Counts the events that change which generation is current, what the transaction holds of a
	/// table's conformance record, or whether the stored-format ledger entry is part of this
	/// transaction: a mint, a floor move, a write or removal of a conformance record, a range
	/// delete that spans a held table's keys and a save point rollback that undid any of them or
	/// the entry. A writer that holds a format it resolved at one value of the epoch holds it only
	/// while the epoch is unchanged ([`Self::layout_epoch`]).
	layout_epoch: AtomicU64,
	/// How many range deletes this transaction has issued. A save point rollback that finds it
	/// moved may have restored keys a delete removed, including layouts of a table the
	/// transaction had not yet read when the delete ran.
	range_deletes: AtomicU64,
	/// Whether this transaction holds the stored-format ledger entry: it wrote the entry, or read
	/// it, and no save point rollback has discarded the observation
	/// ([`Self::admit_stored_format`]).
	stored_format_admitted: AtomicBool,
	/// Whether this transaction read the stored-format ledger entry from storage and found it
	/// absent ([`Self::stored_format_present`]). The entry can enter the transaction's view only
	/// through its own write, which sets [`Self::stored_format_admitted`], and that flag is
	/// consulted first; so a save point rollback that undoes the write leaves this flag, and its
	/// answer, correct.
	stored_format_absent: AtomicBool,
	/// Set when a schema statement failed after writing part of its change, so the writes the
	/// transaction holds are a state no caller asked for. A transaction with it set refuses to
	/// commit, as for the other poisons ([`Self::poisoned`]); a save point rollback does not clear
	/// it.
	catalog_write_poisoned: AtomicBool,
	/// Set when a statement failed after writing part of its change, in a transaction its caller
	/// commits whatever the statement answered ([`Self::poison_failed_statement`]). A transaction
	/// with it set refuses to commit, as for the other poisons ([`Self::poisoned`]); a save point
	/// rollback does not clear it.
	failed_statement_poisoned: AtomicBool,
	/// What each open save point recorded when it opened, innermost last, so that a rollback
	/// undoes exactly what the scope did to the transaction's bookkeeping ([`SavePointMark`]).
	save_point_marks: Mutex<Vec<SavePointMark>>,
	/// How many times this transaction has stamped a table's catalog version
	/// ([`Self::bump_catalog_version`]). It only grows, so a reader that saw
	/// the same count earlier in the transaction knows no table's version has
	/// changed since.
	catalog_stamps: AtomicU64,
	/// How many times this transaction has stamped the version of a catalog
	/// object other than a table ([`Self::catalog_object_version`]), with the
	/// same contract as [`Self::catalog_stamps`] for those objects.
	object_stamps: AtomicU64,
	/// Whether this transaction wrote a definition that moves the catalog
	/// epoch without stamping a catalog version: a namespace, a database, a
	/// user, or a table's definition rewritten in place
	/// ([`Self::touches_catalog`]).
	catalog_touched: AtomicBool,
	/// The version each table this transaction stamped was last stamped
	/// with, so a later write of the table's definition in the transaction
	/// prints the version again from what it wrote
	/// ([`Self::reprint_stamped_table`]).
	stamped_tables: parking_lot::Mutex<HashMap<(NamespaceId, DatabaseId, TableName), Uuid>>,
	/// The indexes this transaction has written value shapes for
	/// ([`Self::record_index_shapes`], [`Self::track_index_shapes`]), kept
	/// until it ends: every rollback to a save point drops what the cache
	/// holds of them, and of their tables' catalog versions, since the writes
	/// it undoes may be among them.
	shaped_indexes: parking_lot::Mutex<Vec<(NamespaceId, DatabaseId, TableName, IndexId)>>,
	/// The count of the datastore's catalog commits finished when this
	/// transaction opened, with none under way, where the datastore is the
	/// store's sole writer; [`NO_CATALOG_EPOCH`] otherwise
	/// ([`Self::catalog_epoch`]).
	catalog_epoch: u64,
	/// How many writes and save-point rollbacks this transaction has taken
	/// ([`Self::state_changes`]).
	state_changes: Arc<AtomicU64>,
	/// The recordings of catalog reads open on this transaction
	/// ([`Self::record_catalog_reads`]).
	catalog_reads: crate::catalog_reads::CatalogReadLog,
	/// The records created in the tables a write of this transaction is
	/// reading ([`Self::watch_creations`]).
	creations: crate::creations::Creations,
	/// The changefeed buffer.
	changefeed: OnceLock<Changefeed>,
	/// The live-query event buffer (dedicated keyspace, Router engine).
	live_events: OnceLock<LiveEventBuffer>,
	/// Per-transaction aggregation of count-index deltas and compaction
	/// triggers, flushed as one entry per index inside this transaction's
	/// commit rather than one per mutated document.
	/// Boxed so the buffer's size does not count against every `Transaction`.
	/// It is allocated only for a transaction that actually mutates an index, and
	/// `Transaction` is held on the stack through deep recursive paths where the
	/// inline size matters.
	index_deltas: OnceLock<Box<IndexDeltaBuffer>>,
	/// Post-commit wake-ups, fired once the commit makes the work visible.
	triggers: Arc<CommitTriggers>,
	/// Do we have to trigger async events after the commit?
	trigger_async_event: AtomicBool,
	/// Did this transaction queue any index-compaction work?
	trigger_index_compaction: AtomicBool,
	trigger_graph_fold: AtomicBool,
	/// Whether a commit of this transaction queued table migration work.
	trigger_table_migration: AtomicBool,
	/// The live-transaction bucket this transaction counted itself into
	/// ([`crate::triggers::LiveTransactions`]), or [`crate::triggers::NO_LIVE_BUCKET`].
	live_bucket: u8,
	/// Write-cardinality guard: maximum number of individual key writes this
	/// transaction may buffer before further writes fail. Unset by default,
	/// leaving the transaction unbounded. Armed once — by the executor for
	/// its own statement transactions via [`Self::with_write_keys_limit`],
	/// or through [`Self::arm_write_keys_limit`] for externally-supplied
	/// (client-owned, `Arc`-wrapped) transactions when statements execute on
	/// them. `OnceLock` for the same reason as `tenant_identity`: the
	/// external path only sees the transaction after it is wrapped in an
	/// `Arc`.
	write_keys_limit: OnceLock<NonZeroU64>,
	/// Set when the write-cardinality guard trips. A poisoned transaction
	/// refuses COMMIT (rolling back instead), because the writes buffered
	/// before the failing reservation are a partial statement; CANCEL
	/// behaves as normal. This is what preserves the guard's atomic
	/// rollback contract on client-owned (RPC/SDK) transactions, whose
	/// lifecycle the executor does not manage.
	write_guard_poisoned: AtomicBool,
	/// Set when the storage layer could not close a save point.
	///
	/// Which of the scope's writes survive is then unknown, so the buffered index
	/// deltas can be neither kept nor dropped: keeping them risks flushing a term
	/// change for a posting that does not exist, and dropping them risks a
	/// posting no bitmap names. Their frames cannot be held at the storage
	/// stack's depth either, so an outer release would fold the wrong one.
	///
	/// Refusing the commit is what makes the choice unnecessary, and it has to be
	/// refused here rather than left to the caller's error handling: a client-owned
	/// (RPC/SDK) transaction survives a statement error and can still issue COMMIT.
	/// Cancelling on the spot would be the more direct answer but is not available
	/// — a caller may hold a cursor, and cancel waits for every cursor to drop,
	/// which that caller cannot do until the call it is blocked in returns.
	save_point_poisoned: AtomicBool,
	/// Set when a record's write stopped part-way because its query could not
	/// continue. See [`Self::poison_interrupted_write`].
	interrupted_write_poisoned: AtomicBool,
	/// Set once this transaction's commit has failed with an outcome the
	/// storage could not report, before its rollback actions run. See
	/// [`Self::commit_outcome_unknown`].
	commit_outcome_unknown: AtomicBool,
	/// Number of write slots reserved against the write-cardinality guard.
	/// Each write operation atomically reserves its slot *before* the
	/// storage call, so concurrent writes on the same transaction (e.g.
	/// graph-pointer maintenance joining several deletes) can never admit
	/// more writes than the limit through a stale read of the counter.
	/// Independent of [`TransactionMetrics`], which records successful
	/// operations for observability: a failed write keeps its reservation,
	/// and a range delete reserves one slot regardless of its (unreported)
	/// per-key expansion.
	guarded_writes: AtomicU64,
	/// Durable index-build reservations to release once this transaction is closed.
	///
	/// Writers enqueue index appendings for a durable concurrent index build after
	/// admission has reserved a ticket in a separate short transaction. Releasing
	/// those reservations after commit/cancel keeps rollback semantics correct and
	/// avoids making the user transaction delete a key that was created after its
	/// snapshot, which can conflict on snapshot-isolated local engines such as
	/// `kv-mem`.
	pending_index_build_reservations: Mutex<Vec<IndexBuildReservationRelease>>,
	/// Per-user-transaction admission reservations, keyed by `(generation, index)`.
	///
	/// One durable `!br` reservation is allocated per user transaction per index;
	/// every indexed mutation in that transaction reuses the cached ticket and
	/// allocates a fresh `mutation_seq`. Avoids paying a reservation commit for
	/// every individual mutation in a multi-row update or insert. Cleared on
	/// commit/cancel — the per-reservation release is queued separately in
	/// [`Self::pending_index_build_reservations`].
	cached_index_build_reservations:
		Mutex<HashMap<CachedIndexBuildReservationKey, CachedIndexBuildReservation>>,
	/// The admission decision of each index this transaction has written
	/// through that needed no build reservation, with the table catalog stamps
	/// ([`Self::catalog_stamps`]) it was made at.
	///
	/// A decision is reused for the rest of the user transaction while this
	/// transaction stamps no catalog version and starts or removes no build
	/// ([`Self::clear_cached_index_admissions`]). A build that starts
	/// elsewhere after the decision is covered as it is for the first write
	/// of the transaction, whose decision is just as old by the time the
	/// transaction commits: the commit conflicts with the builder's writes,
	/// and the builder cleans the index before it scans. Never held by a
	/// speculation.
	cached_index_admissions:
		parking_lot::Mutex<HashMap<CachedIndexBuildReservationKey, (u64, CachedIndexAdmission)>>,
	/// Allocators for sequences this transaction has defined and not committed.
	///
	/// The node's allocator reads definitions through transactions of its own,
	/// so it cannot see a `DEFINE SEQUENCE` this one has written. From the store
	/// alone that is indistinguishable from a definition a concurrent
	/// statement removed or replaced — one wants the caller's view, the other
	/// must not have it — so the statement records itself here, and what gets
	/// built from that view is kept here too rather than in the node's map.
	///
	/// Owning them is what keeps a definition nobody else can see from being
	/// served to anybody else: an allocator built from it has no committed
	/// definition to check its claims against, which is only sound for as long
	/// as this transaction is open. It goes when this transaction goes,
	/// whether that is a commit or a rollback, and a save point rollback puts
	/// it back as it stood when the save point opened — see
	/// [`Self::provisional_save_points`].
	///
	/// A name maps to an empty slot from the moment the statement defines it
	/// until something draws from it.
	provisional_sequences: Mutex<HashMap<(NamespaceId, DatabaseId, String), ProvisionalSequence>>,
	/// Whether [`Self::provisional_sequences`] has ever been written to.
	///
	/// Every `sequence::nextval` asks whether this transaction defined the
	/// sequence it is drawing from, and in all but a `DEFINE SEQUENCE`
	/// transaction the answer is no. This answers that without taking the
	/// lock, so the per-id path pays an atomic load rather than a mutex.
	defines_any_sequence: AtomicBool,
	/// [`Self::provisional_sequences`] as it stood when each open save point
	/// opened, innermost last.
	///
	/// A save point rollback restores the storage it scopes, which includes
	/// every row a provisional allocator wrote — but not the allocators, nor the
	/// record that this transaction defined a sequence, which live here. Left
	/// in place, a `DEFINE SEQUENCE` the rollback undid would still be served
	/// from, on a definition that does not exist; and an allocator built
	/// inside the scope would keep serving from a window whose claim row is
	/// gone, leaving what it issues unfenced for other nodes, which claim past
	/// the rows they can see. Restoring the snapshot puts both back as they
	/// stood, which is what the storage now says too, so a value drawn inside
	/// the scope is undone along with everything else written there.
	///
	/// The node's own eviction of a redefined sequence is not undone. That costs
	/// the node one rebuild of its allocator, from the definition as committed.
	provisional_save_points: Mutex<Vec<ProvisionalSnapshot>>,
	/// Effects owed once this transaction commits.
	///
	/// They are not transactional - stopping a process-local index builder, or
	/// telling a subscriber its subscription is gone - so running them before the
	/// commit would leave them standing after a rollback undid what they answer
	/// to. Drained by [`Self::commit`]; discarded by [`Self::cancel`] and by a
	/// failed commit.
	///
	/// An action queued inside a save point scope answers to writes of that scope: it is
	/// discarded when the scope rolls back, unless it declares otherwise
	/// ([`CommitAction::answers_to_scope`]), and belongs to the enclosing scope when the scope
	/// is released ([`Self::save_point_marks`]).
	commit_actions: Mutex<Vec<Box<dyn CommitAction>>>,
	/// Effects owed if this transaction does *not* commit.
	///
	/// Registered by callers that wrote durable state from a separate transaction
	/// while this one was open, so that state is provisional until this one
	/// commits. `DEFINE INDEX` is the case in point: it starts the builder while
	/// the schema transaction is still open, and the builder may commit build
	/// state and index data of its own before the schema transaction terminates.
	/// Drained by [`Self::cancel`] and by a failed commit; discarded once the
	/// commit succeeds.
	rollback_actions: Mutex<Vec<Box<dyn RollbackAction>>>,
	/// Work queued for this transaction's owner to await once it has committed.
	///
	/// `None` unless the owner promised to await it, by building the
	/// transaction [`Self::with_after_commit_waits`] or
	/// [`Self::with_materialise_waits`]: a statement that finds no queue does
	/// its work inline, or not at all. Held weakly, because the owner holds the
	/// queue (see [`AfterCommitWaits`]); discarded by [`Self::cancel`] and by a
	/// failed commit.
	after_commit_waits: Option<std::sync::Weak<AfterCommitQueue>>,
	/// Whether the owner of [`Self::after_commit_waits`] also accepts a
	/// blocking build's wait for its index to come online, rather than only the
	/// wait for that index to be materialised. See
	/// [`Self::accepts_after_commit_waits`].
	defers_build_waits: bool,
	/// Per-table memo of the settled `graph_doc_ids` marker, backing
	/// [`Self::graph_doc_ids_settled`]. The first read per table is locked
	/// (`getu`), and its conflict registration covers the rest of the
	/// transaction, so later calls — one per record in a multi-record
	/// delete — reuse the answer instead of re-issuing an uncached engine
	/// read each. Cleared by [`Self::rollback_to_save_point`], which may
	/// release the registration on rollback-scoped engines.
	graph_doc_ids_settled: Mutex<HashMap<(NamespaceId, DatabaseId, TableName), bool>>,
}

/// Whether a table's shared doc-ID space is safe for a new consumer to adopt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DocIdsReclaimClaim {
	/// No reclaim had started. Any queued entries were cancelled by this
	/// transaction, so the mappings that remain are internally consistent.
	Available,
	/// At least one reclaim page committed, or an entry of the queued set has
	/// already been retired. The surviving mappings may be torn and must not be
	/// adopted; the queued reclaim is left intact so the background task can
	/// finish it in bounded pages.
	InProgress,
}

const INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP: Duration = Duration::from_millis(100);

/// Lookup key for a per-user-transaction admission reservation.
///
/// One cache entry exists per index per user transaction; every indexed
/// mutation that hits the same index in the same transaction shares this
/// entry and uses `next_mutation_seq` to allocate its `!bg` slot. The cache
/// key omits the build generation because reuse is revalidated on every hit
/// against the current `!bs` state: a generation rotation, an `Online` or
/// `Error` transition, or a vanished build aborts the user transaction with
/// `IndexingBuildingCancelled`. Skipping that recheck would let later
/// mutations in the same transaction write `!bg` against a generation no
/// builder will replay — silent index data loss.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct CachedIndexBuildReservationKey {
	pub ns: NamespaceId,
	pub db: DatabaseId,
	pub tb: TableName,
	pub ix: IndexId,
}

/// An admission decision that took no build reservation, kept for the rest of
/// a user transaction ([`Transaction::lookup_cached_index_admission`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CachedIndexAdmission {
	/// The index is maintained by the writing transaction itself: it has no
	/// build state, or its build is online.
	IndexNormally,
	/// The index is gone from the catalog, so a write skips it.
	Retired,
}

/// Cached admission reservation reused across an entire user transaction.
///
/// First admission for an index runs the short reservation transaction
/// (CAS-incrementing the generation's `!bt` counter and committing `!br`), then stores
/// the resulting `generation`, `ticket`, `initial_complete`, and prepared
/// release here. Subsequent admissions read the cache, take a fresh
/// `mutation_seq`, and write a `!bg` keyed by `(generation, ticket, seq)`.
pub struct CachedIndexBuildReservation {
	pub generation: BuildGeneration,
	pub ticket: BuildTicket,
	pub initial_complete: bool,
	pub next_mutation_seq: BuildTicketMutationSeq,
}

/// Outcome of a per-transaction reservation lookup.
///
/// `FirstUse` is returned the first time admission runs for an index in this
/// transaction; the caller has already registered the prepared release and
/// must still run the durable-admission fence. Subsequent calls return
/// `Reused`, which only carries the ticket and the freshly allocated
/// `mutation_seq`. The shapes are intentionally identical for the fields the
/// caller consumes — the variant tag exists so admission can decide whether
/// to run the fence and emit fault-injection probes.
#[derive(Clone, Copy, Debug)]
pub enum CachedIndexBuildReservationLookup {
	FirstUse {
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
		initial_complete: bool,
	},
	Reused {
		generation: BuildGeneration,
		ticket: BuildTicket,
		mutation_seq: BuildTicketMutationSeq,
		initial_complete: bool,
	},
}

/// Close-time release for a durable index-build reservation owned by a user
/// transaction.
///
/// The release uses a fresh short transaction and a compare-delete against the
/// exact reservation value. That keeps the cleanup idempotent and prevents a
/// late release from deleting a different reservation if ownership changed. The
/// release may run after a queued `!bg` appending commits, or after a write fails
/// before any appending is written. Retryable conflicts are retried so transient
/// cleanup failures do not leave a live-node reservation blocking the index
/// build forever. Non-retryable failures are returned to the transaction close
/// path; if no committed durable appending exists for the reservation, the build
/// is marked `Error` so it cannot remain stuck in `Closing`.
#[derive(Clone)]
pub struct IndexBuildReservationRelease {
	tf: TransactionFactory,
	sequences: Sequences,
	node: Uuid,
	key: Key<'static>,
	val: Val,
}

impl IndexBuildReservationRelease {
	pub fn new(
		tf: TransactionFactory,
		sequences: Sequences,
		node: Uuid,
		key: Key<'static>,
		val: Val,
	) -> Self {
		Self {
			tf,
			sequences,
			node,
			key,
			val,
		}
	}

	async fn release_once(&self) -> Result<()> {
		// Use raw transactor methods here so the cleanup transaction does not
		// recursively run Transaction::commit/cancel and re-enter reservation
		// release handling.
		let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;

		#[cfg(any(test, feature = "test-hooks"))]
		if let Err(err) = maybe_inject_non_retryable_error(
			NonRetryableErrorSite::ConcurrentIndexReservationRelease,
			self.node,
		) {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}

		match tx.tr.delc(self.key.as_borrowed(), Some(&self.val)).await {
			Ok(()) => {}
			Err(KvsError::TransactionConditionNotMet) => {
				let _ = tx.tr.cancel().await;
				return Ok(());
			}
			Err(err) => {
				let _ = tx.tr.cancel().await;
				return Err(err.into());
			}
		}

		#[cfg(any(test, feature = "test-hooks"))]
		if let Err(err) = maybe_inject_retryable_conflict(
			RetryableConflictSite::ConcurrentIndexReservationRelease,
			self.node,
		) {
			let _ = tx.tr.cancel().await;
			return Err(err);
		}

		if let Err(err) = tx.commit_store().await {
			let _ = tx.tr.cancel().await;
			return Err(err.into());
		}
		Ok(())
	}

	async fn mark_build_error_if_uncommitted(&self, release_err: &anyhow::Error) -> Result<()> {
		let br = BuildReservationKey::decode_key(&self.key)?;
		// One reservation covers an entire user transaction's mutation
		// batch on this index. Any committed `!bg(generation, ticket, *)`
		// entry signals that at least one mutation in that batch became
		// durable, so the build does not need to be marked errored. Use the
		// inclusive scan range, not a point exists check on `mutation_seq = 0`,
		// because the first mutation may not be at index zero on retry paths
		// that allocate a fresh ticket.
		let range = BuildAppendTicketPrefix {
			ns: br.ns,
			db: br.db,
			tb: Cow::Borrowed(br.tb.as_ref()),
			ix: br.ix,
			generation: br.generation,
			ticket: br.ticket,
		}
		.range()?;

		let bs = BuildStateKey {
			ns: br.ns,
			db: br.db,
			tb: Cow::Borrowed(br.tb.as_ref()),
			ix: br.ix,
		};

		let reason = format!(
			"Failed to release durable index-build reservation for generation {} ticket {} after transaction close: {release_err}",
			br.generation, br.ticket
		);

		loop {
			let tx = self.tf.transaction(TransactionType::Write, self.sequences.clone()).await?;

			let current_reservation = match tx.tr.get(self.key.as_borrowed(), None).await {
				Ok(current) => current,
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			};
			if current_reservation.as_deref() != Some(self.val.as_slice()) {
				let _ = tx.tr.cancel().await;
				return Ok(());
			}

			match tx.tr.keys(range.clone().into_key_range(), 1, 0, None).await {
				Ok(res) if !res.keys.is_empty() => {
					let _ = tx.tr.cancel().await;
					return Ok(());
				}
				Ok(_) => {}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			}

			let current = match tx.get_key(&bs, None).await {
				Ok(Some(current_state)) => current_state,
				Ok(None) => {
					let _ = tx.tr.cancel().await;
					return Ok(());
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err);
				}
			};
			if current.generation != br.generation
				|| !matches!(current.phase, IndexBuildPhase::Building | IndexBuildPhase::Closing)
			{
				let _ = tx.tr.cancel().await;
				return Ok(());
			}

			let mut next = current;
			next.phase = IndexBuildPhase::Error;
			next.owner = None;
			next.owner_heartbeat_at = None;
			next.updated_at = Utc::now();
			next.error = Some(reason.clone());
			next.report_status = Some(IndexBuildReportStatus::Error);

			match tx.set_key(&bs, &next).await {
				Ok(()) => {}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err);
				}
			}

			match tx.commit_store().await {
				Ok(()) => return Ok(()),
				Err(err) if err.is_retryable() => {
					let _ = tx.tr.cancel().await;
					sleep(INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP).await;
				}
				Err(err) => {
					let _ = tx.tr.cancel().await;
					return Err(err.into());
				}
			}
		}
	}

	pub async fn release(self) -> Result<()> {
		loop {
			match self.release_once().await {
				Ok(()) => return Ok(()),
				Err(err) if is_retryable_transaction_conflict(&err) => {
					tracing::debug!(
						target: "surrealdb::core::kvs::tx",
						node = %self.node,
						error = %err,
						"retryable conflict while releasing durable index-build reservation, retrying"
					);
					sleep(INDEX_BUILD_RESERVATION_RELEASE_RETRY_SLEEP).await;
				}
				Err(err) => {
					tracing::warn!(
						target: "surrealdb::core::kvs::tx",
						node = %self.node,
						"failed to release durable index-build reservation: {err}"
					);
					if let Err(mark_err) = self.mark_build_error_if_uncommitted(&err).await {
						tracing::warn!(
							target: "surrealdb::core::kvs::tx",
							node = %self.node,
							"failed to mark durable index build error after reservation release failure: {mark_err}"
						);
					}
					return Err(err);
				}
			}
		}
	}

	/// Delete every queued `!br` reservation in a single short transaction.
	///
	/// A typical user transaction touches a handful of indexes and produces one
	/// reservation per index. Folding the deletes into one commit removes the
	/// `O(reservations)` extra fsync that the per-reservation path would charge
	/// — the per-mutation cost is already amortized by the per-user-txn
	/// reservation cache, and this drops the close-time cost from one commit
	/// per reservation to exactly one commit per user transaction.
	///
	/// Returns `Ok(())` on success. On any failure the original reservations
	/// are returned via the `Err` arm so the caller can run the slow path
	/// (`release()`), which retries retryable conflicts and marks the build
	/// errored if a non-retryable failure leaves an undeleted `!br` behind.
	async fn release_batch(reservations: Vec<Self>) -> Result<(), Vec<Self>> {
		// One reservation: batching has nothing to amortize, and the per-call
		// path already does everything we need including retry+mark-error.
		if reservations.len() <= 1 {
			return Err(reservations);
		}
		let Some(first) = reservations.first() else {
			return Ok(());
		};
		let tf = first.tf.clone();
		let sequences = first.sequences.clone();
		let tx = match tf.transaction(TransactionType::Write, sequences).await {
			Ok(tx) => tx,
			Err(_) => return Err(reservations),
		};
		for reservation in &reservations {
			match tx.tr.delc(reservation.key.as_borrowed(), Some(&reservation.val)).await {
				Ok(()) => {}
				Err(KvsError::TransactionConditionNotMet) => {
					// The builder already cleaned this reservation up; that
					// is part of the normal release contract, not a failure.
				}
				Err(_) => {
					let _ = tx.tr.cancel().await;
					return Err(reservations);
				}
			}
		}
		match tx.commit_store().await {
			Ok(()) => Ok(()),
			Err(_) => {
				let _ = tx.tr.cancel().await;
				Err(reservations)
			}
		}
	}
}

impl Drop for Transaction {
	fn drop(&mut self) {
		if self.live_bucket != crate::triggers::NO_LIVE_BUCKET {
			self.triggers.live.close(self.live_bucket);
		}
	}
}

impl Deref for Transaction {
	type Target = Transactor;

	fn deref(&self) -> &Self::Target {
		&self.tr
	}
}

/// Caller-owned keys-only scan cursor that records scan metrics into the
/// parent transaction as it pumps batches.
///
/// Returned by [`Transaction::open_keys_cursor`]. Holds a borrow into the
/// parent transaction's metrics counter, so it cannot outlive the
/// transaction at the type level. Each call to `next_batch` advances the
/// underlying RocksDB-or-default cursor and records the batch's keys/bytes
/// against the transaction's scan metrics.
pub struct MeteredKeysCursor<'a> {
	/// The underlying backend-provided cursor (RocksDB-specialised, or the
	/// default impl that wraps single-shot `keys`/`keysr`).
	inner: Box<dyn ScanCursorKeys + 'a>,
	/// Borrow into the parent transaction's metrics counter; updated on
	/// each batch.
	metrics: &'a TransactionMetrics,
}

impl<'a> MeteredKeysCursor<'a> {
	/// Advance the cursor and return up to `limit` keys borrowed from the
	/// cursor's internal buffer. An empty batch signals end of range.
	///
	/// The returned `KeysBatch` borrows from the cursor; the borrow
	/// checker forbids calling `next_batch` again while the previous
	/// batch is still in scope.
	pub async fn next_batch<'s>(&'s mut self, limit: u32) -> Result<KeysBatch<'s>> {
		let batch = self.inner.next_batch(limit).await?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, 0);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per key borrowed directly from the
	/// cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn KeyVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, 0);
		Ok(stats)
	}
}

/// Caller-owned key+value scan cursor with metrics recording. See
/// [`MeteredKeysCursor`] for the rationale.
pub struct MeteredValsCursor<'a> {
	/// The underlying backend-provided cursor.
	inner: Box<dyn ScanCursorVals + 'a>,
	/// Borrow into the parent transaction's metrics counter.
	metrics: &'a TransactionMetrics,
}

impl<'a> MeteredValsCursor<'a> {
	/// Advance the cursor and return up to `limit` `(key, value)` pairs
	/// borrowed from the cursor's internal buffer.
	pub async fn next_batch<'s>(&'s mut self, limit: u32) -> Result<ValsBatch<'s>> {
		let batch = self.inner.next_batch(limit).await?;
		self.metrics.record_scan(batch.len() as u64, batch.key_bytes, batch.value_bytes);
		Ok(batch)
	}

	/// Drive the cursor, invoking `f` per `(key, value)` borrowed directly from
	/// the cursor (zero-copy on backends that override `for_each`). Records this
	/// chunk's keys/bytes against the transaction's scan metrics in a single
	/// `record_scan` call, matching `next_batch`'s metric granularity.
	pub async fn for_each(&mut self, limit: u32, f: &mut dyn ValVisitor) -> Result<ScanChunkStats> {
		let stats = self.inner.for_each(limit, f).await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(stats)
	}
}

/// Database-wide summary of which tables any `REFERENCE` field can target,
/// memoized per transaction for the DELETE reference-purge gate
/// ([`Transaction::table_may_have_incoming_references`]).
struct ReferenceTargets {
	/// Some reference field can hold a record of *any* table (an untyped
	/// `record`), so every table must be treated as potentially referenced.
	any: bool,
	/// The concrete set of tables that typed reference fields can target.
	tables: HashSet<TableName>,
}

impl ReferenceTargets {
	fn can_target(&self, table: &TableName) -> bool {
		self.any || self.tables.contains(table)
	}
}

impl Transaction {
	/// Route a request path to the one `DEFINE API` that handles it, compiling
	/// only that definition.
	///
	/// Routing is decidable on the stored form (see
	/// [`catalog::StoredApiDefinition::route`]), so this reads the definitions
	/// as stored, picks the most specific match, and compiles just that one.
	/// Reading the compiled list instead would parse every handler body,
	/// fallback and permission clause in the database on every request, of
	/// which at most one is executed — and the caller opens a fresh
	/// transaction per request, so the compiled cache is always cold.
	///
	/// Nothing stored escapes: the return is a compiled definition and the
	/// matched path parameters.
	pub async fn find_db_api(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		segments: &[&str],
		method: catalog::ApiMethod,
	) -> Result<Option<(catalog::ApiDefinition, surrealdb_expr::val::Object)>> {
		let range = ApiPrefix {
			ns,
			db,
		}
		.range()?;
		let val = self.tr.getr(range.into_key_range(), None).await?;
		let stored: Arc<[catalog::StoredApiDefinition]> =
			util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;

		let mut best: Option<(&catalog::StoredApiDefinition, surrealdb_expr::val::Object, u8)> =
			None;
		for api in stored.iter() {
			let Some((params, specificity)) = api.route(segments, method)? else {
				continue;
			};
			if best.as_ref().is_none_or(|(_, _, s)| specificity > *s) {
				best = Some((api, params, specificity));
			}
		}

		let Some((api, params, _)) = best else {
			return Ok(None);
		};
		Ok(Some((catalog::ApiDefinition::from_stored(api)?, params)))
	}

	/// Returns `true` if any `DEFINE FIELD ... REFERENCE` in this database could
	/// target a record in `table` (so a record in `table` may have incoming
	/// reference keys).
	///
	/// A reference key is only ever written under a record's range while some
	/// reference field can hold a record id of that record's table, so when no
	/// reference field can target `table` there can be no reference keys for any
	/// record in it. The DELETE purge path uses this to skip the per-record
	/// reference range scan entirely in that case — on a distributed backend
	/// that scan is a read round-trip per deleted record.
	///
	/// The per-database answer is memoized for the transaction, so a batch
	/// delete computes it at most once regardless of how many records or tables
	/// it touches. It is conservative: any reference field whose kind is not
	/// provably unable to hold a record of `table` keeps the scan, so a record
	/// that genuinely needs its references cleaned is never skipped.
	pub async fn table_may_have_incoming_references(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		table: &TableName,
	) -> Result<bool> {
		Ok(self.database_reference_targets(ns, db).await?.can_target(table))
	}

	/// Compute, or fetch the memoized, [`ReferenceTargets`] summary for a
	/// database. Invalidated alongside field definitions (see `put_tb_field`
	/// and the `clear_cache` on ALTER/REMOVE FIELD).
	async fn database_reference_targets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<ReferenceTargets>> {
		let qey = cache::tx::Lookup::DbReferenceTargets(ns, db);
		if let Some(entry) = self.cache.get(&qey) {
			return entry.try_into_type::<ReferenceTargets>();
		}
		let mut any = false;
		let mut tables = HashSet::new();
		// Walk the stored catalog. Whether a field carries a `REFERENCE` is
		// decidable on the stored form, and almost no field does, so only the
		// `TYPE` text of an actual reference field is compiled.
		//
		// Compiling every definition instead would answer the same boolean while
		// widening the blast radius: this runs from the DELETE purge gate for
		// every record deleted, over every table in the database, and
		// `FieldDefinition::from_stored` propagates on any clause it cannot
		// parse. One unreadable `VALUE` on an unrelated table would then fail
		// every delete in the database. Nothing leaves this function but a
		// boolean and a set of names.
		//
		// Entries decode one by one so that an entry a newer release wrote, and
		// this build refuses, can be told from a corrupt one. A refused table or
		// field may be a reference field this build cannot see, so the answer
		// becomes the conservative one: any table may be referenced, and the
		// purge scans.
		let mut refused = false;
		let table_range = TablePrefix {
			ns,
			db,
		}
		.range()?;
		let table_rows = self.tr.getr(table_range.into_key_range(), None).await?;
		'walk: for (_, raw_table) in table_rows.values.iter() {
			let Some(tb) = Self::decode_or_refused::<StoredTableDefinition>(raw_table)? else {
				refused = true;
				break 'walk;
			};
			let tb_name = TableName::from(tb.name.clone());
			let field_range = FieldPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&tb_name),
			}
			.range()?;
			let field_rows = self.tr.getr(field_range.into_key_range(), None).await?;
			for (_, raw_field) in field_rows.values.iter() {
				let Some(fd) =
					Self::decode_or_refused::<catalog::StoredFieldDefinition>(raw_field)?
				else {
					refused = true;
					break 'walk;
				};
				// Only reference fields write reference keys.
				if fd.reference.is_none() {
					continue;
				}
				match &fd.field_kind {
					Some(kind) => {
						if kind.compile()?.collect_reference_target_tables(&mut tables) {
							any = true;
						}
					}
					// A reference field always has a record-like kind (enforced
					// at DEFINE FIELD time); treat a missing kind as able to
					// target anything, erring towards running the scan.
					None => any = true,
				}
			}
		}
		if refused {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"A table or field definition needs a newer release than this build, so \
				 every table is treated as possibly referenced and deletes scan for references"
			);
			any = true;
		}
		let targets = Arc::new(ReferenceTargets {
			any,
			tables,
		});
		self.cache.insert(qey, cache::tx::Entry::Any(targets.clone()));
		Ok(targets)
	}

	/// Decodes one catalog entry, or `None` when a newer release wrote it and this
	/// build refuses it ([`EntryError::NeedsRelease`]). Any other failure is an
	/// error.
	fn decode_or_refused<V>(raw: &[u8]) -> Result<Option<V>>
	where
		V: KVValue<KeyContext = ()>,
	{
		match V::kv_decode_value(raw, ()) {
			Ok(value) => Ok(Some(value)),
			Err(e) if refused_by_release(&e) => Ok(None),
			Err(e) => Err(e),
		}
	}

	/// The table's settled `graph_doc_ids` marker — whether numeric
	/// adjacency blocks consume its shared doc-ID space — read through a
	/// locked (`getu`) read of the table-definition key. `false` for a
	/// table with no definition.
	///
	/// The locked read is what makes the answer settled: a plain snapshot
	/// read registers no commit-time conflict on backends that validate
	/// only what a transaction wrote, so a transaction that skips doc-ID
	/// maintenance because it observed the marker unset could commit that
	/// decision after a concurrent fold set the marker (see
	/// `Document::remove_doc_id`). Conflict registration is per key and
	/// validated once at commit, so one locked read covers the whole
	/// transaction: the answer is memoized, and a multi-record delete pays
	/// one engine read per table instead of one per record.
	///
	/// Whether the registration survives a savepoint rollback is
	/// engine-defined (see `Transactable::getu`), so
	/// [`Self::rollback_to_save_point`] clears the memo and the next call
	/// re-issues the locked read.
	pub async fn graph_doc_ids_settled(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		if let Some(&settled) = self.graph_doc_ids_settled.lock().await.get(&(ns, db, tb.clone())) {
			return Ok(settled);
		}
		let key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let settled = self.getu_key(&key).await?.is_some_and(|stored| stored.graph_doc_ids);
		self.graph_doc_ids_settled.lock().await.insert((ns, db, tb.clone()), settled);
		Ok(settled)
	}

	/// Create a new transaction.
	///
	/// `observer` is dispatched to on commit/cancel; pass
	/// `Arc::new(NoopObserver)` when no observer is configured. `write`
	/// should match the `TransactionType` used to open the underlying
	/// transactor so the emitted event carries the correct attribute.
	///
	/// `catalog_epoch` is the count of the datastore's catalog commits
	/// finished, with none under way, read before the underlying transaction
	/// opened, where the datastore is the store's sole writer, and
	/// [`NO_CATALOG_EPOCH`] otherwise ([`Self::catalog_epoch`]).
	///
	/// `sole_writer` is whether the engine the transaction opened on admits no other writer of the
	/// store ([`Self::sole_writer`]).
	///
	/// `feature` is the store's feature level cell, shared by every transaction of
	/// the store.
	#[expect(clippy::too_many_arguments, reason = "each argument is a distinct shared handle")]
	pub fn new(
		local: bool,
		sequences: Sequences,
		triggers: Arc<CommitTriggers>,
		observer: Arc<dyn ExecutionObserver>,
		tr: Transactor,
		config: &TransactionConfig,
		catalog_epoch: u64,
		sole_writer: bool,
		feature: Arc<FeatureLevelCell>,
	) -> Transaction {
		Transaction {
			local,
			started_at: Instant::now(),
			observer,
			metrics: TransactionMetrics::new(),
			tenant_identity: OnceLock::new(),
			tr,
			cache: TransactionCache::new(config.transaction_cache_size),
			speculative_caches: Mutex::new(Vec::new()),
			sequences,
			feature,
			sole_writer,
			layout_memo: Default::default(),
			conformance_memo: Default::default(),
			held_spans: Default::default(),
			births: Default::default(),
			layout_epoch: AtomicU64::new(0),
			range_deletes: AtomicU64::new(0),
			stored_format_admitted: AtomicBool::new(false),
			stored_format_absent: AtomicBool::new(false),
			catalog_write_poisoned: AtomicBool::new(false),
			failed_statement_poisoned: AtomicBool::new(false),
			save_point_marks: Mutex::new(Vec::new()),
			catalog_stamps: AtomicU64::new(0),
			catalog_touched: AtomicBool::new(false),
			object_stamps: AtomicU64::new(0),
			stamped_tables: Default::default(),
			creations: Default::default(),
			shaped_indexes: Default::default(),
			catalog_epoch,
			state_changes: Arc::new(AtomicU64::new(0)),
			catalog_reads: Default::default(),
			changefeed: OnceLock::new(),
			live_events: OnceLock::new(),
			index_deltas: OnceLock::new(),
			triggers,
			trigger_async_event: AtomicBool::new(false),
			trigger_index_compaction: AtomicBool::new(false),
			trigger_graph_fold: AtomicBool::new(false),
			trigger_table_migration: AtomicBool::new(false),
			live_bucket: crate::triggers::NO_LIVE_BUCKET,
			write_keys_limit: OnceLock::new(),
			write_guard_poisoned: AtomicBool::new(false),
			save_point_poisoned: AtomicBool::new(false),
			interrupted_write_poisoned: AtomicBool::new(false),
			commit_outcome_unknown: AtomicBool::new(false),
			guarded_writes: AtomicU64::new(0),
			pending_index_build_reservations: Mutex::new(Vec::new()),
			cached_index_build_reservations: Mutex::new(HashMap::new()),
			cached_index_admissions: Default::default(),
			provisional_sequences: Mutex::new(HashMap::new()),
			defines_any_sequence: AtomicBool::new(false),
			provisional_save_points: Mutex::new(Vec::new()),
			commit_actions: Mutex::new(Vec::new()),
			rollback_actions: Mutex::new(Vec::new()),
			after_commit_waits: None,
			defers_build_waits: false,
			graph_doc_ids_settled: Mutex::new(HashMap::new()),
		}
	}

	/// Arms the write-cardinality guard: once the transaction has buffered
	/// `limit` individual key writes, every further write fails with
	/// [`crate::error::DatastoreError::TransactionWriteKeysExceeded`], so a statement's
	/// physical fan-out (cascaded deletes, index maintenance, graph-edge
	/// cleanup) stops accumulating at the bound and the transaction rolls
	/// back atomically. `None` leaves the transaction unbounded.
	///
	/// Armed on every statement-execution path (executor transactions,
	/// externally-supplied client-owned transactions, record-access clause
	/// evaluation); internal maintenance transactions (index builds,
	/// compaction, garbage collection) are created without a limit and are
	/// never guarded. Every write reserves one slot atomically before it is
	/// issued, so concurrent writes within the transaction can never admit
	/// more than the limit. The accounting rules (range deletes, commit-time
	/// feed writes, reservations never being refunded) are documented on
	/// `TransactionConfig::transaction_max_write_keys`.
	pub fn with_write_keys_limit(self, limit: Option<NonZeroU64>) -> Self {
		self.arm_write_keys_limit(limit);
		self
	}

	/// Arms the write-cardinality guard on a transaction that is already
	/// wrapped in an `Arc` — the externally-supplied (client-owned)
	/// transactions that statements execute on when one is set on a
	/// `Datastore::run` request.
	/// Same contract as [`Self::with_write_keys_limit`]. Idempotent: the
	/// first arming wins and later calls are silently ignored, so repeated
	/// statement executions on one transaction keep a single limit.
	pub fn arm_write_keys_limit(&self, limit: Option<NonZeroU64>) {
		if let Some(limit) = limit {
			let _ = self.write_keys_limit.set(limit);
		}
	}

	/// Reserves one write slot against the write-cardinality guard, failing
	/// when the transaction has already reserved the configured maximum.
	/// Called before every write operation; the reservation is atomic
	/// (compare-and-increment), so writes issued concurrently on the same
	/// transaction each take a distinct slot and the limit holds under any
	/// interleaving.
	///
	/// The write is counted in [`Self::state_changes`] when the answered
	/// guard is dropped, which the caller holds until the write has taken
	/// effect.
	fn reserve_write_slot(&self) -> Result<StateChange<'_>> {
		let changed = self.state_change();
		// A speculation's writes never reach the storage, so they spend none
		// of the statement's write allowance.
		if self.tr.speculating() {
			return Ok(changed);
		}
		if let Some(limit) = self.write_keys_limit.get()
			&& self
				.guarded_writes
				.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
					(n < limit.get()).then_some(n + 1)
				})
				.is_err()
		{
			// Poison the transaction: the writes already buffered are a
			// partial statement, so a later explicit COMMIT must be refused
			// (see the check at the top of [`Self::commit`]).
			self.write_guard_poisoned.store(true, Ordering::Relaxed);
			return Err(crate::error::DatastoreError::TransactionWriteKeysExceeded {
				limit: limit.get(),
			}
			.into());
		}
		Ok(changed)
	}

	/// Attach pre-resolved tenant identity so the emitted
	/// [`TransactionEvent`] carries the active session's namespace,
	/// database, user, session id, and client IP. Typically called by the
	/// [`crate::Datastore`] entry points that create a transaction
	/// against an authenticated session, before the transaction is enclosed
	/// in an `Arc`. Idempotent: subsequent calls are silently ignored.
	pub fn with_tenant_identity(self, identity: Option<Arc<TenantIdentity>>) -> Self {
		if let Some(id) = identity {
			let _ = self.tenant_identity.set(id);
		}
		self
	}

	/// Attach pre-resolved tenant identity to a transaction that is already
	/// wrapped in an `Arc`. Idempotent: subsequent calls are silently
	/// ignored.
	pub fn set_tenant_identity(&self, identity: Arc<TenantIdentity>) {
		let _ = self.tenant_identity.set(identity);
	}

	/// The counters this transaction has accumulated so far. Only the
	/// commit/cancel event carries them in a normal build; tests read them
	/// mid-transaction to assert what a code path counted.
	#[cfg(any(test, feature = "test-hooks"))]
	#[doc(hidden)]
	pub fn metrics_snapshot_for_test(&self) -> surrealdb_observe::TransactionMetricsSnapshot {
		self.metrics.snapshot()
	}

	/// Emit a [`TransactionEvent`] carrying the current counter snapshot and
	/// elapsed lifetime. Invoked from [`Self::commit`] and [`Self::cancel`].
	///
	/// Short-circuits when the installed observer is a no-op so a
	/// process running with no observers attached pays exactly nothing
	/// per commit/cancel beyond the early-return. The `metrics`
	/// snapshot, the event allocation, and the trait-object dispatch
	/// are all skipped in that case.
	fn emit_transaction_event(&self, outcome: Outcome) {
		self.emit_transaction_event_with_class(outcome, None);
	}

	/// Variant of [`Self::emit_transaction_event`] that records a bounded
	/// `error_class` on the resulting [`TransactionEvent`]. Use the
	/// canonical strings published by the server's `error_class` module
	/// (e.g. `txn_conflict`, `storage`, `internal`) so cardinality stays
	/// closed. Pass `None` for non-error outcomes.
	fn emit_transaction_event_with_class(
		&self,
		outcome: Outcome,
		error_class: Option<&'static str>,
	) {
		if self.observer.is_noop() {
			return;
		}
		self.observer.on_transaction_complete(&TransactionEvent {
			safe: TransactionEventSafe {
				outcome,
				write: self.tr.writeable(),
				duration: self.started_at.elapsed(),
				metrics: self.metrics.snapshot(),
				error_class,
			},
			ctx: self.tenant_identity.get().map(|t| t.to_transaction_ctx()).unwrap_or_default(),
		});
	}

	/// Defer release of a durable index-build reservation until close.
	///
	/// Admission commits the reservation before the user transaction writes the
	/// queued appending. Registering the prepared release immediately gives every
	/// admitted ticket a cleanup path even if fence or queue work fails. Releasing
	/// from a fresh transaction after commit/cancel keeps rollbacks from undoing
	/// the release and avoids snapshot conflicts on local engines.
	pub async fn register_index_build_reservation_release(
		&self,
		release: IndexBuildReservationRelease,
	) {
		self.pending_index_build_reservations.lock().await.push(release);
	}

	/// Look up an admission reservation cached for this user transaction.
	///
	/// Returns `Ok(Some(_))` when this transaction has already reserved a ticket
	/// for the same index earlier in its lifetime. On a hit the caller receives
	/// the cached generation and ticket plus a fresh `mutation_seq`, and `!bg`
	/// can be written without committing a new reservation transaction.
	///
	/// Returns `Ok(None)` on a miss; the caller is expected to run the short
	/// reservation transaction (`reserve_durable_admission`) and then publish
	/// the result via [`Self::insert_cached_index_build_reservation`].
	///
	/// Returns `Err(IndexingBuildingCancelled)` if `next_mutation_seq` would
	/// overflow `u32::MAX`. This caps a single user transaction at
	/// `u32::MAX` mutations on one index — any more would silently collide on
	/// the same `!bg(generation, ticket, MAX)` key, which is data loss.
	///
	/// Calls on a single `Transaction` are sequential in this codebase, so the
	/// lookup-then-allocate sequence has no observable race window.
	pub async fn lookup_cached_index_build_reservation(
		&self,
		key: &CachedIndexBuildReservationKey,
	) -> Result<Option<CachedIndexBuildReservationLookup>> {
		let mut cache = self.cached_index_build_reservations.lock().await;
		let Some(entry) = cache.get_mut(key) else {
			return Ok(None);
		};
		let mutation_seq = entry.next_mutation_seq;
		let next_seq = mutation_seq.checked_add(1).ok_or_else(|| {
			DatastoreError::IndexingBuildingCancelled {
				reason: "Per-user-transaction index build mutation sequence overflowed u32::MAX"
					.to_string(),
			}
		})?;
		entry.next_mutation_seq = next_seq;
		Ok(Some(CachedIndexBuildReservationLookup::Reused {
			generation: entry.generation,
			ticket: entry.ticket,
			mutation_seq,
			initial_complete: entry.initial_complete,
		}))
	}

	/// Test-only helper: seed the per-user-transaction reservation cache with
	/// a specific `next_mutation_seq`. Used by overflow regression tests so
	/// the failure mode can be exercised without running `u32::MAX` lookups.
	#[cfg(feature = "test-hooks")]
	#[doc(hidden)]
	pub async fn seed_cached_index_build_reservation_for_test(
		&self,
		key: CachedIndexBuildReservationKey,
		generation: BuildGeneration,
		ticket: BuildTicket,
		initial_complete: bool,
		next_mutation_seq: BuildTicketMutationSeq,
	) {
		self.cached_index_build_reservations.lock().await.insert(
			key,
			CachedIndexBuildReservation {
				generation,
				ticket,
				initial_complete,
				next_mutation_seq,
			},
		);
	}

	/// The admission decision cached for `key` ([`CachedIndexAdmission`]), if
	/// this transaction has stamped no catalog version since it was stored.
	///
	/// `None` on a speculation.
	pub fn lookup_cached_index_admission(
		&self,
		key: &CachedIndexBuildReservationKey,
	) -> Option<CachedIndexAdmission> {
		if self.tr.speculating() {
			return None;
		}
		let stamps = self.catalog_stamps();
		self.cached_index_admissions
			.lock()
			.get(key)
			.and_then(|(at, decision)| (*at == stamps).then_some(*decision))
	}

	/// Keep `decision` for `key` at the current catalog stamps. Does nothing
	/// on a speculation.
	pub fn insert_cached_index_admission(
		&self,
		key: CachedIndexBuildReservationKey,
		decision: CachedIndexAdmission,
	) {
		if self.tr.speculating() {
			return;
		}
		self.cached_index_admissions.lock().insert(key, (self.catalog_stamps(), decision));
	}

	/// Forget every cached admission decision. Called where this transaction
	/// starts or removes an index build, which changes what a write must
	/// decide without stamping a catalog version.
	pub fn clear_cached_index_admissions(&self) {
		self.cached_index_admissions.lock().clear();
	}

	/// Remove a per-user-transaction admission reservation from the cache.
	///
	/// Called by `consume()` when the first-use fence returns `IndexNormally`:
	/// the cached ticket has already been released by that fence, so subsequent
	/// mutations must re-enter the reservation path and rediscover the online
	/// build phase. Without this, the next mutation would hit a stale cache
	/// entry and write `!bg(old_gen, *)` that no builder will replay.
	pub async fn remove_cached_index_build_reservation(
		&self,
		key: &CachedIndexBuildReservationKey,
	) {
		self.cached_index_build_reservations.lock().await.remove(key);
	}

	/// Publish a freshly allocated admission reservation into the per-user-txn
	/// cache and return the first-use slot.
	///
	/// The returned `mutation_seq` is always `0` (the first slot for the new
	/// ticket); the cache is advanced so the next call to
	/// [`Self::lookup_cached_index_build_reservation`] returns `mutation_seq = 1`.
	/// The caller still owns registering the release with
	/// [`Self::register_index_build_reservation_release`] and running the
	/// durable-admission fence — only first-use callers should do so.
	pub async fn insert_cached_index_build_reservation(
		&self,
		key: CachedIndexBuildReservationKey,
		generation: BuildGeneration,
		ticket: BuildTicket,
		initial_complete: bool,
	) -> CachedIndexBuildReservationLookup {
		let mut cache = self.cached_index_build_reservations.lock().await;
		cache.insert(
			key,
			CachedIndexBuildReservation {
				generation,
				ticket,
				initial_complete,
				next_mutation_seq: 1,
			},
		);
		CachedIndexBuildReservationLookup::FirstUse {
			generation,
			ticket,
			mutation_seq: 0,
			initial_complete,
		}
	}

	/// Whether this transaction may write; `false` for one opened as
	/// [`TransactionType::Read`].
	pub fn writeable(&self) -> bool {
		self.tr.writeable()
	}

	/// Whether a locked read ([`Self::getu_key`]) conflicts only with writes,
	/// never with another transaction's locked read of the same key.
	///
	/// See [`surrealdb_kvs::api::Transactable::shared_locked_reads`].
	pub fn shared_locked_reads(&self) -> bool {
		self.tr.shared_locked_reads()
	}

	/// Queue work to run once this transaction commits.
	///
	/// See [`Self::commit_actions`] for why an effect waits.
	pub async fn on_commit(&self, action: Box<dyn CommitAction>) {
		self.commit_actions.lock().await.push(action);
	}

	/// Accept work that statements leave for the owner to await after commit,
	/// a blocking build's wait for its index to come online included.
	///
	/// An owner that builds its transaction this way takes on awaiting whatever
	/// is queued with [`Self::wait_after_commit`] — taken from `waits` once
	/// [`Self::commit`] has succeeded — and reporting its outcome as the
	/// statement's. Only a transaction holding nothing but one blocking index
	/// statement offers this: a build awaited after the commit may still fail,
	/// and the definition it takes back must be all the transaction wrote.
	pub fn with_after_commit_waits(mut self, waits: &AfterCommitWaits) -> Self {
		self.after_commit_waits = Some(waits.downgrade());
		self.defers_build_waits = true;
		self
	}

	/// Accept, for the owner to await after commit, only a blocking index
	/// statement's wait for its index to be materialised: its build is awaited
	/// inside the transaction, so a build that fails rolls the whole
	/// transaction back.
	///
	/// The owner takes on awaiting what is queued, as for
	/// [`Self::with_after_commit_waits`], and reporting each wait's outcome as
	/// the result of the statement that queued it.
	pub fn with_materialise_waits(mut self, waits: &AfterCommitWaits) -> Self {
		self.after_commit_waits = Some(waits.downgrade());
		self.defers_build_waits = false;
		self
	}

	/// Whether this transaction's owner awaits a blocking build after it
	/// commits, rather than the statement awaiting it inside the transaction
	/// (see [`Self::with_after_commit_waits`]).
	pub fn accepts_after_commit_waits(&self) -> bool {
		self.defers_build_waits && self.accepts_materialise_waits()
	}

	/// Whether this transaction's owner awaits, after it commits, a blocking
	/// index statement's wait for its index to be materialised. True for every
	/// transaction that [`Self::accepts_after_commit_waits`], and for one built
	/// [`Self::with_materialise_waits`].
	pub fn accepts_materialise_waits(&self) -> bool {
		self.after_commit_waits.as_ref().is_some_and(|waits| waits.strong_count() > 0)
	}

	/// Queue work for the owner to await once this transaction has committed.
	///
	/// Hands the work back when the owner made no such promise, or has since
	/// gone (see [`Self::accepts_after_commit_waits`]), so the caller does it
	/// inline.
	pub async fn wait_after_commit(
		&self,
		wait: Box<dyn AfterCommitWait>,
	) -> Option<Box<dyn AfterCommitWait>> {
		match self.after_commit_waits.as_ref().and_then(std::sync::Weak::upgrade) {
			Some(waits) => {
				waits.lock().await.push(wait);
				None
			}
			None => Some(wait),
		}
	}

	/// Queue work to run if this transaction does not commit.
	///
	/// See [`Self::rollback_actions`] for what belongs here.
	pub async fn on_rollback(&self, action: Box<dyn RollbackAction>) {
		self.rollback_actions.lock().await.push(action);
	}

	/// Records that this transaction has defined `sq`, so a draw from it is
	/// served by this transaction rather than from the node's allocator map.
	/// See [`Self::provisional_sequences`].
	pub async fn sequence_defined_here(&self, ns: NamespaceId, db: DatabaseId, sq: String) {
		self.provisional_sequences.lock().await.insert((ns, db, sq), Arc::new(Mutex::new(None)));
		self.defines_any_sequence.store(true, Ordering::Release);
	}

	/// Captures [`Self::provisional_sequences`] for a save point opening now.
	async fn snapshot_provisional_sequences(&self) -> ProvisionalSnapshot {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return Vec::new();
		}
		let held = self.provisional_sequences.lock().await;
		let mut snapshot = Vec::with_capacity(held.len());
		for (key, slot) in held.iter() {
			snapshot.push((key.clone(), slot.lock().await.clone()));
		}
		snapshot
	}

	/// Puts [`Self::provisional_sequences`] back as `snapshot` captured it.
	async fn restore_provisional_sequences(&self, snapshot: ProvisionalSnapshot) {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return;
		}
		*self.provisional_sequences.lock().await = snapshot
			.into_iter()
			.map(|(key, allocator)| (key, Arc::new(Mutex::new(allocator))))
			.collect();
	}

	/// This transaction's slot for `sq`, if this is the transaction that
	/// defined it. See [`ProvisionalSequence`].
	pub(crate) async fn provisional_sequence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &str,
	) -> Option<ProvisionalSequence> {
		if !self.defines_any_sequence.load(Ordering::Acquire) {
			return None;
		}
		self.provisional_sequences.lock().await.get(&(ns, db, sq.to_owned())).cloned()
	}

	/// The sequence allocators this transaction was opened with.
	///
	/// Exposed so a close-time action can open its own transaction from the same
	/// factory: a maintenance transaction needs the same allocators as the one
	/// that registered it.
	pub fn sequences(&self) -> Sequences {
		self.sequences.clone()
	}

	/// Opens a read-only transaction on the same datastore.
	///
	/// The new transaction sees the datastore as committed when it opens, and
	/// none of this transaction's uncommitted writes. An engine that runs one
	/// transaction's operations one at a time serves several of these
	/// concurrently, so work that reads committed state on many tasks can read
	/// it through one each, where reads through a single shared transaction
	/// would queue. The caller cancels it when done.
	pub async fn open_read_snapshot(&self) -> Result<Transaction> {
		self.sequences
			.transaction_factory()
			.transaction(TransactionType::Read, self.sequences.clone())
			.await
	}

	/// Check if the transaction is local or remote
	pub fn is_local(&self) -> bool {
		self.local
	}

	/// Enclose this transaction in an [`Arc`]
	pub fn enclose(self) -> Arc<Transaction> {
		Arc::new(self)
	}

	/// Check if the transaction is finished.
	///
	/// If the transaction has been cancelled or committed,
	/// then this function will return [`true`], and any further
	/// calls to functions on this transaction will result
	/// in a [`surrealdb_kvs::Error::TransactionFinished`] error.
	pub fn closed(&self) -> bool {
		self.tr.closed()
	}

	/// Cancel a transaction.
	///
	/// This reverses all changes made within the transaction.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn cancel(&self) -> Result<()> {
		self.cancel_sealed().await
	}

	#[inline(never)]
	fn cancel_sealed(&self) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.cancel_body())
	}

	async fn cancel_body(&self) -> Result<()> {
		// Clear any buffered changefeed entries
		if let Some(changefeed) = self.changefeed.get() {
			changefeed.clear();
		}
		// Clear any buffered live-query events
		if let Some(live_events) = self.live_events.get() {
			live_events.clear();
		}
		// Discard any buffered index deltas
		if let Some(index_deltas) = self.index_deltas.get() {
			index_deltas.clear();
		}
		// Cancel the underlying transactor. Emit a transaction event on
		// either outcome so counters and durations are always reported
		// even when cancel itself reports a driver-level error.
		//
		// A successful rollback reports [`Outcome::Cancelled`], not
		// `Outcome::Success`: the `outcome` attribute is what separates work
		// that was made durable from work that was discarded, and a commit is
		// the only thing that earns `Success`. `Outcome::Error` stays reserved
		// for a cancel the driver itself failed to perform, which leaves the
		// transaction's durability unknown rather than known-discarded.
		let result = self.tr.cancel().await;
		let cleanup_result = self.run_rollback_actions().await;
		let release_result = self.release_index_build_reservations().await;
		self.discard_commit_actions().await;
		self.emit_transaction_event(match &result {
			Ok(_) => Outcome::Cancelled,
			Err(_) => Outcome::Error,
		});
		result?;
		cleanup_result?;
		release_result?;
		Ok(())
	}

	/// Commit without draining the registered close-time actions.
	///
	/// For the maintenance transactions those actions open themselves: such a
	/// transaction registers nothing, and draining would re-enter the path that
	/// opened it. It emits no transaction event either, so an action's own writes
	/// are not counted as a transaction of the caller's.
	///
	/// It flushes no buffered index deltas either, and refuses rather than drop any
	/// it finds. A transaction that maintained an index has a description of those
	/// writes waiting for [`Self::commit`] to store; committing the writes without
	/// it would leave a record absent from the term bitmaps and statistics that
	/// nothing later reconciles. The transactions this exists for touch keys
	/// directly and buffer nothing, so the check names a caller's mistake rather
	/// than a state to handle.
	///
	/// The poisons are checked for the same reason: a transaction that must not
	/// persist its writes must not persist them by this route either.
	pub async fn commit_bare(&self) -> Result<()> {
		self.poisoned()?;
		if self.index_deltas.get().is_some_and(|buffer| !buffer.is_empty()) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A transaction carrying buffered index deltas cannot be committed bare, \
				          because the deltas describe writes it is about to make durable"
					.to_string(),
			}
			.into());
		}
		Ok(self.commit_store().await?)
	}

	/// The reason this transaction may not commit, if there is one.
	///
	/// Five conditions poison a transaction, and all mean its writes are a state no
	/// caller asked for:
	///
	/// - A tripped write-cardinality guard leaves the writes buffered before the failing
	///   reservation as a partial statement, so committing them would break the guard's
	///   atomic-rollback contract.
	/// - A save point the storage layer could not close leaves it unknown which of that scope's
	///   writes survive. See [`Self::save_point_poisoned`].
	/// - A record's write stopped part-way leaves the record disagreeing with its index entries.
	///   See [`Self::poison_interrupted_write`].
	/// - A schema statement that failed after writing part of its change leaves a catalog change
	///   the statement did not finish. See [`Self::catalog_write_poisoned`].
	/// - Any statement that failed after writing part of its change, in a transaction its caller
	///   owns, leaves that part. See [`Self::poison_failed_statement`].
	///
	/// Each is reachable at an explicit COMMIT: a client-owned (RPC/SDK)
	/// transaction survives the statement error that set the flag. An explicit
	/// CANCEL behaves as normal.
	fn poisoned(&self) -> Result<()> {
		if self.write_guard_poisoned.load(Ordering::Relaxed) {
			let limit = self.write_keys_limit.get().map(|l| l.get()).unwrap_or_default();
			return Err(crate::error::DatastoreError::TransactionWriteKeysExceeded {
				limit,
			}
			.into());
		}
		if self.save_point_poisoned.load(Ordering::Relaxed) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A save point could not be closed, leaving the transaction's writes and \
				          the index deltas describing them unreconcilable"
					.to_string(),
			}
			.into());
		}
		if self.interrupted_write_poisoned.load(Ordering::Relaxed) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A record's write was stopped part-way, leaving the transaction holding \
				          only some of that record's index entries"
					.to_string(),
			}
			.into());
		}
		if self.catalog_write_poisoned.load(Ordering::Relaxed) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A schema statement failed after writing part of its change, so the \
				          transaction cannot commit"
					.to_string(),
			}
			.into());
		}
		if self.failed_statement_poisoned.load(Ordering::Relaxed) {
			return Err(crate::error::DatastoreError::QueryNotExecuted {
				message: "A statement failed after writing part of its change, so the transaction \
				          cannot commit"
					.to_string(),
			}
			.into());
		}
		Ok(())
	}

	/// Refuses this transaction's COMMIT, because a statement run in it failed
	/// after it had written part of its change.
	///
	/// A statement's writes reach the transaction as it runs, and nothing takes
	/// them back when it fails: a `DEFINE INDEX` has written its definition when
	/// its build is refused, a `CREATE` has written its record when a `UNIQUE`
	/// index refuses the value, and a multi-row `INSERT` has written the rows
	/// before the one refused. A transaction the statement ran alone in is
	/// cancelled, and a `BEGIN` block is cancelled whole; but a transaction its
	/// caller owns (RPC/SDK) survives the error and can still issue COMMIT, which
	/// would make the part durable without the rest. The flag makes the refusal
	/// the transaction's own, whatever the caller does with the error, and the
	/// refused commit cancels the transaction, so what the statement left
	/// outside it is cleaned up as for any cancel.
	///
	/// A statement that fails before it writes anything leaves the transaction
	/// as it was, and its caller may still commit what the other statements
	/// wrote.
	pub fn poison_failed_statement(&self) {
		self.failed_statement_poisoned.store(true, Ordering::Relaxed);
	}

	/// Refuses this transaction's COMMIT, because a record's write stopped
	/// part-way when its query could not continue.
	///
	/// A record and its index entries are separate keys, and one index can take
	/// many entries, so a write stopped among them leaves the transaction holding a
	/// record that disagrees with what indexes it. That is a failure the statement
	/// reports, but not every caller that sees a statement fail discards its
	/// transaction: a client-owned (RPC/SDK) transaction survives the error and can
	/// still issue COMMIT, and a caller that turns the error into a value carries on
	/// to commit. The flag makes the refusal the transaction's own, whatever the
	/// caller does with the error.
	///
	/// A rollback to a save point does not clear it, even one taken ahead of the
	/// stopped write, matching the write-cardinality guard. A statement that rolls
	/// back past the stopped write and completes — an `INSERT IGNORE` skipping the
	/// row — still has its transaction's commit refused: that costs the caller a
	/// retry, where committing a half-indexed record would cost a corrupt index.
	pub fn poison_interrupted_write(&self) {
		self.interrupted_write_poisoned.store(true, Ordering::Relaxed);
	}

	/// Whether this transaction's commit failed with an outcome the storage
	/// could not report (`CommitOutcomeUnknown`): its writes may or may not have
	/// applied. Set before the commit runs its rollback actions, so an action
	/// can tell a commit that definitely did not apply, where it undoes what it
	/// stands for, from one that may have, where it must first ask the durable
	/// state.
	pub fn commit_outcome_unknown(&self) -> bool {
		self.commit_outcome_unknown.load(Ordering::Acquire)
	}

	/// Cancel without draining the registered close-time actions. See
	/// [`Self::commit_bare`].
	pub async fn cancel_bare(&self) -> Result<()> {
		Ok(self.tr.cancel().await?)
	}

	/// Commit a transaction.
	///
	/// This attempts to commit all changes made within the transaction.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn commit(&self) -> Result<()> {
		self.commit_sealed().await
	}

	#[inline(never)]
	fn commit_sealed(&self) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.commit_body())
	}

	async fn commit_body(&self) -> Result<()> {
		// Refuse a commit the transaction is no longer allowed to make, and roll
		// back instead. See [`Self::poisoned`].
		if let Err(e) = self.poisoned() {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after a poisoned commit was refused: {err}"
				);
			}
			return Err(e);
		}
		// Flush the per-index aggregates before the changefeed, so they are
		// part of this transaction's write set. Failure falls into the same
		// cancel-and-report path as the changefeed flush below.
		if let Err(e) = self.store_index_deltas().await {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after index-delta storage failed; preserving original error {e}: {err}"
				);
			}
			return Err(e);
		}
		// Store any buffered changefeed entries. Failure here falls into
		// `cancel`, which itself emits the transaction event, so avoid
		// double-emission from this path.
		if let Err(e) = self.store_changes().await {
			if let Err(err) = self.cancel().await {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"transaction cleanup failed after changefeed storage failed; preserving original store_changes error {e}: {err}"
				);
			}
			// The cleanup error is secondary here. Callers need the original
			// store_changes error so retry/error classification uses the
			// operation that first made commit impossible.
			return Err(e);
		}
		// Arm the DiskANN pending-state guards this transaction wrote under,
		// holding their fences until the commit below has returned. See
		// [`Self::arm_diskann_pending_guards`]. Boxed, and only for a transaction
		// that buffered a guard: the arming holds a transaction of its own, which
		// would otherwise grow every commit's future by its size.
		let arming = if self.index_deltas.get().is_some_and(|buffer| buffer.has_pending_guards()) {
			Box::pin(self.arm_diskann_pending_guards()).await
		} else {
			Ok(DiskAnnGuardFences::default())
		};
		let guard_fences = match arming {
			Ok(fences) => fences,
			Err(e) => {
				if let Err(err) = self.cancel().await {
					tracing::warn!(
						target: "surrealdb::core::kvs::tx",
						"transaction cleanup failed after arming DiskANN pending guards failed; preserving original error {e}: {err}"
					);
				}
				return Err(e);
			}
		};
		#[cfg(any(test, feature = "test-hooks"))]
		if let Some(hook) = self.triggers.take_before_commit_hook() {
			hook().await;
		}
		// Commit the transaction
		let committed = self.commit_store().await;
		#[cfg(any(test, feature = "test-hooks"))]
		let committed = match committed {
			Ok(()) if !self.rollback_actions.lock().await.is_empty() => {
				match self.triggers.take_commit_applied_unknown() {
					Some(hook) => {
						if let Some(hook) = hook {
							hook().await;
						}
						Err(KvsError::CommitOutcomeUnknown(
							"injected once the commit applied".to_string(),
						))
					}
					None => Ok(()),
				}
			}
			committed => committed,
		};
		#[cfg(diskann)]
		if committed.is_ok() && guard_fences.leases_relied.is_some() {
			Box::pin(self.recover_diskann_leases(&guard_fences)).await;
		}
		#[cfg(diskann)]
		if committed.is_ok() && !guard_fences.wiped.is_empty() {
			Box::pin(self.arm_diskann_guards_after_wipe(&guard_fences)).await;
		}
		guard_fences.finish();
		if let Err(e) = committed {
			// Record the write-set size, which is what separates an over-large
			// transaction from an unhealthy cluster and which storage-layer
			// errors do not carry.
			//
			// Outcomes callers expect and already handle are excluded, so
			// ordinary traffic cannot amplify into the level operators alert
			// on: retryable conflicts are re-driven in a loop, `Shutdown`
			// arrives once per draining transaction on every graceful restart,
			// and the conditional-write misses are how `put_compare` and
			// `del_compare` report a lost race, which on last-writer-wins
			// backends surfaces here at commit rather than at the call.
			//
			// The error text is omitted rather than interpolated: backend
			// errors can embed encoded record keys. The error itself reaches
			// the caller, and the backend logs its own cause under this span.
			let expected_outcome = e.is_retryable()
				|| matches!(
					e,
					KvsError::Shutdown
						| KvsError::TransactionConditionNotMet
						| KvsError::TransactionKeyAlreadyExists
				);
			if !expected_outcome {
				let written = self.metrics.snapshot();
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					keys_written = written.keys_written,
					bytes_written = written.total_bytes_written,
					// Write ops only: `ops_total` counts reads as well, which
					// would make a read-heavy transaction look write-heavy.
					write_ops = written
						.ops_put
						.saturating_add(written.ops_set)
						.saturating_add(written.ops_del),
					"transaction commit failed",
				);
			}
			// A commit refused because the datastore is shutting down was
			// rejected before it applied (the engine gate blocks it ahead of
			// `inner.commit()`), so the cleanup below is correct: nothing was
			// written. All other cleanup writes it triggers are refused the
			// same way while shutdown is in progress, so a shutting-down
			// datastore stays consistent without special-casing here.
			if matches!(e, KvsError::CommitOutcomeUnknown(_)) {
				self.commit_outcome_unknown.store(true, Ordering::Release);
			}
			let cleanup_result = self.run_rollback_actions().await;
			let release_result = self.release_index_build_reservations().await;
			self.discard_commit_actions().await;
			// The `lq` rows the queued KILLEDs were owed for are still there,
			// so their clients must not be told otherwise. Nothing drains the
			// queue on this path, but leaving it full makes that a latent
			// hazard rather than an impossibility.
			// Classify the commit failure so the surrealdb.transaction.* metric
			// family can carry an `error_class` attribute. `e` is a concrete
			// `kvs::Error` here -- the transactor's `commit` returns
			// `kvs::Result<()>` (see `kvs/tr.rs`) -- so we apply the
			// kvs-layer rule directly: retryable variants collapse to
			// `txn_conflict`, everything else to `storage`. The shared
			// `classify_anyhow_error` helper applies the same rule from
			// the `anyhow::Error` path used by the executor.
			let class = if e.is_retryable() {
				surrealdb_observe::error_class::TXN_CONFLICT
			} else {
				surrealdb_observe::error_class::STORAGE
			};
			self.emit_transaction_event_with_class(Outcome::Error, Some(class));
			if let Err(err) = release_result {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"durable index-build reservation cleanup failed after transaction commit failed; preserving original commit error {e}: {err}"
				);
			}
			if let Err(err) = cleanup_result {
				tracing::warn!(
					target: "surrealdb::core::kvs::tx",
					"uncommitted index-build cleanup failed after transaction commit failed; preserving original commit error {e}: {err}"
				);
			}
			// The cleanup error is secondary here. Callers need the commit
			// error so retryable transaction conflicts keep their retry path.
			anyhow::bail!(e);
		}
		if let Err(err) = self.release_index_build_reservations().await {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"durable index-build reservation cleanup failed after transaction commit; committed appendings remain recoverable: {err}"
			);
		}
		self.discard_rollback_actions().await;
		self.run_commit_actions().await;
		if self.trigger_async_event.load(Ordering::Relaxed) {
			// Notify after commit so queued events are visible to workers.
			self.triggers.async_event.notify_one();
		}
		if self.trigger_index_compaction.load(Ordering::Relaxed) {
			// Notify after commit so the queued request is visible to the
			// compactor. Without this the queue waits out the compaction
			// interval, and nothing relates that timer to the write rate — the
			// gap between the two is what lets the delta log grow unbounded.
			self.triggers.index_compaction.notify_one();
		}
		if self.trigger_graph_fold.load(Ordering::Relaxed) {
			// Notify after commit so the queued fold work is visible to the
			// fold task, for the same reason as the compaction trigger above.
			self.triggers.graph_fold.notify_one();
		}
		if self.trigger_table_migration.load(Ordering::Relaxed) {
			// Notify after commit so the queued table migration is visible to the job.
			self.triggers.table_migration.notify_one();
		}
		self.emit_transaction_event(Outcome::Success);
		Ok(())
	}

	/// Check if a key exists in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn exists_key<K>(&self, key: &K, version: Option<u64>) -> Result<bool>
	where
		K: KVKey + Debug,
	{
		self.exists_encoded(key.encode_key()?, version).await
	}

	/// Fetch a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get_key<K>(
		&self,
		key: &K,
		version: Option<u64>,
	) -> Result<Option<<K as KVKey>::Value>>
	where
		K: KVKey + Debug,
	{
		let val = self.get_encoded(key.encode_key()?, version).await?;
		// Build the decode context only on a hit. For `RecordKey` this
		// avoids a `RecordId` clone (table + key) on every miss.
		val.map(|v| <K as KVKey>::Value::kv_decode_value(&v, key.value_context())).transpose()
	}

	/// Fetch a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get(&self, key: Key<'_>, version: Option<u64>) -> Result<Option<Vec<u8>>> {
		self.get_encoded(key, version).await
	}

	/// [`Self::getu_key`] over an encoded key, without decoding the value: for
	/// a program that encodes keys and values itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getu_raw(&self, key: Key<'_>) -> Result<Option<Val>> {
		self.getu_encoded(key).await
	}

	/// Fetch a key from the datastore, registering the key for
	/// commit-time conflict detection.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getu_key<K>(&self, key: &K) -> Result<Option<<K as KVKey>::Value>>
	where
		K: KVKey + Debug,
	{
		// The conflict registration is only validated when the transaction
		// commits, which read-only transactions never do, so a locked read
		// on one must fail rather than silently lose its guarantee.
		if !self.tr.writeable() {
			anyhow::bail!(KvsError::TransactionReadonly);
		}
		let val = self.getu_encoded(key.encode_key()?).await?;
		// Build the decode context only on a hit. For `RecordKey` this
		// avoids a `RecordId` clone (table + key) on every miss.
		val.map(|v| <K as KVKey>::Value::kv_decode_value(&v, key.value_context())).transpose()
	}

	/// Fetch a specific record value at `version` when given, with the generation its stored
	/// bytes carry.
	///
	/// The record is what [`TableProvider::get_record`] answers. The generation is:
	/// - the one the record's bytes were admitted under: `None` for a `revision` record and for
	///   stamp 0;
	/// - `None` for a lightweight edge, which is synthesised from the adjacency and has no stored
	///   bytes, with no layout read;
	/// - `None` for a record that does not exist.
	///
	/// A cached record carries the generation it was cached with.
	#[inline(never)]
	pub fn get_stamped_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> OutOfLine<impl Future<Output = Result<StampedRecord>>> {
		OutOfLine::new(self.get_stamped_record_body(ns, db, tb, id, version))
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			name = "get_stamped_record",
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip_all
		)
	)]
	async fn get_stamped_record_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<StampedRecord> {
		// Cache is not versioned
		if version.is_some() {
			return match self.read_stamped_record(ns, db, tb, id, version).await? {
				Some((record, generation)) => Ok((record.into_read_only(), generation)),
				None => match self.lightweight_record(ns, db, tb, id, version).await? {
					Some(record) => Ok((record.into_read_only(), None)),
					None => Ok((Arc::new(Default::default()), None)),
				},
			};
		}
		let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
		match self.cache.get(&qey) {
			// The entry is in the cache
			Some(val) => val.try_into_stamped_record(),
			// The entry is not in the cache
			None => {
				match self.read_stamped_record(ns, db, tb, id, None).await? {
					Some((record, generation)) => {
						let record = record.into_read_only();
						let entry = cache::tx::Entry::Val(Arc::clone(&record), generation);
						self.cache.insert(qey, entry);
						Ok((record, generation))
					}
					// A lightweight edge has no record; synthesize
					// it from the adjacency. Cacheable: the delete
					// pipeline clears the entry through
					// `del_record` exactly as for a stored record.
					None => match self.lightweight_record(ns, db, tb, id, None).await? {
						Some(record) => {
							let record = record.into_read_only();
							let entry = cache::tx::Entry::Val(Arc::clone(&record), None);
							self.cache.insert(qey, entry);
							Ok((record, None))
						}
						None => Ok((Arc::new(Default::default()), None)),
					},
				}
			}
		}
	}

	/// Fetch a specific record value, registering the record key for
	/// commit-time conflict detection.
	///
	/// This function will return a new default initialized record if it does not exist.
	///
	/// The read is always issued to the underlying datastore, even when
	/// the record is present in the transaction read cache: the conflict
	/// registration must reach the storage engine. The fetched value
	/// refreshes the cache afterwards.
	///
	/// The stored bytes are decoded against the table's layouts
	/// ([`Self::decode_record`]), so a record of a generation the table does not list, or
	/// lists with a layout this build refuses, is refused rather than read.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get_record_for_update(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
	) -> Result<Arc<Record>> {
		self.get_stamped_record_for_update(ns, db, tb, id).await.map(|(record, _)| record)
	}

	/// [`Self::get_record_for_update`] with the generation the record's bytes carry: `None` for a
	/// `revision` record, stamp 0 and a record that does not exist.
	#[inline(never)]
	pub fn get_stamped_record_for_update(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
	) -> OutOfLine<impl Future<Output = Result<StampedRecord>>> {
		OutOfLine::new(self.get_stamped_record_for_update_body(ns, db, tb, id))
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			name = "get_stamped_record_for_update",
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip_all
		)
	)]
	async fn get_stamped_record_for_update_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
	) -> Result<StampedRecord> {
		let key = RecordKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
		};
		match self.getu_key_raw(&key).await? {
			Some(bytes) => {
				// The key supplies the canonical `id`, and the table's layouts admit a v1
				// record's generation.
				let rid = RecordId {
					table: tb.clone(),
					key: id.clone(),
				};
				let (record, generation) =
					self.decode_stamped_record(ns, db, tb, &bytes, rid, None).await?;
				let record = record.into_read_only();
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				let entry = cache::tx::Entry::Val(Arc::clone(&record), generation);
				self.cache.insert(qey, entry);
				Ok((record, generation))
			}
			None => Ok((Arc::new(Default::default()), None)),
		}
	}

	/// [`TableProvider::get_records`] with the generation each record's bytes carry: `None` for a
	/// `revision` record, stamp 0, a lightweight edge and an id that names no record.
	///
	/// A cached record carries the generation it was cached with. The batch fails at the first
	/// record whose generation is refused.
	#[inline(never)]
	pub fn get_stamped_records(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &[RecordId],
		version: Option<u64>,
		cache_policy: CachePolicy,
	) -> OutOfLine<impl Future<Output = Result<Vec<StampedRecord>>>> {
		OutOfLine::new(self.get_stamped_records_body(ns, db, rids, version, cache_policy))
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			name = "get_stamped_records",
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip_all
		)
	)]
	async fn get_stamped_records_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &[RecordId],
		version: Option<u64>,
		cache_policy: CachePolicy,
	) -> Result<Vec<StampedRecord>> {
		if rids.is_empty() {
			return Ok(Vec::new());
		}
		let mut out: Vec<Option<(Arc<Record>, Option<Generation>)>> = vec![None; rids.len()];
		let mut uncached_rids: Vec<(usize, &RecordId)> = Vec::new();
		if version.is_some() {
			uncached_rids.extend(rids.iter().enumerate());
		} else {
			for (i, rid) in rids.iter().enumerate() {
				let qey = cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
				match self.cache.get(&qey) {
					Some(entry) => out[i] = Some(entry.try_into_stamped_record()?),
					None => uncached_rids.push((i, rid)),
				}
			}
		}
		if !uncached_rids.is_empty() {
			let keys: Vec<RecordKey<'_>> = uncached_rids
				.iter()
				.map(|(_, rid)| RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(&rid.table),
					id: Cow::Borrowed(&rid.key),
				})
				.collect();
			let values = self.getm_raw(keys, version).await?;
			let values = self
				.decode_records(ns, db, uncached_rids.iter().map(|(_, rid)| *rid), values, version)
				.await?;
			let mut misses: Vec<(usize, &RecordId)> = Vec::new();
			for ((i, rid), slot) in uncached_rids.into_iter().zip(values) {
				match slot {
					Some((record, generation)) => {
						// The cache is not versioned, and a read-only policy avoids eviction
						// churn during large sequential scans.
						if version.is_none()
							&& (record.metadata.is_some() || !record.data.is_none())
							&& matches!(cache_policy, CachePolicy::ReadWrite)
						{
							let qey =
								cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
							let entry = cache::tx::Entry::Val(Arc::clone(&record), generation);
							self.cache.insert(qey, entry);
						}
						out[i] = Some((record, generation));
					}
					None => misses.push((i, rid)),
				}
			}
			// A miss can be a lightweight edge; resolve every miss in one batched adjacency read.
			if !misses.is_empty() {
				let miss_rids: Vec<&RecordId> = misses.iter().map(|(_, rid)| *rid).collect();
				let records = self.lightweight_records(ns, db, &miss_rids, version).await?;
				for ((i, rid), record) in misses.into_iter().zip(records) {
					let record = match record {
						Some(record) => record.into_read_only(),
						None => Arc::new(Default::default()),
					};
					if version.is_none()
						&& (record.metadata.is_some() || !record.data.is_none())
						&& matches!(cache_policy, CachePolicy::ReadWrite)
					{
						let qey = cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
						self.cache.insert(qey, cache::tx::Entry::Val(Arc::clone(&record), None));
					}
					out[i] = Some((record, None));
				}
			}
		}
		out.into_iter()
			.map(|o| {
				o.ok_or_else(|| {
					EngineError::Internal("missing record in multi-get batch".into()).into()
				})
			})
			.collect()
	}

	/// Fetch every entry in a range, with its value decoded under the type the range
	/// says it stores.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getr<V>(
		&self,
		rng: TypedRange<V>,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) =
			self.decode_range(rng.into_key_range(), Direction::Forward, None, 0, version).await?;
		self.metrics.record_get(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// Streams a range through the cursor, decoding each value from the bytes the
	/// cursor lends rather than from a copy.
	///
	/// One pass and one output allocation. Nothing materialises the raw rows first,
	/// so the range is never held twice: a backend whose cursor lends borrowed bytes
	/// copies no value at all, and one whose cursor owns its rows owns a batch of
	/// them rather than the whole range.
	///
	/// `limit` bounds the rows read; `None` reads to the end of the range.
	async fn decode_range<V>(
		&self,
		rng: KeyRange<'static>,
		dir: Direction,
		limit: Option<u32>,
		skip: u32,
		version: Option<u64>,
	) -> Result<(Vec<(Vec<u8>, V)>, ScanChunkStats)>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, _, stats) =
			self.decode_range_tolerating(rng, dir, limit, skip, version, false).await?;
		Ok((out, stats))
	}

	/// [`Self::decode_range`], which with `skip_refused` set skips an entry that a newer
	/// release wrote ([`refused_by_release`]) instead of failing, and counts the entries it
	/// skipped. Any other decode failure still fails the read.
	async fn decode_range_tolerating<V>(
		&self,
		rng: KeyRange<'static>,
		dir: Direction,
		limit: Option<u32>,
		skip: u32,
		version: Option<u64>,
		skip_refused: bool,
	) -> Result<(Vec<(Vec<u8>, V)>, usize, ScanChunkStats)>
	where
		// `Send` because the decode happens inside the cursor's visitor, which the
		// storage layer requires to be sendable.
		V: KVValue<KeyContext = ()> + Send,
	{
		let mut refused = 0usize;
		let mut cursor = self.tr.open_vals_cursor(rng, dir, skip, version).await?;
		let mut out: Vec<(Vec<u8>, V)> = match limit {
			// A bounded read knows its ceiling, so size the output once. Unbounded
			// reads grow, because the range's length is not known until it ends.
			Some(limit) => Vec::with_capacity(limit.min(NORMAL_BATCH_SIZE) as usize),
			None => Vec::new(),
		};
		let mut total = ScanChunkStats::default();
		let mut failed: Option<anyhow::Error> = None;

		let mut remaining = limit;
		loop {
			let chunk = remaining.map_or(NORMAL_BATCH_SIZE, |n| n.min(NORMAL_BATCH_SIZE));
			if chunk == 0 {
				break;
			}
			// The visitor's error type belongs to the storage layer, so a decode
			// failure is carried out here and the cursor is abandoned: after a
			// visitor error its resume position is not defined.
			let stats = cursor
				.for_each(chunk, &mut |key: &[u8], val: &[u8]| match V::kv_decode_value(val, ()) {
					Ok(value) => {
						out.push((key.to_vec(), value));
						Ok(ControlFlow::Continue(()))
					}
					Err(e) if skip_refused && refused_by_release(&e) => {
						refused += 1;
						Ok(ControlFlow::Continue(()))
					}
					Err(e) => {
						failed = Some(e);
						Ok(ControlFlow::Break(()))
					}
				})
				.await?;
			if let Some(e) = failed {
				return Err(e);
			}
			total.rows += stats.rows;
			total.key_bytes += stats.key_bytes;
			total.value_bytes += stats.value_bytes;
			// A short chunk means the range is exhausted.
			if stats.rows < chunk as u64 {
				break;
			}
			if let Some(n) = remaining.as_mut() {
				*n -= chunk;
			}
		}

		Ok((out, refused, total))
	}

	/// Fetch every entry in a range or region as bytes.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getr_raw(
		&self,
		rng: impl AnyRange,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
		let val = self.tr.getr(rng.into_key_range(), version).await?;
		self.metrics.record_get(val.values.len() as u64, val.key_bytes, val.value_bytes);
		Ok(val.values)
	}

	/// Retrieve a batch set of keys from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get_many_key<K>(
		&self,
		keys: Vec<K>,
		version: Option<u64>,
	) -> Result<Vec<Option<<K as KVKey>::Value>>>
	where
		K: KVKey + Debug,
	{
		let encoded_keys: Vec<_> = keys.iter().map(|k| k.encode_key()).collect::<Result<_>>()?;
		let values = self.getm_encoded(&encoded_keys, version).await?;
		values
			.into_iter()
			.zip(keys)
			.map(|(v, k)| match v {
				Some(v) => <K as KVKey>::Value::kv_decode_value(&v, k.value_context()).map(Some),
				None => Ok(None),
			})
			.collect()
	}

	/// Retrieve a specific prefix of keys from the datastore.
	///
	/// Range scans are intentionally restricted to value types with
	/// `KeyContext = ()`: per-row context isn't available without decoding
	/// each row's storage key. Callers that need to scan a record range
	/// must decode the storage key per row and reconstruct the context
	/// themselves.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get_prefix_key<K>(
		&self,
		key: &K,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Vec<u8>)>>
	where
		K: KVSubspace + Debug,
	{
		let range = key.raw_range()?;
		let res = self.tr.getr(range.into_key_range(), version).await?;

		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);

		Ok(res.values)
	}

	/// Delete a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let key = key.encode_key()?;
		self.delete_encoded(changed, key, false).await
	}

	/// Delete a key of a prefix that holds no live definition, such as one whose catalog parent
	/// is removed: soft-deleted when `expunge` is false, cleared of every version when true.
	///
	/// Unlike [`Self::del`] and [`Self::clr`], the key is not examined as a definition: a
	/// definition under a removed parent names no live catalog state, so its deletion stamps no
	/// version and forfeits no conformance. Those writes would land under the very prefix being
	/// reclaimed, either deleting a key the reclaim deletes anyway or writing one behind its
	/// cursor. The transaction must hold no layout or conformance of a table under the prefix.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_reclaimed(&self, key: Key<'_>, expunge: bool) -> Result<()> {
		let _changed = self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		if expunge {
			self.tr.clr(key).await?;
		} else {
			self.tr.del(key).await?;
		}
		self.metrics.record_del(1, key_bytes);
		Ok(())
	}

	/// Delete a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del(&self, key: Key<'_>) -> Result<()> {
		let changed = self.reserve_write_slot()?;
		self.delete_encoded(changed, key, false).await
	}

	/// The stored bytes to retry a failed comparison against, or `None` if the
	/// comparison failed for real.
	///
	/// A revisioned value encodes at whatever revision this build declares,
	/// which need not be the revision the stored value was written at. So a
	/// guarded write whose check value says exactly what is stored is still
	/// refused, and a caller that retries such a write retries forever.
	///
	/// The stored bytes are normalised through the same decode/encode round trip
	/// and compared against `want`. Equal normal forms mean the two values say
	/// the same thing, and the retry then fences on the bytes that are actually
	/// there, so it stays as atomic as the byte comparison it repeats. Anything
	/// else leaves the original refusal standing, as a genuine mismatch must:
	/// a different value, nothing stored, or bytes that will not decode — which
	/// disagree with `want` as surely as a value that decodes to something else.
	///
	/// Called only after a comparison has been refused, so the read it costs is
	/// paid on the failing path rather than on every guarded write.
	///
	/// Both `K::encode_key` and `K::Value::kv_encode_value` must be
	/// deterministic. `want` is a second encoding of the check value the caller
	/// already encoded once, and the retry re-encodes the key and value the
	/// first attempt consumed, so an encoder whose output varied between calls
	/// on one value would compare unequal normal forms and refuse a write that
	/// should succeed. Nothing reached from these value types encodes an
	/// unordered collection — `Object` is a `VecMap` — so the property holds
	/// for them, and a value type that breaks it breaks `normalised == want`
	/// regardless of this retry.
	async fn stored_bytes_matching<K>(&self, key: &K, want: &[u8]) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		let Some(stored) = self.get_key_raw(key, None).await? else {
			return Ok(None);
		};
		let normalised = K::Value::kv_decode_value(&stored, key.value_context())
			.and_then(|v| v.kv_encode_value())
			.ok();
		Ok((normalised.as_deref() == Some(want)).then_some(stored))
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_compare_key<K>(&self, key: &K, chk: Option<&K::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		if <K::Value as KVValue>::LEVELLED
			&& let Some(chk) = chk
		{
			return self.delete_compare_levelled(key, chk, false).await;
		}
		let _changed = self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		let defined = self.changed_definition(&encoded, None, None).await?;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.delc(encoded, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its key over rather than copying it; a
				// retry re-encodes, on a path that has already paid for a read.
				match self.tr.delc(encoded, Some(&want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr.delc(key.encode_key()?, Some(&stored)).await?
					}
					other => other?,
				}
			}
		}
		self.metrics.record_del(1, key_bytes);
		self.restamp_defined(defined).await
	}

	/// Delete a key from the datastore if the current value matches a
	/// condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_compare(&self, key: Key<'_>, chk: Option<&[u8]>) -> Result<()> {
		let _changed = self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		let defined = self.changed_definition(&key, None, None).await?;
		self.tr.delc(key, chk).await?;
		self.metrics.record_del(1, key_bytes);
		self.restamp_defined(defined).await
	}

	/// Delete a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn delr(&self, rng: impl AnyRange) -> Result<()> {
		let _changed = self.reserve_write_slot()?;
		let rng = rng.into_key_range();
		if range_may_hold_definitions(&rng.start) {
			self.touches_catalog();
		}
		// Whether or not the delete completes, it may have removed layouts the transaction holds.
		let spans_layouts = self.begin_range_delete(&rng);
		let deleted = self.tr.delr(rng).await;
		if spans_layouts {
			self.invalidate_layouts();
		}
		deleted?;
		// Range/prefix deletes don't report the number of affected keys or
		// their byte size.
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete a range of keys from the datastore, as [`Self::delr`] does.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_prefix_key<K>(&self, rng: &K) -> Result<()>
	where
		K: KVSubspace + Debug,
	{
		let rng = rng.raw_range()?;
		self.delr(rng).await
	}

	/// Delete all versions of a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn clr(&self, key: Key<'_>) -> Result<()> {
		let changed = self.reserve_write_slot()?;
		self.delete_encoded(changed, key, true).await
	}

	/// Delete all versions of a key from the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn clr_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let key = key.encode_key()?;
		self.delete_encoded(changed, key, true).await
	}

	/// Delete all versions of a key from the datastore if the current value
	/// matches a condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn clr_compare_key<K>(&self, key: &K, chk: Option<&<K as KVKey>::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		if <K::Value as KVValue>::LEVELLED
			&& let Some(chk) = chk
		{
			return self.delete_compare_levelled(key, chk, true).await;
		}
		let _changed = self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		let defined = self.changed_definition(&encoded, None, None).await?;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.clrc(encoded, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its key over rather than copying it; a
				// retry re-encodes, on a path that has already paid for a read.
				match self.tr.clrc(encoded, Some(&want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr.clrc(key.encode_key()?, Some(&stored)).await?
					}
					other => other?,
				}
			}
		}
		self.metrics.record_del(1, key_bytes);
		self.restamp_defined(defined).await
	}

	/// Delete all versions of a range of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn clrr(&self, rng: impl AnyRange) -> Result<()> {
		let _changed = self.reserve_write_slot()?;
		let rng = rng.into_key_range();
		if range_may_hold_definitions(&rng.start) {
			self.touches_catalog();
		}
		let spans_layouts = self.begin_range_delete(&rng);
		let cleared = self.tr.clrr(rng).await;
		if spans_layouts {
			self.invalidate_layouts();
		}
		cleared?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Delete all versions of a prefix of keys from the datastore.
	///
	/// This function deletes entries from the underlying datastore in grouped
	/// batches.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn clr_prefix_key<K>(&self, key: &K) -> Result<()>
	where
		K: KVSubspace,
	{
		let _changed = self.reserve_write_slot()?;
		let range = key.raw_range()?.into_key_range();
		let spans_layouts = self.begin_range_delete(&range);
		let cleared = self.tr.clrr(range).await;
		if spans_layouts {
			self.invalidate_layouts();
		}
		cleared?;
		self.metrics.record_del(0, 0);
		Ok(())
	}

	/// Remove a namespace's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// Unlike [`surrealdb_catalog::providers::NamespaceProvider::del_ns`], this
	/// does **not** delete the (potentially huge) `/*{ns}` data prefix inside
	/// the transaction. Only the catalog name→id entry is removed, so the
	/// namespace is immediately unreachable; a reclaim job is enqueued and
	/// [`crate::Datastore::reclaim_tombstones`] destroys the data later.
	/// Because both writes are staged in this transaction, a rollback undoes
	/// the removal and never destroys data.
	pub async fn del_ns_deferred(&self, ns: &str, expunge: bool) -> Result<Option<NamespaceId>> {
		let Some(ns_def) = self.get_ns_by_name(ns, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = NamespaceKey {
			ns: Cow::Borrowed(&ns_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the namespace data prefix.
		let rc = ReclaimKey::namespace(ns_def.namespace_id, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		// Invalidate cached namespace lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Nss);
		self.cache.remove(&cache::tx::Lookup::NsByName(&ns_def.name));
		Ok(Some(ns_def.namespace_id))
	}

	/// Remove a database's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_catalog::providers::DatabaseProvider::del_db`] used by
	/// `REMOVE DATABASE`: the `/*{ns}*{db}` prefix is destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`], not in this transaction.
	pub async fn del_db_deferred(
		&self,
		ns: &str,
		db: &str,
		expunge: bool,
	) -> Result<Option<DatabaseId>> {
		let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
			return Ok(None);
		};
		// Delete only the catalog definition; defer the data deletion.
		let key = DatabaseKey {
			ns: db_def.namespace_id,
			db: Cow::Borrowed(&db_def.name),
		};
		if expunge {
			self.clr_key(&key).await?;
		} else {
			self.del_key(&key).await?;
		}
		// Enqueue background reclaim of the database data prefix.
		let rc =
			ReclaimKey::database(db_def.namespace_id, db_def.database_id, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		// Invalidate cached database lookups so the removal is observed.
		self.cache.remove(&cache::tx::Lookup::Dbs(db_def.namespace_id));
		self.cache.remove(&cache::tx::Lookup::DbByName(ns, &db_def.name));
		Ok(Some(db_def.database_id))
	}

	/// Remove an index's catalog definition and enqueue its data prefix for
	/// asynchronous background reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_catalog::providers::TableProvider::del_tb_index`] used by
	/// `REMOVE INDEX`. The catalog definition and id→name lookup are removed
	/// immediately so the index stops being maintained and used; the
	/// `/*{ns}*{db}*{tb}+{ix}` data prefix is destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`].
	///
	/// Safe against index recreation because index ids are never reused: a new
	/// `DEFINE INDEX` of the same name allocates a fresh id (the old definition
	/// is already gone), so its data prefix is disjoint from the one queued for
	/// reclaim here.
	pub async fn del_tb_index_deferred(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: &str,
	) -> Result<()> {
		let Some(ix_def) = self.get_tb_index(ns, db, tb, ix, None).await? else {
			return Ok(());
		};
		// Delete the catalog definition; defer the index data deletion.
		let key = IndexDefKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: Cow::Borrowed(&ix_def.name),
		};
		self.del_key(&key).await?;
		// Delete the id-to-name lookup.
		let name_lookup_key = IndexNameKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix: ix_def.index_id,
		};
		self.del_key(&name_lookup_key).await?;
		self.bump_catalog_version(ns, db, tb).await?;
		self.enqueue_index_reclaim(ns, db, tb, ix_def.index_id, false).await?;
		// Invalidate the cached list of all indexes for this table.
		self.cache.remove(&cache::tx::Lookup::Ixs(ns, db, tb.as_ref()));
		// Invalidate the cached index entry.
		self.cache.remove(&cache::tx::Lookup::Ix(ns, db, tb.as_ref(), &ix_def.name));
		Ok(())
	}

	/// Writes back a table definition produced by modifying a
	/// previously-read copy, serializing the read-modify-write against
	/// every concurrent writer of the same definition.
	///
	/// The definition key is read locked ([`Self::getu_key`]) before the
	/// write. A blind [`TableProvider::put_tb`] is safe only for a
	/// definition's initial write: on a backend whose commit protocol
	/// validates writes but not plain reads (TiKV), two read-modify-writes
	/// of the same definition would both commit and the later one would
	/// silently erase whatever the earlier one changed — most critically
	/// the `graph_folded` marker a concurrent fold flips, whose loss hides
	/// every folded edge on the table. The locked read registers the key
	/// for commit-time validation on every backend, so the second
	/// committer conflicts and retries from a fresh read instead.
	///
	/// The same locked read is what makes a catalog version name one
	/// committed state of the table: every DDL statement reaches the table
	/// through this method, and the version [`Self::bump_catalog_version`]
	/// writes alongside is a blind write, so two DDL transactions on one
	/// table conflict here and never both commit under versions that each
	/// name a state the other overwrote. A DDL path that wrote a table
	/// definition without this read would break that identity on a
	/// last-writer-wins backend.
	pub async fn replace_tb(
		&self,
		ns: &str,
		db: &str,
		tb: &TableDefinition,
	) -> Result<Arc<TableDefinition>> {
		let key = TableKey {
			ns: tb.namespace_id,
			db: tb.database_id,
			tb: Cow::Borrowed(&tb.name),
		};
		let _ = self.getu_key(&key).await?;
		self.put_tb(ns, db, tb).await
	}

	/// Register this database's changefeed-retention fence for commit-time
	/// conflict detection, so this transaction cannot commit if a retention
	/// change lands first.
	///
	/// Read through [`Self::getu_key`] rather than written: the fence exists only
	/// to give the changefeed collector and the statements that set a retention a
	/// key in common, and a plain read would not serve — on a last-writer-wins
	/// backend nothing validates a read set, which is the backend where the two
	/// otherwise share no key and so never conflict.
	///
	/// The collector calls this inside each page's own write transaction, so the
	/// arming covers that page and no more.
	pub async fn arm_changefeed_retention_fence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		let key = ChangefeedFenceKey {
			ns,
			db,
		};
		let _ = self.getu_key(&key).await?;
		Ok(())
	}

	/// Bump this database's changefeed-retention fence, so a collection page that
	/// armed against it cannot commit.
	///
	/// Called by every path that writes a changefeed retention. The count carries
	/// no meaning beyond changing, and over-bumping costs only a retried page, so
	/// it is bumped whenever a written definition carries a retention rather than
	/// only when one is extended.
	pub async fn bump_changefeed_retention_fence(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<()> {
		let key = ChangefeedFenceKey {
			ns,
			db,
		};
		let next = self.get_key(&key, None).await?.unwrap_or(0).wrapping_add(1);
		self.set_key(&key, &next).await
	}

	/// The identity of `tb`'s catalog entry as this transaction sees it, or
	/// `None` for a table no catalog write has stamped yet.
	///
	/// A stored version is answered only where it is proven against the
	/// table's definition key ([`CatalogVersion::proven`]): a definition
	/// rewritten or removed since the stamp by a build that predates versions
	/// reads as unstamped, so a reader falls back to the definition itself.
	///
	/// The answer is the transaction's snapshot plus its own uncommitted catalog
	/// writes: a transaction that has run DDL on the table sees the version it
	/// minted, and no other transaction can see that version until this one
	/// commits. Two transactions that read the same `Some` value see the same
	/// table definition, fields and indexes, which is what lets
	/// [`crate::cache::ds::TableSnapshot`] share what one of them loaded.
	///
	/// Storage is read once per table per transaction: the answer, `None`
	/// included, is held in the transaction cache, and
	/// [`Self::bump_catalog_version`] replaces it with the version it mints.
	/// Under snapshot isolation nothing else can change the stored value this
	/// transaction sees, so the cached answer is the stored one for the life of
	/// the transaction, or until [`Self::clear_cache`] drops it.
	/// The definition of `tb` as of `version`, read by the storage layer to
	/// lay out a record it reads or writes: whether the table is a
	/// lightweight relation, whether its adjacency is folded, the caps of its
	/// inline caches, its id. The read is the storage layer's own, which the
	/// work it serves does not make, so it is made unsealed
	/// ([`crate::catalog_reads::sealed`]); open recordings still note it.
	pub fn layout_tb<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>> {
		Box::pin(crate::catalog_reads::sealed(false, self.get_tb(ns, db, tb, version)))
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn catalog_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Option<Uuid>> {
		self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
		let qey = cache::tx::Lookup::Cv(ns, db, tb.as_str());
		if let Some(entry) = self.cache.get(&qey) {
			return Ok(*entry.try_into_type::<Option<Uuid>>()?);
		}
		if let Some((epoch, shared)) = self.epoch_catalog()
			&& let Some(version) =
				shared.get::<Option<Uuid>>(&crate::cache::epoch::Lookup::Version(epoch, ns, db, tb))
		{
			self.cache.insert(qey, cache::tx::Entry::Any(version.clone()));
			return Ok(*version);
		}
		let key = CatalogVersionKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let version = match self.get_key(&key, None).await? {
			Some(stamped) => stamped.proven(self.table_stored(ns, db, tb).await?.as_deref()),
			None => None,
		};
		let held = Arc::new(version);
		self.cache.insert(qey, cache::tx::Entry::Any(held.clone()));
		self.keep_for_epoch(|epoch| crate::cache::epoch::Lookup::Version(epoch, ns, db, tb), &held);
		Ok(version)
	}

	/// The value shapes index `ix` of `tb` has stored, as this transaction
	/// sees them ([`crate::index_shapes`]).
	pub async fn index_shapes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<Arc<IndexShapes>> {
		let shapes = self.table_index_shapes_of(ns, db, tb, &[ix]).await?;
		match shapes.0.first() {
			Some((_, shapes)) => Ok(Arc::clone(shapes)),
			None => fail!("index shapes read for no index"),
		}
	}

	/// The value shapes the B-tree indexes among `indexes`, `tb`'s, have
	/// stored, as this transaction sees them, read together.
	pub async fn table_index_shapes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		indexes: &[catalog::IndexDefinition],
	) -> Result<TableIndexShapes> {
		let btree: Vec<IndexId> = indexes
			.iter()
			.filter(|ix| matches!(ix.index, catalog::Index::Idx | catalog::Index::Uniq))
			.map(|ix| ix.index_id)
			.collect();
		self.table_index_shapes_of(ns, db, tb, &btree).await
	}

	/// The value shapes the B-tree indexes among `indexes`, `tb`'s, have
	/// stored, for planning a statement that cannot write
	/// ([`crate::index_shapes`]).
	///
	/// Shared through `cache` under the table's catalog version, for every
	/// index whose build had finished, or had never run, where the shapes were
	/// read: a finished index records a new shape only with a document write,
	/// which stamps the version. An index still building records shapes
	/// without stamping, so it is read through this transaction, as is every
	/// index of a table no catalog write has stamped. A shared entry may
	/// predate a rebuild of an index that the rebuild's statement has not yet
	/// stamped past; the shapes a rebuild records are among those its index
	/// recorded before it, so such an entry claims no less than the index
	/// holds. Writers do not read through here: a writer that took an
	/// outdated shape as recorded would skip recording it.
	pub async fn planning_index_shapes(
		&self,
		cache: Option<&crate::cache::ds::DatastoreCache>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		indexes: &[catalog::IndexDefinition],
	) -> Result<TableIndexShapes> {
		let btree: Vec<IndexId> = indexes
			.iter()
			.filter(|ix| matches!(ix.index, catalog::Index::Idx | catalog::Index::Uniq))
			.map(|ix| ix.index_id)
			.collect();
		if btree.is_empty() {
			return Ok(TableIndexShapes::default());
		}
		let (Some(cache), Some(version)) = (cache, self.catalog_version(ns, db, tb).await?) else {
			return self.table_index_shapes_of(ns, db, tb, &btree).await;
		};
		let lookup = crate::cache::ds::Lookup::Ish(ns, db, tb.as_str(), version);
		let shared = match cache.get(&lookup) {
			Some(entry) => entry.try_into_ish()?,
			None => {
				let (read, finished) =
					self.read_index_shapes_and_builds(ns, db, tb, &btree).await?;
				let finished: Vec<_> =
					read.0.iter().filter(|(ix, _)| finished.contains(ix)).cloned().collect();
				cache.insert(
					crate::cache::ds::Lookup::Ish(ns, db, tb.as_str(), version),
					crate::cache::ds::Entry::Ish(TableIndexShapes(finished.into())),
				);
				return Ok(read);
			}
		};
		let unshared: Vec<IndexId> =
			btree.iter().copied().filter(|ix| shared.of(*ix).is_none()).collect();
		if unshared.is_empty() {
			return Ok(shared);
		}
		let read = self.table_index_shapes_of(ns, db, tb, &unshared).await?;
		Ok(TableIndexShapes(shared.0.iter().chain(read.0.iter()).cloned().collect()))
	}

	/// The value shapes of indexes `ixs` of `tb` as this transaction sees
	/// them, with the indexes among them whose build had finished or never
	/// ran, all read in one request. The shapes are kept in this transaction's
	/// cache.
	async fn read_index_shapes_and_builds(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ixs: &[IndexId],
	) -> Result<(TableIndexShapes, Vec<IndexId>)> {
		let mut keys = Vec::with_capacity(ixs.len() * 4);
		for &ix in ixs {
			keys.push(
				BuildStateKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
				}
				.encode_key()?,
			);
			self.push_index_shape_keys(&mut keys, ns, db, tb, ix)?;
		}
		let key_bytes: u64 = keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(&keys, None).await?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		let mut values = res.values.into_iter();
		let mut shapes = Vec::with_capacity(ixs.len());
		let mut finished = Vec::with_capacity(ixs.len());
		for &ix in ixs {
			let Some(build) = values.next() else {
				fail!("index shapes read answered fewer values than keys");
			};
			let build = match build {
				Some(bytes) => {
					let key = BuildStateKey {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						ix,
					};
					Some(IndexBuildState::kv_decode_value(&bytes, key.value_context())?)
				}
				None => None,
			};
			if build.is_none_or(|state| state.phase == IndexBuildPhase::Online) {
				finished.push(ix);
			}
			let read = Arc::new(Self::decode_index_shapes(&mut values)?);
			self.cache.insert(
				cache::tx::Lookup::Ish(ns, db, tb.as_str(), ix),
				cache::tx::Entry::Any(Arc::clone(&read) as _),
			);
			shapes.push((ix, read));
		}
		Ok((TableIndexShapes(shapes.into()), finished))
	}

	/// Push the keys an index's shapes are read from — its `!it` marker, its
	/// `!ia` and its `!ir` key — in the order [`Self::decode_index_shapes`]
	/// reads their values.
	fn push_index_shape_keys(
		&self,
		keys: &mut Vec<Key<'static>>,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<()> {
		let tb = Cow::Borrowed(tb);
		keys.push(
			IndexShapesTrackedKey {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}
			.encode_key()?,
		);
		keys.push(
			IndexShapesKey {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}
			.encode_key()?,
		);
		keys.push(
			IndexBuildShapesKey {
				ns,
				db,
				tb,
				ix,
			}
			.encode_key()?,
		);
		Ok(())
	}

	/// The shapes of index `ix` from the next three values of `values`, read
	/// from the keys [`Self::push_index_shape_keys`] pushed.
	fn decode_index_shapes(values: &mut impl Iterator<Item = Option<Val>>) -> Result<IndexShapes> {
		let (Some(tracked), Some(written), Some(built)) =
			(values.next(), values.next(), values.next())
		else {
			fail!("index shapes read answered fewer values than keys");
		};
		let decode = |bytes: Option<Val>| match bytes {
			Some(bytes) => <Vec<u8> as KVValue>::kv_decode_value(&bytes, ()),
			None => Ok(Vec::new()),
		};
		Ok(IndexShapes::new(tracked.is_some(), decode(written)?, decode(built)?))
	}

	/// The value shapes of indexes `ixs` of `tb`, read through this
	/// transaction's cache, the ones not cached read together.
	async fn table_index_shapes_of(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ixs: &[IndexId],
	) -> Result<TableIndexShapes> {
		let mut shapes = Vec::with_capacity(ixs.len());
		let mut unread = Vec::new();
		for &ix in ixs {
			match self.cache.get(&cache::tx::Lookup::Ish(ns, db, tb.as_str(), ix)) {
				Some(entry) => shapes.push((ix, entry.try_into_type::<IndexShapes>()?)),
				None => unread.push(ix),
			}
		}
		if !unread.is_empty() {
			// Every index's marker and shapes, read in one request.
			let mut keys = Vec::with_capacity(unread.len() * 3);
			for &ix in &unread {
				self.push_index_shape_keys(&mut keys, ns, db, tb, ix)?;
			}
			let key_bytes: u64 = keys.iter().map(|k| k.len() as u64).sum();
			let res = self.tr.getm(&keys, None).await?;
			self.metrics.record_get(res.records, key_bytes, res.value_bytes);
			let mut values = res.values.into_iter();
			for ix in unread {
				let read = Arc::new(Self::decode_index_shapes(&mut values)?);
				self.cache.insert(
					cache::tx::Lookup::Ish(ns, db, tb.as_str(), ix),
					cache::tx::Entry::Any(Arc::clone(&read) as _),
				);
				shapes.push((ix, read));
			}
		}
		Ok(TableIndexShapes(shapes.into()))
	}

	/// Record that index `ix` of `tb` has stored `shapes`, each a column and
	/// the shape of the value written there, in the transaction that writes
	/// the entries, whether or not the index is tracked.
	///
	/// Nothing is written for shapes already recorded; otherwise `by`'s key is
	/// rewritten from the value this transaction read of it, so a concurrent
	/// recording of the same kind conflicts with this one and is retried
	/// against it. A document write's recording stamps the table's catalog
	/// version, so no plan made before the shape was stored is reused after it;
	/// a build's stamps nothing.
	pub async fn record_index_shapes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		shapes: &[(u16, ValueShape)],
		by: ShapeRecorder,
	) -> Result<()> {
		let known = self.index_shapes(ns, db, tb, ix).await?;
		let Some(stored) = known.with(shapes, by) else {
			return Ok(());
		};
		match by {
			ShapeRecorder::Write => {
				let key = IndexShapesKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
				};
				self.set_key(&key, &stored).await?;
			}
			ShapeRecorder::Build => {
				let key = IndexBuildShapesKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
				};
				self.set_key(&key, &stored).await?;
			}
		}
		self.note_shaped_index(ns, db, tb, ix);
		self.cache.insert(
			cache::tx::Lookup::Ish(ns, db, tb.as_str(), ix),
			cache::tx::Entry::Any(Arc::new(known.replacing(by, stored))),
		);
		if by == ShapeRecorder::Write {
			self.bump_catalog_version(ns, db, tb).await?;
		}
		Ok(())
	}

	/// Mark index `ix` of `tb` as recording every shape it stores from now
	/// on, with `expected`, the shape each column is expected to store,
	/// recorded as the build's: written by the transaction that begins a
	/// build, after it has cleared the index's data and the shapes recorded
	/// with it.
	pub async fn track_index_shapes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		expected: &[(u16, ValueShape)],
	) -> Result<()> {
		let key = IndexShapesTrackedKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix,
		};
		self.set_key(&key, &()).await?;
		let tracked = IndexShapes::new(true, Vec::new(), Vec::new());
		let built = tracked.with(expected, ShapeRecorder::Build).unwrap_or_default();
		if !built.is_empty() {
			let key = IndexBuildShapesKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix,
			};
			self.set_key(&key, &built).await?;
		}
		self.note_shaped_index(ns, db, tb, ix);
		self.cache.insert(
			cache::tx::Lookup::Ish(ns, db, tb.as_str(), ix),
			cache::tx::Entry::Any(Arc::new(tracked.replacing(ShapeRecorder::Build, built))),
		);
		Ok(())
	}

	/// Note that this transaction has written index `ix`'s value shapes.
	fn note_shaped_index(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName, ix: IndexId) {
		let mut shaped = self.shaped_indexes.lock();
		if !shaped.iter().any(|(n, d, t, i)| *n == ns && *d == db && t == tb && *i == ix) {
			shaped.push((ns, db, tb.clone(), ix));
		}
	}

	/// Drop what the cache holds of the value shapes this transaction wrote,
	/// and of their tables' catalog versions, so both are read again as the
	/// store holds them after a rollback to a save point.
	///
	/// The indexes are kept for the rest of the transaction: a write in an
	/// enclosing scope survives an inner rollback, and an outer rollback that
	/// undoes it must drop the shapes read again in between.
	fn forget_shaped_indexes(&self) {
		for (ns, db, tb, ix) in self.shaped_indexes.lock().iter() {
			self.cache.remove(&cache::tx::Lookup::Ish(*ns, *db, tb.as_str(), *ix));
			self.cache.remove(&cache::tx::Lookup::Cv(*ns, *db, tb.as_str()));
		}
	}

	/// The bytes `tb`'s definition key holds as this transaction sees it,
	/// read past the transaction cache: what a table's catalog version is
	/// printed from and proven against ([`CatalogVersion::proven`]).
	async fn table_stored(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Option<Val>> {
		let key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.encode_key()?;
		Ok(self.tr.get(key, None).await?)
	}

	/// Stamp `tb`'s catalog entry with a fresh version.
	///
	/// Called by every provider method that writes a table definition, a field
	/// or an index — the one place the write happens, so no DDL path can forget
	/// it. The stamp is a fresh uuid, never an increment: a version this
	/// transaction mints is visible only to it until commit, and if it rolls back
	/// the value is gone with it and can never be reused by a later writer. The
	/// key is the table's own, so catalog writes to different tables never
	/// conflict over it. The transaction cache is updated in step, so a read of
	/// [`Self::catalog_version`] later in this transaction answers with the
	/// minted version.
	///
	/// The write is blind: nothing here reads the version first, so on its
	/// own it detects no concurrent DDL. That a version names one committed
	/// state of the table rests on every DDL path writing the table through
	/// [`Self::replace_tb`], whose locked read of the definition is what two
	/// DDL transactions on the same table conflict over.
	pub async fn bump_catalog_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		self.stamp_catalog_version(ns, db, tb, None).await.map(|_| ())
	}

	/// Stamp `tb`'s catalog entry with `version`, or a fresh one when
	/// `None`, answering the version stamped. A schema statement run again
	/// after a speculation of it stamps the version the speculation minted,
	/// which what was planned over the speculation read.
	///
	/// A defined table at a level above `Legacy` also gets the generation its rows are written
	/// in ([`Self::mint_generation`]): a stamp mints when the table has no readable head, or
	/// when the layout derived from the table's definition and fields, as the stamp sees them,
	/// differs from the head's. A table this transaction created has its conformance settled
	/// ([`Self::settle_birth_claims`]). The stamp is the one step every DDL path that changes a
	/// table ends with, so no path can leave a table without a generation, with the layout of an
	/// earlier field set or with the claims of one. Every DDL path's last stamp of a table
	/// therefore follows all its writes of that table's field and table keys, and follows every
	/// range delete under the table's root that is not meant to remove its generations or
	/// claims, because the delete removes the descriptor the stamp minted. A mint or settle that
	/// fails poisons the transaction, which then commits nothing: they run after the statement's
	/// own catalog writes, which a client-owned transaction would otherwise commit beside the
	/// failed statement.
	#[inline(never)]
	pub fn stamp_catalog_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<Uuid>,
	) -> OutOfLine<impl Future<Output = Result<Uuid>>> {
		OutOfLine::new(self.stamp_catalog_version_body(ns, db, tb, version))
	}

	async fn stamp_catalog_version_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<Uuid>,
	) -> Result<Uuid> {
		let stored = self.table_stored(ns, db, tb).await?;
		if let Some(table) = stored.as_deref()
			&& !self.write_level().is_legacy()
			&& let Err(e) = self.settle_generation(ns, db, tb, table).await
		{
			self.catalog_write_poisoned.store(true, Ordering::Relaxed);
			return Err(e);
		}
		let version = match version {
			Some(version) => CatalogVersion::stamped(version, stored.as_deref()),
			None => CatalogVersion::fresh(stored.as_deref()),
		};
		let key = CatalogVersionKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		self.set_key(&key, &version).await?;
		self.stamped_tables.lock().insert((ns, db, tb.clone()), version.0);
		self.cache.insert(
			cache::tx::Lookup::Cv(ns, db, tb.as_str()),
			cache::tx::Entry::Any(Arc::new(Some(version.0))),
		);
		if !self.tr.speculating() {
			self.catalog_stamps.fetch_add(1, std::sync::atomic::Ordering::Release);
		}
		Ok(version.0)
	}

	/// Gives `tb` the generation its rows are written in: mints the next generation of its ring
	/// when the table has no readable head, or when the layout derived from its definition and
	/// its field definitions ([`derive_layout`]) differs from the head's. The two layouts are
	/// compared by their canonical bytes, whatever migration the head carries. `stored` is the
	/// table's stored definition as the caller read and
	/// decoded it, `None` when this build does not decode it.
	///
	/// The table's layouts are read afresh, through this transaction, so the answer holds every
	/// generation the transaction has written and none a range delete has removed. A table whose
	/// ring this build refuses is refused with [`RecordLayoutError::RingRefused`], and nothing is
	/// minted on a guess. A table whose definitions this build cannot read
	/// ([`Self::derive_table_layout`] answers `None`) keeps a readable head as it is, and with no
	/// readable head gets a generation in the document layout, so that it can be written. Any
	/// other mint goes through [`Self::mint_with`].
	///
	/// What a mint writes depends only on what the transaction reads, so a speculation of a
	/// statement and its real run mint the same number and the same bytes.
	async fn mint_generation(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
	) -> Result<()> {
		let layouts = self.reload_table_layouts(ns, db, tb, None).await?;
		if let Some(refusal) = ring_refusal_error(&layouts) {
			return Err(refusal);
		}
		let head = match layouts.current() {
			Some((_, GenerationLayout::Readable(head))) => Some(head),
			_ => None,
		};
		let descriptor = match self.derive_table_layout(ns, db, tb, stored).await? {
			Some(descriptor) => descriptor,
			None if head.is_some() => return Ok(()),
			None => LayoutDescriptor::document(tb.clone()),
		};
		if let Some(head) = head
			&& head.has_layout_of(&descriptor)?
		{
			return Ok(());
		}
		self.mint_with(ns, db, tb, stored, &layouts, descriptor).await.map(|_| ())
	}

	/// Mints the generation after the head of `layouts`' ring, whatever the state of the current
	/// generation, with the layout derived from the table's definition and fields, or the
	/// document layout when derivation cannot read them.
	///
	/// `layouts` are the table's layouts as this transaction reads them, and `stored` is the
	/// table's stored definition, `None` when this build does not decode it. A stamp of a table
	/// mints only when the derived layout differs from the head's; this mints unconditionally.
	/// The mint writes the descriptor create-only, reads back and decodes the bytes it wrote,
	/// advances the table's ring and `!gn` mark, and holds the layouts it leaves, as every mint
	/// does. A ring this build cannot read ([`RecordLayoutError::RingRefused`]), a full ring
	/// ([`DatastoreError::GenerationRingFull`]) and a number that already has a layout key
	/// ([`DatastoreError::GenerationOccupied`]) are refused with nothing written.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn mint_next(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
		layouts: &TableLayouts,
	) -> Result<()> {
		let descriptor = self
			.derive_table_layout(ns, db, tb, stored)
			.await?
			.unwrap_or_else(|| LayoutDescriptor::document(tb.clone()));
		self.mint_with(ns, db, tb, stored, layouts, descriptor).await.map(|_| ())
	}

	/// The layout a generation minted for `tb` gives its rows, derived from its definition `stored`
	/// and its field definitions ([`derive_layout`]). `None` when derivation cannot run: `stored`
	/// is `None` (this build does not decode the definition), or one of the table's field keys or
	/// definitions is one this build cannot read or compile.
	///
	/// A table that may not pack ([`TableShape::may_pack`]) derives the document layout without
	/// reading a field. Field definitions are read through the transaction
	/// ([`Self::compiled_fields`]), so the answer holds the transaction's own writes. A storage
	/// error fails the derivation; a definition this build cannot read only answers `None`.
	async fn derive_table_layout(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
	) -> Result<Option<LayoutDescriptor>> {
		let Some(stored) = stored else {
			return Ok(None);
		};
		let shape = TableShape::of(tb, stored);
		if !shape.may_pack() {
			return Ok(Some(LayoutDescriptor::document(tb.clone())));
		}
		let Some(fields) = self.compiled_fields(ns, db, tb).await? else {
			return Ok(None);
		};
		let candidates: Vec<&FieldCandidate> =
			fields.values().map(|compiled| compiled.candidate.as_ref()).collect();
		Ok(Some(derive_layout(shape, &candidates)))
	}

	/// Mints the generation after the head of `layouts`' ring with `descriptor`, and holds the
	/// layouts it leaves.
	///
	/// `layouts` are the table's layouts as this transaction reads them, and `stored` is the
	/// table's stored definition, `None` when this build does not decode it. The generation is the
	/// ring's head plus one, wrapping from 65535 to 1. A table with no ring continues from its
	/// `!gn` mark, which survives the table's removal, so a number is never given to two
	/// descriptors under one table name. The mint:
	///
	/// - refuses a ring this build cannot read ([`RecordLayoutError::RingRefused`]);
	/// - refuses a full ring, where the next generation is the ring's oldest, with
	///   [`DatastoreError::GenerationRingFull`], and writes nothing;
	/// - refuses a number that already has a layout key with
	///   [`DatastoreError::GenerationOccupied`], which no mint of this build leaves behind;
	/// - writes the descriptor create-only, so a concurrent mint of the same number fails at commit
	///   and retries, on every backend;
	/// - reads back the bytes it wrote and decodes them as every other transaction will
	///   ([`decode_generation`]): the layouts it holds afterwards list that decoded descriptor, and
	///   a descriptor that does not read back as written is an encoder fault that fails the
	///   statement ([`RecordLayoutError::CurrentRefused`], [`RecordLayoutError::CurrentMissing`]);
	/// - writes the ring to `!mi` with a compare against the ring `layouts` holds (an absent `!mi`
	///   for a table with no ring), so a mint and a floor move of one table never both commit;
	/// - writes the new head to `!gn`.
	///
	/// A new ring starts with its first generation as the floor when the table was created by
	/// this transaction, is not a view and holds no record ([`Self::born_empty`]): nothing can
	/// have written a row the ring does not describe. Any other new ring starts with an unknown
	/// floor, 0. A ring in use keeps its floor. A table created by this transaction records that
	/// its creation started the ring it holds ([`Birth::minted_ring`]). The transaction's layout
	/// memo is updated with the minted generation and the layout epoch is advanced, so a writer
	/// that resolved the table's format earlier resolves it again. The answer is the generation
	/// minted.
	///
	/// Every mint runs one body, polled through the [`OutOfLine`] this returns.
	#[inline(never)]
	fn mint_with(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
		layouts: &TableLayouts,
		descriptor: LayoutDescriptor,
	) -> OutOfLine<impl Future<Output = Result<Generation>>> {
		OutOfLine::new(self.mint_with_body(ns, db, tb, stored, layouts, descriptor))
	}

	async fn mint_with_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
		layouts: &TableLayouts,
		descriptor: LayoutDescriptor,
	) -> Result<Generation> {
		if let Some(refusal) = ring_refusal_error(layouts) {
			return Err(refusal);
		}
		let ring = layouts.ring();
		let mark_key = GenerationMarkKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let mark = if ring.is_empty() {
			self.get_key(&mark_key, None).await?.map(|mark| mark.head())
		} else {
			ring.head()
		};
		let next = mark.map_or(Generation::FIRST, Generation::next);
		if ring.is_full() {
			let floor = ring.base().ok_or_else(|| {
				CatalogError::Layout(RecordLayoutError::Misnumbered {
					table: tb.clone(),
				})
			})?;
			return Err(DatastoreError::GenerationRingFull {
				table: tb.as_str().to_owned(),
				floor,
			}
			.into());
		}
		let first = if ring.is_empty() {
			stored
		} else {
			None
		};
		let advanced = if ring.is_empty() {
			let floor = match first {
				Some(stored) if self.born_empty(ns, db, tb, stored).await? => Some(next),
				_ => None,
			};
			GenerationRing::new(next, next, floor)
		} else {
			ring.advanced()
		};
		let minted_ring = advanced.ok_or_else(|| {
			CatalogError::Layout(RecordLayoutError::Misnumbered {
				table: tb.clone(),
			})
		})?;
		let layout_key = LayoutKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			generation: next.get(),
		};
		if self.exists_key(&layout_key, None).await? {
			return Err(DatastoreError::GenerationOccupied {
				table: tb.as_str().to_owned(),
				generation: next,
			}
			.into());
		}
		// The descriptor names the newest generation, up to its own, that carries a migration, so
		// the head's descriptor alone says which rows may need a step.
		let descriptor = LayoutDescriptor {
			migrated: if descriptor.migration.is_some() {
				Some(next)
			} else {
				layouts.newest_migration()
			},
			..descriptor
		};
		self.put_compare_key(&layout_key, &descriptor, None).await?;
		let written = self.get_key_raw(&layout_key, None).await?.ok_or_else(|| {
			CatalogError::Layout(RecordLayoutError::CurrentMissing {
				table: tb.clone(),
				generation: next,
			})
		})?;
		let minted = match decode_generation(tb, &written) {
			GenerationLayout::Readable(minted) => minted,
			GenerationLayout::Refused(cause) => {
				return Err(CatalogError::Layout(RecordLayoutError::CurrentRefused {
					table: tb.clone(),
					generation: next,
					cause,
				})
				.into());
			}
		};
		let migrations_key = TableMigrationsKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let migrations = TableMigrations::of(minted_ring).ok_or_else(|| {
			CatalogError::Layout(RecordLayoutError::Misnumbered {
				table: tb.clone(),
			})
		})?;
		let prior = TableMigrations::of(ring);
		self.put_compare_key(&migrations_key, &migrations, prior.as_ref()).await?;
		self.set_key(&mark_key, &GenerationMark::new(next)).await?;
		let minted = layouts.with_minted(minted_ring, minted).map_err(CatalogError::Layout)?;
		self.memoise_layouts(Arc::new(minted));
		self.layout_epoch.fetch_add(1, Ordering::Release);
		if let Some(stored) = first {
			self.note_ring_minted(ns, db, tb, stored);
		}
		Ok(next)
	}

	/// The birth this transaction recorded for `tb` under the table id `stored` carries, if it
	/// recorded one: a definition of another id is another incarnation of the name.
	fn birth_of(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: &StoredTableDefinition,
	) -> Option<Birth> {
		let births = self.births.lock();
		births
			.born
			.get(&(ns, db, tb.clone()))
			.copied()
			.filter(|birth| birth.table_id == stored.table_id)
	}

	/// Records that the first generation of `tb`'s ring was minted by the transaction that created
	/// the table, when `stored` is the definition that creation wrote.
	fn note_ring_minted(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: &StoredTableDefinition,
	) {
		let mut births = self.births.lock();
		if let Some(birth) = births.born.get_mut(&(ns, db, tb.clone()))
			&& birth.table_id == stored.table_id
		{
			birth.minted_ring = true;
		}
	}

	/// Whether `tb` is a table this transaction created, that is not a view and holds no record,
	/// as this transaction sees it: the one state in which no writer that has seen the table can
	/// have written a row the table's first generation does not describe.
	///
	/// `stored` is the table's stored definition. It must carry the id of the creation this
	/// transaction recorded ([`Births`]); a table whose definition was only rewritten, or a
	/// definition of another incarnation of the name, is not born here. The record probe reads
	/// one key through this transaction, so it sees the transaction's own writes.
	async fn born_empty(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: &StoredTableDefinition,
	) -> Result<bool> {
		if stored.view.is_some() || self.birth_of(ns, db, tb, stored).is_none() {
			return Ok(false);
		}
		Ok(!self.holds_record(ns, db, tb).await?)
	}

	/// Whether `tb` holds a record key, as this transaction sees it. The probe reads one key
	/// through this transaction, so it sees the transaction's own writes.
	async fn holds_record(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> Result<bool> {
		let records = RecordPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		Ok(!self.keys_raw(records, 1, 0, None).await?.is_empty())
	}

	/// Moves the floor of `tb` to `floor`, the oldest generation a row may still carry, and
	/// retires the descriptors of every generation before it.
	///
	/// The table's ring then starts at `floor`: its base and its known floor are `floor`, and its
	/// head is unchanged, so a floor never moves past the head. The descriptors of the generations
	/// that leave the ring, from the old base up to but excluding `floor` in ring order, are
	/// cleared with every version of their keys, so no store keeps a descriptor the ring does not
	/// names: a layout key is written once, and a soft delete would leave it readable at an earlier
	/// version. One range is cleared, or two when the retired generations wrap past 65535. A floor
	/// equal to the base retires nothing and writes the ring again. The table's catalog version is
	/// stamped.
	///
	/// The write takes the locked read of the table's definition, as [`Self::replace_tb`] does, so
	/// it conflicts with any concurrent DDL of the table, and writes `!mi` with a compare against
	/// the ring it read, so it conflicts with a concurrent mint on every backend. Refused, with
	/// nothing written:
	///
	/// - a table that is not defined ([`CatalogError::TbNotFound`]);
	/// - a table whose ring this build cannot read ([`RecordLayoutError::RingRefused`]);
	/// - a table with no generation ([`DatastoreError::NoGeneration`]);
	/// - a floor outside the ring ([`DatastoreError::FloorOutsideWindow`]).
	///
	/// A failure after the first write poisons the transaction, which then commits nothing.
	///
	/// The caller guarantees that no row of the table carries a generation before `floor`, the
	/// stamp 0 or the `revision` format, and that no transaction that could write one can still
	/// commit. A versioned read, at a version before the move, of a row in a retired generation is
	/// refused afterwards, because the clear removes the descriptor from history too.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn move_floor(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		floor: Generation,
	) -> Result<()> {
		let table_key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		if self.getu_key(&table_key).await?.is_none() {
			return Err(CatalogError::TbNotFound {
				name: tb.clone(),
			}
			.into());
		}
		let layouts = self.reload_table_layouts(ns, db, tb, None).await?;
		if let Some(refusal) = ring_refusal_error(&layouts) {
			return Err(refusal);
		}
		let ring = layouts.ring();
		if ring.is_empty() {
			return Err(DatastoreError::NoGeneration {
				table: tb.as_str().to_owned(),
			}
			.into());
		}
		let misnumbered = || {
			CatalogError::Layout(RecordLayoutError::Misnumbered {
				table: tb.clone(),
			})
		};
		// A ring that is not empty has a base and a head.
		let (base, head) = ring.base().zip(ring.head()).ok_or_else(misnumbered)?;
		let Some(moved) = ring.with_floor(floor) else {
			return Err(DatastoreError::FloorOutsideWindow {
				table: tb.as_str().to_owned(),
				floor,
				base,
				head,
			}
			.into());
		};
		let migrations = TableMigrations::of(moved).ok_or_else(misnumbered)?;
		let prior = TableMigrations::of(ring).ok_or_else(misnumbered)?;
		// The retired generations are those from the old base up to, and excluding, the new one.
		// They are one run of keys unless the ring wraps past 65535 between them, and then two:
		// the old base to the end of the layout keys, and the start of the layout keys to the new
		// floor. The parts are built before the first write.
		let whole: RawRange = LayoutPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?
		.into();
		let layout_key = |generation: Generation| {
			LayoutKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				generation: generation.get(),
			}
			.encode_key()
		};
		let mut retired: Vec<RawRange> = Vec::with_capacity(2);
		if floor.get() > base.get() {
			let (from, to) = (layout_key(base)?, layout_key(floor)?);
			retired.push(whole.part(from.as_slice(), to.as_slice()));
		} else if floor.get() < base.get() {
			let (lo, hi) = whole.bounds();
			let (from, to) = (layout_key(base)?, layout_key(floor)?);
			retired.push(whole.part(from.as_slice(), hi));
			retired.push(whole.part(lo, to.as_slice()));
		}
		let migrations_key = TableMigrationsKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let written = async {
			for part in retired {
				self.clrr(part).await?;
			}
			self.put_compare_key(&migrations_key, &migrations, Some(&prior)).await?;
			self.reload_table_layouts(ns, db, tb, None).await?;
			self.layout_epoch.fetch_add(1, Ordering::Release);
			self.bump_catalog_version(ns, db, tb).await?;
			Ok::<(), anyhow::Error>(())
		}
		.await;
		if written.is_err() {
			self.catalog_write_poisoned.store(true, Ordering::Relaxed);
		}
		written
	}

	/// Mints the generation of the migration `migration` of `tb`: the next generation of the
	/// table's ring, with the layout derived from the table's definition and its fields as this
	/// transaction sees them, carrying `migration`. Every row stamped before the generation passes
	/// through the migration's step when it is read. The answer is the generation minted.
	///
	/// The generation is minted whether or not the layout differs from the head's. The name is
	/// recorded under the table ([`MigrationRunKey`]) in the same transaction, so the table's
	/// history keeps that the name has run after the descriptor is retired. Refused, with nothing
	/// written:
	///
	/// - at a feature level that does not write the stored grammar
	///   ([`CatalogError::MigrationNeedsStoredFormat`]), where a row carries no stamp;
	/// - a table that is not defined ([`CatalogError::TbNotFound`]);
	/// - a name the table has already run ([`CatalogError::MigrationAlreadyRun`]);
	/// - a table whose ring this build cannot read ([`RecordLayoutError::RingRefused`]).
	///
	/// The generation's layout is the one the definitions this transaction holds when the call is
	/// made derive. A statement that stores field definitions afterwards has its own stamp mint
	/// the layout those definitions derive, when it differs from the head's, so a row written
	/// after the statement is stamped with that later generation and never passes through the
	/// step. A failure after the first write poisons the transaction, which then commits nothing.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			level = "trace",
			target = "surrealdb::core::kvs::tx",
			skip(self, migration)
		)
	)]
	pub async fn mint_tb_migration(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		migration: Migration,
	) -> Result<Generation> {
		if self.write_level().is_legacy() {
			return Err(CatalogError::MigrationNeedsStoredFormat {
				table: tb.as_str().to_owned(),
			}
			.into());
		}
		let name = migration.name.clone();
		let run_key = MigrationRunKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			name: Cow::Borrowed(name.as_str()),
		};
		if self.exists_key(&run_key, None).await? {
			return Err(CatalogError::MigrationAlreadyRun {
				table: tb.as_str().to_owned(),
				name,
			}
			.into());
		}
		let Some(table) = self.table_stored(ns, db, tb).await? else {
			return Err(CatalogError::TbNotFound {
				name: tb.clone(),
			}
			.into());
		};
		let stored = StoredTableDefinition::kv_decode_value(&table, ()).ok();
		let minted = async {
			let layouts = self.reload_table_layouts(ns, db, tb, None).await?;
			if let Some(refusal) = ring_refusal_error(&layouts) {
				return Err(refusal);
			}
			let run = MigrationRun {
				generation: Generation::FIRST,
				field: migration.field().map(str::to_owned),
			};
			let descriptor = self
				.derive_table_layout(ns, db, tb, stored.as_ref())
				.await?
				.unwrap_or_else(|| LayoutDescriptor::document(tb.clone()))
				.with_migration(migration);
			let generation =
				self.mint_with(ns, db, tb, stored.as_ref(), &layouts, descriptor).await?;
			self.set_key(
				&run_key,
				&MigrationRun {
					generation,
					..run
				},
			)
			.await?;
			Ok(generation)
		}
		.await;
		if minted.is_err() {
			self.catalog_write_poisoned.store(true, Ordering::Relaxed);
		}
		minted
	}

	/// The migrations `tb` has run, by name, as the table's permanent record holds them.
	pub async fn tb_migration_runs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Vec<(String, MigrationRun)>> {
		let range = MigrationRunPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		let mut runs = Vec::new();
		for (key, bytes) in self.getr_raw(range, None).await? {
			let name = MigrationRunKey::decode_key(&key)?.name.into_owned();
			runs.push((name, MigrationRun::kv_decode_value(&bytes, ())?));
		}
		Ok(runs)
	}

	/// The newest generation of `tb`'s ring that may carry a migration
	/// ([`TableLayouts::newest_migration`]), as this transaction sees it. `None` when the table has
	/// no ring or no generation of it carries one.
	pub async fn newest_tb_migration(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Option<Generation>> {
		Ok(self.table_layouts(ns, db, tb, None).await?.newest_migration())
	}

	/// What the table migration job will do for table `tb`, as this transaction sees it: the job's
	/// progress with every request not yet folded into it applied in the order made
	/// ([`TableMigrationRequest::apply`]). `None` when the table has neither.
	///
	/// A plain read: no write of the job, nor of another statement, conflicts with this
	/// transaction because of it.
	pub async fn get_tb_migration_job(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Option<TableMigrationState>> {
		let progress = self.get_key(&Self::migration_job_key(ns, db, tb), None).await?;
		let requests = self.getr(Self::migration_requests(ns, db, tb).range()?, None).await?;
		Ok(requests.iter().fold(progress, |state, (_, request)| request.apply(state)))
	}

	/// Up to `limit` of the requests made of the table migration job for table `tb` that it has
	/// not yet folded into its progress, with their keys, in the order made.
	pub async fn tb_migration_requests(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		limit: u32,
	) -> Result<Vec<(Vec<u8>, TableMigrationRequest)>> {
		self.scan(Self::migration_requests(ns, db, tb).range()?, limit, 0, None).await
	}

	/// Queue table `tb` for the rewrite job, in the transaction that mints a migration of it.
	///
	/// The request is a key no other transaction writes ([`Self::request_tb_migration`]). A page
	/// of the job whose snapshot predates the commit leaves it in place, so the job reads the new
	/// migration on a later page and starts another lap, whichever of the two commits first.
	pub async fn queue_tb_rewrite(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		self.request_tb_migration(ns, db, tb, TableMigrationRequest::Rewrite).await
	}

	/// Queue `field` of table `tb` for verification. The job restarts the lap when it folds the
	/// request, and reads every row from the start after every transaction older than its fold has
	/// ended.
	pub async fn queue_tb_verification(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		field: VerifiedField,
	) -> Result<()> {
		self.request_tb_migration(ns, db, tb, TableMigrationRequest::Verify(field)).await
	}

	/// Queue table `tb` for the check for rows holding a top-level key none of the names in
	/// `declared` (sorted) names, in place of any check queued before. The job restarts the lap
	/// when it folds the request, and reads every row from the start after every transaction older
	/// than its fold has ended.
	pub async fn queue_tb_undeclared(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		declared: Vec<String>,
	) -> Result<()> {
		self.request_tb_migration(ns, db, tb, TableMigrationRequest::Undeclared(declared)).await
	}

	/// Run the check for undeclared keys queued on table `tb`, finished or not, again: when
	/// `declared` (sorted) is not the set of names it judges rows against, or when `rows_changed`,
	/// since a step clears keys from the rows it judged. A table with no check queues none.
	///
	/// The check is read as the job will hold it ([`Self::get_tb_migration_job`]), without a
	/// lock, so it conflicts with no page of the job.
	pub async fn recheck_tb_undeclared(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		declared: Vec<String>,
		rows_changed: bool,
	) -> Result<()> {
		let Some(state) = self.get_tb_migration_job(ns, db, tb).await? else {
			return Ok(());
		};
		match &state.undeclared {
			Some(check) if rows_changed || check.declared != declared => {
				self.queue_tb_undeclared(ns, db, tb, declared).await
			}
			_ => Ok(()),
		}
	}

	/// Drop the check for undeclared keys queued on table `tb`, if this transaction sees one: a
	/// `SCHEMALESS` table refuses no key, so the check reports nothing a write refuses. The job
	/// deletes the progress when nothing else is left in it.
	pub async fn drop_tb_undeclared(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		let queued = self
			.get_tb_migration_job(ns, db, tb)
			.await?
			.is_some_and(|state| state.undeclared.is_some());
		if !queued {
			return Ok(());
		}
		self.request_tb_migration(ns, db, tb, TableMigrationRequest::DropUndeclared).await
	}

	/// Restart the verification under way on table `tb`, if this transaction sees one, for a
	/// write that stores a record without checking it (`OPTION IMPORT`) or a change of the
	/// definitions a pass judges a field against.
	///
	/// The request is read and written without a lock, so it conflicts with no page of the job. A
	/// pass whose last page did not see it ends with the counts it judged, as it would had the
	/// write committed after it. A pass that makes a claim instead conflicts with the forfeit of
	/// the table's claims that every caller makes in the same transaction
	/// ([`Self::forfeit_conformance`]), so no claim is made over a row the pass could not judge.
	/// A transaction that sees no verification under way began before the request for it
	/// committed, so before the job folded it, and the job's lap waits for every transaction
	/// older than the fold to end before it reads a row.
	pub async fn restart_tb_verification(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		let Some(state) = self.get_tb_migration_job(ns, db, tb).await? else {
			return Ok(());
		};
		if !state.verifying() {
			return Ok(());
		}
		self.request_tb_migration(ns, db, tb, TableMigrationRequest::Restart).await
	}

	/// Write `request` of the table migration job for table `tb` under a fresh version-7 id, and
	/// wake the job when this transaction commits.
	///
	/// No other transaction writes the key: the job deletes it only once it has read it, and
	/// therefore only once this transaction has committed. The ids one process mints sort in the
	/// order they were minted, so the job folds one node's requests in the order they were made.
	/// The key is inserted rather than upserted, so an id that did collide is refused instead of
	/// replacing another request.
	async fn request_tb_migration(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		request: TableMigrationRequest,
	) -> Result<()> {
		let key = TableMigrationRequestKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			uid: Uuid::now_v7(),
		};
		self.put_key(&key, &request).await?;
		self.trigger_table_migration();
		Ok(())
	}

	fn migration_job_key(ns: NamespaceId, db: DatabaseId, tb: &TableName) -> TableMigrationKey<'_> {
		TableMigrationKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
	}

	fn migration_requests(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> TableMigrationRequestTbPrefix<'_> {
		TableMigrationRequestTbPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
	}

	/// Whether `tb` holds any record, read from its record range in this transaction.
	pub async fn tb_has_records(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		self.holds_record(ns, db, tb).await
	}

	/// The compatibility level a database created in this store without one takes: the store's
	/// [`StoreDefaults`], or [`StoreDefaults::CURRENT`]'s where none is stored.
	pub async fn default_compatibility(&self) -> Result<DatabaseCompatibility> {
		let defaults = self.get_key(&StoreDefaultsKey {}, None).await?;
		compatibility_of(&defaults.unwrap_or(StoreDefaults::CURRENT))
	}

	/// The compatibility level of the row an implicit creation writes for the database `db` of
	/// `ns`: [`DatabaseCompatibility::LEVEL_3`] when the database already holds table definitions,
	/// which were defined under the rules of a database that had no level, and the store's default
	/// ([`Self::default_compatibility`]) otherwise. A store whose feature level does not write the
	/// stored grammar has no way to record another level, so it answers level 3.
	pub async fn implicit_compatibility(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<DatabaseCompatibility> {
		if self.write_level().is_legacy() {
			return Ok(DatabaseCompatibility::LEVEL_3);
		}
		let tables = TablePrefix {
			ns,
			db,
		}
		.range()?;
		if !self.keys_raw(tables, 1, 0, None).await?.is_empty() {
			return Ok(DatabaseCompatibility::LEVEL_3);
		}
		self.default_compatibility().await
	}

	/// Set the compatibility level a database created in this store without one takes. Databases
	/// that already exist keep theirs.
	pub async fn set_default_compatibility(&self, level: DatabaseCompatibility) -> Result<()> {
		self.set_key(
			&StoreDefaultsKey {},
			&StoreDefaults {
				compatibility: level.get(),
			},
		)
		.await
	}

	/// The conformance record of `tb` as this transaction sees it, or as of `version`.
	///
	/// `None` when the record is absent, when this transaction removed it, and when it is there and
	/// this build does not decode it: none of those states a claim. A claim says nothing about a
	/// row stamped 0 or written in `revision`, and a consumer checks it against the field's
	/// current definition ([`surrealdb_catalog::FieldClaim::holds_for`]) and keeps the check of
	/// each row's own generation.
	///
	/// A current read is held until a range delete over the table's keys or a save point rollback
	/// that undid a write of the record clears it. A versioned read is never held.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn table_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> Result<Option<Arc<TableConformance>>> {
		let held = match version {
			None => self.held_conformance(ns, db, tb).await?,
			Some(_) => self.read_conformance(ns, db, tb, version).await?,
		};
		Ok(match held {
			HeldConformance::Claims(claims) => Some(claims),
			HeldConformance::Absent | HeldConformance::Forfeited | HeldConformance::Unreadable => {
				None
			}
		})
	}

	/// Removes `tb`'s conformance record, whatever it holds, so that the table states no claim.
	///
	/// The delete is conditional on the bytes this transaction reads at the key, an absent record
	/// included, so it conflicts with any concurrent write or removal of the record on every
	/// backend: a promotion that read the claims before this forfeit cannot commit beside it. It is
	/// made once per transaction and table while nothing undoes it, and is answered from the
	/// transaction's own view after that.
	///
	/// A forfeit is bookkeeping the statement did not ask for, so it is a counted state change and
	/// takes no slot of the write-cardinality guard. It awaits no encoder and no writer that
	/// notices definitions ([`Self::restamp_defined`]), which is what lets the writers' hooks call
	/// it.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn forfeit_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		self.forfeit_conformance_sealed(ns, db, tb).await
	}

	#[inline(never)]
	fn forfeit_conformance_sealed(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.forfeit_conformance_body(ns, db, tb))
	}

	async fn forfeit_conformance_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		if matches!(self.memoised_conformance(ns, db, tb), Some(HeldConformance::Forfeited)) {
			return Ok(());
		}
		let _changed = self.state_change();
		let key = TableConformanceKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.encode_key()?;
		let key_bytes = key.len() as u64;
		let prior = self.tr.get(key.as_borrowed(), None).await?;
		self.tr.delc(key, prior.as_deref()).await?;
		self.metrics.record_del(1, key_bytes);
		self.hold_conformance(ns, db, tb, HeldConformance::Forfeited);
		self.layout_epoch.fetch_add(1, Ordering::Release);
		Ok(())
	}

	/// Adds, or replaces, the claim that `field` of `tb` conforms, when the field's current
	/// definition is the one with digest `definition`
	/// ([`surrealdb_catalog::definition_digest`]).
	///
	/// Refused with [`DatastoreError::ConformanceStale`] when the field is not defined or its
	/// definition has another digest, and with [`CatalogError::TbNotFound`] for a table that is not
	/// defined. The write takes the locked read of the table's definition, as [`Self::replace_tb`]
	/// does, so it conflicts with concurrent DDL of the table, and writes the record conditional on
	/// the bytes it read, so it conflicts with a concurrent forfeit or promotion on every backend.
	/// The table's catalog version is not stamped: a snapshot keyed by it never holds the record.
	///
	/// The caller guarantees that every row a committed or concurrent transaction can write at a
	/// generation of the table's ring holds, at every occupied path of the field, a value the
	/// definition coerces. A claim only ever answers which code to run, never whether to check a
	/// row's generation.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn promote_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		field: &str,
		definition: i64,
		conformance: Conformance,
	) -> Result<()> {
		let table_key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		if self.getu_key_raw(&table_key).await?.is_none() {
			return Err(CatalogError::TbNotFound {
				name: tb.clone(),
			}
			.into());
		}
		let field_key = FieldKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			fd: Cow::Borrowed(field),
		};
		let current = match self.get_key(&field_key, None).await? {
			Some(stored) => Some(definition_digest(&stored)?),
			None => None,
		};
		if current != Some(definition) {
			return Err(DatastoreError::ConformanceStale {
				table: tb.as_str().to_owned(),
				field: field.to_owned(),
			}
			.into());
		}
		let claim = FieldClaim {
			field: field.to_owned(),
			definition,
			conformance,
		};
		let claimed = match self.held_conformance(ns, db, tb).await? {
			HeldConformance::Claims(claims) => claims.with_claim(claim)?,
			HeldConformance::Absent | HeldConformance::Forfeited | HeldConformance::Unreadable => {
				TableConformance::default().with_claim(claim)?
			}
		};
		self.write_conformance(ns, db, tb, claimed).await
	}

	/// What this transaction holds of `tb`'s conformance record, if it holds anything.
	fn memoised_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Option<HeldConformance> {
		self.conformance_memo.lock().get(&(ns, db)).and_then(|tables| tables.get(tb)).cloned()
	}

	/// Holds `held` as this transaction's view of `tb`'s conformance record, with the table's span.
	fn hold_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		held: HeldConformance,
	) {
		self.hold_span(ns, db, tb);
		self.conformance_memo.lock().entry((ns, db)).or_default().insert(tb.clone(), held);
	}

	/// `tb`'s conformance record read from storage, at `version` when given, bypassing what this
	/// transaction holds.
	///
	/// A record this build cannot decode is [`HeldConformance::Unreadable`], which states no claim:
	/// the record never licenses anything, so a record that cannot be read is the same as none.
	/// Only a storage error fails the read.
	async fn read_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> Result<HeldConformance> {
		let key = TableConformanceKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let Some(bytes) = self.get_key_raw(&key, version).await? else {
			return Ok(HeldConformance::Absent);
		};
		match TableConformance::kv_decode_value(&bytes, ()) {
			Ok(claims) => Ok(HeldConformance::Claims(Arc::new(claims))),
			Err(_) => {
				tracing::debug!(
					target: "surrealdb::core::kvs::tx",
					table = tb.as_str(),
					"a conformance record this build cannot read states no claim"
				);
				Ok(HeldConformance::Unreadable)
			}
		}
	}

	/// `tb`'s conformance record as this transaction sees it: what it holds, or else one read of
	/// the record, which it then holds.
	async fn held_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<HeldConformance> {
		if let Some(held) = self.memoised_conformance(ns, db, tb) {
			return Ok(held);
		}
		let held = self.read_conformance(ns, db, tb, None).await?;
		self.hold_conformance(ns, db, tb, held.clone());
		Ok(held)
	}

	/// Writes `value` as `tb`'s conformance record, conditional on the bytes this transaction reads
	/// there, and holds it. The transaction takes `value` over as the record it holds.
	///
	/// The write is bookkeeping the statement did not ask for: a counted state change that takes no
	/// slot of the write-cardinality guard. It is made through the transactor and notices no
	/// definition, and it encodes, so nothing reached from [`Self::restamp_defined`] may await it.
	/// The encoding carries the floor of the stored grammar, which admits the stored-format ledger
	/// entry in this transaction.
	async fn write_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		value: TableConformance,
	) -> Result<()> {
		let _changed = self.state_change();
		let key = TableConformanceKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.encode_key()?;
		let prior = self.tr.get(key.as_borrowed(), None).await?;
		let bytes = self.encode_with_prior(&value, prior.as_deref()).await?;
		let key_bytes = key.len() as u64;
		let value_bytes = bytes.len() as u64;
		self.tr.putc(key, bytes, prior).await?;
		self.metrics.record_put(key_bytes, value_bytes);
		self.hold_conformance(ns, db, tb, HeldConformance::Claims(Arc::new(value)));
		self.layout_epoch.fetch_add(1, Ordering::Release);
		Ok(())
	}

	/// Makes `claims` the whole of `tb`'s conformance record: no claim removes the record
	/// ([`Self::forfeit_conformance`]), claims equal to the ones this transaction holds write
	/// nothing, and any others replace the record.
	#[inline(never)]
	fn replace_conformance(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		claims: Vec<FieldClaim>,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.replace_conformance_body(ns, db, tb, claims))
	}

	async fn replace_conformance_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		claims: Vec<FieldClaim>,
	) -> Result<()> {
		if claims.is_empty() {
			return self.forfeit_conformance(ns, db, tb).await;
		}
		if let HeldConformance::Claims(held) = self.held_conformance(ns, db, tb).await?
			&& held.claims() == claims.as_slice()
		{
			return Ok(());
		}
		self.write_conformance(ns, db, tb, TableConformance::new(claims)?).await
	}

	/// Settles the conformance of a table this transaction created, once its definition and fields
	/// are written: the table's field definitions then describe every row it will hold from its
	/// first generation on, because it has none yet.
	///
	/// Acts only on a table that [`Births`] lists as unsettled, and only when `stored`, the table's
	/// decoded definition, carries the id of the creation recorded. A table whose definition
	/// carries another id is not this transaction's birth: the hooks have already forfeited
	/// whatever its changes required, and nothing is written. For the table that is the birth, the
	/// record written is:
	///
	/// - none, when the definition does not decode (`stored` is `None`), when the ring the table
	///   holds predates the birth ([`Birth::minted_ring`]) so that rows an earlier incarnation left
	///   may sit under it, when the table is a view or a lightweight relation, when it holds a
	///   record key, or when a field definition does not decode or compile;
	/// - otherwise the claims [`surrealdb_catalog::birth_claims_of`] takes from the field
	///   definitions.
	///
	/// A field definition is compiled once for as long as its stored bytes stand
	/// ([`Births::compiled`]), so the settles of a transaction that defines many fields read the
	/// definitions again and compile only the ones that changed. A table whose fields change again
	/// in this transaction is listed as unsettled again by the hooks and is settled again at its
	/// next stamp. A storage error fails the settle; a definition this build cannot read only
	/// withholds claims.
	async fn settle_birth_claims(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		stored: Option<&StoredTableDefinition>,
	) -> Result<()> {
		let key = (ns, db, tb.clone());
		let birth = {
			let mut births = self.births.lock();
			if births.unsettled.is_empty() {
				return Ok(());
			}
			if !births.unsettled.remove(&key) {
				return Ok(());
			}
			match births.born.get(&key) {
				Some(birth) => *birth,
				None => return Ok(()),
			}
		};
		let Some(stored) = stored else {
			return self.replace_conformance(ns, db, tb, Vec::new()).await;
		};
		if stored.table_id != birth.table_id {
			return Ok(());
		}
		if !birth.minted_ring
			|| stored.view.is_some()
			|| matches!(&stored.table_type, TableType::Relation(r) if r.lightweight)
		{
			return self.replace_conformance(ns, db, tb, Vec::new()).await;
		}
		if self.holds_record(ns, db, tb).await? {
			return self.replace_conformance(ns, db, tb, Vec::new()).await;
		}
		let Some(fields) = self.compiled_fields(ns, db, tb).await? else {
			return self.withhold_for_undecodable_fields(ns, db, tb).await;
		};
		let claims = {
			let candidates: Vec<&FieldCandidate> =
				fields.values().map(|compiled| compiled.candidate.as_ref()).collect();
			birth_claims_of(&candidates)
		};
		self.replace_conformance(ns, db, tb, claims).await
	}

	/// The field definitions of `tb`, as this transaction reads them and compiled, by the `fd`
	/// component of each field's key. `None` when one of the table's field keys does not decode or
	/// one of its definitions does not decode or compile: a build that cannot read a definition
	/// states nothing from the table's fields.
	///
	/// The definitions are read raw through the transaction, never through the field cache, which
	/// the catalog's raw field writes do not invalidate, so the answer holds every write of this
	/// transaction. A definition is compiled once for as long as its stored bytes stand
	/// ([`Births::compiled`]): a read compiles only the definitions whose bytes changed since the
	/// last one. A storage error fails the read.
	#[inline(never)]
	fn compiled_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> OutOfLine<impl Future<Output = Result<Option<Arc<CompiledFields>>>>> {
		OutOfLine::new(self.compiled_fields_body(ns, db, tb))
	}

	async fn compiled_fields_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Option<Arc<CompiledFields>>> {
		let fields = FieldPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		let rows = self.getr_raw(fields, None).await?;
		let key = (ns, db, tb.clone());
		let held = self.births.lock().compiled.remove(&key);
		let mut previous: CompiledFields = held.map(Arc::unwrap_or_clone).unwrap_or_default();
		let mut current: CompiledFields = HashMap::with_capacity(rows.len());
		for (field_key, value) in rows {
			let Ok(decoded) = FieldKey::decode_key(&field_key) else {
				return Ok(None);
			};
			let fd = decoded.fd.into_owned();
			let compiled = match previous.remove(&fd) {
				Some(reused) if *reused.bytes == *value.as_slice() => reused,
				_ => {
					let candidate = StoredFieldDefinition::kv_decode_value(&value, ())
						.ok()
						.and_then(|definition| FieldCandidate::compile(&fd, &definition));
					match candidate {
						Some(candidate) => CompiledField {
							bytes: Arc::from(value),
							candidate: Arc::new(candidate),
						},
						None => return Ok(None),
					}
				}
			};
			current.insert(fd, compiled);
		}
		let current = Arc::new(current);
		self.births.lock().compiled.insert(key, Arc::clone(&current));
		Ok(Some(current))
	}

	/// Removes the conformance record of a born table one of whose field definitions does not
	/// decode or compile: a table whose definitions this build cannot read claims nothing.
	async fn withhold_for_undecodable_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		tracing::debug!(
			target: "surrealdb::core::kvs::tx",
			table = tb.as_str(),
			"a table whose field definitions do not decode claims nothing"
		);
		self.replace_conformance(ns, db, tb, Vec::new()).await
	}

	/// Gives `tb` the generation its rows are written in ([`Self::mint_generation`]) and settles
	/// the conformance of a table this transaction created ([`Self::settle_birth_claims`]).
	///
	/// The table's stored definition is decoded once for both. The settle follows the mint because
	/// a claim is only sound under the ring the birth minted.
	async fn settle_generation(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		table: &[u8],
	) -> Result<()> {
		let stored = StoredTableDefinition::kv_decode_value(table, ()).ok();
		self.mint_generation(ns, db, tb, stored.as_ref()).await?;
		self.settle_birth_claims(ns, db, tb, stored.as_ref()).await
	}

	/// The version of `object` as this transaction sees it, or `None` for an
	/// object no catalog write by this build has stamped.
	///
	/// A table answers [`Self::catalog_version`]. Every other kind answers the
	/// value under its `!ov` key, which the transaction's write methods mint
	/// afresh whenever a write or deletion of the object's definition key
	/// changes what it holds ([`Self::restamp_defined`]), so no DDL path can
	/// change a definition without moving its version. A removal mints too, leaving a tombstone
	/// value behind, rather than deleting the version: an object that was
	/// never stamped and one that has been removed would otherwise both read
	/// `None`, and a reader that held `None` for the first would not see the
	/// second as a change.
	///
	/// `None` names no state. An object written only by a build that predates
	/// these versions reads `None` whatever its definition, as does one such
	/// a build has rewritten or removed since its last stamp: a stored version
	/// is answered only where it is proven against the definition key
	/// ([`CatalogVersion::proven`]). A reader must not take two `None` reads
	/// to mean the same definition; only equal `Some` values do.
	///
	/// # What makes a version name one committed state
	///
	/// Each of these objects is one definition key, and every write of that
	/// key writes the version beside it in the same transaction. A commit is
	/// atomic, so the pair a reader's snapshot holds always comes from one
	/// transaction, whatever the backend's conflict rules: two concurrent
	/// writers of the same object either conflict, or both commit and the
	/// later commit replaces both keys. No locked read of the definition is
	/// needed for that, unlike a table, whose state spans its definition,
	/// fields and indexes, and whose writers serialise through
	/// [`Self::replace_tb`]'s locked read so that two DDL transactions
	/// writing different keys of one table cannot both commit.
	///
	/// Storage is read once per object per transaction, as for a table: the
	/// answer, `None` included, is held in the transaction cache, and a stamp
	/// replaces it with the version it mints.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub async fn catalog_object_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		object: CatalogObject<'_>,
	) -> Result<Option<Uuid>> {
		let (kind, name, sub) = match object.versioned_as() {
			Ok(versioned) => versioned,
			Err(tb) => return self.catalog_version(ns, db, tb).await,
		};
		match kind {
			CatalogObjectKind::Function => {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Function(ns, db, name))
			}
			CatalogObjectKind::Param => {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Param(ns, db, name))
			}
			_ => {}
		}
		let qey = cache::tx::Lookup::Ov(ns, db, kind, name, sub);
		if let Some(entry) = self.cache.get(&qey) {
			return Ok(*entry.try_into_type::<Option<Uuid>>()?);
		}
		let key = ObjectVersionKey {
			ns,
			db,
			kind,
			name: Cow::Borrowed(name),
			sub: Cow::Borrowed(sub),
		};
		let version = match self.get_key(&key, None).await? {
			Some(stamped) => {
				let defined = DefinedObject::key_of(ns, db, kind, name, sub)?;
				stamped.proven(self.tr.get(defined, None).await?.as_deref())
			}
			None => None,
		};
		self.cache.insert(qey, cache::tx::Entry::Any(Arc::new(version)));
		Ok(version)
	}

	/// Stamp `object` with a fresh version, answering the version stamped.
	///
	/// A table is stamped through [`Self::stamp_catalog_version`]. Any other
	/// object is stamped by every write of its definition already, so a caller
	/// needs this only to move a version without writing the definition,
	/// which is always safe: a fresh version naming an unchanged definition
	/// costs a reader a fallback, never a wrong answer.
	pub async fn bump_catalog_object_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		object: CatalogObject<'_>,
	) -> Result<Uuid> {
		match object.versioned_as() {
			Ok((kind, name, sub)) => self.stamp_object_version(ns, db, kind, name, sub).await,
			Err(tb) => self.stamp_catalog_version(ns, db, tb, None).await,
		}
	}

	/// Write a fresh version under `object`'s `!ov` key and hold it in the
	/// transaction cache. Written through the transactor directly rather than
	/// [`Self::set_key`], whose own restamp would otherwise recurse here.
	async fn stamp_object_version(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		kind: CatalogObjectKind,
		name: &str,
		sub: &str,
	) -> Result<Uuid> {
		let defined = DefinedObject::key_of(ns, db, kind, name, sub)?;
		let version = CatalogVersion::fresh(self.tr.get(defined, None).await?.as_deref());
		let key = ObjectVersionKey {
			ns,
			db,
			kind,
			name: Cow::Borrowed(name),
			sub: Cow::Borrowed(sub),
		}
		.encode_key()?;
		let val = version.kv_encode_value()?;
		let _changed = self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		self.tr.set(key, val).await?;
		self.metrics.record_set(key_bytes, value_bytes);
		self.cache.insert(
			cache::tx::Lookup::Ov(ns, db, kind, name, sub),
			cache::tx::Entry::Any(Arc::new(Some(version.0))),
		);
		if !self.tr.speculating() {
			self.object_stamps.fetch_add(1, std::sync::atomic::Ordering::Release);
		}
		Ok(version.0)
	}

	/// The change a write of `val` at `key` makes to a catalog definition, or a deletion of `key`
	/// when `val` is `None`, for [`Self::restamp_defined`] to follow once the write succeeds.
	///
	/// A table's definition key answers a [`TableChange`] when this transaction has stamped the
	/// table, whatever the write changed, and otherwise only when the write creates, rewrites or
	/// removes the definition. A write that leaves the definition as it is leaves the table
	/// unnoticed unless it is stamped. A table's field definition key answers
	/// [`Defined::Field`] when the write changes the bytes it holds, and is examined only for a
	/// deletion or a write that read its key. Any other key answers the object whose definition it
	/// is, and `None` when it is no versioned object's definition, and also when the write leaves
	/// the definition as it is (the same bytes written again, or a definition that is not there
	/// deleted), so a statement that rewrites a definition unchanged leaves its version standing,
	/// and two runs of it leave the catalog as one does. Only a key that is a definition costs the
	/// read of what it holds.
	///
	/// `prior` is what the caller has already read at `key`: `Some(held)` where
	/// `held` is the stored bytes, or `None` for a key that holds nothing; and
	/// `None` for a caller that has read nothing, which leaves this to read the
	/// key when it must. A levelled write reads the key to encode against it, so
	/// it passes what it read and the key is read once.
	#[inline(never)]
	fn changed_definition(
		&self,
		key: &Key<'_>,
		val: Option<&[u8]>,
		prior: Option<Option<&[u8]>>,
	) -> OutOfLine<impl Future<Output = Result<Option<Defined>>>> {
		if root_definition_at(key) {
			self.touches_catalog();
		}
		OutOfLine::new(self.changed_definition_body(key, val, prior))
	}

	async fn changed_definition_body(
		&self,
		key: &Key<'_>,
		val: Option<&[u8]>,
		prior: Option<Option<&[u8]>>,
	) -> Result<Option<Defined>> {
		if let Some(table) = table_definition_at(key) {
			let read;
			let prior = match prior {
				Some(held) => held,
				None => {
					read = self.tr.get(key.as_borrowed(), None).await?;
					read.as_deref()
				}
			};
			let stamped = {
				let stamped = self.stamped_tables.lock();
				!stamped.is_empty() && stamped.contains_key(&table)
			};
			let change = TableDefinitionChange::of(prior, val);
			if !stamped && matches!(change, TableDefinitionChange::Unchanged) {
				return Ok(None);
			}
			// A table's definition may be rewritten without a new catalog
			// version, as a live query's registration rewrites it.
			self.touches_catalog();
			return Ok(Some(Defined::Table(TableChange {
				table,
				stamped,
				change,
			})));
		}
		// Field definitions are written levelled, which reads the key, so a write of one passes
		// what it read. A raw write of any other key under a table, a record's above all, passes
		// nothing and is not examined: only a deletion, or a write that read its key, can be a
		// field definition.
		if (val.is_none() || prior.is_some())
			&& let Some(table) = field_definition_table(key)
		{
			let read;
			let prior = match prior {
				Some(held) => held,
				None => {
					read = self.tr.get(key.as_borrowed(), None).await?;
					read.as_deref()
				}
			};
			return Ok((prior != val).then_some(Defined::Field(table)));
		}
		let Some(defined) = DefinedObject::at(key)? else {
			return Ok(None);
		};
		let read;
		let prior = match prior {
			Some(held) => held,
			None => {
				read = self.tr.get(key.as_borrowed(), None).await?;
				read.as_deref()
			}
		};
		Ok((prior != val).then_some(Defined::Object(defined)))
	}

	/// Print the version this transaction last stamped `table` with again,
	/// from the definition as it now stands: a version names the state the
	/// transaction commits, so a definition written after its stamp is what
	/// the version is proven against. Uncounted, as the version is unmoved.
	async fn reprint_stamped_table(
		&self,
		(ns, db, tb): (NamespaceId, DatabaseId, TableName),
	) -> Result<()> {
		let Some(version) = self.stamped_tables.lock().get(&(ns, db, tb.clone())).copied() else {
			return Ok(());
		};
		let stored = self.table_stored(ns, db, &tb).await?;
		let key = CatalogVersionKey {
			ns,
			db,
			tb: Cow::Borrowed(&tb),
		}
		.encode_key()?;
		let val = CatalogVersion::stamped(version, stored.as_deref()).kv_encode_value()?;
		let _changed = self.reserve_write_slot()?;
		self.tr.set(key, val).await?;
		Ok(())
	}

	/// Mint a fresh version for the object a write method just wrote or
	/// deleted the definition of, as [`Self::changed_definition`] found it.
	///
	/// Every single-key write and delete method of the transaction calls this
	/// after its write succeeds, which is what lets the versions of these
	/// objects stand without any DDL path stamping them. Range deletes do
	/// not: the only range that spans these definitions is a whole
	/// database's, which takes their versions with it, and a database's id is
	/// never reused.
	///
	/// A table is not minted a version here: a table this transaction has
	/// stamped prints its stamped version again from the definition as it now
	/// stands, and the change is noted ([`Self::note_table_change`]). A change to
	/// one of a table's field definitions is noted too
	/// ([`Self::note_field_change`]). A noting that fails poisons the
	/// transaction, because the definition is already written.
	#[inline(never)]
	fn restamp_defined(
		&self,
		defined: Option<Defined>,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.restamp_defined_body(defined))
	}

	async fn restamp_defined_body(&self, defined: Option<Defined>) -> Result<()> {
		match defined {
			Some(Defined::Object(defined)) => {
				self.stamp_object_version(
					defined.ns,
					defined.db,
					defined.kind,
					&defined.name,
					&defined.sub,
				)
				.await?;
			}
			Some(Defined::Table(change)) => {
				if change.stamped {
					self.reprint_stamped_table(change.table.clone()).await?;
				}
				if let Err(e) = self.note_table_change(change).await {
					self.catalog_write_poisoned.store(true, Ordering::Relaxed);
					return Err(e);
				}
			}
			Some(Defined::Field(table)) => {
				if let Err(e) = self.note_field_change(table).await {
					self.catalog_write_poisoned.store(true, Ordering::Relaxed);
					return Err(e);
				}
			}
			None => {}
		}
		Ok(())
	}

	/// Takes note of what a write of a table's definition changed, as
	/// [`Self::changed_definition`] classified it.
	///
	/// A created table is a birth ([`Births`]): it is recorded with the id the write gave it, and
	/// its conformance is left for the next stamp to settle. A creation whose definition does not
	/// decode records no birth, and drops any an earlier creation of the name left. A rewrite that
	/// makes the table a view, a lightweight relation or `SCHEMAFULL`, or leaves a definition this
	/// build cannot decode, forfeits the table's conformance, as does the removal of the
	/// definition, and leaves a born table for the next stamp to settle again. Any other rewrite
	/// changes no row's relation to the field definitions, and leaves the conformance as it is.
	///
	/// A forfeit awaits [`Self::forfeit_conformance`], which awaits no writer that reaches
	/// [`Self::restamp_defined`], so the futures of the write methods stay finite.
	async fn note_table_change(&self, change: TableChange) -> Result<()> {
		let TableChange {
			table,
			change,
			..
		} = change;
		let forfeits = {
			let mut births = self.births.lock();
			match change {
				TableDefinitionChange::Created(Some(table_id)) => {
					births.unsettled.insert(table.clone());
					births.compiled.remove(&table);
					births.born.insert(
						table.clone(),
						Birth {
							table_id,
							minted_ring: false,
						},
					);
					false
				}
				TableDefinitionChange::Created(None) => {
					births.born.remove(&table);
					births.compiled.remove(&table);
					false
				}
				TableDefinitionChange::Rewritten {
					forfeits: true,
				} => {
					if births.born.contains_key(&table) {
						births.unsettled.insert(table.clone());
					}
					true
				}
				TableDefinitionChange::Removed => true,
				TableDefinitionChange::Rewritten {
					forfeits: false,
				}
				| TableDefinitionChange::Unchanged => false,
			}
		};
		if forfeits {
			let (ns, db, tb) = table;
			self.forfeit_conformance(ns, db, &tb).await?;
		}
		Ok(())
	}

	/// Takes note of a change to one of a table's field definitions: the table's conformance
	/// claims were settled against the field set that changed, so they are forfeited
	/// ([`Self::forfeit_conformance`]), and a born table is left for the next stamp to settle
	/// again.
	///
	/// Nothing here awaits a writer that reaches [`Self::restamp_defined`], so the futures of the
	/// write methods stay finite.
	async fn note_field_change(&self, table: (NamespaceId, DatabaseId, TableName)) -> Result<()> {
		{
			let mut births = self.births.lock();
			if births.born.contains_key(&table) {
				births.unsettled.insert(table.clone());
			}
		}
		let (ns, db, tb) = table;
		self.forfeit_conformance(ns, db, &tb).await
	}

	/// The table definitions of a database, read for a compilation that
	/// specialises per table: each table is noted in the recordings of
	/// catalog reads as a read of its own definition, and the listing as a
	/// read of which tables the database defines, so what is compiled from it
	/// is reused while every table it read is as it was and no table has been
	/// defined or removed.
	pub async fn all_tb_each(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<[TableDefinition]>> {
		self.catalog_reads.note(crate::catalog_reads::CatalogRead::Listing(ns, db));
		let tables = self.all_tb_listed(ns, db, None).await?;
		for table in tables.iter() {
			self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, &table.name));
		}
		Ok(tables)
	}

	/// What identifies the catalog of every table of a database, as this
	/// transaction sees it, read in two scans of keys and small values: the
	/// tables the database defines, from the keys of their definitions alone,
	/// and every catalog version stamped in it that is proven against its
	/// table's definition ([`CatalogVersion::proven`]). Two reads that answer
	/// the same see the same tables, each in the state its version names; a
	/// table the database defines and no proven version stamps has no version
	/// here, and its state is its definition. Noted in the recordings of catalog
	/// reads as a read of the database's listing.
	///
	/// Storage is read once per database for as long as the transaction
	/// stamps no catalog version ([`Self::catalog_stamps`]): every catalog
	/// write stamps one, so the answer held in the transaction cache with the
	/// count it was read at is the stored one while the count stands. A
	/// speculating transaction, whose stamps are not counted, reads storage
	/// each time.
	pub async fn database_catalog_state(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<DatabaseCatalogState>> {
		self.catalog_reads.note(crate::catalog_reads::CatalogRead::Listing(ns, db));
		self.catalog_state_unnoted(ns, db).await
	}

	/// [`Self::database_catalog_state`], with no read noted.
	#[inline(never)]
	fn catalog_state_unnoted(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> OutOfLine<impl Future<Output = Result<Arc<DatabaseCatalogState>>>> {
		OutOfLine::new(self.catalog_state_unnoted_body(ns, db))
	}

	async fn catalog_state_unnoted_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
	) -> Result<Arc<DatabaseCatalogState>> {
		let held = !self.tr.speculating();
		let stamps = self.catalog_stamps();
		let qey = cache::tx::Lookup::DbCatalog(ns, db);
		if held && let Some(entry) = self.cache.get(&qey) {
			let read = entry.try_into_type::<(u64, Arc<DatabaseCatalogState>)>()?;
			if read.0 == stamps {
				return Ok(Arc::clone(&read.1));
			}
		}
		let defined = self
			.scan_raw(
				TablePrefix {
					ns,
					db,
				}
				.range()?,
				u32::MAX,
				0,
				None,
			)
			.await?
			.into_iter()
			.map(|(key, stored)| {
				TableKey::decode_key(&key).map(|key| (key.tb.into_owned(), stored))
			})
			.collect::<Result<Vec<_>>>()?;
		// A version stands for its table only where it is proven against the
		// definition it was stamped beside; any other leaves the table
		// unstamped here, as if no write had stamped it.
		let stored_of: HashMap<&TableName, &[u8]> =
			defined.iter().map(|(tb, stored)| (tb, stored.as_slice())).collect();
		let mut versions = Vec::new();
		for (key, stamped) in self
			.getr(
				CatalogVersionPrefix {
					ns,
					db,
				}
				.range()?,
				None,
			)
			.await?
		{
			let tb = CatalogVersionKey::decode_key(&key)?.tb.into_owned();
			if let Some(version) = stamped.proven(stored_of.get(&tb).copied()) {
				versions.push((tb, version));
			}
		}
		drop(stored_of);
		let tables = defined.into_iter().map(|(tb, _)| tb).collect();
		let state = Arc::new(DatabaseCatalogState {
			tables,
			versions,
		});
		if held {
			self.cache.insert(qey, cache::tx::Entry::Any(Arc::new((stamps, Arc::clone(&state)))));
		}
		Ok(state)
	}

	/// The `REFERENCE` fields of every table of `ns`/`db`, as this
	/// transaction reads the catalog, with no read noted: a caller that
	/// compiles from them notes what it depends on itself
	/// ([`Self::note_references_read`]).
	///
	/// The fields of a table are read only where its catalog version differs
	/// from the one `previous` was read under, or where no catalog write has
	/// stamped it; every other table's are taken from `previous`. Finding the
	/// fields after a change to one table so reads that table's fields and the
	/// two scans that identify the catalog of every table, which are held for
	/// the transaction while it stamps no catalog version. The answer is held
	/// for the transaction under the catalog state it was read in.
	#[inline(never)]
	pub fn reference_fields(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		previous: Option<&Arc<ReferenceFields>>,
	) -> OutOfLine<impl Future<Output = Result<Arc<ReferenceFields>>>> {
		OutOfLine::new(self.reference_fields_body(ns, db, previous))
	}

	async fn reference_fields_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		previous: Option<&Arc<ReferenceFields>>,
	) -> Result<Arc<ReferenceFields>> {
		let state = self.catalog_state_unnoted(ns, db).await?;
		let held = !self.tr.speculating();
		let qey = cache::tx::Lookup::DbReferenceFields(ns, db);
		if held && let Some(entry) = self.cache.get(&qey) {
			let read = entry.try_into_type::<ReferenceFields>()?;
			if Arc::ptr_eq(&read.state, &state) || *read.state == *state {
				return Ok(read);
			}
		}
		if let Some(previous) = previous
			&& *previous.state == *state
		{
			let fields = Arc::new(ReferenceFields {
				state,
				tables: previous.tables.clone(),
			});
			if held {
				self.cache.insert(qey, cache::tx::Entry::Any(Arc::clone(&fields) as _));
			}
			return Ok(fields);
		}
		let version_of = |state: &DatabaseCatalogState| -> HashMap<TableName, Uuid> {
			state.versions.iter().cloned().collect()
		};
		let now = version_of(&state);
		let before = previous.map(|previous| (version_of(&previous.state), &previous.tables));
		let mut tables = Vec::new();
		for table in &state.tables {
			let version = now.get(table);
			if let Some((versions, kept)) = &before
				&& version.is_some()
				&& versions.get(table) == version
			{
				if let Some((_, fields)) = kept.iter().find(|(name, _)| name == table) {
					tables.push((table.clone(), Arc::clone(fields)));
				}
				continue;
			}
			let fields = self.tb_fields_unnoted(ns, db, table).await?;
			let declared: Arc<[catalog::FieldDefinition]> =
				fields.iter().filter(|field| field.reference.is_some()).cloned().collect();
			if !declared.is_empty() {
				tables.push((table.clone(), declared));
			}
		}
		let fields = Arc::new(ReferenceFields {
			state,
			tables,
		});
		if held {
			self.cache.insert(qey, cache::tx::Entry::Any(Arc::clone(&fields) as _));
		}
		Ok(fields)
	}

	/// Note in the open recordings a read of the `REFERENCE` fields of
	/// `ns`/`db` that can name a record of `target`, or of any table where it
	/// is `None`.
	pub fn note_references_read(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		target: Option<&TableName>,
	) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::References(ns, db, target));
	}

	/// Note in the open recordings a read of `tb`'s catalog that the caller
	/// answered without reading it.
	pub fn note_table_read(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
	}

	/// Note in the open recordings a read of the `DEFINE PARAM $pa` of
	/// `ns`/`db` that the caller answered without reading it.
	pub fn note_param_read(&self, ns: NamespaceId, db: DatabaseId, pa: &str) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Param(ns, db, pa));
	}

	/// Note in the open recordings a read of the definition of function `fc`
	/// of `ns`/`db` that the caller answered without reading it.
	pub fn note_function_read(&self, ns: NamespaceId, db: DatabaseId, fc: &str) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Function(ns, db, fc));
	}

	/// Note in the open recordings a read of database `db` of namespace `ns`,
	/// by name, that the caller answered without reading it.
	pub fn note_database_read(&self, ns: &str, db: &str) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Database(ns, db));
	}

	/// Note in the open recordings a read of something in the catalog the
	/// recordings cannot name, which the caller answered without reading it.
	pub fn note_opaque_read(&self) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Opaque);
	}

	/// The fields of `tb` as [`TableProvider::all_tb_fields`] reads them at
	/// the current version, with no read noted.
	async fn tb_fields_unnoted(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<Arc<[catalog::FieldDefinition]>> {
		let qey = cache::tx::Lookup::Fds(ns, db, tb.as_str());
		if let Some(val) = self.cache.get(&qey) {
			return val.try_into_fds();
		}
		let range = FieldPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		let val = self.tr.getr(range.into_key_range(), None).await?;
		let stored: Arc<[catalog::StoredFieldDefinition]> =
			util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
		let val = catalog::fields_from_stored(&stored)?;
		self.cache.insert(qey, cache::tx::Entry::Fds(Arc::clone(&val)));
		Ok(val)
	}

	/// Answer this transaction's later reads of `tb`'s definition, fields and
	/// indexes with `definition`, `fields` and `indexes`, as though it had read
	/// them itself.
	///
	/// The caller holds them from a snapshot built under the catalog version
	/// this transaction read for the table, so they are what its own reads
	/// would return. An entry the transaction already holds is kept, and an
	/// undefined table records no definition, as a read of it records none.
	///
	/// `indexes` is `None` when the snapshot holds no complete index list
	/// because an index entry needs a newer release. The transaction then
	/// records nothing for the indexes, so its own read of them refuses, which
	/// is what keeps a write to the table from proceeding without them.
	pub(crate) fn adopt_table_catalog(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		definition: Option<&Arc<TableDefinition>>,
		fields: &Arc<[catalog::FieldDefinition]>,
		indexes: Option<&Arc<[catalog::IndexDefinition]>>,
	) {
		if let Some(definition) = definition {
			let qey = cache::tx::Lookup::Tb(ns, db, tb.as_str());
			if self.cache.get(&qey).is_none() {
				self.cache.insert(qey, cache::tx::Entry::Any(Arc::clone(definition) as _));
			}
		}
		let qey = cache::tx::Lookup::Fds(ns, db, tb.as_str());
		if self.cache.get(&qey).is_none() {
			self.cache.insert(qey, cache::tx::Entry::Fds(Arc::clone(fields)));
		}
		if let Some(indexes) = indexes {
			let qey = cache::tx::Lookup::Ixs(ns, db, tb.as_str());
			if self.cache.get(&qey).is_none() {
				self.cache.insert(qey, cache::tx::Entry::Ixs(Arc::clone(indexes)));
			}
		}
	}

	/// Answer this transaction's later reads of the event and view listings of `tb` with `events`
	/// and `views`, which a catalog snapshot served it under the version, or the definition
	/// content, it read; a listing it holds already is kept.
	pub(crate) fn adopt_table_listings(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		events: &Arc<[catalog::EventDefinition]>,
		views: &Arc<[TableDefinition]>,
	) {
		let qey = cache::tx::Lookup::Evs(ns, db, tb.as_str());
		if self.cache.get(&qey).is_none() {
			self.cache.insert(qey, cache::tx::Entry::Evs(Arc::clone(events)));
		}
		let qey = cache::tx::Lookup::Fts(ns, db, tb.as_str());
		if self.cache.get(&qey).is_none() {
			self.cache.insert(qey, cache::tx::Entry::Fts(Arc::clone(views)));
		}
	}

	/// The listing of [`TableProvider::all_tb`], with no read noted.
	async fn all_tb_listed(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> Result<Arc<[TableDefinition]>> {
		if version.is_some() {
			let range = TablePrefix {
				ns,
				db,
			}
			.range()?;
			let val = self.tr.getr(range.into_key_range(), version).await?;
			let stored: Arc<[StoredTableDefinition]> =
				util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
			return catalog::from_stored_all(&stored);
		}
		let qey = cache::tx::Lookup::Tbs(ns, db);
		match self.cache.get(&qey) {
			Some(val) => val.try_into_tbs(),
			None => {
				let range = TablePrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), None).await?;
				let stored: Arc<[StoredTableDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				let entry = cache::tx::Entry::Tbs(Arc::clone(&val));
				self.cache.insert(qey, entry);
				Ok(val)
			}
		}
	}

	/// How many writes and save-point rollbacks this transaction has taken.
	///
	/// It only grows, and grows once each has taken effect or failed, so two
	/// equal readings bracket a stretch of the transaction in which nothing
	/// it reads changed: what was read after the first still holds at the
	/// second.
	pub fn state_changes(&self) -> u64 {
		self.state_changes.load(std::sync::atomic::Ordering::Acquire)
	}

	/// A guard that counts a write or a save-point rollback in
	/// [`Self::state_changes`] when dropped.
	fn state_change(&self) -> StateChange<'_> {
		StateChange(&self.state_changes)
	}

	/// Open a recording of the catalog reads this transaction answers until
	/// it is finished ([`crate::catalog_reads`]).
	pub fn record_catalog_reads(&self) -> crate::catalog_reads::CatalogReadRecording<'_> {
		self.catalog_reads.begin()
	}

	/// Note in the open recordings a read of the listing of the tables of
	/// `ns`/`db` that the caller answered without reading it: code compiled
	/// from an answer kept under a listing it read earlier depends on that
	/// listing as a fresh read would.
	pub fn note_listing_read(&self, ns: NamespaceId, db: DatabaseId) {
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Listing(ns, db));
	}

	/// How many table catalog versions this transaction has stamped so far.
	///
	/// Two reads of the same count bracket a stretch of the transaction in
	/// which no table's catalog version changed, so what was read of the
	/// versions at the first still holds at the second.
	/// Whether this transaction changed the catalog: it stamped a table's
	/// catalog version, or wrote a definition that moves the catalog epoch
	/// without one ([`Self::catalog_touched`]). Its commit is counted among the
	/// datastore's catalog commits, and it names no catalog epoch.
	fn changed_catalog(&self) -> bool {
		self.catalog_stamps() > 0 || self.catalog_touched.load(Ordering::Acquire)
	}

	/// Note a write of a definition that moves the catalog epoch without
	/// stamping a catalog version. A speculation's writes are not counted, as
	/// its stamps are not.
	fn touches_catalog(&self) {
		if !self.tr.speculating() {
			self.catalog_touched.store(true, Ordering::Release);
		}
	}

	/// The definitions shared by the transactions of this transaction's
	/// catalog epoch, with the epoch; `None` where the transaction names no
	/// epoch ([`Self::catalog_epoch`]). Only a read-only transaction is served
	/// from them, so a write's own reads of the catalog stay reads of the
	/// store.
	fn epoch_catalog(&self) -> Option<(u64, &crate::cache::epoch::EpochCatalog)> {
		if self.writeable() {
			return None;
		}
		self.catalog_epoch().map(|epoch| (epoch, &self.triggers.epoch_catalog))
	}

	/// Keep `value`, read for `lookup`, among the definitions of this
	/// transaction's catalog epoch. The epoch is asked after the read, which it
	/// then covers.
	fn keep_for_epoch<'k, T: Send + Sync + 'static>(
		&self,
		lookup: impl FnOnce(u64) -> crate::cache::epoch::Lookup<'k>,
		value: &Arc<T>,
	) {
		if let Some(epoch) = self.catalog_epoch() {
			self.triggers.epoch_catalog.insert(lookup(epoch), Arc::clone(value));
		}
	}

	pub fn catalog_stamps(&self) -> u64 {
		self.catalog_stamps.load(std::sync::atomic::Ordering::Acquire)
	}

	/// Whether this transaction is a speculation's view, whose catalog
	/// stamps are not counted ([`Self::catalog_stamps`]): nothing read
	/// through it can be kept on the strength of an equal count.
	pub fn speculating(&self) -> bool {
		self.tr.speculating()
	}

	/// How many versions of catalog objects other than tables this
	/// transaction has stamped so far: the same bracket as
	/// [`Self::catalog_stamps`], over [`Self::catalog_object_version`].
	pub fn catalog_object_stamps(&self) -> u64 {
		self.object_stamps.load(std::sync::atomic::Ordering::Acquire)
	}

	/// The count of the datastore's catalog commits whose catalog this
	/// transaction sees, where that count names it: the datastore is the
	/// store's sole writer, no catalog commit was under way when the
	/// transaction opened and none has started since, and the transaction
	/// has stamped no catalog version of its own and is not a speculation.
	/// Two transactions answering the same count see the same catalog: every
	/// table the store defines, each in the same state.
	///
	/// The snapshot a transaction reads is taken at or after it opens, and no
	/// commit that writes the catalog overlapped the stretch from the opening
	/// to this call, so the snapshot holds exactly the catalog commits
	/// finished before the opening.
	pub fn catalog_epoch(&self) -> Option<u64> {
		if self.catalog_epoch == NO_CATALOG_EPOCH || self.changed_catalog() || self.tr.speculating()
		{
			return None;
		}
		(self.triggers.catalog_epoch.settled() == Some(self.catalog_epoch))
			.then_some(self.catalog_epoch)
	}

	/// Commit the underlying transaction, counted among the datastore's
	/// catalog commits ([`crate::triggers::CatalogEpoch`]) from before it
	/// starts until it has returned, where it stamped a catalog version.
	async fn commit_store(&self) -> std::result::Result<(), KvsError> {
		let _counted = self.changed_catalog().then(|| self.triggers.catalog_epoch.committing());
		self.tr.commit().await
	}

	/// Enqueue background reclaim of an index-data prefix by its stable id.
	///
	/// The id is sufficient after catalog retirement because index ids are never
	/// reused, so the prefix this names can gain no new writer. `expunge`
	/// preserves the all-version deletion policy of a cascading parent removal.
	pub async fn enqueue_index_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		expunge: bool,
	) -> Result<()> {
		let rc = ReclaimKey::index(ns, db, tb, ix, expunge, Uuid::now_v7());
		self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		Ok(())
	}

	/// Enqueue a table's shared doc-ID space for asynchronous background
	/// reclaim.
	///
	/// The deferred companion to
	/// [`surrealdb_idx::docids::TableDocIds::remove_all`], for the callers that
	/// drop the table's last doc-ID-consuming index. The space holds one entry
	/// per record in each direction, so deleting it inline would put a write per
	/// record into the caller's transaction; the prefixes are destroyed later by
	/// [`crate::Datastore::reclaim_tombstones`] instead.
	///
	/// Each prefix is enqueued separately, because the table's key space
	/// interleaves them with the doc-ID sequence counter (`!dd` < `!dh` < `!di` <
	/// `!dj` < `!dp` < `!ds`) and the counter must survive: a monotonic counter is
	/// what stops a later index handing a second record an id the first still
	/// holds. Separate entries also give each prefix its own resume cursor.
	///
	/// The caller must guarantee no doc-ID-consuming index can adopt a surviving
	/// mapping before the reclaim completes. The prefixes are reclaimed
	/// independently and page by page, so an interrupted reclaim leaves the space
	/// torn — a `!dj` entry whose `!dd` twin is already gone. Adopting one of
	/// those through
	/// [`resolve_or_assign`](surrealdb_idx::docids::TableDocIds::resolve_or_assign)'s
	/// fast path would give an index entries pointing at a doc-ID that resolves
	/// to no record. `DEFINE INDEX` upholds this with
	/// [`Self::claim_tb_doc_ids_reclaim`], which it calls in the transaction
	/// that defines the consumer.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn del_tb_doc_ids_deferred(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		// All of them in one transaction, so a strict subset surviving in
		// the queue is itself evidence that the reaper has drained and retired
		// the rest. `Self::claim_tb_doc_ids_reclaim` reads that.
		for kind in ALL_DOC_ID_KINDS {
			let rc = ReclaimKey {
				kind,
				ns,
				db,
				tb: Cow::Borrowed(tb),
				// The space is table-scoped: no index id addresses it.
				ix: IndexId(0),
				expunge: Expunge::Keep,
				uid: Uuid::now_v7(),
			};
			self.set_key(&rc, &crate::key::reclaim::ReclaimState::enqueued()).await?;
		}
		Ok(())
	}

	/// Claim a table's shared doc-ID space for a new consumer when it is still
	/// internally consistent.
	///
	/// `DEFINE INDEX` calls this before defining a doc-ID-consuming index, and
	/// the graph fold before recording a table's `graph_doc_ids` marker. If every
	/// entry of the set still exists and none carries a cursor, no deletion has
	/// committed and they are conditionally cancelled in this transaction. The
	/// conditional deletes make the cancellation atomic with a concurrent page on
	/// last-writer-wins backends: either this transaction removes the claim and
	/// defines the consumer, or the page advances the state and this transaction
	/// is rejected.
	///
	/// Once a cursor exists, or a strict subset of the entries survives, the
	/// space may be torn. In that case no entry is cancelled: the caller must not
	/// define a consumer, and the background task remains able to finish the
	/// purge in bounded pages. No queued entries means either no reclaim was ever
	/// needed or it completed, both of which are safe for a new consumer.
	///
	/// Two things say the space is torn. A surviving entry carrying a resume
	/// cursor names a prefix the reaper has deleted from. And because the set —
	/// one entry per kind in [`ALL_DOC_ID_KINDS`] — is enqueued in one
	/// transaction, a strict subset of it surviving means the reaper drained and
	/// retired the rest: a tear no surviving cursor records, since retiring an
	/// entry destroys its cursor with it.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn claim_tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<DocIdsReclaimClaim> {
		self.tb_doc_ids_reclaim(ns, db, tb, true).await
	}

	/// [`Self::claim_tb_doc_ids_reclaim`] without the cancellation: reports
	/// whether the space could be claimed, writing nothing.
	///
	/// For a caller whose claim decision spans several tables and is
	/// all-or-nothing — probing every table first keeps one unclaimable
	/// table from leaving another table's queued reclaim already cancelled
	/// in the same transaction. Within one transaction a probe's verdict
	/// holds for a following claim: both scan the same snapshot.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn probe_tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<DocIdsReclaimClaim> {
		self.tb_doc_ids_reclaim(ns, db, tb, false).await
	}

	async fn tb_doc_ids_reclaim(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		cancel: bool,
	) -> Result<DocIdsReclaimClaim> {
		// Whether a surviving entry names a prefix already deleted from, and
		// which of the kinds are still queued for this table, slot for slot with
		// `ALL_DOC_ID_KINDS`.
		let mut started = false;
		let mut queued = [false; ALL_DOC_ID_KINDS.len()];
		// Exact key/value bytes become compare-delete guards if every entry proves
		// untouched. An unreadable value is retained and blocks adoption.
		let mut entries = Vec::new();
		// The queue is keyed by kind first, so a table's entries are not
		// contiguous: each kind is scanned over its own `(kind, ns, db, tb)`
		// range rather than the whole queue being walked to pick them out. Every
		// key such a range holds is one of this table's by construction, so none
		// has to be decoded to be attributed. Each range is still read in batches
		// so an unexpectedly duplicated queue cannot force one unbounded backend
		// scan result; normal operation has one entry per kind.
		for (slot, kind) in ALL_DOC_ID_KINDS.into_iter().enumerate() {
			let mut window = ReclaimTbPrefix {
				kind,
				ns,
				db,
				tb: Cow::Borrowed(tb),
			}
			.range()?;
			loop {
				let items = self.scan_raw(window.clone(), NORMAL_BATCH_SIZE, 0, None).await?;
				let Some((last, _)) = items.last() else {
					break;
				};
				let last = last.clone();
				let exhausted = items.len() < NORMAL_BATCH_SIZE as usize;
				for (k, v) in &items {
					queued[slot] = true;
					// A state that cannot be read is treated as started, because
					// a cursor it may have held cannot be ruled out.
					started |= match ReclaimState::kv_decode_value(v, ()) {
						Ok(state) => state.cursor.is_some(),
						Err(_) => true,
					};
					entries.push((k.clone(), v.clone()));
				}
				if exhausted {
					break;
				}
				window = window.resume_after(&last, Direction::Forward);
			}
		}
		// A strict subset of the set surviving means the reaper drained and
		// retired the rest, so the space is torn whatever the survivors' cursors
		// say. Leave every surviving entry intact: finishing them in bounded pages
		// is the recovery path.
		let queued_any = queued.iter().any(|&q| q);
		let queued_all = queued.iter().all(|&q| q);
		if started || (queued_any && !queued_all) {
			return Ok(DocIdsReclaimClaim::InProgress);
		}
		// No entries means the space was never queued or has been fully drained.
		if !queued_any {
			return Ok(DocIdsReclaimClaim::Available);
		}
		// Every entry is untouched. Withdraw exactly the values observed above so
		// a page that advances one concurrently wins the race rather than having
		// its cursor deleted and its partial purge adopted by the new consumer.
		if cancel {
			for (key, value) in entries {
				self.del_compare(Key::from(key), Some(&value)).await?;
			}
		}
		Ok(DocIdsReclaimClaim::Available)
	}

	/// Open a watch on the records this transaction creates in table `tb` from
	/// now until the watch is dropped ([`crate::creations`]). A writer that
	/// creates a record notes it ([`Self::note_creation`]) for the watch to
	/// answer.
	pub fn watch_creations(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> crate::creations::CreationWatch {
		self.creations.watch(ns, db, tb)
	}

	/// Whether a creation watch is open on table `tb`, so a writer that
	/// creates a record in it must note it ([`Self::note_creation`]). With no
	/// watch open on any table this is one atomic load.
	pub fn creations_watched(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> bool {
		self.creations.watches(ns, db, tb)
	}

	/// An estimate of the bytes this transaction's creation log holds
	/// ([`crate::creations`]), which it holds in memory until the watches on
	/// the tables they belong to close: read without a lock, for the query
	/// whose writes opened the watches to charge to its memory.
	pub fn creation_log_bytes(&self) -> usize {
		self.creations.bytes()
	}

	/// How many creation watches are open on this transaction.
	pub fn creation_watches(&self) -> usize {
		self.creations.open()
	}

	/// Note that the record stored at `key`, a record of table `tb`, was
	/// created, for the creation watches open on `tb` to answer: called by a
	/// writer that encodes the record's key and bytes itself, once the record
	/// is written.
	pub fn note_creation(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName, key: Vec<u8>) {
		self.creations.note(ns, db, tb, key)
	}

	/// Hold `record` in this transaction's cache as the record at `id` of
	/// table `tb`, as [`TableProvider::put_record`] leaves it once the record
	/// is written: for a writer that encodes the record's key and bytes itself.
	///
	/// `generation` is the one the record's bytes carry
	/// ([`surrealdb_catalog::written_generation`]), `None` for stamp 0.
	pub fn cache_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		record: Arc<Record>,
		generation: Option<Generation>,
	) {
		let qey = crate::cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
		self.cache.insert(qey, crate::cache::tx::Entry::Val(record, generation));
	}

	/// Drop the record at `id` of table `tb` from this transaction's cache, as
	/// [`TableProvider::set_record`] and [`TableProvider::del_record`] do once
	/// the record is overwritten or removed: for a writer that encodes the
	/// record's key and bytes itself.
	pub fn forget_record(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName, id: &RecordIdKey) {
		let qey = crate::cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
		self.cache.remove(&qey);
	}

	/// Insert or update a key in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn set_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let (val, prior) = self.encode_levelled(&key, val).await?;
		self.write_encoded(changed, WriteOp::Set, key, val, held(&prior)).await
	}

	/// Insert or update a key whose write slot was already reserved.
	///
	/// Only for a write the write-cardinality guard was charged for earlier, so
	/// that it is not charged twice. The buffered index deltas are the case:
	/// `buffer_term_change` charges each key the flush will write at the moment
	/// the buffer gains it, which is what lets an over-limit statement report
	/// before its caller commits.
	///
	/// Upsert rather than insert, unlike the count deltas, and the difference is
	/// how many keys each writes. Every engine implements insert as an existence
	/// check followed by a write — a remote round trip per key on TiKV. A count
	/// delta is one key per index, so it can afford to have a colliding
	/// discriminator reported rather than silently absorbed. Term changes are one
	/// key per distinct term, so a read for each would restore the per-key cost the
	/// buffer exists to collapse. What keeps the upsert safe is the discriminator:
	/// a freshly minted v7 UUID per flush, so no key written here is a key another
	/// transaction writes.
	async fn set_key_prereserved<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.state_change();
		let key = key.encode_key()?;
		let (val, prior) = self.encode_levelled(&key, val).await?;
		self.write_encoded(changed, WriteOp::Set, key, val, held(&prior)).await
	}

	/// Insert or update a key in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn set<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let val = val.into_vec();
		self.write_encoded(changed, WriteOp::Set, key, val, None).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn put_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let (val, prior) = self.encode_levelled(&key, val).await?;
		self.write_encoded(changed, WriteOp::Put, key, val, held(&prior)).await
	}

	/// Insert a key if it doesn't exist in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn put<V>(&self, key: Key<'_>, val: V) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let val = val.into_vec();
		self.write_encoded(changed, WriteOp::Put, key, val, None).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn put_compare_key<K>(
		&self,
		key: &K,
		val: &K::Value,
		chk: Option<&K::Value>,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		if <K::Value as KVValue>::LEVELLED {
			return self.put_compare_levelled(key, val, chk).await;
		}
		let _changed = self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let encoded_val = val.kv_encode_value()?;
		let key_bytes = encoded.len() as u64;
		let value_bytes = encoded_val.len() as u64;
		let defined = self.changed_definition(&encoded, Some(&encoded_val), None).await?;
		match chk {
			// Nothing to fence on, so nothing to re-examine.
			None => self.tr.putc(encoded, encoded_val, None).await?,
			Some(chk) => {
				let want = chk.kv_encode_value()?;
				// The first attempt hands its bytes over rather than copying them:
				// it is the path every guarded write takes, and the value can be
				// large. A retry re-derives them, on a path that has already paid
				// for a read.
				match self.tr.putc(encoded, encoded_val, Some(want)).await {
					Err(e) if matches!(e, KvsError::TransactionConditionNotMet) => {
						let want = chk.kv_encode_value()?;
						let Some(stored) = self.stored_bytes_matching(key, &want).await? else {
							return Err(e.into());
						};
						self.tr
							.putc(key.encode_key()?, val.kv_encode_value()?, Some(stored))
							.await?
					}
					other => other?,
				}
			}
		}
		self.metrics.record_put(key_bytes, value_bytes);
		self.restamp_defined(defined).await
	}

	/// Update a key in the datastore if the current value matches a condition.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn put_compare<V>(&self, key: Key<'_>, val: V, chk: Option<V>) -> Result<()>
	where
		V: IntoBytes + Debug,
	{
		let _changed = self.reserve_write_slot()?;
		let val = val.into_bytes();
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		let defined = self.changed_definition(&key, Some(&val), None).await?;
		self.tr.putc(key, val, chk.map(|x| x.into_bytes())).await?;
		self.metrics.record_put(key_bytes, value_bytes);
		self.restamp_defined(defined).await
	}

	/// Insert or replace a key in the datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn replace_key<K>(&self, key: &K, val: &K::Value) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let changed = self.reserve_write_slot()?;
		let key = key.encode_key()?;
		let (val, prior) = self.encode_levelled(&key, val).await?;
		self.write_encoded(changed, WriteOp::Replace, key, val, held(&prior)).await
	}

	/// [`Self::replace_key`] over an encoded key and value: for a program that
	/// encodes keys and values itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn replace_raw(&self, key: Key<'_>, val: Val) -> Result<()> {
		let changed = self.reserve_write_slot()?;
		self.write_encoded(changed, WriteOp::Replace, key, val, None).await
	}

	// --------------------------------------------------
	// Encoded-key cores
	// --------------------------------------------------

	/// Fetch an encoded key, recording the read.
	async fn get_encoded(&self, key: Key<'_>, version: Option<u64>) -> Result<Option<Val>> {
		let key_bytes = key.len() as u64;
		let val = self.tr.get(key, version).await?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Fetch an encoded key with a lock that registers it for commit-time conflict
	/// detection, recording the read. A read-only transaction cannot hold the lock
	/// and is refused.
	async fn getu_encoded(&self, key: Key<'_>) -> Result<Option<Val>> {
		if !self.tr.writeable() {
			anyhow::bail!(KvsError::TransactionReadonly);
		}
		let key_bytes = key.len() as u64;
		let val = self.tr.getu(key).await?;
		let (keys_found, value_bytes) = match &val {
			Some(v) => (1, v.len() as u64),
			None => (0, 0),
		};
		self.metrics.record_get(keys_found, key_bytes, value_bytes);
		Ok(val)
	}

	/// Whether an encoded key exists, recording the read.
	async fn exists_encoded(&self, key: Key<'_>, version: Option<u64>) -> Result<bool> {
		let key_bytes = key.len() as u64;
		let found = self.tr.exists(key, version).await?;
		self.metrics.record_get(u64::from(found), key_bytes, 0);
		Ok(found)
	}

	/// Fetch encoded keys in one call, recording the read. The values answer in
	/// the order of `keys`.
	async fn getm_encoded(
		&self,
		keys: &[Key<'static>],
		version: Option<u64>,
	) -> Result<Vec<Option<Val>>> {
		let key_bytes: u64 = keys.iter().map(|k| k.len() as u64).sum();
		let res = self.tr.getm(keys, version).await?;
		self.metrics.record_get(res.records, key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Write an encoded key and value with `op`, then restamp any catalog
	/// definition the key names. `prior` is what the caller already read at `key`.
	/// `changed` is held until the write has taken effect or failed.
	#[inline(never)]
	fn write_encoded(
		&self,
		changed: StateChange<'_>,
		op: WriteOp,
		key: Key<'_>,
		val: Val,
		prior: Option<Option<&[u8]>>,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.write_encoded_body(changed, op, key, val, prior))
	}

	async fn write_encoded_body(
		&self,
		_changed: StateChange<'_>,
		op: WriteOp,
		key: Key<'_>,
		val: Val,
		prior: Option<Option<&[u8]>>,
	) -> Result<()> {
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		let defined = self.changed_definition(&key, Some(&val), prior).await?;
		match op {
			WriteOp::Set => {
				self.tr.set(key, val).await?;
				self.metrics.record_set(key_bytes, value_bytes);
			}
			WriteOp::Put => {
				self.tr.put(key, val).await?;
				self.metrics.record_put(key_bytes, value_bytes);
			}
			WriteOp::Replace => {
				self.tr.replace(key, val).await?;
				self.metrics.record_put(key_bytes, value_bytes);
			}
		}
		self.restamp_defined(defined).await
	}

	/// Delete an encoded key, or every version of it when `all_versions`, then
	/// restamp any catalog definition the key names. `changed` is held until the
	/// delete has taken effect or failed.
	#[inline(never)]
	fn delete_encoded(
		&self,
		changed: StateChange<'_>,
		key: Key<'_>,
		all_versions: bool,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.delete_encoded_body(changed, key, all_versions))
	}

	async fn delete_encoded_body(
		&self,
		_changed: StateChange<'_>,
		key: Key<'_>,
		all_versions: bool,
	) -> Result<()> {
		let key_bytes = key.len() as u64;
		let defined = self.changed_definition(&key, None, None).await?;
		if all_versions {
			self.tr.clr(key).await?;
		} else {
			self.tr.del(key).await?;
		}
		self.metrics.record_del(1, key_bytes);
		self.restamp_defined(defined).await
	}

	// --------------------------------------------------
	// Raw bytes functions
	// --------------------------------------------------

	/// Fetch a key from the datastore, without decoding its value.
	///
	/// The key is still typed; only the value comes back as bytes. That is what a
	/// caller wants when the bytes themselves are the point — a value whose decode
	/// would splice in data from the key that this caller must not have, or one
	/// that has to be compared byte for byte against what is stored. Everything
	/// else should use [`Self::get_key`] and let the key name the value's type.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn get_key_raw<K>(&self, key: &K, version: Option<u64>) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		self.get_encoded(key.encode_key()?, version).await
	}

	/// Retrieve a batch set of keys from the datastore, without decoding.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getm_raw<K>(&self, keys: Vec<K>, version: Option<u64>) -> Result<Vec<Option<Val>>>
	where
		K: KVKey + Debug,
	{
		// Consume each typed key as it is encoded so owned fields do not remain
		// resident alongside the complete encoded request during the await.
		let keys = keys.into_iter().map(|k| k.encode_key()).collect::<Result<Vec<_>>>()?;
		self.getm_encoded(&keys, version).await
	}

	/// Read the records `rids` name at the current version, as the caller's multi-get window: the
	/// cached record for an id the transaction's record cache holds, otherwise the stored bytes
	/// one multi-get found, with no decode.
	///
	/// `slots` is cleared and refilled with one [`StoredSlot`] per id of `rids`, in order, and
	/// `found` holds the multi-get's answer, which a [`StoredSlot::Stored`] names an entry of.
	/// The call decodes nothing, resolves no lightweight edge and inserts nothing into the cache,
	/// so a record that is not served reads for itself where the caller consumes it, and raises
	/// what its own read raises; a caller that decodes a record's bytes holds it in the cache
	/// itself ([`Self::cache_record`]). It records one get with the transaction's metrics, as
	/// [`Self::getm_raw`] does, and no get when every id is cached.
	///
	/// A failed read is the caller's to ignore: `slots` and `found` are then unspecified.
	pub async fn getm_records_stored(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &[&RecordId],
		scratch: &mut RecordKeys,
		slots: &mut Vec<StoredSlot>,
		found: &mut surrealdb_kvs::GetMultiResult,
	) -> Result<()> {
		slots.clear();
		let RecordKeys {
			bytes,
			spans,
			keys,
		} = scratch;
		bytes.clear();
		spans.clear();
		for rid in rids {
			let qey = cache::tx::Lookup::Record(ns, db, rid.table.as_str(), &rid.key);
			match self.cache.get(&qey) {
				Some(cache::tx::Entry::Val(record, generation)) if !record.data.is_none() => {
					slots.push(StoredSlot::Cached(record, generation));
					continue;
				}
				Some(_) => {
					slots.push(StoredSlot::Absent);
					continue;
				}
				None => {}
			}
			let start = bytes.len();
			RecordKey {
				ns,
				db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
			}
			.encode_buffer(bytes)?;
			slots.push(StoredSlot::Stored(spans.len()));
			spans.push((start, bytes.len()));
		}
		if spans.is_empty() {
			*found = Default::default();
			return Ok(());
		}
		let mut borrowed: Vec<Key<'_>> = std::mem::take(keys);
		borrowed.extend(spans.iter().map(|&(start, end)| Key::from(&bytes[start..end])));
		let answer = self.tr.getm(&borrowed, None).await;
		// The vector is empty again before the bytes it borrowed are touched, and keeps its
		// allocation for the next call.
		borrowed.clear();
		*keys = borrowed.into_iter().map(Key::into_static).collect();
		let answer = answer?;
		self.metrics.record_get(answer.records, bytes.len() as u64, answer.value_bytes);
		*found = answer;
		for slot in slots.iter_mut() {
			if let StoredSlot::Stored(at) = slot
				&& found.values.get(*at).is_none_or(Option::is_none)
			{
				*slot = StoredSlot::Absent;
			}
		}
		Ok(())
	}

	// --------------------------------------------------
	// Range functions
	// --------------------------------------------------

	/// Retrieve the keys in a range.
	///
	/// The keys come back as bytes because a key *is* bytes until it is decoded;
	/// what the typed range guarantees is which key type they all are, so the
	/// caller's `decode_key` cannot be the wrong one.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn keys<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		self.keys_raw(rng, limit, skip, version).await
	}

	/// Retrieve the keys in a range, in reverse order.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn keysr<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		self.keysr_raw(rng, limit, skip, version).await
	}

	/// Retrieve the keys in a region whose contents are of more than one type.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn keys_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keys(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve the keys in a region, in reverse order. See [`Self::keys_raw`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn keysr_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let res = self.tr.keysr(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.keys.len() as u64, res.key_bytes, 0);
		Ok(res.keys)
	}

	/// Retrieve a range of keys and their values, decoded under the type the range
	/// says they store.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn scan<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) = self
			.decode_range(rng.into_key_range(), Direction::Forward, Some(limit), skip, version)
			.await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// As [`Self::scan`], in reverse order.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn scanr<V>(
		&self,
		rng: TypedRange<V>,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, V)>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (out, stats) = self
			.decode_range(rng.into_key_range(), Direction::Backward, Some(limit), skip, version)
			.await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(out)
	}

	/// Retrieve a range of keys and their values as bytes.
	///
	/// For a region holding several kinds of key, and for the scan paths that
	/// deliberately work on bytes — the pre-decode filter reads a value's wire form
	/// without building it.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn scan_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scan(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// As [`Self::scan_raw`], in reverse order.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn scanr_raw(
		&self,
		rng: impl AnyRange,
		limit: u32,
		skip: u32,
		version: Option<u64>,
	) -> Result<Vec<(Vec<u8>, Val)>> {
		let res = self.tr.scanr(rng.into_key_range(), limit, skip, version).await?;
		self.metrics.record_scan(res.values.len() as u64, res.key_bytes, res.value_bytes);
		Ok(res.values)
	}

	/// Count the keys in a range or region.
	///
	/// Counting reads no values, so it takes either kind of range.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn count(&self, rng: impl AnyRange, version: Option<u64>) -> Result<usize> {
		let n = self.tr.count(rng.into_key_range(), version).await?;
		// `count` only reports the number of keys, not their byte size.
		self.metrics.record_scan(n as u64, 0, 0);
		Ok(n)
	}

	/// Count the keys in a range or region, examining at most `budget` of
	/// them, and answer the key to resume at when it holds more
	/// ([`Transactor::count_bounded`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn count_bounded(
		&self,
		rng: impl AnyRange,
		version: Option<u64>,
		budget: u32,
	) -> Result<(usize, Option<Vec<u8>>)> {
		let (n, resume) = self.tr.count_bounded(rng.into_key_range(), version, budget).await?;
		self.metrics.record_scan(n as u64, 0, 0);
		Ok((n, resume.map(|key| key.into_inner().into_owned())))
	}

	/// Record `rows` entries and their key and value bytes read through this transaction's read
	/// view ([`Self::snapshot`]), whose cursors are not metered, as the chunk of a scan the
	/// transaction's own cursors record themselves.
	pub fn record_view_scan(&self, rows: u64, key_bytes: u64, value_bytes: u64) {
		self.metrics.record_scan(rows, key_bytes, value_bytes);
	}

	/// Record a batched point read made through this transaction's read view
	/// ([`Self::snapshot`]), whose reads are not metered, as the transaction's own batched read
	/// records one: one get, `records` keys found, `key_bytes` the length of every key asked
	/// for, and `value_bytes` the length of every value found.
	pub fn record_view_get(&self, records: u64, key_bytes: u64, value_bytes: u64) {
		self.metrics.record_get(records, key_bytes, value_bytes);
	}

	/// This transaction's owned read view, or `None` when the backend cannot give one
	/// ([`Transactor::snapshot`]).
	pub fn snapshot(&self) -> Option<Arc<dyn Snapshot>> {
		self.tr.snapshot()
	}

	/// Whether a plain read through this transaction joins its conflict set
	/// ([`surrealdb_kvs::api::Transactable::plain_reads_tracked`]): a static fact of the
	/// datastore, the same for every transaction of it.
	pub fn plain_reads_tracked(&self) -> bool {
		self.tr.inner.plain_reads_tracked()
	}

	/// What read views the datastore can hand out, the same for every transaction opened on it
	/// ([`Transactor::view_capabilities`]). A caller reads it to decide, before opening a
	/// transaction, whether one would give a view.
	pub fn view_capabilities(&self) -> ViewCapabilities {
		self.tr.view_capabilities()
	}

	/// What a range or region holds ([`Transactor::estimate`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn estimate(&self, rng: impl AnyRange, version: Option<u64>) -> Result<Estimate> {
		Ok(self.tr.estimate(rng.into_key_range(), version).await?)
	}

	/// Boundary keys that divide a range or region into parts
	/// ([`Transactor::split`]).
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn split(
		&self,
		rng: impl AnyRange,
		target: SplitTarget,
		version: Option<u64>,
	) -> Result<Vec<Vec<u8>>> {
		let bounds = self.tr.split(rng.into_key_range(), target, version).await?;
		Ok(bounds.into_iter().map(|key| key.into_inner().into_owned()).collect())
	}

	// --------------------------------------------------
	// Cursor functions
	// --------------------------------------------------

	/// Open a stateful keys-only scan cursor over a typed range.
	///
	/// The cursor reuses one underlying iterator across batches for the
	/// duration of a single logical scan (e.g. an outer table walk or one
	/// prefix of a graph traversal). Each [`ScanCursorKeys::next_batch`]
	/// call advances the same iterator instead of re-seeking from scratch.
	/// `skip` is applied once on the first batch.
	///
	/// The keys come back as bytes because a key *is* bytes until it is decoded;
	/// what the typed range guarantees is which key type they all are, so the
	/// caller's `decode_key` cannot be the wrong one.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn open_keys_cursor<'a, V>(
		&'a self,
		rng: TypedRange<V>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'a>> {
		self.open_keys_cursor_raw(rng, dir, skip, version).await
	}

	/// Open a keys-only scan cursor over a region whose contents are of more than
	/// one type. See [`Self::open_keys_cursor`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn open_keys_cursor_raw<'a>(
		&'a self,
		rng: impl AnyRange,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredKeysCursor<'a>> {
		let inner = self.tr.open_keys_cursor(rng.into_key_range(), dir, skip, version).await?;
		Ok(MeteredKeysCursor {
			inner,
			metrics: &self.metrics,
		})
	}

	/// Visit the keys of several regions in one read: the keys of each of `ranges`, in range order
	/// and key order within a range, at most `limit` from each, handed to `f` with the index of
	/// the range. `f`'s `Break` ends the whole read. The keys come back as bytes, as
	/// [`Self::open_keys_cursor_raw`]'s do.
	///
	/// It records one scan with the transaction's metrics for the keys it visited, as one cursor
	/// chunk does.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn visit_key_ranges(
		&self,
		ranges: &[KeyRange<'_>],
		limit: u32,
		version: Option<u64>,
		f: &mut dyn RangeKeyVisitor,
	) -> Result<ScanChunkStats> {
		let stats = self.tr.visit_key_ranges(ranges, limit, version, f).await?;
		self.metrics.record_scan(stats.rows, stats.key_bytes, 0);
		Ok(stats)
	}

	/// Open a stateful key+value scan cursor over a typed range. See
	/// [`Self::open_keys_cursor`].
	///
	/// Both halves are lent as bytes. The typed range fixes which key type and
	/// which value type the region holds, so a caller that defers the decode past
	/// a filter still knows what to decode each surviving row as.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn open_vals_cursor<'a, V>(
		&'a self,
		rng: TypedRange<V>,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'a>> {
		self.open_vals_cursor_raw(rng, dir, skip, version).await
	}

	/// Open a key+value scan cursor over a region whose contents are of more than
	/// one type. See [`Self::open_keys_cursor`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn open_vals_cursor_raw<'a>(
		&'a self,
		rng: impl AnyRange,
		dir: Direction,
		skip: u32,
		version: Option<u64>,
	) -> Result<MeteredValsCursor<'a>> {
		let inner = self.tr.open_vals_cursor(rng.into_key_range(), dir, skip, version).await?;
		Ok(MeteredValsCursor {
			inner,
			metrics: &self.metrics,
		})
	}

	// --------------------------------------------------
	// Batch functions
	// --------------------------------------------------

	/// Retrieve a batched scan over a specific range of keys in the datastore.
	///
	/// This function fetches the keys in batches, with multiple requests to the
	/// underlying datastore.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn batch_keys<V>(
		&self,
		rng: TypedRange<V>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		self.batch_keys_raw(rng, batch, version).await
	}

	/// Retrieve a batched scan over a region whose contents are of more than one
	/// type.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn batch_keys_raw(
		&self,
		rng: impl AnyRange,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<Vec<u8>>> {
		Ok(self.tr.batch_keys(rng.into_key_range(), batch, version).await?)
	}

	/// Retrieve a batched scan of keys and values over a specific range of keys in
	/// the datastore.
	///
	/// This function fetches the key-value pairs in batches, with multiple
	/// requests to the underlying datastore. Both halves are returned as bytes;
	/// the typed range is what says which key type and which value type they are.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn batch_keys_vals<V>(
		&self,
		rng: TypedRange<V>,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		self.batch_keys_vals_raw(rng, batch, version).await
	}

	/// Retrieve a batched scan of keys and values over a region whose contents are
	/// of more than one type. See [`Self::batch_keys_vals`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn batch_keys_vals_raw(
		&self,
		rng: impl AnyRange,
		batch: u32,
		version: Option<u64>,
	) -> Result<Batch<(Vec<u8>, Val)>> {
		Ok(self.tr.batch_keys_vals(rng.into_key_range(), batch, version).await?)
	}

	// --------------------------------------------------
	// Layout functions
	// --------------------------------------------------

	/// Whether the engine this transaction runs on admits no other writer of the store.
	///
	/// Answered by the engine when the transaction opened. A store on such an engine has no other
	/// process whose release must be consulted, and none that can write between this
	/// transaction's read and its commit without this process's knowledge.
	///
	/// `false` for a datastore that runs the DiskANN writer-lease protocol
	/// regardless, as the tests of that protocol have it do.
	pub fn sole_writer(&self) -> bool {
		self.sole_writer && !self.triggers.pending_guard_fences.assumes_shared_store()
	}

	/// Count this transaction as live in `bucket` of its datastore's
	/// [`crate::triggers::LiveTransactions`], counted in before its snapshot was taken; it is
	/// counted out when the transaction is dropped.
	pub(crate) fn with_live_bucket(mut self, bucket: u8) -> Self {
		self.live_bucket = bucket;
		self
	}

	/// The count of events that change which generation of a table is current, what the
	/// transaction holds of a table's conformance record, or whether the stored-format ledger entry
	/// is part of this transaction: a mint, a floor move, a write or removal of a conformance
	/// record, a range delete that spans a held table's keys and a save point rollback that undid
	/// any of them or the entry.
	///
	/// It only grows. A writer that resolved a table's record format at one value may write rows
	/// in that format for as long as the value stands, and resolves it again once it changes.
	pub fn layout_epoch(&self) -> u64 {
		self.layout_epoch.load(Ordering::Acquire)
	}

	/// Notes that `range` is about to be deleted, and forgets what the transaction holds of the
	/// layouts and conformance records when the range spans the `!fc`, `!lg` or `!mi` keys of some
	/// held table.
	///
	/// Returns whether it did, so the caller forgets again once the delete has run, however it
	/// ended. A range that spans the keys of no table the transaction holds changes nothing the
	/// transaction holds: a table it has not read is read after the delete. The span of a table is
	/// computed once, when the table enters a memo ([`Self::hold_span`]), so the comparison
	/// encodes and allocates nothing. The count of range deletes moves either way, so that a save
	/// point rollback, which may restore what the delete removed, judges the scope by it.
	fn begin_range_delete(&self, range: &KeyRange<'_>) -> bool {
		self.range_deletes.fetch_add(1, Ordering::AcqRel);
		let spans_layouts = {
			let spans = self.held_spans.lock();
			spans.values().any(|tables| tables.values().any(|span| span.overlaps(range)))
		};
		if spans_layouts {
			self.invalidate_layouts();
		}
		spans_layouts
	}

	/// Forgets what this transaction holds of any table's layouts and conformance records, and the
	/// spans of the tables it held, and moves the layout epoch.
	///
	/// The three locks are taken one after another, never nested. A table that enters a memo again
	/// takes its span again ([`Self::hold_span`]).
	fn invalidate_layouts(&self) {
		self.layout_memo.lock().clear();
		self.conformance_memo.lock().clear();
		self.held_spans.lock().clear();
		self.layout_epoch.fetch_add(1, Ordering::Release);
	}

	/// Holds the span of `tb`'s `!fc`, `!lg` and `!mi` keys, unless it is held already.
	///
	/// The three keys lie under the table's root as `!fc`, `!lg{generation}` and `!mi`, which sort
	/// in that order, so one span from the first to just past the last covers them all. It also
	/// covers the table's keys between them, whose range deletes are DDL and only cost a reload. A
	/// key that cannot be encoded holds a span that overlaps every range.
	///
	/// Held before the table enters the layout memo or the conformance memo, so a table either memo
	/// holds always has its span.
	fn hold_span(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) {
		let mut spans = self.held_spans.lock();
		let tables = spans.entry((ns, db)).or_default();
		if tables.contains_key(tb) {
			return;
		}
		let start = TableConformanceKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.encode_key();
		let end = TableMigrationsKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.encode_key();
		let span = match (start, end) {
			(Ok(start), Ok(end)) => HeldSpan::Bounded {
				start,
				end: end.next(),
			},
			_ => HeldSpan::Unbounded,
		};
		tables.insert(tb.clone(), span);
	}

	/// The layouts of `tb` this transaction already holds, without a read: `None` when the next
	/// current read of them goes to storage. A caller that only needs to know whether the table has
	/// anything to say about its rows, as the read path of every record does, takes this before the
	/// asynchronous [`Self::table_layouts`].
	pub fn held_table_layouts(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Option<Arc<TableLayouts>> {
		self.memoised_layouts(ns, db, tb)
	}

	/// What this transaction holds of `tb`'s layouts, if it holds any.
	fn memoised_layouts(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Option<Arc<TableLayouts>> {
		self.layout_memo.lock().get(&(ns, db)).and_then(|tables| tables.get(tb)).cloned()
	}

	/// Holds `layouts` as this transaction's view of their table, with the table's span.
	fn memoise_layouts(&self, layouts: Arc<TableLayouts>) {
		self.hold_span(layouts.namespace(), layouts.database(), layouts.table());
		self.layout_memo
			.lock()
			.entry((layouts.namespace(), layouts.database()))
			.or_default()
			.insert(layouts.table().clone(), layouts);
	}

	/// The layouts of `tb` as this transaction sees them, or as of `version`.
	///
	/// A current read (`version` of `None`) is held for the rest of the transaction until a range
	/// delete over the table's layout keys or a save point rollback that undid a mint or a range
	/// delete clears it, so a table's layouts cost one point read and one range read per
	/// transaction. A versioned read is never held: it reads the table's `!mi` and `!lg` keys at
	/// `version`, the version the caller reads the table's rows at.
	///
	/// A table that has no `!mi` answers empty layouts. The load fails only on a storage error: a
	/// key or descriptor this build cannot read is counted or listed as a refused generation in the
	/// answer, never a failure. A `!mi` this build refuses makes every nonzero stamp and every
	/// write of the table a refusal ([`RecordLayoutError::RingRefused`]).
	pub async fn table_layouts(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> Result<Arc<TableLayouts>> {
		if version.is_none()
			&& let Some(held) = self.memoised_layouts(ns, db, tb)
		{
			return Ok(held);
		}
		self.reload_table_layouts(ns, db, tb, version).await
	}

	/// The layouts of `tb` read from storage, bypassing what this transaction holds.
	///
	/// A current read replaces what the transaction holds of the table's layouts.
	#[inline(never)]
	pub fn reload_table_layouts(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> OutOfLine<impl Future<Output = Result<Arc<TableLayouts>>>> {
		OutOfLine::new(self.reload_table_layouts_body(ns, db, tb, version))
	}

	async fn reload_table_layouts_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> Result<Arc<TableLayouts>> {
		let layouts = Arc::new(self.read_table_layouts(ns, db, tb, version).await?);
		if version.is_none() {
			self.memoise_layouts(Arc::clone(&layouts));
		}
		Ok(layouts)
	}

	/// Reads `tb`'s ring and `!lg` keys, at `version` when given, into its layouts.
	///
	/// The table's `!mi` is read first, and decides what the table has:
	///
	/// - absent: empty layouts, and the `!lg` range is not read. A `!lg` key without a `!mi` is
	///   never decoded, and a nonzero stamp then meets an empty ring and is refused;
	/// - present but refused by this build, because a newer release wrote it or it fails its
	///   validation: refused layouts, and the `!lg` range is not read. Any other failure to decode
	///   it fails the read;
	/// - readable: its ring bounds the window, and the `!lg` range is read.
	///
	/// A `!lg` key that does not decode as a generation, one that names generation 0, and one
	/// that names a generation outside the ring is counted in [`TableLayouts::damaged_keys`] and
	/// never decoded: it is a leftover of an earlier incarnation of the table or a retired
	/// generation. The listed generations are sorted by their position in the ring, and the layouts
	/// decode only the last listed descriptor ([`TableLayouts::from_stored`]); every other
	/// descriptor is decoded when a row first names it. A descriptor this build refuses is
	/// reported as a refused generation when a row first names it: its entry error is the cause
	/// when a newer release wrote it or it fails its validation, and
	/// [`LayoutRefusal::NamesTable`] is the cause when it names another table. Only a storage
	/// error, or a failure to decode the table's ring that is no refusal, fails the read.
	async fn read_table_layouts(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		version: Option<u64>,
	) -> Result<TableLayouts> {
		// The storage layer's own read of a table's layouts, which the work it serves does not
		// make: open recordings note the dependency, and work sealed off the catalog may make it.
		self.catalog_reads.answered(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
		let migrations_key = TableMigrationsKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let Some(stored) = self.get_key_raw(&migrations_key, version).await? else {
			return Ok(TableLayouts::empty(ns, db, tb.clone()));
		};
		// The decode validates the entry through its ring, so the one failure path covers both.
		let ring = match TableMigrations::kv_decode_value(&stored, ())
			.and_then(|migrations| Ok(migrations.ring().map_err(CatalogError::Entry)?))
		{
			Ok(ring) => ring,
			Err(e) => {
				return match e.chain().find_map(EntryError::of_cause) {
					Some(refusal) => Ok(TableLayouts::refused(
						ns,
						db,
						tb.clone(),
						LayoutRefusal::Entry(refusal),
						0,
					)),
					None => Err(e),
				};
			}
		};
		let range = LayoutPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		let rows = self.getr_raw(range, version).await?;
		let mut generations: Vec<(Generation, Box<[u8]>)> = Vec::with_capacity(rows.len());
		let mut damaged_keys = 0u32;
		for (key, bytes) in rows {
			let generation = match LayoutKey::decode_key(&key)
				.ok()
				.and_then(|decoded| Generation::new(decoded.generation))
			{
				Some(generation) if ring.contains(generation) => generation,
				_ => {
					damaged_keys = damaged_keys.saturating_add(1);
					continue;
				}
			};
			generations.push((generation, bytes.into_boxed_slice()));
		}
		generations
			.sort_unstable_by_key(|(generation, _)| ring.position(*generation).unwrap_or(u16::MAX));
		Ok(TableLayouts::from_stored(ns, db, tb.clone(), ring, generations, damaged_keys)
			.map_err(CatalogError::Layout)?)
	}

	/// Answers this transaction's later reads of a table's layouts with `layouts`, as though it
	/// had read them itself.
	///
	/// The caller holds them from a snapshot served under the catalog version this transaction
	/// read for the table, so they list the generations its own read would. They are ignored for
	/// a table this transaction has stamped, which reads its layouts through itself because a
	/// stamp may follow a mint that a save point rollback has undone, and for a table whose
	/// layouts the transaction already holds.
	pub fn adopt_table_layouts(&self, layouts: &Arc<TableLayouts>) {
		let (ns, db, tb) = (layouts.namespace(), layouts.database(), layouts.table());
		{
			let stamped = self.stamped_tables.lock();
			if !stamped.is_empty() && stamped.contains_key(&(ns, db, tb.clone())) {
				return;
			}
		}
		self.hold_span(ns, db, tb);
		let mut memo = self.layout_memo.lock();
		if memo.get(&(ns, db)).is_some_and(|tables| tables.contains_key(tb)) {
			return;
		}
		memo.entry((ns, db)).or_default().insert(tb.clone(), Arc::clone(layouts));
	}

	/// The format this transaction writes a record of `tb` in, with the stored-format ledger
	/// entry admitted when the format is not `revision`.
	///
	/// A table that has a ring of generations is written in the layout of its current generation's
	/// descriptor ([`TableLayouts::write_format`]) at every feature level: documents stamped with
	/// the generation, or rows packed as its descriptor says ([`RecordFormat::Packed`]). The
	/// answer is refused when that generation's layout is one this build refuses
	/// ([`RecordLayoutError::CurrentRefused`]) or its ring is refused
	/// ([`RecordLayoutError::RingRefused`]). A table without one is written as
	/// [`RecordFormat::Revision`], which every release reads, at the legacy level, and otherwise as
	/// a document of generation 0.
	///
	/// A [`RecordFormat::Packed`] answer is written through [`Self::record_writer`], which holds
	/// the layouts a packed row is laid out by; `encode_record` refuses it
	/// ([`RecordLayoutError::WriteWithoutLayouts`]).
	///
	/// A ring exists only in a store that holds the stored-format ledger entry. At the legacy
	/// level a transaction whose view of the store holds no such entry therefore has no table
	/// with a ring, and the answer is `revision` with no layout read
	/// ([`Self::stored_format_present`]). A record written as a document is bytes a 3.x release
	/// cannot read, so the entry that says so commits with it ([`Self::admit_stored_format`]).
	pub async fn record_format(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<RecordFormat> {
		self.resolve_record_format(ns, db, tb).await.map(|(format, _)| format)
	}

	/// The writer of `tb`'s rows: the format [`Self::record_format`] answers, with the table's
	/// layouts for a format of a nonzero generation. Every writer of a table row takes its format
	/// from here (or from a write context's slot) and passes the row's id to
	/// [`RecordWriter::encode`], which lets a packed table's rows pack and checks the layouts
	/// against the row's table.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn record_writer(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<RecordWriter> {
		let (format, layouts) = self.resolve_record_format(ns, db, tb).await?;
		Ok(RecordWriter::new(format, layouts))
	}

	/// [`Self::record_format`] with the layouts it resolved the format from, kept for a format of a
	/// nonzero generation ([`RecordFormat::Document`] from generation 1 and
	/// [`RecordFormat::Packed`]) and `None` for `revision` and generation 0.
	async fn resolve_record_format(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<(RecordFormat, Option<Arc<TableLayouts>>)> {
		let legacy = self.write_level().is_legacy();
		if legacy && !self.stored_format_present().await? {
			return Ok((RecordFormat::Revision, None));
		}
		let layouts = self.table_layouts(ns, db, tb, None).await?;
		if layouts.has_generation() {
			let format = layouts.write_format().map_err(CatalogError::Layout)?;
			self.admit_stored_format().await?;
			return Ok((format, Some(layouts)));
		}
		if legacy {
			return Ok((RecordFormat::Revision, None));
		}
		self.admit_stored_format().await?;
		Ok((
			RecordFormat::Document {
				generation: None,
			},
			None,
		))
	}

	/// Whether this transaction's view of the store holds the stored-format ledger entry.
	///
	/// True when a committed transaction was observed to hold it
	/// ([`FeatureLevelCell::stored_format_recorded`]) or this transaction admitted it
	/// ([`Self::admit_stored_format`]); false when this transaction already read it as absent;
	/// otherwise one point read of the entry, whose answer is held for the rest of the
	/// transaction when negative. A positive answer is the observation
	/// [`Self::admit_stored_format`] makes: the transaction counts as holding the entry and the
	/// datastore's cell learns of it once the transaction commits.
	async fn stored_format_present(&self) -> Result<bool> {
		if self.feature.stored_format_recorded()
			|| self.stored_format_admitted.load(Ordering::Acquire)
		{
			return Ok(true);
		}
		if self.stored_format_absent.load(Ordering::Acquire) {
			return Ok(false);
		}
		if self.exists_key(&MigrationKey::new(RECORD_FORMAT_V1.raw()), None).await? {
			self.stored_format_admitted.store(true, Ordering::Release);
			self.on_commit(Box::new(ObserveStoredFormat {
				cell: Arc::clone(&self.feature),
			}))
			.await;
			return Ok(true);
		}
		self.stored_format_absent.store(true, Ordering::Release);
		Ok(false)
	}

	/// Commits the stored-format ledger entry with this transaction's first stored-grammar bytes.
	///
	/// The entry says that records or catalog entries of the store may be in the stored-value
	/// grammar, which a release that does not know it refuses to open the store for. It must
	/// therefore commit in every transaction that commits such bytes, and with no earlier
	/// transaction's help: the check reads the key through this transaction, so the answer sees
	/// the transaction's own writes.
	///
	/// The storage version stamp `!v` commits with the entry, raised to [`MajorVersion::LATEST`]:
	/// a release that checks only that stamp, and so cannot read the ledger, refuses the store at
	/// startup from the first commit that holds the entry.
	///
	/// At most one read per transaction, and none once a committed transaction has observed the
	/// entry ([`FeatureLevelCell::stored_format_recorded`]). The entry and the stamp are written
	/// when the entry is absent, uncharged against the write-key allowance, because they are
	/// bookkeeping the statement did not ask for. The datastore's cell learns of the entry once
	/// this transaction commits. A save point rollback of a scope that wrote or observed the
	/// entry discards both this transaction's observation and the queued action, so the next
	/// stored write checks again, and the cell never claims an entry that did not commit.
	#[inline(never)]
	pub fn admit_stored_format(&self) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.admit_stored_format_body())
	}

	async fn admit_stored_format_body(&self) -> Result<()> {
		if self.feature.stored_format_recorded()
			|| self.stored_format_admitted.load(Ordering::Acquire)
		{
			return Ok(());
		}
		let key = MigrationKey::new(RECORD_FORMAT_V1.raw());
		if !self.exists_key(&key, None).await? {
			let record = MigrationRecord::applied(
				RECORD_FORMAT_V1_NAME,
				RECORD_FORMAT_V1_VERSION,
				self.sequences.node_id(),
				unix_millis_now(),
			);
			self.set_uncharged(key.encode_key()?, record.kv_encode_value()?).await?;
			self.stamp_latest_storage_version().await?;
		}
		self.stored_format_admitted.store(true, Ordering::Release);
		self.on_commit(Box::new(ObserveStoredFormat {
			cell: Arc::clone(&self.feature),
		}))
		.await;
		Ok(())
	}

	/// Raises the storage version stamp `!v` to [`MajorVersion::LATEST`] when it carries a lower
	/// one. A store that carries no stamp is left alone: the datastore stamps it when it settles
	/// the version.
	async fn stamp_latest_storage_version(&self) -> Result<()> {
		let key = VersionKey {};
		let Some(stamped) = self.get_key(&key, None).await? else {
			return Ok(());
		};
		if u16::from(stamped) >= MajorVersion::LATEST {
			return Ok(());
		}
		self.set_uncharged(key.encode_key()?, MajorVersion::latest().kv_encode_value()?).await
	}

	/// [`Self::set`] for bookkeeping the statement did not ask for: it takes no slot of the
	/// write-cardinality guard, so the write is not charged to the statement's write-key allowance.
	///
	/// The write is still a state change and a counted write, and a key that is a versioned
	/// object's definition is restamped as for any write.
	#[inline(never)]
	fn set_uncharged(
		&self,
		key: Key<'_>,
		val: Vec<u8>,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.set_uncharged_body(key, val))
	}

	async fn set_uncharged_body(&self, key: Key<'_>, val: Vec<u8>) -> Result<()> {
		let _changed = self.state_change();
		let key_bytes = key.len() as u64;
		let value_bytes = val.len() as u64;
		let defined = self.changed_definition(&key, Some(&val), None).await?;
		self.tr.set(key, val).await?;
		self.metrics.record_set(key_bytes, value_bytes);
		self.restamp_defined(defined).await
	}

	/// [`Self::admit_record_bytes`] where it needs no read: the answer is `None` for a read at a
	/// `version`, for a table whose layouts the transaction does not hold, and for a generation
	/// those layouts do not list, which a reload of them may settle. Every other answer is the one
	/// [`Self::admit_record_bytes`] gives: both run [`stamped_envelope`] and then
	/// [`admit_with_layouts`].
	fn admit_resident(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		version: Option<u64>,
	) -> Option<Result<Admitted>> {
		let (generation, packed) = match stamped_envelope(bytes) {
			Ok(Some(stamped)) => stamped,
			Ok(None) => return Some(Ok(Admitted::UNSTAMPED)),
			Err(e) => return Some(Err(e)),
		};
		if version.is_some() {
			return None;
		}
		let layouts = self.memoised_layouts(ns, db, tb)?;
		match admit_with_layouts(ns, db, tb, generation, packed, layouts) {
			Ok(admitted) => Some(Ok(admitted)),
			Err(
				RecordLayoutError::UnknownGeneration {
					..
				}
				| RecordLayoutError::OutsideWindow {
					..
				},
			) => None,
			Err(e) => Some(Err(CatalogError::Layout(e).into())),
		}
	}

	/// Checks that a stored record of `tb` may be read: its envelope parses, and a v1 record's
	/// generation is listed by the table's layouts with a descriptor this build reads. Answers the
	/// generation the record's bytes carry (`None` for a `revision` record and for stamp 0) and,
	/// for a packed row only, the layouts that admitted it, which its decode reads its descriptor
	/// from.
	///
	/// Layouts are loaded only for a nonzero generation, and reloaded at most once for one that is
	/// unlisted or outside the table's ring, at `version`. A refusal is a [`CatalogError::Layout`]
	/// that a caller can downcast to.
	async fn admit_record_bytes(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		version: Option<u64>,
	) -> Result<Admitted> {
		let Some((generation, packed)) = stamped_envelope(bytes)? else {
			return Ok(Admitted::UNSTAMPED);
		};
		let layouts = self.table_layouts(ns, db, tb, version).await?;
		match admit_with_layouts(ns, db, tb, generation, packed, layouts) {
			Ok(admitted) => Ok(admitted),
			Err(
				RecordLayoutError::UnknownGeneration {
					..
				}
				| RecordLayoutError::OutsideWindow {
					..
				},
			) => {
				let layouts = self.reload_table_layouts(ns, db, tb, version).await?;
				admit_with_layouts(ns, db, tb, generation, packed, layouts)
					.map_err(|e| CatalogError::Layout(e).into())
			}
			Err(e) => Err(CatalogError::Layout(e).into()),
		}
	}

	/// Decodes the stored bytes of the record `rid` of `tb`, in either encoding, checking a v1
	/// record's generation against the table's layouts at `version`.
	///
	/// The layouts are loaded only for a nonzero generation and reloaded at most once for an
	/// unlisted one; a `revision` record and a record of generation 0 cost no layout read.
	pub async fn decode_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		rid: RecordId,
		version: Option<u64>,
	) -> Result<Record> {
		self.decode_stamped_record(ns, db, tb, bytes, rid, version).await.map(|(record, _)| record)
	}

	/// [`Self::decode_record`] with the generation the stored bytes carry: `None` for a `revision`
	/// record and for stamp 0.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn decode_stamped_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		rid: RecordId,
		version: Option<u64>,
	) -> Result<(Record, Option<Generation>)> {
		let admitted = self.admit_record_bytes(ns, db, tb, bytes, version).await?;
		let record = Record::decode_admitted(bytes, admitted.layouts.as_deref(), rid)?;
		Ok((record, admitted.generation))
	}

	/// [`Self::decode_stamped_record`] where it needs no read of the table's layouts, and `None`
	/// where it does ([`Self::admit_resident`]). The decode runs without an await, so a caller
	/// that reads many records pays no future for the usual one.
	pub fn decode_stamped_resident(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		rid: RecordId,
		version: Option<u64>,
	) -> Option<Result<(Record, Option<Generation>)>> {
		let admitted = match self.admit_resident(ns, db, tb, bytes, version)? {
			Ok(admitted) => admitted,
			Err(e) => return Some(Err(e)),
		};
		Some(
			Record::decode_admitted(bytes, admitted.layouts.as_deref(), rid)
				.map(|record| (record, admitted.generation)),
		)
	}

	/// [`Self::decode_record`] for bytes read back as they were written: an `"id"` the encoding
	/// left out is restored from `rid`, and any other is left as stored, as a view row needs.
	pub async fn decode_record_as_written(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		bytes: &[u8],
		rid: RecordId,
		version: Option<u64>,
	) -> Result<Record> {
		let admitted = self.admit_record_bytes(ns, db, tb, bytes, version).await?;
		Record::decode_admitted_as_written(bytes, admitted.layouts.as_deref(), || rid)
	}

	/// The record `id` of `tb` as stored, at `version` when given, decoded against the table's
	/// layouts ([`Self::decode_stamped_record`]) with the generation its bytes carry; `None` when
	/// no key holds one.
	///
	/// One read of the record's key. The layouts are consulted only for a v1 record of a
	/// nonzero generation.
	async fn read_stamped_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<(Record, Option<Generation>)>> {
		let key = RecordKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			id: Cow::Borrowed(id),
		};
		let Some(bytes) = self.get(key.encode_key()?, version).await? else {
			return Ok(None);
		};
		let rid = RecordId {
			table: tb.clone(),
			key: id.clone(),
		};
		self.decode_stamped_record(ns, db, tb, &bytes, rid, version).await.map(Some)
	}

	/// Decodes the stored bytes a multi-get returned, one slot per record id in the order of
	/// `rids`, against the layouts of each record's table at `version`.
	///
	/// A single [`LayoutSet`] serves the whole batch: a table's layouts are loaded on the first
	/// row of it that names a nonzero generation, and reloaded at most once, and a packed row is
	/// decoded with the layouts that admitted it. Each decoded record carries the generation its
	/// bytes carry. A missing record stays `None`. The first record
	/// that cannot be admitted or decoded fails the batch.
	async fn decode_records<'r>(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: impl Iterator<Item = &'r RecordId>,
		values: Vec<Option<Val>>,
		version: Option<u64>,
	) -> Result<Vec<Option<(Arc<Record>, Option<Generation>)>>> {
		let mut layouts = LayoutSet::new(ns, db, version);
		let mut out = Vec::with_capacity(values.len());
		for (rid, value) in rids.zip(values) {
			let Some(bytes) = value else {
				out.push(None);
				continue;
			};
			let (generation, admitted) =
				layouts.admit_with_layouts(self, &rid.table, &bytes).await?;
			let record = Record::decode_admitted(&bytes, admitted.map(|l| &**l), rid.clone())?;
			out.push(Some((record.into_read_only(), generation)));
		}
		Ok(out)
	}

	// --------------------------------------------------
	// Feature level functions
	// --------------------------------------------------

	/// The level this transaction's catalog writes are encoded at: the store's
	/// level, capped at this build's release.
	///
	/// One atomic load of the datastore's cell, which may lag the store in either
	/// direction (see [`crate::feature`]). The level only chooses what a write
	/// encodes: [`Self::admit_floor`] decides whether the write may proceed, and
	/// a write encoded above the store's level is refused there. A cell that
	/// says `Legacy` is never trusted over a stored-grammar entry: a write of a
	/// levelled value that meets one reads the store's level first.
	pub fn write_level(&self) -> FeatureLevel {
		self.feature.level().capped(Release::CURRENT)
	}

	/// Admits a write that uses the features of release `floor`, and records
	/// that the store uses them.
	///
	/// Answers at once when `floor` is no newer than the newest release the
	/// datastore last saw in use. Otherwise the store's record is read with a
	/// locked read, so that a concurrent raise or lower conflicts with the first
	/// write that uses a feature rather than racing it, and:
	/// - `floor` above the store's level is refused with [`DatastoreError::FeatureNotEnabled`]. At
	///   `Legacy`, that is a floor above the first stored release;
	/// - a record whose `in_use` is below `floor` is rewritten with `in_use` at `floor`, keeping
	///   what a newer release added to the record. The datastore's cell learns of it once this
	///   transaction commits, so a rolled back transaction leaves the cell's `in_use` no higher
	///   than the store's.
	///
	/// The durable `in_use` only rises. Never use this on a read-only
	/// transaction: the locked read is refused.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn admit_floor(&self, floor: Release) -> Result<()> {
		if self.feature.in_use().is_some_and(|in_use| floor <= in_use) {
			return Ok(());
		}
		let raw = self.getu_key_raw(&FeatureLevelKey {}).await?;
		let Some(record) = raw
			.as_deref()
			.map(|bytes| <StoredFeatureLevel as KVValue>::kv_decode_value(bytes, ()))
			.transpose()?
		else {
			return if FeatureLevel::Legacy.admits(floor) {
				Ok(())
			} else {
				Err(DatastoreError::FeatureNotEnabled {
					needed: floor,
					level: FeatureLevel::Legacy,
				}
				.into())
			};
		};
		self.feature.observe_level(record.level());
		if !record.feature_level().admits(floor) {
			return Err(DatastoreError::FeatureNotEnabled {
				needed: floor,
				level: record.feature_level(),
			}
			.into());
		}
		if floor <= record.in_use() {
			return Ok(());
		}
		let raised = StoredFeatureLevel::try_new(record.level(), floor)?;
		let bytes = raised
			.kv_encode_value_at(&EncodeContext {
				level: FeatureLevel::At(Release::CURRENT),
				prior: raw.as_deref(),
			})?
			.bytes;
		self.set(FeatureLevelKey {}.encode_key()?, bytes).await?;
		self.on_commit(Box::new(ObserveFeatureLevel {
			cell: Arc::clone(&self.feature),
			record: raised,
		}))
		.await;
		Ok(())
	}

	/// The release each node last recorded beside the heartbeat given, in one
	/// batched read, in the order of `nodes`.
	///
	/// A node's answer is `Some` only when its release record names exactly the heartbeat
	/// given: the record is written beside the node's own heartbeat, so a record beside any
	/// other heartbeat describes a state of the node that is gone. A node with no record, or
	/// one recorded beside another heartbeat, answers `None`, which a caller treats as older
	/// than any release: a release that rewrites its heartbeat without recording its own is
	/// one that cannot be shown to be current.
	///
	/// A record that does not decode fails the read.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn node_releases(&self, nodes: &[(Uuid, Timestamp)]) -> Result<Vec<Option<Release>>> {
		if nodes.is_empty() {
			return Ok(Vec::new());
		}
		let keys: Vec<NodeReleaseKey> = nodes
			.iter()
			.map(|(nd, _)| NodeReleaseKey {
				nd: *nd,
			})
			.collect();
		let values = self.getm_raw(keys, None).await?;
		nodes
			.iter()
			.zip(values)
			.map(|((_, heartbeat), value)| {
				let Some(bytes) = value else {
					return Ok(None);
				};
				let recorded = <NodeRelease as KVValue>::kv_decode_value(&bytes, ())?;
				Ok((recorded.heartbeat == *heartbeat).then_some(recorded.release))
			})
			.collect()
	}

	/// The store's recorded feature level, read without a lock.
	///
	/// A store with no record answers [`FeatureLevelState::LEGACY`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn feature_level_state(&self) -> Result<FeatureLevelState> {
		Ok(self
			.get_key(&FeatureLevelKey {}, None)
			.await?
			.as_ref()
			.map_or(FeatureLevelState::LEGACY, FeatureLevelState::of))
	}

	/// [`Self::getu_key`] without decoding the value: the stored bytes of a key,
	/// read with a lock that registers the key for commit-time conflict
	/// detection.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn getu_key_raw<K>(&self, key: &K) -> Result<Option<Val>>
	where
		K: KVKey + Debug,
	{
		self.getu_raw(key.encode_key()?).await
	}

	/// Every value of a range that this build can read, in key order.
	///
	/// An entry that a newer release wrote, and that this build therefore
	/// refuses with a release it needs ([`EntryError::NeedsRelease`]), is skipped
	/// and counted in [`Readable::refused`]. Any other failure to decode fails
	/// the read.
	///
	/// Reads through the storage cursor and caches nothing. For passes that must
	/// carry on past an entry they cannot read, such as the background passes
	/// over the catalog hierarchy, and not for a read that answers a statement,
	/// which must fail when an entry it needs is refused.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn all_readable<V>(&self, rng: TypedRange<V>) -> Result<Readable<V>>
	where
		V: KVValue<KeyContext = ()> + Send,
	{
		let (rows, refused, stats) = self
			.decode_range_tolerating(rng.into_key_range(), Direction::Forward, None, 0, None, true)
			.await?;
		self.metrics.record_get(stats.rows, stats.key_bytes, stats.value_bytes);
		Ok(Readable {
			values: rows.into_iter().map(|(_, value)| value).collect(),
			refused,
		})
	}

	/// The level a write that replaces `prior` is encoded at.
	///
	/// The store's level as the cell holds it, except where the cell says `Legacy`
	/// and `prior` is a stored-grammar entry: the cell is behind the store, and
	/// encoding at `Legacy` would write `revision` over an entry that may carry
	/// fields this build does not know. The store's record is then read with a
	/// locked read and the cell is brought up to it. A stored-grammar entry in a
	/// store with no record is [`DatastoreError::FeatureLevelMissing`].
	async fn level_for_write(&self, prior: Option<&[u8]>) -> Result<FeatureLevel> {
		let level = self.write_level();
		if !level.is_legacy() || prior.and_then(<[u8]>::first) != Some(&CATALOG_MARKER) {
			return Ok(level);
		}
		let Some(raw) = self.getu_key_raw(&FeatureLevelKey {}).await? else {
			return Err(DatastoreError::FeatureLevelMissing.into());
		};
		let record = <StoredFeatureLevel as KVValue>::kv_decode_value(&raw, ())?;
		self.feature.observe(Some(&record));
		Ok(record.feature_level().capped(Release::CURRENT))
	}

	/// `val` encoded for a write that replaces `prior`, with the floor it
	/// carries admitted.
	async fn encode_with_prior<V: KVValue>(
		&self,
		val: &V,
		prior: Option<&[u8]>,
	) -> Result<Vec<u8>> {
		self.encode_admitted(prior, |ctx| val.kv_encode_value_at(ctx)).await
	}

	/// The bytes `encode` produces for a write that replaces `prior`, with the
	/// floor they carry admitted.
	///
	/// `encode` runs at the level [`Self::level_for_write`] answers. When the
	/// floor is refused because the store's level is below it, the refusal has
	/// already brought the cell down to the store's level, and `encode` runs
	/// once more at that level, so a value whose features the lower level does
	/// not admit is encoded without them or refused by the encoder, and the
	/// write is not refused for a level the cell merely remembered. A second
	/// refusal, or one at an unchanged level, is the write's answer.
	///
	/// An encoding that carries a floor is in the stored grammar, so once its floor is
	/// admitted the stored-format ledger entry is admitted too
	/// ([`Self::admit_stored_format`]): the entry commits in the transaction that writes the
	/// bytes. An encoding with no floor is `revision` or a fixed encoding and needs no entry.
	async fn encode_admitted(
		&self,
		prior: Option<&[u8]>,
		mut encode: impl FnMut(&EncodeContext<'_>) -> Result<Encoded>,
	) -> Result<Vec<u8>> {
		let mut level = self.level_for_write(prior).await?;
		let mut retried = false;
		loop {
			let Encoded {
				bytes,
				floor,
			} = encode(&EncodeContext {
				level,
				prior,
			})?;
			let Some(floor) = floor else {
				return Ok(bytes);
			};
			match self.admit_encoding(floor, level, retried).await? {
				Admission::Accept => return Ok(bytes),
				Admission::RetryAt(refreshed) => {
					level = refreshed;
					retried = true;
				}
			}
		}
	}

	/// Admit the floor an encoding at `level` carries. A refusal because the
	/// store's level is below the floor, on a first attempt, answers the level the
	/// cell has been brought down to when that differs from `level`; any other
	/// refusal is the error.
	#[inline(never)]
	fn admit_encoding(
		&self,
		floor: Release,
		level: FeatureLevel,
		retried: bool,
	) -> OutOfLine<impl Future<Output = Result<Admission>>> {
		OutOfLine::new(self.admit_encoding_body(floor, level, retried))
	}

	async fn admit_encoding_body(
		&self,
		floor: Release,
		level: FeatureLevel,
		retried: bool,
	) -> Result<Admission> {
		match self.admit_floor(floor).await {
			Ok(()) => {
				// A floor means the bytes are in the stored grammar, which a release that
				// does not know the ledger entry cannot read.
				self.admit_stored_format().await?;
				Ok(Admission::Accept)
			}
			Err(e) if !retried && refused_by_level(&e) => {
				let refreshed = self.write_level();
				if refreshed == level {
					return Err(e);
				}
				Ok(Admission::RetryAt(refreshed))
			}
			Err(e) => Err(e),
		}
	}

	/// `val` encoded for a write to `key`, and the bytes `key` held.
	///
	/// A value that is not levelled encodes as it always has, and the key is not
	/// read: the second part of the answer is `None`. A levelled value reads the
	/// key, which is how its encoding keeps what a newer release added to the
	/// entry, and answers what it read as `Some(held)` for
	/// [`Self::changed_definition`], so a defined object's key is read once per
	/// write.
	async fn encode_levelled<V: KVValue>(
		&self,
		key: &Key<'_>,
		val: &V,
	) -> Result<(Vec<u8>, Option<Option<Val>>)> {
		if !V::LEVELLED {
			return Ok((val.kv_encode_value()?, None));
		}
		let prior = self.tr.get(key.as_borrowed(), None).await?;
		let bytes = self.encode_with_prior(val, prior.as_deref()).await?;
		Ok((bytes, Some(prior)))
	}

	/// Whether the bytes a key holds say what `chk` says: `chk` and the stored
	/// value are both absent, or both present and equal by
	/// [`KVValue::kv_canonical`].
	///
	/// Equality is by meaning rather than by bytes, because a value encodes at
	/// the level of the store it is written to, which need not be the level of
	/// the entry that is stored. Stored bytes that do not decode do not match.
	fn prior_matches<K>(key: &K, prior: Option<&[u8]>, chk: Option<&K::Value>) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let matches = match (prior, chk) {
			(None, None) => true,
			(Some(prior), Some(chk)) => {
				let want = chk.kv_canonical()?;
				K::Value::kv_decode_value(prior, key.value_context())
					.and_then(|stored| stored.kv_canonical())
					.is_ok_and(|have| have == want)
			}
			(None, Some(_)) | (Some(_), None) => false,
		};
		if matches {
			Ok(())
		} else {
			Err(KvsError::TransactionConditionNotMet.into())
		}
	}

	/// [`Self::put_compare_key`] for a levelled value: the key is read, compared
	/// with `chk` by meaning, and written with a condition on the exact bytes read.
	async fn put_compare_levelled<K>(
		&self,
		key: &K,
		val: &K::Value,
		chk: Option<&K::Value>,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let _changed = self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let prior = self.tr.get(encoded.as_borrowed(), None).await?;
		Self::prior_matches(key, prior.as_deref(), chk)?;
		let bytes = self.encode_with_prior(val, prior.as_deref()).await?;
		let key_bytes = encoded.len() as u64;
		let value_bytes = bytes.len() as u64;
		let defined =
			self.changed_definition(&encoded, Some(&bytes), Some(prior.as_deref())).await?;
		self.tr.putc(encoded, bytes, prior).await?;
		self.metrics.record_put(key_bytes, value_bytes);
		self.restamp_defined(defined).await
	}

	/// [`Self::del_compare_key`], or with `all_versions` [`Self::clr_compare_key`],
	/// for a levelled value: the key is read, compared with `chk` by meaning, and
	/// removed with a condition on the exact bytes read.
	async fn delete_compare_levelled<K>(
		&self,
		key: &K,
		chk: &K::Value,
		all_versions: bool,
	) -> Result<()>
	where
		K: KVKey + Debug,
	{
		let _changed = self.reserve_write_slot()?;
		let encoded = key.encode_key()?;
		let key_bytes = encoded.len() as u64;
		let prior = self.tr.get(encoded.as_borrowed(), None).await?;
		Self::prior_matches(key, prior.as_deref(), Some(chk))?;
		let defined = self.changed_definition(&encoded, None, Some(prior.as_deref())).await?;
		if all_versions {
			self.tr.clrc(encoded, prior.as_deref()).await?;
		} else {
			self.tr.delc(encoded, prior.as_deref()).await?;
		}
		self.metrics.record_del(1, key_bytes);
		self.restamp_defined(defined).await
	}

	/// Writes a levelled value that the caller encodes: for a program that
	/// encodes keys and values itself, such as the VM's.
	///
	/// `encode` is handed the level the write is encoded at and the bytes `key`
	/// holds, and answers the bytes to write with the floor they carry, as
	/// [`KVValue::kv_encode_value_at`] does; the floor is admitted
	/// ([`Self::admit_floor`]). `encode` runs a second time, at the store's
	/// level, when the first floor is refused because the cell remembered a
	/// level above the store's. The write is as [`Self::set_key`] makes it, or
	/// as [`Self::replace_raw`] for [`LevelledMode::Replace`].
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn put_levelled_raw(
		&self,
		key: Key<'_>,
		mode: LevelledMode,
		encode: impl FnMut(&EncodeContext<'_>) -> Result<Encoded>,
	) -> Result<()> {
		let _changed = self.reserve_write_slot()?;
		let key_bytes = key.len() as u64;
		let prior = self.tr.get(key.as_borrowed(), None).await?;
		let bytes = self.encode_admitted(prior.as_deref(), encode).await?;
		let value_bytes = bytes.len() as u64;
		let defined = self.changed_definition(&key, Some(&bytes), Some(prior.as_deref())).await?;
		match mode {
			LevelledMode::Set => self.tr.set(key, bytes).await?,
			LevelledMode::Replace => self.tr.replace(key, bytes).await?,
		}
		self.metrics.record_set(key_bytes, value_bytes);
		self.restamp_defined(defined).await
	}

	// --------------------------------------------------
	// Speculation functions
	// --------------------------------------------------

	/// Speculate from `overlay` ([`crate::speculation`]): a view of this
	/// transaction, answered with the guard that ends it, whose writes land in
	/// the overlay and whose reads of the current state see them. Only code
	/// handed the view ([`Speculation::txn`]) reads through the overlay; this
	/// transaction, its cache and its count of catalog stamps are left as they
	/// were, for every other holder of it too.
	///
	/// The speculation reads through the cache a speculation from the same
	/// state that wrote nothing left, adding to it until its first write,
	/// after which it caches apart ([`TransactionCache::begin_speculation`]).
	///
	/// `None` on a view: speculations do not nest.
	pub fn speculate(
		self: &Arc<Self>,
		overlay: crate::speculation::Overlay,
	) -> Option<Speculation> {
		if self.tr.speculating() {
			return None;
		}
		let base_state = self.state_changes();
		let kept = self.speculative_caches.try_lock().ok().and_then(|kept| {
			kept.iter()
				.find(|(from, at, _)| from.writes_as(&overlay) && *at == base_state)
				.map(|(_, _, c)| Arc::clone(c))
		});
		// The view's overlay lists only the catalog ids held in this
		// speculation: those `overlay` lists stay with whoever handed it in.
		let mut own = overlay.clone();
		own.take_held();
		let held = Arc::new(std::sync::Mutex::new(Some(own)));
		let tr = Transactor::speculative(
			Box::new(crate::speculation::Base::new(Arc::clone(self), Arc::clone(&held))),
			held,
		);
		let cache = TransactionCache::new(1);
		cache.begin_speculation(kept, Arc::clone(&tr.writes), Arc::clone(&self.state_changes));
		let view = Transaction {
			local: self.local,
			started_at: Instant::now(),
			observer: Arc::new(surrealdb_observe::NoopObserver),
			metrics: TransactionMetrics::new(),
			tenant_identity: OnceLock::new(),
			tr,
			cache,
			speculative_caches: Mutex::new(Vec::new()),
			catalog_reads: Default::default(),
			sequences: self.sequences.clone(),
			feature: Arc::clone(&self.feature),
			sole_writer: self.sole_writer,
			layout_memo: Default::default(),
			conformance_memo: Default::default(),
			held_spans: Default::default(),
			births: parking_lot::Mutex::new(self.births.lock().clone()),
			layout_epoch: AtomicU64::new(0),
			range_deletes: AtomicU64::new(0),
			stored_format_admitted: AtomicBool::new(false),
			stored_format_absent: AtomicBool::new(false),
			catalog_write_poisoned: AtomicBool::new(false),
			failed_statement_poisoned: AtomicBool::new(false),
			save_point_marks: Mutex::new(Vec::new()),
			catalog_stamps: AtomicU64::new(self.catalog_stamps()),
			catalog_touched: AtomicBool::new(self.catalog_touched.load(Ordering::Acquire)),
			object_stamps: AtomicU64::new(self.catalog_object_stamps()),
			stamped_tables: Default::default(),
			creations: Default::default(),
			shaped_indexes: Default::default(),
			catalog_epoch: NO_CATALOG_EPOCH,
			state_changes: Arc::new(AtomicU64::new(0)),
			changefeed: OnceLock::new(),
			live_events: OnceLock::new(),
			index_deltas: OnceLock::new(),
			triggers: Arc::new(CommitTriggers::new()),
			trigger_async_event: AtomicBool::new(false),
			trigger_index_compaction: AtomicBool::new(false),
			trigger_graph_fold: AtomicBool::new(false),
			trigger_table_migration: AtomicBool::new(false),
			live_bucket: crate::triggers::NO_LIVE_BUCKET,
			write_keys_limit: OnceLock::new(),
			write_guard_poisoned: AtomicBool::new(false),
			save_point_poisoned: AtomicBool::new(false),
			interrupted_write_poisoned: AtomicBool::new(false),
			commit_outcome_unknown: AtomicBool::new(false),
			guarded_writes: AtomicU64::new(0),
			pending_index_build_reservations: Mutex::new(Vec::new()),
			cached_index_build_reservations: Mutex::new(HashMap::new()),
			cached_index_admissions: Default::default(),
			provisional_sequences: Mutex::new(HashMap::new()),
			defines_any_sequence: AtomicBool::new(false),
			provisional_save_points: Mutex::new(Vec::new()),
			commit_actions: Mutex::new(Vec::new()),
			rollback_actions: Mutex::new(Vec::new()),
			after_commit_waits: None,
			defers_build_waits: false,
			graph_doc_ids_settled: Mutex::new(HashMap::new()),
		};
		Some(Speculation {
			base: Arc::clone(self),
			view: Arc::new(view),
			from: overlay,
			base_state,
			ended: false,
		})
	}

	/// Mark that the speculation in progress raised a refusal its statement
	/// declares.
	pub fn note_speculative_refusal(&self) {
		if let Some(overlay) =
			self.tr.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut()
		{
			overlay.refuse();
		}
	}

	/// Mark that the speculation in progress left an effect for the real run.
	pub fn note_speculative_skip(&self) {
		if let Some(overlay) =
			self.tr.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut()
		{
			overlay.skip();
		}
	}

	/// Keep `note` with the speculation in progress's writes, when one is.
	pub fn note_speculative(&self, note: (u16, u128)) {
		if let Some(overlay) =
			self.tr.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut()
		{
			overlay.note(note);
		}
	}

	/// Keep catalog id `held`, held by the speculation in progress, with its
	/// writes, when one is: `false` when none is, and nothing keeps it.
	pub fn note_speculative_hold(&self, held: (crate::sequences::CatalogSequence, u32)) -> bool {
		match self.tr.overlay.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).as_mut() {
			Some(overlay) => {
				overlay.hold(held);
				true
			}
			None => false,
		}
	}

	// --------------------------------------------------
	// Savepoint functions
	// --------------------------------------------------

	/// Set a new save point on the transaction.
	/// The buffered index deltas are scoped alongside the storage save point.
	/// They describe writes this transaction has made, so a rollback that undoes
	/// those writes has to undo the buffered description of them too. So are
	/// the notes of the records it created ([`crate::creations`]) and the
	/// provisional sequences — see [`Self::provisional_save_points`].
	pub async fn new_save_point(&self) -> Result<()> {
		// The storage save point opens first: the buffer's frames mirror its
		// stack, so a failed open must not leave a frame behind for some outer
		// save point's release to pop instead of its own.
		self.inner.new_save_point().await?;
		// Initialised rather than merely inspected: the save point is opened
		// before the mutations it scopes, so waiting for the buffer to exist
		// would leave the first mutation in the transaction's own frame, where a
		// rollback cannot reach it.
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).push_save_point();
		self.cache.push_save_point();
		self.creations.push_save_point();
		let snapshot = self.snapshot_provisional_sequences().await;
		self.provisional_save_points.lock().await.push(snapshot);
		// The commit actions queued from here on answer to this scope's writes, and the layout
		// state a rollback must judge is the state as it stands now.
		let actions = self.commit_actions.lock().await.len();
		self.save_point_marks.lock().await.push(SavePointMark {
			actions,
			layout_epoch: self.layout_epoch.load(Ordering::Acquire),
			range_deletes: self.range_deletes.load(Ordering::Acquire),
			stored_format_admitted: self.stored_format_admitted.load(Ordering::Acquire),
		});
		Ok(())
	}

	/// Release the last save point.
	pub async fn release_last_save_point(&self) -> Result<()> {
		if let Err(e) = self.inner.release_last_save_point().await {
			Self::poison_unless_inert(&e, &self.save_point_poisoned);
			return Err(e.into());
		}
		if let Some(buffer) = self.index_deltas.get() {
			buffer.release_save_point();
		}
		self.cache.release_save_point();
		self.creations.release_save_point();
		// Released, the scope folds into the enclosing one, whose snapshot
		// still describes the state an outer rollback returns to.
		self.provisional_save_points.lock().await.pop();
		// The scope's commit actions, and whatever it did to the layouts, belong to the enclosing
		// scope.
		self.save_point_marks.lock().await.pop();
		Ok(())
	}

	/// Rollback to the last save point.
	pub async fn rollback_to_save_point(&self) -> Result<()> {
		let _changed = self.state_change();
		if let Err(e) = self.inner.rollback_to_save_point().await {
			Self::poison_unless_inert(&e, &self.save_point_poisoned);
			return Err(e.into());
		}
		if let Some(buffer) = self.index_deltas.get() {
			buffer.rollback_save_point();
		}
		// A record or definition cached since the save point may describe a
		// write the rollback undid, and a read must not be answered from it.
		self.cache.rollback_save_point();
		// The caches kept for later speculations were filled by reads that saw
		// the writes the rollback undoes, and the journal above reaches only this
		// transaction's own cache, so none of them may outlive the rollback.
		self.speculative_caches.lock().await.clear();
		self.creations.rollback_save_point();
		// Whether a locked read's conflict registration survives a savepoint
		// rollback is engine-defined, so no memo of one may outlive it: the
		// next `graph_doc_ids_settled` call re-issues the locked read.
		self.graph_doc_ids_settled.lock().await.clear();
		// The rollback may undo a shape write and the catalog stamp beside it,
		// and a later write of the same shape must find it unrecorded.
		self.forget_shaped_indexes();
		let snapshot = self.provisional_save_points.lock().await.pop();
		if let Some(snapshot) = snapshot {
			self.restore_provisional_sequences(snapshot).await;
		}
		let mark = self.save_point_marks.lock().await.pop();
		if let Some(mark) = mark {
			// An effect queued in the scope answers to writes the rollback undid, unless it
			// answers to the end of the transaction instead.
			let mut actions = self.commit_actions.lock().await;
			if actions.len() > mark.actions {
				let queued = actions.split_off(mark.actions);
				actions.extend(queued.into_iter().filter(|action| !action.answers_to_scope()));
			}
			drop(actions);
			// What the transaction holds of the layouts, and whether it holds the stored-format
			// ledger entry, is stale only if the scope changed it: a mint, a floor move, a write or
			// removal of a conformance record, or a range delete over held layouts, moves the
			// epoch; any range delete may have removed layouts of a table the transaction had not
			// read yet; and an entry the scope wrote or observed set the admitted flag. A scope
			// that did none of these, as the failed creating attempt of an upsert does not,
			// leaves what the transaction holds describing storage as it stands, so the next
			// write resolves nothing again.
			let admitted_in_scope =
				!mark.stored_format_admitted && self.stored_format_admitted.load(Ordering::Acquire);
			if admitted_in_scope {
				self.stored_format_admitted.store(false, Ordering::Release);
			}
			if admitted_in_scope
				|| self.layout_epoch.load(Ordering::Acquire) != mark.layout_epoch
				|| self.range_deletes.load(Ordering::Acquire) != mark.range_deletes
			{
				self.invalidate_layouts();
			}
		}
		Ok(())
	}

	/// Poison the transaction for a save-point failure that leaves its state in
	/// doubt, which is every one the engine raises after it has begun reverting.
	///
	/// The three below it raises first, from the guards a save-point call opens with:
	/// a scope that is not open, a transaction that cannot be written, and one that is
	/// already closed. Nothing has been reverted in any of them, so the writes stand
	/// exactly as they did and the buffer's frames still mirror the storage stack —
	/// neither side moved. Each reports a caller's mistake it can handle, and killing
	/// its transaction would answer that with one it cannot, replacing a precise error
	/// with a vaguer one.
	///
	/// What must poison is a failure partway through: an engine unwinding a scope that
	/// absorbed released save points reverts once per save point, and one of those
	/// failing leaves it unknown which of the scope's writes survive.
	fn poison_unless_inert(e: &KvsError, poisoned: &AtomicBool) {
		let inert = matches!(
			e,
			KvsError::NoSavepoint | KvsError::TransactionReadonly | KvsError::TransactionFinished
		);
		if !inert {
			poisoned.store(true, Ordering::Relaxed);
		}
	}

	// --------------------------------------------------
	// Timestamp functions
	// --------------------------------------------------

	/// Get the current monotonic timestamp
	pub async fn timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.timestamp().await?)
	}

	/// Get the current safe (closed) watermark timestamp — the versionstamp at or
	/// below which every committed transaction is final and visible. The
	/// live-query router uses this so it never advances past a commit that could
	/// still become visible with a lower versionstamp. Defaults to
	/// [`Self::timestamp`]; distributed backends override it.
	pub async fn safe_timestamp(&self) -> Result<BoxTimeStamp> {
		Ok(self.tr.safe_timestamp().await?)
	}

	/// Returns `true` if the table has at least one durable live-query
	/// subscription row, read within this transaction's snapshot.
	///
	/// This is the Router-engine change-capture gate. It deliberately reads the
	/// committed `key::table::lq` rows — the cluster-wide source of truth — rather
	/// than any node-local in-memory cache: on a shared/replicated store the
	/// per-node caches have no cross-node invalidation, so a cache could miss a
	/// subscription created on another node and the write would fail to capture an
	/// event that subscriber needs (a loss that can never be replayed). Reading
	/// within the write's own snapshot makes the gate consistent and cluster-wide.
	/// It is a limit-1 key scan, so the cost is independent of the subscriber
	/// count on the table.
	pub async fn table_has_live_query(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<bool> {
		let range = SubscriptionPrefix {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		}
		.range()?;
		Ok(!self.keys(range, 1, 0, None).await?.is_empty())
	}

	/// Returns the timestamp implementation of the stamps [`Self::timestamp`]
	/// mints, which key the change feed and the live-query events.
	pub fn timestamp_impl(&self) -> BoxTimeStampImpl {
		self.tr.timestamp_impl()
	}

	/// Returns the timestamp implementation of the version a versioned read
	/// takes, through which a `VERSION <datetime>` clause is converted.
	pub fn version_impl(&self) -> BoxTimeStampImpl {
		self.tr.version_impl()
	}

	/// The key and value bytes this transaction has written, which its write
	/// buffer holds until it commits: deletes count their keys.
	pub fn bytes_written(&self) -> u64 {
		self.metrics.bytes_written()
	}

	/// Whether the backend bounds the memory its write buffer takes, so what
	/// the transaction has written need not be counted against a query's
	/// memory.
	pub fn bounds_write_buffer(&self) -> bool {
		self.tr.bounds_write_buffer()
	}

	// --------------------------------------------------
	// Changefeed functions
	// --------------------------------------------------

	/// Records the table (re)definition in the changefeed if enabled.
	pub fn changefeed_buffer_table_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		dt: &StoredTableDefinition,
	) {
		self.changefeed.get_or_init(Changefeed::new).buffer_table_change(ns, db, tb, dt)
	}

	/// change will record the change in the changefeed if enabled.
	/// To actually persist the record changes into the underlying kvs,
	/// you must call the `complete_changes` function and then commit the
	/// transaction.
	#[expect(clippy::too_many_arguments)]
	pub fn changefeed_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: Arc<Record>,
		current: Arc<Record>,
		store_difference: bool,
	) {
		self.changefeed.get_or_init(Changefeed::new).buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous,
			current,
			store_difference,
		)
	}

	/// Accumulate a signed count-index delta for this transaction.
	///
	/// Buffered rather than written per document: all of a transaction's
	/// mutations to one index collapse into a single `!iu` entry at commit. The
	/// entry is still a blind write of a key no other transaction shares, so
	/// this keeps the contention-free property the delta log exists for while
	/// removing the per-document fan-out that made the log — and therefore every
	/// `count()` read — grow with write volume.
	pub fn buffer_count_delta(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		delta: i64,
		nid: Uuid,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_count_delta(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			delta,
			nid,
		)
	}

	/// The net count delta this transaction has buffered but not yet written.
	///
	/// The count read path adds this to the committed entries it scans, so a
	/// read still observes its own transaction's uncommitted mutations.
	pub fn pending_count_delta(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> i64 {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_count(&BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}),
			None => 0,
		}
	}

	/// Register that an index wants compaction once this transaction commits.
	///
	/// Deduplicated per index for the same reason as the count deltas: the
	/// queue only needs to name the index once per transaction, not once per
	/// mutated document.
	pub fn buffer_compaction_trigger(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		nid: Uuid,
	) {
		self.index_deltas
			.get_or_init(|| Box::new(IndexDeltaBuffer::new()))
			.buffer_compaction_trigger(
				BufferedIndex {
					ns,
					db,
					tb: tb.clone(),
					ix,
				},
				nid,
			)
	}

	/// Whether this transaction's commit fails if any key it read was written
	/// since it opened ([`Transactor::validates_reads`]), or the datastore
	/// covers DiskANN guards as if it did, as the tests of that path have it
	/// do.
	pub fn validates_reads(&self) -> bool {
		self.tr.validates_reads() || self.triggers.pending_guard_fences.assumes_validated_reads()
	}

	/// Record that this transaction read the DiskANN pending-state guards of the
	/// index, so a guard it then writes under is covered in this transaction
	/// rather than by the arming transaction, whose write would fail this
	/// transaction's commit ([`Self::arm_diskann_pending_guards`]). Records
	/// nothing on a store that does not validate every read.
	pub fn note_diskann_guards_read(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) {
		if !self.validates_reads() {
			return;
		}
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).note_guards_read(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
		)
	}

	/// Register that this transaction wrote a DiskANN pending update into
	/// `shard` of the index, so the shard's pending-state guard (`!dy`) is
	/// covered when it commits ([`Self::arm_diskann_pending_guards`]): armed
	/// where this datastore is the store's sole writer, and otherwise held by
	/// this datastore's writer lease (`!dz`). Reads nothing, so the guard and
	/// the lease stay out of this transaction's reads.
	pub fn buffer_diskann_pending_guard(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		shard: u16,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_pending_guard(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			shard,
		)
	}

	/// The shards of the index this transaction has written a DiskANN pending
	/// update into that a rollback has not undone, as a bit per shard. Their
	/// guards are not armed until the commit, so a lookup in this transaction
	/// scans those shards whatever the stored guards say.
	pub fn diskann_buffered_pending_guard_shards(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> u64 {
		self.index_deltas.get().map_or(0, |buffer| buffer.pending_guard_shards(ns, db, tb, ix))
	}

	/// Whether `e` is a conditional write or delete that found a value other
	/// than the one it was conditioned on.
	#[cfg(diskann)]
	fn condition_not_met(e: &anyhow::Error) -> bool {
		matches!(storage_error(e), Some(KvsError::TransactionConditionNotMet))
	}

	/// Names the index's guard fence: the start of its `!dy` guard range, which
	/// is distinct per index and orders indexes the same way for every caller.
	#[cfg(diskann)]
	fn diskann_pending_guard_fence_id(
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<Vec<u8>> {
		Ok(DiskannPendingPrefix::new(ns, db, Cow::Borrowed(tb), ix).range()?.start().to_vec())
	}

	/// Record that this transaction wiped the data of a DiskANN index, its
	/// pending-state guards and writer leases included, so its commit holds the
	/// index's guard fence exclusively and forgets what this datastore knew of
	/// those guards and leases (`PendingGuardFences`).
	pub fn buffer_diskann_guard_wipe(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_guard_wipe(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
		)
	}

	/// Cover the DiskANN pending-state guards this transaction buffered
	/// ([`Self::buffer_diskann_pending_guard`]), returning their indexes'
	/// fences. The caller holds them until its commit has returned, then
	/// finishes them ([`DiskAnnGuardFences::finish`]).
	///
	/// The fences are taken in the order of the indexes' encoded prefixes, so
	/// every committer takes them in one order: shared for an index this
	/// transaction wrote into, exclusively for one whose data it wiped
	/// ([`Self::buffer_diskann_guard_wipe`]).
	///
	/// Where this datastore is the store's sole writer, a guard the fence knows
	/// to read `NonEmpty` needs nothing, and a commit needing nothing opens no
	/// transaction. The rest are armed in a transaction of their own, opened
	/// once the fences are held: a guard read `Empty`, `MaybeEmpty` or absent is
	/// set `NonEmpty`.
	///
	/// Elsewhere every commit that buffered a guard opens that transaction,
	/// and reads in it this datastore's lease on each shard the fence records
	/// it holding. A shard whose lease is there, with more than
	/// [`DISKANN_WRITER_LEASE_RENEWAL`] left, needs nothing: no node steps a
	/// guard under a lease, and a release forgets the lease under the exclusive
	/// fence before it goes. Any other shard (its lease near expiry, removed by a
	/// wipe or a retirement on another node, or not recorded held) has its
	/// guard read locked and written `NonEmpty` with a new generation, even if it
	/// already reads `NonEmpty`, and the lease written alongside with a fresh
	/// expiry. The guard write is what orders a lease against another node's step
	/// of the guard on every store, the locked read alone not conflicting with a
	/// step where the store validates reads rather than locking them: a step that
	/// read the guard before this commit fails its conditional write, so a step
	/// either sees the lease or is refused.
	///
	/// A transaction that wrote nothing is cancelled. Two writers covering one
	/// guard conflict only with each other, and the retry finds it covered.
	///
	/// To be covered, a guard is neither read nor written by this transaction,
	/// so neither a guard step nor another covering can fail its commit. The
	/// exception is a store that validates every read
	/// ([`Self::validates_reads`]), where a guard this transaction already read
	/// (a lookup in it, [`Self::note_diskann_guards_read`]) is covered in this
	/// transaction instead: the arming transaction's write would fail this
	/// commit, while the read already makes a step of the guard fail it.
	///
	/// A guard armed for a commit that then fails is left `NonEmpty` over a
	/// range holding nothing of it. Where this datastore is
	/// the sole writer, the arming queues a compaction pass for the index, whose
	/// guard step returns it to `Empty`; elsewhere the pass the lease's release
	/// queues does.
	#[cfg(diskann)]
	async fn arm_diskann_pending_guards(&self) -> Result<DiskAnnGuardFences> {
		let mut fences = DiskAnnGuardFences::default();
		let Some(buffer) = self.index_deltas.get() else {
			return Ok(fences);
		};
		let guards = buffer.take_pending_guards();
		let wipes = buffer.take_guard_wipes();
		// Two kinds of guard are covered here, in this transaction, rather than
		// by another one:
		// - a guard of an index whose data this transaction wiped: the wipe's delete commits with
		//   this transaction, after any other transaction's arming, and would remove it;
		// - on a store that validates every read, a guard this transaction read: another
		//   transaction's arming would fail this one's commit.
		// Either way this transaction already conflicts with a step of the
		// guard, by writing it or by having read it, so writing it adds no
		// conflict, and a commit that lands leaves the guard `NonEmpty` over its
		// entry. Nothing else is needed for it: no lease, and nothing left behind
		// if the commit fails.
		let read = buffer.take_guards_read();
		let guards = if read.is_empty() && wipes.is_empty() {
			guards
		} else {
			let (in_place, guards): (Vec<_>, Vec<_>) = guards
				.into_iter()
				.partition(|(index, _)| read.contains(index) || wipes.contains(index));
			let keys: Vec<_> = in_place
				.iter()
				.map(|(index, shard)| DiskannPendingKey {
					ns: index.ns,
					db: index.db,
					tb: Cow::Borrowed(&index.tb),
					ix: index.ix,
					shard: *shard,
				})
				.collect();
			// Written even where it already reads `NonEmpty`: a step that opened
			// before this commit and lands after it would otherwise step the guard
			// with nothing to conflict with, where the new generation makes its
			// conditional write fail.
			let currents = self.get_many_key(keys.clone(), None).await?;
			for (key, current) in keys.iter().zip(currents) {
				let next = DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: current.map_or(0, |s| s.generation).wrapping_add(1),
				};
				self.set_key(key, &next).await?;
			}
			guards
		};
		if guards.is_empty() && wipes.is_empty() {
			return Ok(fences);
		}
		let shared_store = !self.sole_writer();
		let fenced = &self.triggers.pending_guard_fences;
		// What each index needs, keyed by its encoded prefix.
		struct Needs {
			index: BufferedIndex,
			keys: Vec<DiskannPendingKey<'static>>,
			wiped: bool,
		}
		let mut by_index: std::collections::BTreeMap<Vec<u8>, Needs> = Default::default();
		for index in wipes {
			let prefix =
				Self::diskann_pending_guard_fence_id(index.ns, index.db, &index.tb, index.ix)?;
			by_index
				.entry(prefix)
				.or_insert_with(|| Needs {
					index,
					keys: Vec::new(),
					wiped: true,
				})
				.wiped = true;
		}
		for (index, shard) in guards {
			let prefix =
				Self::diskann_pending_guard_fence_id(index.ns, index.db, &index.tb, index.ix)?;
			// The index is cloned once, for its first shard; each key takes the
			// table name its buffered entry owns.
			let needs = by_index.entry(prefix).or_insert_with(|| Needs {
				index: index.clone(),
				keys: Vec::new(),
				wiped: false,
			});
			needs.keys.push(DiskannPendingKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Owned(index.tb),
				ix: index.ix,
				shard,
			});
		}
		// Take the fences, and keep the guards still to cover, with the index's
		// place among the shared fences and, elsewhere than on a sole writer, the
		// shards whose leases the fence records held, which are read before
		// relying on them. A wiped index has no guard left to cover here.
		let mut uncovered: Vec<(Vec<DiskannPendingKey<'static>>, usize, u64)> = Vec::new();
		for (prefix, needs) in by_index {
			let Needs {
				index,
				keys,
				wiped,
			} = needs;
			if wiped {
				fences.wiped.push(fenced.exclusive(&prefix, || index).await);
				continue;
			}
			let fence = fenced.shared(&prefix, || index).await;
			let (keys, held): (Vec<_>, u64) = if shared_store {
				let shards = keys.iter().fold(0u64, |shards, key| shards | 1 << key.shard);
				(keys, fence.holds_leases(shards))
			} else {
				(keys.into_iter().filter(|key| !fence.armed(key.shard)).collect(), 0)
			};
			fences.shared.push(fence);
			if !keys.is_empty() {
				uncovered.push((keys, fences.shared.len() - 1, held));
			}
		}
		if uncovered.is_empty() {
			return Ok(fences);
		}
		let deadline = self.cover_diskann_guards(&uncovered, shared_store).await?;
		// Every guard covered now reads `NonEmpty`, and no step can change that
		// while the fence is held, nor, elsewhere, while the lease written with it
		// holds.
		for (keys, i, _) in &uncovered {
			if shared_store {
				fences.shared[*i]
					.use_leases(keys.iter().fold(0, |shards, key| shards | 1 << key.shard));
			} else {
				for key in keys {
					fences.shared[*i].mark_armed(key.shard);
				}
			}
		}
		if let Some(deadline) = deadline {
			fences.leases_relied =
				Some((deadline, uncovered.into_iter().map(|(keys, i, _)| (keys, i, 0)).collect()));
		}
		Ok(fences)
	}

	/// Arm the guard of every shard of an index this commit wiped that still
	/// holds a pending entry, once the wipe has landed.
	///
	/// A wipe deletes the index's keys as its transaction read them, so an entry
	/// another transaction committed meanwhile, under a guard it knew armed,
	/// survives while its guard does not. The wipe's exclusive fence is held from
	/// its covering until here, so every writer of this datastore has either
	/// committed (its entry is seen) or waits to re-arm (the wipe forgets what was
	/// armed). Each such guard is set `NonEmpty`, with a compaction pass queued
	/// for the index. A failure is logged: the pass the hidden entry's own write
	/// queued repairs its guard (`Transaction::repair_diskann_pending_guards`).
	#[cfg(diskann)]
	async fn arm_diskann_guards_after_wipe(&self, fences: &DiskAnnGuardFences) {
		let result: Result<()> = async {
			let probe = self
				.sequences
				.transaction_factory()
				.transaction(TransactionType::Read, self.sequences.clone())
				.await?;
			let mut holding: Vec<(Vec<DiskannPendingKey<'static>>, usize, u64)> = Vec::new();
			let probed: Result<()> = async {
				for fence in &fences.wiped {
					let index = fence.index();
					let shards = probe
						.diskann_shards_holding_entries(index.ns, index.db, &index.tb, index.ix)
						.await?;
					let keys: Vec<_> = (0..DISKANN_PENDING_STATE_SHARDS)
						.filter(|shard| shards & (1 << shard) != 0)
						.map(|shard| DiskannPendingKey {
							ns: index.ns,
							db: index.db,
							tb: Cow::Owned(index.tb.clone()),
							ix: index.ix,
							shard,
						})
						.collect();
					if !keys.is_empty() {
						holding.push((keys, 0, 0));
					}
				}
				Ok(())
			}
			.await;
			let _ = probe.tr.cancel().await;
			probed?;
			if !holding.is_empty() {
				// Armed as a sole writer arms, with no lease: the entries have
				// landed, so no step on any node can find their shard empty.
				self.cover_diskann_guards(&holding, false).await?;
			}
			Ok(())
		}
		.await;
		if let Err(e) = result {
			tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"arming the DiskANN guards left over entries after a wipe failed: {e}"
			);
		}
	}

	/// The shards of the DiskANN index holding at least one pending entry
	/// (`!dw`), as a bit per shard.
	///
	/// Read by skipping: each probe reads the first entry from the shard after the
	/// last one found to the end of the index's pending entries, so the read costs
	/// one probe more than the shards holding entries, and one for an index whose
	/// pending entries are all compacted.
	#[cfg(diskann)]
	pub async fn diskann_shards_holding_entries(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> Result<u64> {
		let shards = (0..DISKANN_PENDING_STATE_SHARDS)
			.map(|shard| {
				DiskannRecordPendingShardShardPrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
					shard,
				}
				.range()
			})
			.collect::<Result<Vec<_>>>()?;
		let Some(last) = shards.last() else {
			return Ok(0);
		};
		let root = IdxRoot {
			ns,
			db,
			tb: Cow::Borrowed(tb),
			ix,
		}
		.raw_range()?;
		let end = last.end().as_slice().to_vec();
		let mut holding = 0u64;
		let mut from = 0;
		while from < shards.len() {
			let rest = root.part(shards[from].start().as_slice(), &end);
			let Some(key) = self.keys_raw(rest, 1, 0, None).await?.into_iter().next() else {
				break;
			};
			let Some(found) = shards[from..].iter().position(|shard| {
				key.as_slice() >= shard.start().as_slice()
					&& key.as_slice() < shard.end().as_slice()
			}) else {
				break;
			};
			holding |= 1 << (from + found);
			from += found + 1;
		}
		Ok(holding)
	}

	/// Arm the guards of `shards` of the index, which a compaction pass found
	/// `Empty` or absent over pending entries, and queue a compaction pass for
	/// the index.
	///
	/// The covering protocol leaves no entry under such a guard except where a
	/// repair after the fact failed or never ran: a wipe's arming of the guards it
	/// left over entries, and, on a store other datastores write to, a writer on
	/// another node committing across a wipe, or a commit that outlasted its lease
	/// and could not cover its guard again. Lookup and compaction skip such a shard,
	/// so its entries stay hidden until repaired, and each of those writes left
	/// a queue entry whose pass finds it here. Arming a guard over entries is
	/// always safe: no step finds their shard empty. The pass that found it does
	/// not compact those shards; the one queued here does.
	#[cfg(diskann)]
	pub async fn repair_diskann_pending_guards(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		shards: &[u16],
	) -> Result<()> {
		if shards.is_empty() {
			return Ok(());
		}
		tracing::warn!(
			target: "surrealdb::core::kvs::tx",
			"repairing {} DiskANN pending-state guards left empty over pending entries",
			shards.len()
		);
		let fenced = &self.triggers.pending_guard_fences;
		fenced.count_repairs(shards.len());
		let prefix = Self::diskann_pending_guard_fence_id(ns, db, tb, ix)?;
		let _fence = fenced
			.shared(&prefix, || BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			})
			.await;
		let keys = shards
			.iter()
			.map(|&shard| DiskannPendingKey {
				ns,
				db,
				tb: Cow::Owned(tb.clone()),
				ix,
				shard,
			})
			.collect();
		// Armed as a sole writer arms, with no lease, which also queues the pass.
		self.cover_diskann_guards(&[(keys, 0, 0)], false).await?;
		Ok(())
	}

	/// Cover again the guards a commit that has landed covered with writer
	/// leases, if it landed too close to the earliest of those leases' expiry.
	///
	/// A lease relied on at the covering can expire before the commit lands, if
	/// the commit stalls. Another node may then retire it and, finding the
	/// shard's pending range still empty, step its guard to `Empty`, leaving the
	/// landed entry under it. Its fences still held, the commit covers its guards
	/// again, without relying on any lease, once it lands within
	/// [`DISKANN_WRITER_LEASE_RENEWAL`] (the clock skew nodes may have) of that
	/// expiry. A lease that holds, by this node's clock, for longer than that once
	/// the commit has landed was not retired before it, and a step after it sees
	/// the entry. A failure is logged: the commit has landed, and its next
	/// covering of the shard sees the lease gone and arms the guard.
	#[cfg(diskann)]
	async fn recover_diskann_leases(&self, fences: &DiskAnnGuardFences) {
		let Some((deadline, guards)) = &fences.leases_relied else {
			return;
		};
		if Utc::now() + DISKANN_WRITER_LEASE_RENEWAL < *deadline {
			return;
		}
		match self.cover_diskann_guards(guards, true).await {
			// The leases written are relied on from now, so an idle release does
			// not take them as idle since before the stall.
			Ok(_) => {
				for (keys, i, _) in guards {
					fences.shared[*i]
						.use_leases(keys.iter().fold(0, |shards, key| shards | 1 << key.shard));
				}
			}
			Err(e) => tracing::warn!(
				target: "surrealdb::core::kvs::tx",
				"covering DiskANN guards again after a commit that outlasted its writer lease failed: {e}"
			),
		}
	}

	/// Cover `uncovered`'s guards, each listed with its index's place among the
	/// shared fences and the shards whose leases the fence records held, in a
	/// transaction of its own, retried against other coverings
	/// ([`Self::arm_diskann_pending_guards`]). Returns, on a store other
	/// datastores write to, the earliest expiry among the leases the covering
	/// relies on.
	#[cfg(diskann)]
	async fn cover_diskann_guards(
		&self,
		uncovered: &[(Vec<DiskannPendingKey<'static>>, usize, u64)],
		shared_store: bool,
	) -> Result<Option<chrono::DateTime<Utc>>> {
		let node = self.sequences.node_id();
		let process = self.triggers.pending_guard_fences.process();
		let mut attempt = 1;
		loop {
			let arm = self
				.sequences
				.transaction_factory()
				.transaction(TransactionType::Write, self.sequences.clone())
				.await?;
			// The earliest expiry among the leases this attempt relies on.
			let mut deadline: Option<chrono::DateTime<Utc>> = None;
			let wrote: Result<bool> = async {
				let mut wrote = false;
				let now = Utc::now();
				let expires_at = now + DISKANN_WRITER_LEASE_TTL;
				for (keys, _, held) in uncovered {
					// The leases the fence records held that this transaction still
					// reads with time to spare, read in one batch.
					let mut found = 0u64;
					if *held != 0 {
						let leased: Vec<_> =
							keys.iter().filter(|key| held & (1 << key.shard) != 0).collect();
						let leases = arm
							.get_many_key(
								leased
									.iter()
									.map(|key| diskann_writer_lease_key(key, node))
									.collect(),
								None,
							)
							.await?;
						for (key, lease) in leased.into_iter().zip(leases) {
							if let Some(lease) = lease.filter(|lease| {
								lease.process == process
									&& lease.expires_at > now + DISKANN_WRITER_LEASE_RENEWAL
							}) {
								found |= 1 << key.shard;
								deadline = Some(
									deadline.map_or(lease.expires_at, |d| d.min(lease.expires_at)),
								);
							}
						}
					}
					let mut index_armed = false;
					let covering: Vec<_> =
						keys.iter().filter(|key| found & (1 << key.shard) == 0).collect();
					// Read in one batch where a plain read suffices; a locked read is
					// taken key by key.
					let currents = if shared_store {
						let mut currents = Vec::with_capacity(covering.len());
						for key in &covering {
							currents.push(arm.getu_key(*key).await?);
						}
						currents
					} else {
						arm.get_many_key(
							covering
								.iter()
								.map(|key| DiskannPendingKey {
									ns: key.ns,
									db: key.db,
									tb: Cow::Borrowed(key.tb.as_ref()),
									ix: key.ix,
									shard: key.shard,
								})
								.collect(),
							None,
						)
						.await?
					};
					for (key, current) in covering.into_iter().zip(currents) {
						// Where a lease is taken the guard is written even if it reads
						// `NonEmpty`, so a step that read it before fails.
						if shared_store
							|| !current
								.as_ref()
								.is_some_and(|s| s.kind == DiskAnnPendingStateKind::NonEmpty)
						{
							let next = DiskAnnPendingState {
								kind: DiskAnnPendingStateKind::NonEmpty,
								generation: current
									.as_ref()
									.map_or(0, |s| s.generation)
									.wrapping_add(1),
							};
							arm.put_compare_key(key, &next, current.as_ref()).await?;
							index_armed = true;
						}
						if shared_store {
							arm.set_key(
								&diskann_writer_lease_key(key, node),
								&DiskAnnWriterLease {
									process,
									expires_at,
								},
							)
							.await?;
							deadline = Some(deadline.map_or(expires_at, |d| d.min(expires_at)));
							wrote = true;
						}
					}
					// Queue a pass for the index with the arming, so a guard this
					// commit armed is stepped back even if the commit then fails and
					// takes its own queue entry with it. A lease's release queues one
					// instead.
					if let (true, false, Some(key)) = (index_armed, shared_store, keys.first()) {
						let queued = IndexCompactionKey {
							ns: key.ns,
							db: key.db,
							tb: Cow::Borrowed(key.tb.as_ref()),
							ix: key.ix,
							nid: node,
							uid: Uuid::now_v7(),
						};
						arm.put_key(&queued, &()).await?;
					}
					wrote |= index_armed;
				}
				Ok(wrote)
			}
			.await;
			let committed = match wrote {
				Ok(true) => {
					// Committed without its own sync: this transaction's commit
					// follows it through the same store and makes it durable, and a
					// covering that does not outlive a crash took no committed write
					// with it.
					match arm.tr.commit_unsynced().await {
						Ok(()) => {
							if !shared_store {
								self.triggers.index_compaction.notify_one();
							}
							Ok(())
						}
						Err(e) => {
							let _ = arm.tr.cancel().await;
							Err(anyhow::Error::from(e))
						}
					}
				}
				Ok(false) => arm.tr.cancel().await.map_err(anyhow::Error::from),
				Err(e) => {
					let _ = arm.tr.cancel().await;
					Err(e)
				}
			};
			match committed {
				Ok(()) => return Ok(deadline),
				Err(e)
					if attempt < DISKANN_GUARD_ARM_ATTEMPTS
						&& (is_retryable_transaction_conflict(&e)
							|| Self::condition_not_met(&e)) =>
				{
					// Lost to another writer covering the same guard, reported as a
					// conflict or, by a backend that reports a lost conditional write
					// that way, as an unmet condition, from the write or the commit.
					// Back off before the retry, so the writers that lost to one winner
					// do not all retry in step and collide again.
					sleep(diskann_guard_arm_backoff(attempt)).await;
					attempt += 1;
				}
				Err(e) => return Err(e),
			}
		}
	}

	/// Without DiskANN no write buffers a guard, so there is nothing to cover.
	#[cfg(not(diskann))]
	async fn arm_diskann_pending_guards(&self) -> Result<DiskAnnGuardFences> {
		Ok(DiskAnnGuardFences::default())
	}

	/// Step to `Empty` each of `shards` of the index whose pending range
	/// (`!dw{shard}`) holds no entry, and report whether any guard was stepped.
	///
	/// The step holds the index's fence exclusively from before its transaction
	/// opens until it commits, so its snapshot holds every writer of this
	/// datastore that covered the guard before it, and no such writer commits
	/// between that snapshot and the step: one that covers the guard later reads
	/// the step and sets the guard back. Where this datastore is the store's sole
	/// writer, that is every writer.
	///
	/// Elsewhere a shard is stepped only if no writer lease (`!dz`) on it holds,
	/// this datastore's own included. A lease holds until its datastore releases
	/// it or it expires ([`DISKANN_WRITER_LEASE_TTL`]), whichever datastore took
	/// it; a lease this node took in an earlier process never holds, since this
	/// process knows it has none. Leases that no longer hold are deleted with
	/// the step, each on condition that it is still the one read, and an
	/// expired one of this datastore's is forgotten by its fence, so its next
	/// commit into the shard takes the lease again. A writer
	/// covering the shard concurrently reads the guard locked, so it and the step
	/// conflict and one of them is refused.
	///
	/// An emptiness seen here is therefore one no writer can still contradict,
	/// which is why a single step suffices. Every shard it steps, or tries to,
	/// is forgotten by the fence as armed. A step lost to a concurrent write of
	/// the guard is left for the next pass.
	#[cfg(diskann)]
	pub async fn step_diskann_pending_guards(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		shards: &[u16],
	) -> Result<bool> {
		if shards.is_empty() {
			return Ok(false);
		}
		let shared_store = !self.sole_writer();
		let fenced = &self.triggers.pending_guard_fences;
		let prefix = Self::diskann_pending_guard_fence_id(ns, db, tb, ix)?;
		let fence = fenced
			.exclusive(&prefix, || BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			})
			.await;
		let step = self
			.sequences
			.transaction_factory()
			.transaction(TransactionType::Write, self.sequences.clone())
			.await?;
		let (node, process, now) = (self.sequences.node_id(), fenced.process(), Utc::now());
		let mut stepping = Vec::new();
		let mut retired_own = 0u64;
		let mut wrote = false;
		let stepped: Result<bool> = async {
			for &shard in shards {
				let key = DiskannPendingKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
					shard,
				};
				let Some(current) = step.get_key(&key, None).await? else {
					continue;
				};
				if current.kind == DiskAnnPendingStateKind::Empty {
					continue;
				}
				if shared_store {
					let range = DiskannWriterLeaseShardPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
						ix,
						shard,
					}
					.range()?;
					let mut held = false;
					for (raw, lease) in step.scan(range, u32::MAX, 0, None).await? {
						let lease_key = DiskannWriterLeaseKey::decode_key(&raw)?;
						let own = lease_key.nid == node && lease.process == process;
						let stale = lease.expires_at <= now || (lease_key.nid == node && !own);
						if !stale {
							held = true;
							continue;
						}
						match step.del_compare_key(&lease_key, Some(&lease)).await {
							Ok(()) => {
								wrote = true;
								if own {
									retired_own |= 1 << shard;
								}
							}
							// Renewed since it was read: it holds.
							Err(e) if Self::condition_not_met(&e) => held = true,
							Err(e) => return Err(e),
						}
					}
					if held {
						continue;
					}
				}
				let range = DiskannRecordPendingShardShardPrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix,
					shard,
				}
				.range()?;
				if !step.keys(range, 1, 0, None).await?.is_empty() {
					continue;
				}
				let next = DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::Empty,
					generation: current.generation.wrapping_add(1),
				};
				match step.put_compare_key(&key, &next, Some(&current)).await {
					Ok(()) => {
						stepping.push(shard);
						wrote = true;
					}
					// A writer covered the guard since it was read: the shard is not
					// stepped, and the others still are.
					Err(e) if Self::condition_not_met(&e) => {}
					Err(e) => return Err(e),
				}
			}
			Ok(!stepping.is_empty())
		}
		.await;
		// Committed without its own sync, since writers wait on the fence while it
		// is held: a step that does not outlive a crash leaves the guard
		// `NonEmpty`, which only makes lookups scan a drained shard until the next
		// pass steps it again.
		let result = match stepped {
			Ok(stepped) if wrote => match step.tr.commit_unsynced().await {
				Ok(()) => Ok(stepped),
				Err(e) => {
					let _ = step.tr.cancel().await;
					let e = anyhow::Error::from(e);
					// Lost to a concurrent write of a guard, which a backend checking
					// conditional writes at commit reports as an unmet condition: the
					// step is left for the next pass.
					if is_retryable_transaction_conflict(&e) || Self::condition_not_met(&e) {
						Ok(false)
					} else {
						Err(e)
					}
				}
			},
			Ok(_) => step.tr.cancel().await.map(|()| false).map_err(anyhow::Error::from),
			Err(e) => {
				let _ = step.tr.cancel().await;
				Err(e)
			}
		};
		// Forgotten whatever the outcome: a step that may have landed must not
		// leave its shard recorded as armed, nor a lease it may have retired as
		// held. Forgetting a lease that is still there costs only its next
		// commit a re-take.
		for shard in stepping {
			fence.forget(shard);
		}
		if retired_own != 0 {
			fence.release_leases(retired_own);
		}
		result
	}

	/// Register that a graph adjacency scope wants folding once this
	/// transaction commits. Written by the edge-deletion path when it
	/// tombstones a pointer key on a folded table, so the tombstone is
	/// consumed even if the scope is never traversed again.
	pub fn buffer_graph_fold_trigger(&self, scope: GraphFoldScope, nid: Uuid) {
		self.index_deltas
			.get_or_init(|| Box::new(IndexDeltaBuffer::new()))
			.buffer_graph_fold_trigger(scope, nid)
	}

	/// Record a hot adjacency scope observed by a read, waking the fold
	/// task. Purely in-memory: reads cannot write a queue entry, so the
	/// fold task drains these into the durable queue on its pass.
	pub fn observe_graph_fold(&self, scope: GraphFoldScope) {
		self.triggers.observe_graph_fold(scope);
	}

	/// Record that a document gained (`add`) or lost a full-text term.
	///
	/// Buffered rather than written per (term, document): a `!tt` key per pair
	/// makes the delta log grow with indexed *work*, where one key per distinct
	/// term per transaction bounds it by the vocabulary. The flushed `!tx` entry
	/// is still a blind write of a key no other transaction shares.
	///
	/// The write-cardinality guard is charged here rather than at the flush, once
	/// for each key the flush will write. Charging at the flush would leave an
	/// over-limit statement on a caller-owned transaction reporting nothing until
	/// that caller committed, because nothing else in the statement writes a key
	/// per term any more. The count is the same either way; only when the caller
	/// learns of it differs.
	#[expect(clippy::too_many_arguments, reason = "the index identity is four fields")]
	pub fn buffer_term_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		term: &str,
		doc_id: u64,
		add: bool,
		nid: Uuid,
	) -> Result<()> {
		let fresh =
			self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_term_change(
				BufferedTerm {
					index: BufferedIndex {
						ns,
						db,
						tb: tb.clone(),
						ix,
					},
					term: term.to_string(),
				},
				doc_id,
				add,
				nid,
			);
		if fresh {
			let _changed = self.reserve_write_slot()?;
		}
		Ok(())
	}

	/// The document ids this transaction has buffered for one term but not yet
	/// written, as `(added, removed)`.
	///
	/// The full-text read path applies these over the committed entries it scans,
	/// so a query observes the documents its own transaction has just indexed.
	pub fn pending_term_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		term: &str,
	) -> (RoaringTreemap, RoaringTreemap) {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_term_change(&BufferedTerm {
				index: BufferedIndex {
					ns,
					db,
					tb: tb.clone(),
					ix,
				},
				term: term.to_string(),
			}),
			None => Default::default(),
		}
	}

	/// Add one document's length to a full-text index's running statistics.
	pub fn buffer_doc_stats(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
		stats: DocLengthAndCount,
		nid: Uuid,
	) {
		self.index_deltas.get_or_init(|| Box::new(IndexDeltaBuffer::new())).buffer_doc_stats(
			BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			},
			stats,
			nid,
		)
	}

	/// The document statistics this transaction has buffered but not yet written,
	/// so a scorer weighs the documents its own transaction has just indexed.
	pub fn pending_doc_stats(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		ix: IndexId,
	) -> DocLengthAndCount {
		match self.index_deltas.get() {
			Some(buffer) => buffer.pending_doc_stats(&BufferedIndex {
				ns,
				db,
				tb: tb.clone(),
				ix,
			}),
			None => DocLengthAndCount::default(),
		}
	}

	/// Write the buffered index deltas: one entry per index for the counts,
	/// compaction triggers and statistics, and one per term and direction for the
	/// term changes.
	///
	/// Called from [`Self::commit`] before the underlying commit, so the entries
	/// land in the same transaction as the document changes that produced them
	/// and the count stays atomic with the data. Like the changefeed flush,
	/// these writes are metered against the write-cardinality guard.
	async fn store_index_deltas(&self) -> Result<()> {
		let Some(buffer) = self.index_deltas.get() else {
			return Ok(());
		};
		if buffer.is_empty() {
			return Ok(());
		}
		for (index, delta, nid) in buffer.take_counts() {
			let key = IndexCountKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				uid: Some((nid, Uuid::now_v7())),
				pos: delta > 0,
				count: delta.unsigned_abs(),
			};
			self.put_key(&key, &()).await?;
		}
		let compactions = buffer.take_compactions();
		if !compactions.is_empty() {
			self.trigger_index_compaction();
		}
		for (index, nid) in compactions {
			let key = IndexCompactionKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				nid,
				uid: Uuid::now_v7(),
			};
			self.put_key(&key, &()).await?;
		}
		let graph_folds = buffer.take_graph_folds();
		if !graph_folds.is_empty() {
			self.trigger_graph_fold.store(true, Ordering::Relaxed);
		}
		for (scope, nid) in graph_folds {
			let key = GraphFoldKey {
				ns: scope.ns,
				db: scope.db,
				tb: Cow::Borrowed(&scope.tb),
				id: Cow::Borrowed(&scope.id),
				dir: scope.dir,
				nid,
				uid: Uuid::now_v7(),
			};
			self.put_key(&key, &()).await?;
		}
		// One discriminator for the whole flush. Every key it appears in is
		// already distinct within the flush — `!tx` by term and direction, `!dx`
		// by index — and separate transactions mint separate values, which is
		// what stops one transaction's contributions from overwriting another's.
		//
		// `!dx` is inserted rather than upserted, so a discriminator that did
		// collide is reported instead of silently absorbing one of the two
		// contributions. It can afford the existence check that costs, because it
		// is one key per index; `!tx` is one key per distinct term and cannot, so
		// it upserts and rests on the discriminator alone.
		//
		// Minted on first use: the value costs an entropy syscall, and most
		// commits reaching here carry only count deltas.
		let term_changes = buffer.take_term_changes();
		let doc_stats = buffer.take_doc_stats();
		let uid = if term_changes.is_empty() && doc_stats.is_empty() {
			Uuid::nil()
		} else {
			Uuid::now_v7()
		};
		for (term, delta) in term_changes {
			let index = &term.index;
			for (add, docs) in [(true, delta.added), (false, delta.removed)] {
				if docs.is_empty() {
					continue;
				}
				let key = TermChangeBatchKey {
					ns: index.ns,
					db: index.db,
					tb: Cow::Borrowed(&index.tb),
					ix: index.ix,
					term: Cow::Borrowed(&term.term),
					nid: delta.nid,
					uid,
					add,
				};
				self.set_key_prereserved(&key, &docs).await?;
			}
		}
		for (index, stats, nid) in doc_stats {
			let key = DocStatsBatchKey {
				ns: index.ns,
				db: index.db,
				tb: Cow::Borrowed(&index.tb),
				ix: index.ix,
				nid,
				uid,
			};
			self.put_key(&key, &stats).await?;
		}
		Ok(())
	}

	/// Records a record change into the dedicated live-query event buffer.
	///
	/// Independent of the changefeed: it always retains full before/after values
	/// and is flushed to the `lqe` keyspace at commit (see [`Self::store_changes`]).
	pub fn live_event_buffer_record_change(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordId,
		previous: Value,
		current: Value,
	) {
		self.live_events.get_or_init(LiveEventBuffer::new).buffer_record_change(
			ns,
			db,
			tb,
			id.clone(),
			previous,
			current,
		)
	}

	/// complete_changes will complete the changefeed recording for the given
	/// namespace and database.
	///
	/// This function writes all buffered changefeed entries to the datastore
	/// with the current transaction timestamp. Every change must be recorded by
	/// calling this struct's `changefeed_buffer_record_change` function beforehand.
	/// If there were no preceding calls for this transaction, this function
	/// will do nothing.
	///
	/// This function should be called only after all the changes have been made to
	/// the transaction. Otherwise, changes are missed in the change feed.
	///
	/// This function should be called immediately before calling the commit function
	/// to ensure the timestamp reflects the actual commit time.
	///
	/// The changefeed versionstamp is taken from [`Self::timestamp`] (the backend timestamp
	/// oracle). It is process-local-monotonic on mem/rocksdb/surrealkv and globally monotonic
	/// only on backends with a coordinated oracle (TiKV TSO) — so cross-node changefeed ordering
	/// is a property of the backend, not of this engine.
	pub async fn store_changes(&self) -> Result<()> {
		// Gather buffered changefeed entries (if any).
		let cf_changes = match self.changefeed.get() {
			Some(changefeed) => changefeed.changes()?,
			None => Vec::new(),
		};
		// Gather buffered live-query events (if any).
		let lqe_changes = match self.live_events.get() {
			Some(live_events) => live_events.changes()?,
			None => Vec::new(),
		};
		// Nothing buffered in either keyspace -> nothing to do.
		if cf_changes.is_empty() && lqe_changes.is_empty() {
			return Ok(());
		}
		// Both keyspaces share this commit's versionstamp.
		let buf = &mut [0u8; _];
		let ts = self.timestamp().await?.encode(buf);
		// Write the buffered changefeed entries. These commit-time writes are
		// part of the transaction's write set, so they are metered and
		// checked against the write-cardinality guard like any other write.
		// Writes are issued sequentially so each capacity check observes
		// every previous write — concurrent checks against a stale count
		// could otherwise admit more keys than the configured limit. Entries
		// are few (one per table per keyspace), so sequencing costs no
		// meaningful concurrency.
		for (ns, db, tb, value) in cf_changes {
			// Create the changefeed key with the current timestamp
			let key = ChangeFeedKey {
				ns,
				db,
				ts: Cow::Borrowed(ts),
				tb: Cow::Borrowed(&tb),
			}
			.encode_key()?;
			let _changed = self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			// Write the changefeed entry using the raw transactor API
			self.tr.set(key, value).await?;
			self.metrics.record_set(key_bytes, value_bytes);
		}
		// Write the live-query event entries to the dedicated keyspace,
		// metered, guarded, and sequenced like the changefeed writes above.
		for (ns, db, tb, value) in lqe_changes {
			let key = LiveEventsKey {
				ns,
				db,
				tb: Cow::Borrowed(&tb),
				ts: Cow::Borrowed(ts),
			}
			.encode_key()?;
			let _changed = self.reserve_write_slot()?;
			let key_bytes = key.len() as u64;
			let value_bytes = value.len() as u64;
			self.tr.set(key, value).await?;
			self.metrics.record_set(key_bytes, value_bytes);
		}
		// All good
		Ok(())
	}

	// --------------------------------------------------
	// Index functions
	// --------------------------------------------------

	/// Drain and release every queued durable index-build reservation.
	///
	/// Called from both the commit and cancel paths so that each admitted
	/// ticket is released exactly once after the user transaction terminates.
	/// All reservations are attempted even when one fails; the first error is
	/// preserved and returned so the caller can surface it while later
	/// releases still get a chance to run.
	///
	/// As an optimization, when there are multiple queued reservations they
	/// are deleted in a single short transaction instead of one transaction
	/// per release. A user transaction that wrote to several indexes therefore
	/// pays one commit at close time instead of one per index. If the batch
	/// commit fails (retryable conflict, snapshot conflict on local engines,
	/// or a non-retryable storage error), the function falls back to the
	/// per-reservation release path so each failure can drive its own
	/// `mark_build_error_if_uncommitted` reasoning.
	#[inline(never)]
	fn release_index_build_reservations(&self) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.release_index_build_reservations_body())
	}

	async fn release_index_build_reservations_body(&self) -> Result<()> {
		// Take the queued reservations under the lock so concurrent registrations
		// see an empty queue while releases are in flight.
		let reservations = {
			let mut pending = self.pending_index_build_reservations.lock().await;
			std::mem::take(&mut *pending)
		};
		if reservations.is_empty() {
			return Ok(());
		}
		// Try the batched delete first, then fall back to the per-reservation
		// path on any error. The fallback preserves the per-reservation
		// retry + build-error-marking behavior for any release that didn't
		// already succeed in the batch.
		let reservations = match IndexBuildReservationRelease::release_batch(reservations).await {
			Ok(()) => return Ok(()),
			Err(remaining) => remaining,
		};
		// Per-reservation slow path. Reused for the rare case where the batch
		// failed; each release individually retries on retryable conflicts
		// and marks the build errored if its appendings never landed.
		let mut first_error = None;
		for reservation in reservations {
			if let Err(err) = reservation.release().await
				&& first_error.is_none()
			{
				first_error = Some(err);
			}
		}
		if let Some(err) = first_error {
			Err(err)
		} else {
			Ok(())
		}
	}

	/// Run every queued rollback action.
	///
	/// Every action is attempted even when one fails, and the first error is
	/// returned: each answers to durable state written by a different
	/// transaction, so skipping the rest would leave that state orphaned.
	///
	/// The queue runs one action at a time, so they are all handed the instant
	/// the drain began: an action that waits bounds itself against that, and a
	/// transaction that queued several cannot turn one allowance into several.
	async fn run_rollback_actions(&self) -> Result<()> {
		// Take the queue under the lock to detach it from concurrent registrations
		let actions = {
			let mut pending = self.rollback_actions.lock().await;
			std::mem::take(&mut *pending)
		};
		let drain_started_at = Instant::now();
		let mut first_error = None;
		for action in actions {
			if let Err(err) = action.run(drain_started_at).await
				&& first_error.is_none()
			{
				first_error = Some(err);
			}
		}
		match first_error {
			Some(err) => Err(err),
			None => Ok(()),
		}
	}

	/// Drop the queued rollback actions without running them.
	///
	/// Invoked once the commit has succeeded: the state they would remove is no
	/// longer provisional.
	async fn discard_rollback_actions(&self) {
		self.rollback_actions.lock().await.clear();
	}

	/// Run every queued commit action.
	///
	/// Invoked after a successful commit, so everything they name is durably in
	/// place. Infallible by contract; an action logs its own failures.
	async fn run_commit_actions(&self) {
		let actions = {
			let mut pending = self.commit_actions.lock().await;
			std::mem::take(&mut *pending)
		};
		for action in actions {
			action.run().await;
		}
	}

	/// Drop the queued commit actions and after-commit waits without running
	/// them.
	///
	/// Invoked on cancel and on commit failure: the change each answers to never
	/// became durable, so the effect must not happen either.
	async fn discard_commit_actions(&self) {
		self.commit_actions.lock().await.clear();
		if let Some(waits) = self.after_commit_waits.as_ref().and_then(std::sync::Weak::upgrade) {
			waits.lock().await.clear();
		}
	}

	// --------------------------------------------------
	// Cache functions
	// --------------------------------------------------

	/// Bump the given table's `cache_lives_ts`, committing the change with this
	/// transaction. The live-query cache keys on this committed timestamp (see
	/// [`surrealdb_catalog::StoredTableDefinition::cache_lives_ts`]), so callers that
	/// change the set of live queries on a table (LIVE / KILL) must call this in
	/// the same transaction as the live-query row write. The table's committed
	/// IDs are taken from `tb`, so no namespace/database name lookup is needed.
	///
	/// The table's catalog version is left as it is: the live-query stamp is
	/// not part of the catalog a bound statement depends on, so a registration
	/// neither evicts the node-wide [`crate::cache::ds::TableSnapshot`] nor
	/// conflicts with anything but writes to this table. Consequently a
	/// definition read from that snapshot carries the `cache_lives_ts` of the
	/// last catalog write, not of the last registration; the live-query cache
	/// must key on a definition read through the transaction.
	///
	/// The stamp is written under a locked read of the definition
	/// ([`Self::getu_key`]), as [`Self::replace_tb`] writes every DDL change,
	/// and the value written is the one that read returned. A registration
	/// that races a catalog write to the same table therefore conflicts at
	/// commit instead of overwriting it: on a last-writer-wins backend a blind
	/// rewrite of the whole definition would revert a concurrently committed
	/// `ALTER TABLE` while the table's version still named the newer state, so
	/// the node-wide snapshot and a read through the transaction would
	/// disagree on the table's permissions.
	#[inline(never)]
	pub fn bump_table_lives_cache(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> OutOfLine<impl Future<Output = Result<()>>> {
		OutOfLine::new(self.bump_table_lives_cache_body(ns, db, tb))
	}

	async fn bump_table_lives_cache_body(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		let key = TableKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let Some(mut stored) = self.getu_key(&key).await? else {
			// The table is gone (e.g. removed in the same batch); nothing to do.
			return Ok(());
		};
		let version_key = CatalogVersionKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let prior = self.table_stored(ns, db, tb).await?;
		let proven = match self.get_key(&version_key, None).await? {
			Some(stamped) => stamped.proven(prior.as_deref()),
			None => None,
		};
		stored.cache_lives_ts = uuid::Uuid::now_v7();
		self.set_key(&key, &stored).await?;
		// The registration changes no catalog state, so a version that named
		// the table's state before it names it after: it is printed again
		// from the rewritten definition, uncounted as a stamp.
		if let Some(version) = proven {
			let stored = self.table_stored(ns, db, tb).await?;
			let reprinted = CatalogVersion::stamped(version, stored.as_deref());
			let _changed = self.reserve_write_slot()?;
			self.tr.set(version_key.encode_key()?, reprinted.kv_encode_value()?).await?;
		}
		// Every cached view of this table carries a stale `cache_lives_ts`:
		// the by-id and by-name definitions, the database's table list, and the
		// table's compiled live-query list. A re-read within this transaction
		// serving any of them would key the datastore live-query cache on the
		// pre-bump timestamp and fan notifications from the pre-bump subscriber
		// list, so the whole transaction cache is dropped. A subset cannot be
		// refreshed in any case: the by-name entry is keyed on the namespace and
		// database *names*, which this call does not have.
		self.clear_cache();
		Ok(())
	}

	/// Clears all keys from the transaction cache.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip(self))
	)]
	pub fn clear_cache(&self) {
		self.cache.clear()
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn compact<K>(&self, key: &K) -> Result<()>
	where
		K: KVSubspace + Debug,
	{
		let range = key.raw_range()?;
		self.tr.inner.compact(Some(range.into_key_range())).await?;
		Ok(())
	}

	/// [`Self::compact`] over a range a program encoded itself.
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn compact_raw(&self, range: impl AnyRange) -> Result<()> {
		self.tr.inner.compact(Some(range.into_key_range())).await?;
		Ok(())
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(level = "trace", target = "surrealdb::core::kvs::tx", skip_all)
	)]
	pub async fn compact_all(&self) -> Result<()> {
		self.tr.inner.compact(None).await?;
		Ok(())
	}

	/// Mark this transaction to wake the async event processor after commit.
	/// Mark that this transaction queued index-compaction work, so the
	/// compactor is woken once the commit makes it visible.
	pub fn trigger_index_compaction(&self) {
		self.trigger_index_compaction.store(true, Ordering::Relaxed);
	}

	/// Mark that this transaction queued table migration work, so the job is woken once the
	/// commit makes it visible.
	pub fn trigger_table_migration(&self) {
		self.trigger_table_migration.store(true, Ordering::Relaxed);
	}

	pub fn trigger_async_event(&self) {
		self.trigger_async_event.store(true, Ordering::Relaxed);
	}

	/// The views among `registered`, the registrations stored under table `tb`, whose own
	/// definition at `version` is a view that reads `tb`.
	///
	/// A registration is the copy of a view's definition that `DEFINE TABLE ... AS SELECT` writes
	/// under each table the view reads. One whose view table has since been removed, or redefined
	/// as a plain table or as a view over other tables, names a view `tb` no longer feeds, and is
	/// left out. Answers `registered` itself when every registration is current.
	async fn views_reading(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		registered: Arc<[TableDefinition]>,
		version: Option<u64>,
	) -> Result<Arc<[TableDefinition]>> {
		let mut current = Vec::with_capacity(registered.len());
		for registration in registered.iter() {
			let reads =
				self.get_tb(ns, db, &registration.name, version).await?.is_some_and(|view| {
					view.view.as_ref().is_some_and(|view| view.source_tables().contains(tb))
				});
			if reads {
				current.push(registration.clone());
			}
		}
		if current.len() == registered.len() {
			return Ok(registered);
		}
		Ok(current.into())
	}
}

// --------------------------------------------------
// Node implementation functions
// --------------------------------------------------

impl NodeProvider for Transaction {
	/// Retrieve all nodes belonging to this cluster.
	fn all_nodes(&self) -> BoxProviderFut<'_, Result<Arc<[Node]>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Nds;
				match self.cache.get(&qey) {
					Some(val) => val.try_into_nds(),
					None => {
						let range = NodePrefix {}.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let val =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let entry = cache::tx::Entry::Nds(Arc::clone(&val));
						self.cache.insert(qey, entry);
						Ok(val)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_nodes")),
		)
	}

	/// Retrieve a specific node in the cluster.
	fn get_node(&self, id: Uuid) -> BoxProviderFut<'_, Result<Option<Arc<Node>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Nd(id);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = NodeKey {
							nd: id,
						};
						let Some(val) = self.get_key(&key, None).await? else {
							return Ok(None);
						};
						let val = cache::tx::Entry::Any(Arc::new(val));
						self.cache.insert(qey, val.clone());
						val.try_into_type().map(Option::Some)
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_node")),
		)
	}
}

// --------------------------------------------------
// Root implementation functions
// --------------------------------------------------

impl RootProvider for Transaction {
	fn get_default_config(&self) -> BoxProviderFut<'_, Result<Option<Arc<DefaultConfig>>>> {
		Box::pin(async move {
			self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
			let qey = cache::tx::Lookup::Rcg("default");
			match self.cache.get(&qey) {
				Some(val) => val,
				None => {
					let key = RootConfigKey {
						ty: Cow::Borrowed("default"),
					};
					let Some(val) = self.get_key(&key, None).await? else {
						return Ok(None);
					};
					let StoredConfigDefinition::Default(val) = val else {
						fail!("Expected a default config but found {val:?} instead");
					};
					let val = cache::tx::Entry::Any(Arc::new(val));
					self.cache.insert(qey, val.clone());
					val
				}
			}
			.try_into_type()
			.map(Option::Some)
		})
	}

	/// Retrieve a specific config definition from the root.
	fn get_root_config<'a>(
		&'a self,
		cg: &'a str,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Rcg(cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = RootConfigKey {
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
							let val = Arc::new(catalog::ConfigDefinition::from_stored(&val)?);
							let entr = cache::tx::Entry::Any(val.clone());
							self.cache.insert(qey, entr);
							Ok(Some(val))
						} else {
							Ok(None)
						}
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_config")),
		)
	}
}

// --------------------------------------------------
// Namespace implementation functions
// --------------------------------------------------

impl NamespaceProvider for Transaction {
	/// Retrieve all namespace definitions in a datastore.
	fn all_ns(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[NamespaceDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Nss;
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_nss();
				}
				let range = NamespacePrefix {}.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Nss(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns")),
		)
	}

	fn get_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<NamespaceDefinition>>>> {
		Box::pin(async move {
			let qey = cache::tx::Lookup::NsByName(ns);
			if version.is_none() {
				if let Some(val) = self.cache.get(&qey) {
					return val.try_into_type().map(Some);
				}
				if let Some((epoch, shared)) = self.epoch_catalog()
					&& let Some(def) = shared.get::<NamespaceDefinition>(
						&crate::cache::epoch::Lookup::Namespace(epoch, ns),
					) {
					self.cache.insert(qey, cache::tx::Entry::Any(def.clone()));
					return Ok(Some(def));
				}
			}
			let name = ns;
			let key = NamespaceKey {
				ns: Cow::Borrowed(ns),
			};
			let Some(ns) = self.get_key(&key, version).await? else {
				return Ok(None);
			};

			let ns = Arc::new(ns);
			if version.is_none() {
				let entr = cache::tx::Entry::Any(ns.clone());
				self.cache.insert(qey, entr);
				self.keep_for_epoch(
					|epoch| crate::cache::epoch::Lookup::Namespace(epoch, name),
					&ns,
				);
			}
			Ok(Some(ns))
		})
	}

	fn expect_ns_by_name<'a>(
		&'a self,
		ns: &'a str,
	) -> BoxProviderFut<'a, Result<Arc<NamespaceDefinition>>> {
		Box::pin(async move {
			match self.get_ns_by_name(ns, None).await? {
				Some(val) => Ok(val),
				None => anyhow::bail!(CatalogError::NsNotFound {
					name: ns.to_owned(),
				}),
			}
		})
	}

	fn put_ns(
		&self,
		ns: NamespaceDefinition,
	) -> BoxProviderFut<'_, Result<Arc<NamespaceDefinition>>> {
		Box::pin(async move {
			let key = NamespaceKey {
				ns: Cow::Borrowed(&ns.name),
			};
			self.set_key(&key, &ns).await?;

			// Invalidate the cached list of all namespaces
			let list_key = cache::tx::Lookup::Nss;
			self.cache.remove(&list_key);

			// Populate cache
			let cached_ns = Arc::new(ns.clone());

			let entry = cache::tx::Entry::Any(Arc::clone(&cached_ns) as Arc<dyn Any + Send + Sync>);
			let qey = cache::tx::Lookup::NsByName(&ns.name);
			self.cache.insert(qey, entry);

			Ok(cached_ns)
		})
	}

	fn get_next_ns_id<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
	) -> BoxProviderFut<'a, Result<NamespaceId>> {
		Box::pin(async move { self.sequences.next_namespace_id(ctx).await })
	}
}

// --------------------------------------------------
// Database implementation functions
// --------------------------------------------------

impl DatabaseProvider for Transaction {
	/// Retrieve all database definitions for a specific namespace.
	fn all_db(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[DatabaseDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Dbs(ns);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_dbs();
				}
				let range = DatabasePrefix {
					ns,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Dbs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db")),
		)
	}

	/// Retrieve a specific database definition.
	fn get_db_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<DatabaseDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Database(ns, db));
				let qey = cache::tx::Lookup::DbByName(ns, db);
				if version.is_none() {
					if let Some(val) = self.cache.get(&qey) {
						return val.try_into_type().map(Some);
					}
					if let Some((epoch, shared)) = self.epoch_catalog()
						&& let Some(def) = shared.get::<DatabaseDefinition>(
							&crate::cache::epoch::Lookup::Database(epoch, ns, db),
						) {
						self.cache.insert(qey, cache::tx::Entry::Any(def.clone()));
						return Ok(Some(def));
					}
				}
				let ns_name = ns;
				let Some(ns) = self.get_ns_by_name(ns, version).await? else {
					return Ok(None);
				};

				let key = DatabaseKey {
					ns: ns.namespace_id,
					db: Cow::Borrowed(db),
				};
				let Some(db_def) = self.get_key(&key, version).await? else {
					return Ok(None);
				};

				let val = Arc::new(db_def);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
					self.keep_for_epoch(
						|epoch| crate::cache::epoch::Lookup::Database(epoch, ns_name, db),
						&val,
					);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_by_name")),
		)
	}

	/// Get or add a database with a default configuration, only if we are in
	/// dynamic mode.
	fn get_or_add_db_upwards<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
		ns: &'a str,
		db: &'a str,
		upwards: bool,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::DbByName(ns, db);
				match self.cache.get(&qey) {
					// The entry is in the cache
					Some(val) => {
						let t = val.try_into_type()?;
						Ok(t)
					}
					// The entry is not in the cache
					None => {
						let db_def = self.get_db_by_name(ns, db, None).await?;
						if let Some(db_def) = db_def {
							return Ok(db_def);
						}

						let ns_def = if upwards {
							self.get_or_add_ns(ctx, ns).await?
						} else {
							match self.get_ns_by_name(ns, None).await? {
								Some(ns_def) => ns_def,
								None => {
									return Err(CatalogError::NsNotFound {
										name: ns.to_owned(),
									}
									.into());
								}
							}
						};

						let database_id = self.get_next_db_id(ctx, ns_def.namespace_id).await?;
						let db_def = DatabaseDefinition {
							namespace_id: ns_def.namespace_id,
							database_id,
							name: db.into(),
							comment: None,
							changefeed: None,
							strict: false,
							compatibility: Unrevisioned(
								self.implicit_compatibility(ns_def.namespace_id, database_id)
									.await?,
							),
						};

						return self.put_db(ns_def.name.as_str(), db_def).await;
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_or_add_db_upwards")),
		)
	}

	fn get_next_db_id<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
		ns: NamespaceId,
	) -> BoxProviderFut<'a, Result<DatabaseId>> {
		Box::pin(async move { self.sequences.next_database_id(ctx, ns).await })
	}

	fn put_db<'a>(
		&'a self,
		ns: &'a str,
		db: DatabaseDefinition,
	) -> BoxProviderFut<'a, Result<Arc<DatabaseDefinition>>> {
		Box::pin(async move {
			let key = DatabaseKey {
				ns: db.namespace_id,
				db: Cow::Borrowed(&db.name),
			};
			self.set_key(&key, &db).await?;

			// Invalidate the cached list of all databases for this namespace
			let list_key = cache::tx::Lookup::Dbs(db.namespace_id);
			self.cache.remove(&list_key);

			// Populate cache
			let cached_db = Arc::new(db.clone());

			let entry = cache::tx::Entry::Any(Arc::clone(&cached_db) as Arc<dyn Any + Send + Sync>);
			let qey = cache::tx::Lookup::DbByName(ns, &db.name);
			self.cache.insert(qey, entry);

			Ok(cached_db)
		})
	}

	/// Retrieve all analyzer definitions for a specific database.
	fn all_db_analyzers(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AnalyzerDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Azs(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_azs();
				}
				let range = AnalyzerPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Azs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_analyzers")),
		)
	}

	/// Retrieve all sequences definitions for a specific database.
	fn all_db_sequences(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::SequenceDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Sqs(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_sqs();
				}
				let range = SequencePrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Sqs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_sequences")),
		)
	}

	/// Retrieve all function definitions for a specific database.
	fn all_db_functions(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::FunctionDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Fcs(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_fcs();
				}
				let range = FunctionPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredFunctionDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Fcs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_functions")),
		)
	}

	/// Retrieve all module definitions for a specific database.
	fn all_db_modules(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ModuleDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Mds(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_mds();
				}
				let range = ModulePrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredModuleDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Mds(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_modules")),
		)
	}

	/// Retrieve all param definitions for a specific database.
	fn all_db_params(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ParamDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Pas(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_pas();
				}
				let range = ParamPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredParamDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Pas(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_params")),
		)
	}

	/// Retrieve all model definitions for a specific database.
	fn all_db_models(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::MlModelDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Mls(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_mls();
				}
				let range = MlModelPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredMlModelDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Mls(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_models")),
		)
	}

	/// Retrieve all config definitions for a specific database.
	fn all_db_configs(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ConfigDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Cgs(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_cgs();
				}
				let range = DbConfigPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[StoredConfigDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Cgs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_configs")),
		)
	}

	/// Retrieve a specific model definition from a database.
	fn get_db_model<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ml: &'a str,
		vn: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::MlModelDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Ml(ns, db, ml, vn);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = MlModelKey {
					ns,
					db,
					ml: Cow::Borrowed(ml),
					vn: Cow::Borrowed(vn),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::MlModelDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_model")),
		)
	}

	/// Retrieve a specific analyzer definition from a database.
	fn get_db_analyzer<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		az: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::AnalyzerDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Az(ns, db, az);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = AnalyzerKey {
					ns,
					db,
					az: Cow::Borrowed(az),
				};
				let val =
					self.get_key(&key, version).await?.ok_or_else(|| CatalogError::AzNotFound {
						name: az.to_owned(),
					})?;
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_analyzer")),
		)
	}

	/// Retrieve a specific sequence definition from a database.
	fn get_db_sequence<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		sq: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::SequenceDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Sq(ns, db, sq);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = SequenceKey {
					ns,
					db,
					sq: Cow::Borrowed(sq),
				};
				let val = self.get_key(&key, version).await?.ok_or_else(|| {
					CatalogError::SeqNotFound {
						name: sq.to_owned(),
					}
				})?;
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_sequence")),
		)
	}

	/// Retrieve a specific function definition from a database.
	fn get_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::FunctionDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Function(ns, db, fc));
				let qey = cache::tx::Lookup::Fc(ns, db, fc);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = FunctionKey {
					ns,
					db,
					fc: Cow::Borrowed(fc),
				};
				let val =
					self.get_key(&key, version).await?.ok_or_else(|| CatalogError::FcNotFound {
						name: format!("fn::{fc}"),
					})?;
				let val = Arc::new(catalog::FunctionDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_function")),
		)
	}

	/// Retrieve a specific module definition from a database.
	fn get_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ModuleDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Md(ns, db, md);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = ModuleKey {
					ns,
					db,
					md: Cow::Borrowed(md),
				};
				let val =
					self.get_key(&key, version).await?.ok_or_else(|| CatalogError::MdNotFound {
						name: md.to_owned(),
					})?;
				let val = Arc::new(catalog::ModuleDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_module")),
		)
	}

	/// Retrieve a specific param definition from a database.
	fn get_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::ParamDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Param(ns, db, pa));
				let qey = cache::tx::Lookup::Pa(ns, db, pa);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = ParamKey {
					ns,
					db,
					pa: Cow::Borrowed(pa),
				};
				let val =
					self.get_key(&key, version).await?.ok_or_else(|| CatalogError::PaNotFound {
						name: pa.to_owned(),
					})?;
				let val = Arc::new(catalog::ParamDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_param")),
		)
	}

	/// Retrieve a specific config definition from a database.
	fn get_db_config<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		cg: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ConfigDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				if version.is_some() {
					let key = DbConfigKey {
						ns,
						db,
						ty: Cow::Borrowed(cg),
					};
					if let Some(val) = self.get_key(&key, version).await? {
						return Ok(Some(Arc::new(catalog::ConfigDefinition::from_stored(&val)?)));
					} else {
						return Ok(None);
					}
				}
				let qey = cache::tx::Lookup::Cg(ns, db, cg);
				match self.cache.get(&qey) {
					Some(val) => val.try_into_type().map(Option::Some),
					None => {
						let key = DbConfigKey {
							ns,
							db,
							ty: Cow::Borrowed(cg),
						};
						if let Some(val) = self.get_key(&key, None).await? {
							let val = Arc::new(catalog::ConfigDefinition::from_stored(&val)?);
							let entr = cache::tx::Entry::Any(val.clone());
							self.cache.insert(qey, entr);
							Ok(Some(val))
						} else {
							Ok(None)
						}
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_config")),
		)
	}

	fn put_db_function<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		fc: &'a catalog::FunctionDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let stored = fc.to_stored();
			let key = FunctionKey {
				ns,
				db,
				fc: Cow::Borrowed(&fc.name),
			};
			self.set_key(&key, &stored).await?;

			// Invalidate the cached list of all functions for this database
			let list_key = cache::tx::Lookup::Fcs(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Fc(ns, db, &fc.name);
			let entry = cache::tx::Entry::Any(Arc::new(fc.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_module<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		md: &'a catalog::ModuleDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let stored = md.to_stored();
			let name = stored.get_storage_name()?;
			let key = ModuleKey {
				ns,
				db,
				md: Cow::Borrowed(name.as_str()),
			};
			self.set_key(&key, &stored).await?;

			// Invalidate the cached list of all modules for this database
			let list_key = cache::tx::Lookup::Mds(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Md(ns, db, &name);
			let entry = cache::tx::Entry::Any(Arc::new(md.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_param<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		pa: &'a catalog::ParamDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let stored = pa.to_stored();
			let key = ParamKey {
				ns,
				db,
				pa: Cow::Borrowed(&pa.name),
			};
			self.set_key(&key, &stored).await?;

			// Invalidate the cached list of all params for this database
			let list_key = cache::tx::Lookup::Pas(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Pa(ns, db, &pa.name);
			let entry = cache::tx::Entry::Any(Arc::new(pa.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Table implementation functions
// --------------------------------------------------

/// Compile stored live-query subscriptions.
///
/// Every stored subscription appears in the result, including one whose text
/// does not compile: that is carried as
/// [`catalog::SubscriptionQuery::Uncompilable`] rather than dropped. A
/// subscription in that state can never match another document, but it still
/// has `lq`/`lv` keys and a client waiting on them, so the statements that tear
/// subscriptions down or report them must still see it. Dropping it here would leave
/// such a client waiting on a `KILLED` that could never
/// arrive, and would hide the row from the `INFO` an operator would use to find it.
///
/// Compiling must also not fail the reading statement, which is the other half
/// of the same requirement: the subscription belongs to some other client, and
/// every write to the table reads this list, so an error would turn one
/// unreadable row into an outage for the whole table.
///
/// Callers memoize the returned list against the table's `cache_lives_ts`. That
/// cannot hide a subscription that would otherwise be delivered to: compiling is
/// a pure function of the stored bytes, which never change for a given
/// subscription, and the timestamp is bumped whenever the set of subscriptions
/// on the table changes.
fn compile_subscriptions(
	tb: &TableName,
	stored: &[catalog::StoredSubscriptionDefinition],
) -> Arc<[catalog::SubscriptionDefinition]> {
	stored
		.iter()
		.map(|lv| {
			let compiled = catalog::SubscriptionDefinition::compile(lv);
			if matches!(compiled.query, catalog::SubscriptionQuery::Uncompilable { .. }) {
				// Debug, not warn, and without the compile error: this runs on
				// every `Lookup::Lvs` cache miss, which is every mutation of the
				// table, and the condition cannot self-heal because the stored
				// bytes never change. At warn level one uncompilable row emits a
				// line per write.
				//
				// The error is omitted because the parser renders a snippet of
				// the offending source into it, and that is the user's own query
				// text — a `WHERE` clause is exactly where a literal would sit.
				// The id and table are what an operator needs to find and `KILL`
				// it; `INFO ... STRUCTURE` carries the reason.
				tracing::debug!(
					target: "surrealdb::core::kvs",
					subscription_id = %lv.id,
					table = %tb,
					"LIVE subscription is inert: its stored text no longer compiles, so it will \
					 receive no further notifications until it is killed"
				);
			}
			compiled
		})
		.collect()
}

/// What identifies the catalog of every table of a database; see
/// [`Transaction::database_catalog_state`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabaseCatalogState {
	/// The tables the database defines, in key order.
	pub tables: Vec<TableName>,
	/// Every proven catalog version stamped in the database, by table, in key
	/// order: those of the tables it defines and of tables since removed.
	pub versions: Vec<(TableName, Uuid)>,
}

/// The `REFERENCE` fields of every table of a database, and the catalog
/// state they were read in ([`Transaction::reference_fields`]).
#[derive(Debug)]
pub struct ReferenceFields {
	/// The catalog of every table of the database the fields were read in.
	pub state: Arc<DatabaseCatalogState>,
	/// Each table that defines a `REFERENCE` field, in the order of
	/// `state.tables`, with those fields.
	pub tables: Vec<(TableName, Arc<[catalog::FieldDefinition]>)>,
}

impl TableProvider for Transaction {
	/// Retrieve all table definitions for a specific database.
	fn all_tb(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[TableDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				self.all_tb_listed(ns, db, version).await
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb")),
		)
	}

	/// Retrieve all view definitions for a specific table: the registrations stored under it
	/// whose view still reads it ([`Transaction::views_reading`]).
	fn all_tb_views<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[TableDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Fts(ns, db, tb.as_str());
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_fts();
				}
				let range = ForeignTablePrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[StoredTableDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				let val = self.views_reading(ns, db, tb, val, version).await?;
				if version.is_none() {
					let entry = cache::tx::Entry::Fts(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_views")),
		)
	}

	/// Get or add a table with a default configuration, only if we are in
	/// dynamic mode. When a version is specified, skips the auto-create path.
	fn get_or_add_tb<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(
			async move {
				if version.is_some() {
					let Some(db_def) = self.get_db_by_name(ns, db, version).await? else {
						return Err(anyhow::anyhow!(CatalogError::DbNotFound {
							name: db.to_owned(),
						}));
					};
					let table_key = TableKey {
						ns: db_def.namespace_id,
						db: db_def.database_id,
						tb: Cow::Borrowed(tb),
					};
					if let Some(tb_def) = self.get_key(&table_key, version).await? {
						return Ok(Arc::new(TableDefinition::from_stored(&tb_def)?));
					}
					return Err(CatalogError::TbNotFound {
						name: tb.to_owned(),
					}
					.into());
				}
				let qey = cache::tx::Lookup::TbByName(ns, db, tb.as_str());
				match self.cache.get(&qey) {
					// The entry is in the cache
					Some(val) => val.try_into_type(),
					// The entry is not in the cache
					None => {
						let Some(db_def) = self.get_db_by_name(ns, db, None).await? else {
							return Err(anyhow::anyhow!(CatalogError::DbNotFound {
								name: db.to_owned(),
							}));
						};

						let table_key = TableKey {
							ns: db_def.namespace_id,
							db: db_def.database_id,
							tb: Cow::Borrowed(tb),
						};
						if let Some(tb_def) = self.get_key(&table_key, None).await? {
							let cached_tb = Arc::new(TableDefinition::from_stored(&tb_def)?);
							let cached_entry = cache::tx::Entry::Any(
								Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>
							);
							self.cache.insert(qey, cached_entry);
							return Ok(cached_tb);
						}

						if db_def.strict {
							return Err(CatalogError::TbNotFound {
								name: tb.to_owned(),
							}
							.into());
						}

						let stored = StoredTableDefinition::new(
							db_def.namespace_id,
							db_def.database_id,
							self.get_next_tb_id(ctx, db_def.namespace_id, db_def.database_id)
								.await?,
							tb.clone(),
						);
						let tb_def = TableDefinition::from_stored(&stored)?;
						self.put_tb(ns, db, &tb_def).await
					}
				}
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_or_add_tb")),
		)
	}

	fn get_tb_by_name<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>> {
		Box::pin(async move {
			self.catalog_reads.note(crate::catalog_reads::CatalogRead::TableByName(ns, db, tb));
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.as_str());
			if version.is_none() {
				if let Some(val) = self.cache.get(&qey) {
					return val.try_into_type().map(Some);
				}
				if let Some((epoch, shared)) = self.epoch_catalog()
					&& let Some(def) = shared.get::<TableDefinition>(
						&crate::cache::epoch::Lookup::Table(epoch, ns, db, tb),
					) {
					// What a read through the database would have noted.
					self.catalog_reads.note(crate::catalog_reads::CatalogRead::Database(ns, db));
					self.cache.insert(qey, cache::tx::Entry::Any(def.clone()));
					return Ok(Some(def));
				}
			}
			let (ns_name, db_name) = (ns, db);
			let Some(db) = self.get_db_by_name(ns, db, version).await? else {
				return Ok(None);
			};

			let key = TableKey {
				ns: db.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(tb),
			};
			let Some(tb) = self.get_key(&key, version).await? else {
				return Ok(None);
			};

			let tb = Arc::new(TableDefinition::from_stored(&tb)?);
			if version.is_none() {
				let entr = cache::tx::Entry::Any(tb.clone());
				self.cache.insert(qey, entr);
				self.keep_for_epoch(
					|epoch| crate::cache::epoch::Lookup::Table(epoch, ns_name, db_name, &tb.name),
					&tb,
				);
			}
			Ok(Some(tb))
		})
	}

	fn put_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableDefinition,
	) -> BoxProviderFut<'a, Result<Arc<TableDefinition>>> {
		Box::pin(async move {
			let stored = tb.to_stored();
			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			match self.set_key(&key, &stored).await {
				Ok(_) => {}
				Err(e) => {
					if matches!(storage_error(&e), Some(surrealdb_kvs::Error::TransactionReadonly))
					{
						return Err(CatalogError::TbNotFound {
							name: tb_name,
						}
						.into());
					}
					return Err(e);
				}
			}

			self.bump_catalog_version(tb.namespace_id, tb.database_id, &tb_name).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Populate cache
			let cached_tb = Arc::new(tb.clone());
			let cached_entry =
				cache::tx::Entry::Any(Arc::clone(&cached_tb) as Arc<dyn Any + Send + Sync>);

			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.insert(qey, cached_entry.clone());

			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
			self.cache.insert(qey, cached_entry);

			Ok(cached_tb)
		})
	}

	fn del_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let Some(tb) = self.get_tb_by_name(ns, db, tb, None).await? else {
				return Err(CatalogError::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			self.del_key(&key).await?;
			self.bump_catalog_version(tb.namespace_id, tb.database_id, &tb_name).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
			self.cache.remove(&qey);

			Ok(())
		})
	}

	fn clr_tb<'a>(
		&'a self,
		ns: &'a str,
		db: &'a str,
		tb: &'a TableName,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let Some(tb) = self.get_tb_by_name(ns, db, tb, None).await? else {
				return Err(CatalogError::TbNotFound {
					name: tb.clone(),
				}
				.into());
			};

			let tb_name = tb.name.clone();
			let key = TableKey {
				ns: tb.namespace_id,
				db: tb.database_id,
				tb: Cow::Borrowed(&tb_name),
			};
			self.clr_key(&key).await?;
			self.bump_catalog_version(tb.namespace_id, tb.database_id, &tb_name).await?;

			// Invalidate the cached list of all tables for this database
			let list_key = cache::tx::Lookup::Tbs(tb.namespace_id, tb.database_id);
			self.cache.remove(&list_key);

			// Clear the cache
			let qey = cache::tx::Lookup::Tb(tb.namespace_id, tb.database_id, tb.name.as_str());
			self.cache.remove(&qey);
			let qey = cache::tx::Lookup::TbByName(ns, db, tb.name.as_str());
			self.cache.remove(&qey);

			Ok(())
		})
	}

	/// Retrieve all event definitions for a specific table.
	fn all_tb_events<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::EventDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Evs(ns, db, tb.as_str());
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_evs();
				}
				let range = EventPrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredEventDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Evs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_events")),
		)
	}

	/// Retrieve all field definitions for a specific table.
	fn all_tb_fields<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::FieldDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				if version.is_some() {
					let range = FieldPrefix {
						ns,
						db,
						tb: Cow::Borrowed(tb),
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredFieldDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::fields_from_stored(&stored);
				}
				// The transaction's cache answers without another await.
				let qey = cache::tx::Lookup::Fds(ns, db, tb.as_str());
				if let Some(val) = self.cache.get(&qey) {
					return val.try_into_fds();
				}
				self.tb_fields_unnoted(ns, db, tb).await
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_fields")),
		)
	}

	/// Retrieve all index definitions for a specific table.
	fn all_tb_indexes<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::IndexDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Ixs(ns, db, tb.as_str());
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_ixs();
				}
				let range = IndexDefPrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredIndexDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Ixs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_indexes")),
		)
	}

	/// Retrieve all live definitions for a specific table.
	fn all_tb_lives<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::SubscriptionDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Lvs(ns, db, tb.as_str());
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_lvs();
				}
				let range = SubscriptionPrefix {
					ns,
					db,
					tb: Cow::Borrowed(tb),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredSubscriptionDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = compile_subscriptions(tb, &stored);
				if version.is_none() {
					let entry = cache::tx::Entry::Lvs(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_tb_lives")),
		)
	}

	/// Retrieve a specific table definition.
	fn get_tb<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<TableDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Tb(ns, db, tb.as_str());
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = TableKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(TableDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb")),
		)
	}

	/// Retrieve an event for a table.
	fn get_tb_event<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ev: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<catalog::EventDefinition>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Ev(ns, db, tb.as_str(), ev);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type();
				}
				let key = EventKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ev: Cow::Borrowed(ev),
				};
				let val =
					self.get_key(&key, version).await?.ok_or_else(|| CatalogError::EvNotFound {
						name: ev.to_owned(),
					})?;
				let val = Arc::new(catalog::EventDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_event")),
		)
	}

	/// Retrieve a field for a table.
	fn get_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::FieldDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Fd(ns, db, tb.as_str(), fd);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = FieldKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					fd: Cow::Borrowed(fd),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::FieldDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_field")),
		)
	}

	fn put_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a catalog::FieldDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// A definition derived from a named record id component exists only in
			// the list `all_tb_fields` builds; the catalog stores the `id` that
			// names it.
			if fd.key_part.is_some() {
				return Err(EngineError::Internal(format!(
					"the field `{}` of table `{tb}` is derived from the record id and cannot be stored",
					fd.name.to_raw_string(),
				))
				.into());
			}
			let stored = fd.to_stored();
			let name = catalog::FieldDefinition::storage_name(&fd.name);
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				fd: Cow::Borrowed(&name),
			};
			self.set_key(&key, &stored).await?;
			self.bump_catalog_version(ns, db, tb).await?;

			// Invalidate the cached list of all fields for this table
			let list_key = cache::tx::Lookup::Fds(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Defining a field can add (or change) a REFERENCE, so the
			// database-wide reference-target summary memoized for the DELETE
			// purge gate may be stale. Drop it so it is recomputed on next
			// use. (ALTER/REMOVE FIELD instead clear the whole transaction
			// cache, which covers this entry too.)
			self.cache.remove(&cache::tx::Lookup::DbReferenceTargets(ns, db));

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Fd(ns, db, tb.as_str(), &name);
			let entry = cache::tx::Entry::Any(Arc::new(fd.clone()));
			self.cache.insert(qey, entry);
			Ok(())
		})
	}

	fn del_tb_field<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		fd: &'a str,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				fd: Cow::Borrowed(fd),
			};
			self.del_key(&key).await?;
			self.bump_catalog_version(ns, db, tb).await?;

			// Invalidate the cached list of all fields for this table
			self.cache.remove(&cache::tx::Lookup::Fds(ns, db, tb.as_ref()));
			// The removed field may have been the one REFERENCE that could target
			// some table, so the database-wide reference-target summary is stale.
			self.cache.remove(&cache::tx::Lookup::DbReferenceTargets(ns, db));
			// Invalidate the cached field entry
			self.cache.remove(&cache::tx::Lookup::Fd(ns, db, tb.as_str(), fd));
			Ok(())
		})
	}

	/// Retrieve an index for a table.
	fn get_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
				let qey = cache::tx::Lookup::Ix(ns, db, tb.as_str(), ix);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = IndexDefKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					ix: Cow::Borrowed(ix),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::IndexDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_tb_index")),
		)
	}

	fn get_tb_index_by_id<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: IndexId,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::IndexDefinition>>>> {
		Box::pin(async move {
			self.catalog_reads.note(crate::catalog_reads::CatalogRead::Table(ns, db, tb));
			let key = IndexNameKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix,
			};
			let Some(index_name) = self.get_key(&key, version).await? else {
				return Ok(None);
			};

			self.get_tb_index(ns, db, tb, &index_name, version).await
		})
	}

	fn put_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a catalog::IndexDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let stored = ix.to_stored();
			let key = IndexDefKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.set_key(&key, &stored).await?;

			let name_lookup_key = IndexNameKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.set_key(&name_lookup_key, &ix.name.to_string()).await?;
			self.bump_catalog_version(ns, db, tb).await?;

			// Invalidate the cached list of all indexes for this table
			let list_key = cache::tx::Lookup::Ixs(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ix(ns, db, tb.as_str(), &ix.name);
			let entry = cache::tx::Entry::Any(Arc::new(ix.clone()));
			self.cache.insert(qey, entry);
			Ok(())
		})
	}

	fn del_tb_index<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		ix: &'a str,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Get the index definition
			let Some(ix) = self.get_tb_index(ns, db, tb, ix, None).await? else {
				return Ok(());
			};

			// Remove the index data
			let key = IdxRoot {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.del_prefix_key(&key).await?;

			// Delete the definition
			let key = IndexDefKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: Cow::Borrowed(&ix.name),
			};
			self.del_key(&key).await?;

			// Delete the id-to-name lookup
			let name_lookup_key = IndexNameKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				ix: ix.index_id,
			};
			self.del_key(&name_lookup_key).await?;
			self.bump_catalog_version(ns, db, tb).await?;

			// Invalidate the cached list of all indexes for this table
			let list_key = cache::tx::Lookup::Ixs(ns, db, tb.as_ref());
			self.cache.remove(&list_key);

			// Invalidate the cached index entry
			let index_key = cache::tx::Lookup::Ix(ns, db, tb.as_ref(), &ix.name);
			self.cache.remove(&index_key);

			Ok(())
		})
	}

	/// Fetch a specific record value, at `version` when given.
	///
	/// This function will return a new default initialized record if it does not exist.
	///
	/// The stored bytes are decoded against the table's layouts at the same version as the
	/// record ([`Transaction::decode_record`]), and a record is cached only once it has decoded.
	/// The canonical `id` comes from the key. [`Transaction::get_stamped_record`] answers the
	/// same record with its generation.
	fn get_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<Record>>> {
		Box::pin(
			async move {
				self.get_stamped_record(ns, db, tb, id, version).await.map(|(record, _)| record)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_record")),
		)
	}

	/// Fetch the records `rids` name, in one batched read, at `version` when given.
	///
	/// Each slot holds the record, or a default record for an id that names none. The stored
	/// bytes are decoded against the layouts of each record's table at the same version as the
	/// rows, and the batch fails at the first record whose generation is refused.
	fn get_records<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &'a [RecordId],
		version: Option<u64>,
		cache_policy: CachePolicy,
	) -> BoxProviderFut<'a, Result<Vec<Arc<Record>>>> {
		Box::pin(
			async move {
				let stamped = self.get_stamped_records(ns, db, rids, version, cache_policy).await?;
				Ok(stamped.into_iter().map(|(record, _)| record).collect())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_records")),
		)
	}

	fn record_exists<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<bool>> {
		Box::pin(async move {
			let key = RecordKey {
				ns,
				db,
				tb: Cow::Borrowed(tb),
				id: Cow::Borrowed(id),
			};
			if self.exists_key(&key, version).await? {
				return Ok(true);
			}
			// A lightweight edge has no record; its existence is the
			// adjacency's.
			Ok(self.lightweight_record(ns, db, tb, id, version).await?.is_some())
		})
	}

	/// Insert a record that must not exist yet, in the format its table is written in
	/// ([`Transaction::record_writer`]): the layout of the ring's head when the table has a
	/// generation, at any feature level (a packed row when that layout is packed and the record
	/// fits it, a document stamped with the head otherwise); otherwise `revision` at Legacy and a
	/// generation-0 document above it. The record's key lets its `"id"` be left out and checks the
	/// writer's layouts against the table. The record is cached under the generation its bytes
	/// carry.
	fn put_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				let writer = self.record_writer(ns, db, tb).await?;
				let generation = surrealdb_catalog::written_generation(
					writer.format(),
					record.metadata.as_ref(),
				);
				let rid = RecordId {
					table: tb.clone(),
					key: id.clone(),
				};
				let bytes = writer.encode_whole(record.as_ref(), Some(&rid))?;
				self.put(key.encode_key()?, bytes).await?;
				// Set the value in the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				self.cache.insert(qey, cache::tx::Entry::Val(record, generation));
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "put_record")),
		)
	}

	/// Write a record, replacing any record at its key, in the format its table is written in
	/// ([`Transaction::record_writer`]), as [`Self::put_record`] writes one. Any cached copy of the
	/// record is dropped.
	fn set_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
		record: Arc<Record>,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				// Set the value in the datastore
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				let writer = self.record_writer(ns, db, tb).await?;
				let rid = RecordId {
					table: tb.clone(),
					key: id.clone(),
				};
				let bytes = writer.encode_whole(record.as_ref(), Some(&rid))?;
				self.set(key.encode_key()?, bytes).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				self.cache.remove(&qey);
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "set_record")),
		)
	}

	fn del_record<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &'a TableName,
		id: &'a RecordIdKey,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(
			async move {
				// Delete the value in the datastore
				let key = RecordKey {
					ns,
					db,
					tb: Cow::Borrowed(tb),
					id: Cow::Borrowed(id),
				};
				self.del_key(&key).await?;
				// Clear the value from the cache
				let qey = cache::tx::Lookup::Record(ns, db, tb.as_str(), id);
				self.cache.remove(&qey);
				// Return nothing
				Ok(())
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "del_record")),
		)
	}

	fn get_next_tb_id<'a>(
		&'a self,
		ctx: Option<&'a dyn CancellationProbe>,
		ns: NamespaceId,
		db: DatabaseId,
	) -> BoxProviderFut<'a, Result<TableId>> {
		Box::pin(async move { self.sequences.next_table_id(ctx, ns, db).await })
	}
}

// --------------------------------------------------
// User implementation functions
// --------------------------------------------------

impl UserProvider for Transaction {
	/// Retrieve all ROOT level users in a datastore.
	fn all_root_users(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Rus;
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_rus();
				}
				let range = RootUserPrefix {}.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Rus(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_users")),
		)
	}

	/// Retrieve all namespace user definitions for a specific namespace.
	fn all_ns_users(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Nus(ns);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_nus();
				}
				let range = NsUserPrefix {
					ns,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Nus(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_users")),
		)
	}

	/// Retrieve all database user definitions for a specific database.
	fn all_db_users(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::UserDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Dus(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_dus();
				}
				let range = DbUserPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Dus(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_users")),
		)
	}

	/// Retrieve a specific root user definition.
	fn get_root_user<'a>(
		&'a self,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Ru(us);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = RootUserKey {
					user: Cow::Borrowed(us),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_user")),
		)
	}

	/// Retrieve a specific namespace user definition.
	fn get_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Nu(ns, us);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = NsUserKey {
					ns,
					user: Cow::Borrowed(us),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_user")),
		)
	}

	/// Retrieve a specific user definition from a database.
	fn get_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::UserDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Du(ns, db, us);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = DbUserKey {
					ns,
					db,
					user: Cow::Borrowed(us),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};

				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_user")),
		)
	}

	fn put_root_user<'a>(
		&'a self,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = RootUserKey {
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

			// Invalidate the cached list of all root users
			let list_key = cache::tx::Lookup::Rus;
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ru(&us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_ns_user<'a>(
		&'a self,
		ns: NamespaceId,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = NsUserKey {
				ns,
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

			// Invalidate the cached list of all namespace users
			let list_key = cache::tx::Lookup::Nus(ns);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Nu(ns, &us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}

	fn put_db_user<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		us: &'a catalog::UserDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let key = DbUserKey {
				ns,
				db,
				user: Cow::Borrowed(&us.name),
			};
			self.set_key(&key, us).await?;

			// Invalidate the cached list of all database users
			let list_key = cache::tx::Lookup::Dus(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Du(ns, db, &us.name);
			let entry = cache::tx::Entry::Any(Arc::new(us.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Authorisation implementation functions
// --------------------------------------------------

impl AuthorisationProvider for Transaction {
	/// Retrieve all ROOT level accesses in a datastore.
	fn all_root_accesses(
		&self,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Ras;
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_ras();
				}
				let range = RootAccessMethodPrefix {}.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredAccessDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Ras(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_accesses")),
		)
	}

	/// Retrieve all root access grants in a datastore.
	fn all_root_access_grants<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Rgs(ra);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_rag();
				}
				let range = RootGrantPrefix {
					ac: Cow::Borrowed(ra),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Rag(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_root_access_grants")),
		)
	}

	/// Retrieve all namespace access definitions for a specific namespace.
	fn all_ns_accesses(
		&self,
		ns: NamespaceId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Nas(ns);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_nas();
				}
				let range = NsAccessMethodPrefix {
					ns,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredAccessDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Nas(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_accesses")),
		)
	}

	/// Retrieve all namespace access grants for a specific namespace.
	fn all_ns_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Ngs(ns, na);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_nag();
				}
				let range = NsGrantPrefix {
					ns,
					ac: Cow::Borrowed(na),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Nag(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_ns_access_grants")),
		)
	}

	/// Retrieve all database access definitions for a specific database.
	fn all_db_accesses(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::AccessDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Das(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_das();
				}
				let range = DbAccessMethodPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredAccessDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Das(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_accesses")),
		)
	}

	/// Retrieve all database access grants for a specific database.
	fn all_db_access_grants<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Arc<[catalog::AccessGrant]>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Dgs(ns, db, da);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_dag();
				}
				let range = DbGrantPrefix {
					ns,
					db,
					ac: Cow::Borrowed(da),
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let val = util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				if version.is_none() {
					let entry = cache::tx::Entry::Dag(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_access_grants")),
		)
	}

	/// Retrieve a specific root access definition.
	fn get_root_access<'a>(
		&'a self,
		ra: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Ra(ra);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = RootAccessMethodKey {
					ac: Cow::Borrowed(ra),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_access")),
		)
	}

	/// Retrieve a specific root access grant.
	fn get_root_access_grant<'a>(
		&'a self,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Rg(ac, gr);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = RootGrantKey {
					ac: Cow::Borrowed(ac),
					gr: Cow::Borrowed(gr),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_root_access_grant")),
		)
	}

	/// Retrieve a specific namespace access definition.
	fn get_ns_access<'a>(
		&'a self,
		ns: NamespaceId,
		na: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Na(ns, na);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = NsAccessMethodKey {
					ns,
					ac: Cow::Borrowed(na),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_access")),
		)
	}

	/// Retrieve a specific namespace access grant.
	fn get_ns_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Ng(ns, ac, gr);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = NsGrantKey {
					ns,
					ac: Cow::Borrowed(ac),
					gr: Cow::Borrowed(gr),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_ns_access_grant")),
		)
	}

	/// Retrieve a specific database access definition.
	fn get_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Da(ns, db, da);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = DbAccessMethodKey {
					ns,
					db,
					ac: Cow::Borrowed(da),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::AccessDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_access")),
		)
	}

	/// Retrieve a specific database access grant.
	fn get_db_access_grant<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ac: &'a str,
		gr: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::AccessGrant>>>> {
		Box::pin(
			async move {
				let qey = cache::tx::Lookup::Dg(ns, db, ac, gr);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = DbGrantKey {
					ns,
					db,
					ac: Cow::Borrowed(ac),
					gr: Cow::Borrowed(gr),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(val);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_access_grant")),
		)
	}

	fn del_root_access<'a>(&'a self, ra: &'a str) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = RootAccessMethodKey {
				ac: Cow::Borrowed(ra),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = RootAccessRoot {
				ac: Cow::Borrowed(ra),
			};
			self.del_prefix_key(&key).await?;

			// Invalidate the cached list of all root accesses
			let list_key = cache::tx::Lookup::Ras;
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Ra(ra);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Rgs(ra);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}

	fn del_ns_access<'a>(&'a self, ns: NamespaceId, na: &'a str) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = NsAccessMethodKey {
				ns,
				ac: Cow::Borrowed(na),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = NsAccessRoot {
				ns,
				ac: Cow::Borrowed(na),
			};
			self.del_prefix_key(&key).await?;

			// Invalidate the cached list of all namespace accesses
			let list_key = cache::tx::Lookup::Nas(ns);
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Na(ns, na);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Ngs(ns, na);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}

	fn del_db_access<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		da: &'a str,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			// Delete the definition
			let key = DbAccessMethodKey {
				ns,
				db,
				ac: Cow::Borrowed(da),
			};
			self.del_key(&key).await?;
			// Delete any associated data including access grants.
			let key = DbAccessRoot {
				ns,
				db,
				ac: Cow::Borrowed(da),
			};
			self.del_prefix_key(&key).await?;

			// Invalidate the cached list of all database accesses
			let list_key = cache::tx::Lookup::Das(ns, db);
			self.cache.remove(&list_key);

			// Invalidate the cached access entry and grants
			let access_key = cache::tx::Lookup::Da(ns, db, da);
			self.cache.remove(&access_key);
			let grants_key = cache::tx::Lookup::Dgs(ns, db, da);
			self.cache.remove(&grants_key);

			Ok(())
		})
	}
}

// --------------------------------------------------
// API implementation functions
// --------------------------------------------------

impl ApiProvider for Transaction {
	/// Retrieve all api definitions for a specific database.
	fn all_db_apis(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::ApiDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				if version.is_some() {
					let range = ApiPrefix {
						ns,
						db,
					}
					.range()?;
					let val = self.tr.getr(range.into_key_range(), version).await?;
					let stored: Arc<[catalog::StoredApiDefinition]> =
						util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
					return catalog::from_stored_all(&stored);
				}
				let qey = cache::tx::Lookup::Aps(ns, db);
				match self.cache.get(&qey) {
					Some(val) => val,
					None => {
						let range = ApiPrefix {
							ns,
							db,
						}
						.range()?;
						let val = self.tr.getr(range.into_key_range(), None).await?;
						let stored: Arc<[catalog::StoredApiDefinition]> =
							util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
						let val = cache::tx::Entry::Aps(catalog::from_stored_all(&stored)?);
						self.cache.insert(qey, val.clone());
						val
					}
				}
				.try_into_aps()
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_apis")),
		)
	}

	/// Retrieve a specific api definition.
	fn get_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::ApiDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Ap(ns, db, ap);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = ApiKey {
					ns,
					db,
					ap: Cow::Borrowed(ap),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let val = Arc::new(catalog::ApiDefinition::from_stored(&val)?);
				if version.is_none() {
					let entry = cache::tx::Entry::Any(val.clone());
					self.cache.insert(qey, entry);
				}
				Ok(Some(val))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_api")),
		)
	}

	fn put_db_api<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		ap: &'a catalog::ApiDefinition,
	) -> BoxProviderFut<'a, Result<()>> {
		Box::pin(async move {
			let stored = ap.to_stored();
			let name = ap.path.to_string();
			let key = ApiKey {
				ns,
				db,
				ap: Cow::Borrowed(&name),
			};
			self.set_key(&key, &stored).await?;

			// Invalidate the cached list of all APIs for this database
			let list_key = cache::tx::Lookup::Aps(ns, db);
			self.cache.remove(&list_key);

			// Set the entry in the cache
			let qey = cache::tx::Lookup::Ap(ns, db, &name);
			let entry = cache::tx::Entry::Any(Arc::new(ap.clone()));
			self.cache.insert(qey, entry);

			Ok(())
		})
	}
}

// --------------------------------------------------
// Bucket implementation functions
// --------------------------------------------------

impl BucketProvider for Transaction {
	/// Retrieve all bucket definitions for a specific database.
	fn all_db_buckets(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		version: Option<u64>,
	) -> BoxProviderFut<'_, Result<Arc<[catalog::BucketDefinition]>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Bus(ns, db);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_bus();
				}
				let range = BucketPrefix {
					ns,
					db,
				}
				.range()?;
				let val = self.tr.getr(range.into_key_range(), version).await?;
				let stored: Arc<[catalog::StoredBucketDefinition]> =
					util::deserialize_cache(val.values.iter().map(|x| x.1.as_slice()))?;
				let val = catalog::from_stored_all(&stored)?;
				if version.is_none() {
					let entry = cache::tx::Entry::Bus(Arc::clone(&val));
					self.cache.insert(qey, entry);
				}
				Ok(val)
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "all_db_buckets")),
		)
	}

	/// Retrieve a specific bucket definition from a database.
	fn get_db_bucket<'a>(
		&'a self,
		ns: NamespaceId,
		db: DatabaseId,
		bu: &'a str,
		version: Option<u64>,
	) -> BoxProviderFut<'a, Result<Option<Arc<catalog::BucketDefinition>>>> {
		Box::pin(
			async move {
				self.catalog_reads.note(crate::catalog_reads::CatalogRead::Opaque);
				let qey = cache::tx::Lookup::Bu(ns, db, bu);
				if version.is_none()
					&& let Some(val) = self.cache.get(&qey)
				{
					return val.try_into_type().map(Some);
				}
				let key = BucketKey {
					ns,
					db,
					bu: Cow::Borrowed(bu),
				};
				let Some(val) = self.get_key(&key, version).await? else {
					return Ok(None);
				};
				let bucket_def = Arc::new(catalog::BucketDefinition::from_stored(&val)?);
				if version.is_none() {
					let entr = cache::tx::Entry::Any(bucket_def.clone());
					self.cache.insert(qey, entr);
				}
				Ok(Some(bucket_def))
			}
			.instrument(trace_span!(target: "surrealdb::core::kvs::tx", "get_db_bucket")),
		)
	}
}

// --------------------------------------------------
// Catalog provider
// --------------------------------------------------

impl CatalogProvider for Transaction {}

/// The record a lightweight edge presents to readers, derived entirely from
/// its table, canonical id, and the two endpoints the id carries.
fn synthesize_lightweight_record(
	tb: &TableName,
	id: &RecordIdKey,
	l: &RecordId,
	r: &RecordId,
) -> Record {
	let rid = RecordId {
		table: tb.clone(),
		key: id.clone(),
	};
	let mut data = surrealdb_expr::val::Object::default();
	data.insert("id".to_owned(), Value::RecordId(rid));
	data.insert("in".to_owned(), Value::RecordId(l.clone()));
	data.insert("out".to_owned(), Value::RecordId(r.clone()));
	Record {
		metadata: Some(Metadata {
			record_type: RecordType::Edge {
				variant: LATEST_EDGE_VARIANT,
			},
			aggregation_stats: Vec::new(),
		}),
		data: Value::Object(data),
	}
}

/// The two endpoints of a lightweight edge's canonical `[in, out]` id, when
/// the id has that shape.
pub fn lightweight_edge_parts(id: &RecordIdKey) -> Option<(&RecordId, &RecordId)> {
	let RecordIdKey::Array(arr) = id else {
		return None;
	};
	match arr.as_slice() {
		[Value::RecordId(l), Value::RecordId(r)] => Some((l, r)),
		_ => None,
	}
}

/// Lightweight-relation record synthesis.
///
/// A lightweight relation stores each edge as its two vertex-side adjacency
/// keys alone — no record. The record providers below synthesize the record
/// on demand from the edge's canonical `[in, out]` id, so every reader of
/// records — point reads, the processors, the delete pipeline, cascades,
/// permission gates, FETCH — is correct without knowing the table is
/// lightweight. Existence is decided by the merged adjacency truth for the
/// `in` endpoint: the exact pointer key (live or tombstone) wins, and a
/// folded vertex table is additionally probed through its covering packed
/// chunk, resolving numeric entries through the shared doc-id space.
impl Transaction {
	/// Synthesizes the record for a lightweight edge, or returns `None`
	/// when the id is not a lightweight edge of this table or the edge does
	/// not exist at the read snapshot.
	async fn lightweight_record(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<Record>> {
		let Some((l, r)) = lightweight_edge_parts(id) else {
			return Ok(None);
		};
		let Some(tbdef) = self.layout_tb(ns, db, tb, version).await? else {
			return Ok(None);
		};
		let TableType::Relation(rel) = &tbdef.table_type else {
			return Ok(None);
		};
		if !rel.lightweight {
			return Ok(None);
		}
		if !self.lightweight_edge_present(ns, db, tb, l, r, id, version).await? {
			return Ok(None);
		}
		Ok(Some(synthesize_lightweight_record(tb, id, l, r)))
	}

	/// Batched [`Self::lightweight_record`]: one slot per input id, `None`
	/// where the id is not a live lightweight edge.
	///
	/// The candidates' exact pointer keys are read with a single batched get;
	/// only edges whose `in` vertex table has folded adjacency fall back to
	/// the per-edge covering-chunk probe, and the last fetched chunk is
	/// memoized per `(in vertex, edge table)` scope, so a hub's edges — which
	/// typically share their covering chunk — do not refetch it.
	async fn lightweight_records(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		rids: &[&RecordId],
		version: Option<u64>,
	) -> Result<Vec<Option<Record>>> {
		use surrealdb_expr::expr::Dir;
		let mut out: Vec<Option<Record>> = (0..rids.len()).map(|_| None).collect();
		// Keep the ids with the canonical `[in, out]` shape on a lightweight
		// relation; anything else can never synthesize a record.
		let mut candidates: Vec<(usize, &RecordId, &RecordId, &RecordId)> = Vec::new();
		for (i, rid) in rids.iter().enumerate() {
			let Some((l, r)) = lightweight_edge_parts(&rid.key) else {
				continue;
			};
			let Some(tbdef) = self.layout_tb(ns, db, &rid.table, version).await? else {
				continue;
			};
			let TableType::Relation(rel) = &tbdef.table_type else {
				continue;
			};
			if !rel.lightweight {
				continue;
			}
			candidates.push((i, rid, l, r));
		}
		if candidates.is_empty() {
			return Ok(out);
		}
		// One batched read over the candidates' exact `in`-side pointer keys:
		// a live value proves the edge, a tombstone proves its absence.
		let keys: Vec<GraphPointerKey<'_>> = candidates
			.iter()
			.map(|(_, rid, l, r)| GraphPointerKey {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
				target_table: Cow::Borrowed(&r.table),
				target_key: Cow::Borrowed(&r.key),
			})
			.collect();
		let values = self.get_many_key(keys, version).await?;
		// A pointer-key miss can only live in a packed chunk, which only
		// exists once the `in` vertex's table has folded. The covering-chunk
		// probe stays per-edge, but the fetched chunk is memoized per scope:
		// an edge found in the memoized block is present (it has no
		// tombstone, or the batched read above would have seen it), and a
		// probe that lands on the memoized chunk again resolves without
		// refetching the block.
		type Scope = (TableName, RecordIdKey, TableName);
		// The scope tuple hashes by value; the ids in it are never mutated
		// while memoized.
		#[expect(clippy::mutable_key_type)]
		let mut chunks: HashMap<Scope, (Vec<u8>, AdjacencyBlock)> = HashMap::new();
		for ((i, rid, l, r), value) in candidates.into_iter().zip(values) {
			if let Some(value) = value {
				if !value.is_tombstone() {
					out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
				}
				continue;
			}
			let folded =
				self.layout_tb(ns, db, &l.table, version).await?.is_some_and(|tb| tb.graph_folded);
			if !folded {
				continue;
			}
			let scope = (l.table.clone(), l.key.clone(), rid.table.clone());
			if let Some((_, block)) = chunks.get(&scope)
				&& self.block_holds_edge(ns, db, &rid.table, block, &rid.key, r, version).await?
			{
				out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
				continue;
			}
			// The covering chunk: the last chunk starting at or before the
			// edge key; entries are partitioned by `chunk_start`, so no other
			// chunk can hold it.
			let up_to_bound = AdjacencyEdgeTablePrefix {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				edge_table: Cow::Borrowed(&rid.table),
			}
			.range_where((
				std::ops::Bound::Unbounded,
				std::ops::Bound::Included(Cow::Borrowed(&rid.key)),
			))?;
			let covering = self.keysr_raw(up_to_bound, 1, 0, version).await?;
			let Some(chunk_key_bytes) = covering.into_iter().next() else {
				continue;
			};
			// The probe landed on the memoized chunk, which was already
			// checked above: the edge is absent.
			if chunks.get(&scope).is_some_and(|(key, _)| *key == chunk_key_bytes) {
				continue;
			}
			let chunk_key = AdjacencyKey::decode_key(&chunk_key_bytes)?;
			let Some(block) = self.get_key(&chunk_key, version).await? else {
				continue;
			};
			let holds =
				self.block_holds_edge(ns, db, &rid.table, &block, &rid.key, r, version).await?;
			chunks.insert(scope, (chunk_key_bytes, block));
			if holds {
				out[i] = Some(synthesize_lightweight_record(&rid.table, &rid.key, l, r));
			}
		}
		Ok(out)
	}

	/// Whether the lightweight edge `l -[tb:id]-> r` exists at the read
	/// snapshot: its exact `in`-side pointer key (live beats absent,
	/// tombstone beats everything), falling back to the covering packed
	/// chunk when the `in` vertex's table has folded adjacency.
	#[allow(clippy::too_many_arguments)]
	async fn lightweight_edge_present(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		l: &RecordId,
		r: &RecordId,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<bool> {
		use surrealdb_expr::expr::Dir;
		let key = GraphPointerKey {
			ns,
			db,
			tb: Cow::Borrowed(&l.table),
			id: Cow::Borrowed(&l.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(tb),
			foreign_key: Cow::Borrowed(id),
			target_table: Cow::Borrowed(&r.table),
			target_key: Cow::Borrowed(&r.key),
		};
		if let Some(value) = self.get_key(&key, version).await? {
			return Ok(!value.is_tombstone());
		}
		// No delta key: the edge can only live in a packed chunk, which only
		// exists once the vertex table has folded.
		let folded =
			self.layout_tb(ns, db, &l.table, version).await?.is_some_and(|tb| tb.graph_folded);
		if !folded {
			return Ok(false);
		}
		// The covering chunk: the last chunk starting at or before the edge
		// key; entries are partitioned by `chunk_start`, so no other chunk
		// can hold it.
		let prefix = || AdjacencyEdgeTablePrefix {
			ns,
			db,
			tb: Cow::Borrowed(&l.table),
			id: Cow::Borrowed(&l.key),
			dir: Dir::Out,
			edge_table: Cow::Borrowed(tb),
		};
		let up_to_bound = prefix().range_where((
			std::ops::Bound::Unbounded,
			std::ops::Bound::Included(Cow::Borrowed(id)),
		))?;
		let covering = self.keysr_raw(up_to_bound, 1, 0, version).await?;
		let Some(chunk_key) = covering.into_iter().next() else {
			return Ok(false);
		};
		let chunk_key = AdjacencyKey::decode_key(&chunk_key)?;
		let Some(block) = self.get_key(&chunk_key, version).await? else {
			return Ok(false);
		};
		self.block_holds_edge(ns, db, tb, &block, id, r, version).await
	}

	/// Resolves `id` to the doc-ID this table has mapped it to, or `None`.
	///
	/// Reads the forward mapping `!dj`, which keys the record id through a
	/// lossless encoding, so ids differing only in a number's kind — `c:[1]` and
	/// `c:[1dec]` — have a mapping each. A hit is adopted as it is: every writer
	/// publishes and deletes `!dj` together with the reverse mapping `!dd`, and
	/// the startup migration that retired the type-erased band left every `!dj`
	/// backed by its `!dd`.
	///
	/// `version` selects the snapshot to resolve against: mappings are removed
	/// when their record is deleted, so a time-travel read must resolve through
	/// the state its snapshot saw.
	pub async fn resolve_doc_id(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		id: &RecordIdKey,
		version: Option<u64>,
	) -> Result<Option<DocId>> {
		let dj = DocLookupIdentityKey::new(ns, db, Cow::Borrowed(tb), RecordIdentity(id.clone()));
		self.get_key(&dj, version).await
	}

	/// Whether a decoded packed chunk holds the edge `(id -> r)`, matching
	/// on the full entry identity. Numeric chunks are matched through the
	/// forward key→doc-id mapping; a key with no mapping was never
	/// compressed and cannot be in a numeric chunk.
	#[allow(clippy::too_many_arguments)]
	async fn block_holds_edge(
		&self,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
		block: &AdjacencyBlock,
		id: &RecordIdKey,
		r: &RecordId,
		version: Option<u64>,
	) -> Result<bool> {
		match &block.entries {
			BlockEntries::Plain(entries) => {
				let edge_key = storekey::encode_vec(id).map_err(anyhow::Error::from_boxed)?;
				let target_key = storekey::encode_vec(&r.key).map_err(anyhow::Error::from_boxed)?;
				Ok(entries.iter().any(|e| {
					e.edge_key == edge_key
						&& e.target_key == target_key
						&& block.targets.get(e.target_table as usize).is_some_and(|t| *t == r.table)
				}))
			}
			BlockEntries::Numeric(entries) => {
				// Both sides resolve through the injective mapping, so one of
				// a numerically colliding pair never answers with the other's
				// doc-ID.
				let Some(edge_id) = self.resolve_doc_id(ns, db, tb, id, version).await? else {
					return Ok(false);
				};
				let Some(entry) = entries.iter().find(|e| e.edge == edge_id) else {
					return Ok(false);
				};
				if block.targets.get(entry.target_table as usize) != Some(&r.table) {
					return Ok(false);
				}
				let target = self.resolve_doc_id(ns, db, &r.table, &r.key, version).await?;
				Ok(target == Some(entry.target))
			}
		}
	}
}

/// What a single-key write changed that a catalog version stands for.
enum Defined {
	/// The definition of a versioned object other than a table.
	Object(DefinedObject),
	/// The definition of a table: written while this transaction has stamped it, or created,
	/// rewritten or removed.
	Table(TableChange),
	/// A field definition of the named table, written with other bytes or removed.
	Field((NamespaceId, DatabaseId, TableName)),
}

/// What a write of a table's definition did ([`Transaction::changed_definition`]).
struct TableChange {
	/// The table the definition names.
	table: (NamespaceId, DatabaseId, TableName),
	/// Whether this transaction had stamped the table's catalog version when it was written.
	stamped: bool,
	/// How the write changed what the key held.
	change: TableDefinitionChange,
}

/// How a write changed a table's definition key.
#[derive(Clone, Copy, Debug)]
enum TableDefinitionChange {
	/// The key held nothing and now holds a definition, carrying this table id; `None` when the
	/// definition does not decode.
	Created(Option<TableId>),
	/// The key held a definition and now holds different bytes. `forfeits` is whether the new
	/// definition makes the table a view or a lightweight relation, makes it `SCHEMAFULL`, or does
	/// not decode: whatever was settled about the table's rows then does not hold. A `SCHEMAFULL`
	/// table's definitions refuse a key none of them declares beneath an object, which the rows
	/// written before it may hold.
	Rewritten {
		forfeits: bool,
	},
	/// The key held a definition and now holds nothing.
	Removed,
	/// The write leaves the key as it was.
	Unchanged,
}

impl TableDefinitionChange {
	/// The change from `prior`, the bytes the key held, to `val`, the bytes it holds afterwards
	/// (`None` for a key that holds nothing).
	fn of(prior: Option<&[u8]>, val: Option<&[u8]>) -> Self {
		match (prior, val) {
			(None, Some(new)) => Self::Created(
				StoredTableDefinition::kv_decode_value(new, ()).ok().map(|stored| stored.table_id),
			),
			(Some(old), Some(new)) if old != new => Self::Rewritten {
				forfeits: match StoredTableDefinition::kv_decode_value(new, ()) {
					Ok(stored) => {
						stored.view.is_some()
							|| matches!(&stored.table_type, TableType::Relation(r) if r.lightweight)
							|| (stored.schemafull
								&& StoredTableDefinition::kv_decode_value(old, ())
									.ok()
									.is_none_or(|old| !old.schemafull))
					}
					Err(_) => true,
				},
			},
			(Some(_), None) => Self::Removed,
			_ => Self::Unchanged,
		}
	}
}

/// The table whose definition `key` is, or `None` for any other key.
///
/// Called on every single-key write, so it rejects on the fixed bytes of a database-scoped
/// catalog key (`/*{ns}*{db}!tb`, the namespace and database ids being four bytes each) before
/// decoding anything.
/// Whether `key` is the definition of a namespace, a database, a user or an
/// access method at the root, a namespace or a database: the definitions whose
/// writes move the catalog epoch without stamping a catalog version. Called on
/// every write that is examined as a definition, so it checks fixed bytes only.
fn root_definition_at(key: &[u8]) -> bool {
	let tag = |at: usize| key.get(at..at + 3);
	match (key.first(), key.get(1)) {
		(Some(b'/'), Some(b'!')) => {
			matches!(tag(1), Some(b"!ns") | Some(b"!us") | Some(b"!ac"))
		}
		(Some(b'/'), Some(b'*')) => match key.get(6) {
			Some(b'!') => matches!(tag(6), Some(b"!db") | Some(b"!us") | Some(b"!ac")),
			Some(b'*') => key.get(11) == Some(&b'!') && matches!(tag(11), Some(b"!us")),
			_ => false,
		},
		_ => false,
	}
}

/// Whether a range deleted from `start` may hold a definition whose removal
/// moves the catalog epoch: one that starts at a namespace's or a database's
/// root, at the root keyspace, or at a definition [`root_definition_at`] or
/// [`table_definition_at`] names. A range of records starts under a table and
/// is not one.
fn range_may_hold_definitions(start: &[u8]) -> bool {
	if start.len() <= 1 || root_definition_at(start) {
		return true;
	}
	if start.first() != Some(&b'/') || start.get(1) != Some(&b'*') {
		return false;
	}
	match start.len() {
		..=6 => true,
		_ if start.get(6) == Some(&b'*') && start.len() <= 11 => true,
		_ => {
			start.get(6) == Some(&b'*')
				&& start.get(11) == Some(&b'!')
				&& start.get(11..14) == Some(b"!tb".as_slice())
		}
	}
}

fn table_definition_at(key: &[u8]) -> Option<(NamespaceId, DatabaseId, TableName)> {
	if key.len() < 14
		|| key.first() != Some(&b'/')
		|| key.get(1) != Some(&b'*')
		|| key.get(6) != Some(&b'*')
		|| key.get(11..14) != Some(b"!tb".as_slice())
	{
		return None;
	}
	let table = TableKey::decode_key(key).ok()?;
	Some((table.ns, table.db, table.tb.into_owned()))
}

/// The table whose field definition `key` is, or `None` for any other key.
///
/// Called on every deletion and every write that read its key, so it rejects on the fixed bytes of
/// a table-scoped key (`/*{ns}*{db}*{tb}`, the namespace and database ids being four bytes each)
/// and then on the tag after the table's name before decoding anything, with no allocation and
/// only checked arithmetic. A table's name is a string in the key's encoding: it ends at the first
/// zero byte, and a byte below 2 inside it is written with a one byte escape before it.
fn field_definition_table(key: &[u8]) -> Option<(NamespaceId, DatabaseId, TableName)> {
	if key.len() < 16
		|| key.first() != Some(&b'/')
		|| key.get(1) != Some(&b'*')
		|| key.get(6) != Some(&b'*')
		|| key.get(11) != Some(&b'*')
	{
		return None;
	}
	let mut at = 12usize;
	loop {
		match *key.get(at)? {
			0x00 => break,
			0x01 => at = at.checked_add(2)?,
			_ => at = at.checked_add(1)?,
		}
	}
	let tag = at.checked_add(1)?..at.checked_add(4)?;
	if key.get(tag) != Some(b"!fd".as_slice()) {
		return None;
	}
	let field = FieldKey::decode_key(key).ok()?;
	Some((field.ns, field.db, field.tb.into_owned()))
}

/// A database-scoped catalog object identified from the key of its
/// definition: the subject of the version a change to that key mints
/// ([`Transaction::restamp_defined`]).
struct DefinedObject {
	ns: NamespaceId,
	db: DatabaseId,
	kind: CatalogObjectKind,
	name: String,
	sub: String,
}

impl DefinedObject {
	/// The definition key of the object of `kind` named `name` and `sub`.
	fn key_of(
		ns: NamespaceId,
		db: DatabaseId,
		kind: CatalogObjectKind,
		name: &str,
		sub: &str,
	) -> Result<Key<'static>> {
		let name = Cow::Borrowed(name);
		Ok(match kind {
			CatalogObjectKind::Function => FunctionKey {
				ns,
				db,
				fc: name,
			}
			.encode_key()?,
			CatalogObjectKind::Param => ParamKey {
				ns,
				db,
				pa: name,
			}
			.encode_key()?,
			CatalogObjectKind::Access => DbAccessMethodKey {
				ns,
				db,
				ac: name,
			}
			.encode_key()?,
			CatalogObjectKind::Model => MlModelKey {
				ns,
				db,
				ml: name,
				vn: Cow::Borrowed(sub),
			}
			.encode_key()?,
			CatalogObjectKind::Module => ModuleKey {
				ns,
				db,
				md: name,
			}
			.encode_key()?,
			CatalogObjectKind::Analyzer => AnalyzerKey {
				ns,
				db,
				az: name,
			}
			.encode_key()?,
			CatalogObjectKind::Sequence => SequenceKey {
				ns,
				db,
				sq: name,
			}
			.encode_key()?,
			CatalogObjectKind::Api => ApiKey {
				ns,
				db,
				ap: name,
			}
			.encode_key()?,
		})
	}

	/// The object whose definition `key` is, or `None` for any other key.
	///
	/// Called on every single-key write, so it rejects on four fixed bytes
	/// before decoding anything: a database-scoped catalog key is
	/// `/*{ns}*{db}!` and a two-byte tag, the namespace and database ids are
	/// four bytes each, and a record or any other key under a table has `*`
	/// where the catalog has `!`. The tags are those of the definition keys in
	/// [`crate::key::schema`]; a key that carries one of them is that key,
	/// since the schema allows no two keys to share bytes, so a decode failure
	/// is a malformed key and is reported.
	fn at(key: &[u8]) -> Result<Option<Self>> {
		if key.len() < 14 || key[0] != b'/' || key[1] != b'*' || key[6] != b'*' || key[11] != b'!' {
			return Ok(None);
		}
		let (ns, db, kind, name, sub) = match &key[12..14] {
			b"fn" => {
				let k = FunctionKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Function, k.fc, Cow::Borrowed(""))
			}
			b"pa" => {
				let k = ParamKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Param, k.pa, Cow::Borrowed(""))
			}
			b"ac" => {
				let k = DbAccessMethodKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Access, k.ac, Cow::Borrowed(""))
			}
			b"ml" => {
				let k = MlModelKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Model, k.ml, k.vn)
			}
			b"md" => {
				let k = ModuleKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Module, k.md, Cow::Borrowed(""))
			}
			b"az" => {
				let k = AnalyzerKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Analyzer, k.az, Cow::Borrowed(""))
			}
			b"sd" => {
				let k = SequenceKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Sequence, k.sq, Cow::Borrowed(""))
			}
			b"ap" => {
				let k = ApiKey::decode_key(key)?;
				(k.ns, k.db, CatalogObjectKind::Api, k.ap, Cow::Borrowed(""))
			}
			_ => return Ok(None),
		};
		Ok(Some(Self {
			ns,
			db,
			kind,
			name: name.into_owned(),
			sub: sub.into_owned(),
		}))
	}
}

#[cfg(test)]
mod compare_swap_tests {
	use std::sync::Arc;

	use anyhow::Result;
	use revision::revisioned;
	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::key::KVKey;
	use surrealdb_kvs::value::KVValue;
	use surrealdb_kvs::{Key, TransactionType};

	use crate::factory::TransactionFactory;
	use crate::sequences::Sequences;

	/// A stored value as an earlier release wrote it: one field, revision 1.
	#[revisioned(revision = 1)]
	#[derive(Clone, Debug, PartialEq)]
	struct StateV1 {
		generation: u64,
	}

	/// The same value after a field was added: revision 2. Decoding the earlier
	/// revision fills the new field with its default, which is what makes the
	/// two spellings mean the same thing while encoding differently.
	#[revisioned(revision = 2)]
	#[derive(Clone, Debug, PartialEq)]
	struct StateV2 {
		generation: u64,
		#[revision(start = 2)]
		cursor: Option<u64>,
	}

	surrealdb_kvs::impl_kv_value_revisioned!(StateV1);
	surrealdb_kvs::impl_kv_value_revisioned!(StateV2);

	#[derive(Debug)]
	struct StateKey;

	impl KVKey for StateKey {
		type Value = StateV2;

		fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
			buffer.extend_from_slice(b"/!cas-revision-test");
			Ok(())
		}

		fn value_context(&self) {}
	}

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	/// A compare-and-swap must fence on what is stored, not on a re-encoding of
	/// it.
	///
	/// A revisioned value encodes at whatever revision the running build
	/// declares, which need not be the one it was stored at. Encoding the check
	/// value here and comparing those bytes therefore never matches a value an
	/// earlier release wrote, so the swap cannot succeed however often it is
	/// retried — and a caller that retries a failed acquisition spins instead of
	/// making progress.
	#[tokio::test]
	async fn a_swap_matches_a_value_stored_at_an_earlier_revision() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());

		// Exactly what the earlier release left behind.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		tx.put_compare(
			key.clone(),
			StateV1 {
				generation: 7,
			}
			.kv_encode_value()
			.unwrap(),
			None,
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		// This build reads it through the current revision ...
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let current = tx.get_key(&StateKey, None).await.unwrap().expect("the stored state");
		assert_eq!(
			current,
			StateV2 {
				generation: 7,
				cursor: None
			}
		);

		// ... and swaps it for the next one, fencing on what it just read.
		let next = StateV2 {
			generation: 8,
			cursor: None,
		};
		tx.put_compare_key(&StateKey, &next, Some(&current)).await.unwrap();
		tx.commit().await.unwrap();

		let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
		assert_eq!(tx.get_key(&StateKey, None).await.unwrap(), Some(next));
		tx.cancel().await.unwrap();
	}

	/// Fencing on the stored bytes must not make every swap succeed: a check
	/// value that disagrees with what is stored still fails, and a swap against
	/// a key holding nothing fails too.
	#[tokio::test]
	async fn a_swap_still_fails_when_the_check_value_disagrees() {
		let (tf, sq) = factory().await;

		// Nothing stored: a swap expecting a value must not create one.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let absent = StateV2 {
			generation: 1,
			cursor: None,
		};
		assert!(tx.put_compare_key(&StateKey, &absent, Some(&absent)).await.is_err());
		tx.cancel().await.unwrap();

		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		tx.put_compare_key(
			&StateKey,
			&StateV2 {
				generation: 7,
				cursor: None,
			},
			None,
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();

		// Stored, but the check value names a different state.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let wrong = StateV2 {
			generation: 99,
			cursor: None,
		};
		let next = StateV2 {
			generation: 8,
			cursor: None,
		};
		assert!(tx.put_compare_key(&StateKey, &next, Some(&wrong)).await.is_err());
		tx.cancel().await.unwrap();

		// A field that only the current revision carries still distinguishes.
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let cursor_set = StateV2 {
			generation: 7,
			cursor: Some(3),
		};
		assert!(tx.put_compare_key(&StateKey, &next, Some(&cursor_set)).await.is_err());
		tx.cancel().await.unwrap();

		// The value is untouched by any of the failed swaps.
		let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
		assert_eq!(
			tx.get_key(&StateKey, None).await.unwrap(),
			Some(StateV2 {
				generation: 7,
				cursor: None
			})
		);
		tx.cancel().await.unwrap();
	}

	/// A stored value that will not decode must fail the *comparison*, not the
	/// statement.
	///
	/// Callers treat the refusal as the benign outcome and anything else as
	/// fatal, and several exist precisely to tolerate a value an older or newer
	/// binary wrote — a delete that tolerates a foreign index entry, a session
	/// sweep that skips what it cannot read. Surfacing a decode error here would
	/// escape into those callers as a failed statement.
	#[tokio::test]
	async fn an_undecodable_stored_value_fails_the_comparison_not_the_statement() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());
		let chk = StateV2 {
			generation: 7,
			cursor: None,
		};

		for which in ["put", "del", "clr"] {
			// Bytes no revisioned decode will accept.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			tx.set(key.clone(), b"not a revisioned value".to_vec()).await.unwrap();
			tx.commit().await.unwrap();

			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			let e = match which {
				"put" => tx.put_compare_key(&StateKey, &chk, Some(&chk)).await,
				"del" => tx.del_compare_key(&StateKey, Some(&chk)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&chk)).await,
			}
			.expect_err("an undecodable value cannot satisfy the check");
			assert!(
				matches!(
					crate::storage_error(&e),
					Some(surrealdb_kvs::Error::TransactionConditionNotMet)
				),
				"{which}: expected the comparison to be refused, got: {e}"
			);
			tx.cancel().await.unwrap();

			// And the bytes are still there, untouched by the refusal.
			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(
				tx.get(key.clone(), None).await.unwrap().as_deref(),
				Some(b"not a revisioned value".as_slice()),
				"{which}: the stored value must survive"
			);
			tx.cancel().await.unwrap();
		}
	}

	/// The conditional deletes fence the same way, and each is worth covering
	/// because each reaches the backend through a different method.
	///
	/// A delete that cannot match a value an earlier release wrote is the same
	/// defect wearing different clothes: the caller sees a mismatch where the
	/// value has not changed, and a retrying one never finishes.
	#[tokio::test]
	async fn a_conditional_delete_matches_a_value_stored_at_an_earlier_revision() {
		let (tf, sq) = factory().await;
		let key = Key::from(b"/!cas-revision-test".to_vec());

		let stored = StateV2 {
			generation: 7,
			cursor: None,
		};
		let wrong = StateV2 {
			generation: 99,
			cursor: None,
		};

		for which in ["del", "clr"] {
			// Exactly what the earlier release left behind.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			tx.put_compare(
				key.clone(),
				StateV1 {
					generation: 7,
				}
				.kv_encode_value()
				.unwrap(),
				None,
			)
			.await
			.unwrap();
			tx.commit().await.unwrap();

			// A check value that disagrees still fails, and leaves the value.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			let refused = match which {
				"del" => tx.del_compare_key(&StateKey, Some(&wrong)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&wrong)).await,
			};
			assert!(refused.is_err(), "{which}: a disagreeing check value must not delete");
			tx.cancel().await.unwrap();

			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(tx.get_key(&StateKey, None).await.unwrap().as_ref(), Some(&stored));
			tx.cancel().await.unwrap();

			// The value this build reads back does match, across the revisions.
			let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
			match which {
				"del" => tx.del_compare_key(&StateKey, Some(&stored)).await,
				_ => tx.clr_compare_key(&StateKey, Some(&stored)).await,
			}
			.unwrap_or_else(|e| panic!("{which}: {e}"));
			tx.commit().await.unwrap();

			let tx = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
			assert_eq!(tx.get_key(&StateKey, None).await.unwrap(), None, "{which}: not deleted");
			tx.cancel().await.unwrap();
		}
	}
}

/// A speculation in progress on a transaction ([`Transaction::speculate`]),
/// ended when [`Speculation::end`] is called or the guard is dropped.
///
/// The catalog ids held in the speculation
/// ([`crate::sequences::Sequences::hold_catalog_id`]) are answered by
/// [`Speculation::end`] to the caller, which confirms or releases each. A
/// guard dropped without `end` — its caller cancelled mid-run — releases
/// them itself, since nothing else can reach them.
pub struct Speculation {
	/// The transaction speculated on.
	base: Arc<Transaction>,
	/// The speculation's view of it.
	view: Arc<Transaction>,
	/// The state the speculation started from.
	from: crate::speculation::Overlay,
	/// The base's [`Transaction::state_changes`] reading when the speculation
	/// started.
	base_state: u64,
	ended: bool,
}

impl Speculation {
	/// The speculation's view: the transaction whose reads see the
	/// speculation's writes and whose writes are the speculation's.
	pub fn txn(&self) -> &Arc<Transaction> {
		&self.view
	}

	/// End the speculation, answering the writes it took.
	pub fn end(mut self) -> crate::speculation::Overlay {
		self.ended = true;
		self.finish()
	}

	/// End the view. A speculation that wrote nothing, over a base that did
	/// not change while it ran, leaves its cache's entries describing the
	/// state it started from, so they are kept for the next speculation from
	/// that state; any other drops them.
	fn finish(&mut self) -> crate::speculation::Overlay {
		let overlay = self.view.tr.end_speculation();
		let cache = self.view.cache.end_speculation();
		if let Some(cache) = cache
			&& overlay.writes_as(&self.from)
			&& self.base.state_changes() == self.base_state
			&& let Ok(mut kept) = self.base.speculative_caches.try_lock()
		{
			kept.retain(|(from, _, _)| !from.writes_as(&self.from));
			// A handful of states is all a block's planning revisits.
			if kept.len() >= 8 {
				kept.remove(0);
			}
			kept.push((self.from.clone(), self.base_state, cache));
		}
		overlay
	}
}

impl Drop for Speculation {
	fn drop(&mut self) {
		if !self.ended {
			let mut overlay = self.finish();
			for (sequence, id) in overlay.take_held() {
				self.base.sequences.release_catalog_id(&sequence, id);
			}
		}
	}
}

#[cfg(test)]
mod speculation_tests {
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::{Direction, Key, KeyRange, TransactionType};

	use crate::factory::TransactionFactory;
	use crate::sequences::Sequences;
	use crate::speculation::Overlay;

	async fn factory() -> (TransactionFactory, Sequences) {
		factory_on("mem://").await
	}

	async fn factory_on(url: &str) -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder(url, Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	fn key(bytes: &[u8]) -> Key<'static> {
		Key::from(bytes.to_vec())
	}

	/// A speculation that writes caches apart once it has, so what it caches
	/// after a write never reaches the cache kept for the state it started
	/// from, which a later speculation from that state reads through.
	#[tokio::test]
	async fn a_speculation_that_writes_leaves_the_kept_cache_alone() {
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::val::TableName;

		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
		let (ns, db, tb) = (NamespaceId(1), DatabaseId(1), TableName::new("t"));

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);

		let writing = tx.speculate(Overlay::default()).expect("not a view");
		writing.txn().bump_catalog_version(ns, db, &tb).await.unwrap();
		assert!(writing.txn().catalog_version(ns, db, &tb).await.unwrap().is_some());
		drop(writing);

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);
		assert_eq!(tx.catalog_version(ns, db, &tb).await.unwrap(), None);
		tx.cancel().await.unwrap();
	}

	/// A save point's rollback undoes writes a speculation may have read, so a
	/// cache kept from such a speculation must not answer the next one from
	/// the same overlay.
	#[tokio::test]
	async fn a_save_point_rollback_drops_the_caches_kept_for_speculations() {
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::val::TableName;

		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
		let (ns, db, tb) = (NamespaceId(1), DatabaseId(1), TableName::new("t"));

		tx.new_save_point().await.unwrap();
		tx.bump_catalog_version(ns, db, &tb).await.unwrap();

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert!(reading.txn().catalog_version(ns, db, &tb).await.unwrap().is_some());
		drop(reading);

		tx.rollback_to_save_point().await.unwrap();

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);
		tx.cancel().await.unwrap();
	}

	/// A change to the base under a live speculation can undo what the
	/// speculation has cached, so the speculation reads past those entries,
	/// and its cache is not kept for the next speculation either.
	#[tokio::test]
	async fn a_live_speculation_reads_past_a_base_change_beneath_it() {
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::val::TableName;

		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
		let (ns, db, tb) = (NamespaceId(1), DatabaseId(1), TableName::new("t"));

		tx.new_save_point().await.unwrap();
		tx.bump_catalog_version(ns, db, &tb).await.unwrap();

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert!(reading.txn().catalog_version(ns, db, &tb).await.unwrap().is_some());
		tx.rollback_to_save_point().await.unwrap();
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);

		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);

		// A base write is a change too: a cache kept from before it is not
		// reused after it.
		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert_eq!(reading.txn().catalog_version(ns, db, &tb).await.unwrap(), None);
		drop(reading);
		tx.bump_catalog_version(ns, db, &tb).await.unwrap();
		let reading = tx.speculate(Overlay::default()).expect("not a view");
		assert!(reading.txn().catalog_version(ns, db, &tb).await.unwrap().is_some());
		drop(reading);
		tx.cancel().await.unwrap();
	}

	/// The catalog ids held in a speculation are answered by `end` to its
	/// caller, which releases or confirms them; a speculation dropped
	/// without `end` releases them, so its sequence hands them out again.
	/// A speculation resumed from an overlay that lists held ids answers and
	/// releases only its own.
	#[tokio::test]
	async fn a_speculation_dropped_unended_releases_the_ids_held_in_it() {
		use crate::catalog::{DatabaseId, NamespaceId};
		use crate::sequences::CatalogSequence;

		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
		let tables = CatalogSequence::Tables(NamespaceId(1), DatabaseId(1));
		let hold = |speculation: &super::Speculation, id: u32| {
			assert!(speculation.txn().note_speculative_hold((tables.clone(), id)));
		};

		let ended = tx.speculate(Overlay::default()).expect("not a view");
		let kept = sq.hold_catalog_id(None, &tables).await.unwrap();
		hold(&ended, kept);
		let mut overlay = ended.end();
		let mut resumed = overlay.clone();
		assert_eq!(overlay.take_held(), vec![(tables.clone(), kept)]);

		let dropped = tx.speculate(resumed.clone()).expect("not a view");
		let released = sq.hold_catalog_id(None, &tables).await.unwrap();
		hold(&dropped, released);
		drop(dropped);
		assert_eq!(sq.next_catalog_id(None, &tables).await.unwrap(), released);
		assert_eq!(resumed.take_held(), vec![(tables.clone(), kept)]);

		let next = sq.next_catalog_id(None, &tables).await.unwrap();
		assert!(next > released, "{kept} is still held, {released} handed out: {next}");
		sq.release_catalog_id(&tables, kept);
		assert_eq!(sq.next_catalog_id(None, &tables).await.unwrap(), kept);
		tx.cancel().await.unwrap();
	}

	/// A speculation's writes are read back through its view, resumed from
	/// its overlay, and never by the transaction itself, during the
	/// speculation or after it.
	#[tokio::test]
	async fn a_speculation_reads_its_writes_and_leaves_the_storage_alone() {
		let (tf, sq) = factory().await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq.clone()).await.unwrap());
		tx.set(key(b"/a1"), b"stored".to_vec()).await.unwrap();
		tx.set(key(b"/a2"), b"stored".to_vec()).await.unwrap();
		let range = || KeyRange {
			start: key(b"/a"),
			end: key(b"/b"),
		};
		let keys_of = |values: Vec<(Vec<u8>, Vec<u8>)>| -> Vec<Vec<u8>> {
			values.into_iter().map(|(k, _)| k).collect()
		};

		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		let view = Arc::clone(speculation.txn());
		assert!(view.speculate(Overlay::default()).is_none(), "speculations do not nest");
		view.set(key(b"/a3"), b"speculated".to_vec()).await.unwrap();
		view.del(key(b"/a1")).await.unwrap();
		assert!(view.put(key(b"/a2"), b"again".to_vec()).await.is_err());
		assert_eq!(view.get(key(b"/a3"), None).await.unwrap().as_deref(), Some(&b"speculated"[..]));
		assert_eq!(view.get(key(b"/a1"), None).await.unwrap(), None);
		assert_eq!(
			keys_of(view.tr.getr(range(), None).await.unwrap().values),
			vec![b"/a2".to_vec(), b"/a3".to_vec()]
		);
		// While the speculation lasts, the transaction itself reads and writes
		// its storage, and the view reads what the transaction writes.
		assert_eq!(tx.get(key(b"/a3"), None).await.unwrap(), None);
		assert_eq!(tx.get(key(b"/a1"), None).await.unwrap().as_deref(), Some(&b"stored"[..]));
		tx.set(key(b"/a4"), b"stored".to_vec()).await.unwrap();
		assert_eq!(view.get(key(b"/a4"), None).await.unwrap().as_deref(), Some(&b"stored"[..]));
		// A save point the view opens frames its overlay.
		view.new_save_point().await.unwrap();
		view.set(key(b"/a5"), b"rolled back".to_vec()).await.unwrap();
		view.rollback_to_save_point().await.unwrap();
		assert_eq!(view.get(key(b"/a5"), None).await.unwrap(), None);
		let overlay = speculation.end();
		assert!(!overlay.is_empty());

		// A view held past its speculation's end takes no write.
		assert!(view.set(key(b"/a6"), b"late".to_vec()).await.is_err());
		assert!(view.del(key(b"/a2")).await.is_err());
		assert_eq!(tx.get(key(b"/a6"), None).await.unwrap(), None);
		assert_eq!(tx.get(key(b"/a2"), None).await.unwrap().as_deref(), Some(&b"stored"[..]));
		drop(view);

		// Resumed from its overlay, a speculation sees its earlier writes.
		let speculation = tx.speculate(overlay).expect("not a view");
		let view = Arc::clone(speculation.txn());
		view.tr.delr(range()).await.unwrap();
		view.set(key(b"/a9"), b"after".to_vec()).await.unwrap();
		assert_eq!(
			keys_of(view.tr.getr(range(), None).await.unwrap().values),
			vec![b"/a9".to_vec()]
		);
		// A cursor over the range reads it as the speculation leaves it.
		let mut cursor =
			view.tr.open_keys_cursor(range(), Direction::Backward, 0, None).await.unwrap();
		let batch = cursor.next_batch(10).await.unwrap();
		let keys: Vec<Vec<u8>> = batch.iter().map(<[u8]>::to_vec).collect();
		assert_eq!(keys, vec![b"/a9".to_vec()]);
		drop(cursor);
		drop(speculation);
		drop(view);
		assert_eq!(
			keys_of(tx.tr.getr(range(), None).await.unwrap().values),
			vec![b"/a1".to_vec(), b"/a2".to_vec(), b"/a4".to_vec()]
		);
		tx.cancel().await.unwrap();
	}

	/// A speculation's view converts a datetime as the storage beneath it
	/// does, both into the stamps the change feed is keyed by and into the
	/// version a read takes, so a statement planned over the view reads the
	/// catalog at the version its `VERSION` clause names. Only a backend whose
	/// versions are not its stamps tells the two apart: SurrealKV, which this
	/// crate opens when built with `kv-surrealkv`.
	async fn a_view_converts_a_datetime_as_its_storage_does(url: &str) {
		use surrealdb_kvs::timestamp::{BoxTimeStampImpl, MAX_TIMESTAMP_BYTES};

		let (tf, sq) = factory_on(url).await;
		let tx = Arc::new(tf.transaction(TransactionType::Write, sq).await.unwrap());
		let speculation = tx.speculate(Overlay::default()).expect("not a view");
		let view = speculation.txn();
		let now = chrono::Utc::now();
		let encoded = |stamps: BoxTimeStampImpl| {
			let stamp = stamps.create_from_datetime(now).expect("now converts");
			let mut bytes = [0; MAX_TIMESTAMP_BYTES];
			stamp.encode(&mut bytes).to_vec()
		};
		assert_eq!(encoded(view.version_impl()), encoded(tx.version_impl()));
		assert_eq!(encoded(view.timestamp_impl()), encoded(tx.timestamp_impl()));
		drop(speculation);
		tx.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn a_view_converts_a_datetime_as_its_storage_does_on_mem() {
		a_view_converts_a_datetime_as_its_storage_does("mem://").await;
	}

	#[cfg(feature = "kv-surrealkv")]
	#[tokio::test]
	async fn a_view_converts_a_datetime_as_its_storage_does_on_surrealkv() {
		let dir = temp_dir::TempDir::new().unwrap();
		let url = format!("surrealkv://{}", dir.path().display());
		a_view_converts_a_datetime_as_its_storage_does(&url).await;
	}
}

#[cfg(test)]
mod state_changes_tests {
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::{Key, TransactionType};

	use crate::factory::TransactionFactory;
	use crate::sequences::Sequences;

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	/// A read leaves the count as it was; a write, a deletion and a rollback to
	/// a save point, which changes what the transaction reads without writing,
	/// each grow it.
	#[tokio::test]
	async fn every_change_to_what_the_transaction_reads_is_counted() {
		let (tf, sq) = factory().await;
		let tx = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		let key = || Key::from(b"/state-changes".to_vec());
		let start = tx.state_changes();
		tx.get(key(), None).await.unwrap();
		assert_eq!(tx.state_changes(), start, "a read changes nothing");
		tx.set(key(), b"1".to_vec()).await.unwrap();
		let written = tx.state_changes();
		assert!(written > start, "a write is counted");
		tx.new_save_point().await.unwrap();
		tx.set(key(), b"2".to_vec()).await.unwrap();
		let before_rollback = tx.state_changes();
		assert!(before_rollback > written, "a write under a save point is counted");
		tx.rollback_to_save_point().await.unwrap();
		assert!(tx.state_changes() > before_rollback, "a rollback is counted");
		assert_eq!(tx.get(key(), None).await.unwrap(), Some(b"1".to_vec()));
		let rolled_back = tx.state_changes();
		tx.del(key()).await.unwrap();
		assert!(tx.state_changes() > rolled_back, "a deletion is counted");
		tx.cancel().await.unwrap();
	}
}

#[cfg(test)]
mod catalog_epoch_tests {
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::{Key, TransactionType};

	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::factory::TransactionFactory;
	use crate::sequences::Sequences;
	use crate::val::TableName;

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	/// A transaction names the catalog it sees by the count of catalog
	/// commits finished before it opened, for as long as no other starts:
	/// one open across a catalog commit, one that stamped a version of its
	/// own and a speculation name none, and one opened after the commit names
	/// the next count. A commit that stamped nothing moves no count.
	#[tokio::test]
	async fn a_transaction_names_the_catalog_commits_it_sees() {
		let (tf, sq) = factory().await;
		let (ns, db, tb) = (NamespaceId(1), DatabaseId(1), TableName::new("t"));
		let before = tf.transaction(TransactionType::Read, sq.clone()).await.unwrap();
		let epoch = before.catalog_epoch().expect("a memory store is written by its datastore");
		let data = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		data.set(Key::from(b"data".to_vec()), b"1".to_vec()).await.unwrap();
		data.commit().await.unwrap();
		assert_eq!(before.catalog_epoch(), Some(epoch), "a data commit moved the count");
		let writer = tf.transaction(TransactionType::Write, sq.clone()).await.unwrap();
		assert_eq!(writer.catalog_epoch(), Some(epoch));
		writer.bump_catalog_version(ns, db, &tb).await.unwrap();
		assert_eq!(writer.catalog_epoch(), None, "a transaction that stamped a version");
		writer.commit().await.unwrap();
		assert_eq!(before.catalog_epoch(), None, "a transaction open across a catalog commit");
		let after = Arc::new(tf.transaction(TransactionType::Read, sq.clone()).await.unwrap());
		assert_eq!(after.catalog_epoch(), Some(epoch + 1));
		let view = after.speculate(crate::speculation::Overlay::default()).expect("not a view");
		assert_eq!(view.txn().catalog_epoch(), None, "a speculation");
		drop(view);
		before.cancel().await.unwrap();
		after.cancel().await.unwrap();
	}
}

#[cfg(test)]
mod object_version_tests {
	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::key::KVKey;
	use surrealdb_kvs::{Key, TransactionType};

	use super::Transaction;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::factory::TransactionFactory;
	use crate::key::schema::{
		AnalyzerKey, ApiKey, DbAccessMethodKey, FunctionKey, MlModelKey, ModuleKey,
		NsAccessMethodKey, ParamKey, RecordKey, SequenceKey, TableKey,
	};
	use crate::sequences::Sequences;
	use crate::val::TableName;
	use crate::values::catalog_version::CatalogObject;

	const NS: NamespaceId = NamespaceId(3);
	const DB: DatabaseId = DatabaseId(7);

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	async fn write(tf: &TransactionFactory, sq: &Sequences) -> Transaction {
		tf.transaction(TransactionType::Write, sq.clone()).await.unwrap()
	}

	/// Every kind of object with a version of its own, with the key of its
	/// definition: each one's name fields in `ns`/`db`.
	fn objects() -> Vec<(CatalogObject<'static>, Key<'static>)> {
		let (ns, db) = (NS, DB);
		let b = Cow::Borrowed;
		vec![
			(
				CatalogObject::Function("f"),
				FunctionKey {
					ns,
					db,
					fc: b("f"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Param("p"),
				ParamKey {
					ns,
					db,
					pa: b("p"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Access("a"),
				DbAccessMethodKey {
					ns,
					db,
					ac: b("a"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Model {
					name: "m",
					version: "1.0.0",
				},
				MlModelKey {
					ns,
					db,
					ml: b("m"),
					vn: b("1.0.0"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Module("mod::x"),
				ModuleKey {
					ns,
					db,
					md: b("mod::x"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Analyzer("az"),
				AnalyzerKey {
					ns,
					db,
					az: b("az"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Sequence("s"),
				SequenceKey {
					ns,
					db,
					sq: b("s"),
				}
				.encode_key()
				.unwrap(),
			),
			(
				CatalogObject::Api("/x"),
				ApiKey {
					ns,
					db,
					ap: b("/x"),
				}
				.encode_key()
				.unwrap(),
			),
		]
	}

	/// Defining, altering and removing each kind of object mints a version
	/// every time, through each of the write methods a definition is written
	/// or deleted with, and a removal leaves a version behind rather than
	/// none.
	#[tokio::test]
	async fn define_alter_and_remove_each_mint_a_new_version() {
		let (tf, sq) = factory().await;
		for (object, key) in objects() {
			let tx = write(&tf, &sq).await;
			assert_eq!(
				tx.catalog_object_version(NS, DB, object).await.unwrap(),
				None,
				"{object:?}"
			);
			tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
			let defined = tx.catalog_object_version(NS, DB, object).await.unwrap();
			assert!(defined.is_some(), "a definition stamps {object:?}");
			tx.replace_raw(key.clone(), b"altered".to_vec()).await.unwrap();
			let altered = tx.catalog_object_version(NS, DB, object).await.unwrap();
			assert!(altered.is_some() && altered != defined, "an alteration restamps {object:?}");
			tx.del(key.clone()).await.unwrap();
			let removed = tx.catalog_object_version(NS, DB, object).await.unwrap();
			assert!(removed.is_some() && removed != altered, "a removal restamps {object:?}");
			tx.commit().await.unwrap();

			let tx = write(&tf, &sq).await;
			assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), removed);
			tx.put(key.clone(), b"redefined".to_vec()).await.unwrap();
			let redefined = tx.catalog_object_version(NS, DB, object).await.unwrap();
			assert!(redefined.is_some() && redefined != removed, "{object:?}");
			tx.put_compare(key.clone(), b"again".to_vec(), Some(b"redefined".to_vec()))
				.await
				.unwrap();
			let compared = tx.catalog_object_version(NS, DB, object).await.unwrap();
			assert!(compared != redefined, "a guarded write restamps {object:?}");
			tx.clr(key.clone()).await.unwrap();
			assert!(tx.catalog_object_version(NS, DB, object).await.unwrap() != compared);
			tx.cancel().await.unwrap();
		}
	}

	/// A write that leaves a definition as it is — the same bytes again, or
	/// a deletion of a definition that is not there — leaves its version
	/// standing.
	#[tokio::test]
	async fn an_unchanged_definition_keeps_its_version() {
		let (tf, sq) = factory().await;
		let (object, key) = objects().remove(0);
		let tx = write(&tf, &sq).await;
		tx.del(key.clone()).await.unwrap();
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), None);
		tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		let defined = tx.catalog_object_version(NS, DB, object).await.unwrap();
		tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		tx.replace_raw(key.clone(), b"defined".to_vec()).await.unwrap();
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), defined);
		tx.del(key.clone()).await.unwrap();
		let removed = tx.catalog_object_version(NS, DB, object).await.unwrap();
		assert_ne!(removed, defined);
		tx.del(key.clone()).await.unwrap();
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), removed);
		tx.cancel().await.unwrap();
	}

	/// Each kind's version is its own: stamping one object moves no other
	/// object's version, and objects of different kinds that share a name are
	/// different objects.
	#[tokio::test]
	async fn objects_are_versioned_apart() {
		let (tf, sq) = factory().await;
		let tx = write(&tf, &sq).await;
		let all = objects();
		for (_, key) in &all {
			tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		}
		let mut versions = Vec::new();
		for (object, _) in &all {
			versions.push(tx.catalog_object_version(NS, DB, *object).await.unwrap().unwrap());
		}
		let mut distinct = versions.clone();
		distinct.sort();
		distinct.dedup();
		assert_eq!(distinct.len(), versions.len(), "each object holds a version of its own");
		tx.set(all[0].1.clone(), b"altered".to_vec()).await.unwrap();
		for ((object, _), before) in all.iter().zip(&versions).skip(1) {
			assert_eq!(
				tx.catalog_object_version(NS, DB, *object).await.unwrap(),
				Some(*before),
				"{object:?}"
			);
		}
		assert_eq!(
			tx.catalog_object_version(NS, DB, CatalogObject::Param("f")).await.unwrap(),
			None,
			"a param named as a function is another object"
		);
		tx.cancel().await.unwrap();
	}

	/// A version minted by a transaction that rolls back is never seen by
	/// anyone: the object reads as it did before.
	#[tokio::test]
	async fn a_rolled_back_version_is_never_visible() {
		let (tf, sq) = factory().await;
		let (object, key) = objects().remove(0);
		let tx = write(&tf, &sq).await;
		tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		let committed = tx.catalog_object_version(NS, DB, object).await.unwrap();
		tx.commit().await.unwrap();

		let tx = write(&tf, &sq).await;
		tx.set(key.clone(), b"altered".to_vec()).await.unwrap();
		let minted = tx.catalog_object_version(NS, DB, object).await.unwrap();
		assert_ne!(minted, committed);
		let concurrent = write(&tf, &sq).await;
		assert_eq!(
			concurrent.catalog_object_version(NS, DB, object).await.unwrap(),
			committed,
			"an uncommitted version is the writer's alone"
		);
		concurrent.cancel().await.unwrap();
		tx.cancel().await.unwrap();

		let tx = write(&tf, &sq).await;
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), committed);
		tx.cancel().await.unwrap();
	}

	/// A transaction reads an object's version from storage once, `None`
	/// included; a version it mints is what it reads afterwards, without a
	/// read; and a version committed by another transaction meanwhile is not.
	#[tokio::test]
	async fn the_version_is_cached_per_transaction() {
		let (tf, sq) = factory().await;
		let (object, key) = objects().remove(1);
		let (ghost, _) = objects().remove(2);
		let tx = write(&tf, &sq).await;
		tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		tx.commit().await.unwrap();

		let tx = write(&tf, &sq).await;
		let seen = tx.catalog_object_version(NS, DB, object).await.unwrap();
		assert!(seen.is_some());
		assert_eq!(tx.catalog_object_version(NS, DB, ghost).await.unwrap(), None);
		let reads = tx.metrics_snapshot_for_test().ops_get;

		let other = write(&tf, &sq).await;
		other.set(key.clone(), b"altered".to_vec()).await.unwrap();
		other.commit().await.unwrap();

		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), seen);
		assert_eq!(tx.catalog_object_version(NS, DB, ghost).await.unwrap(), None);
		assert_eq!(tx.metrics_snapshot_for_test().ops_get, reads, "answered from the cache");

		let stamps = tx.catalog_object_stamps();
		let minted = tx.bump_catalog_object_version(NS, DB, object).await.unwrap();
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), Some(minted));
		assert_eq!(tx.metrics_snapshot_for_test().ops_get, reads);
		assert_eq!(tx.catalog_object_stamps(), stamps + 1, "a stamp is counted");
		tx.cancel().await.unwrap();
	}

	/// A table answers its `!cv` version through the same call, and writes
	/// that are not of a versioned object's definition — a record, a table
	/// definition, a namespace-level access method — stamp nothing here.
	#[tokio::test]
	async fn tables_and_other_keys_are_not_object_versions() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("person");
		let tx = write(&tf, &sq).await;
		let stamps = tx.catalog_object_stamps();
		tx.set(
			RecordKey {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&tb),
				id: Cow::Owned(crate::val::RecordIdKey::Number(1)),
			}
			.encode_key()
			.unwrap(),
			b"record".to_vec(),
		)
		.await
		.unwrap();
		tx.set(
			TableKey {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&tb),
			}
			.encode_key()
			.unwrap(),
			b"table".to_vec(),
		)
		.await
		.unwrap();
		tx.set(
			NsAccessMethodKey {
				ns: NS,
				ac: Cow::Borrowed("a"),
			}
			.encode_key()
			.unwrap(),
			b"access".to_vec(),
		)
		.await
		.unwrap();
		assert_eq!(tx.catalog_object_stamps(), stamps, "nothing here is a versioned object");
		assert_eq!(
			tx.catalog_object_version(NS, DB, CatalogObject::Table(&tb)).await.unwrap(),
			None
		);
		tx.bump_catalog_version(NS, DB, &tb).await.unwrap();
		assert_eq!(
			tx.catalog_object_version(NS, DB, CatalogObject::Table(&tb)).await.unwrap(),
			tx.catalog_version(NS, DB, &tb).await.unwrap()
		);
		assert_eq!(tx.catalog_object_stamps(), stamps);
		tx.cancel().await.unwrap();
	}
}

#[cfg(test)]
mod older_build_write_tests {
	//! A build that predates catalog versions shares the store during a
	//! rolling upgrade and writes definitions without moving their versions.
	//! These write through the transactor directly, as such a build does.

	use std::borrow::Cow;
	use std::sync::Arc;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_kvs::key::KVKey;
	use surrealdb_kvs::value::KVValue;
	use surrealdb_kvs::{Key, TransactionType};

	use super::Transaction;
	use crate::catalog::{DatabaseId, NamespaceId, StoredTableDefinition, TableId};
	use crate::factory::TransactionFactory;
	use crate::key::schema::{CatalogVersionKey, FunctionKey, TableKey};
	use crate::sequences::Sequences;
	use crate::val::TableName;
	use crate::values::catalog_version::{CatalogObject, CatalogVersion};

	const NS: NamespaceId = NamespaceId(3);
	const DB: DatabaseId = DatabaseId(7);

	async fn factory() -> (TransactionFactory, Sequences) {
		let builder = surrealdb_kvs_any::Backends::community()
			.new_transaction_builder("mem://", Default::default(), ConfigMap::default())
			.await
			.unwrap();
		let tf = TransactionFactory::new(
			Arc::new(crate::triggers::CommitTriggers::new()),
			builder,
			Arc::new(Default::default()),
		);
		let sequences = Sequences::new(tf.clone(), uuid::Uuid::new_v4());
		(tf, sequences)
	}

	async fn txn(tf: &TransactionFactory, sq: &Sequences) -> Transaction {
		tf.transaction(TransactionType::Write, sq.clone()).await.unwrap()
	}

	fn table_key(tb: &TableName) -> Key<'static> {
		TableKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(tb),
		}
		.encode_key()
		.unwrap()
	}

	fn stored(tb: &TableName) -> Vec<u8> {
		StoredTableDefinition::new(NS, DB, TableId(1), tb.clone()).kv_encode_value().unwrap()
	}

	/// A table stamped by this build and then redefined, or removed, by an
	/// older build reads as unstamped — through its own version and through
	/// its database's listing — until this build stamps it again.
	#[tokio::test]
	async fn a_table_rewritten_by_an_older_build_is_unstamped() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("person");
		let tx = txn(&tf, &sq).await;
		tx.tr.set(table_key(&tb), stored(&tb)).await.unwrap();
		tx.bump_catalog_version(NS, DB, &tb).await.unwrap();
		tx.commit().await.unwrap();

		let tx = txn(&tf, &sq).await;
		let stamped = tx.catalog_version(NS, DB, &tb).await.unwrap();
		assert!(stamped.is_some(), "this build's stamp is proven");
		let listed = tx.database_catalog_state(NS, DB).await.unwrap();
		assert_eq!(listed.versions, vec![(tb.clone(), stamped.unwrap())]);
		tx.cancel().await.unwrap();

		// An older build redefines the table: `!tb` changes, `!cv` stays.
		let tx = txn(&tf, &sq).await;
		tx.tr.set(table_key(&tb), stored(&tb)).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), None);
		let listed = tx.database_catalog_state(NS, DB).await.unwrap();
		assert_eq!(listed.tables, vec![tb.clone()]);
		assert!(listed.versions.is_empty(), "an unproven version is not listed");
		tx.cancel().await.unwrap();

		// This build stamps it again, then an older build removes it.
		let tx = txn(&tf, &sq).await;
		tx.bump_catalog_version(NS, DB, &tb).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert!(tx.catalog_version(NS, DB, &tb).await.unwrap().is_some());
		tx.tr.del(table_key(&tb)).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), None);
		let listed = tx.database_catalog_state(NS, DB).await.unwrap();
		assert!(listed.tables.is_empty() && listed.versions.is_empty());
		tx.cancel().await.unwrap();
	}

	/// A function stamped by this build and then redefined by an older build
	/// has no version until this build writes it again.
	#[tokio::test]
	async fn an_object_rewritten_by_an_older_build_is_unstamped() {
		let (tf, sq) = factory().await;
		let key = FunctionKey {
			ns: NS,
			db: DB,
			fc: Cow::Borrowed("f"),
		}
		.encode_key()
		.unwrap();
		let object = CatalogObject::Function("f");
		let tx = txn(&tf, &sq).await;
		tx.set(key.clone(), b"defined".to_vec()).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert!(tx.catalog_object_version(NS, DB, object).await.unwrap().is_some());
		tx.tr.set(key.clone(), b"redefined".to_vec()).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_object_version(NS, DB, object).await.unwrap(), None);
		tx.set(key.clone(), b"defined again".to_vec()).await.unwrap();
		assert!(tx.catalog_object_version(NS, DB, object).await.unwrap().is_some());
		tx.cancel().await.unwrap();
	}

	/// A definition written after its table was stamped, in the same
	/// transaction, is what the stamped version is proven against.
	#[tokio::test]
	async fn a_definition_written_after_its_stamp_keeps_the_version_proven() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("person");
		let key = TableKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
		};
		let tx = txn(&tf, &sq).await;
		let version = tx.stamp_catalog_version(NS, DB, &tb, None).await.unwrap();
		let stored = StoredTableDefinition::new(NS, DB, TableId(1), tb.clone());
		tx.set_key(&key, &stored).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), Some(version));
		tx.cancel().await.unwrap();
	}

	/// A version stored with no print proves nothing.
	#[tokio::test]
	async fn a_version_without_a_print_is_unstamped() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("person");
		let tx = txn(&tf, &sq).await;
		tx.tr.set(table_key(&tb), stored(&tb)).await.unwrap();
		let key = CatalogVersionKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&tb),
		};
		tx.tr
			.set(
				key.encode_key().unwrap(),
				CatalogVersion(uuid::Uuid::now_v7(), None).kv_encode_value().unwrap(),
			)
			.await
			.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), None);
		tx.cancel().await.unwrap();
	}

	/// A live-query registration rewrites the definition with no catalog
	/// change, and keeps a proven version proven and unmoved.
	#[tokio::test]
	async fn a_live_registration_keeps_the_version_proven() {
		let (tf, sq) = factory().await;
		let tb = TableName::new("person");
		let tx = txn(&tf, &sq).await;
		tx.tr.set(table_key(&tb), stored(&tb)).await.unwrap();
		let version = tx.stamp_catalog_version(NS, DB, &tb, None).await.unwrap();
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		tx.bump_table_lives_cache(NS, DB, &tb).await.unwrap();
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), Some(version));
		tx.commit().await.unwrap();
		let tx = txn(&tf, &sq).await;
		assert_eq!(tx.catalog_version(NS, DB, &tb).await.unwrap(), Some(version));
		tx.cancel().await.unwrap();
	}
}
