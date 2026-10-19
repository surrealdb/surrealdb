//! DiskANN index orchestration.
//!
//! This module connects SurrealDB index writes, background compaction, and KNN lookup to the
//! KV-backed DiskANN graph provider. User writes append shard-prefixed pending updates (`!dw`) and
//! mark that shard's sharded pending-state guard (`!dy`) non-empty. Compaction consumes a bounded
//! pending batch, mutates the graph/document mappings, and advances each drained shard's `!dy`
//! guard toward empty only after empty-range confirmation. Lookup scans the `!dw` range of every
//! non-empty `!dy` shard, plus the legacy unsharded `!dr` range unconditionally for the dual-read
//! migration (a cheap empty probe once that range has drained). The legacy `!dp` guard is owned by
//! pre-change nodes only.
//!
//! Mixed-version note: a pre-change node (one that predates the `!dw`/`!dy` layout) scans only the
//! legacy `!dr` range, so during a rolling upgrade it cannot see un-compacted `!dw` writes made by
//! upgraded nodes — a KNN query routed to such a node may briefly omit those records until a new
//! compactor folds them into the graph (which every version reads) or the upgrade completes. This
//! is transient and never loses or corrupts data; full read consistency during the upgrade would
//! require gating `!dw` writes on a cluster storage version, which is intentionally not done here.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, bail};
use common::EngineError;
use diskann::graph::DiskANNIndex as RawDiskAnnIndex;
use diskann::graph::config::experimental::InsertRetry;
use diskann::graph::config::{Builder, IntraBatchCandidates, MaxDegree, PruneKind};
use diskann::graph::glue::{HybridPredicate, Predicate, PredicateMut, SearchAccessor as _};
use diskann::graph::search::Knn;
use diskann::graph::search_output_buffer::IdDistance;
use diskann::neighbor::{Neighbor, NeighborPriorityQueue};
use diskann::provider::{Delete, Guard, SetElement};
use diskann_utils::views::Matrix;
use diskann_vector::Half;
use diskann_vector::distance::Metric;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::api::ValsBatch;
use surrealdb_kvs::{Direction, Error as KvsError, Val};
use tokio::sync::{RwLock, RwLockWriteGuard};

use crate::catalog::{
	DiskAnnParams, Distance, TableId, VectorType, keeps_pending_members, keeps_vector_doc_elements,
};
use crate::docids::{DocId, LegacyVectorDocIds};
use crate::env::{IndexEnv, expect_not_done};
use crate::key::schema::{DiskannRecordPendingKey, DiskannRecordPendingShardKey};
use crate::key::{KVKey, KVKeyDecode, KVValue, Key, TypedRange};
use crate::parallel::ReadView;
use crate::trees::diskann::cache::{CacheView, Continuity, DiskAnnCache};
use crate::trees::diskann::docs::{DiskAnnDocs, DiskAnnVecDocs, StagedVector};
use crate::trees::diskann::filter::{DiskAnnTruthyDocumentFilter, FilterCache};
use crate::trees::diskann::frame::{DiskAnnBuildFrame, FrameEnv, FrameKv, ViewGeneration};
#[cfg(test)]
use crate::trees::diskann::frame::{FrameFaults, FrameHooks};
use crate::trees::diskann::provider::{
	DiskAnnProvider, DiskAnnProviderContext, DiskAnnSearchAccessor, DiskAnnStrategy,
	DiskAnnVectorElement, NodeWrite,
};
use crate::trees::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnElement, DiskAnnPendingState, DiskAnnPendingStateKind,
	DiskAnnRecordPendingMember, DiskAnnRecordPendingUpdate, ElementId,
};
use crate::trees::hnsw::VectorId;
use crate::trees::knn::{Ids64, KnnResultBuilder, OVERQUERY_CEILING_FACTOR};
use crate::trees::pending::{
	PendingBacklogReport, PendingScan, PendingScanStats, take_pending_record_id,
};
use crate::trees::vector::{
	DistanceExt as _, SerializedVector, SharedVector, Vector, serialized_vector_from_value,
};
use crate::trees::{KnnCondFilter, KnnIteratorResult, doc_elements};
use crate::val::{Number, RecordId, RecordIdKey, Value};
use crate::{
	CompactionApply, IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};

/// Soft per-batch limits for [`DiskAnnIndex::prepare_compaction`]. When either cap fires,
/// `has_more = true` is set on the [`DiskAnnCompactionPlan`] and the caller is expected to run
/// another compaction iteration.
///
/// Pending records are sharded under `!dw{shard}` and guarded per shard by `!dy`, so compaction
/// drains and advances one shard's guard at a time and lookup scans only the non-empty shards.
/// That is what bounds KNN's pending work to the active backlog rather than to the whole pending
/// set on every query.
const DISKANN_COMPACTION_MAX_PENDING_KEYS: usize = 1024;
const DISKANN_COMPACTION_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

/// The most new vectors one compaction plan captures, summed over its pendings' `new_vectors`.
///
/// Bounds a frame's fresh vectors, and with them its overlay and its write batch. An empty plan
/// still admits its first entry whatever it carries, so a record whose own vectors exceed the cap
/// is captured in a plan of its own, in one frame. Either way the frame inserts its fresh vectors
/// in calls of at most [`DISKANN_INSERT_BATCH_MAX_VECTORS`].
const DISKANN_COMPACTION_MAX_PENDING_VECTORS: usize = 1024;

/// Positional intra-batch candidates a batched insert considers for each vector: the batch members
/// next to it that its prune sees beside what its search of the pre-batch graph found.
const DISKANN_INTRA_BATCH_CANDIDATES: u32 = 32;

/// The most fresh vectors one call of upstream's batched insert takes. A frame inserts its fresh
/// vectors in consecutive calls of at most this many, in batch order, each searching a graph that
/// holds the calls before it.
///
/// This bounds graph quality, not memory. A vector in a batched insert searches the graph as it
/// stood before its call, and sees the call's other vectors only through the
/// [`DISKANN_INTRA_BATCH_CANDIDATES`] positionally next to it. Batch order is pending-key order,
/// which says nothing about similarity, so two near vectors of one call farther apart than that
/// window never see each other, where a serial build links them. The smaller the call, the fewer
/// such pairs; below this size, recall stops improving while each of a frame's tasks gets less
/// work between the call's join barriers.
const DISKANN_INSERT_BATCH_MAX_VECTORS: usize = 256;

/// How often a frame checks whether the statement running it has been cancelled.
const DISKANN_FRAME_WATCHDOG_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

/// Exact pending key/value observed by a DiskANN compaction read phase.
///
/// Held as bytes rather than as the key and value they decode to, for the reason
/// spelled out on the HNSW type of the same name: the delete is conditional on a
/// byte comparison with what is stored, and a re-encode is not guaranteed to
/// reproduce those bytes.
struct CapturedPendingKey {
	/// Exact pending key captured during the read phase.
	key: Vec<u8>,
	/// Value observed for the key; apply deletes it conditionally before mutating the graph.
	value: Val,
}

#[derive(Clone)]
struct PendingOperation {
	/// Owning record/document ID after coalescing record-keyed pending updates.
	id: VectorId,
	/// Record key when this operation came from a record-keyed pending.
	///
	/// The graph baseline in `old_vectors` lives under the *captured* doc-ID
	/// (`id`), but a delete removes the record's shared doc-ID mapping and a
	/// re-create before compaction folds its vectors into this same pending. The
	/// record key lets compaction re-resolve the *current* doc-ID for
	/// `new_vectors`, so re-created records are inserted under a live mapping
	/// instead of the stale captured one.
	record: Option<Arc<RecordIdKey>>,
	/// Vectors currently represented by the compacted graph.
	old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should be represented after compaction.
	new_vectors: Vec<SerializedVector>,
}

/// Snapshot of all DiskANN pending-state guard shards observed by a read transaction.
type PendingStateSnapshot = Vec<Option<DiskAnnPendingState>>;

/// Prepared read-phase DiskANN compaction batch.
///
/// The plan captures exact pending keys and values so the write phase can delete them with `delc`
/// before applying graph mutations. It also carries the compaction generation and pending-state
/// snapshot used to reject stale plans and to clear `!dy` shards conservatively.
pub struct DiskAnnCompactionPlan {
	/// Compaction generation observed while preparing the plan.
	generation: Option<u64>,
	/// Pending-state guard shards observed before scanning `!dr`.
	pending_state: PendingStateSnapshot,
	/// Pending keys captured for conditional deletion.
	captured_keys: Vec<CapturedPendingKey>,
	/// Coalesced graph/document operations derived from captured pending records.
	pending: Vec<PendingOperation>,
	/// Shards whose pending range was fully drained this pass and may step toward empty on apply.
	/// Only populated once the legacy `!dr` range is empty (see [`Self::prepare_compaction`]).
	cleared_shards: Vec<u16>,
	/// True when prepare stopped because the bounded batch limit was reached.
	has_more: bool,
}

impl DiskAnnCompactionPlan {
	/// Returns whether the plan captured pending keys to apply.
	pub(crate) fn has_work(&self) -> bool {
		!self.captured_keys.is_empty()
	}

	/// Returns whether the write phase should run for this plan.
	///
	/// Only `Some(non-Empty)` `!dy` shards are applyable — a `None` shard never had a `!dy` key, so
	/// it holds no sharded data and nothing to clear (this matches the phase-2 scan, which skips
	/// `None`/`Empty` shards). Treating `None` as applyable would make a quiescent index with
	/// untouched shards schedule a no-op write transaction on every compaction cycle forever.
	pub fn requires_apply(&self) -> bool {
		self.has_work()
			|| self.pending_state.iter().any(|state| {
				state.as_ref().is_some_and(|state| state.kind != DiskAnnPendingStateKind::Empty)
			})
	}

	/// Returns whether another compaction pass should be scheduled for remaining pending keys.
	pub fn has_more(&self) -> bool {
		self.has_more
	}
}

/// Coalesces record-keyed pending updates into a bounded compaction plan.
struct PendingPlanBuilder {
	generation: Option<u64>,
	pending_state: PendingStateSnapshot,
	captured_keys: Vec<CapturedPendingKey>,
	pending: Vec<PendingOperation>,
	pending_by_id: HashMap<VectorId, usize>,
	encoded_bytes: usize,
	/// The new vectors of every captured pending, counted as captured: an upper bound on the
	/// frame's fresh vectors, since coalescing replaces a record's vectors rather than adding to
	/// them, and the apply only removes vectors already in the graph or repeated in the batch.
	new_vectors: usize,
	has_more: bool,
}

impl PendingPlanBuilder {
	fn new(generation: Option<u64>, pending_state: PendingStateSnapshot) -> Self {
		Self {
			generation,
			pending_state,
			captured_keys: Vec::new(),
			pending: Vec::new(),
			pending_by_id: HashMap::default(),
			encoded_bytes: 0,
			new_vectors: 0,
			has_more: false,
		}
	}

	/// Captures one pending key/value and folds its operations into the plan:
	/// one per record the entry holds.
	fn add(
		&mut self,
		key: Vec<u8>,
		value: Val,
		pending: impl IntoIterator<Item = PendingOperation>,
	) -> bool {
		let pending: Vec<PendingOperation> = pending.into_iter().collect();
		let vectors: usize = pending.iter().map(|p| p.new_vectors.len()).sum();
		if self.captured_keys.len() >= DISKANN_COMPACTION_MAX_PENDING_KEYS
			|| (!self.captured_keys.is_empty()
				&& (self.encoded_bytes + key.len() + value.len()
					> DISKANN_COMPACTION_MAX_PENDING_BYTES
					|| self.new_vectors + vectors > DISKANN_COMPACTION_MAX_PENDING_VECTORS))
		{
			self.has_more = true;
			return false;
		}
		self.add_authorized(key, value, pending);
		true
	}

	/// Whether the bounded batch can still admit `keys` more captured keys totalling `bytes` and
	/// carrying `vectors` new vectors, mirroring the reject condition in [`Self::add`]. Used to
	/// admit a legacy `!dr` entry and its folded sharded `!dw` counterpart as a single atomic pair,
	/// so the budget never splits the pair across compaction passes (which would reintroduce the
	/// phantom the fold prevents).
	fn has_room_for(&self, keys: usize, bytes: usize, vectors: usize) -> bool {
		if self.captured_keys.len() + keys > DISKANN_COMPACTION_MAX_PENDING_KEYS {
			return false;
		}
		// An empty batch always admits its first entries (mirrors `add`'s `!is_empty()` guard) so a
		// single oversized pair can still make progress.
		self.captured_keys.is_empty()
			|| (self.encoded_bytes + bytes <= DISKANN_COMPACTION_MAX_PENDING_BYTES
				&& self.new_vectors + vectors <= DISKANN_COMPACTION_MAX_PENDING_VECTORS)
	}

	/// Captures a key/op already authorized by [`Self::has_room_for`], bypassing the per-call
	/// budget guard in [`Self::add`].
	///
	/// Used for each half of a folded `!dr`+`!dw` pair: `has_room_for` authorizes the whole pair up
	/// front, so neither half may be rejected afterwards. Without this, `add`'s byte guard could
	/// admit the legacy half and then reject the sharded half once the batch is non-empty (when the
	/// combined value exceeds `DISKANN_COMPACTION_MAX_PENDING_BYTES`), orphaning the sharded entry
	/// — phase 2 skips it as folded, so it is neither applied nor deleted and resurfaces as a
	/// phantom.
	fn add_authorized(
		&mut self,
		key: Vec<u8>,
		value: Val,
		pending: impl IntoIterator<Item = PendingOperation>,
	) {
		self.encoded_bytes += key.len() + value.len();
		self.captured_keys.push(CapturedPendingKey {
			key,
			value,
		});
		for pending in pending {
			self.new_vectors += pending.new_vectors.len();
			self.add_pending(pending);
		}
		if self.captured_keys.len() >= DISKANN_COMPACTION_MAX_PENDING_KEYS
			|| self.encoded_bytes >= DISKANN_COMPACTION_MAX_PENDING_BYTES
			|| self.new_vectors >= DISKANN_COMPACTION_MAX_PENDING_VECTORS
		{
			self.has_more = true;
		}
	}

	fn add_pending(&mut self, pending: PendingOperation) {
		if let Some(&pos) = self.pending_by_id.get(&pending.id) {
			let existing = &mut self.pending[pos];
			// A record can briefly carry both a legacy `!dr` and a sharded `!dw` pending entry
			// during the migration. `add_pending` only sees this pair from the phase-1 fold,
			// where `existing` is the legacy `!dr` and `pending` is its sharded `!dw`
			// counterpart — and the `!dw` write is always the *earlier* of the two (new writes
			// fold legacy away, so a `!dr` only reappears when an older node writes after a
			// newer one). `pending` is therefore the chain head. Coalesce by the old->new vector
			// chain, checking `pending_precedes` first so an exact inverse pair (X->Y and Y->X,
			// e.g. an A->B->A revert, where *both* predicates hold) resolves to the true head's
			// `old_vectors` rather than the intermediate value — picking the intermediate would
			// leave the reverted-away vector behind as a phantom.
			let existing_precedes = existing.new_vectors == pending.old_vectors;
			let pending_precedes = pending.new_vectors == existing.old_vectors;
			if pending_precedes {
				existing.old_vectors = pending.old_vectors;
			} else if existing_precedes {
				existing.new_vectors = pending.new_vectors;
			} else {
				// Genuine dual-layer entries always chain (each post-fold write records
				// `old_vectors` = the value the previous write produced). Reaching here means the
				// chain invariant is broken — fail loudly in dev/CI so the regression is caught,
				// and fall back to the later-scanned update in release rather than silently
				// mis-coalescing a phantom vector into the graph.
				debug_assert!(
					existing_precedes || pending_precedes,
					"DiskANN pending coalesce: non-chaining entries for {:?} (existing {:?} -> {:?}, incoming {:?} -> {:?})",
					existing.id,
					existing.old_vectors,
					existing.new_vectors,
					pending.old_vectors,
					pending.new_vectors,
				);
				existing.new_vectors = pending.new_vectors;
			}
			return;
		}
		let pos = self.pending.len();
		self.pending_by_id.insert(pending.id.clone(), pos);
		self.pending.push(pending);
	}

	fn into_plan(self, cleared_shards: Vec<u16>) -> DiskAnnCompactionPlan {
		DiskAnnCompactionPlan {
			generation: self.generation,
			pending_state: self.pending_state,
			captured_keys: self.captured_keys,
			pending: self.pending,
			cleared_shards,
			has_more: self.has_more,
		}
	}
}

/// One DiskANN index instance cached inside [`IndexStores`](crate::trees::store::IndexStores).
pub struct DiskAnnIndex {
	/// Expected vector dimensionality.
	dim: usize,
	/// Public SurrealDB distance semantics for pending-vector scoring and result materialization.
	distance: Distance,
	/// Shared key builder for this index.
	ikb: IndexKeyBase,
	/// Stable table id used for process-local cache scoping.
	table_id: TableId,
	/// Configured vector representation accepted by this index.
	vector_type: VectorType,
	/// Process-local DiskANN cache shared by graph/provider/document helpers.
	cache: DiskAnnCache,
	/// In-process DiskANN graph wrapper; writes take the lock during compaction.
	graph: RwLock<DiskAnnGraph>,
	/// Vector-to-document mapping helper for `!dq`/`!dh` resolution.
	vec_docs: DiskAnnVecDocs,
	/// Whether the index uses shared or legacy private doc-ID mappings.
	format_version: u16,
	/// Set from the moment a KNN scan reads more of this index's pending queue
	/// than one materialisation batch holds, by entry count or by a scoring
	/// rollover. Re-armed by a scan that runs to completion inside both budgets
	/// only if no newer scan observed another crossing, so the warning is
	/// emitted once per crossing rather than once per query.
	pending_backlog_reported: PendingBacklogReport,
	/// What the KNN pending scans on this index materialised. Zero-sized outside
	/// test builds.
	pending_scan_stats: PendingScanStats,
	/// Faults the next compaction frames on this index inject.
	#[cfg(test)]
	faults: std::sync::Mutex<Option<Arc<FrameFaults>>>,
	/// Every compaction frame this index opened, for the tests that read their stats.
	#[cfg(test)]
	frames: std::sync::Mutex<Vec<Arc<DiskAnnBuildFrame>>>,
}

/// Context passed from SurrealDB execution into DiskANN provider calls.
pub(super) struct DiskAnnContext<'a> {
	/// The environment of the query driving this operation, consulted for
	/// cancellation and doc-ID resolution.
	pub(super) env: &'a dyn IndexEnv,
	/// Transaction used by the current graph/search/compaction operation.
	pub(super) tx: Arc<Transaction>,
	/// Index key builder copied into provider-facing calls.
	pub(super) ikb: IndexKeyBase,
	/// DiskANN provider context containing transaction/key state.
	pub(super) provider_context: DiskAnnProviderContext,
}

impl<'a> DiskAnnContext<'a> {
	fn new(
		env: &'a dyn IndexEnv,
		ikb: IndexKeyBase,
		provider_context: DiskAnnProviderContext,
	) -> Self {
		let tx = env.tx();
		Self {
			env,
			tx,
			ikb,
			provider_context,
		}
	}

	/// How this operation reaches KV: through its transaction, checked against the frame's
	/// cancellation before every operation when it runs in one.
	pub(super) fn kv(&self) -> FrameKv<'_> {
		FrameKv::new(&self.tx, self.provider_context.frame())
	}

	/// The compaction frame this operation runs in, if any.
	pub(super) fn frame(&self) -> Option<&DiskAnnBuildFrame> {
		self.provider_context.frame()
	}
}

/// Decides which graph elements an inline-filtered search
/// ([`DiskAnnGraph::search_filtered`]) admits as results.
pub(super) trait ElementClassifier {
	/// Returns one verdict per candidate, in input order: `true` admits the
	/// element as a result. Each candidate carries its raw DiskANN distance.
	/// The search navigates through every candidate whatever its verdict.
	async fn classify(&mut self, candidates: &[DiskAnnSearchResult]) -> Result<Vec<bool>>;
}

/// Output of one inline-filtered walk ([`DiskAnnGraph::search_filtered`]).
struct FilteredWalk {
	/// Admitted elements, distance-ascending, with raw DiskANN distances.
	results: Vec<DiskAnnSearchResult>,
	/// Whether the walk reached every element reachable from the start points,
	/// so that a wider search list cannot surface anything new.
	exhausted: bool,
}

/// The visited set of an inline-filtered search, as the predicate
/// [`SearchAccessor::expand_beam`] admits neighbours through: an element is
/// admitted once, the first time it is offered.
struct NotVisited<'a>(&'a mut HashSet<ElementId>);

impl Predicate<ElementId> for NotVisited<'_> {
	fn eval(&self, item: &ElementId) -> bool {
		!self.0.contains(item)
	}
}

impl PredicateMut<ElementId> for NotVisited<'_> {
	fn eval_mut(&mut self, item: &ElementId) -> bool {
		self.0.insert(*item)
	}
}

impl HybridPredicate<ElementId> for NotVisited<'_> {}

/// Runs `$body` with `$values` bound to `$vector`'s elements as a typed slice, or fails for a
/// vector type or storage DiskANN cannot insert.
macro_rules! with_typed_slice {
	($vector:expr, $values:ident => $body:expr) => {
		match $vector {
			Vector::F32(v) => match v.as_slice() {
				Some($values) => $body,
				None => bail!("DISKANN vector storage must be contiguous"),
			},
			Vector::F16(v) => match v.as_slice() {
				Some($values) => $body,
				None => bail!("DISKANN vector storage must be contiguous"),
			},
			Vector::I8(v) => match v.as_slice() {
				Some($values) => $body,
				None => bail!("DISKANN vector storage must be contiguous"),
			},
			Vector::U8(v) => match v.as_slice() {
				Some($values) => $body,
				None => bail!("DISKANN vector storage must be contiguous"),
			},
			_ => bail!("DISKANN supports TYPE F32, F16, I8, and U8"),
		}
	};
}

/// An element type whose vectors a batched insert reads rows from.
trait TypedVector: Sized {
	/// `vector`'s elements, when it holds this type in contiguous storage.
	fn slice_of(vector: &Vector) -> Option<&[Self]>;
}

impl TypedVector for f32 {
	fn slice_of(vector: &Vector) -> Option<&[Self]> {
		match vector {
			Vector::F32(v) => v.as_slice(),
			_ => None,
		}
	}
}

impl TypedVector for Half {
	fn slice_of(vector: &Vector) -> Option<&[Self]> {
		match vector {
			Vector::F16(v) => v.as_slice(),
			_ => None,
		}
	}
}

impl TypedVector for i8 {
	fn slice_of(vector: &Vector) -> Option<&[Self]> {
		match vector {
			Vector::I8(v) => v.as_slice(),
			_ => None,
		}
	}
}

impl TypedVector for u8 {
	fn slice_of(vector: &Vector) -> Option<&[Self]> {
		match vector {
			Vector::U8(v) => v.as_slice(),
			_ => None,
		}
	}
}

/// Thin wrapper around the upstream DiskANN graph using SurrealDB's provider implementation.
///
/// Its methods take `&self`: exclusive access to the graph comes from the `RwLock` around it, not
/// from `&mut`.
pub(super) struct DiskAnnGraph {
	index: RawDiskAnnIndex<DiskAnnProvider>,
	/// The span over which this wrapper's cached graph has followed every commit to the index.
	/// Only a compaction frame changes it, under the graph write lock.
	continuity: Option<Continuity>,
	/// What the upstream configuration was built from, so a frame can build a handle of its own
	/// sized to its place grant ([`Self::frame_index`]).
	degree: usize,
	l_build: usize,
	alpha: f32,
	metric: Metric,
	/// Whether inserts run with the construction retry ([`insert_retry`]); only a test turns it
	/// off.
	insert_retry: bool,
	/// How many times a frame opened the span afresh, for the tests that pin when a frame
	/// invalidates what the wrapper cached.
	#[cfg(test)]
	span_opens: u64,
}

/// The upstream configuration of one DiskANN graph: its degree, build list, alpha and prune
/// kind, the construction retry when `retry` is set, and — for a frame's batched insert —
/// `tasks` as its minibatch parallelism, with its intra-batch candidates.
fn graph_config(
	degree: usize,
	l_build: usize,
	alpha: f32,
	metric: Metric,
	retry: bool,
	tasks: Option<NonZeroUsize>,
) -> Result<diskann::graph::Config> {
	let mut builder =
		Builder::new(degree, MaxDegree::default_slack(), l_build, PruneKind::from_metric(metric));
	builder.alpha(alpha);
	if retry {
		builder.insert_retry(insert_retry(degree));
	}
	if let Some(tasks) = tasks {
		builder.max_minibatch_par(tasks.get());
		builder.intra_batch_candidates(IntraBatchCandidates::new(DISKANN_INTRA_BATCH_CANDIDATES));
	}
	Ok(builder.build()?)
}

/// Raw graph-search output before document filtering.
///
/// The distance is the value returned by the DiskANN distance computer for the index metric. It is
/// converted to SurrealDB's public distance semantics before it reaches the KNN result builder.
type DiskAnnSearchResult = (ElementId, f64);

impl DiskAnnGraph {
	/// Builds the upstream DiskANN graph configuration and provider for one SurrealDB index.
	fn new(
		ikb: IndexKeyBase,
		tb: TableId,
		p: &DiskAnnParams,
		cache: DiskAnnCache,
		format_version: u16,
	) -> Result<Self> {
		let metric = distance_to_metric(&p.distance)?;
		let alpha = p.alpha.to_float() as f32;
		if !alpha.is_finite() || alpha <= 0.0 {
			bail!("DISKANN ALPHA must be finite and greater than 0")
		}
		let config =
			graph_config(p.degree as usize, p.l_build as usize, alpha, metric, true, None)?;
		let provider =
			DiskAnnProvider::new(ikb, tb, format_version, cache, p.dimension as usize, metric);
		Ok(Self {
			index: RawDiskAnnIndex::new(config, provider, None),
			continuity: None,
			degree: p.degree as usize,
			l_build: p.l_build as usize,
			alpha,
			metric,
			insert_retry: true,
			#[cfg(test)]
			span_opens: 0,
		})
	}

	/// Rebuilds the upstream index without the construction retry, for the tests that measure
	/// what it changes.
	#[cfg(test)]
	fn without_insert_retry(&mut self) -> Result<()> {
		self.insert_retry = false;
		let config = graph_config(self.degree, self.l_build, self.alpha, self.metric, false, None)?;
		self.index = RawDiskAnnIndex::new(config, self.provider().same_owner(), None);
		Ok(())
	}

	/// The provider the graph reads and writes through.
	pub(super) fn provider(&self) -> &DiskAnnProvider {
		self.index.provider()
	}

	/// The upstream index one batched insert runs on, with `tasks` as both its minibatch
	/// parallelism and its scratch-pool size.
	///
	/// `tasks` is the frame's place grant: upstream spawns at most `tasks` tasks in any phase of a
	/// batched insert, so every task it can spawn has a place. The handle wraps a provider that
	/// keeps this wrapper's cache owner, so what it writes through is attributed exactly as the
	/// long-lived index's writes are. It holds no graph state, and is dropped when the insert
	/// returns; exclusive access to the graph still comes from the lock around this type.
	fn frame_index(&self, tasks: NonZeroUsize) -> Result<Arc<RawDiskAnnIndex<DiskAnnProvider>>> {
		let config = graph_config(
			self.degree,
			self.l_build,
			self.alpha,
			self.metric,
			self.insert_retry,
			Some(tasks),
		)?;
		Ok(Arc::new(RawDiskAnnIndex::new(config, self.provider().same_owner(), Some(tasks))))
	}

	/// Reads the stored elements (vector and tombstone) of `element_ids`, in input order, through
	/// `context`'s view of the cache. A missing element is `None`.
	pub(super) async fn read_elements(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Arc<DiskAnnElement>>>> {
		self.index.provider().read_elements(context, element_ids).await
	}

	/// The provider context for a transaction that reads the graph, whose snapshot holds `!dg` at
	/// `generation`. Taken, and used, under the graph lock.
	pub(super) fn context(
		&self,
		tx: Arc<Transaction>,
		generation: Option<u64>,
	) -> DiskAnnProviderContext {
		self.index.provider().context(tx, generation, self.continuity)
	}

	/// Opens a compaction frame whose snapshot holds `!dg` at `generation`, under the graph write
	/// lock. Returns `None` when this wrapper has already committed past that snapshot, so the
	/// frame's conditional bump of `!dg` cannot commit.
	///
	/// A snapshot anywhere but the span's head reopens the span there: either the wrapper has not
	/// committed yet, or another process has committed since, and the wrapper cannot tell which
	/// entries a commit it did not make changed.
	pub(super) fn open_frame(
		&mut self,
		tx: Arc<Transaction>,
		generation: Option<u64>,
	) -> Option<DiskAnnProviderContext> {
		match self.continuity {
			Some(span) if generation < span.head => return None,
			Some(span) if generation == span.head => {}
			_ => {
				#[cfg(test)]
				{
					self.span_opens += 1;
				}
				self.continuity = Some(Continuity {
					base: generation,
					head: generation,
				})
			}
		}
		Some(self.index.provider().frame_context(tx, generation, self.continuity))
	}

	/// Extends the span over `committed`, the generation a frame has just committed.
	fn commit_frame(&mut self, committed: Option<u64>) {
		if let Some(span) = self.continuity.as_mut() {
			span.head = committed;
		}
	}

	/// Closes the span after a frame failed to commit. The caller clears the index's entries
	/// before it releases the graph write lock.
	fn abandon_frame(&mut self) {
		self.continuity = None;
	}

	/// The view a transaction reading the graph under the lock now would read this wrapper's
	/// entries through, at `generation`.
	fn reader_view(&self, generation: Option<u64>) -> CacheView {
		self.index.provider().reader_view(generation, self.continuity)
	}

	/// Inserts one vector into the graph and returns its new element ID.
	pub(super) async fn insert(
		&self,
		ctx: &DiskAnnContext<'_>,
		vector: &Vector,
	) -> Result<ElementId> {
		let element_id = self.provider().allocate_element_id(&ctx.provider_context).await?;
		with_typed_slice!(vector, values => self.insert_typed(ctx, values, element_id).await)?;
		Ok(element_id)
	}

	/// Inserts one vector into the graph under `element_id`, which the caller allocated in this
	/// transaction and the graph does not hold, through upstream's single-vector insert. Spawns
	/// nothing.
	pub(super) async fn insert_with_id(
		&self,
		ctx: &DiskAnnContext<'_>,
		vector: &Vector,
		element_id: ElementId,
	) -> Result<()> {
		with_typed_slice!(vector, values => self.insert_typed(ctx, values, element_id).await)
	}

	/// Inserts a typed vector slice under `element_id`: as the graph's first element when it has no
	/// entry point, through the upstream insertion strategy otherwise.
	async fn insert_typed<T>(
		&self,
		ctx: &DiskAnnContext<'_>,
		values: &[T],
		element_id: ElementId,
	) -> Result<()>
	where
		T: DiskAnnVectorElement,
		for<'a> DiskAnnProvider: SetElement<&'a [T], SetError = diskann::ANNError>,
	{
		let provider = self.provider();
		if provider.valid_starting_points(&ctx.provider_context).await?.is_empty() {
			self.seed_typed(ctx, values, element_id).await
		} else {
			let strategy = DiskAnnStrategy::<T>::default();
			self.index.insert(&strategy, &ctx.provider_context, &element_id, values).await?;
			provider.ensure_entry_point(&ctx.provider_context, element_id).await
		}
	}

	/// Seeds an empty graph with `vector` as its first element and entry point, and returns the
	/// element's id.
	pub(super) async fn seed_first(
		&self,
		ctx: &DiskAnnContext<'_>,
		vector: &Vector,
	) -> Result<ElementId> {
		let element_id = self.provider().allocate_element_id(&ctx.provider_context).await?;
		with_typed_slice!(vector, values => self.seed_typed(ctx, values, element_id).await)?;
		Ok(element_id)
	}

	/// Writes `element_id`'s vector, an empty adjacency list, and the entry point naming it.
	async fn seed_typed<T>(
		&self,
		ctx: &DiskAnnContext<'_>,
		values: &[T],
		element_id: ElementId,
	) -> Result<()>
	where
		T: DiskAnnVectorElement,
		for<'a> DiskAnnProvider: SetElement<&'a [T], SetError = diskann::ANNError>,
	{
		let provider = self.provider();
		let guard = provider.set_element(&ctx.provider_context, &element_id, values).await?;
		guard.complete().await;
		provider
			.write_node(&ctx.provider_context, element_id, Default::default(), NodeWrite::Own)
			.await?;
		provider.set_entry_point(&ctx.provider_context, Some(element_id)).await
	}

	/// Inserts `vectors` under `ids` through upstream's batched insert, on at most `tasks` tasks
	/// per phase.
	///
	/// `ids` come from `allocate_element_ids` in this transaction, one per vector in order, and the
	/// graph holds none of them; the graph already has a live entry point. `tasks` is the frame's
	/// place grant, so every task upstream spawns is backed by a place.
	pub(super) async fn insert_batch(
		&self,
		ctx: &DiskAnnContext<'_>,
		vectors: &[&Vector],
		ids: &[ElementId],
		tasks: NonZeroUsize,
	) -> Result<()> {
		match vectors.first() {
			Some(Vector::F32(_)) => self.insert_batch_typed::<f32>(ctx, vectors, ids, tasks).await,
			Some(Vector::F16(_)) => self.insert_batch_typed::<Half>(ctx, vectors, ids, tasks).await,
			Some(Vector::I8(_)) => self.insert_batch_typed::<i8>(ctx, vectors, ids, tasks).await,
			Some(Vector::U8(_)) => self.insert_batch_typed::<u8>(ctx, vectors, ids, tasks).await,
			Some(_) => bail!("DISKANN supports TYPE F32, F16, I8, and U8"),
			None => Ok(()),
		}
	}

	/// Typed body of [`Self::insert_batch`]: packs the batch into one row-major matrix.
	async fn insert_batch_typed<T>(
		&self,
		ctx: &DiskAnnContext<'_>,
		vectors: &[&Vector],
		ids: &[ElementId],
		tasks: NonZeroUsize,
	) -> Result<()>
	where
		T: DiskAnnVectorElement + TypedVector,
		for<'a> DiskAnnProvider: SetElement<&'a [T], SetError = diskann::ANNError>,
	{
		let dim = self.provider().dim();
		let mut data: Vec<T> = Vec::with_capacity(vectors.len() * dim);
		for vector in vectors {
			let Some(row) = T::slice_of(vector) else {
				bail!("DISKANN vector storage must be contiguous")
			};
			Vector::check_expected_dimension(row.len(), dim)?;
			data.extend_from_slice(row);
		}
		let batch: Matrix<T> = Matrix::try_from(data.into_boxed_slice(), vectors.len(), dim)
			.map_err(|_| anyhow::anyhow!("DISKANN batch dimension mismatch"))?;
		self.frame_index(tasks)?
			.multi_insert::<DiskAnnStrategy<T>, Matrix<T>>(
				DiskAnnStrategy::<T>::default(),
				&ctx.provider_context,
				Arc::new(batch),
				Arc::<[ElementId]>::from(ids),
			)
			.await?;
		Ok(())
	}

	/// Re-prunes the adjacency lists of the live elements among `ids` that have grown past the
	/// degree, through upstream `prune_range`.
	///
	/// The chunk's adjacency lists are read in one batch first, so only the lists past the degree
	/// go further: their elements are read in one more batch, which also leaves upstream's
	/// one-at-a-time reads of both served from the frame's overlay or the cache.
	pub(super) async fn reprune(
		&self,
		ctx: &DiskAnnContext<'_>,
		ids: std::ops::Range<ElementId>,
	) -> Result<()> {
		let context = &ctx.provider_context;
		let ids: Vec<ElementId> = ids.collect();
		let nodes = self.provider().read_nodes(context, &ids).await?;
		let over: Vec<ElementId> = ids
			.into_iter()
			.zip(nodes)
			.filter(|(_, node)| {
				node.as_ref().is_some_and(|node| node.neighbors.len() > self.degree)
			})
			.map(|(id, _)| id)
			.collect();
		if over.is_empty() {
			return Ok(());
		}
		let elements = self.read_elements(context, &over).await?;
		let live: Vec<(ElementId, Arc<DiskAnnElement>)> = over
			.into_iter()
			.zip(elements)
			.filter_map(|(id, element)| Some((id, element.filter(|e| !e.deleted)?)))
			.collect();
		let Some((_, first)) = live.first() else {
			return Ok(());
		};
		let ids: Vec<ElementId> = live.iter().map(|(id, _)| *id).collect();
		match &first.vector {
			SerializedVector::F32(_) => {
				self.index.prune_range(&DiskAnnStrategy::<f32>::default(), context, ids).await?
			}
			SerializedVector::F16(_) => {
				self.index.prune_range(&DiskAnnStrategy::<Half>::default(), context, ids).await?
			}
			SerializedVector::I8(_) => {
				self.index.prune_range(&DiskAnnStrategy::<i8>::default(), context, ids).await?
			}
			SerializedVector::U8(_) => {
				self.index.prune_range(&DiskAnnStrategy::<u8>::default(), context, ids).await?
			}
			_ => bail!("DISKANN supports TYPE F32, F16, I8, and U8"),
		}
		Ok(())
	}

	/// Marks one graph element deleted and refreshes the entry point if needed.
	pub(super) async fn remove(
		&self,
		ctx: &DiskAnnContext<'_>,
		element_id: ElementId,
	) -> Result<()> {
		let provider = self.index.provider();
		provider.delete(&ctx.provider_context, &element_id).await?;
		// Reads what it persists through the transaction: a cached entry point, validated against
		// a cached element, would overwrite a live entry point another process committed with one
		// it has already deleted.
		provider.retire_start_point(&ctx.provider_context, element_id).await
	}

	/// Dispatches a typed graph search based on the prepared query vector representation.
	async fn search(
		&self,
		ctx: &DiskAnnContext<'_>,
		query: &DiskAnnQuery,
		k: usize,
		l: usize,
	) -> Result<Vec<DiskAnnSearchResult>> {
		match query {
			DiskAnnQuery::F32(query) => self.search_typed(ctx, query, k, l).await,
			DiskAnnQuery::F16(query) => self.search_typed(ctx, query, k, l).await,
			DiskAnnQuery::I8(query) => self.search_typed(ctx, query, k, l).await,
			DiskAnnQuery::U8(query) => self.search_typed(ctx, query, k, l).await,
		}
	}

	/// Runs the upstream DiskANN search and preserves graph element IDs with their raw distances.
	async fn search_typed<T>(
		&self,
		ctx: &DiskAnnContext<'_>,
		query: &[T],
		k: usize,
		l: usize,
	) -> Result<Vec<DiskAnnSearchResult>>
	where
		T: DiskAnnVectorElement,
	{
		if self.index.provider().valid_starting_points(&ctx.provider_context).await?.is_empty() {
			return Ok(Vec::new());
		}
		let limit = l.max(k).max(1);
		let params = Knn::new_default(limit)?;
		let mut ids = vec![0; limit];
		let mut distances = vec![0.0; limit];
		let mut output = IdDistance::new(&mut ids, &mut distances);
		let strategy = DiskAnnStrategy::<T>::default();
		let stats =
			self.index.search(params, &strategy, &ctx.provider_context, query, &mut output).await?;
		let result_count = stats.result_count as usize;
		Ok(ids
			.into_iter()
			.zip(distances)
			.take(result_count)
			.map(|(id, distance)| (id, distance as f64))
			.collect())
	}

	/// Inline-filtered graph search: the greedy walk of [`Self::search`], with
	/// every element it computes a distance for — start points and expanded
	/// neighbours alike — offered to `classifier`.
	///
	/// Navigation is unfiltered: the search list holds the `l` nearest elements
	/// visited, admitted or not, so the walk crosses regions the filter rejects
	/// and terminates exactly as the unfiltered search does, once every element
	/// in that list has been expanded. The results are every admitted element it
	/// reached, distance-ascending, so a selective filter finds admitted elements
	/// that rank far beyond the `l` nearest overall. The walk's length is set by
	/// `l`, not by how many elements it admits: a pass can admit fewer than `k`
	/// where a query sits in a dense region the filter rejects, and only a wider
	/// list walks further. The caller widens `l` and walks again for that case
	/// (see [`DiskAnnIndex::search_graph`]); [`FilteredWalk::exhausted`] says when
	/// widening cannot help. The classifier runs in this future, not in the
	/// upstream search's `Send` futures.
	async fn search_filtered<C: ElementClassifier>(
		&self,
		ctx: &DiskAnnContext<'_>,
		query: &DiskAnnQuery,
		k: usize,
		l: usize,
		classifier: &mut C,
	) -> Result<FilteredWalk> {
		match query {
			DiskAnnQuery::F32(query) => {
				self.search_filtered_typed(ctx, query, k, l, classifier).await
			}
			DiskAnnQuery::F16(query) => {
				self.search_filtered_typed(ctx, query, k, l, classifier).await
			}
			DiskAnnQuery::I8(query) => {
				self.search_filtered_typed(ctx, query, k, l, classifier).await
			}
			DiskAnnQuery::U8(query) => {
				self.search_filtered_typed(ctx, query, k, l, classifier).await
			}
		}
	}

	/// Typed body of [`Self::search_filtered`].
	async fn search_filtered_typed<T, C>(
		&self,
		ctx: &DiskAnnContext<'_>,
		query: &[T],
		k: usize,
		l: usize,
		classifier: &mut C,
	) -> Result<FilteredWalk>
	where
		T: DiskAnnVectorElement,
		C: ElementClassifier,
	{
		let provider = self.index.provider();
		if provider.valid_starting_points(&ctx.provider_context).await?.is_empty() {
			return Ok(FilteredWalk {
				results: Vec::new(),
				exhausted: true,
			});
		}
		let limit = l.max(k).max(1);
		let beam_width = Knn::new_default(limit)?.beam_width().get();
		let mut accessor = DiskAnnSearchAccessor::<T>::new(provider, &ctx.provider_context, query)?;
		let mut best = NeighborPriorityQueue::new(limit);
		let mut visited: HashSet<ElementId> = HashSet::new();
		let mut matched: Vec<DiskAnnSearchResult> = Vec::new();
		let mut batch: Vec<(ElementId, f32)> = Vec::new();
		let mut beam: Vec<ElementId> = Vec::with_capacity(beam_width);
		let mut hops = 0usize;
		accessor.start_point_distances(|id, distance| batch.push((id, distance))).await?;
		for &(id, _) in &batch {
			visited.insert(id);
		}
		loop {
			// Every visited element navigates; the admitted ones are results.
			let candidates: Vec<DiskAnnSearchResult> =
				batch.iter().map(|&(id, distance)| (id, f64::from(distance))).collect();
			for &(id, distance) in &batch {
				best.insert(Neighbor::new(id, distance));
			}
			if !candidates.is_empty() {
				let verdicts = classifier.classify(&candidates).await?;
				matched.extend(
					candidates.into_iter().zip(verdicts).filter(|(_, ok)| *ok).map(|(c, _)| c),
				);
			}
			hops += 1;
			expect_not_done(ctx.env, Some(hops)).await?;
			beam.clear();
			while beam.len() < beam_width {
				let Some(closest) = best.closest_notvisited() else {
					break;
				};
				beam.push(*closest.id());
			}
			if beam.is_empty() {
				break;
			}
			batch.clear();
			accessor
				.expand_beam(beam.iter().copied(), NotVisited(&mut visited), |id, distance| {
					batch.push((id, distance))
				})
				.await?;
		}
		matched.sort_by(|a, b| a.1.total_cmp(&b.1));
		Ok(FilteredWalk {
			results: matched,
			// A walk that visited fewer elements than its list holds never filled
			// the list, so it reached every element it could: a wider list walks
			// no further.
			exhausted: visited.len() < limit,
		})
	}
}

/// Cancels `tx` and discards any error from a tx that is already closed.
///
/// Used by [`DiskAnnIndex::apply_compaction`] on every stale-plan / apply-error path so the
/// transaction-lifecycle policy lives in one place rather than scattered across five call
/// sites.
async fn cancel_silently(tx: &Transaction) {
	let _ = tx.cancel().await;
}

/// How a frame's batch ended: on its own, or stopped by the watchdog, with what the watchdog saw.
enum FrameOutcome {
	Done(Result<()>),
	Stopped(Result<()>),
}

/// Resolves once the statement running a frame is done: `Ok(())` when it was cancelled or timed
/// out, and the error `is_done` raised otherwise, such as the memory threshold, unchanged.
async fn frame_watchdog(env: &dyn IndexEnv, frame: &DiskAnnBuildFrame) -> Result<()> {
	let watch = async {
		loop {
			if env.is_done(None).await? {
				return Ok(());
			}
			common::time::sleep(DISKANN_FRAME_WATCHDOG_INTERVAL).await;
		}
	};
	#[cfg(test)]
	if let Some(faults) = &frame.faults
		&& let (Some(park), Some(parked)) =
			(&faults.park_set_element, faults.panic_frame_when_parked)
	{
		tokio::select! {
			_ = park.wait_for(parked) => panic!("injected panic in the frame task"),
			r = watch => return r,
		}
	}
	#[cfg(not(test))]
	let _ = frame;
	watch.await
}

/// Undoes a frame that will not commit: the lock's cleanup and release, then the write
/// transaction's cancel, inline.
async fn fail(
	lock: FrameLock<'_>,
	tx: &Transaction,
	tx_guard: TxGuard,
	e: anyhow::Error,
) -> Result<CompactionApply> {
	let frame = lock.frame.clone();
	lock.abort().await;
	if let Some(frame) = &frame {
		frame.stats.tx_ended(frame.live_tasks());
	}
	cancel_silently(tx).await;
	tx_guard.disarm();
	Err(e)
}

/// Cancels a frame's write transaction if the code that opened it does not end it: on an early
/// `?` or a panic.
///
/// Its cancel runs in a task of its own, since `Drop` cannot await, and only after every task of
/// the attached frame has exited, so no worker is still writing into the transaction, or reading
/// through it, when it ends. A transaction that has already ended is left alone.
struct TxGuard {
	/// The transaction, until [`Self::disarm`].
	tx: Option<Arc<Transaction>>,
	frame: Option<Arc<DiskAnnBuildFrame>>,
}

impl TxGuard {
	fn new(tx: Arc<Transaction>) -> Self {
		Self {
			tx: Some(tx),
			frame: None,
		}
	}

	/// Called once the transaction has ended inline.
	fn disarm(mut self) {
		self.tx = None;
	}
}

impl Drop for TxGuard {
	fn drop(&mut self) {
		let Some(tx) = self.tx.take() else {
			return;
		};
		let frame = self.frame.take();
		if let Some(frame) = &frame {
			frame.cancel();
		}
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		drop(runtime.spawn(async move {
			if let Some(frame) = &frame {
				frame.drain_tasks().await;
				frame.stats.tx_ended(frame.live_tasks());
			}
			if !tx.closed() {
				cancel_silently(&tx).await;
			}
		}));
	}
}

/// The graph write lock of one frame, and what the frame must undo if it ends without committing:
/// the span `open_frame` moved, the cache entries the frame wrote through, and the frame itself
/// with its places. It does not hold the write transaction, which stays with the code that opened
/// it.
///
/// [`Self::committed`], called once the write transaction has committed, extends the span and
/// closes the frame under the lock, then releases it. [`Self::abort`] first cancels the frame and
/// waits for every task it spawned — including one upstream detached — to exit, then undoes the
/// frame under the lock and releases it. Undoing, in both `abort` and `Drop`, is synchronous:
///  1. the frame is closed, so a task still running writes nothing through or publishes nothing
///     afterwards, and the frame's share of its places is dropped;
///  2. the continuity span is closed;
///  3. every cache entry of this index is cleared.
///
/// The frame's read view is closed — its read transaction cancelled — only after the lock is
/// released: inline in `committed` and `abort`, which have drained the frame's tasks first.
///
/// `Drop` without `committed` or `abort` happens only on a panic or an early `?` in the frame's
/// task, and undoes the frame the same way, with no `.await` between the drop and the lock's
/// release. It leaves the read transaction to a spawned task, which drains the frame's tasks
/// before it cancels it.
struct FrameLock<'a> {
	index: &'a DiskAnnIndex,
	/// Attached once the frame is open; `None` before.
	frame: Option<Arc<DiskAnnBuildFrame>>,
	done: bool,
	/// Declared last, so it is dropped after `Drop::drop` has run.
	graph: RwLockWriteGuard<'a, DiskAnnGraph>,
}

impl<'a> FrameLock<'a> {
	fn new(index: &'a DiskAnnIndex, graph: RwLockWriteGuard<'a, DiskAnnGraph>) -> Self {
		Self {
			index,
			frame: None,
			done: false,
			graph,
		}
	}

	/// Gives `frame` to this lock and to the write transaction's guard in one synchronous step,
	/// before anything can spawn a task, so no task runs while either guard does not know it.
	fn attach(&mut self, frame: Arc<DiskAnnBuildFrame>, tx_guard: &mut TxGuard) {
		tx_guard.frame = Some(Arc::clone(&frame));
		self.frame = Some(frame);
	}

	fn graph(&self) -> &DiskAnnGraph {
		&self.graph
	}

	/// Extends the span over `generation`, which the frame's transaction has just committed,
	/// closes the frame, releases the lock, then cancels the frame's read transaction.
	async fn committed(mut self, generation: Option<u64>) {
		self.graph.commit_frame(generation);
		let view = self.frame.as_ref().and_then(|frame| frame.close_now());
		self.done = true;
		let frame = self.frame.take();
		// Releases the graph lock: no read transaction is ended while it is held.
		drop(self);
		close_view(frame.as_deref(), view).await;
	}

	/// Cancels the frame, waits for every task it spawned to exit, undoes it, releases the lock,
	/// then cancels the frame's read transaction.
	async fn abort(mut self) {
		if let Some(frame) = &self.frame {
			frame.cancel();
			frame.drain_tasks().await;
		}
		let view = self.undo();
		self.done = true;
		let frame = self.frame.take();
		drop(self);
		close_view(frame.as_deref(), view).await;
	}

	/// The synchronous cleanup: closes the frame, closes the span and clears the index's cache
	/// entries. Returns the frame's read view, for the caller to close once the lock is released.
	fn undo(&mut self) -> Option<Arc<ReadView>> {
		let view = self.frame.as_ref().and_then(|frame| frame.close_now());
		self.graph.abandon_frame();
		self.index.clear_local_cache_now();
		view
	}
}

impl Drop for FrameLock<'_> {
	/// Undoes the frame before the lock is released, then leaves the read transaction to a task
	/// of its own, which cancels it once every task the frame spawned has exited.
	fn drop(&mut self) {
		if self.done {
			return;
		}
		let view = self.undo();
		let frame = self.frame.take();
		if view.is_none() {
			return;
		}
		let Ok(runtime) = tokio::runtime::Handle::try_current() else {
			return;
		};
		drop(runtime.spawn(async move {
			if let Some(frame) = &frame {
				frame.drain_tasks().await;
			}
			close_view(frame.as_deref(), view).await;
		}));
	}
}

/// Cancels a frame's read transaction, once no task of the frame reads through it any more.
async fn close_view(frame: Option<&DiskAnnBuildFrame>, view: Option<Arc<ReadView>>) {
	let Some(view) = view else {
		return;
	};
	if let Some(frame) = frame {
		frame.stats.tx_ended(frame.live_tasks());
	}
	view.close().await;
}

/// The most fresh vectors one initial-batch sink frame inserts: a scan batch is cut before the
/// record that would take it past this, unless that record is the batch's first.
pub const DISKANN_DIRECT_MAX_VECTORS: usize = DISKANN_COMPACTION_MAX_PENDING_VECTORS;

/// How many element ids one re-prune frame covers.
pub const DISKANN_REPRUNE_CHUNK: ElementId = 1024;

/// What one frame does to the graph.
enum FrameWork {
	/// Folds pending operations: removals, then fresh inserts.
	Apply(Vec<PendingOperation>),
	/// Re-prunes the adjacency lists of the live elements with these ids.
	Reprune(std::ops::Range<ElementId>),
}

/// A build transaction's guard while a DiskANN sink frame may run in it: cancels the transaction if
/// the code that opened it does not end it — on an early return or a panic — once every task of
/// the frame [`DiskAnnIndex::apply_direct`] attached has exited. A transaction that has already
/// ended is left alone.
pub struct BuildTxGuard(TxGuard);

impl BuildTxGuard {
	/// Guards `tx`, the build transaction a scan batch is indexed in.
	pub fn new(tx: Arc<Transaction>) -> Self {
		Self(TxGuard::new(tx))
	}

	/// Called once the transaction has ended inline.
	pub fn disarm(self) {
		self.0.disarm()
	}
}

/// A sink frame applied in a build transaction that has not committed yet: the graph write lock,
/// the frame, and what undoing it takes (see [`DiskAnnIndex::apply_direct`]).
pub struct DirectApplyGuard<'a> {
	lock: FrameLock<'a>,
	committed: Option<u64>,
}

impl DirectApplyGuard<'_> {
	/// Called once the build transaction has committed: extends the span over the generation it
	/// committed, closes the frame and releases the graph lock, then cancels the frame's read
	/// transaction.
	pub async fn finish(self) {
		self.lock.committed(self.committed).await
	}

	/// Called when the build transaction will not commit, before the caller cancels it: undoes
	/// the frame under the graph lock, releases it, then cancels the frame's read transaction.
	pub async fn abort(self) {
		self.lock.abort().await
	}
}

/// How many attempts an insert makes at an adjacency list of at least half its degree.
const DISKANN_INSERT_ATTEMPTS: u32 = 3;

/// The upstream construction retry every insert runs with: an insert whose pruned adjacency list
/// holds fewer than half the index's degree searches again with a doubled list, up to
/// [`DISKANN_INSERT_ATTEMPTS`] attempts, keeping the list of its last attempt. Both the
/// single-vector and the batched insert honour it.
///
/// No attempt saturates its list. Pruning legitimately leaves short lists where it occludes most
/// candidates — points along a line keep the nearest on either side — and saturating those adds
/// edges the prune had good reason to drop, which the post-build re-prune then strips again.
fn insert_retry(degree: usize) -> InsertRetry {
	let threshold = u32::try_from(degree / 2).unwrap_or(u32::MAX);
	InsertRetry::new(
		NonZeroU32::new(DISKANN_INSERT_ATTEMPTS).unwrap_or(NonZeroU32::MIN),
		NonZeroU32::new(threshold).unwrap_or(NonZeroU32::MIN),
		false,
	)
}

/// How many elements a graph seeded from a batch starts its searches from: its entry point and
/// the start points chosen beside it.
const DISKANN_START_POINTS: usize = 4;

/// How many of a seeding batch's vectors, in batch order, the start points are chosen among.
const DISKANN_START_POINT_SAMPLE: usize = 256;

/// The positions, among the first [`DISKANN_START_POINT_SAMPLE`] of `vectors`, of the graph's
/// start points: first the medoid — the vector with the least summed distance to the others —
/// then, greedily, the vector farthest from every one chosen so far, until
/// [`DISKANN_START_POINTS`] are chosen or the sample runs out. Ties go to the earlier position.
///
/// Distances are Euclidean over the vectors' components, normalised first for an angular
/// metric, which is all a seed's spread needs. The choice is a function of the batch's vectors
/// and their order alone, so it is the same whatever the frame's grant.
fn start_point_positions(vectors: &[&Vector], metric: Metric) -> Vec<usize> {
	let normalise =
		matches!(metric, Metric::Cosine | Metric::CosineNormalized | Metric::InnerProduct);
	let sample: Vec<Vec<f64>> = vectors
		.iter()
		.take(DISKANN_START_POINT_SAMPLE)
		.map(|v| {
			let mut c = components(v);
			if normalise {
				let norm = c.iter().map(|x| x * x).sum::<f64>().sqrt();
				if norm > 0.0 {
					c.iter_mut().for_each(|x| *x /= norm);
				}
			}
			c
		})
		.collect();
	if sample.is_empty() {
		return Vec::new();
	}
	let distance = |a: &[f64], b: &[f64]| -> f64 {
		a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum::<f64>().sqrt()
	};
	let medoid = (0..sample.len())
		.map(|i| (i, sample.iter().map(|other| distance(&sample[i], other)).sum::<f64>()))
		.fold((0, f64::INFINITY), |best, (i, total)| {
			if total < best.1 {
				(i, total)
			} else {
				best
			}
		})
		.0;
	let mut chosen = vec![medoid];
	let mut nearest: Vec<f64> = sample.iter().map(|v| distance(v, &sample[medoid])).collect();
	while chosen.len() < DISKANN_START_POINTS {
		let (far, gap) = nearest.iter().copied().enumerate().fold((0, 0.0), |best, (i, d)| {
			if d > best.1 {
				(i, d)
			} else {
				best
			}
		});
		if gap <= 0.0 {
			break;
		}
		chosen.push(far);
		for (i, v) in sample.iter().enumerate() {
			nearest[i] = nearest[i].min(distance(v, &sample[far]));
		}
	}
	chosen
}

/// A vector's components as `f64`.
fn components(vector: &Vector) -> Vec<f64> {
	match vector {
		Vector::F32(v) => v.iter().map(|x| f64::from(*x)).collect(),
		Vector::F16(v) => v.iter().map(|x| f64::from(x.to_f32())).collect(),
		Vector::I8(v) => v.iter().map(|x| f64::from(*x)).collect(),
		Vector::U8(v) => v.iter().map(|x| f64::from(*x)).collect(),
		Vector::F64(v) => v.iter().copied().collect(),
		Vector::I64(v) => v.iter().map(|x| *x as f64).collect(),
		Vector::I32(v) => v.iter().map(|x| f64::from(*x)).collect(),
		Vector::I16(v) => v.iter().map(|x| f64::from(*x)).collect(),
	}
}

/// Converts SurrealDB's distance enum to the metric supported by the DiskANN crate.
fn distance_to_metric(distance: &Distance) -> Result<Metric> {
	match distance {
		Distance::Euclidean => Ok(Metric::L2),
		Distance::Cosine => Ok(Metric::Cosine),
		Distance::InnerProduct => Ok(Metric::InnerProduct),
		Distance::CosineNormalized => Ok(Metric::CosineNormalized),
		_ => bail!(
			"DISKANN supports EUCLIDEAN, COSINE, INNER_PRODUCT, and COSINE_NORMALIZED distances"
		),
	}
}

enum DiskAnnQuery {
	/// F32 query vector.
	F32(Vec<f32>),
	/// F16 query vector.
	F16(Vec<Half>),
	/// I8 query vector.
	I8(Vec<i8>),
	/// U8 query vector.
	U8(Vec<u8>),
}

/// Prepared typed query used by one DiskANN lookup.
struct DiskAnnSearch {
	/// Query vector in the shared SurrealDB representation, used for exact pending scoring. Shared
	/// so its cosine norm is computed once per search.
	pt: SharedVector,
	/// Query vector converted to the type expected by the upstream DiskANN graph.
	query: DiskAnnQuery,
	/// Result limit.
	k: usize,
	/// DiskANN search list size.
	l: usize,
}

impl DiskAnnSearch {
	fn new(pt: Vector, k: usize, l: usize) -> Result<Self> {
		let query = match &pt {
			Vector::F32(values) => DiskAnnQuery::F32(values.to_vec()),
			Vector::F16(values) => DiskAnnQuery::F16(values.to_vec()),
			Vector::I8(values) => DiskAnnQuery::I8(values.to_vec()),
			Vector::U8(values) => DiskAnnQuery::U8(values.to_vec()),
			_ => bail!("DISKANN supports TYPE F32, F16, I8, and U8"),
		};
		Ok(Self {
			query,
			pt: pt.into(),
			k,
			l,
		})
	}
}

/// Mutable search state threaded through graph result filtering.
struct DiskAnnGraphSearch<'a, 'b> {
	/// Read-locked graph used for the ANN search.
	graph: &'a DiskAnnGraph,
	/// Prepared typed query and limits.
	search: &'a DiskAnnSearch,
	/// Document IDs with pending updates that should suppress compacted graph results.
	pending_docs: Option<RoaringTreemap>,
	/// Doc-IDs admitted by the index-covered WHERE conjuncts (#548). When set,
	/// candidates outside the bitmap are rejected without any record fetch.
	allow_list: Option<&'a RoaringTreemap>,
	/// Optional condition filter applied before admitting candidates to the result builder.
	filter: &'a mut Option<DiskAnnTruthyDocumentFilter<'b>>,
	/// Shared result builder combining pending and graph candidates.
	builder: &'a mut KnnResultBuilder,
}

/// Classifies the elements an inline-filtered graph search visits by the
/// documents they hold: an element is admitted when one of its documents passes
/// every admission gate the result builder would apply — no pending update
/// supersedes it, the allow-list holds it, and the truthy filter (the table's
/// SELECT permission, then any residual condition) accepts it.
///
/// Elements that cannot place in the top k are rejected without evaluating
/// the filter: those farther than the k nearest documents admitted so far, or
/// than the result builder already holds from the pending scan. The bound
/// counts documents, each at its nearest admitted element, as the result does,
/// so a document reached through several elements holds one place in it.
struct GraphCandidateClassifier<'s, 'a, 'b> {
	/// Index the search runs on, for document lookup and distance conversion.
	index: &'s DiskAnnIndex,
	/// Context of the search's transaction.
	ctx: &'s DiskAnnContext<'a>,
	/// The truthy filter, whose verdict cache the result admission reuses.
	filter: &'s mut DiskAnnTruthyDocumentFilter<'b>,
	/// Document IDs whose pending update supersedes their graph entry.
	pending_docs: Option<&'s RoaringTreemap>,
	/// Doc-IDs the index-covered WHERE conjuncts admit.
	allow_list: Option<&'s RoaringTreemap>,
	/// The result builder, holding the pending scan's candidates.
	builder: &'s KnnResultBuilder,
	/// The k nearest documents admitted so far, each at the public distance of
	/// its nearest admitted element.
	admitted: KnnResultBuilder,
	/// Document sets of the admitted elements, resolved during classification
	/// and handed to the result admission so it resolves none again.
	docs: HashMap<ElementId, Ids64>,
}

impl GraphCandidateClassifier<'_, '_, '_> {
	/// Whether an element at public distance `distance` can still place in the
	/// top k.
	fn can_place(&self, distance: f64) -> bool {
		self.builder.check_add(distance) && self.admitted.check_add(distance)
	}

	/// Records the admission of `doc_id` at public distance `distance`, keeping
	/// the k nearest documents.
	fn record(&mut self, doc_id: u64, distance: f64) {
		self.admitted.add_vector_id_result(distance, VectorId::DocId(doc_id));
	}

	/// Whether a document passes the gates that need no record.
	fn doc_eligible(&self, doc_id: u64) -> bool {
		!self.pending_docs.is_some_and(|pending| pending.contains(doc_id))
			&& self.allow_list.is_none_or(|allow| allow.contains(doc_id))
	}
}

impl ElementClassifier for GraphCandidateClassifier<'_, '_, '_> {
	async fn classify(&mut self, candidates: &[DiskAnnSearchResult]) -> Result<Vec<bool>> {
		let mut verdicts = vec![false; candidates.len()];
		// Public distances, ascending, so the top-k bound tightens as elements are
		// admitted.
		let mut order: Vec<(usize, ElementId, f64)> = candidates
			.iter()
			.enumerate()
			.map(|(pos, &(id, distance))| (pos, id, self.index.graph_distance(distance)))
			.filter(|&(_, _, distance)| self.can_place(distance))
			.collect();
		if order.is_empty() {
			return Ok(verdicts);
		}
		order.sort_by(|a, b| a.2.total_cmp(&b.2));
		let lookups: Vec<(ElementId, f64)> =
			order.iter().map(|&(_, id, distance)| (id, distance)).collect();
		let docs = self
			.index
			.vec_docs
			.get_docs_by_element_batch(&self.ctx.tx, &self.ctx.provider_context.view, &lookups)
			.await?;
		// One multi-get for every record this batch may evaluate.
		let prefetch: Vec<VectorId> = docs
			.iter()
			.filter_map(|(_, _, docs)| docs.as_ref())
			.flat_map(|docs| docs.iter())
			.filter(|&doc_id| self.doc_eligible(doc_id))
			.map(VectorId::DocId)
			.collect();
		self.filter.prefetch_records(self.ctx, &prefetch).await?;
		for (&(pos, id, distance), (_, _, docs)) in order.iter().zip(&docs) {
			if !self.can_place(distance) {
				continue;
			}
			let Some(docs) = docs else {
				continue;
			};
			for doc_id in docs.iter() {
				if !self.doc_eligible(doc_id) {
					continue;
				}
				if self.filter.check_vector_id_truthy(self.ctx, VectorId::DocId(doc_id)).await? {
					verdicts[pos] = true;
					self.record(doc_id, distance);
					self.docs.insert(id, docs.clone());
					break;
				}
			}
		}
		Ok(verdicts)
	}
}

/// Which record-keyed pending layout a scanned range belongs to.
///
/// Both hold at most one pending state per record, in the entry its key names, which may
/// also hold the states of other records spelled alike; they differ in how the key is decoded
/// and in which of them owns a record the other also holds.
#[derive(Clone, Copy)]
enum PendingLayout {
	/// Sharded `!dw{shard}` entries, the layout every current write uses.
	Sharded,
	/// Legacy unsharded `!dr` entries, read and drained but never written.
	Legacy,
}

/// The record a DiskANN pending entry belongs to.
///
/// The rule is [`take_pending_record_id`]; this supplies the key decoder it
/// falls back to, which layout decides — the sharded and legacy keys differ,
/// the shard key carrying a `shard` field ahead of its id.
fn pending_record_id(
	key: &[u8],
	layout: PendingLayout,
	pending: &mut DiskAnnRecordPendingUpdate,
) -> Result<RecordIdKey> {
	take_pending_record_id(&mut pending.id, || {
		Ok(match layout {
			PendingLayout::Sharded => {
				DiskannRecordPendingShardKey::decode_key(key)?.id.into_owned()
			}
			PendingLayout::Legacy => DiskannRecordPendingKey::decode_key(key)?.id.into_owned(),
		})
	})
}

/// Mutable state threaded through the pending scan of one KNN lookup.
struct DiskAnnPendingScan<'a, 'b> {
	/// Prepared typed query and limits.
	search: &'a DiskAnnSearch,
	/// Optional condition filter applied before admitting candidates to the result
	/// builder.
	filter: &'a mut Option<DiskAnnTruthyDocumentFilter<'b>>,
	/// Doc-IDs admitted by the index-covered WHERE conjuncts. When set, candidates
	/// outside the bitmap are rejected before they take batch budget.
	allow_list: Option<&'a RoaringTreemap>,
	/// Shared result builder combining pending and graph candidates.
	builder: &'a mut KnnResultBuilder,
	/// Cursor paging, residency budget and backlog accounting, shared with the
	/// HNSW read path. This is the scan's whole vector residency; see
	/// [`DiskAnnIndex::search_pendings`].
	pending: PendingScan<'a>,
	/// Doc-IDs seen anywhere in the pending keyspace, which suppress the graph
	/// entries they supersede.
	suppressed: RoaringTreemap,
	/// Shards that at least one legacy `!dr` record hashes to, one bit per shard.
	/// A shard outside this mask can hold no entry the legacy range supersedes, so
	/// its scan resolves no ownership.
	legacy_shards: u32,
}

/// Every pending shard the keyspace defines gets a bit in
/// [`DiskAnnPendingScan::legacy_shards`].
const _: () = assert!(DISKANN_PENDING_STATE_SHARDS as u32 <= u32::BITS);

impl DiskAnnIndex {
	pub(crate) fn format_version(&self) -> u16 {
		self.format_version
	}

	/// Creates a DiskANN index wrapper and validates the configured type/metric combination.
	pub(crate) async fn new(
		ikb: IndexKeyBase,
		tb: TableId,
		p: &DiskAnnParams,
		cache: DiskAnnCache,
		format_version: u16,
	) -> Result<Self> {
		if !matches!(
			p.vector_type,
			VectorType::F32 | VectorType::F16 | VectorType::I8 | VectorType::U8
		) {
			bail!("DISKANN supports TYPE F32, F16, I8, and U8")
		}
		if matches!(p.distance, Distance::CosineNormalized)
			&& matches!(p.vector_type, VectorType::I8 | VectorType::U8)
		{
			bail!("DISKANN COSINE_NORMALIZED supports TYPE F32 and F16 only")
		}
		distance_to_metric(&p.distance)?;
		Ok(Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			distance: p.distance.clone(),
			table_id: tb,
			cache: cache.clone(),
			graph: RwLock::new(DiskAnnGraph::new(
				ikb.clone(),
				tb,
				p,
				cache.clone(),
				format_version,
			)?),
			vec_docs: DiskAnnVecDocs::new(
				ikb.clone(),
				tb,
				cache,
				p.use_hashed_vector,
				format_version,
			),
			ikb,
			format_version,
			pending_backlog_reported: PendingBacklogReport::default(),
			pending_scan_stats: PendingScanStats::default(),
			#[cfg(test)]
			faults: std::sync::Mutex::new(None),
			#[cfg(test)]
			frames: std::sync::Mutex::new(Vec::new()),
		})
	}

	/// Injects `faults` into every compaction frame this index opens from now on.
	#[cfg(test)]
	pub(crate) fn set_faults(&self, faults: Option<Arc<FrameFaults>>) {
		*self.faults.lock().unwrap_or_else(|p| p.into_inner()) = faults;
	}

	/// The last compaction frame this index opened.
	#[cfg(test)]
	pub(crate) fn last_frame(&self) -> Option<Arc<DiskAnnBuildFrame>> {
		self.frames.lock().unwrap_or_else(|p| p.into_inner()).last().cloned()
	}

	/// Every compaction frame this index opened, oldest first.
	#[cfg(test)]
	pub(crate) fn frames(&self) -> Vec<Arc<DiskAnnBuildFrame>> {
		self.frames.lock().unwrap_or_else(|p| p.into_inner()).clone()
	}

	/// What the KNN pending scans on this index materialised.
	#[cfg(test)]
	pub(crate) fn pending_scan_stats(&self) -> &PendingScanStats {
		&self.pending_scan_stats
	}

	/// Whether the pending-backlog report is armed for this index, which it is
	/// from the moment a scan crosses a materialisation budget until one
	/// completes inside both.
	#[cfg(test)]
	pub(crate) fn pending_backlog_reported(&self) -> bool {
		self.pending_backlog_reported.is_reported()
	}

	/// Converts upstream DiskANN scores to SurrealDB's public distance semantics.
	fn graph_distance(&self, distance: f64) -> f64 {
		match self.distance {
			// DiskANN's L2 scorer returns squared L2. SurrealDB's EUCLIDEAN distance is the true
			// Euclidean distance, and pending vectors are scored with that public value.
			Distance::Euclidean => distance.sqrt(),
			_ => distance,
		}
	}

	/// Converts indexed field values into validated serialized vectors for pending storage.
	fn content_to_vectors(&self, content: Vec<Value>) -> Result<Vec<SerializedVector>> {
		let mut vectors = Vec::with_capacity(content.len());
		for value in content.into_iter().filter(|v| !v.is_nullish()) {
			let vector = serialized_vector_from_value(self.vector_type, self.dim, value)?;
			Vector::check_expected_dimension(vector.dimension(), self.dim)?;
			vectors.push(vector);
		}
		Ok(vectors)
	}

	/// Maps a record key to the pending-state shard that should be bumped by its writer.
	fn pending_state_shard(id: &RecordIdKey) -> u16 {
		if let RecordIdKey::Number(id) = id {
			return id.rem_euclid(i64::from(DISKANN_PENDING_STATE_SHARDS)) as u16;
		}
		let mut hasher = DefaultHasher::new();
		id.hash(&mut hasher);
		(hasher.finish() % u64::from(DISKANN_PENDING_STATE_SHARDS)) as u16
	}

	/// Reads the sharded `!dy` pending-state guard for every shard, in one ordered batch.
	///
	/// This guard tracks the sharded `!dw` layout only. It is deliberately separate from the legacy
	/// `!dp` guard so a pre-change node's compactor (which clears `!dp` on `!dr`-emptiness, unaware
	/// of `!dw`) can never mark a shard empty while a `!dw` entry still exists. Legacy `!dr`
	/// records are not reflected here; lookup scans the legacy range unconditionally instead.
	///
	/// The read is noted on the transaction ([`Transaction::note_diskann_guards_read`]): on a store
	/// that validates every read, a guard this transaction read is covered by its own commit.
	///
	/// A shard this transaction wrote a pending update into reads `NonEmpty` whatever is stored:
	/// on a sole-writer store its guard is armed only when the transaction commits
	/// ([`Self::mark_pending_non_empty`]), and a lookup in the transaction must still see the
	/// write.
	async fn read_pending_state(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<PendingStateSnapshot> {
		let keys: Vec<_> =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|shard| ikb.new_dy_key(shard)).collect();
		let mut states: PendingStateSnapshot = tx.get_many_key(keys, None).await?;
		tx.note_diskann_guards_read(ikb.ns(), ikb.db(), ikb.table(), ikb.index());
		let buffered =
			tx.diskann_buffered_pending_guard_shards(ikb.ns(), ikb.db(), ikb.table(), ikb.index());
		if buffered == 0 {
			return Ok(states);
		}
		for (shard, state) in states.iter_mut().enumerate() {
			if state.as_ref().is_some_and(|s| s.kind != DiskAnnPendingStateKind::Empty) {
				continue;
			}
			if buffered & (1 << shard) != 0 {
				*state = Some(DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: state.as_ref().map_or(0, |s| s.generation),
				});
			}
		}
		Ok(states)
	}

	/// Covers the sharded `!dy` pending-state guard of a `!dw` update this transaction wrote.
	///
	/// Lookup and compaction skip a shard whose guard is `Empty`, so a `!dw` entry must not
	/// commit under an `Empty` guard. The writing transaction neither reads nor writes the guard,
	/// so no guard step can fail its commit: it names the shard, and its commit covers the guard
	/// under the index's guard fence (`Transaction::buffer_diskann_pending_guard`).
	///
	/// Where this datastore is the store's sole writer ([`Transaction::sole_writer`]), the commit
	/// arms the guard, and the fence orders the arming against every guard step. Elsewhere other
	/// nodes step guards too, and the commit covers the guard with this datastore's writer lease
	/// on the shard instead (`!dz`), under which no node steps it. The commit reads that lease
	/// in a transaction of its own, so a lease another node's wipe of the index's data removed is
	/// taken again, and the guard armed with it.
	fn mark_pending_non_empty(tx: &Transaction, ikb: &IndexKeyBase, id: &RecordIdKey) {
		tx.buffer_diskann_pending_guard(
			ikb.ns(),
			ikb.db(),
			ikb.table(),
			ikb.index(),
			Self::pending_state_shard(id),
		);
	}

	/// Records a transaction's old/new vectors as a coalesced pending update.
	pub(crate) async fn index(
		&self,
		env: &dyn IndexEnv,
		id: &RecordIdKey,
		old_values: Option<Vec<Value>>,
		new_values: Option<Vec<Value>>,
	) -> Result<()> {
		if old_values.is_none() && new_values.is_none() {
			return Ok(());
		}
		let old_vectors = if let Some(v) = old_values {
			self.content_to_vectors(v)?
		} else {
			vec![]
		};
		let new_vectors = if let Some(v) = new_values {
			self.content_to_vectors(v)?
		} else {
			vec![]
		};
		let tx = env.tx();
		// New writes always use the sharded `!dw` layout so compaction and lookup can work one
		// shard at a time. The shard matches the `!dy` guard shard this write bumps below.
		let shard = Self::pending_state_shard(id);
		let key = self.ikb.new_dw_key(shard, id);
		// Always reclaim any legacy `!dr` entry for this record, even when a sharded `!dw` entry
		// already exists. A mixed-version cluster can leave both layouts for one record (an older
		// node writes `!dr` after a newer node wrote `!dw`); if the stale legacy entry survived,
		// lookup — which scans the legacy range last — would let it overwrite the newer vectors.
		// The two layouts always chain (each write's `old_vectors` is the previous value), so fold
		// them into a single `!dw` entry that keeps the chain head's `old_vectors`.
		//
		// The entry's key spells `id` without its numbers' kinds, so it may hold
		// other records. On an index stamped for it, a write folds into its own
		// record's state, kept apart from the others'. Only a release predating
		// that stamp writes `!dr`, so the dual-layout arms below read an entry
		// naming one record, and keep any others it holds.
		let legacy = Self::take_legacy_pending(&tx, &self.ikb, id).await?;
		let members = keeps_pending_members(self.format_version);
		let mut pending = match (tx.get_key(&key, None).await?, legacy) {
			(Some(mut sharded), None)
				if members
					&& sharded
						.id
						.as_ref()
						.is_some_and(|named| !named.addresses_same_record(id)) =>
			{
				if let Some(member) =
					sharded.others.iter_mut().find(|m| m.id.addresses_same_record(id))
				{
					member.new_vectors = new_vectors;
				} else {
					sharded.others.push(DiskAnnRecordPendingMember {
						doc_id: DiskAnnDocs::get_doc_id(&self.ikb, self.format_version, &tx, id)
							.await?,
						old_vectors,
						new_vectors,
						id: id.clone(),
					});
				}
				tx.set_key(&key, &sharded).await?;
				Self::mark_pending_non_empty(&tx, &self.ikb, id);
				return Ok(());
			}
			(Some(mut sharded), None) => {
				sharded.new_vectors = new_vectors;
				sharded
			}
			(None, Some(mut legacy)) => {
				legacy.new_vectors = new_vectors;
				legacy
			}
			(Some(sharded), Some(legacy)) => {
				let sharded_precedes = sharded.new_vectors == legacy.old_vectors;
				let legacy_precedes = legacy.new_vectors == sharded.old_vectors;
				debug_assert!(
					sharded_precedes || legacy_precedes,
					"DiskANN write fold: non-chaining dual entries for {id:?} (sharded {:?} -> {:?}, legacy {:?} -> {:?})",
					sharded.old_vectors,
					sharded.new_vectors,
					legacy.old_vectors,
					legacy.new_vectors,
				);
				DiskAnnRecordPendingUpdate {
					doc_id: sharded.doc_id.or(legacy.doc_id),
					// The sharded `!dw` is the earlier write (the chain head), so keep its
					// `old_vectors`. Keying off `sharded_precedes` (not `legacy_precedes`) also
					// resolves an exact inverse pair — where both predicates hold — to the head
					// rather than the intermediate value, which would otherwise survive as a
					// phantom.
					old_vectors: if sharded_precedes {
						sharded.old_vectors
					} else {
						legacy.old_vectors
					},
					new_vectors,
					id: None,
					others: sharded.others,
				}
			}
			(None, None) => DiskAnnRecordPendingUpdate {
				doc_id: DiskAnnDocs::get_doc_id(&self.ikb, self.format_version, &tx, id).await?,
				old_vectors,
				new_vectors,
				id: None,
				others: Vec::new(),
			},
		};
		// Every write stamps the exact id, whichever arm built the entry, so
		// compaction and lookup never have to recover it from a key that cannot
		// spell it. Stamping here rather than per arm keeps that true of an arm
		// added later.
		pending.id = Some(id.clone());
		tx.set_key(&key, &pending).await?;
		Self::mark_pending_non_empty(&tx, &self.ikb, id);
		Ok(())
	}

	/// Reads and removes any legacy unsharded `!dr` pending entry for one record so the write path
	/// can fold it into the sharded `!dw` entry during the dual-read migration. Returns the legacy
	/// update (with its original `old_vectors`/`doc_id`) when one existed.
	async fn take_legacy_pending(
		tx: &Transaction,
		ikb: &IndexKeyBase,
		id: &RecordIdKey,
	) -> Result<Option<DiskAnnRecordPendingUpdate>> {
		let legacy_key = ikb.new_dr_key(id);
		let Some(legacy) = tx.get_key(&legacy_key, None).await? else {
			return Ok(None);
		};
		tx.del_key(&legacy_key).await?;
		Ok(Some(legacy))
	}

	/// Converts a persisted record-keyed pending value into a graph compaction operation.
	fn record_pending_to_operation(
		id: RecordIdKey,
		pending: DiskAnnRecordPendingUpdate,
	) -> PendingOperation {
		let record = Arc::new(id);
		let id = if let Some(doc_id) = pending.doc_id {
			VectorId::DocId(doc_id)
		} else {
			VectorId::RecordKey(Arc::clone(&record))
		};
		PendingOperation {
			id,
			record: Some(record),
			old_vectors: pending.old_vectors,
			new_vectors: pending.new_vectors,
		}
	}

	/// Builds a context that shares the current transaction with the DiskANN provider.
	fn new_diskann_context<'a>(
		&'a self,
		env: &'a dyn IndexEnv,
		provider_context: DiskAnnProviderContext,
	) -> DiskAnnContext<'a> {
		DiskAnnContext::new(env, self.ikb.clone(), provider_context)
	}

	/// Scans bounded pending ranges and prepares a conditional compaction batch.
	///
	/// Draining is staged:
	///   1. The legacy unsharded `!dr` range is drained first; while it isn't fully drained in a
	///      pass the pass returns early, so `cleared_shards` stays empty until legacy is exhausted.
	///   2. Once legacy is empty, each shard that may hold data has its `!dw` range drained in
	///      shard order until the batch budget is hit. Every shard fully drained within budget is
	///      recorded in `cleared_shards` so apply advances only those shards' `!dy` guard — letting
	///      lookup stop scanning drained shards instead of sweeping the whole index.
	///
	/// The `!dy` guard clear is itself decoupled from legacy drain
	/// ([`Self::pending_shard_ranges_empty`] checks only the per-shard `!dw` range), but this
	/// phase-1-first staging still gates *when* phase 2 runs: while a legacy backlog exceeds one
	/// batch, no `!dy` shard advances toward `Empty`, so during a large rolling-upgrade drain
	/// lookup keeps scanning every non-empty `!dy` shard's `!dw` range. That cost is transient
	/// (legacy is monotonically non-increasing — writes fold it away via `take_legacy_pending`)
	/// and never wrong; reserving batch budget for phase 2 so the two overlap is a possible future
	/// refinement.
	pub(crate) async fn prepare_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
	) -> Result<DiskAnnCompactionPlan> {
		let tx = env.tx();
		let generation = read_compaction_generation(&tx, &ikb.new_dg_key()).await?;
		let pending_state = Self::read_pending_state(&tx, ikb).await?;
		let mut builder = PendingPlanBuilder::new(generation, pending_state.clone());
		let mut count = 0;
		// `!dw` keys folded into phase 1 next to their legacy `!dr` counterpart, so phase 2 skips
		// them instead of capturing — and conditionally deleting — the same key twice.
		let mut folded_shard_keys: HashSet<Vec<u8>> = HashSet::new();
		// Phase 1: legacy `!dr` (dual-read transition). Drains to empty over time. Each legacy
		// record's sharded `!dw` counterpart, if any, is folded into the same builder so a
		// dual-layout record is always coalesced here rather than split across compaction passes.
		let legacy_drained = Self::capture_legacy_range(
			env,
			&tx,
			ikb,
			&mut count,
			&mut builder,
			&mut folded_shard_keys,
		)
		.await?;
		if !legacy_drained {
			// The legacy `!dr` range still holds entries that didn't fit this batch, so more passes
			// are needed. `capture_legacy_range` admits a legacy+`!dw` pair via `has_room_for` — a
			// pure check that, unlike `PendingPlanBuilder::add`, does not set `has_more` — so a
			// byte- or key-bounded legacy backlog can bail with `has_more` still false. Force it
			// here: `process_diskann_compaction` ends the compaction cycle as soon as a plan
			// reports `has_more == false`, which would otherwise strand the undrained legacy
			// entries (and the expensive full-range legacy lookup scans this change exists to
			// drain) until the next write happens to re-enqueue the index.
			builder.has_more = true;
			return Ok(builder.into_plan(Vec::new()));
		}
		// Phase 2: sharded `!dw` per shard that may hold data. A write commits its `!dw` key only
		// once its shard's `!dy` guard reads `NonEmpty`, and no guard is stepped to `Empty` over
		// a committed entry (`mark_pending_non_empty`, within the engine limits it documents).
		// Absent/Empty shards therefore hold no committed entry, except where a repair after the
		// fact failed or never ran. So an `Empty` or absent shard is probed, and one holding
		// entries has its guard repaired, with a pass queued to compact it
		// (`Transaction::repair_diskann_pending_guards`). With no such shard there is nothing to
		// probe. Read in this snapshot, the probe also tells phase 2 which armed shards hold
		// nothing, which it then clears without scanning.
		let unarmed = |state: &Option<DiskAnnPendingState>| {
			state.as_ref().is_none_or(|s| s.kind == DiskAnnPendingStateKind::Empty)
		};
		let holding = if pending_state.iter().any(unarmed) {
			let holding = tx
				.diskann_shards_holding_entries(ikb.ns(), ikb.db(), ikb.table(), ikb.index())
				.await?;
			let orphaned: Vec<u16> = pending_state
				.iter()
				.enumerate()
				.filter(|(shard, state)| holding & (1 << shard) != 0 && unarmed(state))
				.map(|(shard, _)| shard as u16)
				.collect();
			tx.repair_diskann_pending_guards(
				ikb.ns(),
				ikb.db(),
				ikb.table(),
				ikb.index(),
				&orphaned,
			)
			.await?;
			Some(holding)
		} else {
			None
		};
		let mut cleared_shards = Vec::new();
		for (shard, state) in pending_state.iter().enumerate() {
			if unarmed(state) {
				continue;
			}
			let shard = shard as u16;
			if holding.is_some_and(|holding| holding & (1 << shard) == 0) {
				cleared_shards.push(shard);
				continue;
			}
			let drained = Self::capture_shard_range(
				env,
				&tx,
				ikb.new_dw_shard_range(shard)?,
				&mut count,
				&mut builder,
				&folded_shard_keys,
			)
			.await?;
			if !drained {
				break;
			}
			cleared_shards.push(shard);
		}
		Ok(builder.into_plan(cleared_shards))
	}

	/// Captures the legacy `!dr` range for phase 1 of the dual-read migration, folding each
	/// record's sharded `!dw` counterpart (when one exists) into the same builder. Returns whether
	/// the legacy range was fully drained within the batch budget.
	///
	/// The fold is what keeps a dual-layout record correct. `prepare_compaction` drains the whole
	/// legacy range before phase 2 touches any shard, so without folding, a record holding both a
	/// `!dr` and a `!dw` entry could have them captured in separate passes and applied uncoalesced.
	/// Because the legacy write is always the later one, applying it alone (or the record-keyed
	/// insert path, which never removes the old vector) leaves the intermediate vector behind as a
	/// phantom. Capturing both keys here lets [`PendingPlanBuilder::add_pending`] coalesce them by
	/// the old->new chain, and deletes both in the same apply.
	///
	/// The per-record `!dw` probe only runs while the legacy range is non-empty (the migration
	/// window); once legacy drains, this is a single empty range scan with no probes.
	async fn capture_legacy_range(
		env: &dyn IndexEnv,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		count: &mut usize,
		builder: &mut PendingPlanBuilder,
		folded_shard_keys: &mut HashSet<Vec<u8>>,
	) -> Result<bool> {
		let mut cursor =
			tx.open_vals_cursor(ikb.new_dr_range()?, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				return Ok(true);
			}
			for (legacy_key, legacy_value) in batch.iter() {
				if env.is_done(Some(*count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				let mut legacy_update =
					DiskAnnRecordPendingUpdate::kv_decode_value(legacy_value, ())?;
				let mut id =
					pending_record_id(legacy_key, PendingLayout::Legacy, &mut legacy_update)?;
				// Probe the record's sharded `!dw` counterpart so a dual-layout record is folded in
				// here rather than split across passes. Deletes happen only at apply, so the `!dw`
				// entry is still present for this read.
				//
				// The probe keys on the spelling recovered from the legacy key, and both the shard
				// and the key's id component come from it, so it finds the counterpart only when
				// that spelling is what the `!dw` key was written under. It is for every id whose
				// numeric components survive the codec unchanged — integers, exact decimals and
				// exactly representable floats. An inexact float does not: it is re-encoded at a
				// shorter scale, so the probe misses and the pair is folded across two passes
				// instead of one. Usually that costs a pass and nothing else, since
				// record-level resolution reads the exact id from the value below. The
				// exception is a legacy entry written after an uncompacted sharded one
				// for such an id: the probe misses, so the legacy entry folds alone
				// under the key's spelling and carries no doc-ID of its own. Fixing it
				// needs the shard, which needs the exact id, which only the entry it
				// cannot find carries. Only a node old enough to still write the legacy
				// layout can produce it, and 3.2.0 was the release that stopped — so it
				// takes a 3.1.x node running alongside this one, not the adjacent skew.
				// A 3.2+ node writes `!dw`, reads it, and fails on the revision it
				// cannot decode rather than leaving this behind.
				let (shard_key, shard_value) = {
					let shard_key = ikb.new_dw_key(Self::pending_state_shard(&id), &id);
					let value = tx.get_key_raw(&shard_key, None).await?;
					let encoded = value.is_some().then(|| shard_key.encode_key()).transpose()?;
					(encoded, value)
				};
				// The sharded entry's records: its own, which the legacy entry pairs with, then
				// any other record sharing its key, each carrying its id.
				let mut shard_records = shard_value
					.as_ref()
					.map(|v| {
						DiskAnnRecordPendingUpdate::kv_decode_value(v, ())
							.map(|u| u.into_records().collect::<Vec<_>>())
					})
					.transpose()?;
				// Nothing writes the legacy layout any more, so its entries never carry the id and
				// the spelling above is whatever the key could express — decimalised, for an id
				// holding a number. Both entries name one record, so when the sharded counterpart
				// carries the exact id, that is the one the pair folds under.
				if let Some(exact) =
					shard_records.as_mut().and_then(|r| r.first_mut()).and_then(|u| u.id.take())
				{
					id = exact;
				}
				let legacy_op = Self::record_pending_to_operation(id.clone(), legacy_update);
				let shard_entry = match (shard_key, shard_value, shard_records) {
					(Some(shard_key), Some(shard_value), Some(records)) => {
						let mut ops = Vec::with_capacity(records.len());
						for (i, mut update) in records.into_iter().enumerate() {
							let record = match update.id.take() {
								Some(other) if i > 0 => other,
								_ => id.clone(),
							};
							ops.push(Self::record_pending_to_operation(record, update));
						}
						Some((shard_key, shard_value, ops))
					}
					_ => None,
				};
				// Admit the legacy entry and its counterpart atomically: deferring only one of the
				// pair to a later pass would reintroduce the cross-pass split the fold prevents.
				let pair_keys = 1 + usize::from(shard_entry.is_some());
				let pair_bytes = legacy_key.len()
					+ legacy_value.len()
					+ shard_entry.as_ref().map_or(0, |(k, v, _)| k.len() + v.len());
				let pair_vectors = legacy_op.new_vectors.len()
					+ shard_entry
						.as_ref()
						.map_or(0, |(_, _, ops)| ops.iter().map(|op| op.new_vectors.len()).sum());
				if !builder.has_room_for(pair_keys, pair_bytes, pair_vectors) {
					return Ok(false);
				}
				// The pair is authorized; capture both halves unconditionally so the byte guard can
				// never admit one and reject the other (which would orphan the sharded half). The
				// plan needs each entry's exact bytes for its conditional delete, so the copy is
				// made here rather than for the whole batch up front: the caps above stop a
				// part-read batch as a matter of course, and what the loop never reaches is never
				// copied.
				builder.add_authorized(legacy_key.to_vec(), legacy_value.to_vec(), [legacy_op]);
				if let Some((shard_key_bytes, shard_value, shard_op)) = shard_entry {
					folded_shard_keys.insert(shard_key_bytes.as_slice().to_vec());
					builder.add_authorized(
						shard_key_bytes.as_slice().to_vec(),
						shard_value,
						shard_op,
					);
				}
				*count += 1;
				if builder.has_more {
					return Ok(false);
				}
			}
		}
	}

	/// Captures one shard's `!dw` range for phase 2, skipping any key already folded into phase 1
	/// (`folded_shard_keys`) so a dual-layout record's `!dw` entry is never captured — and
	/// conditionally deleted — twice in one plan. Returns whether the range was fully drained
	/// within the batch budget.
	async fn capture_shard_range(
		env: &dyn IndexEnv,
		tx: &Transaction,
		rng: TypedRange<DiskAnnRecordPendingUpdate>,
		count: &mut usize,
		builder: &mut PendingPlanBuilder,
		folded_shard_keys: &HashSet<Vec<u8>>,
	) -> Result<bool> {
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				return Ok(true);
			}
			for (key, value) in batch.iter() {
				if env.is_done(Some(*count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				if folded_shard_keys.contains(key) {
					// Already folded next to its legacy counterpart in phase 1; the plan deletes
					// it.
					continue;
				}
				// One operation per record the entry holds; only the first can
				// predate the id field, and so need its key to name it.
				let mut operations = Vec::with_capacity(1);
				for mut pending in
					DiskAnnRecordPendingUpdate::kv_decode_value(value, ())?.into_records()
				{
					let id = pending_record_id(key, PendingLayout::Sharded, &mut pending)?;
					operations.push(Self::record_pending_to_operation(id, pending));
				}
				// Copied per entry rather than per batch, for the reason given in
				// `collect_legacy_record_pending`.
				if !builder.add(key.to_vec(), value.to_vec(), operations) {
					return Ok(false);
				}
				*count += 1;
				if builder.has_more {
					return Ok(false);
				}
			}
		}
	}

	/// Applies a prepared compaction plan if its generation and captured keys are still current.
	///
	/// The transaction lifecycle is owned by this method:
	///   * [`CompactionApply::Applied`] — mutations were applied and the transaction has been
	///     committed, or the plan captured no pending and a drained shard's guard was stepped in a
	///     transaction of its own, the plan's transaction having been cancelled.
	///   * [`CompactionApply::Empty`] — the plan captured no pending and no drained shard's guard
	///     was stepped; the transaction has been cancelled. A guard left unstepped is stepped by a
	///     later pass.
	///   * [`CompactionApply::LostRace`] — the plan was stale (generation drift, captured-key
	///     mismatch, or a commit this process already made past the plan's snapshot); the
	///     transaction has been cancelled and no mutations are in KV or in the process-local cache.
	///   * `Err(_)` — the apply or commit step failed; the transaction has been cancelled and, if
	///     any graph mutations had been buffered, the per-index [`DiskAnnCache`] has been cleared
	///     while the graph lock was still held so concurrent KNN searches cannot observe a cache
	///     state that disagrees with KV.
	///
	/// This frame is what rule (2) of the
	/// [cache coherency invariant](crate::trees::diskann::provider) refers to: writable-tx
	/// cache write-throughs in the provider are sound only because they happen inside it.
	pub(crate) async fn apply_compaction(
		&self,
		env: &dyn IndexEnv,
		plan: DiskAnnCompactionPlan,
	) -> Result<CompactionApply> {
		let DiskAnnCompactionPlan {
			generation,
			pending_state: _,
			captured_keys,
			pending,
			cleared_shards,
			has_more: _,
		} = plan;
		let tx = env.tx();
		// The guards are stepped after this transaction, under the index's guard fence, so this
		// transaction never writes a guard a writer covers.
		if captured_keys.is_empty() {
			cancel_silently(&tx).await;
			return Ok(if self.step_pending_guards(&tx, &cleared_shards).await? {
				CompactionApply::Applied
			} else {
				CompactionApply::Empty
			});
		}
		// The write transaction belongs to this call from here: it is committed or cancelled inline
		// on every path, and the guard covers an early `?` or a panic by cancelling it in a task of
		// its own once every task of the frame has exited.
		let mut tx_guard = TxGuard::new(Arc::clone(&tx));
		match bump_compaction_generation(&tx, &self.ikb.new_dg_key(), generation).await {
			Ok(true) => {}
			Ok(false) => {
				cancel_silently(&tx).await;
				tx_guard.disarm();
				return Ok(CompactionApply::LostRace);
			}
			Err(e) => {
				cancel_silently(&tx).await;
				tx_guard.disarm();
				return Err(e);
			}
		}
		for captured in &captured_keys {
			match tx.del_compare(Key::from(&captured.key), Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => {
					cancel_silently(&tx).await;
					tx_guard.disarm();
					return Ok(CompactionApply::LostRace);
				}
				Err(e) => {
					cancel_silently(&tx).await;
					tx_guard.disarm();
					return Err(e);
				}
			}
		}
		// From here on we mutate the per-index [`DiskAnnCache`] through the
		// provider write-through paths. The graph write lock is held across both
		// the mutations and the eventual commit/cancel so a concurrent
		// `knn_search` (which takes `graph.read()`) cannot observe a cache state
		// that pre-empts KV.
		let mut graph = self.graph.write().await;
		let Some(provider_context) = graph.open_frame(Arc::clone(&tx), generation) else {
			drop(graph);
			cancel_silently(&tx).await;
			tx_guard.disarm();
			return Ok(CompactionApply::LostRace);
		};
		let committed = provider_context.view.commit_generation();
		// The span moved: the lock owns it, and the cache cleanup, from here.
		let mut lock = FrameLock::new(self, graph);
		let frame = match self.open_frame_view(env, &tx, &lock).await {
			// One synchronous step gives the frame to both guards, before any task can be spawned.
			Ok((frame, held)) => {
				lock.attach(Arc::clone(&frame), &mut tx_guard);
				// The view holds the generation every commit of the graph advances. One other than
				// the plan's means a commit landed after this frame's fence, so the frame's
				// conditional bump cannot commit.
				if let ViewGeneration::Holds(held) = held
					&& held != generation
				{
					lock.abort().await;
					frame.stats.tx_ended(frame.live_tasks());
					cancel_silently(&tx).await;
					tx_guard.disarm();
					return Ok(CompactionApply::LostRace);
				}
				frame
			}
			Err(e) => return fail(lock, &tx, tx_guard, e).await,
		};
		if let Err(e) =
			self.run_frame(env, &lock, &frame, provider_context, FrameWork::Apply(pending)).await
		{
			return fail(lock, &tx, tx_guard, e).await;
		}
		if let Err(e) = tx.commit().await {
			tx_guard.disarm();
			lock.abort().await;
			return Err(e);
		}
		tx_guard.disarm();
		// Extends the span, closes the frame and releases the graph lock, then cancels the frame's
		// read transaction. By the time any concurrent reader can acquire `graph.read()` the cache
		// and KV are consistent and the span covers the committed generation — or, on the error
		// paths above, the cache has been cleared and the span closed before the lock was
		// released.
		lock.committed(committed).await;
		// The apply has committed, so a failed step does not undo it: the guard is left as it was,
		// which may only make lookups scan a drained shard, and the next pass steps it.
		if let Err(e) = self.step_pending_guards(&tx, &cleared_shards).await {
			tracing::warn!(
				target: "surrealdb::idx::diskann",
				"failed to step the DiskANN pending-state guards after a compaction: {e}"
			);
		}
		Ok(CompactionApply::Applied)
	}

	/// Runs one frame's graph work, `pending`, under `lock`, and returns once every task the frame
	/// spawned has exited. A failure — the batch's, one a task recorded, or the statement stopping
	/// — is returned only after the batch has been driven to completion; the caller then undoes
	/// the frame and cancels its write transaction.
	async fn run_frame(
		&self,
		env: &dyn IndexEnv,
		lock: &FrameLock<'_>,
		frame: &Arc<DiskAnnBuildFrame>,
		provider_context: DiskAnnProviderContext,
		work: FrameWork,
	) -> Result<()> {
		let docs = DiskAnnDocs::with_format_version(self.ikb.clone(), self.format_version);
		let diskann_ctx =
			self.new_diskann_context(env, provider_context.in_frame(Arc::clone(frame)));
		// The watchdog polls the statement's cancellation and stops the frame. The batch is always
		// driven to completion afterwards, so no task it spawned outlives it unobserved: each fails
		// at its next KV operation, which checks the frame first.
		let outcome = {
			let work = async {
				match work {
					FrameWork::Apply(pending) => {
						self.apply_pending_batch(&diskann_ctx, &docs, lock.graph(), pending).await
					}
					FrameWork::Reprune(ids) => lock.graph().reprune(&diskann_ctx, ids).await,
				}
			};
			let mut work = std::pin::pin!(work);
			let first = tokio::select! {
				biased;
				r = &mut work => FrameOutcome::Done(r),
				stop = frame_watchdog(env, frame) => FrameOutcome::Stopped(stop),
			};
			match first {
				FrameOutcome::Done(r) => FrameOutcome::Done(r),
				FrameOutcome::Stopped(stop) => {
					frame.cancel();
					let _ = work.await;
					FrameOutcome::Stopped(stop)
				}
			}
		};
		let r = match outcome {
			FrameOutcome::Done(r) => r,
			// `is_done` answers `true` for a cancellation or a timeout, and fails for the memory
			// threshold; the latter keeps its classification.
			FrameOutcome::Stopped(Err(e)) => return Err(e),
			FrameOutcome::Stopped(Ok(())) => return Err(EngineError::QueryCancelled.into()),
		};
		// A failure a task recorded wins over the stringified error upstream returned, so its
		// classification survives.
		if let Some(e) = frame.take_error() {
			return Err(e);
		}
		r?;
		// No task the frame spawned may still be writing into its transaction when it commits.
		// Upstream has joined them all on this path, so this returns at once.
		frame.drain_tasks().await;
		Ok(())
	}

	/// Opens the frame of one apply under `lock`, with a read view taken alongside `tx` where the
	/// engine gives one. What the view reads is published as a reader at the view's own generation
	/// would publish it, under the span the frame holds.
	async fn open_frame_view(
		&self,
		env: &dyn IndexEnv,
		tx: &Transaction,
		lock: &FrameLock<'_>,
	) -> Result<(Arc<DiskAnnBuildFrame>, ViewGeneration)> {
		let graph = lock.graph();
		let publication = |held: Option<u64>| graph.reader_view(held).publication();
		#[cfg(not(test))]
		let opened =
			DiskAnnBuildFrame::open(tx, env.places(), &self.ikb.new_dg_key(), publication).await?;
		#[cfg(test)]
		let opened = {
			let hooks = FrameHooks {
				faults: self.faults.lock().unwrap_or_else(|p| p.into_inner()).clone(),
			};
			let opened = DiskAnnBuildFrame::open(
				tx,
				env.places(),
				&self.ikb.new_dg_key(),
				publication,
				hooks,
			)
			.await?;
			self.frames.lock().unwrap_or_else(|p| p.into_inner()).push(Arc::clone(&opened.0));
			opened
		};
		Ok(opened)
	}

	/// Applies a batch of freshly scanned records straight to the graph inside `env`'s write
	/// transaction, the build transaction of the scan batch that read them, as a frame with no
	/// captured pending keys: the initial-batch sink of a DiskANN build.
	///
	/// `range` is the first and last record key of the scan batch the records come from. The sink
	/// keeps no state across batches beyond it, so calls over disjoint ranges that tile a table
	/// index the same documents as one call over the whole range.
	///
	/// `records` are the records of the batch with no queued mutation, each with the vectors of
	/// its current value, holding at most [`DISKANN_DIRECT_MAX_VECTORS`] vectors between them
	/// unless one record alone exceeds it. Each is inserted under the doc-ID its record resolves
	/// to, exactly as a compaction folding its pending would insert it; nothing is written to
	/// `!dw`.
	///
	/// Returns a guard rather than `()`, because the build transaction is still open: the caller
	/// writes its continuation checkpoint after this returns and commits both together. The guard
	/// holds the graph write lock and the frame until then, so no search is served the frame's
	/// write-throughs before the commit makes them real. After the commit the caller calls
	/// [`DirectApplyGuard::finish`]; on any other path [`DirectApplyGuard::abort`], and then
	/// cancels its transaction. A guard dropped without either, on a panic or an early return,
	/// undoes the frame in `Drop`.
	///
	/// `tx_guard` is the caller's guard on the build transaction. The frame is attached to it in
	/// the same synchronous step as to the lock, so a panic anywhere after the frame's tasks can
	/// exist cancels the build transaction only after those tasks have drained.
	///
	/// Before it returns — with the guard or with an error — every task the frame spawned has
	/// exited, so nothing but the caller writes into the build transaction afterwards. On an error
	/// the frame is already undone; the build transaction stays the caller's to cancel.
	pub async fn apply_direct<'a>(
		&'a self,
		env: &dyn IndexEnv,
		range: (&[u8], &[u8]),
		records: Vec<(RecordIdKey, Vec<SerializedVector>)>,
		tx_guard: &mut BuildTxGuard,
	) -> Result<DirectApplyGuard<'a>> {
		let tx = env.tx();
		tracing::trace!(
			target: "surrealdb::idx::diskann",
			records = records.len(),
			from = ?range.0,
			to = ?range.1,
			"applying a scan batch directly to the DiskANN graph"
		);
		let (lock, frame, provider_context, committed) =
			self.open_direct_frame(env, &tx, tx_guard).await?;
		let pending = records
			.into_iter()
			.filter(|(_, vectors)| !vectors.is_empty())
			.map(|(id, new_vectors)| {
				let record = Arc::new(id);
				PendingOperation {
					id: VectorId::RecordKey(Arc::clone(&record)),
					record: Some(record),
					old_vectors: Vec::new(),
					new_vectors,
				}
			})
			.collect();
		if let Err(e) =
			self.run_frame(env, &lock, &frame, provider_context, FrameWork::Apply(pending)).await
		{
			lock.abort().await;
			return Err(e);
		}
		Ok(DirectApplyGuard {
			lock,
			committed,
		})
	}

	/// Opens a frame in `env`'s write transaction, `tx`, the build transaction a sink or a
	/// re-prune step runs in: fences the graph at the generation the transaction reads, takes
	/// the graph write lock, opens the span and the frame's read view, and attaches the frame to
	/// the lock and to `tx_guard` in one step. On an error nothing is left open but `tx`, the
	/// caller's to cancel.
	async fn open_direct_frame<'a>(
		&'a self,
		env: &dyn IndexEnv,
		tx: &Arc<Transaction>,
		tx_guard: &mut BuildTxGuard,
	) -> Result<(FrameLock<'a>, Arc<DiskAnnBuildFrame>, DiskAnnProviderContext, Option<u64>)> {
		// The generation this transaction reads, so the frame's bump fences any compaction planned
		// against an older one.
		let dg = self.ikb.new_dg_key();
		let generation = read_compaction_generation(tx, &dg).await?;
		if !bump_compaction_generation(tx, &dg, generation).await? {
			bail!(EngineError::unreachable(
				"a DiskANN generation read in the transaction that bumps it moved"
			));
		}
		let mut graph = self.graph.write().await;
		let Some(provider_context) = graph.open_frame(Arc::clone(tx), generation) else {
			// This wrapper committed past the transaction's snapshot: the build transaction cannot
			// commit its bump, and the builder retries the batch in a fresh one.
			bail!(KvsError::TransactionConflict(
				"a DiskANN commit landed after this build transaction began".into()
			));
		};
		let committed = provider_context.view.commit_generation();
		let mut lock = FrameLock::new(self, graph);
		let frame = match self.open_frame_view(env, tx, &lock).await {
			Ok((frame, held)) => {
				lock.attach(Arc::clone(&frame), &mut tx_guard.0);
				if let ViewGeneration::Holds(held) = held
					&& held != generation
				{
					lock.abort().await;
					bail!(KvsError::TransactionConflict(
						"a DiskANN commit landed after this build transaction began".into()
					));
				}
				frame
			}
			Err(e) => {
				lock.abort().await;
				return Err(e);
			}
		};
		Ok((lock, frame, provider_context, committed))
	}

	/// Re-prunes, in `env`'s write transaction, the adjacency lists of the live elements whose ids
	/// lie in one chunk of [`DISKANN_REPRUNE_CHUNK`] from `from`: every list grown past the
	/// degree since it was pruned — by back-edges later inserts appended — is pruned again
	/// against the graph as it now stands, the pass upstream runs at the end of a build.
	///
	/// Runs as a frame, as [`Self::apply_direct`] does, and returns its guard the same way, with
	/// the id the next chunk starts at; or no guard and no next id once `from` is past every id
	/// the graph has allocated. A list at or under the degree is left as it is, so a sweep that
	/// starts again from the beginning after an interruption redoes nothing it has done.
	pub async fn reprune_direct<'a>(
		&'a self,
		env: &dyn IndexEnv,
		from: ElementId,
		tx_guard: &mut BuildTxGuard,
	) -> Result<(Option<DirectApplyGuard<'a>>, Option<ElementId>)> {
		let tx = env.tx();
		// An index that has never been written has no state, and no ids to sweep.
		let state: crate::trees::diskann::DiskAnnState =
			tx.get_key(&self.ikb.new_ds_key(), None).await?.unwrap_or_default();
		if from >= state.next_element_id {
			return Ok((None, None));
		}
		let end = from.saturating_add(DISKANN_REPRUNE_CHUNK).min(state.next_element_id);
		let (lock, frame, provider_context, committed) =
			self.open_direct_frame(env, &tx, tx_guard).await?;
		if let Err(e) = self
			.run_frame(env, &lock, &frame, provider_context, FrameWork::Reprune(from..end))
			.await
		{
			lock.abort().await;
			return Err(e);
		}
		Ok((
			Some(DirectApplyGuard {
				lock,
				committed,
			}),
			(end < state.next_element_id).then_some(end),
		))
	}

	/// The vectors a record's indexed values hold, as a write of them would queue them: one per
	/// non-null column, each checked against the index's type and dimension.
	pub fn vectors_of(&self, content: Vec<Value>) -> Result<Vec<SerializedVector>> {
		self.content_to_vectors(content)
	}

	/// Steps the drained shards' guards to `Empty`, in a transaction of their own under the index's
	/// guard fence. On a store other datastores write to, a shard under a writer lease that still
	/// holds is skipped, and leases that no longer hold are retired with the step. See
	/// `Transaction::step_diskann_pending_guards`.
	///
	/// Boxed because the step holds a transaction of its own, which would otherwise grow the
	/// compaction's future by its size.
	async fn step_pending_guards(&self, tx: &Transaction, shards: &[u16]) -> Result<bool> {
		if shards.is_empty() {
			return Ok(false);
		}
		Box::pin(tx.step_diskann_pending_guards(
			self.ikb.ns(),
			self.ikb.db(),
			self.ikb.table(),
			self.ikb.index(),
			shards,
		))
		.await
	}

	/// Drops every entry in the process-local [`DiskAnnCache`] that is scoped to this index,
	/// keeping the [`DiskAnnIndex`] registration intact so the graph `RwLock` continues to
	/// serialise compaction and KNN search. Synchronous, so a frame's cleanup runs it on a path
	/// that cannot await.
	fn clear_local_cache_now(&self) {
		self.cache.remove_index_now(
			self.ikb.ns(),
			self.ikb.db(),
			self.table_id,
			self.ikb.index(),
			self.format_version,
		);
	}

	/// One frame's graph work.
	///
	/// Phase A applies removals and resolves doc-ids in plan order, one pending at a time. Phase B
	/// resolves each distinct new vector against the vector → docs mapping in one multi-get, so
	/// duplicates within the batch and vectors already in the graph never reach the graph. Phase C
	/// inserts the remaining fresh vectors: the first into an empty graph as its seed, a lone one
	/// through the single-vector insert, and more than one under ids allocated together, in
	/// consecutive batches of at most [`DISKANN_INSERT_BATCH_MAX_VECTORS`] — through
	/// upstream's batched insert on the frame's place grant, or one at a time on the calling task
	/// when the grant is empty. Every batch is checked before any doc mapping of a fresh vector is
	/// written: the first failure a task recorded fails the frame, and then every element of the
	/// batch must have an adjacency list written by the frame.
	async fn apply_pending_batch(
		&self,
		ctx: &DiskAnnContext<'_>,
		docs: &DiskAnnDocs,
		graph: &DiskAnnGraph,
		pending: Vec<PendingOperation>,
	) -> Result<()> {
		let inserts = self.apply_removals(ctx, docs, graph, pending).await?;
		let fresh: Vec<StagedVector> = self.vec_docs.stage_inserts(ctx, inserts).await?;
		if fresh.is_empty() {
			return Ok(());
		}
		let provider = graph.provider();
		let mut fresh = fresh;
		// An empty graph is seeded from its batch's medoid, and starts its searches from the
		// medoid and the batch vectors spread farthest from it, once they are inserted.
		let mut seeding: Option<(ElementId, Vec<usize>)> = None;
		if provider.valid_starting_points(&ctx.provider_context).await?.is_empty() {
			let vectors: Vec<&Vector> = fresh.iter().map(StagedVector::vector).collect();
			let positions = start_point_positions(&vectors, graph.metric);
			let Some(&medoid) = positions.first() else {
				return Ok(());
			};
			let first = fresh.remove(medoid);
			let id = graph.seed_first(ctx, first.vector()).await?;
			self.vec_docs.commit_fresh(ctx, std::iter::once((first, id))).await?;
			// The other start points' positions among the vectors left once the seed is out.
			let others = positions[1..]
				.iter()
				.map(|&p| {
					if p > medoid {
						p - 1
					} else {
						p
					}
				})
				.collect();
			seeding = Some((id, others));
		}
		let rest: Vec<StagedVector> = fresh;
		let ids: Vec<ElementId> = match rest.as_slice() {
			[] => return Ok(()),
			[only] => vec![graph.insert(ctx, only.vector()).await?],
			_ => {
				let n = rest.len();
				let ids: Vec<ElementId> =
					provider.allocate_element_ids(&ctx.provider_context, n as u64).await?.collect();
				let frame = ctx.frame().ok_or_else(|| {
					EngineError::unreachable("a DiskANN compaction runs inside a frame")
				})?;
				// The frame's up-front grant: it never waits, returns between none and the
				// requested places, and is held until the frame ends. No more than one task per
				// vector of a batch is useful.
				let granted = frame.take_places(n.min(DISKANN_INSERT_BATCH_MAX_VECTORS));
				for (vectors, chunk) in rest
					.chunks(DISKANN_INSERT_BATCH_MAX_VECTORS)
					.zip(ids.chunks(DISKANN_INSERT_BATCH_MAX_VECTORS))
				{
					match granted {
						// No place: nothing is spawned, and the vectors are inserted one at a time
						// on this task, in batch order, under the ids allocated above.
						None => {
							for (vector, id) in vectors.iter().zip(chunk) {
								graph.insert_with_id(ctx, vector.vector(), *id).await?;
							}
							frame.stats.sequential_vectors.fetch_add(
								vectors.len() as u64,
								std::sync::atomic::Ordering::Relaxed,
							);
						}
						Some(tasks) => {
							let rows: Vec<&Vector> =
								vectors.iter().map(StagedVector::vector).collect();
							graph.insert_batch(ctx, &rows, chunk, tasks).await?;
							frame
								.stats
								.batch_calls
								.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
							frame.stats.batched_vectors.fetch_add(
								vectors.len() as u64,
								std::sync::atomic::Ordering::Relaxed,
							);
							frame
								.stats
								.largest_batch
								.fetch_max(vectors.len(), std::sync::atomic::Ordering::Relaxed);
						}
					}
					// Upstream's batched insert answers `Ok(())` even when its tasks failed. A
					// failure a task recorded, or a task panic, wins; then the structural
					// post-condition.
					if let Some(e) = frame.take_error() {
						return Err(e);
					}
					frame.verify_nodes_written(chunk)?;
				}
				ids
			}
		};
		let first_inserted = ids[0];
		if let Some((_, others)) = &seeding {
			let start_points: Vec<ElementId> = others.iter().map(|&p| ids[p]).collect();
			provider.set_start_points(&ctx.provider_context, start_points).await?;
		}
		self.vec_docs.commit_fresh(ctx, rest.into_iter().zip(ids)).await?;
		// A no-op once the graph has an entry point, which it has by now on every path.
		provider.ensure_entry_point(&ctx.provider_context, first_inserted).await
	}

	/// Phase A of [`Self::apply_pending_batch`]: removes every pending's old vectors and resolves
	/// the doc-id its new vectors are inserted under, in plan order, and returns those inserts.
	async fn apply_removals(
		&self,
		ctx: &DiskAnnContext<'_>,
		docs: &DiskAnnDocs,
		graph: &DiskAnnGraph,
		pending: Vec<PendingOperation>,
	) -> Result<Vec<(DocId, SerializedVector)>> {
		let mut inserts = Vec::new();
		for pending in pending {
			match pending.id {
				VectorId::DocId(doc_id) => {
					// Remove the graph baseline under the doc-ID those vectors live under.
					for vector in pending.old_vectors {
						let vector = Vector::from(vector);
						self.vec_docs.remove(ctx, &vector, doc_id, graph).await?;
					}
					if pending.new_vectors.is_empty() {
						if self.format_version < crate::catalog::DOC_IDS_FORMAT_VERSION
							&& let Some(record) = &pending.record
						{
							// A bounded group of operations of its own: checked once, before it.
							ctx.kv().check()?;
							LegacyVectorDocIds::DiskAnn(self.ikb.clone())
								.remove(&ctx.env.tx(), record)
								.await?;
						}
						docs.remove(doc_id, self.table_id, &self.cache);
					} else {
						// A delete removes the record's shared doc-ID mapping and a
						// re-create before compaction folds its vectors in here, so
						// resolve the *current* doc-ID from the record key rather than
						// reusing the captured (possibly deleted) one. For a plain
						// update the mapping is unchanged and this returns the same id.
						let insert_doc_id = match &pending.record {
							Some(record) => self.resolve_doc_id(ctx, docs, record).await?,
							None => doc_id,
						};
						inserts.extend(pending.new_vectors.into_iter().map(|v| (insert_doc_id, v)));
					}
				}
				VectorId::RecordKey(id) => {
					if !pending.new_vectors.is_empty() {
						let doc_id = self.resolve_doc_id(ctx, docs, &id).await?;
						inserts.extend(pending.new_vectors.into_iter().map(|v| (doc_id, v)));
					}
				}
			}
		}
		Ok(inserts)
	}

	/// Resolves, or allocates, the doc-id of `record` for a frame.
	///
	/// The resolution is one bounded group of KV operations, checked against the frame before it
	/// starts, and the sequence allocator it may call polls the frame through its probe before
	/// every window claim. So once the frame is cancelled no further record is resolved, and at
	/// most the one window claim already in flight completes.
	async fn resolve_doc_id(
		&self,
		ctx: &DiskAnnContext<'_>,
		docs: &DiskAnnDocs,
		record: &RecordIdKey,
	) -> Result<DocId> {
		ctx.kv().check()?;
		match ctx.frame() {
			Some(frame) => {
				let env = FrameEnv {
					inner: ctx.env,
					frame,
				};
				// Its probe is this environment, which fails once the frame is cancelled.
				docs.resolve(&env, record).await
			}
			None => docs.resolve(ctx.env, record).await,
		}
	}

	/// Placeholder consistency hook matching the HNSW index-store interface.
	pub async fn check_state(&self) -> Result<()> {
		Ok(())
	}

	/// Executes a DiskANN KNN lookup and returns ordered iterator results.
	///
	/// Lookup scans pending updates unless the distributed pending-state guard is explicitly empty.
	/// Compacted graph candidates are resolved through process-local caches before any remaining KV
	/// reads, and final document IDs are materialized in one batch.
	/// `allow_list` restricts candidate admission to the given doc-IDs (over
	/// the table's shared doc-ID space) without fetching records. `None`
	/// leaves admission unrestricted.
	pub async fn knn_search(
		&self,
		env: &dyn IndexEnv,
		pt: &[Number],
		k: usize,
		ef: usize,
		cond_filter: Option<KnnCondFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let pending_state = Self::read_pending_state(&env.tx(), &self.ikb).await?;
		let compaction_generation =
			read_compaction_generation(&env.tx(), &self.ikb.new_dg_key()).await?;
		let mut filter = cond_filter.map(|f| {
			DiskAnnTruthyDocumentFilter::new(
				self.ikb.clone(),
				self.format_version,
				self.table_id,
				self.cache.clone(),
				compaction_generation,
				f.cond,
				f.select_gate,
				f.metrics,
				f.version,
			)
		});
		let vector = Vector::try_from_vector(self.vector_type, pt)?;
		vector.check_dimension(self.dim)?;
		let search = DiskAnnSearch::new(vector, k, ef)?;
		let graph = self.graph.read().await;
		let provider_context = graph.context(env.tx(), compaction_generation);
		let ctx = self.new_diskann_context(env, provider_context);
		let mut builder = KnnResultBuilder::new(k);
		// `pending_state` reflects only the sharded `!dy` guard; legacy `!dr` records are not
		// tracked there, so we always scan pendings. `search_pendings` sweeps the legacy `!dr`
		// range unconditionally and the per-shard `!dw` ranges of the non-empty `!dy` shards;
		// when nothing is pending the cost is a single empty legacy range probe.
		let pending_docs = self
			.search_pendings(&ctx, &search, &mut filter, allow_list, &mut builder, &pending_state)
			.await?;
		self.search_graph(
			&ctx,
			DiskAnnGraphSearch {
				graph: &graph,
				search: &search,
				pending_docs,
				allow_list,
				filter: &mut filter,
				builder: &mut builder,
			},
		)
		.await?;
		let cache = filter.map(DiskAnnTruthyDocumentFilter::release);
		self.resolve_results(&ctx, builder, cache.as_ref(), compaction_generation).await
	}

	/// Replaces the doc-IDs of a finished result with their record IDs,
	/// nearest first, taking the records a truthy filter already fetched from
	/// its `cache`. A doc-ID that no longer resolves takes no result slot.
	async fn resolve_results(
		&self,
		ctx: &DiskAnnContext<'_>,
		builder: KnnResultBuilder,
		cache: Option<&FilterCache>,
		compaction_generation: Option<u64>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let result = builder.collect();
		let doc_ids: Vec<_> = result
			.iter()
			.filter_map(|(_, id)| match id {
				VectorId::DocId(doc_id) => Some(*doc_id),
				VectorId::RecordKey(_) => None,
			})
			.collect();
		let mut doc_rids = DiskAnnDocs::get_things_batch(
			&ctx.ikb,
			self.format_version,
			self.table_id,
			&self.cache,
			&ctx.tx,
			&doc_ids,
			compaction_generation,
		)
		.await?
		.into_iter();
		let mut res = VecDeque::with_capacity(result.len());
		for (dist, id) in result {
			let dist: f64 = dist.into();
			let cached = cache.and_then(|cache| cache.get(&id)).cloned();
			match id {
				VectorId::DocId(_) => {
					let rid = doc_rids.next().unwrap_or(None);
					if let Some(Some((rid, record))) = cached {
						res.push_back((rid, dist, Some(record)));
					} else if let Some(rid) = rid {
						res.push_back((rid, dist, None));
					}
				}
				VectorId::RecordKey(key) => {
					if let Some(Some((rid, record))) = cached {
						res.push_back((rid, dist, Some(record)));
						continue;
					}
					let rid = RecordId::new(self.ikb.table().clone(), key.as_ref().clone());
					res.push_back((Arc::new(rid), dist, None));
				}
			}
		}
		Ok(res)
	}

	/// Exact k-nearest neighbours among the `allow_list` members, scored from
	/// the vectors the index stores rather than from the records.
	///
	/// Every member is scored: pending (not yet compacted) vectors by the same
	/// pending scan the graph search runs, and committed vectors through the
	/// doc-ID → elements mapping, with every doc-ID the pending queue names
	/// suppressed from the committed side. Distances use the index's public
	/// distance semantics, as pending scoring does. No graph is walked and no
	/// record is fetched; only the final `k` doc-IDs are resolved to record IDs.
	/// A document indexing several vectors takes one slot, at its nearest
	/// vector.
	///
	/// Candidates are admitted on allow-list membership alone, so the caller is
	/// responsible for anything a record would have to be read for — a residual
	/// condition, or a table permission evaluated per record.
	///
	/// Returns `None`, having read nothing, for an index stamped before the
	/// mapping was kept (see [`crate::catalog::keeps_vector_doc_elements`]).
	pub async fn exact_knn_search(
		&self,
		env: &dyn IndexEnv,
		pt: &[Number],
		k: usize,
		allow_list: &RoaringTreemap,
	) -> Result<Option<VecDeque<KnnIteratorResult>>> {
		if !keeps_vector_doc_elements(self.format_version) {
			return Ok(None);
		}
		let vector = Vector::try_from_vector(self.vector_type, pt)?;
		vector.check_dimension(self.dim)?;
		// No member can be offered: answered before any read, once the query
		// vector has been checked.
		if allow_list.is_empty() || k == 0 {
			return Ok(Some(VecDeque::new()));
		}
		let tx = env.tx();
		let pending_state = Self::read_pending_state(&tx, &self.ikb).await?;
		let compaction_generation = read_compaction_generation(&tx, &self.ikb.new_dg_key()).await?;
		let search = DiskAnnSearch::new(vector, k, k)?;
		// The pending scan and the result resolution read only the transaction:
		// the graph is held to build their context, and again only while member
		// elements are read through it, so a long pending backlog does not keep
		// a compaction waiting on the graph.
		let ctx = self.new_diskann_context(
			env,
			self.graph.read().await.context(Arc::clone(&tx), compaction_generation),
		);
		let mut builder = KnnResultBuilder::new(k);
		let pending_docs = self
			.search_pendings(
				&ctx,
				&search,
				&mut None,
				Some(allow_list),
				&mut builder,
				&pending_state,
			)
			.await?;
		// The members the pendings did not already score: the allow-list itself
		// when nothing is pending, else the set difference, taken over roaring
		// containers rather than by a lookup per member.
		let unpending = pending_docs.as_ref().filter(|p| !p.is_empty()).map(|p| allow_list - p);
		let members = unpending.as_ref().unwrap_or(allow_list);
		// With every member pending, there is nothing left to score, and the
		// graph is not taken.
		if !members.is_empty() {
			let (this, tx, pt) = (self, &tx, &search.pt);
			doc_elements::score_members(
				env,
				members.iter(),
				move |docs: Vec<DocId>| async move {
					let sets = this.vec_docs.elements_of(tx, &docs).await?;
					Ok((docs, sets))
				},
				move |elements: Vec<ElementId>| async move {
					// Each batch's elements are read under one guard, through a
					// context built under it, and the guard is released before
					// they are scored: the graph is never held across the
					// doc → elements reads or the scoring.
					let read = {
						let graph = this.graph.read().await;
						let provider_context = graph.context(Arc::clone(tx), compaction_generation);
						graph.read_elements(&provider_context, &elements).await?
					};
					// An element read from the store is owned, and its vector is
					// scored as it is: `Vector::from` adopts the decoded buffer
					// without allocating or copying (F16 alone converts). A cached
					// element is shared with the cache: it is copied into one scratch
					// vector rather than cloned into a vector of its own. Routing
					// owned elements through the scratch vector too would add a full
					// copy per candidate and save no allocation.
					let mut scratch: Option<Vector> = None;
					let distances = read
						.into_iter()
						.map(|element| {
							let element = element.filter(|e| !e.deleted)?;
							let d = match Arc::try_unwrap(element) {
								Ok(owned) => {
									this.distance.calculate_against(pt, &Vector::from(owned.vector))
								}
								Err(shared) => {
									let vector = match scratch.as_mut() {
										Some(v) => {
											v.assign(&shared.vector);
											v
										}
										None => scratch.insert(Vector::from(shared.vector.clone())),
									};
									this.distance.calculate_against(pt, vector)
								}
							};
							Some(d)
						})
						.collect();
					Ok((elements, distances))
				},
				&mut builder,
			)
			.await?;
		}
		self.resolve_results(&ctx, builder, None, compaction_generation).await.map(Some)
	}

	/// Searches the compacted graph and adds visible candidate documents to the result builder.
	async fn search_graph(
		&self,
		ctx: &DiskAnnContext<'_>,
		state: DiskAnnGraphSearch<'_, '_>,
	) -> Result<()> {
		// With a truthy filter (a SELECT permission or a residual condition) the
		// walk is filtered: `graph.search_filtered` offers every element it reaches
		// to the filter, together with the allow-list and pending gates, and
		// returns the admitted ones, so a selective filter finds admitted documents
		// that rank far beyond the `l` nearest overall. The walk's length is still
		// set by `l`, so where it admits fewer than k a wider list walks further.
		//
		// With an allow-list alone this is a *gated post-filter*: `graph.search`
		// walks unaware of the bitmap and returns the top-`l` candidates overall,
		// so a selective allow-list can leave fewer than k admissible docs in that
		// list even when the graph holds plenty.
		//
		// Without either, the top-`l` elements can still name fewer than k
		// records: the builder holds one slot per record, and a record indexing
		// several vectors is reached through each of them.
		//
		// In every case, over-query: retry with a doubled search list until the
		// builder holds k records, the graph is exhausted, the builder's distance
		// gate closes (every remaining candidate is farther than the k already
		// held), or the retry ceiling is reached. A retry offers again the docs an
		// earlier pass admitted: the builder keeps each at its nearest offer, so a
		// wider list that reaches a closer element of one moves it there. `admitted`
		// only spares prefetching their records; the filter's verdict cache spares
		// evaluating them again.
		let mut l = state.search.l;
		let retry_ceiling = l.saturating_mul(OVERQUERY_CEILING_FACTOR);
		let mut admitted: HashSet<u64> = HashSet::default();
		loop {
			// A truthy filter (a SELECT permission or a residual condition) is
			// applied inside the walk, which returns only admitted elements; the
			// pass below then admits their documents from the filter's verdict
			// cache. Without one, the walk is unfiltered and the pass below is the
			// only gate.
			let (results, graph_exhausted, resolved) = match state.filter.as_mut() {
				Some(filter) => {
					let mut classifier = GraphCandidateClassifier {
						index: self,
						ctx,
						filter,
						pending_docs: state.pending_docs.as_ref(),
						allow_list: state.allow_list,
						builder: state.builder,
						admitted: KnnResultBuilder::new(state.search.k),
						docs: HashMap::default(),
					};
					let walk = state
						.graph
						.search_filtered(
							ctx,
							&state.search.query,
							state.search.k,
							l,
							&mut classifier,
						)
						.await?;
					(walk.results, walk.exhausted, Some(classifier.docs))
				}
				None => {
					let results =
						state.graph.search(ctx, &state.search.query, state.search.k, l).await?;
					// Fewer results than requested means the traversal ran dry: a
					// wider list cannot surface anything new.
					let exhausted = results.len() < l;
					(results, exhausted, None)
				}
			};
			let mut gate_closed = false;
			// Keep the distances returned by graph search instead of re-reading each vector only to
			// recompute the same score. The remaining vector reads are only needed to resolve
			// vector-to-document keys.
			let candidates: Vec<_> = results
				.into_iter()
				.map(|(element_id, distance)| (element_id, self.graph_distance(distance)))
				.filter(|(_, distance)| state.builder.check_add(*distance))
				.collect();
			if !candidates.is_empty() {
				// Resolve candidate graph elements to document id sets before applying
				// pending-update suppression and optional truthy filtering. Warm doc-set cache
				// hits avoid re-reading the graph vector; misses fetch only the missing vectors
				// before falling back to Dq/Dh mappings.
				let mut docs = match resolved {
					// The filtered walk resolved the document set of every element
					// it admitted.
					Some(mut resolved) => candidates
						.into_iter()
						.map(|(id, distance)| (id, distance, resolved.remove(&id)))
						.collect(),
					None => {
						self.vec_docs
							.get_docs_by_element_batch(
								&ctx.tx,
								&ctx.provider_context.view,
								&candidates,
							)
							.await?
					}
				};
				// Candidates come back distance-ascending from the graph search; sort
				// defensively (NaN-safe via `total_cmp`, a no-op when already ordered) so
				// the `break` on a closed `check_add` gate below stays sound even if the
				// upstream ordering contract ever changes. Keep the sort and the `break`s
				// together — the early-exit is only valid because the list is sorted.
				docs.sort_by(|a, b| a.1.total_cmp(&b.1));
				// Prefetch candidate records in distance-ascending windows that grow
				// geometrically. Each window warms the transaction record cache with one
				// multi-get, so the per-candidate `get_record` calls in the eval pass
				// become cache hits instead of individual round-trips. Windowing bounds
				// the over-fetch: the eval pass tightens `check_add` as the result builder
				// fills, and once the gate closes the remaining (farther) candidates are
				// never fetched. A non-selective filter fills the builder inside the first
				// window and stops there; a selective filter (the builder rarely fills)
				// walks the whole list in O(log n) windows — a handful of multi-gets, far
				// fewer round-trips than one fetch per candidate. The per-candidate eval
				// body is unchanged (only iterated by reference), so results are identical.
				let mut idx = 0usize;
				let mut window =
					(*surrealdb_cnf::DISKANN_FILTER_PREFETCH_MIN_CHUNK).max(state.search.k).max(1);
				'windows: while idx < docs.len() {
					let end = idx.saturating_add(window).min(docs.len());
					let slice = &docs[idx..end];
					// Warm this window's filter-eligible records in a single multi-get.
					if let Some(filter) = state.filter.as_mut() {
						let mut prefetch_ids: Vec<VectorId> = Vec::new();
						for (_, distance, docs) in slice {
							// Sorted ascending: a closed gate stays closed, so stop.
							if !state.builder.check_add(*distance) {
								break;
							}
							let Some(docs) = docs else {
								continue;
							};
							for doc_id in docs.iter() {
								if state
									.pending_docs
									.as_ref()
									.is_some_and(|pending| pending.contains(doc_id))
								{
									continue;
								}
								// Bitmap-rejected candidates are never evaluated by the
								// filter, so their records must not be prefetched either;
								// neither are docs already admitted by an earlier
								// over-query pass.
								if state.allow_list.is_some_and(|allow| !allow.contains(doc_id))
									|| admitted.contains(&doc_id)
								{
									continue;
								}
								prefetch_ids.push(VectorId::DocId(doc_id));
							}
						}
						filter.prefetch_records(ctx, &prefetch_ids).await?;
					}
					// Evaluate this window against the now-warm cache.
					for (_, distance, docs) in slice {
						// Sorted ascending: once the gate closes, every later candidate
						// (here and in all later windows) fails — stop entirely.
						if !state.builder.check_add(*distance) {
							gate_closed = true;
							break 'windows;
						}
						let Some(docs) = docs else {
							continue;
						};
						for doc_id in docs.iter() {
							if state
								.pending_docs
								.as_ref()
								.is_some_and(|pending| pending.contains(doc_id))
							{
								continue;
							}
							// #548: allow-list admission — a doc outside the bitmap is
							// rejected before any record fetch. DiskANN admission is
							// per-doc (unlike HNSW's per-element queue), so this gate
							// alone keeps disallowed sibling docs of a shared vector out
							// of the result builder.
							if state.allow_list.is_some_and(|allow| !allow.contains(doc_id)) {
								continue;
							}
							let id = VectorId::DocId(doc_id);
							if let Some(filter) = state.filter.as_mut()
								&& !filter.check_vector_id_truthy(ctx, id.clone()).await?
							{
								continue;
							}
							admitted.insert(doc_id);
							if let Some(evicted_id) =
								state.builder.add_vector_id_result(*distance, id)
								&& let Some(filter) = state.filter.as_mut()
							{
								filter.expire(&evicted_id);
							}
						}
					}
					idx = end;
					window = window
						.saturating_mul(2)
						.min(*surrealdb_cnf::DISKANN_FILTER_PREFETCH_MAX_CHUNK);
				}
			}
			// Stop as soon as retrying cannot improve the result set.
			if state.builder.len() >= state.search.k
				|| gate_closed
				|| graph_exhausted
				|| l >= retry_ceiling
			{
				return Ok(());
			}
			l = l.saturating_mul(2).min(retry_ceiling);
		}
	}

	/// Scores every live pending vector against the query and returns the
	/// suppression bitmap of doc-IDs seen anywhere in the pending keyspace, which
	/// the graph search uses to hide the stale entries those pendings supersede.
	///
	/// # Residency
	///
	/// The whole pending queue is read, but what the scan holds at any moment is
	/// one cursor page plus the candidates read from it and not yet scored,
	/// bounded by [`PendingScan`]: at most `PENDING_MAX_BATCH_KEYS` buffered
	/// entries, at most `PENDING_MAX_ROWS` entries in a page, and page and batch
	/// together sized for `PENDING_MAX_BYTES` — a target the page sizer converges
	/// to rather than a ceiling, for the reason given on
	/// [`crate::trees::pending`]. Candidates are buffered, scored into the result
	/// builder, and dropped before the scan advances; the page they came from
	/// shares the budget because it stays borrowed while they are scored. A deep
	/// queue therefore costs more batches, not more memory — which matters because
	/// this cost is paid per concurrent search, and the queue is deepest exactly
	/// when bulk writes are outrunning compaction.
	///
	/// Two things are deliberately *not* bounded, because bounding them would
	/// change results rather than only memory:
	///
	/// * The suppression bitmap is complete. It is a [`RoaringTreemap`] of doc-IDs and stays small
	///   even for a deep queue, and dropping an id from it would resurrect that document's stale
	///   graph vector.
	/// * Every live pending vector is scored. Truncating the scan would drop candidates that can
	///   win a top-K slot, silently degrading recall.
	///
	/// # Coalescing
	///
	/// Both pending layouts are record-keyed and a record maps to exactly one `!dw`
	/// shard, so each range holds at most one pending state per record and every state read
	/// is live on its own — with a single exception. One record can carry an entry
	/// in both layouts, which a mixed-version cluster produces when a pre-sharding
	/// node writes `!dr` for a record an upgraded node already wrote `!dw` for. The
	/// legacy entry is then the later write, and supersedes the sharded one whether
	/// it replaces the record's vectors or deletes them.
	///
	/// That conflict is resolved from the legacy side: the `!dr` range is scanned
	/// first and owns every record it holds, and a shard scan yields any record the
	/// legacy range also holds. Ownership is resolved one cursor page at a time —
	/// see [`Self::legacy_owned_rows`] — and gated twice so it costs nothing
	/// outside the migration window: once on the legacy range holding anything at
	/// all, and once on the shard being one that a legacy record actually hashes
	/// to.
	async fn search_pendings(
		&self,
		ctx: &DiskAnnContext<'_>,
		search: &DiskAnnSearch,
		filter: &mut Option<DiskAnnTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
		pending_state: &[Option<DiskAnnPendingState>],
	) -> Result<Option<RoaringTreemap>> {
		let mut scan = DiskAnnPendingScan {
			search,
			filter,
			allow_list,
			builder,
			// One of these across every range read, so only the scan's first page
			// is sized before any entry size is known, the cancellation checks
			// keep their back-off schedule over the whole scan, and the backlog
			// report fires on what the whole scan has read.
			pending: PendingScan::new(
				"diskann",
				&self.ikb,
				&self.pending_backlog_reported,
				&self.pending_scan_stats,
			),
			suppressed: RoaringTreemap::new(),
			legacy_shards: 0,
		};
		// Phase 1: the legacy range, which owns every record it holds. Nothing
		// writes it, so on an index that has drained it this is a single empty
		// range probe and `legacy_shards` stays clear.
		let rng = self.ikb.new_dr_range()?;
		self.scan_pending_range(ctx, &mut scan, rng, PendingLayout::Legacy, false).await?;
		// Phase 2: the sharded ranges of every shard that may hold data. A write
		// commits its `!dw` key only once its shard's `!dy` guard reads `NonEmpty`,
		// and no guard is stepped to `Empty` over a committed entry
		// (`mark_pending_non_empty`, within the engine limits it documents); a shard
		// this transaction wrote into reads `NonEmpty` here before its commit arms it.
		// A shard that is absent or confirmed `Empty` therefore holds no `!dw` entry
		// this lookup can see. Skipping those shards keeps the scan proportional
		// to the un-compacted backlog of the few active shards rather than the whole
		// index.
		for shard in 0..DISKANN_PENDING_STATE_SHARDS {
			let state = pending_state.get(usize::from(shard)).and_then(|state| state.as_ref());
			if state.is_none_or(|state| state.kind == DiskAnnPendingStateKind::Empty) {
				continue;
			}
			let probe_legacy = scan.legacy_shards & (1u32 << u32::from(shard)) != 0;
			let rng = self.ikb.new_dw_shard_range(shard)?;
			self.scan_pending_range(ctx, &mut scan, rng, PendingLayout::Sharded, probe_legacy)
				.await?;
		}
		self.score_pending_batch(ctx, &mut scan).await?;
		scan.pending.finish();
		if scan.suppressed.is_empty() {
			return Ok(None);
		}
		Ok(Some(scan.suppressed))
	}

	/// Streams one pending-update range into the scan, scoring each batch of
	/// candidates as it fills so that one cursor page and one batch of vectors are
	/// the whole of the scan's residency.
	///
	/// `layout` selects the key decoding and, for the legacy range, records which
	/// shards its records hash to. `probe_legacy` is set for a `!dw` shard that at
	/// least one legacy record hashes to: each page of that shard resolves which of
	/// its records the legacy range also holds, and yields ownership for those.
	async fn scan_pending_range(
		&self,
		ctx: &DiskAnnContext<'_>,
		scan: &mut DiskAnnPendingScan<'_, '_>,
		rng: TypedRange<DiskAnnRecordPendingUpdate>,
		layout: PendingLayout,
		probe_legacy: bool,
	) -> Result<()> {
		let mut cursor = ctx.tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let read = cursor.next_batch(scan.pending.rows()).await?;
			// The page stays borrowed while the candidates it fills are scored, so
			// it shares the residency budget with them. An empty page — which is
			// what ends every range — releases the charge.
			scan.pending.observe_page(&read);
			if read.is_empty() {
				break;
			}
			// Ownership for the whole page, resolved before any of its entries are
			// examined. Empty for a shard the legacy range cannot hold records of.
			let legacy_owned = if probe_legacy {
				// Score what the batch already holds first. The reply carries up
				// to one legacy value per row of this page, so answering it with
				// a full batch resident would put page, batch, request and reply live
				// together; scoring first leaves only page, request and reply. The cost is
				// a scoring batch per page instead of per budget, and only while
				// the legacy range still holds records of this shard.
				self.score_pending_batch(ctx, scan).await?;
				self.legacy_owned_rows(ctx, &read, &scan.pending).await?
			} else {
				Vec::new()
			};
			for (row, (key, value)) in read.iter().enumerate() {
				// The entry's bytes go to the first of its records the batch keeps;
				// the others it holds add none.
				let mut entry_bytes = key.len() + value.len();
				// Charged before the checkpoint, so an entry the scan has read is
				// accounted — and reported, if it crosses — before an expired
				// deadline can bail out of the scan.
				scan.pending.charge_entry(entry_bytes);
				if ctx.env.is_done(Some(scan.pending.entries())).await? {
					bail!(EngineError::QueryCancelled)
				}
				// One candidate per record the entry holds; only the first can
				// predate the id field, and so need its key to name it.
				for (record, mut pending) in DiskAnnRecordPendingUpdate::kv_decode_value(value, ())?
					.into_records()
					.enumerate()
				{
					let first = record == 0;
					let id = pending_record_id(key, layout, &mut pending)?;
					// The suppression bitmap covers every doc-ID the queue mentions,
					// superseded entries included: the graph entry such an id masks is
					// stale either way.
					if let Some(doc_id) = pending.doc_id {
						scan.suppressed.insert(doc_id);
					}
					match layout {
						// A legacy record can collide only with the one shard it hashes
						// to, so recording the shard is enough to spare every other
						// shard the resolution below.
						PendingLayout::Legacy => {
							scan.legacy_shards |= 1u32 << u32::from(Self::pending_state_shard(&id));
						}
						// A legacy entry stands for the record the row names first, the
						// one a release writing that layout knew of.
						PendingLayout::Sharded if probe_legacy => {
							if first && legacy_owned[row] {
								continue;
							}
						}
						PendingLayout::Sharded => {}
					}
					if pending.new_vectors.is_empty() {
						continue;
					}
					// Drop candidates outside the allow-list before they take batch
					// budget, so non-members are never fetched. The suppression bitmap
					// above is deliberately NOT gated: a pending doc must mask its stale
					// graph entries whether or not it is allowed.
					if !self.allow_list_admits(&ctx.tx, scan.allow_list, &id).await? {
						continue;
					}
					// Every pending candidate is scored under its record key rather than
					// under the doc-ID the entry captured. A record deleted and
					// re-created before compaction leaves that captured id without a
					// shared mapping, so a DocId-keyed hit could not be resolved back to
					// a record and would be silently dropped; the record key is always
					// resolvable, and the captured id still masks the graph's stale
					// entries through the bitmap above.
					let bytes = std::mem::take(&mut entry_bytes);
					if scan.pending.rollover_required(bytes) {
						self.score_pending_batch(ctx, scan).await?;
					}
					scan.pending.push(
						VectorId::RecordKey(Arc::new(id)),
						pending.new_vectors,
						bytes,
					);
				}
			}
		}
		Ok(())
	}

	/// Resolves, in one round trip, which rows of a `!dw` page the legacy `!dr`
	/// range also holds. Row `i` of the result answers row `i` of the page.
	///
	/// Asked once per cursor page rather than once per record. A record-at-a-time
	/// existence check makes every entry of every shard a legacy record hashes to
	/// cost a serial point read, and a migration backlog large enough to reach all
	/// [`DISKANN_PENDING_STATE_SHARDS`] shards makes that every sharded entry in
	/// the queue — turning a lookup made of sequential range scans into one point
	/// read per pending record, which is what remote key-value backends charge
	/// most for. Batching leaves the shard gating in place and reduces the cost of
	/// a shard that survives it to one round trip per page.
	///
	/// The reply and encoded request each carry at most one item per row, so they
	/// are bounded like the page that produced them; the reply is reduced to one
	/// flag per row before any page entry is read. The caller scores the pending
	/// batch before asking, so page, request, and reply are the only pending
	/// allocations live while the lookup is answered.
	async fn legacy_owned_rows(
		&self,
		ctx: &DiskAnnContext<'_>,
		read: &ValsBatch<'_>,
		pending: &PendingScan<'_>,
	) -> Result<Vec<bool>> {
		debug_assert!(
			pending.batch.is_empty(),
			"the pending batch is scored before an ownership reply is read, so only the page, \
			 request, and reply are live while the reply is"
		);
		// Move each decoded ID straight into its legacy key. `getm_raw` consumes
		// these typed keys as it encodes them, so the page is accompanied by one
		// request-key representation rather than retained ID and encoded-key copies.
		let keys = read
			.iter()
			.map(|(key, _)| {
				let key = DiskannRecordPendingShardKey::decode_key(key)?;
				Ok(DiskannRecordPendingKey {
					ns: key.ns,
					db: key.db,
					tb: key.tb,
					ix: key.ix,
					id: key.id,
				})
			})
			.collect::<Result<Vec<_>>>()?;
		let found = ctx.tx.getm_raw(keys, None).await?;
		let reply_bytes: usize = found.iter().flatten().map(|value| value.len()).sum();
		// A legacy `!dr` request key is the corresponding `!dw` page key minus
		// its shard field, so the page's key bytes are a conservative charge for
		// the encoded request without retaining or re-encoding that request here.
		pending.record_side_read(
			read.len(),
			pending.batch.bytes()
				+ pending.batch.page_bytes()
				+ read.key_bytes as usize
				+ reply_bytes,
		);
		Ok(found.into_iter().map(|value| value.is_some()).collect())
	}

	/// Returns whether a pending candidate is admitted by the allow-list.
	///
	/// Membership is over the table's shared doc-ID space and every pending
	/// candidate is record-keyed, so the record resolves its *current* doc-ID
	/// first. A record with no doc-ID in that space has no entries in any
	/// doc-ID-carrying index, so the index-covered conjuncts cannot hold for it. No
	/// allow-list admits everything.
	async fn allow_list_admits(
		&self,
		tx: &Transaction,
		allow_list: Option<&RoaringTreemap>,
		record: &RecordIdKey,
	) -> Result<bool> {
		let Some(allow) = allow_list else {
			return Ok(true);
		};
		Ok(match DiskAnnDocs::get_doc_id(&self.ikb, self.format_version, tx, record).await? {
			Some(doc_id) => allow.contains(doc_id),
			None => false,
		})
	}

	/// Scores one buffered batch of pending candidates and empties it.
	///
	/// The records behind the batch are warmed in a single multi-get before the
	/// per-candidate truthy checks, so those checks read the transaction cache
	/// instead of fetching one record at a time.
	///
	/// A candidate that takes no top-K slot has its filter-cache entry released on
	/// the way out, so the cache the search carries grows with `k` rather than with
	/// the length of the queue — the filter keeps a full record per truthy
	/// candidate and otherwise drops one only when the result builder evicts it.
	/// Releasing an entry costs at most a re-evaluation, and never happens to a
	/// candidate that holds a slot: the ids reaching this function are distinct
	/// record keys, and the graph search that follows keys its own candidates by
	/// doc-ID, so no released verdict is asked for again.
	async fn score_pending_batch(
		&self,
		ctx: &DiskAnnContext<'_>,
		scan: &mut DiskAnnPendingScan<'_, '_>,
	) -> Result<()> {
		if scan.pending.batch.is_empty() {
			return Ok(());
		}
		scan.pending.begin_scoring();
		if let Some(filter) = scan.filter.as_mut() {
			filter.prefetch_records(ctx, &scan.pending.batch.ids).await?;
		}
		let batch = &mut scan.pending.batch;
		for (id, vectors) in batch.ids.drain(..).zip(batch.vectors.drain(..)) {
			let truthy = match scan.filter.as_mut() {
				Some(filter) => filter.check_vector_id_truthy(ctx, id.clone()).await?,
				None => true,
			};
			if truthy {
				// A document indexing several vectors takes one slot, at its
				// nearest, as the exact tier offers a compacted one.
				let nearest = vectors
					.into_iter()
					.map(|vector| {
						self.distance.calculate_against(&scan.search.pt, &Vector::from(vector))
					})
					.min_by(f64::total_cmp);
				if let Some(d) = nearest
					&& scan.builder.check_add(d)
					&& let Some(evicted_id) = scan.builder.add_vector_id_result(d, id.clone())
					&& let Some(filter) = scan.filter.as_mut()
				{
					filter.expire(&evicted_id);
				}
			}
			if let Some(filter) = scan.filter.as_mut()
				&& !scan.builder.contains(&id)
			{
				filter.expire(&id);
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::borrow::Cow;
	use std::collections::BTreeMap;

	use rand::rngs::SmallRng;
	use rand::{Rng, SeedableRng};
	use surrealdb_datastore::is_retryable_transaction_conflict;
	use surrealdb_kvs::TransactionType;
	#[cfg(feature = "kv-rocksdb")]
	use temp_dir::TempDir;
	use tracing::info;
	use uuid::Uuid;

	use super::*;
	use crate::catalog::{
		DOC_IDS_FORMAT_VERSION, DatabaseId, IndexId, NamespaceId,
		VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
	};
	use crate::docids::TableDocIds;
	use crate::test_env::{CancelAtCheckpoint, TestIndexEnv, TestIndexStore};
	use crate::trees::diskann::cache::DiskAnnCache;
	use crate::trees::diskann::{DiskAnnElement, DiskAnnNode, DiskAnnState};
	use crate::trees::pending::{
		PENDING_MAX_BATCH_KEYS, PENDING_MAX_BYTES, PENDING_MAX_PAGE_BYTES, PENDING_MAX_ROWS,
		PENDING_PROBE_ROWS,
	};

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	fn cache() -> DiskAnnCache {
		DiskAnnCache::new(1024 * 1024)
	}

	fn params(vector_type: VectorType, distance: Distance) -> DiskAnnParams {
		wide_params(4, vector_type, distance)
	}

	/// The same parameters at an arbitrary dimension. Width is what decides
	/// whether the byte budget or the row cap sizes a pending cursor page.
	fn wide_params(dimension: u16, vector_type: VectorType, distance: Distance) -> DiskAnnParams {
		DiskAnnParams {
			dimension,
			distance,
			vector_type,
			degree: 16,
			l_build: 32,
			alpha: 1.2.into(),
			use_hashed_vector: false,
		}
	}

	fn diskann_pending_state(kind: DiskAnnPendingStateKind) -> DiskAnnPendingState {
		DiskAnnPendingState {
			kind,
			generation: 0,
		}
	}

	fn diskann_empty_pending_states() -> PendingStateSnapshot {
		(0..DISKANN_PENDING_STATE_SHARDS)
			.map(|_| Some(diskann_pending_state(DiskAnnPendingStateKind::Empty)))
			.collect()
	}

	fn diskann_compaction_plan(
		pending_state: PendingStateSnapshot,
		captured_keys: Vec<CapturedPendingKey>,
	) -> DiskAnnCompactionPlan {
		DiskAnnCompactionPlan {
			generation: None,
			pending_state,
			captured_keys,
			pending: Vec::new(),
			cleared_shards: Vec::new(),
			has_more: false,
		}
	}

	async fn new_ctx(ds: &TestIndexStore, tt: TransactionType) -> TestIndexEnv {
		// The environment carries the shared table-level doc-ID sequence, which
		// compaction's `resolve` needs.
		ds.env(tt).await
	}

	async fn diskann_pending_states(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<Vec<Option<DiskAnnPendingState>>> {
		let keys: Vec<_> =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|shard| ikb.new_dy_key(shard)).collect();
		tx.get_many_key(keys, None).await
	}

	fn diskann_any_pending_state_non_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().flatten().any(|state| state.kind == DiskAnnPendingStateKind::NonEmpty)
	}

	/// True when no pending-state shard is non-empty. Untouched (`None`) shards are ignored: with
	/// per-shard clearing only the shards that actually held data are advanced to `Empty`.
	fn diskann_all_pending_states_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().flatten().all(|state| state.kind == DiskAnnPendingStateKind::Empty)
	}

	fn f32_value(values: &[f32]) -> Value {
		Value::from(values.iter().map(|v| Value::from(*v as f64)).collect::<Vec<_>>())
	}

	fn f32_content(values: &[f32]) -> Vec<Value> {
		vec![f32_value(values)]
	}

	fn f32_pending(values: &[f32]) -> DiskAnnRecordPendingUpdate {
		DiskAnnRecordPendingUpdate {
			doc_id: None,
			old_vectors: vec![],
			new_vectors: vec![SerializedVector::F32(values.to_vec())],
			id: None,
			others: Vec::new(),
		}
	}

	/// The sharded `!dw` key a write for `id` lands on, for tests asserting on persisted pending
	/// records (the write path stores under this key, not the legacy `!dr` key).
	fn dw_key<'a>(ikb: &'a IndexKeyBase, id: &'a RecordIdKey) -> DiskannRecordPendingShardKey<'a> {
		ikb.new_dw_key(DiskAnnIndex::pending_state_shard(id), id)
	}

	fn f32_query(values: &[f32]) -> Vec<Number> {
		values.iter().map(|v| Number::from(*v)).collect()
	}

	async fn knn_len_with_k(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		values: &[f32],
		k: usize,
	) -> Result<usize> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let query = f32_query(values);
		let res = index.knn_search(&ctx, &query, k, 8, None, None).await?;
		ctx.tx().cancel().await?;
		Ok(res.len())
	}

	async fn knn_len(index: &DiskAnnIndex, ds: &TestIndexStore, values: &[f32]) -> Result<usize> {
		knn_len_with_k(index, ds, values, 1).await
	}

	/// Distance of the nearest neighbour to `values`, or `None` when the index returns nothing.
	async fn knn_nearest(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		values: &[f32],
	) -> Result<Option<f64>> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let query = f32_query(values);
		let res = index.knn_search(&ctx, &query, 1, 8, None, None).await?;
		ctx.tx().cancel().await?;
		Ok(res.front().map(|(_, dist, _)| *dist))
	}

	/// #548 test helper: runs a KNN search with an optional allow-list and
	/// returns the sorted numeric record keys of the results.
	async fn knn_keys_with_allow(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		values: &[f32],
		k: usize,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<Vec<i64>> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let query = f32_query(values);
		let res = index.knn_search(&ctx, &query, k, 8, None, allow_list).await?;
		ctx.tx().cancel().await?;
		let mut keys: Vec<i64> = res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		keys.sort();
		Ok(keys)
	}

	#[cfg(test)]
	async fn new_index(
		ikb: IndexKeyBase,
		table_id: TableId,
		params: &DiskAnnParams,
		cache: DiskAnnCache,
	) -> Result<DiskAnnIndex> {
		DiskAnnIndex::new(ikb, table_id, params, cache, DOC_IDS_FORMAT_VERSION).await
	}

	/// #548: the DiskANN graph tier post-filters `graph.search`'s top-`l`
	/// list, so when every near candidate is outside the allow-list the
	/// search must over-query (bounded list doubling) instead of returning
	/// fewer than k rows while admissible vectors exist farther out.
	#[tokio::test]
	async fn diskann_allow_list_overqueries_past_disallowed_neighbours() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		// Records 1..=30 cluster around the query point but are all outside
		// the allow-list; records 31..=40 are allowed but strictly farther,
		// beyond the initial l=8 candidate list.
		let mut allow = RoaringTreemap::new();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 1..=40i64 {
				let id = RecordIdKey::Number(i);
				let doc = docids.resolve_or_assign(&ctx, &id).await?;
				let v = if i <= 30 {
					i as f32
				} else {
					1_000.0 + i as f32
				};
				if i > 30 {
					allow.insert(doc);
				}
				index.index(&ctx, &id, None, Some(f32_content(&[v, v, v, v]))).await?;
			}
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}
		// k=5 with l=8: the top-8 overall are all disallowed, so a single
		// gated pass would return nothing — the over-query retries must reach
		// the allowed cluster and return the 5 nearest allowed records.
		let res = knn_keys_with_allow(&index, &ds, &[0.0f32; 4], 5, Some(&allow)).await?;
		assert_eq!(res, vec![31, 32, 33, 34, 35]);
		Ok(())
	}

	/// #548: allow-list admission for DiskANN — gates pending updates, the
	/// compacted graph (per doc, including the disallowed sibling of a shared
	/// vector), and admits nothing on an empty allow-list.
	#[tokio::test]
	async fn diskann_allow_list_restricts_admission() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		// Records 1 and 2 share one vector; records 3 and 4 are distinct.
		// Each record's shared doc-ID is resolved at write time, as the
		// document pipeline does through any doc-ID-carrying sibling index.
		let vectors: [[f32; 4]; 4] = [
			[7.0, 7.0, 7.0, 7.0],
			[7.0, 7.0, 7.0, 7.0],
			[1.0, 1.0, 1.0, 1.0],
			[2.0, 2.0, 2.0, 2.0],
		];
		let mut docs = Vec::new();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for (i, v) in vectors.iter().enumerate() {
				let id = RecordIdKey::Number(i as i64 + 1);
				docs.push(docids.resolve_or_assign(&ctx, &id).await?);
				index.index(&ctx, &id, None, Some(f32_content(v))).await?;
			}
			ctx.tx().commit().await?;
		}
		let query = [0.0f32; 4];
		let allow = RoaringTreemap::from_iter([docs[0], docs[2]]);
		// Pending phase: only allow-members are admitted.
		assert_eq!(knn_keys_with_allow(&index, &ds, &query, 4, Some(&allow)).await?, vec![1, 3]);
		// Compacted phase: per-doc gating also holds for the graph — record 2
		// shares its vector with admitted record 1 but must not surface.
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(knn_keys_with_allow(&index, &ds, &query, 4, Some(&allow)).await?, vec![1, 3]);
		// No allow-list: unrestricted. Empty allow-list: nothing.
		assert_eq!(knn_keys_with_allow(&index, &ds, &query, 4, None).await?, vec![1, 2, 3, 4]);
		let empty = RoaringTreemap::new();
		assert!(knn_keys_with_allow(&index, &ds, &query, 4, Some(&empty)).await?.is_empty());
		Ok(())
	}

	/// A search reads the graph as of its own snapshot, even when a compaction
	/// commits between the search's transaction opening and the search. The
	/// snapshot still holds the records' pendings and none of the doc-IDs the
	/// compaction assigned, so every record must come back exactly once, from
	/// its pending.
	#[tokio::test]
	async fn diskann_search_reads_the_graph_as_of_its_snapshot() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 0..5_i64 {
				let v = 10.0 * i as f32;
				index
					.index(
						&ctx,
						&RecordIdKey::Number(i),
						None,
						Some(f32_content(&[v, 0.0, 0.0, 0.0])),
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}
		let query = new_ctx(&ds, TransactionType::Read).await;
		index.check_state().await?;
		while compact_once(&index, &ds, &ikb).await? {}
		let pt = f32_query(&[0.0, 0.0, 0.0, 0.0]);
		let res = index.knn_search(&query, &pt, 5, 8, None, None).await?;
		query.tx().cancel().await?;
		let keys: Vec<i64> = res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		assert_eq!(keys, vec![0, 1, 2, 3, 4], "every record, nearest first: {res:?}");
		Ok(())
	}

	/// A search reads the graph as of its own snapshot when a compaction on the same node has since
	/// deleted a vector the snapshot still holds. The compaction wrote the tombstone through at the
	/// generation it committed, which is newer than the reader's, so the reader is not served it
	/// and still finds the record at its old vector.
	#[tokio::test]
	async fn diskann_search_behind_a_local_commit_is_not_served_its_tombstones() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let at = |x: f32| [x, 0.0, 0.0, 0.0];
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 0..5_i64 {
				let v = at(10.0 * i as f32);
				index.index(&ctx, &RecordIdKey::Number(i), None, Some(f32_content(&v))).await?;
			}
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}
		let query = new_ctx(&ds, TransactionType::Read).await;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let (old, new) = (at(20.0), at(500.0));
			index
				.index(
					&ctx,
					&RecordIdKey::Number(2),
					Some(f32_content(&old)),
					Some(f32_content(&new)),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(
			knn_keys_in(&index, &query, &at(20.0), 1).await?,
			vec![2],
			"the reader's snapshot holds record 2 at the queried point"
		);
		query.tx().cancel().await?;
		assert_eq!(knn_keys_with_allow(&index, &ds, &at(500.0), 1, None).await?, vec![2]);
		Ok(())
	}

	async fn compact_once(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
	) -> Result<bool> {
		let plan = {
			let ctx = new_ctx(ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let ctx = new_ctx(ds, TransactionType::Write).await;
		// `apply_compaction` commits the tx itself when it reports `Applied` and
		// cancels it otherwise, so the test must not double-commit.
		Ok(index.apply_compaction(&ctx, plan).await?.applied())
	}

	/// Drains every pending update through prepare/apply until a prepared plan
	/// asks for no write phase at all.
	///
	/// One prepare pass is capped at [`DISKANN_COMPACTION_MAX_PENDING_KEYS`] keys
	/// and [`DISKANN_COMPACTION_MAX_PENDING_BYTES`] bytes, so a corpus wider than
	/// one batch needs several passes. Returns the number of
	/// passes that captured pending keys, which is what a caller asserts on when
	/// it cares that the drain was batched. The pass cap turns a drain that fails
	/// to converge into a failure rather than a hang.
	async fn compact_all(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
	) -> Result<usize> {
		const MAX_PASSES: usize = 4096;
		let mut applied = 0;
		for _ in 0..MAX_PASSES {
			let plan = {
				let ctx = new_ctx(ds, TransactionType::Read).await;
				let plan = DiskAnnIndex::prepare_compaction(&ctx, ikb).await?;
				ctx.tx().cancel().await?;
				plan
			};
			if !plan.requires_apply() {
				return Ok(applied);
			}
			let had_work = plan.has_work();
			let ctx = new_ctx(ds, TransactionType::Write).await;
			// `apply_compaction` owns the write transaction's outcome.
			index.apply_compaction(&ctx, plan).await?;
			if had_work {
				applied += 1;
			}
		}
		bail!("DiskANN compaction did not drain within {MAX_PASSES} passes")
	}

	/// The doc set `index` has cached for `element_id`, as a reader at `generation` is served it.
	async fn cached_doc_ids(
		index: &DiskAnnIndex,
		cache: &DiskAnnCache,
		ikb: &IndexKeyBase,
		element_id: ElementId,
		generation: Option<u64>,
	) -> Option<Vec<u64>> {
		let view = index.graph.read().await.reader_view(generation);
		cache
			.get_doc_set(
				(ikb.ns(), ikb.db(), TableId(4), ikb.index(), DOC_IDS_FORMAT_VERSION),
				element_id,
				&view,
			)
			.map(|docs| docs.iter().collect())
	}

	#[test]
	fn diskann_compaction_plan_requires_apply_for_captured_keys() {
		let plan = diskann_compaction_plan(
			diskann_empty_pending_states(),
			vec![CapturedPendingKey {
				key: vec![0],
				value: vec![1],
			}],
		);

		assert!(plan.has_work());
		assert!(plan.requires_apply());
	}

	#[test]
	fn diskann_compaction_plan_skips_apply_when_empty_confirmed() {
		let plan = diskann_compaction_plan(diskann_empty_pending_states(), Vec::new());

		assert!(!plan.has_work());
		assert!(!plan.requires_apply());
	}

	#[test]
	fn diskann_compaction_plan_requires_apply_only_for_non_empty_shards() {
		// A Some(non-Empty) shard still needs an apply pass (to drain its `!dw` or step its guard).
		let mut maybe_empty = diskann_empty_pending_states();
		maybe_empty[0] = Some(diskann_pending_state(DiskAnnPendingStateKind::MaybeEmpty));

		let mut non_empty = diskann_empty_pending_states();
		non_empty[0] = Some(diskann_pending_state(DiskAnnPendingStateKind::NonEmpty));

		for pending_state in [maybe_empty, non_empty] {
			let plan = diskann_compaction_plan(pending_state, Vec::new());
			assert!(!plan.has_work());
			assert!(plan.requires_apply());
		}

		// Untouched (`None`) shards are NOT applyable: a quiescent index — whether some shards are
		// `None` among `Empty` ones, or every shard is `None` — must not schedule no-op applies.
		let mut missing = diskann_empty_pending_states();
		missing[0] = None;
		let all_none: PendingStateSnapshot =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|_| None).collect();
		for pending_state in [missing, all_none] {
			let plan = diskann_compaction_plan(pending_state, Vec::new());
			assert!(!plan.has_work());
			assert!(!plan.requires_apply());
		}
	}

	#[tokio::test]
	async fn diskann_accepts_supported_vector_types_and_distances() -> Result<()> {
		for (vector_type, distance) in [
			(VectorType::F32, Distance::Euclidean),
			(VectorType::F16, Distance::CosineNormalized),
			(VectorType::U8, Distance::InnerProduct),
			(VectorType::I8, Distance::Euclidean),
		] {
			new_index(ikb(), TableId(4), &params(vector_type, distance), cache()).await?;
		}
		Ok(())
	}

	#[tokio::test]
	async fn diskann_rejects_unsupported_type_metric_combinations() -> Result<()> {
		assert!(
			new_index(ikb(), TableId(4), &params(VectorType::I16, Distance::Euclidean), cache())
				.await
				.is_err()
		);
		assert!(
			new_index(
				ikb(),
				TableId(4),
				&params(VectorType::U8, Distance::CosineNormalized),
				cache()
			)
			.await
			.is_err()
		);
		assert!(
			new_index(
				ikb(),
				TableId(4),
				&params(VectorType::I8, Distance::CosineNormalized),
				cache()
			)
			.await
			.is_err()
		);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_graph_distance_matches_public_euclidean_distance() -> Result<()> {
		let index =
			new_index(ikb(), TableId(4), &params(VectorType::F32, Distance::Euclidean), cache())
				.await?;
		assert_eq!(index.graph_distance(9.0), 3.0);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_doc_set_cache_evicted_and_refilled_for_duplicate_vector_updates() -> Result<()>
	{
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = cache();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache.clone(),
		)
		.await?;
		let first_id = RecordIdKey::Number(1);
		let second_id = RecordIdKey::Number(2);
		let vector = [1.0, 2.0, 3.0, 4.0];

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &first_id, None, Some(f32_content(&vector))).await?;
			index.index(&ctx, &second_id, None, Some(f32_content(&vector))).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert!(cached_doc_ids(&index, &cache, &ikb, 0, Some(1)).await.is_none());

		assert_eq!(knn_len(&index, &ds, &vector).await?, 1);
		assert_eq!(cached_doc_ids(&index, &cache, &ikb, 0, Some(1)).await, Some(vec![0, 1]));

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &first_id, Some(f32_content(&vector)), None).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert!(cached_doc_ids(&index, &cache, &ikb, 0, Some(2)).await.is_none());

		assert_eq!(knn_len(&index, &ds, &vector).await?, 1);
		assert_eq!(cached_doc_ids(&index, &cache, &ikb, 0, Some(2)).await, Some(vec![1]));

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second_id, Some(f32_content(&vector)), None).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert!(cached_doc_ids(&index, &cache, &ikb, 0, Some(3)).await.is_none());
		assert_eq!(knn_len(&index, &ds, &vector).await?, 0);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_index_write_marks_pending_state_non_empty() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let id = RecordIdKey::Number(1);

		index.index(&ctx, &id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;

		let pending: DiskAnnRecordPendingUpdate =
			tx.get_key(&dw_key(&ikb, &id), None).await?.unwrap();
		// The writing transaction reads its shard as holding entries, while the
		// stored guard waits for the commit to arm it.
		let shard = DiskAnnIndex::pending_state_shard(&id) as usize;
		let states = DiskAnnIndex::read_pending_state(&tx, &ikb).await?;
		assert_eq!(
			states[shard].as_ref().map(|state| state.kind),
			Some(DiskAnnPendingStateKind::NonEmpty)
		);
		assert!(diskann_pending_states(&tx, &ikb).await?[shard].is_none());
		assert!(pending.old_vectors.is_empty());
		assert_eq!(pending.new_vectors, vec![SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0])]);

		index
			.index(
				&ctx,
				&id,
				Some(f32_content(&[1.0, 2.0, 3.0, 4.0])),
				Some(f32_content(&[4.0, 3.0, 2.0, 1.0])),
			)
			.await?;
		let pending: DiskAnnRecordPendingUpdate =
			tx.get_key(&dw_key(&ikb, &id), None).await?.unwrap();
		assert_eq!(pending.new_vectors, vec![SerializedVector::F32(vec![4.0, 3.0, 2.0, 1.0])]);
		tx.commit().await?;
		// The commit arms the guard once, however many writes named the shard.
		let ctx = new_ctx(&ds, TransactionType::Read).await;
		let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard].clone().unwrap();
		ctx.tx().cancel().await?;
		assert_eq!(state.kind, DiskAnnPendingStateKind::NonEmpty);
		assert_eq!(state.generation, 1);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_lookup_skips_sharded_pendings_only_when_guard_is_empty() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let shard = DiskAnnIndex::pending_state_shard(&id);

		// Seed a sharded `!dw` pending entry and mark its `!dy` guard shard non-empty.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(&ikb.new_dw_key(shard, &id), &f32_pending(&[1.0, 2.0, 3.0, 4.0])).await?;
			tx.set_key(
				&ikb.new_dy_key(shard),
				&DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 1,
				},
			)
			.await?;
			tx.commit().await?;
		}
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		// MaybeEmpty still scans the shard.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dy_key(shard),
					&DiskAnnPendingState {
						kind: DiskAnnPendingStateKind::MaybeEmpty,
						generation: 2,
					},
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		// Empty skips the shard's `!dw` scan, so the pending vector is no longer visible.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dy_key(shard),
					&DiskAnnPendingState {
						kind: DiskAnnPendingStateKind::Empty,
						generation: 3,
					},
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 0);
		Ok(())
	}

	/// Mixed-version rolling upgrade: a new node writes a sharded `!dw` record (bumping the `!dy`
	/// guard), then a pre-change node's compactor clears every legacy `!dp` guard shard (it scans
	/// only `!dr`, sees it empty, and has no knowledge of `!dw`/`!dy`). Because sharded visibility
	/// is gated on `!dy`, the record must stay visible. Before decoupling the guards, the `!dw`
	/// scan was gated on `!dp`, so this clear hid the record.
	#[tokio::test]
	async fn diskann_old_compactor_clearing_legacy_guard_keeps_sharded_visible() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);

		// New node writes a sharded record; this bumps the `!dy` guard, never `!dp`.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		// A pre-change node's compactor clears every legacy `!dp` guard shard to Empty.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for s in 0..DISKANN_PENDING_STATE_SHARDS {
				ctx.tx()
					.set_key(
						&ikb.new_dp_key(s),
						&DiskAnnPendingState {
							kind: DiskAnnPendingStateKind::Empty,
							generation: 1,
						},
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}

		// The record is still visible: the `!dw` scan is gated on `!dy`, which the old compactor
		// never touched.
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);
		Ok(())
	}

	/// A DiskANN index holding `n` uncompacted sharded pendings: record key `i`
	/// carries vector `[i, 0, 0, 0]` for `i` in `1..=n`. Nothing reaches the graph,
	/// so a search over it reads the whole queue.
	///
	/// Record `i` hashes to shard `i % 32` and the scan walks the shards in order,
	/// so scan position is not record order — which is what lets the tests below
	/// place a query's neighbours in different materialisation batches.
	async fn new_pending_backlog_fixture(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
		n: i64,
	) -> Result<DiskAnnIndex> {
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let ctx = new_ctx(ds, TransactionType::Write).await;
		for i in 1..=n {
			index
				.index(
					&ctx,
					&RecordIdKey::Number(i),
					None,
					Some(f32_content(&[i as f32, 0.0, 0.0, 0.0])),
				)
				.await?;
		}
		ctx.tx().commit().await?;
		Ok(index)
	}

	/// A pending queue whose retained identity bytes live primarily in the
	/// record keys rather than the vector values.
	async fn new_large_key_pending_backlog_fixture(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
		n: i64,
		key_payload_bytes: usize,
	) -> Result<DiskAnnIndex> {
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let ctx = new_ctx(ds, TransactionType::Write).await;
		index
			.index(&ctx, &RecordIdKey::Number(0), None, Some(f32_content(&[0.0, 0.0, 0.0, 0.0])))
			.await?;
		let suffix = "x".repeat(key_payload_bytes);
		for i in 1..=n {
			let id = RecordIdKey::String(format!("{i:04}-{suffix}").into());
			index.index(&ctx, &id, None, Some(f32_content(&[i as f32, 0.0, 0.0, 0.0]))).await?;
		}
		ctx.tx().commit().await?;
		Ok(index)
	}

	/// A KNN search holds at most one cursor page and one materialisation batch of
	/// pending vectors at a time, however deep the queue is. Pinned as exact
	/// accounting: a queue of a known length scores in a known number of batches,
	/// no batch exceeds the entry budget, no page exceeds its byte share, and page
	/// and batch together stay inside the residency budget.
	#[tokio::test]
	async fn diskann_pending_scan_materialises_bounded_batches() -> Result<()> {
		// Two full batches plus a remainder, so the counts below pin the budget and
		// the split of a queue across batches, not just that some batching
		// happened.
		const PENDING: i64 = PENDING_MAX_BATCH_KEYS as i64 * 2 + 7;
		const EXPECTED_BATCHES: usize = 3;
		// The fixture's 4-D f32 vectors are orders of magnitude smaller than the
		// byte budget, so the entry budget alone decides where a batch ends and the
		// batch count above is exact.
		const _: () = assert!(PENDING_MAX_BYTES > PENDING_MAX_BATCH_KEYS * 1024);

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_pending_backlog_fixture(&ds, &ikb, PENDING).await?;

		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 4, None).await?,
			vec![1, 2, 3, 4]
		);

		let stats = index.pending_scan_stats();
		assert_eq!(
			stats.batches(),
			EXPECTED_BATCHES,
			"a {PENDING}-entry queue scores in {EXPECTED_BATCHES} batches"
		);
		assert_eq!(
			stats.peak_batch_entries(),
			PENDING_MAX_BATCH_KEYS,
			"no batch holds more than the entry budget"
		);
		assert!(
			stats.peak_batch_bytes() < PENDING_MAX_BYTES,
			"the byte budget must not bind for this fixture: {} bytes",
			stats.peak_batch_bytes()
		);
		assert!(
			stats.peak_page_entries() <= PENDING_MAX_ROWS as usize,
			"no page holds more than the row cap: {} entries",
			stats.peak_page_entries()
		);
		assert!(
			stats.peak_page_bytes() <= PENDING_MAX_PAGE_BYTES,
			"no page holds more than its byte share: {} bytes",
			stats.peak_page_bytes()
		);
		assert!(
			stats.peak_resident_bytes() <= PENDING_MAX_BYTES,
			"page and batch together stay inside the residency budget: {} bytes",
			stats.peak_resident_bytes()
		);
		assert_eq!(stats.side_reads(), 0, "an empty legacy range costs no ownership resolution");
		Ok(())
	}

	/// Large user record IDs remain buffered as `VectorId::RecordKey` values
	/// after the cursor page that supplied them is released. Their encoded key
	/// bytes must therefore roll the scoring batch even when the vector values
	/// themselves and the entry count fit in one batch.
	#[tokio::test]
	async fn diskann_pending_scan_accounts_for_record_key_bytes() -> Result<()> {
		const PENDING: i64 = 600;
		const KEY_PAYLOAD_BYTES: usize = 8 * 1024;
		const _: () = assert!(PENDING as usize + 1 < PENDING_MAX_BATCH_KEYS);
		const _: () = assert!(PENDING as usize * KEY_PAYLOAD_BYTES > PENDING_MAX_BYTES);

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index =
			new_large_key_pending_backlog_fixture(&ds, &ikb, PENDING, KEY_PAYLOAD_BYTES).await?;

		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![0]
		);

		let stats = index.pending_scan_stats();
		assert!(
			stats.batches() > 1,
			"record key bytes alone must split the queue into several scoring batches"
		);
		assert!(
			stats.peak_batch_bytes() <= PENDING_MAX_BYTES,
			"retained record-key bytes stay inside the scoring-batch budget: {} bytes",
			stats.peak_batch_bytes(),
		);
		Ok(())
	}

	/// A DiskANN index holding `n` uncompacted sharded pendings whose vectors are
	/// `dimension` wide, all landing in the same shard: record key
	/// `i * DISKANN_PENDING_STATE_SHARDS` carries `[i, 0, ...]` for `i` in
	/// `1..=n`.
	///
	/// One shard is what makes the scan's paging a single sequence rather than one
	/// per shard, and the width is what decides whether the byte budget or the row
	/// cap sizes each page.
	async fn new_wide_pending_backlog_fixture(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
		dimension: u16,
		n: i64,
	) -> Result<DiskAnnIndex> {
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&wide_params(dimension, VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let ctx = new_ctx(ds, TransactionType::Write).await;
		for i in 1..=n {
			let mut values = vec![0.0f32; usize::from(dimension)];
			values[0] = i as f32;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(i * i64::from(DISKANN_PENDING_STATE_SHARDS)),
					None,
					Some(vec![f32_value(&values)]),
				)
				.await?;
		}
		ctx.tx().commit().await?;
		Ok(index)
	}

	/// Wide vectors shrink the cursor pages instead of the residency bound.
	///
	/// A page is requested by entry count, so a fixed count would let entries this
	/// wide materialise several times the budget in the cursor's arena, where they
	/// stay resident for as long as the scan holds the page. Pinned as exact
	/// accounting: the row cap does not bind, the byte share does, and page and
	/// batch together stay inside the residency budget.
	#[tokio::test]
	async fn diskann_pending_scan_pages_shrink_for_wide_vectors() -> Result<()> {
		// Wide enough that a full-size page of these entries would not fit the page
		// budget, and deep enough that the byte budget — not the entry budget — is
		// what ends a scoring batch.
		const DIMENSION: u16 = 16_384;
		const PENDING: i64 = 64;

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_wide_pending_backlog_fixture(&ds, &ikb, DIMENSION, PENDING).await?;

		let query = vec![0.0f32; usize::from(DIMENSION)];
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 2, None).await?,
			vec![
				i64::from(DISKANN_PENDING_STATE_SHARDS),
				2 * i64::from(DISKANN_PENDING_STATE_SHARDS)
			]
		);

		let stats = index.pending_scan_stats();
		let entry_bytes = stats.peak_page_bytes().div_ceil(stats.peak_page_entries());
		assert!(
			entry_bytes * PENDING_MAX_ROWS as usize > PENDING_MAX_PAGE_BYTES,
			"the fixture must be wide enough for a full-size page to overrun the budget: \
			 {entry_bytes} bytes per entry"
		);
		assert!(
			stats.peak_page_entries() < PENDING_MAX_ROWS as usize,
			"the byte share, not the row cap, sized the pages: {} entries",
			stats.peak_page_entries()
		);
		assert!(
			stats.peak_page_bytes() <= PENDING_MAX_PAGE_BYTES,
			"no page holds more than its byte share: {} bytes",
			stats.peak_page_bytes()
		);
		assert!(
			stats.peak_batch_entries() < PENDING_MAX_BATCH_KEYS,
			"the byte budget, not the entry budget, ended a batch: {} entries",
			stats.peak_batch_entries()
		);
		assert!(
			stats.peak_resident_bytes() <= PENDING_MAX_BYTES,
			"page and batch together stay inside the residency budget: {} bytes",
			stats.peak_resident_bytes()
		);
		// The empty legacy range costs one page. The one populated shard costs a
		// probe page, pages of the observed byte-derived size for everything the
		// probe left, and the empty page that ends it.
		let after_probe = PENDING as usize - PENDING_PROBE_ROWS as usize;
		let shard_pages = 1 + after_probe.div_ceil(stats.peak_page_entries()) + 1;
		assert_eq!(
			stats.pages(),
			1 + shard_pages,
			"every page after the probe is the size the byte share allows"
		);
		Ok(())
	}

	/// A pending queue whose entries widen partway through, all in one shard:
	/// `narrow` records carrying one `dimension`-wide vector each, then `wide`
	/// records carrying `vectors_per_wide_record` of them.
	///
	/// A shard's range is scanned in record order, so the narrow prefix is what
	/// the scan measures its first pages from and the wide tail is what those
	/// pages then have to hold. Record key `i * DISKANN_PENDING_STATE_SHARDS`
	/// carries `[i, 0, ...]`, so the nearest neighbours to the origin are the head
	/// of the queue whichever half they fall in.
	async fn new_widening_pending_backlog_fixture(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
		dimension: u16,
		narrow: i64,
		wide: i64,
		vectors_per_wide_record: usize,
	) -> Result<DiskAnnIndex> {
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&wide_params(dimension, VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let ctx = new_ctx(ds, TransactionType::Write).await;
		for i in 1..=(narrow + wide) {
			let mut values = vec![0.0f32; usize::from(dimension)];
			values[0] = i as f32;
			let copies = if i <= narrow {
				1
			} else {
				vectors_per_wide_record
			};
			index
				.index(
					&ctx,
					&RecordIdKey::Number(i * i64::from(DISKANN_PENDING_STATE_SHARDS)),
					None,
					Some(vec![f32_value(&values); copies]),
				)
				.await?;
		}
		ctx.tx().commit().await?;
		Ok(index)
	}

	/// An abrupt increase in entry width cannot restore the ordinary 500-row
	/// cursor amplification.
	///
	/// The narrow probe sizes the next page at the pending scan's conservative
	/// row cap. The entries then widen enough that an ordinary 500-row cursor page
	/// would exceed the whole residency target, but the pending page stays inside
	/// its byte share because it can hold only sixteen. Results are unaffected.
	#[tokio::test]
	async fn diskann_pending_scan_caps_pages_when_entries_widen() -> Result<()> {
		// A narrow entry small enough that the row cap, not the byte target, sizes
		// the page after the probe...
		const DIMENSION: u16 = 256;
		const NARROW: i64 = PENDING_PROBE_ROWS as i64;
		// ...and a wide entry large enough that the engine-wide 500-row default
		// would overrun the whole residency target.
		const VECTORS_PER_WIDE_RECORD: usize = 10;
		// Several conservatively capped pages pin that the narrow prefix cannot
		// increase the request beyond the pending-specific cap.
		const WIDE: i64 = PENDING_MAX_ROWS as i64 * 3 + 5;

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_widening_pending_backlog_fixture(
			&ds,
			&ikb,
			DIMENSION,
			NARROW,
			WIDE,
			VECTORS_PER_WIDE_RECORD,
		)
		.await?;

		let shard = i64::from(DISKANN_PENDING_STATE_SHARDS);
		let query = vec![0.0f32; usize::from(DIMENSION)];
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 4, None).await?,
			vec![shard, 2 * shard, 3 * shard, 4 * shard],
			"the conservative page cap changes residency, not results"
		);

		let stats = index.pending_scan_stats();
		assert_eq!(
			stats.peak_page_entries(),
			PENDING_MAX_ROWS as usize,
			"the narrow prefix sized the next page at the row cap"
		);
		let wide_entry_bytes = stats.peak_page_bytes().div_ceil(PENDING_MAX_ROWS as usize);
		assert!(
			wide_entry_bytes * surrealdb_kvs::consts::NORMAL_BATCH_SIZE as usize
				> PENDING_MAX_BYTES,
			"the ordinary row cap must recreate the allocation this regression guards: \
			 {wide_entry_bytes} bytes per entry"
		);
		assert!(
			stats.peak_page_bytes() <= PENDING_MAX_PAGE_BYTES,
			"the pending-specific row cap keeps the widening page inside its share: {} bytes",
			stats.peak_page_bytes()
		);
		assert!(
			stats.peak_resident_bytes() <= PENDING_MAX_BYTES,
			"page and batch stay inside the residency target: {} bytes",
			stats.peak_resident_bytes()
		);
		// The empty legacy range costs one page. The populated shard costs the
		// probe, capped pages for the wide tail, and its empty page.
		assert_eq!(
			stats.pages(),
			1 + 1 + (WIDE as usize).div_ceil(PENDING_MAX_ROWS as usize) + 1,
			"the widening tail stays split by the conservative row cap"
		);
		Ok(())
	}

	/// The backlog report arms on a scoring rollover, from inside the scan, for a
	/// queue whose raw bytes are well inside the residency budget.
	///
	/// A batch shares that budget with the cursor page it is filled from, so a
	/// queue of wide vectors costs several materialisation batches long before its
	/// raw values add up to [`PENDING_MAX_BYTES`]. A report gated on a raw byte
	/// total would say nothing about it. Deriving the crossing from the rollover
	/// itself is what makes the two agree. Draining the queue re-arms the report
	/// for the next crossing, so a sustained backlog stays one record rather than
	/// one per query.
	#[tokio::test]
	async fn diskann_pending_backlog_report_arms_on_a_scoring_rollover() -> Result<()> {
		// Wide enough that page residency rolls a batch over...
		const DIMENSION: u16 = 12_000;
		const PENDING: i64 = 80;
		// ...while the entry count stays inside the entry budget, so the entry
		// count alone cannot be what arms the report.
		const _: () = assert!(PENDING as usize <= PENDING_MAX_BATCH_KEYS);

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_wide_pending_backlog_fixture(&ds, &ikb, DIMENSION, PENDING).await?;
		assert!(!index.pending_backlog_reported(), "no scan has read the queue yet");

		let query = vec![0.0f32; usize::from(DIMENSION)];
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 1, None).await?,
			vec![i64::from(DISKANN_PENDING_STATE_SHARDS)]
		);
		let stats = index.pending_scan_stats();
		assert!(
			stats.batches() > 1,
			"the fixture must cost more than one batch to score: {} batches",
			stats.batches()
		);
		// Keys included, the whole queue is inside the residency budget: a
		// threshold over raw bytes could not have armed this report.
		let entry_bytes = stats.peak_page_bytes().div_ceil(stats.peak_page_entries());
		assert!(
			stats.entries_read() * entry_bytes < PENDING_MAX_BYTES,
			"the queue's raw bytes must stay inside the budget: {} entries of {entry_bytes} bytes",
			stats.entries_read()
		);
		assert!(
			index.pending_backlog_reported(),
			"a queue that costs several materialisation batches arms the report"
		);

		// Drain the queue behind the index's back: every record of this fixture
		// lands in the one shard, and nothing was ever compacted, so removing that
		// shard's range leaves an empty index. The `!dy` guard still says the shard
		// may hold data, so the scan still opens it.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx().delr(ikb.new_dw_shard_range(0)?).await?;
			ctx.tx().commit().await?;
		}
		assert!(knn_keys_with_allow(&index, &ds, &query, 1, None).await?.is_empty());
		assert!(
			!index.pending_backlog_reported(),
			"a scan that completes inside both budgets re-arms the report"
		);
		Ok(())
	}

	/// A scan cancelled at a checkpoint has already accounted for the entry that
	/// checkpoint belongs to.
	///
	/// The cancellation schedule deep-checks on entry counts that bracket the
	/// entry-count crossing, so a loop that checked before charging would return
	/// `QueryCancelled` out of the very scan the backlog report exists to
	/// explain, having read the crossing entry but never accounted for it. Pinned
	/// as exact accounting: cancelling at the checkpoint of the entry that
	/// crosses leaves that entry charged and the report armed.
	#[tokio::test]
	async fn diskann_pending_scan_reports_the_entry_it_is_cancelled_on() -> Result<()> {
		// One entry past the entry budget is the crossing; the queue is deeper so
		// the scan is stopped by the checkpoint rather than by the range end.
		const CROSSING: usize = PENDING_MAX_BATCH_KEYS + 1;
		const PENDING: i64 = PENDING_MAX_BATCH_KEYS as i64 * 2;

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_pending_backlog_fixture(&ds, &ikb, PENDING).await?;
		assert!(!index.pending_backlog_reported(), "no scan has read the queue yet");

		// The empty legacy range costs no checkpoints, so the scan's
		// `CROSSING`-th checkpoint is the crossing entry's own.
		let ctx = CancelAtCheckpoint::new(ds.env(TransactionType::Read).await, CROSSING);
		let query = f32_query(&[0.0, 0.0, 0.0, 0.0]);
		let err = index
			.knn_search(&ctx, &query, 4, 8, None, None)
			.await
			.expect_err("the scan is cancelled before it finishes reading the queue");
		assert!(
			matches!(err.downcast_ref::<EngineError>(), Some(EngineError::QueryCancelled)),
			"unexpected error: {err}"
		);
		ctx.tx().cancel().await?;

		let stats = index.pending_scan_stats();
		assert_eq!(
			stats.entries_read(),
			CROSSING,
			"the entry the scan is cancelled on is charged before the checkpoint"
		);
		assert!(
			index.pending_backlog_reported(),
			"the crossing is reported before the deadline bails out of the scan"
		);
		Ok(())
	}

	/// A byte-based rollover is reported before cancellation on the entry that
	/// would force it, even while the queue remains below the entry threshold.
	#[tokio::test]
	async fn diskann_pending_scan_reports_byte_rollover_before_cancellation() -> Result<()> {
		const DIMENSION: u16 = 256;
		const PENDING: i64 = 2;
		const VECTORS_PER_RECORD: usize = 1536;
		const CANCEL_AT: usize = 2;
		const _: () = assert!((PENDING as usize) < PENDING_MAX_BATCH_KEYS);

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_widening_pending_backlog_fixture(
			&ds,
			&ikb,
			DIMENSION,
			0,
			PENDING,
			VECTORS_PER_RECORD,
		)
		.await?;
		assert!(!index.pending_backlog_reported(), "no scan has read the queue yet");

		let ctx = CancelAtCheckpoint::new(ds.env(TransactionType::Read).await, CANCEL_AT);
		let query_values = vec![0.0f32; usize::from(DIMENSION)];
		let query = f32_query(&query_values);
		let err = index
			.knn_search(&ctx, &query, 1, 8, None, None)
			.await
			.expect_err("the second entry cancels before the batch can be scored");
		assert!(
			matches!(err.downcast_ref::<EngineError>(), Some(EngineError::QueryCancelled)),
			"unexpected error: {err}"
		);
		ctx.tx().cancel().await?;

		assert_eq!(index.pending_scan_stats().entries_read(), CANCEL_AT);
		assert!(
			index.pending_backlog_reported(),
			"the byte rollover must be reported before its cancellation checkpoint"
		);
		Ok(())
	}

	/// Legacy ownership is resolved per cursor page, not per record.
	///
	/// Once any legacy record hashes to a shard, every entry of that shard has to
	/// be checked against the legacy range. A record-at-a-time existence check
	/// makes that one serial point read per pending record; resolving a page at a
	/// time makes it one round trip per page. Pinned as exact accounting: a shard
	/// holding many records resolves them in the number of round trips its pages
	/// take.
	#[tokio::test]
	async fn diskann_legacy_ownership_resolves_one_page_at_a_time() -> Result<()> {
		// Every record lands in shard 0, so one shard carries the whole backlog and
		// the page arithmetic below is a single sequence.
		const PENDING: i64 = PENDING_MAX_ROWS as i64 + 7;
		let shard = i64::from(DISKANN_PENDING_STATE_SHARDS);

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 1..=PENDING {
				index
					.index(
						&ctx,
						&RecordIdKey::Number(i * shard),
						None,
						Some(f32_content(&[i as f32, 0.0, 0.0, 0.0])),
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}
		// One legacy entry, which puts the shard every record lives in inside the
		// mask and so makes every one of them need resolving. Its record is parked
		// far from the query, so scoring the sharded entry instead would surface it.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dr_key(&RecordIdKey::Number(shard)),
					&f32_pending(&[9999.0, 0.0, 0.0, 0.0]),
				)
				.await?;
			ctx.tx().commit().await?;
		}

		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 2, None).await?,
			vec![2 * shard, 3 * shard],
			"the legacy entry owns its record, so the sharded entry for it is not scored"
		);

		let stats = index.pending_scan_stats();
		assert_eq!(
			stats.side_read_keys(),
			PENDING as usize,
			"every record of the masked shard is resolved"
		);
		// The preceding legacy phase warms the shared sizer to the row cap, so
		// ownership resolution starts with capped pages rather than another probe.
		// The empty page that ends the range asks about nothing.
		let expected_batches = (PENDING as usize).div_ceil(PENDING_MAX_ROWS as usize);
		assert_eq!(
			stats.side_reads(),
			expected_batches,
			"resolving them costs one round trip per page, not one per record"
		);
		let first_request_bytes: usize = (1..=PENDING_MAX_ROWS as i64)
			.map(|i| ikb.new_dr_key(&RecordIdKey::Number(i * shard)).encode_key().unwrap().len())
			.sum();
		assert!(
			stats.peak_resident_bytes() >= stats.peak_page_bytes() + first_request_bytes,
			"ownership residency includes the encoded lookup request: page={} request={} peak={}",
			stats.peak_page_bytes(),
			first_request_bytes,
			stats.peak_resident_bytes(),
		);
		assert!(
			stats.peak_resident_bytes() <= PENDING_MAX_BYTES,
			"the ownership request and reply land inside the residency budget: {} bytes",
			stats.peak_resident_bytes()
		);
		Ok(())
	}

	/// Splitting the pending queue into batches must not change which candidates
	/// win. The query's four neighbours hash to shards 30, 31, 0 and 1, which the
	/// scan reads in the second, third and first batch respectively, so a top-K
	/// kept per batch rather than across the whole scan would return a different
	/// set.
	#[tokio::test]
	async fn diskann_pending_scan_batching_preserves_top_k() -> Result<()> {
		const PENDING: i64 = PENDING_MAX_BATCH_KEYS as i64 * 2 + 7;
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_pending_backlog_fixture(&ds, &ikb, PENDING).await?;

		// Midway between records 2047 and 2048: the winners are the two records
		// either side of the query, then the two beyond them.
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[2047.5, 0.0, 0.0, 0.0], 4, None).await?,
			vec![2046, 2047, 2048, 2049]
		);
		assert_eq!(index.pending_scan_stats().batches(), 3);
		Ok(())
	}

	/// A queue below the entry budget scores in a single batch, and returns the
	/// same neighbours the batched scan returns for the same records.
	#[tokio::test]
	async fn diskann_pending_scan_below_budget_scores_in_one_batch() -> Result<()> {
		const SHALLOW: i64 = PENDING_MAX_BATCH_KEYS as i64 - 1;
		const DEEP: i64 = PENDING_MAX_BATCH_KEYS as i64 * 2 + 7;
		// A neighbourhood both fixtures hold, so the two answers are comparable.
		const QUERY: [f32; 4] = [511.5, 0.0, 0.0, 0.0];
		const EXPECTED: [i64; 4] = [510, 511, 512, 513];

		let ds = TestIndexStore::new().await;
		let shallow_ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3));
		let deep_ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let shallow = new_pending_backlog_fixture(&ds, &shallow_ikb, SHALLOW).await?;
		let deep = new_pending_backlog_fixture(&ds, &deep_ikb, DEEP).await?;

		assert_eq!(knn_keys_with_allow(&shallow, &ds, &QUERY, 4, None).await?, EXPECTED.to_vec());
		assert_eq!(
			shallow.pending_scan_stats().batches(),
			1,
			"a queue one entry short of the budget scores in a single batch"
		);
		assert_eq!(
			knn_keys_with_allow(&deep, &ds, &QUERY, 4, None).await?,
			EXPECTED.to_vec(),
			"batching the same records across three batches returns the same neighbours"
		);
		assert_eq!(deep.pending_scan_stats().batches(), 3);
		Ok(())
	}

	/// The suppression bitmap covers every document seen anywhere in the pending
	/// queue, whatever batch its vectors were scored in. The moved record hashes
	/// to the last shard and sorts after every filler in it, so its doc-ID enters
	/// the bitmap only after two batches have already been scored; without it the
	/// graph search would return that record at the position it no longer
	/// occupies.
	#[tokio::test]
	async fn diskann_pending_scan_masks_stale_graph_entries_across_batches() -> Result<()> {
		const FILLERS: i64 = PENDING_MAX_BATCH_KEYS as i64 * 2;
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let near = RecordIdKey::Number(9023);
		let winner = RecordIdKey::Number(9024);

		// Two committed records: the nearest to the origin, and the runner-up.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &near, None, Some(f32_content(&[1.0, 0.0, 0.0, 0.0]))).await?;
			index.index(&ctx, &winner, None, Some(f32_content(&[5.0, 0.0, 0.0, 0.0]))).await?;
			ctx.tx().commit().await?;
		}
		// Fold both into the graph, so what follows is the only pending work.
		for _ in 0..8 {
			if !compact_once(&index, &ds, &ikb).await? {
				break;
			}
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &near), None).await?.is_none());
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &winner), None).await?.is_none());
			ctx.tx().cancel().await?;
		}

		// A queue deep enough to span several batches, every entry farther from the
		// origin than the runner-up.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 1..=FILLERS {
				index
					.index(
						&ctx,
						&RecordIdKey::Number(i),
						None,
						Some(f32_content(&[10000.0 + i as f32, 0.0, 0.0, 0.0])),
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}
		// Move the committed nearest record away, leaving its graph entry stale.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&near,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
					Some(f32_content(&[30000.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}

		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![9024],
			"the moved record's stale graph entry must stay masked"
		);
		assert_eq!(
			index.pending_scan_stats().batches(),
			3,
			"the moved record is scored alone in the third batch"
		);
		Ok(())
	}

	/// A record carrying an entry in both pending layouts is owned by the legacy
	/// one, which a mixed-version cluster leaves as the later write. Its sharded
	/// entry is neither scored in place of the legacy one nor scored alongside it.
	#[tokio::test]
	async fn diskann_legacy_pending_supersedes_sharded_entry() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let first = RecordIdKey::Number(1);
		let second = RecordIdKey::Number(2);

		// Record 1 in both layouts: the sharded entry puts it nearest the origin,
		// the legacy entry farthest.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(&dw_key(&ikb, &first), &f32_pending(&[1.0, 0.0, 0.0, 0.0])).await?;
			tx.set_key(
				&ikb.new_dy_key(DiskAnnIndex::pending_state_shard(&first)),
				&DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 1,
				},
			)
			.await?;
			tx.set_key(&ikb.new_dr_key(&first), &f32_pending(&[9.0, 0.0, 0.0, 0.0])).await?;
			tx.commit().await?;
		}
		// Record 2 sits between the two, so which entry owns record 1 decides the
		// order.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second, None, Some(f32_content(&[5.0, 0.0, 0.0, 0.0]))).await?;
			ctx.tx().commit().await?;
		}

		// Scoring the sharded entry instead would hand the single slot to record 1.
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![2]
		);
		// Scoring it as well would return record 1 twice.
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 3, None).await?,
			vec![1, 2]
		);
		assert_eq!(
			index.pending_scan_stats().side_reads(),
			2,
			"one round trip per search, for the single shard a legacy record hashes to"
		);
		Ok(())
	}

	/// A legacy delete cancels the record's sharded entry. The legacy layout owns
	/// the record and its entry says the record is gone, so the superseded
	/// vectors must not resurface.
	#[tokio::test]
	async fn diskann_legacy_pending_delete_cancels_sharded_entry() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let first = RecordIdKey::Number(1);
		let second = RecordIdKey::Number(2);

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(&dw_key(&ikb, &first), &f32_pending(&[1.0, 0.0, 0.0, 0.0])).await?;
			tx.set_key(
				&ikb.new_dy_key(DiskAnnIndex::pending_state_shard(&first)),
				&DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 1,
				},
			)
			.await?;
			// The delete carries the doc-ID the record held when it was captured,
			// which is the shape that reaches the scan under a shared doc-ID space.
			tx.set_key(
				&ikb.new_dr_key(&first),
				&DiskAnnRecordPendingUpdate {
					doc_id: Some(7),
					old_vectors: vec![SerializedVector::F32(vec![1.0, 0.0, 0.0, 0.0])],
					new_vectors: vec![],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second, None, Some(f32_content(&[5.0, 0.0, 0.0, 0.0]))).await?;
			ctx.tx().commit().await?;
		}

		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 3, None).await?,
			vec![2],
			"the deleted record's sharded entry must not resurface"
		);
		Ok(())
	}

	/// The legacy ownership probe is gated twice: no probe at all while the legacy
	/// range is empty, and afterwards only for the shards a legacy record actually
	/// hashes to.
	#[tokio::test]
	async fn diskann_pending_scan_probes_only_shards_a_legacy_record_touches() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;

		// Three records, one per shard, all in the sharded layout.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in 1..=3 {
				index
					.index(
						&ctx,
						&RecordIdKey::Number(i),
						None,
						Some(f32_content(&[i as f32, 0.0, 0.0, 0.0])),
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 4, None).await?,
			vec![1, 2, 3]
		);
		assert_eq!(
			index.pending_scan_stats().side_read_keys(),
			0,
			"an empty legacy range costs no ownership resolution"
		);

		// One legacy entry, for the record in the third shard.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dr_key(&RecordIdKey::Number(3)),
					&f32_pending(&[7.0, 0.0, 0.0, 0.0]),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		// Record 3 now scores at 7.0, from its legacy entry alone.
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[0.0, 0.0, 0.0, 0.0], 4, None).await?,
			vec![1, 2, 3]
		);
		assert_eq!(
			index.pending_scan_stats().side_read_keys(),
			1,
			"only the one shard a legacy record hashes to is resolved"
		);
		assert_eq!(
			index.pending_scan_stats().side_reads(),
			1,
			"and that shard's single page costs one round trip"
		);
		Ok(())
	}
	#[tokio::test]
	async fn diskann_compaction_clears_pending_state_after_empty_confirmation() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());
		assert!(!plan.has_more());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			// apply_compaction commits the tx internally on `Applied`.
			assert_eq!(index.apply_compaction(&ctx, plan).await?, CompactionApply::Applied);
		}

		// On a sole-writer store the pass that drains the shard confirms it empty
		// under the guard fence, so one step takes the guard to `Empty`.
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_all_pending_states_empty(&states));
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}

		assert!(!compact_once(&index, &ds, &ikb).await?, "nothing is left to step");
		Ok(())
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test]
	async fn diskann_rocksdb_clear_race_keeps_concurrent_writer_visible() -> Result<()> {
		let dir = TempDir::new()?;
		let path = format!("rocksdb:{}", dir.path().to_string_lossy());
		let ds = TestIndexStore::with_path(&path).await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let first_id = RecordIdKey::Number(1);
		let second_id = RecordIdKey::Number(1 + i64::from(DISKANN_PENDING_STATE_SHARDS));
		assert_eq!(
			DiskAnnIndex::pending_state_shard(&first_id),
			DiskAnnIndex::pending_state_shard(&second_id)
		);

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &first_id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());
		assert!(!plan.has_more());

		let apply_ctx = new_ctx(&ds, TransactionType::Write).await;

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second_id, None, Some(f32_content(&[4.0, 3.0, 2.0, 1.0]))).await?;
			ctx.tx().commit().await?;
		}

		// apply_compaction commits the tx internally on `Applied`.
		assert_eq!(index.apply_compaction(&apply_ctx, plan).await?, CompactionApply::Applied);

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			let shard = DiskAnnIndex::pending_state_shard(&second_id) as usize;
			// The step after the apply reads the writer's entry, so it leaves the guard alone.
			assert_eq!(
				states[shard].as_ref().map(|state| state.kind),
				Some(DiskAnnPendingStateKind::NonEmpty)
			);
			assert!(ctx.tx().get_key(&dw_key(&ikb, &second_id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}

		assert_eq!(knn_len_with_k(&index, &ds, &[4.0, 3.0, 2.0, 1.0], 2).await?, 2);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_empty_compaction_plan_does_not_clear_concurrent_pending_write() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(!plan.has_work());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			// apply_compaction cancels the tx itself when it applies nothing.
			assert_eq!(index.apply_compaction(&ctx, plan).await?, CompactionApply::Empty);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_any_pending_state_non_empty(&states));
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[tokio::test]
	async fn diskann_final_compaction_plan_preserves_concurrent_pending_write() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let first_id = RecordIdKey::Number(1);
		let second_id = RecordIdKey::Number(2);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &first_id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());
		assert!(!plan.has_more());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second_id, None, Some(f32_content(&[4.0, 3.0, 2.0, 1.0]))).await?;
			ctx.tx().commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			// apply_compaction commits the tx internally on `Applied`.
			assert_eq!(index.apply_compaction(&ctx, plan).await?, CompactionApply::Applied);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_any_pending_state_non_empty(&states));
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &first_id), None).await?.is_none());
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &second_id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	/// Compaction passes that step a writer's shard to `Empty` while the writer
	/// is still open must not hide the writer's pending once it commits: search
	/// and compaction both skip a shard whose guard is `Empty`. The writer's commit
	/// covers the guard after those steps, and is never refused for them. Where
	/// other datastores write to the store, the shard empties once the earlier
	/// write's lease is released, and the writer's commit takes the lease again.
	#[tokio::test]
	async fn diskann_pending_written_while_its_shard_empties_stays_visible() -> Result<()> {
		diskann_pending_written_while_its_shard_empties_stays_visible_on(
			TestIndexStore::new().await,
		)
		.await
	}

	/// The body of [`diskann_pending_written_while_its_shard_empties_stays_visible`], on any store.
	async fn diskann_pending_written_while_its_shard_empties_stays_visible_on(
		ds: TestIndexStore,
	) -> Result<()> {
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Records 1 and 33 share a shard; record 1 marks it non-empty.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(1),
					None,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		let writer = new_ctx(&ds, TransactionType::Write).await;
		index
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		ds.release_writer_leases().await?;
		for _ in 0..3 {
			compact_once(&index, &ds, &ikb).await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let state = DiskAnnIndex::read_pending_state(&ctx.tx(), &ikb).await?;
			ctx.tx().cancel().await?;
			assert_eq!(
				state[shard as usize].as_ref().map(|state| state.kind),
				Some(DiskAnnPendingStateKind::Empty),
				"the shard must reach Empty while the writer is open for this to test anything"
			);
		}
		writer.tx().commit().await?;
		let query = [33.0, 0.0, 0.0, 0.0];
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 2, None).await?,
			vec![1, 33],
			"search must see the committed pending"
		);
		ds.release_writer_leases().await?;
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 2, None).await?,
			vec![1, 33],
			"compaction must fold the committed pending into the graph"
		);
		assert_eq!(ds.guard_repairs(), 0, "the covering protocol left no guard to repair");
		Ok(())
	}

	/// A lookup in the transaction that wrote a pending update sees it although
	/// the shard's stored guard reads `Empty`: on a sole-writer store the guard
	/// is armed only when that transaction commits.
	#[tokio::test]
	async fn diskann_lookup_in_the_writing_transaction_sees_its_pending_under_an_empty_guard()
	-> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Records 1 and 33 share a shard; record 1 is folded so its guard ends `Empty`.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(1),
					None,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		compact_all(&index, &ds, &ikb).await?;
		let writer = new_ctx(&ds, TransactionType::Write).await;
		assert_eq!(
			diskann_pending_states(&writer.tx(), &ikb).await?[shard as usize]
				.as_ref()
				.map(|state| state.kind),
			Some(DiskAnnPendingStateKind::Empty),
			"the stored guard must read Empty for this to test anything"
		);
		index
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		let query = f32_query(&[33.0, 0.0, 0.0, 0.0]);
		let found = index.knn_search(&writer, &query, 2, 8, None, None).await?;
		writer.tx().cancel().await?;
		let mut keys: Vec<i64> = found
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		keys.sort_unstable();
		assert_eq!(keys, vec![1, 33], "the writing transaction must see its own pending update");
		Ok(())
	}

	/// A writer that commits while the pass that empties its shard is open must
	/// not end up under an `Empty` guard. On a sole-writer store the pass's
	/// apply never writes the guard, and its step reads the guard and the range
	/// under the guard fence after the writer has armed it, so the writer's
	/// commit is never refused.
	#[tokio::test]
	async fn diskann_writer_committing_inside_the_emptying_pass_stays_visible() -> Result<()> {
		diskann_writer_committing_inside_the_emptying_pass_stays_visible_on(
			TestIndexStore::new().await,
		)
		.await
	}

	/// The body of [`diskann_writer_committing_inside_the_emptying_pass_stays_visible`], on any
	/// store.
	async fn diskann_writer_committing_inside_the_emptying_pass_stays_visible_on(
		ds: TestIndexStore,
	) -> Result<()> {
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Records 1 and 33 share a shard; record 1 marks it non-empty.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(1),
					None,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		let writer = new_ctx(&ds, TransactionType::Write).await;
		index
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		// The first pass drains record 1 and steps the guard to `Empty`.
		assert!(compact_once(&index, &ds, &ikb).await?);
		// The emptying pass plans and opens its apply before the writer commits.
		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let emptying = new_ctx(&ds, TransactionType::Write).await;
		writer.tx().commit().await?;
		// The pass then applies over what it planned; a refusal is equally correct.
		match index.apply_compaction(&emptying, plan).await {
			Ok(_) => {}
			Err(e) if is_retryable_transaction_conflict(&e) => {}
			Err(e) => return Err(e),
		}
		let query = [33.0, 0.0, 0.0, 0.0];
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 2, None).await?,
			vec![1, 33],
			"search must see the committed pending"
		);
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &query, 2, None).await?,
			vec![1, 33],
			"compaction must fold the committed pending into the graph"
		);
		assert_eq!(ds.guard_repairs(), 0, "the covering protocol left no guard to repair");
		Ok(())
	}

	/// Overlapping writers into a shard whose `!dy` guard already reads
	/// `NonEmpty` all commit: none of them writes the guard.
	#[tokio::test]
	async fn diskann_overlapping_writers_into_a_non_empty_shard_all_commit() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Records 1, 33 and 65 share a shard; record 1 marks it non-empty.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		for id in [33, 65] {
			assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(id)));
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(1),
					None,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		let guard = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let state = DiskAnnIndex::read_pending_state(&ctx.tx(), &ikb).await?;
			ctx.tx().cancel().await?;
			state[shard as usize].clone().expect("record 1 sets its shard's guard")
		};
		assert_eq!(guard.kind, DiskAnnPendingStateKind::NonEmpty);
		let first = new_ctx(&ds, TransactionType::Write).await;
		let second = new_ctx(&ds, TransactionType::Write).await;
		assert!(
			first.tx().sole_writer(),
			"the store must be one this datastore is the sole writer of for the fenced path"
		);
		index
			.index(
				&first,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		index
			.index(
				&second,
				&RecordIdKey::Number(65),
				None,
				Some(f32_content(&[65.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		first.tx().commit().await?;
		second.tx().commit().await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let state = DiskAnnIndex::read_pending_state(&ctx.tx(), &ikb).await?;
			ctx.tx().cancel().await?;
			assert_eq!(state[shard as usize], Some(guard), "neither writer rewrites the guard");
		}
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[33.0, 0.0, 0.0, 0.0], 3, None).await?,
			vec![1, 33, 65],
			"search must see both writers' pendings"
		);
		Ok(())
	}

	/// Writers into a shard whose guard reads `Empty` both arm it as they
	/// commit. Their arming transactions conflict only with each other, and the
	/// one that loses retries and finds the guard armed, so both commits land.
	#[tokio::test]
	async fn diskann_concurrent_writers_arming_an_empty_shard_both_commit() -> Result<()> {
		diskann_concurrent_writers_arming_an_empty_shard_both_commit_on(TestIndexStore::new().await)
			.await
	}

	/// The body of [`diskann_concurrent_writers_arming_an_empty_shard_both_commit`], on any store.
	async fn diskann_concurrent_writers_arming_an_empty_shard_both_commit_on(
		ds: TestIndexStore,
	) -> Result<()> {
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Records 1, 33 and 65 share a shard; record 1 is folded so its guard ends `Empty`.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		for id in [33, 65] {
			assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(id)));
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(1),
					None,
					Some(f32_content(&[1.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		ds.release_writer_leases().await?;
		compact_all(&index, &ds, &ikb).await?;
		let first = new_ctx(&ds, TransactionType::Write).await;
		let second = new_ctx(&ds, TransactionType::Write).await;
		for (ctx, id) in [(&first, 33), (&second, 65)] {
			index
				.index(
					ctx,
					&RecordIdKey::Number(id),
					None,
					Some(f32_content(&[id as f32, 0.0, 0.0, 0.0])),
				)
				.await?;
		}
		let (first_tx, second_tx) = (first.tx(), second.tx());
		let (a, b) = tokio::join!(first_tx.commit(), second_tx.commit());
		a?;
		b?;
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard as usize].clone();
			ctx.tx().cancel().await?;
			assert_eq!(state.map(|s| s.kind), Some(DiskAnnPendingStateKind::NonEmpty));
		}
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[33.0, 0.0, 0.0, 0.0], 3, None).await?,
			vec![1, 33, 65],
			"search must see both writers' pendings"
		);
		assert_eq!(ds.guard_repairs(), 0, "the covering protocol left no guard to repair");
		Ok(())
	}

	/// A guard armed for a commit that then fails is left `NonEmpty` over a range
	/// holding nothing of it. The arming queued a compaction pass for the index,
	/// so the failed commit taking its own queue entry with it does not leave the
	/// guard stuck: the pass returns it to `Empty`.
	#[tokio::test]
	async fn diskann_guard_armed_for_a_failed_commit_is_stepped_back() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		// Record 1 is folded, so its shard's guard ends `Empty`.
		let id = RecordIdKey::Number(1);
		let shard = DiskAnnIndex::pending_state_shard(&id) as usize;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[1.0, 0.0, 0.0, 0.0]))).await?;
			ctx.tx().commit().await?;
		}
		compact_all(&index, &ds, &ikb).await?;
		let queued = |tx: Arc<Transaction>| {
			let ikb = ikb.clone();
			async move {
				let range = crate::key::schema::IndexCompactionIxPrefix {
					ns: ikb.ns(),
					db: ikb.db(),
					tb: Cow::Borrowed(ikb.table()),
					ix: ikb.index(),
				}
				.range()?;
				let n = tx.keys(range, u32::MAX, 0, None).await?.len();
				tx.cancel().await?;
				Ok::<_, anyhow::Error>(n)
			}
		};
		let before = queued(new_ctx(&ds, TransactionType::Read).await.tx()).await?;
		// The writer also writes a key a second transaction commits first, so its
		// own commit is refused after its arming has committed.
		let contested = crate::key::Key::from(b"/7575-contested".as_slice());
		let writer = new_ctx(&ds, TransactionType::Write).await;
		index
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		writer.tx().set(contested.clone(), b"writer".to_vec()).await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx().set(contested, b"winner".to_vec()).await?;
			ctx.tx().commit().await?;
		}
		let refused = writer.tx().commit().await;
		assert!(
			refused.as_ref().is_err_and(is_retryable_transaction_conflict),
			"the writer's commit must lose the contested key: {refused:?}"
		);
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard].clone();
			ctx.tx().cancel().await?;
			assert_eq!(
				state.map(|s| s.kind),
				Some(DiskAnnPendingStateKind::NonEmpty),
				"the arming committed although the writer's commit did not"
			);
		}
		assert_eq!(
			queued(new_ctx(&ds, TransactionType::Read).await.tx()).await?,
			before + 1,
			"the arming queues a compaction pass for the index"
		);
		assert!(compact_once(&index, &ds, &ikb).await?);
		let ctx = new_ctx(&ds, TransactionType::Read).await;
		let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard].clone();
		ctx.tx().cancel().await?;
		assert_eq!(state.map(|s| s.kind), Some(DiskAnnPendingStateKind::Empty));
		Ok(())
	}

	/// On a store that validates every read, a transaction that read a shard's
	/// guard covers that guard in its own commit, whichever came first, the
	/// lookup or the write: another transaction's arming would invalidate the
	/// read and fail it. A guard covered that way goes with a commit that fails,
	/// where one the arming transaction covered outlives it. A write with no
	/// lookup is still covered by the arming transaction.
	#[tokio::test]
	async fn diskann_guard_read_on_a_validating_store_is_covered_by_its_own_commit() -> Result<()> {
		let ds = TestIndexStore::new().await.validating_node();
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33));
		// Writes record 33 in a transaction whose commit is refused, after or
		// before a lookup if `lookup` says so, and returns the shard's guard as
		// it stands afterwards.
		let refused_write = |lookup: Option<bool>| {
			let (ds, index, ikb) = (&ds, &index, ikb.clone());
			async move {
				let contested = crate::key::Key::from(b"/7575-validated".as_slice());
				let writer = new_ctx(ds, TransactionType::Write).await;
				if lookup == Some(true) {
					DiskAnnIndex::read_pending_state(&writer.tx(), &ikb).await?;
				}
				index
					.index(
						&writer,
						&RecordIdKey::Number(33),
						None,
						Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
					)
					.await?;
				if lookup == Some(false) {
					DiskAnnIndex::read_pending_state(&writer.tx(), &ikb).await?;
				}
				writer.tx().set(contested.clone(), b"writer".to_vec()).await?;
				{
					let ctx = new_ctx(ds, TransactionType::Write).await;
					ctx.tx().set(contested, b"winner".to_vec()).await?;
					ctx.tx().commit().await?;
				}
				let refused = writer.tx().commit().await;
				assert!(
					refused.as_ref().is_err_and(is_retryable_transaction_conflict),
					"the writer's commit must lose the contested key: {refused:?}"
				);
				stored_guard(ds, shard).await
			}
		};
		assert_eq!(refused_write(Some(true)).await?, None, "lookup, then write");
		assert_eq!(refused_write(Some(false)).await?, None, "write, then lookup");
		assert_eq!(
			refused_write(None).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"a write with no lookup is armed by a transaction of its own"
		);
		// A lookup, then a write, that commits leaves its guard `NonEmpty`.
		let other = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(2));
		assert_ne!(other, shard);
		let writer = new_ctx(&ds, TransactionType::Write).await;
		DiskAnnIndex::read_pending_state(&writer.tx(), &ikb).await?;
		index
			.index(&writer, &RecordIdKey::Number(2), None, Some(f32_content(&[2.0, 0.0, 0.0, 0.0])))
			.await?;
		writer.tx().commit().await?;
		assert_eq!(stored_guard(&ds, other).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		Ok(())
	}

	/// A wipe of the index's data removes its guards, so what the datastore knew
	/// of them as armed is forgotten as the wipe commits: the next write into a
	/// shard it had recorded arms the guard again, rather than leaving its
	/// pending update under an absent guard that lookup skips.
	#[tokio::test]
	async fn diskann_write_after_a_wipe_arms_the_guard_again() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = new_index(ikb.clone(), TableId(4), &p, cache()).await?;
		// Records 1 and 33 share a shard; record 1's commit arms its guard.
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		let guard = |ds: &TestIndexStore| {
			let ikb = ikb.clone();
			let ds = ds.clone();
			async move {
				let ctx = new_ctx(&ds, TransactionType::Read).await;
				let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard as usize].clone();
				ctx.tx().cancel().await?;
				Ok::<_, anyhow::Error>(state.map(|s| s.kind))
			}
		};
		for id in [1, 33] {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(id),
					None,
					Some(f32_content(&[id as f32, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(guard(&ds).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			crate::wipe_index_data(&ctx.tx(), &ikb, &crate::catalog::Index::DiskAnn(p.clone()))
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(guard(&ds).await?, None, "the wipe removes the guard");
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(65),
					None,
					Some(f32_content(&[65.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(
			guard(&ds).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"a write after the wipe must arm the guard again"
		);
		// A wipe and a write in one transaction: the guard is covered after the
		// wipe's delete, which would otherwise remove it as the commit lands.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			crate::wipe_index_data(&ctx.tx(), &ikb, &crate::catalog::Index::DiskAnn(p.clone()))
				.await?;
			index
				.index(
					&ctx,
					&RecordIdKey::Number(97),
					None,
					Some(f32_content(&[97.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(
			guard(&ds).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"a write in the wiping transaction must leave its guard armed"
		);
		Ok(())
	}

	/// A wipe deletes the index's keys as its transaction saw them, so a writer committing into a
	/// shard whose guard it knows armed, while the wipe is open, leaves an entry the wipe does not
	/// delete under a guard it does. The wipe's commit arms the guard of every shard still holding
	/// entries once it has landed.
	#[tokio::test]
	async fn diskann_write_committed_during_a_wipe_keeps_its_guard() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = new_index(ikb.clone(), TableId(4), &p, cache()).await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		write_one(&index, &ds, 1).await?;
		let wipe = new_ctx(&ds, TransactionType::Write).await;
		crate::wipe_index_data(&wipe.tx(), &ikb, &crate::catalog::Index::DiskAnn(p.clone()))
			.await?;
		write_one(&index, &ds, 33).await?;
		wipe.tx().commit().await?;
		let pending = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let range = crate::key::schema::DiskannRecordPendingShardShardPrefix {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
				shard,
			}
			.range()?;
			let n = ctx.tx().keys(range, u32::MAX, 0, None).await?.len();
			ctx.tx().cancel().await?;
			n
		};
		assert_eq!(pending, 1, "the wipe leaves the entry it did not see");
		assert_eq!(
			stored_guard(&ds, shard).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"the entry left behind must be under an armed guard"
		);
		Ok(())
	}

	/// Two datastores over one in-memory store, each a node of a cluster running
	/// the writer-lease protocol, with their own handles on one index.
	async fn lease_nodes() -> Result<(TestIndexStore, DiskAnnIndex, TestIndexStore, DiskAnnIndex)> {
		let base = TestIndexStore::new().await;
		let (a, b) = (base.shared_node(), base.shared_node());
		let p = params(VectorType::F32, Distance::Euclidean);
		let index_a = new_index(ikb(), TableId(4), &p, cache()).await?;
		let index_b = new_index(ikb(), TableId(4), &p, cache()).await?;
		Ok((a, index_a, b, index_b))
	}

	/// Commits one record's vector through `index` on `ds`.
	async fn write_one(index: &DiskAnnIndex, ds: &TestIndexStore, id: i64) -> Result<()> {
		let ctx = new_ctx(ds, TransactionType::Write).await;
		index
			.index(
				&ctx,
				&RecordIdKey::Number(id),
				None,
				Some(f32_content(&[id as f32, 0.0, 0.0, 0.0])),
			)
			.await?;
		ctx.tx().commit().await
	}

	/// The kind of `shard`'s stored guard, as `ds` reads it.
	async fn stored_guard(
		ds: &TestIndexStore,
		shard: u16,
	) -> Result<Option<DiskAnnPendingStateKind>> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let state = diskann_pending_states(&ctx.tx(), &ikb()).await?[shard as usize].clone();
		ctx.tx().cancel().await?;
		Ok(state.map(|state| state.kind))
	}

	/// The writer lease node `nid` holds on `shard`, as `ds` reads it.
	fn lease_key(shard: u16, nid: Uuid) -> crate::key::schema::DiskannWriterLeaseKey<'static> {
		let ikb = ikb();
		crate::key::schema::DiskannWriterLeaseKey {
			ns: ikb.ns(),
			db: ikb.db(),
			tb: Cow::Owned(ikb.table().clone()),
			ix: ikb.index(),
			shard,
			nid,
		}
	}

	async fn stored_lease(
		ds: &TestIndexStore,
		shard: u16,
		nid: Uuid,
	) -> Result<Option<surrealdb_datastore::values::diskann::DiskAnnWriterLease>> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let lease = ctx.tx().get_key(&lease_key(shard, nid), None).await?;
		ctx.tx().cancel().await?;
		Ok(lease)
	}

	/// Writes a lease for `nid` directly, as another process would have left it.
	async fn plant_lease(ds: &TestIndexStore, shard: u16, nid: Uuid, expired: bool) -> Result<()> {
		let ctx = new_ctx(ds, TransactionType::Write).await;
		ctx.tx()
			.set_key(
				&lease_key(shard, nid),
				&surrealdb_datastore::values::diskann::DiskAnnWriterLease::foreign(expired),
			)
			.await?;
		ctx.tx().commit().await
	}

	/// A node's writer lease keeps another node's compaction from returning the
	/// shard's guard to `Empty`, whatever that compaction drains, until the node
	/// releases the lease. The release queues the pass that then steps it.
	#[tokio::test]
	async fn diskann_lease_blocks_a_foreign_step_until_released() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_some(), "the write takes a lease");
		for _ in 0..3 {
			compact_once(&index_b, &b, &ikb()).await?;
		}
		assert_eq!(
			stored_guard(&b, shard).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"no node steps a guard under another node's lease"
		);
		assert_eq!(a.release_writer_leases().await?, 1);
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_none());
		compact_once(&index_b, &b, &ikb()).await?;
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		assert_eq!(
			knn_keys_with_allow(&index_b, &b, &[1.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![1]
		);
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// The shared-store form of a guard covered for a commit that then fails: the
	/// lease taken with it keeps the guard `NonEmpty` over a range holding nothing
	/// of the commit until the node releases the lease, after which a pass
	/// returns the guard to `Empty`.
	#[tokio::test]
	async fn diskann_lease_taken_for_a_failed_commit_is_released_and_stepped() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33));
		// The writer also writes a key a second transaction commits first, so its
		// own commit is refused after its covering has committed.
		let contested = crate::key::Key::from(b"/7575-contested".as_slice());
		let writer = new_ctx(&a, TransactionType::Write).await;
		index_a
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		writer.tx().set(contested.clone(), b"writer".to_vec()).await?;
		{
			let ctx = new_ctx(&a, TransactionType::Write).await;
			ctx.tx().set(contested, b"winner".to_vec()).await?;
			ctx.tx().commit().await?;
		}
		let refused = writer.tx().commit().await;
		assert!(
			refused.as_ref().is_err_and(is_retryable_transaction_conflict),
			"the writer's commit must lose the contested key: {refused:?}"
		);
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_some());
		compact_once(&index_b, &b, &ikb()).await?;
		assert_eq!(
			stored_guard(&b, shard).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"the lease holds although its commit failed"
		);
		assert_eq!(a.release_writer_leases().await?, 1);
		compact_once(&index_b, &b, &ikb()).await?;
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_none());
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// The cross-node form of #7575: a writer on one node commits while another
	/// node compacts its shard, and both its commit and its record survive.
	#[tokio::test]
	async fn diskann_writer_commits_while_another_node_compacts_its_shard() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		assert_eq!(shard, DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33)));
		write_one(&index_a, &a, 1).await?;
		let writer = new_ctx(&a, TransactionType::Write).await;
		index_a
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		for _ in 0..3 {
			compact_once(&index_b, &b, &ikb()).await?;
		}
		writer.tx().commit().await?;
		let query = [33.0, 0.0, 0.0, 0.0];
		assert_eq!(knn_keys_with_allow(&index_b, &b, &query, 2, None).await?, vec![1, 33]);
		for _ in 0..3 {
			compact_once(&index_b, &b, &ikb()).await?;
		}
		assert_eq!(knn_keys_with_allow(&index_b, &b, &query, 2, None).await?, vec![1, 33]);
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// A writer that found its node's lease held relies on it only if the node
	/// still holds it when the writer commits. A release in between, and a step
	/// another node takes after it, leave the writer to take the lease again, so
	/// its record is not committed under an `Empty` guard.
	#[tokio::test]
	async fn diskann_release_while_a_writer_is_open_does_not_hide_its_write() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		let writer = new_ctx(&a, TransactionType::Write).await;
		index_a
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		assert_eq!(a.release_writer_leases().await?, 1);
		for _ in 0..3 {
			compact_once(&index_b, &b, &ikb()).await?;
		}
		assert_eq!(
			stored_guard(&b, shard).await?,
			Some(DiskAnnPendingStateKind::Empty),
			"the shard must reach Empty while the writer is open for this to test anything"
		);
		writer.tx().commit().await?;
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_some());
		assert_eq!(
			knn_keys_with_allow(&index_b, &b, &[33.0, 0.0, 0.0, 0.0], 2, None).await?,
			vec![1, 33]
		);
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// Another node's wipe of the index's data removes this node's lease with the
	/// guards, so this node's next write takes the lease again and arms the
	/// guard, rather than relying on what it held before the wipe.
	#[tokio::test]
	async fn diskann_write_after_another_nodes_wipe_takes_the_lease_again() -> Result<()> {
		let (a, index_a, b, _index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		{
			let ctx = new_ctx(&b, TransactionType::Write).await;
			let index =
				crate::catalog::Index::DiskAnn(params(VectorType::F32, Distance::Euclidean));
			crate::wipe_index_data(&ctx.tx(), &ikb(), &index).await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(stored_guard(&a, shard).await?, None);
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_none());
		write_one(&index_a, &a, 33).await?;
		assert_eq!(stored_guard(&a, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_some());
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// A writer whose transaction is open across another node's wipe of the index's data commits
	/// under a lease and guard it takes again: its fence still records the lease the wipe
	/// removed, and the commit reads the lease before relying on it. A wipe and a write in one
	/// transaction cover the guard in that transaction, after the wipe's delete, with no lease.
	#[tokio::test]
	async fn diskann_writer_open_across_another_nodes_wipe_takes_the_lease_again() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		let writer = new_ctx(&a, TransactionType::Write).await;
		index_a
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		let diskann = crate::catalog::Index::DiskAnn(params(VectorType::F32, Distance::Euclidean));
		{
			let ctx = new_ctx(&b, TransactionType::Write).await;
			crate::wipe_index_data(&ctx.tx(), &ikb(), &diskann).await?;
			ctx.tx().commit().await?;
		}
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_none());
		writer.tx().commit().await?;
		assert_eq!(stored_guard(&a, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_some());
		// Node B wipes and writes in one transaction.
		{
			let ctx = new_ctx(&b, TransactionType::Write).await;
			crate::wipe_index_data(&ctx.tx(), &ikb(), &diskann).await?;
			index_b
				.index(
					&ctx,
					&RecordIdKey::Number(65),
					None,
					Some(f32_content(&[65.0, 0.0, 0.0, 0.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&b, shard, b.node_id()).await?.is_none());
		assert!(stored_lease(&b, shard, a.node_id()).await?.is_none());
		assert_eq!(
			knn_keys_with_allow(&index_b, &b, &[65.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![65]
		);
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// Taking a writer lease writes the guard with a new generation even when it already reads
	/// `NonEmpty`, so a step on another node that read the guard before the lease landed fails its
	/// conditional write, whether or not the store's locked read conflicts with it.
	#[tokio::test]
	async fn diskann_taking_a_lease_rewrites_an_armed_guard() -> Result<()> {
		let (a, index_a, b, _index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1)) as usize;
		write_one(&index_a, &a, 1).await?;
		assert_eq!(a.release_writer_leases().await?, 1);
		let generation = || {
			let a = a.clone();
			async move {
				let ctx = new_ctx(&a, TransactionType::Read).await;
				let state = diskann_pending_states(&ctx.tx(), &ikb()).await?[shard].clone();
				ctx.tx().cancel().await?;
				let state = state.expect("the guard is armed");
				assert_eq!(state.kind, DiskAnnPendingStateKind::NonEmpty);
				Ok::<_, anyhow::Error>(state.generation)
			}
		};
		let before = generation().await?;
		write_one(&index_a, &a, 33).await?;
		assert!(generation().await? > before, "taking the lease rewrote the armed guard");
		assert_eq!(a.guard_repairs() + b.guard_repairs(), 0);
		Ok(())
	}

	/// An idle release lets go of every lease the node holds on the index, across shards, and the
	/// pass it queues steps each of their guards.
	#[tokio::test]
	async fn diskann_idle_release_lets_go_of_every_shards_lease() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let ids = [1, 2, 3];
		for id in ids {
			write_one(&index_a, &a, id).await?;
		}
		let shards: Vec<u16> = ids
			.iter()
			.map(|id| DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(*id)))
			.collect();
		for &shard in &shards {
			assert!(stored_lease(&a, shard, a.node_id()).await?.is_some());
		}
		assert_eq!(a.release_writer_leases().await?, ids.len());
		compact_all(&index_b, &b, &ikb()).await?;
		for &shard in &shards {
			assert!(stored_lease(&a, shard, a.node_id()).await?.is_none());
			assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		}
		assert_eq!(a.guard_repairs() + b.guard_repairs(), 0);
		Ok(())
	}

	/// A commit relying on a lease close to expiry renews it, so a node that keeps
	/// writing into a shard never lets its lease lapse.
	#[tokio::test]
	async fn diskann_commit_renews_a_lease_close_to_expiry() -> Result<()> {
		let (a, index_a, _b, _index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		let lease = stored_lease(&a, shard, a.node_id()).await?.expect("the write took a lease");
		assert!(lease.holds_for(20));
		{
			let ctx = new_ctx(&a, TransactionType::Write).await;
			ctx.tx().set_key(&lease_key(shard, a.node_id()), &lease.expiring_in(1)).await?;
			ctx.tx().commit().await?;
		}
		write_one(&index_a, &a, 33).await?;
		let renewed = stored_lease(&a, shard, a.node_id()).await?.expect("the lease is still held");
		assert_eq!(renewed.process, lease.process);
		assert!(renewed.holds_for(20), "the commit renewed the lease");
		assert_eq!(a.guard_repairs(), 0, "the covering protocol left no guard to repair");
		Ok(())
	}

	/// A commit that stalls between covering its guard and landing can outlast the lease it relied
	/// on: another node retires the lease and, finding the shard's range still empty, steps the
	/// guard to `Empty`. A commit that lands that close to the lease's expiry covers its guard
	/// again, so its entry is not left under an `Empty` guard.
	#[tokio::test]
	async fn diskann_commit_outlasting_its_lease_covers_its_guard_again() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		compact_once(&index_b, &b, &ikb()).await?;
		// Node A relies on its lease with just over the renewal margin left.
		let lease = stored_lease(&a, shard, a.node_id()).await?.expect("the write took a lease");
		{
			let ctx = new_ctx(&a, TransactionType::Write).await;
			ctx.tx().set_key(&lease_key(shard, a.node_id()), &lease.expiring_in(16)).await?;
			ctx.tx().commit().await?;
		}
		let writer = new_ctx(&a, TransactionType::Write).await;
		index_a
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		// The commit stalls once its guard is covered: the lease expires, and node B retires it
		// and steps the guard.
		let (b_hook, a_node) = (b.clone(), a.node_id());
		a.before_next_commit(Box::new(move || {
			Box::pin(async move {
				tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
				let ctx = new_ctx(&b_hook, TransactionType::Write).await;
				ctx.tx()
					.set_key(&lease_key(shard, a_node), &lease.expiring_in(-1))
					.await
					.expect("expiring the lease");
				ctx.tx().commit().await.expect("expiring the lease");
				compact_once(&index_b, &b_hook, &ikb()).await.expect("node B's pass");
				assert_eq!(
					stored_guard(&b_hook, shard).await.expect("reading the guard"),
					Some(DiskAnnPendingStateKind::Empty),
					"node B stepped the guard under the stalled commit"
				);
			})
		}));
		writer.tx().commit().await?;
		assert_eq!(stored_guard(&a, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(
			stored_lease(&a, shard, a.node_id()).await?.is_some_and(|lease| lease.holds_for(20))
		);
		assert_eq!(
			knn_keys_with_allow(&index_a, &a, &[33.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![33]
		);
		// The lease the re-cover took is relied on from the commit, not from before the stall: an
		// idle release shorter than the stall leaves it.
		assert_eq!(
			a.release_idle_writer_leases(std::time::Duration::from_secs(1)).await?,
			0,
			"the re-cover's lease counts as just relied on"
		);
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// The shards holding pending entries are found by skipping from one to the next, including
	/// the first and the last shard, and none for an index with no pending entry.
	#[tokio::test]
	async fn diskann_shards_holding_entries_are_found_by_skipping() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let index =
			new_index(ikb(), TableId(4), &params(VectorType::F32, Distance::Euclidean), cache())
				.await?;
		let holding = |ds: &TestIndexStore| {
			let ds = ds.clone();
			async move {
				let ctx = new_ctx(&ds, TransactionType::Read).await;
				let ikb = ikb();
				let shards = ctx
					.tx()
					.diskann_shards_holding_entries(ikb.ns(), ikb.db(), ikb.table(), ikb.index())
					.await?;
				ctx.tx().cancel().await?;
				Ok::<_, anyhow::Error>(shards)
			}
		};
		assert_eq!(holding(&ds).await?, 0);
		let ids = [32, 5, 37, 31, 63];
		for id in ids {
			write_one(&index, &ds, id).await?;
		}
		let expected = ids.iter().fold(0u64, |shards, id| {
			shards | 1 << DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(*id))
		});
		assert_eq!(expected.count_ones(), 3, "the ids cover shards 0, 5 and 31");
		assert_eq!(holding(&ds).await?, expected);
		Ok(())
	}

	/// Where the datastore is the store's sole writer too, compaction repairs a guard left `Empty`
	/// over entries, as a wipe whose arming of such guards failed leaves one.
	#[tokio::test]
	async fn diskann_compaction_repairs_an_empty_guard_on_a_sole_writer() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let index =
			new_index(ikb(), TableId(4), &params(VectorType::F32, Distance::Euclidean), cache())
				.await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33));
		write_one(&index, &ds, 33).await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx().del_key(&ikb().new_dy_key(shard)).await?;
			ctx.tx().commit().await?;
		}
		compact_once(&index, &ds, &ikb()).await?;
		assert_eq!(ds.guard_repairs(), 1);
		assert_eq!(stored_guard(&ds, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		compact_all(&index, &ds, &ikb()).await?;
		assert_eq!(
			knn_keys_with_allow(&index, &ds, &[33.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![33]
		);
		Ok(())
	}

	/// On a store that validates every read, a guard a transaction read and wrote under is written
	/// by its commit even when it already reads `NonEmpty`, with a new generation, so a step that
	/// opened before the commit and lands after it fails its conditional write.
	#[tokio::test]
	async fn diskann_guard_read_on_a_validating_store_is_rewritten_when_armed() -> Result<()> {
		let ds = TestIndexStore::new().await.validating_node();
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1)) as usize;
		write_one(&index, &ds, 1).await?;
		let generation = |ds: &TestIndexStore| {
			let (ds, ikb) = (ds.clone(), ikb.clone());
			async move {
				let ctx = new_ctx(&ds, TransactionType::Read).await;
				let state = diskann_pending_states(&ctx.tx(), &ikb).await?[shard].clone();
				ctx.tx().cancel().await?;
				let state = state.expect("the write armed the guard");
				assert_eq!(state.kind, DiskAnnPendingStateKind::NonEmpty);
				Ok::<_, anyhow::Error>(state.generation)
			}
		};
		let before = generation(&ds).await?;
		let writer = new_ctx(&ds, TransactionType::Write).await;
		DiskAnnIndex::read_pending_state(&writer.tx(), &ikb).await?;
		index
			.index(
				&writer,
				&RecordIdKey::Number(33),
				None,
				Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
			)
			.await?;
		writer.tx().commit().await?;
		assert!(generation(&ds).await? > before, "the commit rewrote the armed guard");
		Ok(())
	}

	/// On a store other datastores write to, compaction probes the shards whose guard reads
	/// `Empty`, and a shard holding entries has its guard repaired and its entries compacted by
	/// the pass the repair queues, rather than left hidden from lookup.
	#[tokio::test]
	async fn diskann_compaction_repairs_a_guard_left_empty_over_entries() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(33));
		write_one(&index_a, &a, 33).await?;
		// A gap in the covering leaves the guard `Empty` over the entry.
		{
			let ctx = new_ctx(&a, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb().new_dy_key(shard),
					&DiskAnnPendingState {
						kind: DiskAnnPendingStateKind::Empty,
						generation: 1_000,
					},
				)
				.await?;
			ctx.tx().commit().await?;
		}
		assert_eq!(a.release_writer_leases().await?, 1);
		compact_once(&index_b, &b, &ikb()).await?;
		assert_eq!(b.guard_repairs(), 1);
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		compact_all(&index_b, &b, &ikb()).await?;
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		assert_eq!(
			knn_keys_with_allow(&index_b, &b, &[33.0, 0.0, 0.0, 0.0], 1, None).await?,
			vec![33]
		);
		Ok(())
	}

	/// A node's own lease, taken in this process, still lapses once it expires: the node's own step
	/// retires it and steps the guard, and the node's next write into the shard takes it again.
	#[tokio::test]
	async fn diskann_step_retires_its_own_expired_lease() -> Result<()> {
		let (a, index_a, _b, _index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		compact_once(&index_a, &a, &ikb()).await?;
		assert_eq!(
			stored_guard(&a, shard).await?,
			Some(DiskAnnPendingStateKind::NonEmpty),
			"a lease that holds keeps the guard"
		);
		let lease = stored_lease(&a, shard, a.node_id()).await?.expect("the write took a lease");
		{
			let ctx = new_ctx(&a, TransactionType::Write).await;
			ctx.tx().set_key(&lease_key(shard, a.node_id()), &lease.expiring_in(-1)).await?;
			ctx.tx().commit().await?;
		}
		compact_once(&index_a, &a, &ikb()).await?;
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_none());
		assert_eq!(stored_guard(&a, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		write_one(&index_a, &a, 33).await?;
		assert_eq!(stored_guard(&a, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&a, shard, a.node_id()).await?.is_some());
		assert_eq!(a.guard_repairs(), 0, "the covering protocol left no guard to repair");
		Ok(())
	}

	/// A lease holds until its node releases it or it expires. A step deletes one
	/// that no longer holds, and leaves one that does.
	#[tokio::test]
	async fn diskann_step_retires_only_leases_that_no_longer_hold() -> Result<()> {
		let (a, index_a, b, index_b) = lease_nodes().await?;
		let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
		write_one(&index_a, &a, 1).await?;
		assert_eq!(a.release_writer_leases().await?, 1);
		// An unexpired lease of another node holds.
		let other = Uuid::new_v4();
		plant_lease(&b, shard, other, false).await?;
		for _ in 0..3 {
			compact_once(&index_b, &b, &ikb()).await?;
		}
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
		assert!(stored_lease(&b, shard, other).await?.is_some());
		// Expired, it no longer holds, even though its node is a live member: a
		// node that kept its id across a restart leaves its earlier process's
		// leases behind.
		{
			let ctx = new_ctx(&b, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&crate::key::schema::NodeKey::new(other),
					&crate::catalog::node::Node::new(other, 0.into(), false),
				)
				.await?;
			ctx.tx().commit().await?;
		}
		plant_lease(&b, shard, other, true).await?;
		compact_once(&index_b, &b, &ikb()).await?;
		assert!(stored_lease(&b, shard, other).await?.is_none());
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		// A lease this node took in an earlier process never holds for it.
		write_one(&index_a, &a, 33).await?;
		assert_eq!(a.release_writer_leases().await?, 1);
		plant_lease(&b, shard, b.node_id(), false).await?;
		compact_once(&index_b, &b, &ikb()).await?;
		assert!(stored_lease(&b, shard, b.node_id()).await?.is_none());
		assert_eq!(stored_guard(&b, shard).await?, Some(DiskAnnPendingStateKind::Empty));
		assert_eq!(
			a.guard_repairs() + b.guard_repairs(),
			0,
			"the covering protocol left no guard to repair"
		);
		Ok(())
	}

	/// Dual-read transition: a record left pending in the legacy unsharded `!dr` layout by an older
	/// binary must stay visible to lookup, be drained by compaction, and not block the sharded
	/// `!dw` layout that new writes use.
	#[tokio::test]
	async fn diskann_dual_read_drains_legacy_pending_then_uses_shards() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;

		// Simulate a pre-upgrade pending write: a legacy `!dr` entry. New code finds it via the
		// unconditional `!dr` scan, independent of the `!dp` guard an old binary would also have
		// set, so no guard seed is needed here.
		let legacy_id = RecordIdKey::Number(1);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(&ikb.new_dr_key(&legacy_id), &f32_pending(&[1.0, 2.0, 3.0, 4.0])).await?;
			tx.commit().await?;
		}

		// Dual-read: lookup sees the legacy-pending vector even though it is in the old layout.
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		// Compaction drains the legacy entry into the graph and removes it from KV.
		assert!(compact_once(&index, &ds, &ikb).await?);
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get_key::<_>(&ikb.new_dr_key(&legacy_id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}
		// A second pass steps the shard's pending state the rest of the way to Empty.
		compact_once(&index, &ds, &ikb).await?;
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_all_pending_states_empty(&states));
			ctx.tx().cancel().await?;
		}
		// The vector now lives in the compacted graph and is still found.
		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		// A subsequent write uses the sharded layout, never the legacy key, and stays visible.
		let new_id = RecordIdKey::Number(2);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &new_id, None, Some(f32_content(&[4.0, 3.0, 2.0, 1.0]))).await?;
			ctx.tx().commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &new_id), None).await?.is_some());
			assert!(ctx.tx().get_key::<_>(&ikb.new_dr_key(&new_id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}
		assert_eq!(knn_len_with_k(&index, &ds, &[4.0, 3.0, 2.0, 1.0], 2).await?, 2);
		Ok(())
	}

	/// A pending entry written before the value carried the record id must be
	/// stamped with it when a later write reuses it — through either layout.
	///
	/// Without that, the reused entry keeps `id: None` and lookup falls back to
	/// the key, whose codec omits a number's kind. The write that was supposed
	/// to repair such a record would then still be searched and compacted under
	/// a decimalised identity no record is stored under.
	#[tokio::test]
	async fn diskann_write_stamps_the_id_on_an_entry_that_predates_it() -> Result<()> {
		use crate::val::{Number, Value};
		let arr = |n: i64| RecordIdKey::Array(vec![Value::Number(Number::Int(n))].into());

		for (layout, sharded) in [("sharded", true), ("legacy", false)] {
			let ds = TestIndexStore::new().await;
			let ikb = ikb();
			let index = new_index(
				ikb.clone(),
				TableId(4),
				&params(VectorType::F32, Distance::Euclidean),
				cache(),
			)
			.await?;
			let id = arr(1);

			// Exactly what an older binary left: no id in the value.
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let tx = ctx.tx();
				let mut pending = f32_pending(&[1.0, 2.0, 3.0, 4.0]);
				pending.id = None;
				if sharded {
					let shard = DiskAnnIndex::pending_state_shard(&id);
					tx.set_key(&ikb.new_dw_key(shard, &id), &pending).await?;
				} else {
					tx.set_key(&ikb.new_dr_key(&id), &pending).await?;
				}
				tx.commit().await?;
			}

			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				index
					.index(
						&ctx,
						&id,
						Some(f32_content(&[1.0, 2.0, 3.0, 4.0])),
						Some(f32_content(&[9.0, 9.0, 9.0, 9.0])),
					)
					.await?;
				ctx.tx().commit().await?;
			}

			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let shard = DiskAnnIndex::pending_state_shard(&id);
			let stored: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&ikb.new_dw_key(shard, &id), None).await?.expect("an entry");
			ctx.tx().cancel().await?;
			// Compared as encoded keys, not with `==`: `Number`'s equality is
			// cross-variant, so an id stamped from the key — the regression this
			// guards — would satisfy an equality check while addressing a
			// different record.
			assert_eq!(
				stored.id.as_ref().map(storekey::encode_vec).transpose().unwrap(),
				Some(storekey::encode_vec(&id).unwrap()),
				"the {layout} arm must stamp the record id on the entry it reuses"
			);
		}
		Ok(())
	}

	/// A record pending in both layouts folds under the id the value carries,
	/// not the one the legacy key can spell.
	///
	/// Nothing writes the legacy layout any more, so its entries never carry the
	/// id and the spelling recovered from the key is decimalised for an id
	/// holding a number. The sharded counterpart does carry the exact id, and
	/// both entries name one record — so folding the pair under the key-derived
	/// spelling would map the doc-ID to a key no record is stored under, and the
	/// KNN hit would be dropped after consuming its top-k slot.
	#[tokio::test]
	async fn diskann_dual_layout_folds_under_the_id_the_value_carries() -> Result<()> {
		use crate::val::{Number, Value};
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let docs = TableDocIds::new(NamespaceId(1), DatabaseId(2), "tb".into());
		let id = RecordIdKey::Array(vec![Value::Number(Number::Int(1))].into());

		// A newer node writes the sharded entry, carrying the exact id.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[1.0, 2.0, 3.0, 4.0]))).await?;
			ctx.tx().commit().await?;
		}
		// An older node then writes the legacy entry for the same record, which
		// cannot carry it.
		// The pair has to chain, as a real pair does: the legacy write follows the
		// sharded one, so its `old_vectors` are what that write left.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(
				&ikb.new_dr_key(&id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0])],
					new_vectors: vec![SerializedVector::F32(vec![5.0, 6.0, 7.0, 8.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.commit().await?;
		}

		while compact_once(&index, &ds, &ikb).await? {}

		let ctx = new_ctx(&ds, TransactionType::Read).await;
		let tx = ctx.tx();
		let doc_id = docs.get_doc_id(&tx, &id).await?.expect("mapped after compaction");
		let mapped = docs.get_record_id(&tx, doc_id).await?.expect("reverse mapping");
		tx.cancel().await?;
		// Encoded, not `==`: `Number`'s equality is cross-variant, so the
		// decimalised spelling would satisfy an equality check while naming a key
		// no record is stored under.
		assert_eq!(
			storekey::encode_vec(&mapped).unwrap(),
			storekey::encode_vec(&id).unwrap(),
			"folded under {mapped:?}, which is not the key the record is stored under"
		);
		Ok(())
	}

	/// Forward migration order: a record left pending in the legacy `!dr` layout and then updated
	/// by new code must end up with a single sharded entry carrying the new value -- the legacy
	/// entry is folded in and deleted, so a later reader never returns the stale legacy value.
	#[tokio::test]
	async fn diskann_write_folds_legacy_pending_into_shard() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);

		// Pre-upgrade legacy `!dr` entry written by an older binary (record value [1,2,3,4]). New
		// code finds it via the unconditional `!dr` scan, so the `!dp` guard an old binary would
		// also have set is irrelevant here.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(&ikb.new_dr_key(&id), &f32_pending(&[1.0, 2.0, 3.0, 4.0])).await?;
			tx.commit().await?;
		}

		// New code updates the same record [1,2,3,4] -> [9,9,9,9].
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&id,
					Some(f32_content(&[1.0, 2.0, 3.0, 4.0])),
					Some(f32_content(&[9.0, 9.0, 9.0, 9.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}

		// The legacy entry is folded away; a single sharded entry carries the new value.
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get_key::<_>(&ikb.new_dr_key(&id), None).await?.is_none());
			let folded: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&dw_key(&ikb, &id), None).await?.unwrap();
			assert_eq!(folded.new_vectors, vec![SerializedVector::F32(vec![9.0, 9.0, 9.0, 9.0])]);
			ctx.tx().cancel().await?;
		}

		// Lookup reflects the new value, not the stale legacy one: nearest to [9,9,9,9] is exact.
		assert_eq!(knn_nearest(&index, &ds, &[9.0, 9.0, 9.0, 9.0]).await?, Some(0.0));
		Ok(())
	}

	/// A write must fold and delete the legacy entry even when a sharded `!dw` entry already
	/// exists. A mixed-version cluster can leave both layouts for one record (older node writes
	/// `!dr` after a newer node wrote `!dw`); a later upgraded write must not leave the stale
	/// legacy entry behind, or lookup (scanning the legacy range last) would return the old value.
	#[tokio::test]
	async fn diskann_write_folds_legacy_even_when_sharded_exists() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let shard = DiskAnnIndex::pending_state_shard(&id);

		// Mixed-version state: a sharded `!dw` entry [1,1,1,1]->[2,2,2,2] (newer node) plus a
		// legacy `!dr` entry [2,2,2,2]->[3,3,3,3] (older node wrote afterwards) for the SAME
		// record.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(
				&ikb.new_dw_key(shard, &id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![1.0, 1.0, 1.0, 1.0])],
					new_vectors: vec![SerializedVector::F32(vec![2.0, 2.0, 2.0, 2.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.set_key(
				&ikb.new_dr_key(&id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![2.0, 2.0, 2.0, 2.0])],
					new_vectors: vec![SerializedVector::F32(vec![3.0, 3.0, 3.0, 3.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.set_key(
				&ikb.new_dy_key(shard),
				&DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 1,
				},
			)
			.await?;
			tx.commit().await?;
		}

		// An upgraded write updates the record [3,3,3,3] -> [4,4,4,4]. It must fold both layouts
		// and delete the legacy key, leaving one sharded entry [1,1,1,1]->[4,4,4,4].
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index
				.index(
					&ctx,
					&id,
					Some(f32_content(&[3.0, 3.0, 3.0, 3.0])),
					Some(f32_content(&[4.0, 4.0, 4.0, 4.0])),
				)
				.await?;
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			// Legacy entry deleted; the single sharded entry carries the chain head's old_vectors
			// and the newest new_vectors.
			assert!(ctx.tx().get_key::<_>(&ikb.new_dr_key(&id), None).await?.is_none());
			let folded: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&dw_key(&ikb, &id), None).await?.unwrap();
			assert_eq!(folded.old_vectors, vec![SerializedVector::F32(vec![1.0, 1.0, 1.0, 1.0])]);
			assert_eq!(folded.new_vectors, vec![SerializedVector::F32(vec![4.0, 4.0, 4.0, 4.0])]);
			ctx.tx().cancel().await?;
		}

		// Lookup returns the new value, never the stale legacy [3,3,3,3].
		assert_eq!(knn_nearest(&index, &ds, &[4.0, 4.0, 4.0, 4.0]).await?, Some(0.0));
		Ok(())
	}

	/// Reverse migration order: an older node writes a legacy `!dr` update *after* a newer node
	/// wrote the sharded `!dw` entry for the same record, so both layouts hold a pending entry.
	/// Compaction must coalesce them by the old->new vector chain (not scan order), so the graph
	/// settles at the true latest value rather than the older sharded one.
	#[tokio::test]
	async fn diskann_compaction_orders_cross_layout_pending_by_vector_chain() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let shard = DiskAnnIndex::pending_state_shard(&id);

		// Sharded `!dw` entry [10]->[20] (newer node), then legacy `!dr` entry [20]->[30] (older
		// node, written afterwards). The true chain is [10]->[20]->[30]; range order is the
		// reverse.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(
				&ikb.new_dw_key(shard, &id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![10.0, 10.0, 10.0, 10.0])],
					new_vectors: vec![SerializedVector::F32(vec![20.0, 20.0, 20.0, 20.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.set_key(
				&ikb.new_dr_key(&id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![20.0, 20.0, 20.0, 20.0])],
					new_vectors: vec![SerializedVector::F32(vec![30.0, 30.0, 30.0, 30.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.set_key(
				&ikb.new_dy_key(shard),
				&DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 1,
				},
			)
			.await?;
			tx.commit().await?;
		}

		// Drain everything into the graph.
		while compact_once(&index, &ds, &ikb).await? {}

		// The record settled at the chain tail [30,30,30,30]: that point is exact, while
		// [20,20,20,20] (the older sharded value) is not in the graph (distance sqrt(4*10^2)=20).
		assert_eq!(knn_nearest(&index, &ds, &[30.0, 30.0, 30.0, 30.0]).await?, Some(0.0));
		assert_eq!(knn_nearest(&index, &ds, &[20.0, 20.0, 20.0, 20.0]).await?, Some(20.0));
		Ok(())
	}

	/// Inverse-pair coalescing (compaction fold): a record reverted across layouts (sharded `!dw`
	/// `A->B`, then legacy `!dr` `B->A` by an older node) is an exact inverse, so *both* chain
	/// predicates hold. Compaction must resolve to the true chain head (net no-op, `A` stays), not
	/// the intermediate `B`, which would otherwise survive as a phantom returning distance 0.
	#[tokio::test]
	async fn diskann_revert_across_layouts_leaves_no_phantom() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let a = [1.0, 1.0, 1.0, 1.0];
		let b = [2.0, 2.0, 2.0, 2.0];

		// Insert A and compact so the graph holds A under a doc_id.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&a))).await?;
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}

		// New node writes A->B into the sharded layout.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, Some(f32_content(&a)), Some(f32_content(&b))).await?;
			ctx.tx().commit().await?;
		}
		// Reuse the doc_id the sharded entry resolved to for the simulated legacy revert.
		let doc_id = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let dw: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&dw_key(&ikb, &id), None).await?.unwrap();
			ctx.tx().cancel().await?;
			dw.doc_id
		};
		assert!(doc_id.is_some());

		// Older node reverts B->A in the legacy layout — an exact inverse of the sharded A->B.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dr_key(&id),
					&DiskAnnRecordPendingUpdate {
						doc_id,
						old_vectors: vec![SerializedVector::F32(b.to_vec())],
						new_vectors: vec![SerializedVector::F32(a.to_vec())],
						id: None,
						others: Vec::new(),
					},
				)
				.await?;
			ctx.tx().commit().await?;
		}

		// Compaction coalesces the inverse pair to a net no-op: A stays, B is not left behind.
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(knn_nearest(&index, &ds, &a).await?, Some(0.0));
		assert_eq!(knn_nearest(&index, &ds, &b).await?, Some(2.0));
		Ok(())
	}

	/// Inverse-pair coalescing (write-path fold): when a write finds both an inverse `!dw` (`A->B`)
	/// and `!dr` (`B->A`) for a compacted record at A, the fold must keep the sharded `!dw`'s
	/// `old_vectors` (the chain head A), not the legacy intermediate (B). Otherwise compaction
	/// removes the wrong vector and the stale A survives as a phantom.
	#[tokio::test]
	async fn diskann_write_fold_inverse_pair_keeps_chain_head() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let shard = DiskAnnIndex::pending_state_shard(&id);
		let a = [1.0, 1.0, 1.0, 1.0];
		let b = [2.0, 2.0, 2.0, 2.0];
		let c = [3.0, 3.0, 3.0, 3.0];

		// Insert A and compact so the graph holds A under a doc_id.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&a))).await?;
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}

		// New node writes A->B (sharded). Capture its doc_id for the legacy revert.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, Some(f32_content(&a)), Some(f32_content(&b))).await?;
			ctx.tx().commit().await?;
		}
		let doc_id = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let dw: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&dw_key(&ikb, &id), None).await?.unwrap();
			ctx.tx().cancel().await?;
			dw.doc_id
		};

		// Older node reverts B->A in the legacy layout (inverse of the sharded A->B).
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			ctx.tx()
				.set_key(
					&ikb.new_dr_key(&id),
					&DiskAnnRecordPendingUpdate {
						doc_id,
						old_vectors: vec![SerializedVector::F32(b.to_vec())],
						new_vectors: vec![SerializedVector::F32(a.to_vec())],
						id: None,
						others: Vec::new(),
					},
				)
				.await?;
			ctx.tx().commit().await?;
		}

		// A new write A->C now hits the write-path fold with both inverse layouts present.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, Some(f32_content(&a)), Some(f32_content(&c))).await?;
			ctx.tx().commit().await?;
		}
		// The fold kept the chain head A as old_vectors; the legacy entry is gone.
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get_key::<_>(&ikb.new_dr_key(&id), None).await?.is_none());
			let folded: DiskAnnRecordPendingUpdate =
				ctx.tx().get_key(&ikb.new_dw_key(shard, &id), None).await?.unwrap();
			assert_eq!(folded.old_vectors, vec![SerializedVector::F32(a.to_vec())]);
			assert_eq!(folded.new_vectors, vec![SerializedVector::F32(c.to_vec())]);
			ctx.tx().cancel().await?;
		}

		// Compaction nets the record to C: C is exact, the reverted-away A is not a phantom.
		while compact_once(&index, &ds, &ikb).await? {}
		assert_eq!(knn_nearest(&index, &ds, &c).await?, Some(0.0));
		assert_eq!(knn_nearest(&index, &ds, &a).await?, Some(4.0));
		Ok(())
	}

	/// A folded `!dr`+`!dw` pair whose combined value exceeds the byte budget — admitted as the
	/// first entry of a pass via `has_room_for`'s empty-batch escape hatch — must capture both
	/// halves. `add_authorized` bypasses the per-call byte guard that `add` re-applies once the
	/// batch is non-empty; without it the sharded half is rejected after the legacy half is
	/// captured, orphaning it (phase 2 skips it as folded) and resurfacing it as a phantom.
	#[test]
	fn diskann_builder_authorized_pair_survives_byte_budget() {
		fn op(n: i64) -> PendingOperation {
			let record = Arc::new(RecordIdKey::Number(n));
			PendingOperation {
				id: VectorId::RecordKey(Arc::clone(&record)),
				record: Some(record),
				old_vectors: vec![],
				new_vectors: vec![],
			}
		}
		// Two halves whose sum exceeds the budget (each just over half).
		let half = DISKANN_COMPACTION_MAX_PENDING_BYTES / 2 + 1;
		let empty_state = vec![None; DISKANN_PENDING_STATE_SHARDS as usize];

		// The fix: both halves of an authorized pair are captured.
		let mut builder = PendingPlanBuilder::new(None, empty_state.clone());
		assert!(
			builder.has_room_for(2, 2 + half + half, 0),
			"empty batch admits the oversized pair"
		);
		builder.add_authorized(vec![0u8; 1], vec![0u8; half], [op(1)]);
		builder.add_authorized(vec![1u8; 1], vec![0u8; half], [op(2)]);
		assert_eq!(builder.captured_keys.len(), 2, "both halves captured");

		// Contrast: plain `add` rejects the second half once the batch is non-empty and over the
		// byte budget — the orphaning path this fix closes.
		let mut naive = PendingPlanBuilder::new(None, empty_state);
		assert!(naive.add(vec![0u8; 1], vec![0u8; half], [op(1)]));
		assert!(!naive.add(vec![1u8; 1], vec![0u8; half], [op(2)]));
		assert_eq!(naive.captured_keys.len(), 1, "second half rejected by the byte guard");
	}

	/// Regression: when the legacy `!dr` backlog spans more than one compaction batch, a record
	/// that carries *both* a legacy and a sharded pending entry (an old node wrote `!dr` after a
	/// new node wrote `!dw` for the same record) must not be left indexed at two positions.
	///
	/// `prepare_compaction` drains the entire legacy range before it touches any shard, so once the
	/// legacy set exceeds one batch the record's legacy entry is captured, applied, and deleted in
	/// an early phase-1-only pass while its sharded entry is captured by phase 2 in a *later* pass.
	/// The two never reach `add_pending` together, so the cross-layout chain coalescing can't fold
	/// them, and `apply_pending_operation` inserts both the intermediate and the final vector —
	/// leaving a phantom. We pad the legacy range past `DISKANN_COMPACTION_MAX_PENDING_KEYS` with
	/// no-op entries to force that split deterministically.
	#[tokio::test]
	async fn diskann_compaction_split_batch_does_not_leave_phantom_vector() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;

		// The dual-layout record. `Number(1)` sorts before every filler below, so its legacy entry
		// lands in the first phase-1 batch (and is deleted there, before its `!dw` entry is seen).
		let id = RecordIdKey::Number(1);

		// 1) New node writes the sharded `!dw` entry (insert -> [2,2,2,2]) and bumps its `!dy`
		//    shard.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &id, None, Some(f32_content(&[2.0, 2.0, 2.0, 2.0]))).await?;
			ctx.tx().commit().await?;
		}

		// 2) An older node then writes a legacy `!dr` entry for the SAME record ([2,2,2,2] ->
		//    [3,3,3,3]), followed by enough no-op legacy fillers that the legacy range exceeds one
		//    compaction batch. Fillers carry empty vectors (no graph op on apply), so they only
		//    serve to push the batch boundary between this record's `!dr` and `!dw` entries.
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set_key(
				&ikb.new_dr_key(&id),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![2.0, 2.0, 2.0, 2.0])],
					new_vectors: vec![SerializedVector::F32(vec![3.0, 3.0, 3.0, 3.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			for i in 0..DISKANN_COMPACTION_MAX_PENDING_KEYS {
				let filler = RecordIdKey::Number(1000 + i as i64);
				tx.set_key(
					&ikb.new_dr_key(&filler),
					&DiskAnnRecordPendingUpdate {
						doc_id: None,
						old_vectors: vec![],
						new_vectors: vec![],
						id: None,
						others: Vec::new(),
					},
				)
				.await?;
			}
			tx.commit().await?;
		}

		// Drain everything: pass 1 captures a full batch of legacy (this record's `!dr` among them)
		// and deletes it; a later pass captures this record's orphaned `!dw` entry on its own.
		while compact_once(&index, &ds, &ikb).await? {}

		// The record's final value [3,3,3,3] is folded into the graph (exact match).
		assert_eq!(knn_nearest(&index, &ds, &[3.0, 3.0, 3.0, 3.0]).await?, Some(0.0));

		// The superseded intermediate value [2,2,2,2] must NOT be indexed. With the bug it is
		// (applied uncoalesced from the orphaned `!dw` entry), so the nearest distance is 0.0;
		// correct behaviour leaves only [3,3,3,3], so the nearest to [2,2,2,2] is sqrt(4*1^2)=2.0.
		assert_eq!(
			knn_nearest(&index, &ds, &[2.0, 2.0, 2.0, 2.0]).await?,
			Some(2.0),
			"phantom vector: the superseded [2,2,2,2] is still indexed for record {id:?}; the \
			 legacy/sharded pending pair was applied uncoalesced across separate compaction batches",
		);
		Ok(())
	}

	/// Regression: when the legacy `!dr` backlog can't be fully captured in one compaction batch,
	/// `prepare_compaction` must report `has_more == true` so `process_diskann_compaction` runs
	/// another pass. Phase 1 admits a legacy entry (and its folded `!dw` counterpart) via
	/// `PendingPlanBuilder::has_room_for` — a pure check that, unlike `add`, does not set
	/// `has_more` — so when a legacy+`!dw` pair straddles the key budget it bails without it. Left
	/// unset, the driver ends the cycle after one batch and strands the rest of the legacy backlog
	/// (and the full-range legacy lookup scans this change exists to drain) until the next write
	/// re-enqueues the index.
	///
	/// Setup: `MAX - 1` single-key legacy fillers take the batch to one below the key budget, then
	/// one dual-layout record (both `!dr` and `!dw`, sorting last) presents the 2-key pair that
	/// can't fit — exactly the case where phase 1 returns "not drained" without tripping
	/// `has_more`.
	#[tokio::test]
	async fn diskann_legacy_overflow_reports_has_more() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;

		// The dual-layout record sorts after every filler, so phase 1 captures all fillers first
		// and only then meets its 2-key pair. Its `!dw` entry (and `!dy` guard) is written via the
		// normal index path; its legacy `!dr` entry is written by hand below.
		let dual = RecordIdKey::Number(1_000_000);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &dual, None, Some(f32_content(&[1.0, 1.0, 1.0, 1.0]))).await?;
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			// `MAX - 1` single-key legacy fillers (no `!dw` counterpart) fill the batch to one key
			// below the budget.
			for i in 0..(DISKANN_COMPACTION_MAX_PENDING_KEYS - 1) {
				let filler = RecordIdKey::Number(i as i64);
				tx.set_key(
					&ikb.new_dr_key(&filler),
					&DiskAnnRecordPendingUpdate {
						doc_id: None,
						old_vectors: vec![],
						new_vectors: vec![],
						id: None,
						others: Vec::new(),
					},
				)
				.await?;
			}
			// The dual record's legacy `!dr` entry: folded with the `!dw` written above, it is the
			// 2-key pair that overflows the key budget.
			tx.set_key(
				&ikb.new_dr_key(&dual),
				&DiskAnnRecordPendingUpdate {
					doc_id: None,
					old_vectors: vec![SerializedVector::F32(vec![1.0, 1.0, 1.0, 1.0])],
					new_vectors: vec![SerializedVector::F32(vec![2.0, 2.0, 2.0, 2.0])],
					id: None,
					others: Vec::new(),
				},
			)
			.await?;
			tx.commit().await?;
		}

		let ctx = new_ctx(&ds, TransactionType::Read).await;
		let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
		ctx.tx().cancel().await?;

		assert!(plan.has_work(), "the batch captured the legacy fillers");
		assert!(
			plan.has_more(),
			"legacy `!dr` backlog exceeded one batch but the plan reported no more work; \
			 process_diskann_compaction would strand the remaining legacy entries until the next \
			 write re-enqueues the index",
		);
		Ok(())
	}

	/// Seeds four records and returns two identical compaction plans over them, with an apply
	/// context for each opened before either commits, so the second's snapshot still sees the
	/// captured `!dr` keys and the pre-bump `!dg` after the first commits.
	async fn racing_plans(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
	) -> Result<[(DiskAnnCompactionPlan, TestIndexEnv); 2]> {
		for i in 0..4_i64 {
			let ctx = new_ctx(ds, TransactionType::Write).await;
			let v = [i as f32, 0.0, 0.0, 0.0];
			index.index(&ctx, &RecordIdKey::Number(i), None, Some(f32_content(&v))).await?;
			ctx.tx().commit().await?;
		}
		let mut plans = Vec::new();
		for _ in 0..2 {
			let ctx = new_ctx(ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, ikb).await?;
			ctx.tx().cancel().await?;
			assert!(plan.has_work());
			plans.push(plan);
		}
		let plan_b = plans.pop().expect("two plans");
		let plan_a = plans.pop().expect("two plans");
		let ctx_a = new_ctx(ds, TransactionType::Write).await;
		let ctx_b = new_ctx(ds, TransactionType::Write).await;
		Ok([(plan_a, ctx_a), (plan_b, ctx_b)])
	}

	/// Two processes race on the same `!dr` plan. The late one's commit conflicts on `!dg` after
	/// it has already written through to its own cache during the apply phase, at the generation
	/// the other process did commit. `apply_compaction` must clear the index's entries before it
	/// returns the error, or a reader at that generation would be served a graph that was never
	/// committed.
	#[tokio::test]
	async fn diskann_failed_compaction_clears_cache_and_keeps_knn_working() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = new_index(ikb.clone(), TableId(4), &p, cache()).await?;
		let cache_b = cache();
		let index_b = new_index(ikb.clone(), TableId(4), &p, cache_b.clone()).await?;
		let [(plan_a, ctx_a), (plan_b, ctx_b)] = racing_plans(&index, &ds, &ikb).await?;

		// Apply A first — succeeds and commits.
		assert_eq!(index.apply_compaction(&ctx_a, plan_a).await?, CompactionApply::Applied);

		// Apply B — passes the write-time checks (its snapshot still sees the captured values and
		// the pre-A generation), writes through during the apply phase, then must fail on commit
		// because OCC catches the snapshot violation.
		let res = index_b.apply_compaction(&ctx_b, plan_b).await;
		assert!(res.is_err(), "expected commit failure, got {res:?}");

		// B's write-throughs were stamped with the generation B would have committed, which is the
		// generation A did commit. The failed frame cleared them while it still held the graph
		// write lock, so B's cache holds nothing at all.
		assert_eq!(cache_b.weight(), 0, "the failed frame left entries in its cache");

		// KNN on B now goes to KV, populates the cache fresh, and returns the elements
		// committed by A.
		assert_eq!(knn_len_with_k(&index_b, &ds, &[2.0, 0.0, 0.0, 0.0], 4).await?, 4);
		Ok(())
	}

	/// A frame whose snapshot predates a commit its own wrapper made cannot commit: its
	/// conditional bump of `!dg` expects a generation that has moved on. It is recognised as a
	/// stale plan before it writes anything, so the cache the committed frame warmed survives.
	#[tokio::test]
	async fn diskann_frame_behind_a_local_commit_is_stale() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = cache();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache.clone(),
		)
		.await?;
		let [(plan_a, ctx_a), (plan_b, ctx_b)] = racing_plans(&index, &ds, &ikb).await?;
		assert_eq!(index.apply_compaction(&ctx_a, plan_a).await?, CompactionApply::Applied);
		assert_eq!(knn_len_with_k(&index, &ds, &[2.0, 0.0, 0.0, 0.0], 4).await?, 4);
		let warmed = cache.weight();
		assert!(warmed > 0);

		assert_eq!(
			index.apply_compaction(&ctx_b, plan_b).await?,
			CompactionApply::LostRace,
			"a stale frame applies nothing"
		);
		assert_eq!(cache.weight(), warmed, "a stale frame leaves the cache as it found it");
		let view = index.graph.read().await.reader_view(Some(1));
		let cache_index = (ikb.ns(), ikb.db(), TableId(4), ikb.index(), DOC_IDS_FORMAT_VERSION);
		assert!(
			cache.get_state(cache_index, &view).is_some(),
			"the committed state is still served"
		);
		assert_eq!(knn_len_with_k(&index, &ds, &[2.0, 0.0, 0.0, 0.0], 4).await?, 4);
		Ok(())
	}

	/// T3 — end-to-end compaction + KNN in `use_hashed_vector` mode. The existing
	/// `docs.rs` tests cover the in-bucket disambiguation only on synthetic
	/// pre-seeded buckets; this exercises the real compaction → graph build → search
	/// path with hashed-vector storage enabled.
	#[tokio::test]
	async fn diskann_hashed_vector_compaction_and_knn() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = cache();
		let params = DiskAnnParams {
			use_hashed_vector: true,
			..params(VectorType::F32, Distance::Euclidean)
		};
		let index = new_index(ikb.clone(), TableId(4), &params, cache.clone()).await?;

		let v0 = [1.0_f32, 0.0, 0.0, 0.0];
		let v1 = [0.0_f32, 1.0, 0.0, 0.0];
		let v2 = [0.0_f32, 0.0, 1.0, 0.0];

		for (id, v) in [(0, &v0), (1, &v1), (2, &v2)] {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &RecordIdKey::Number(id), None, Some(f32_content(v))).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);

		// KNN queries through the hashed path must still find each record.
		assert_eq!(knn_len(&index, &ds, &v0).await?, 1);
		assert_eq!(knn_len(&index, &ds, &v1).await?, 1);
		assert_eq!(knn_len(&index, &ds, &v2).await?, 1);

		// Two records sharing the *same* vector → one bucket entry, two docs. Remove
		// one and the bucket entry survives with the other doc still searchable.
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		index.index(&ctx, &RecordIdKey::Number(3), None, Some(f32_content(&v0))).await?;
		ctx.tx().commit().await?;
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert_eq!(knn_len(&index, &ds, &v0).await?, 1);

		// Remove one of the shared docs; the other must remain searchable through the
		// surviving bucket entry (exercises `RemoveResult::BucketShrunk`).
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		index.index(&ctx, &RecordIdKey::Number(0), Some(f32_content(&v0)), None).await?;
		ctx.tx().commit().await?;
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert_eq!(knn_len(&index, &ds, &v0).await?, 1);

		// Remove the last shared doc; bucket entry is removed, the graph element
		// goes (exercises `RemoveResult::EntryRemoved` / `Empty`). KNN now returns
		// the next nearest neighbour, not v0.
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		index.index(&ctx, &RecordIdKey::Number(3), Some(f32_content(&v0)), None).await?;
		ctx.tx().commit().await?;
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert_eq!(knn_len(&index, &ds, &v1).await?, 1);
		Ok(())
	}

	// ------------------------------------------------------------------
	// Recall harness
	// ------------------------------------------------------------------
	//
	// What DiskANN's KNN answers are worth is not covered by any assertion on
	// key counts or result cardinality: a graph that has lost its long-range
	// edges still returns `k` rows, just the wrong ones. The harness below
	// measures Recall@k — the fraction of the exact top-`k` the graph search
	// actually finds — against a brute-force ground truth over the same corpus
	// and the same metric, and pins a floor per (dataset, `ef`).
	//
	// The floors are the reference point for any later change to how the graph
	// is built: a build that reorders, batches or parallelises insertion
	// produces a different graph, and the only thing that says the different
	// graph is still a good one is that it answers queries as well.

	/// Corpus seed used when `TEST_SEED` is unset.
	const RECALL_SEED: u64 = 0x0D15_C0FF_EE5E_ED01;

	/// A compaction plan stops at whichever cap it hits first. Every harness
	/// corpus is `F32` at 768 dimensions or narrower — around 3 KiB of vector per
	/// pending update at the widest — so the key cap always binds first and
	/// [`measure_recall`] can assert an exact pass count. A wider corpus would
	/// hit the byte cap instead, and that assertion would not hold for it.
	const _: () = assert!(
		DISKANN_COMPACTION_MAX_PENDING_KEYS * 768 * size_of::<f32>()
			< DISKANN_COMPACTION_MAX_PENDING_BYTES
	);

	/// Seeds a recall corpus.
	///
	/// Fixed by default: the DiskANN build is deterministic, so a fixed corpus
	/// makes the measured recall reproducible and the floors below a tripwire
	/// rather than a guess. `TEST_SEED` overrides it to probe a different draw.
	/// The floors leave margin for one, but they are set against the default
	/// corpus: a failure under another seed is not by itself evidence of a
	/// regression.
	fn recall_rng() -> SmallRng {
		let seed = std::env::var("TEST_SEED")
			.map(|s| s.parse::<u64>().expect("TEST_SEED must parse as u64"))
			.unwrap_or(RECALL_SEED);
		SmallRng::seed_from_u64(seed)
	}

	/// Generator for a corpus with cluster structure.
	///
	/// A uniform-random draw is a poor probe for an ANN graph: past a handful of
	/// dimensions every pair of points sits at nearly the same distance, so a
	/// badly connected graph answers a KNN query about as well as a well
	/// connected one and recall stops discriminating between them. Embedding
	/// corpora are clustered, and a pruned graph loses recall exactly where a
	/// search has to hop between clusters.
	///
	/// A centroid is uniform in `[-1, 1]^dim`; a member is its centroid plus
	/// per-component noise uniform in `[-spread, spread]`, with membership
	/// assigned round-robin so no centroid dominates. Both radii scale with
	/// `sqrt(dim)`, so a given `spread` separates clusters equally well at any
	/// dimension; a `spread` well below the typical inter-centroid distance is
	/// what makes the number of clusters — rather than the number of points —
	/// the parameter the graph's degree has to cope with.
	///
	/// Members per cluster must stay **below** the `k` a dataset measures
	/// Recall@`k` at. A cluster of `k` or more members makes the exact top-`k`
	/// one cluster's interior, which the search reaches as soon as it reaches
	/// that cluster at all: recall pins to 1.0 and reports nothing about how the
	/// graph connects clusters, which is the property under test.
	struct ClusteredCorpus {
		centroids: Vec<Vec<f32>>,
		spread: f32,
		next: usize,
	}

	impl ClusteredCorpus {
		fn new(rng: &mut SmallRng, dim: usize, clusters: usize, spread: f32) -> Self {
			assert!(clusters > 0, "a clustered corpus needs at least one centroid");
			let centroids = (0..clusters)
				.map(|_| (0..dim).map(|_| rng.random_range(-1.0f32..=1.0)).collect())
				.collect();
			Self {
				centroids,
				spread,
				next: 0,
			}
		}

		/// Draws `count` further members, continuing both the round-robin over
		/// centroids and the RNG stream.
		///
		/// Successive calls therefore return disjoint points from one
		/// distribution, which is what keeps a held-out query set free of copies
		/// of indexed vectors: a query that is its own nearest neighbour scores a
		/// guaranteed hit and turns the average into an over-estimate.
		fn draw(&mut self, rng: &mut SmallRng, count: usize) -> Vec<Vec<f32>> {
			let mut out = Vec::with_capacity(count);
			for _ in 0..count {
				let centroid = &self.centroids[self.next % self.centroids.len()];
				self.next += 1;
				let member = centroid
					.iter()
					.map(|c| c + rng.random_range(-self.spread..=self.spread))
					.collect();
				out.push(member);
			}
			out
		}
	}

	/// DiskANN parameters at an arbitrary dimension and graph shape. `DEGREE` and
	/// `L_BUILD` are what the harness varies; everything else is the DDL default.
	fn graph_params(
		dimension: u16,
		distance: Distance,
		degree: u16,
		l_build: u16,
	) -> DiskAnnParams {
		DiskAnnParams {
			dimension,
			distance,
			vector_type: VectorType::F32,
			degree,
			l_build,
			alpha: 1.2.into(),
			use_hashed_vector: false,
		}
	}

	/// Exact top-`k` record ids for `query` over `corpus` under `distance`.
	///
	/// Ties at the `k`-th distance are broken by record id. The draws are
	/// continuous floats, so an exact tie at that boundary does not arise; on
	/// data where it could, the ground truth would be one of several equally
	/// correct answers and the recall computed against it a lower bound.
	fn brute_force_top_k(
		corpus: &[Vector],
		query: &[f32],
		distance: &Distance,
		k: usize,
	) -> Vec<i64> {
		let q = Vector::F32(query.to_vec().into());
		let mut scored: Vec<(f64, i64)> = corpus
			.iter()
			.enumerate()
			.map(|(id, v)| (distance.calculate(v, &q), id as i64))
			.collect();
		scored.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
		scored.truncate(k);
		scored.into_iter().map(|(_, id)| id).collect()
	}

	/// Fraction of the exact answer the index returned.
	///
	/// Counts the intersection of two *sets*, not matching entries in `got`. Counting entries
	/// would let a graph that returns one true neighbour ten times score 1.0 instead of 0.1 —
	/// exactly the malformed shape this harness exists to catch. A duplicate in `got` is itself
	/// a defect rather than a recall loss, so it fails here instead of being scored.
	fn recall(truth: &[i64], got: &[i64]) -> f64 {
		let seen: HashSet<i64> = got.iter().copied().collect();
		assert_eq!(seen.len(), got.len(), "index returned duplicate ids: {got:?}");
		let truth: HashSet<i64> = truth.iter().copied().collect();
		let hits = truth.intersection(&seen).count();
		hits as f64 / truth.len() as f64
	}

	/// Record ids the index returns for one query, in result order.
	async fn knn_ids(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		query: &[f32],
		k: usize,
		ef: usize,
	) -> Result<Vec<i64>> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let pt = f32_query(query);
		let res = index.knn_search(&ctx, &pt, k, ef, None, None).await?;
		ctx.tx().cancel().await?;
		Ok(res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect())
	}

	/// Builds one DiskANN index over `corpus` and measures Recall@`k` against
	/// brute-force ground truth at each `(ef, floor)` pair.
	///
	/// The corpus goes in through the production path — `index()` enqueues one
	/// `!dw` pending per record, [`compact_all`] drains them into the graph — so
	/// the graph measured here is the one a `DEFINE INDEX` builds, not a
	/// hand-assembled stand-in. Recall is averaged over the query set; the
	/// per-`ef` floor is asserted on that average.
	async fn measure_recall(
		label: &str,
		params: &DiskAnnParams,
		corpus: &[Vec<f32>],
		queries: &[Vec<f32>],
		k: usize,
		ef_floors: &[(usize, f64)],
	) -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let index = new_index(ikb.clone(), TableId(4), params, cache()).await?;

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for (id, v) in corpus.iter().enumerate() {
				index
					.index(&ctx, &RecordIdKey::Number(id as i64), None, Some(f32_content(v)))
					.await?;
			}
			ctx.tx().commit().await?;
		}
		let passes = compact_all(&index, &ds, &ikb).await?;
		// Exact-count accounting on the drain, not a timing claim: one
		// prepare/apply pass captures at most `DISKANN_COMPACTION_MAX_PENDING_KEYS`
		// pending keys, and a corpus of fresh records is one pending key each, so
		// the drain takes exactly `ceil(n / cap)` passes that do work. Pinning it
		// says the graph under measurement was built through the bounded drain
		// rather than in one unbounded transaction.
		assert_eq!(
			passes,
			corpus.len().div_ceil(DISKANN_COMPACTION_MAX_PENDING_KEYS),
			"{label}: bounded drain of {} pendings took {passes} passes",
			corpus.len()
		);

		let vectors: Vec<Vector> = corpus.iter().map(|v| Vector::F32(v.clone().into())).collect();
		let truth: Vec<Vec<i64>> =
			queries.iter().map(|q| brute_force_top_k(&vectors, q, &params.distance, k)).collect();

		for &(ef, floor) in ef_floors {
			let mut total = 0.0;
			for (query, truth) in queries.iter().zip(truth.iter()) {
				let got = knn_ids(&index, &ds, query, k, ef).await?;
				total += recall(truth, &got);
			}
			let measured = total / queries.len() as f64;
			info!(
				"{label}: n={} q={} DEGREE={} L_BUILD={} ef={ef} -> Recall@{k}={measured:.4} \
				 (floor {floor}, {passes} compaction passes)",
				corpus.len(),
				queries.len(),
				params.degree,
				params.l_build,
			);
			assert!(
				measured >= floor,
				"{label}: ef={ef} Recall@{k}={measured:.4} below floor {floor}"
			);
		}
		Ok(())
	}

	/// Recall floor: the report's corpus shape — far more clusters than a node
	/// has out-edges — at the `DEFINE INDEX … DISKANN` defaults
	/// (`DEGREE 64`, `L_BUILD 100`, `ALPHA 1.2`).
	///
	/// With 2000 centroids against a 64-edge budget the graph cannot hold a
	/// direct edge to most clusters, so answering a query means hopping between
	/// them: this is the structure a build that drops long-range edges breaks
	/// first. At this corpus size a healthy graph saturates — the floors sit just
	/// under 1.0 and catch a graph that has structurally broken rather than one
	/// that has drifted. [`diskann_recall_starved_degree`] is the companion with
	/// headroom in the middle of the curve.
	#[test_log::test(tokio::test(flavor = "multi_thread"))]
	async fn diskann_recall_many_clusters_degree_64() -> Result<()> {
		let mut rng = recall_rng();
		let mut corpus = ClusteredCorpus::new(&mut rng, 32, 2000, 0.35);
		let base = corpus.draw(&mut rng, 2500);
		let queries = corpus.draw(&mut rng, 100);
		let params = graph_params(32, Distance::Cosine, 64, 100);
		measure_recall(
			"many-clusters-degree-64",
			&params,
			&base,
			&queries,
			10,
			&[(10, 0.96), (20, 0.97), (40, 0.98), (100, 0.98), (200, 0.98)],
		)
		.await
	}

	/// Recall floor: the same clustered shape against a deliberately starved
	/// graph budget (`DEGREE 8`, `L_BUILD 24`).
	///
	/// Starving the degree is what buys the harness its resolution. A graph
	/// dense enough to answer every query exactly — which
	/// [`diskann_recall_many_clusters_degree_64`] is, at a corpus this size —
	/// reports 1.0 whether the build is good or merely adequate. Here recall
	/// climbs across the whole `ef` sweep, so a build that produces a slightly
	/// worse graph moves the number instead of staying pinned at the ceiling.
	#[test_log::test(tokio::test(flavor = "multi_thread"))]
	async fn diskann_recall_starved_degree() -> Result<()> {
		let mut rng = recall_rng();
		let mut corpus = ClusteredCorpus::new(&mut rng, 64, 1500, 0.35);
		let base = corpus.draw(&mut rng, 4000);
		let queries = corpus.draw(&mut rng, 100);
		let params = graph_params(64, Distance::Cosine, 8, 24);
		measure_recall(
			"starved-degree",
			&params,
			&base,
			&queries,
			10,
			&[(10, 0.55), (20, 0.67), (40, 0.77), (100, 0.89), (200, 0.94)],
		)
		.await
	}

	/// Diagnostic: the DDL defaults at the reference workload's dimension and an
	/// order of magnitude more rows than CI can pay for — 20 000 × 768-dim cosine
	/// vectors, `DEGREE 64` / `L_BUILD 100`.
	///
	/// This is what says how much of
	/// [`diskann_recall_many_clusters_degree_64`]'s saturation is the corpus rather
	/// than the graph. At the CI dataset's size the shipped configuration answers
	/// essentially every query exactly, while at this size and dimension recall
	/// varies with `ef`. So that ceiling is a property of the CI corpus, and its
	/// floors are a structural tripwire rather than a measurement of graph quality.
	/// The measured curve belongs in the run's report, not here.
	///
	/// Ignored because a 768-dim build is minutes rather than seconds: per-insert
	/// cost scales with the dimension. Run it against a change to the build path:
	///
	/// ```text
	/// cargo test --release -p surrealdb-idx --lib diag_diskann_recall_768 -- --ignored --nocapture
	/// ```
	#[test_log::test(tokio::test(flavor = "multi_thread"))]
	#[ignore = "diagnostic (slow, 768-dim): DiskANN Recall@10 at the reference workload's dimension"]
	async fn diag_diskann_recall_768() -> Result<()> {
		let mut rng = recall_rng();
		let mut corpus = ClusteredCorpus::new(&mut rng, 768, 8000, 0.35);
		let base = corpus.draw(&mut rng, 20_000);
		let queries = corpus.draw(&mut rng, 100);
		measure_recall(
			"diag-768-degree-64",
			&graph_params(768, Distance::Cosine, 64, 100),
			&base,
			&queries,
			10,
			&[(10, 0.86), (20, 0.88), (40, 0.89), (100, 0.91), (200, 0.94)],
		)
		.await
	}

	// ------------------------------------------------------------------
	// The graph cache against generations committed elsewhere, or late
	// ------------------------------------------------------------------

	/// Two DiskANN handles over one KV store, each with its own [`DiskAnnCache`].
	///
	/// This is what two SurrealDB nodes sharing a store look like from the index's point of view.
	/// [`DiskAnnGraph`] keeps no data of its own: every value it serves comes either from the `!d*`
	/// keyspace, which both handles share, or from the cache, which neither handle can reach the
	/// other's copy of. A `Datastore` builds one `IndexStores` — and with it one `DiskAnnCache` and
	/// one index registry — per handle, so two handles here have exactly the separation two
	/// datastores over one store have, without the catalog layer in between.
	///
	/// `!dg` is the only cross-node signal an index carries, and nothing in the cache is scoped to
	/// it: only `DocId` entries hold a generation. Every scenario below turns on that gap.
	struct TwoNodes {
		ds: TestIndexStore,
		/// The handle that produces the foreign generations.
		a: DiskAnnIndex,
		/// The handle whose cache is warmed before those generations land.
		b: DiskAnnIndex,
		/// `b`'s cache, so a control run can drop it at the point a foreign generation lands.
		b_cache: DiskAnnCache,
		ikb: IndexKeyBase,
	}

	/// Degree wide enough that no adjacency list in these scenarios reaches the slacked bound, so
	/// back-edges are pure appends and a persisted adjacency list can only grow. Any neighbour that
	/// disappears from one is therefore a lost edge, not a prune.
	const TWO_NODE_DEGREE: u16 = 64;

	fn two_node_params() -> DiskAnnParams {
		DiskAnnParams {
			degree: TWO_NODE_DEGREE,
			..params(VectorType::F32, Distance::Euclidean)
		}
	}

	impl TwoNodes {
		async fn new(ds: TestIndexStore) -> Result<Self> {
			let ikb = ikb();
			let p = two_node_params();
			let b_cache = cache();
			Ok(Self {
				a: new_index(ikb.clone(), TableId(4), &p, cache()).await?,
				b: new_index(ikb.clone(), TableId(4), &p, b_cache.clone()).await?,
				b_cache,
				ikb,
				ds,
			})
		}

		/// Drops every entry `b` has cached for this index, standing in for a node that has just
		/// started or whose entries have aged out. The control runs use it to pin the cache as the
		/// cause: with it, each scenario behaves as a single node does.
		async fn clear_b_cache(&self) {
			self.b_cache
				.remove_index(
					self.ikb.ns(),
					self.ikb.db(),
					TableId(4),
					self.ikb.index(),
					DOC_IDS_FORMAT_VERSION,
				)
				.await;
		}

		/// Appends a pending update. Which handle records it is immaterial —
		/// [`DiskAnnIndex::index`] only writes `!dw`/`!dy` keys and touches no cache — so every
		/// scenario writes through `a`.
		async fn write(&self, id: i64, old: Option<&[f32]>, new: Option<&[f32]>) -> Result<()> {
			let ctx = new_ctx(&self.ds, TransactionType::Write).await;
			self.a
				.index(&ctx, &RecordIdKey::Number(id), old.map(f32_content), new.map(f32_content))
				.await?;
			ctx.tx().commit().await
		}

		/// Drains every pending update through `node`, which is the handle whose cache the
		/// compaction reads through and writes through, and whose `!dg` bump the fence sees.
		async fn compact_on(&self, node: &DiskAnnIndex) -> Result<()> {
			while compact_once(node, &self.ds, &self.ikb).await? {}
			Ok(())
		}

		async fn next_element_id(&self) -> Result<ElementId> {
			let tx = self.ds.transaction(TransactionType::Read).await?;
			let state: DiskAnnState =
				tx.get_key(&self.ikb.new_ds_key(), None).await?.unwrap_or_default();
			tx.cancel().await?;
			Ok(state.next_element_id)
		}

		/// The persisted adjacency list and vector of every element below `limit`, read straight
		/// from KV so the assertion never consults either handle's cache.
		async fn graph_dump(&self, limit: ElementId) -> Result<GraphDump> {
			let tx = self.ds.transaction(TransactionType::Read).await?;
			let mut dump = GraphDump::new();
			for element_id in 0..limit {
				let node: Option<DiskAnnNode> =
					tx.get_key(&self.ikb.new_dn_key(element_id), None).await?;
				let element: Option<DiskAnnElement> =
					tx.get_key(&self.ikb.new_de_key(element_id), None).await?;
				if let Some(node) = node {
					dump.insert(element_id, (node.neighbors, element.map(|e| e.vector)));
				}
			}
			tx.cancel().await?;
			Ok(dump)
		}
	}

	/// Element id → (adjacency list, vector), as persisted.
	type GraphDump = BTreeMap<ElementId, (Vec<ElementId>, Option<SerializedVector>)>;

	/// What one compaction left behind in KV.
	struct CompactionObservation {
		/// `!ds.next_element_id` before and after the compaction. A compaction only ever
		/// allocates, so the second value must be at least the first.
		next_element_id: (ElementId, ElementId),
		/// Elements whose persisted vector the compaction replaced. A compaction inserting new
		/// vectors allocates fresh ids, so it must replace none.
		overwritten_elements: Vec<ElementId>,
		/// Adjacency edges the compaction dropped, excluding those leaving an element in
		/// `overwritten_elements`: an element the compaction allocated over was rewritten
		/// wholesale, so its edges going missing measures the id collision rather than a stale
		/// adjacency read. What is left is what a read-modify-write of an adjacency list lost.
		dropped_edges: Vec<(ElementId, ElementId)>,
		/// Batched point reads B's compaction frame sent to its read view: none on an engine
		/// that gives no view.
		b_view_reads: u64,
	}

	/// Node A builds a graph, node B warms its cache over it, node A commits a further generation,
	/// and then node B compacts one more vector. Returns what B's compaction did to KV.
	///
	/// `keep_b_cache_warm` selects the control: when false, B's cache is dropped at the point A's
	/// foreign generation lands, which is the only difference between the two runs.
	async fn two_node_compaction(
		ds: TestIndexStore,
		keep_b_cache_warm: bool,
	) -> Result<CompactionObservation> {
		let nodes = TwoNodes::new(ds).await?;
		for i in 1..=12i64 {
			let v = i as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;

		// One query per vector, so B caches the graph state and the adjacency of every element a
		// search can reach. This is an ordinary read-only query path: nothing here is contrived.
		for i in 1..=12i64 {
			let v = i as f32;
			knn_len_with_k(&nodes.b, &nodes.ds, &[v, v, v, v], 4).await?;
		}

		// The foreign generation. A's compaction allocates eight elements interleaved with the
		// first twelve and writes back-edges to them into the adjacency lists B has cached.
		for i in 13..=20i64 {
			let v = (i - 12) as f32 + 0.5;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;
		if !keep_b_cache_warm {
			nodes.clear_b_cache().await;
		}

		let before_id = nodes.next_element_id().await?;
		// Dump past the current allocation point so elements stay visible even if the compaction
		// rewinds `next_element_id`.
		let limit = before_id + 4;
		let before = nodes.graph_dump(limit).await?;

		nodes.write(21, None, Some(&[6.25f32, 6.25, 6.25, 6.25])).await?;
		nodes.compact_on(&nodes.b).await?;

		let after_id = nodes.next_element_id().await?;
		let after = nodes.graph_dump(limit).await?;
		let overwritten_elements: Vec<ElementId> = before
			.iter()
			.filter(|(id, (_, vector))| after.get(id).is_some_and(|(_, current)| current != vector))
			.map(|(id, _)| *id)
			.collect();
		let mut dropped_edges = Vec::new();
		for (from, (neighbors, _)) in &before {
			if overwritten_elements.contains(from) {
				continue;
			}
			let current = after.get(from).map(|(neighbors, _)| neighbors);
			for to in neighbors {
				if !current.is_some_and(|current| current.contains(to)) {
					dropped_edges.push((*from, *to));
				}
			}
		}
		let b_view_reads = nodes
			.b
			.last_frame()
			.map_or(0, |frame| frame.stats.reads_view.load(std::sync::atomic::Ordering::Acquire));
		Ok(CompactionObservation {
			next_element_id: (before_id, after_id),
			overwritten_elements,
			dropped_edges,
			b_view_reads,
		})
	}

	/// Nearest neighbour each node reports for one query point.
	struct QueryObservation {
		a: Vec<i64>,
		b: Vec<i64>,
		a_distance: Option<f64>,
		b_distance: Option<f64>,
	}

	/// Node B caches document `1` at vector `v`, node A moves that document's vector far away and
	/// compacts, and both nodes are then queried at `v`.
	async fn two_node_moved_document(
		ds: TestIndexStore,
		keep_b_cache_warm: bool,
	) -> Result<QueryObservation> {
		let nodes = TwoNodes::new(ds).await?;
		for i in 1..=8i64 {
			let v = i as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;
		let query = [1.0f32, 1.0, 1.0, 1.0];
		assert_eq!(
			knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 1, None).await?,
			vec![1],
			"the run needs B's cache warmed with record 1 at the query point"
		);

		nodes.write(1, Some(&query), Some(&[900.0f32, 900.0, 900.0, 900.0])).await?;
		nodes.compact_on(&nodes.a).await?;
		if !keep_b_cache_warm {
			nodes.clear_b_cache().await;
		}

		Ok(QueryObservation {
			a: knn_keys_with_allow(&nodes.a, &nodes.ds, &query, 1, None).await?,
			a_distance: knn_nearest(&nodes.a, &nodes.ds, &query).await?,
			b: knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 1, None).await?,
			b_distance: knn_nearest(&nodes.b, &nodes.ds, &query).await?,
		})
	}

	/// Node B caches a graph of eight distant vectors, node A inserts four much nearer ones and
	/// compacts, and both nodes are then queried at the origin. `k` is larger than the whole index
	/// on the second query, so a result that still omits the new vectors omits them at every `ef`.
	async fn two_node_new_vectors(
		ds: TestIndexStore,
		keep_b_cache_warm: bool,
	) -> Result<(QueryObservation, Vec<i64>)> {
		let nodes = TwoNodes::new(ds).await?;
		for i in 1..=8i64 {
			let v = 100.0 + i as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;
		let query = [0.0f32, 0.0, 0.0, 0.0];
		assert_eq!(
			knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 4, None).await?,
			vec![1, 2, 3, 4],
			"the run needs B's cache warmed over the pre-existing graph"
		);

		for i in 9..=12i64 {
			let v = (i - 8) as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;
		if !keep_b_cache_warm {
			nodes.clear_b_cache().await;
		}

		let observed = QueryObservation {
			a: knn_keys_with_allow(&nodes.a, &nodes.ds, &query, 4, None).await?,
			a_distance: knn_nearest(&nodes.a, &nodes.ds, &query).await?,
			b: knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 4, None).await?,
			b_distance: knn_nearest(&nodes.b, &nodes.ds, &query).await?,
		};
		let b_exhaustive = knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 12, None).await?;
		Ok((observed, b_exhaustive))
	}

	/// The record keys a KNN search through `ctx` returns, in ascending order.
	async fn knn_keys_in(
		index: &DiskAnnIndex,
		ctx: &TestIndexEnv,
		values: &[f32],
		k: usize,
	) -> Result<Vec<i64>> {
		let query = f32_query(values);
		let res = index.knn_search(ctx, &query, k, 8, None, None).await?;
		let mut keys: Vec<i64> = res
			.iter()
			.map(|(rid, _, _)| match &rid.key {
				RecordIdKey::Number(n) => *n,
				other => panic!("unexpected record key: {other:?}"),
			})
			.collect();
		keys.sort();
		Ok(keys)
	}

	/// Which wrapper the late reader in [`single_node_late_publish`] searches through.
	#[derive(Clone, Copy)]
	enum LateReader {
		/// The wrapper that compacts.
		SameWrapper,
		/// A second wrapper over the same cache, standing in for one the registry dropped while a
		/// search still held it.
		DroppedWrapper,
	}

	/// One node, one cache. A reader opens its snapshot, the node compacts four more vectors into
	/// the graph, eviction empties every entry that compaction wrote through, and only then does
	/// the reader search — publishing what its snapshot holds. Returns what a reader opened after
	/// the compaction finds, and the edges a further compaction on the same node drops.
	async fn single_node_late_publish(
		ds: TestIndexStore,
		reader: LateReader,
	) -> Result<(Vec<i64>, Vec<(ElementId, ElementId)>)> {
		let nodes = TwoNodes::new(ds).await?;
		let dropped = match reader {
			LateReader::SameWrapper => None,
			LateReader::DroppedWrapper => Some(
				new_index(nodes.ikb.clone(), TableId(4), &two_node_params(), nodes.b_cache.clone())
					.await?,
			),
		};
		let late_index = dropped.as_ref().unwrap_or(&nodes.b);
		for i in 1..=8i64 {
			let v = 100.0 + i as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.b).await?;
		let late = new_ctx(&nodes.ds, TransactionType::Read).await;

		for i in 9..=12i64 {
			let v = (i - 8) as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.b).await?;
		nodes.clear_b_cache().await;

		let query = [0.0f32, 0.0, 0.0, 0.0];
		assert_eq!(
			knn_keys_in(late_index, &late, &query, 12).await?,
			(1..=8).collect::<Vec<_>>(),
			"the late reader's snapshot holds only the first eight vectors"
		);
		late.tx().cancel().await?;

		let found = knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 12, None).await?;
		let limit = nodes.next_element_id().await? + 4;
		let before = nodes.graph_dump(limit).await?;
		nodes.write(13, None, Some(&[0.5, 0.5, 0.5, 0.5])).await?;
		nodes.compact_on(&nodes.b).await?;
		let after = nodes.graph_dump(limit).await?;
		let dropped = before
			.iter()
			.flat_map(|(id, (neighbors, _))| {
				let kept =
					after.get(id).map(|(neighbors, _)| neighbors.clone()).unwrap_or_default();
				neighbors.iter().filter(move |n| !kept.contains(n)).map(move |n| (*id, *n))
			})
			.collect();
		Ok((found, dropped))
	}

	/// A reader opened after a compaction reaches the vectors that compaction added, however late
	/// an older reader on the same node publishes what its snapshot held.
	#[tokio::test]
	async fn diskann_late_publish_leaves_new_vectors_reachable() -> Result<()> {
		let (found, _) =
			single_node_late_publish(TestIndexStore::new().await, LateReader::SameWrapper).await?;
		assert_eq!(found, (1..=12).collect::<Vec<_>>(), "every committed vector is reachable");
		Ok(())
	}

	/// A compaction keeps every edge the previous compaction on the same node committed, however
	/// late an older reader publishes what its snapshot held.
	#[tokio::test]
	async fn diskann_late_publish_leaves_committed_edges_in_place() -> Result<()> {
		let (_, dropped) =
			single_node_late_publish(TestIndexStore::new().await, LateReader::SameWrapper).await?;
		assert!(dropped.is_empty(), "the compaction dropped committed edges: {dropped:?}");
		Ok(())
	}

	/// The late publish comes from a wrapper the registry dropped, sharing the cache its
	/// replacement compacts through but not its graph lock: a reader opened after the compaction
	/// still reaches the vectors it added.
	#[tokio::test]
	async fn diskann_dropped_wrapper_late_publish_leaves_new_vectors_reachable() -> Result<()> {
		let (found, _) =
			single_node_late_publish(TestIndexStore::new().await, LateReader::DroppedWrapper)
				.await?;
		assert_eq!(found, (1..=12).collect::<Vec<_>>(), "every committed vector is reachable");
		Ok(())
	}

	/// The same late publish from a dropped wrapper: the replacement's next compaction keeps every
	/// edge its previous one committed.
	#[tokio::test]
	async fn diskann_dropped_wrapper_late_publish_leaves_committed_edges_in_place() -> Result<()> {
		let (_, dropped) =
			single_node_late_publish(TestIndexStore::new().await, LateReader::DroppedWrapper)
				.await?;
		assert!(dropped.is_empty(), "the compaction dropped committed edges: {dropped:?}");
		Ok(())
	}

	/// The `!dg` generation, and every persisted adjacency list and vector, as a reader opened now
	/// sees them.
	async fn committed_graph(
		ds: &TestIndexStore,
		ikb: &IndexKeyBase,
	) -> Result<(Option<u64>, BTreeMap<ElementId, (Vec<ElementId>, SerializedVector)>)> {
		let tx = ds.transaction(TransactionType::Read).await?;
		let generation = read_compaction_generation(&tx, &ikb.new_dg_key()).await?;
		let state: DiskAnnState = tx.get_key(&ikb.new_ds_key(), None).await?.unwrap_or_default();
		let mut graph = BTreeMap::new();
		for element_id in 0..state.next_element_id {
			let node: Option<DiskAnnNode> = tx.get_key(&ikb.new_dn_key(element_id), None).await?;
			let element: Option<DiskAnnElement> =
				tx.get_key(&ikb.new_de_key(element_id), None).await?;
			if let (Some(node), Some(element)) = (node, element) {
				graph.insert(element_id, (node.neighbors, element.vector));
			}
		}
		tx.cancel().await?;
		Ok((generation, graph))
	}

	/// A compaction on this process costs the cache only the entries it changed. Every element a
	/// reader cached before it is still served after it, since a compaction never rewrites an
	/// existing vector, and so is every adjacency list it left as it was. An adjacency list it did
	/// change is served, if at all, as it committed it.
	#[tokio::test]
	async fn diskann_local_compaction_keeps_untouched_entries_served() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let cache = cache();
		let index = new_index(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache.clone(),
		)
		.await?;
		for i in 1..=48i64 {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let v = [i as f32, 0.0, 0.0, 0.0];
			index.index(&ctx, &RecordIdKey::Number(i), None, Some(f32_content(&v))).await?;
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}
		for i in 1..=48i64 {
			knn_len_with_k(&index, &ds, &[i as f32, 0.0, 0.0, 0.0], 4).await?;
		}
		let cache_index = (ikb.ns(), ikb.db(), TableId(4), ikb.index(), DOC_IDS_FORMAT_VERSION);
		let (generation, before) = committed_graph(&ds, &ikb).await?;
		let view = index.graph.read().await.reader_view(generation);
		let cached_elements: Vec<ElementId> = before
			.keys()
			.copied()
			.filter(|id| cache.get_element(cache_index, *id, &view).is_some())
			.collect();
		let cached_nodes: Vec<ElementId> = before
			.keys()
			.copied()
			.filter(|id| cache.get_node(cache_index, *id, &view).is_some())
			.collect();
		assert!(!cached_elements.is_empty() && !cached_nodes.is_empty(), "the queries cached");

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let far = [1000.0, 0.0, 0.0, 0.0];
			index.index(&ctx, &RecordIdKey::Number(49), None, Some(f32_content(&far))).await?;
			ctx.tx().commit().await?;
		}
		while compact_once(&index, &ds, &ikb).await? {}
		let (later, after) = committed_graph(&ds, &ikb).await?;
		assert!(later > generation, "the compaction committed a generation");
		let view = index.graph.read().await.reader_view(later);
		for id in &cached_elements {
			assert_eq!(
				cache.get_element(cache_index, *id, &view).map(|e| e.vector.clone()),
				Some(after[id].1.clone()),
				"element {id} is no longer served"
			);
		}
		let mut untouched = 0;
		for id in &cached_nodes {
			let served = cache.get_node(cache_index, *id, &view).map(|n| n.neighbors.clone());
			if before[id].0 == after[id].0 {
				assert_eq!(
					served,
					Some(after[id].0.clone()),
					"untouched adjacency of {id} is gone"
				);
				untouched += 1;
			} else if let Some(served) = served {
				assert_eq!(served, after[id].0, "adjacency of {id} is served as it was");
			}
		}
		assert!(untouched > 0, "the compaction changed every cached adjacency list");
		Ok(())
	}

	/// A compaction on a node whose cache was warmed before another node's generation keeps every
	/// edge that generation committed: nothing it reads to rewrite an adjacency list comes from an
	/// entry of an older generation.
	#[tokio::test]
	async fn diskann_foreign_generation_compaction_keeps_foreign_edges() -> Result<()> {
		let observed = two_node_compaction(TestIndexStore::new().await, true).await?;
		assert!(
			observed.dropped_edges.is_empty(),
			"compaction dropped adjacency edges committed by the other node: {:?}",
			observed.dropped_edges
		);
		Ok(())
	}

	/// The same compaction with node B's cache dropped first: what the cache costs, isolated.
	#[tokio::test]
	async fn diskann_cold_cache_compaction_keeps_foreign_edges() -> Result<()> {
		let observed = two_node_compaction(TestIndexStore::new().await, false).await?;
		assert!(
			observed.dropped_edges.is_empty(),
			"compaction dropped adjacency edges committed by the other node: {:?}",
			observed.dropped_edges
		);
		Ok(())
	}

	/// A compaction must not re-allocate element ids another process already used.
	///
	/// `!ds` carries the allocator alongside the entry point, so a compaction incrementing an
	/// older `next_element_id` would overwrite the elements behind the ids it reissued and
	/// persist a rewound counter, leaving every later allocation on this index to collide again.
	#[tokio::test]
	async fn diskann_foreign_generation_compaction_does_not_reuse_element_ids() -> Result<()> {
		let observed = two_node_compaction(TestIndexStore::new().await, true).await?;
		let (before, after) = observed.next_element_id;
		assert!(after >= before, "compaction rewound next_element_id from {before} to {after}");
		assert!(
			observed.overwritten_elements.is_empty(),
			"compaction overwrote the vectors of elements another node allocated: {:?}",
			observed.overwritten_elements
		);
		Ok(())
	}

	/// The same compaction with node B's cache dropped first.
	#[tokio::test]
	async fn diskann_cold_cache_compaction_does_not_reuse_element_ids() -> Result<()> {
		let observed = two_node_compaction(TestIndexStore::new().await, false).await?;
		let (before, after) = observed.next_element_id;
		assert!(after >= before, "compaction rewound next_element_id from {before} to {after}");
		assert!(
			observed.overwritten_elements.is_empty(),
			"compaction overwrote the vectors of elements another node allocated: {:?}",
			observed.overwritten_elements
		);
		Ok(())
	}

	/// A document whose vector moved on another node is scored against the vector it has now: the
	/// element and doc set a node cached before the move are not served after it.
	#[tokio::test]
	async fn diskann_foreign_generation_query_scores_moved_document_correctly() -> Result<()> {
		let observed = two_node_moved_document(TestIndexStore::new().await, true).await?;
		assert_eq!(
			(&observed.b, observed.b_distance),
			(&observed.a, observed.a_distance),
			"the two nodes disagree about the nearest neighbour of a point one of them moved a \
			 document away from"
		);
		Ok(())
	}

	/// The same query with node B's cache dropped first.
	#[tokio::test]
	async fn diskann_cold_cache_query_scores_moved_document_correctly() -> Result<()> {
		let observed = two_node_moved_document(TestIndexStore::new().await, false).await?;
		assert_eq!(
			(&observed.b, observed.b_distance),
			(&observed.a, observed.a_distance),
			"the two nodes disagree about the nearest neighbour of a point one of them moved a \
			 document away from"
		);
		Ok(())
	}

	/// Vectors another node committed are reachable, at any `k`. The other node's compaction has
	/// already drained the pendings the lookup path would merge, so they are reachable only through
	/// adjacency of their own generation, never through lists cached before it.
	#[tokio::test]
	async fn diskann_foreign_generation_query_reaches_new_vectors() -> Result<()> {
		let (observed, b_exhaustive) =
			two_node_new_vectors(TestIndexStore::new().await, true).await?;
		assert_eq!(
			observed.b, observed.a,
			"the two nodes disagree about the nearest neighbours after one of them inserted \
			 vectors at the query point"
		);
		assert_eq!(
			b_exhaustive,
			(1..=12).collect::<Vec<i64>>(),
			"a k larger than the index still did not reach the vectors the other node committed"
		);
		Ok(())
	}

	/// Node B commits, and warms its cache over its own generation, before node A commits one of
	/// its own vectors near the query point. With `b_commits_again`, B then compacts a vector of
	/// its own on the far side of A's, which rewrites only adjacency lists A created and leaves
	/// those B cached untouched. Returns what an exhaustive query through B finds at the end.
	async fn foreign_generation_over_a_local_span(
		ds: TestIndexStore,
		b_commits_again: bool,
	) -> Result<Vec<i64>> {
		let nodes = TwoNodes::new(ds).await?;
		for i in 1..=8i64 {
			let v = 100.0 + i as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.b).await?;
		let query = [0.0f32, 0.0, 0.0, 0.0];
		assert_eq!(
			knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 4, None).await?,
			vec![1, 2, 3, 4],
			"the run needs B's cache warmed over its own generation"
		);
		for i in 9..=12i64 {
			let v = (i - 8) as f32;
			nodes.write(i, None, Some(&[v, v, v, v])).await?;
		}
		nodes.compact_on(&nodes.a).await?;
		if b_commits_again {
			nodes.write(13, None, Some(&[-50.0, -50.0, -50.0, -50.0])).await?;
			nodes.compact_on(&nodes.b).await?;
		}
		knn_keys_with_allow(&nodes.b, &nodes.ds, &query, 13, None).await
	}

	/// A reader at a generation another node committed is beyond the span B's own commit opened,
	/// so nothing B cached inside that span is served to it: it reaches the vectors the other node
	/// added, through adjacency lists B had cached without them.
	#[tokio::test]
	async fn diskann_foreign_generation_over_a_local_span_query_reaches_new_vectors() -> Result<()>
	{
		let found =
			foreign_generation_over_a_local_span(TestIndexStore::new().await, false).await?;
		assert_eq!(found, (1..=12).collect::<Vec<i64>>(), "B misses the vectors A committed");
		Ok(())
	}

	/// B's next compaction finds A's generation where its span's head should be and reopens the
	/// span there, rather than stretching it across a commit it did not make: a reader after B's
	/// commit still reaches the vectors A added.
	#[tokio::test]
	async fn diskann_local_commit_over_a_foreign_generation_query_reaches_new_vectors() -> Result<()>
	{
		let found = foreign_generation_over_a_local_span(TestIndexStore::new().await, true).await?;
		assert_eq!(
			found,
			(1..=13).collect::<Vec<i64>>(),
			"B stopped reaching the vectors A committed once B committed on top of them"
		);
		Ok(())
	}

	/// The same query with node B's cache dropped first.
	#[tokio::test]
	async fn diskann_cold_cache_query_reaches_new_vectors() -> Result<()> {
		let (observed, b_exhaustive) =
			two_node_new_vectors(TestIndexStore::new().await, false).await?;
		assert_eq!(
			observed.b, observed.a,
			"the two nodes disagree about the nearest neighbours after one of them inserted \
			 vectors at the query point"
		);
		assert_eq!(
			b_exhaustive,
			(1..=12).collect::<Vec<i64>>(),
			"a k larger than the index still did not reach the vectors the other node committed"
		);
		Ok(())
	}

	/// Every scenario above against one on-disk RocksDB store, so none of them depends on the
	/// in-memory backend's visibility rules: what they pin lives in the process-local cache.
	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test]
	async fn diskann_foreign_generation_rocksdb() -> Result<()> {
		async fn store(dir: &TempDir) -> TestIndexStore {
			TestIndexStore::with_path(&format!("rocksdb:{}", dir.path().to_string_lossy())).await
		}

		let compaction_dir = TempDir::new()?;
		let compaction = two_node_compaction(store(&compaction_dir).await, true).await?;
		let (before, after) = compaction.next_element_id;
		assert!(after >= before, "compaction rewound next_element_id from {before} to {after}");
		assert!(
			compaction.overwritten_elements.is_empty(),
			"compaction overwrote the vectors of elements another node allocated: {:?}",
			compaction.overwritten_elements
		);
		assert!(
			compaction.dropped_edges.is_empty(),
			"compaction dropped adjacency edges committed by the other node: {:?}",
			compaction.dropped_edges
		);

		let moved_dir = TempDir::new()?;
		let moved = two_node_moved_document(store(&moved_dir).await, true).await?;
		assert_eq!(
			(&moved.b, moved.b_distance),
			(&moved.a, moved.a_distance),
			"the two nodes disagree about the nearest neighbour of a point one of them moved a \
			 document away from"
		);

		let inserted_dir = TempDir::new()?;
		let (inserted, b_exhaustive) =
			two_node_new_vectors(store(&inserted_dir).await, true).await?;
		assert_eq!(
			inserted.b, inserted.a,
			"the two nodes disagree about the nearest neighbours after one of them inserted \
			 vectors at the query point"
		);
		assert_eq!(
			b_exhaustive,
			(1..=12).collect::<Vec<i64>>(),
			"a k larger than the index still did not reach the vectors the other node committed"
		);
		Ok(())
	}

	/// #1177: two handles over one store, each with its own cache. B warms its cache with
	/// queries, A commits a frame, and B's next frame — reading clean keys through its view on an
	/// engine that gives one — keeps every edge A committed. Then, on one node, a sequence of
	/// frames between queries clears no cache entry and opens the span once, at the first.
	async fn frames_keep_foreign_edges_and_invalidate_nothing_on_one_node(
		two_node_ds: TestIndexStore,
		one_node_ds: TestIndexStore,
		with_view: bool,
	) -> Result<()> {
		let compaction = two_node_compaction(two_node_ds, true).await?;
		assert!(compaction.overwritten_elements.is_empty());
		assert!(
			compaction.dropped_edges.is_empty(),
			"B's frame dropped edges A committed: {:?}",
			compaction.dropped_edges
		);
		assert_eq!(compaction.b_view_reads > 0, with_view, "B's frame read through a view");

		let cache = cache();
		let p = two_node_params();
		let index = new_index(ikb(), TableId(4), &p, cache.clone()).await?;
		for round in 0..5i64 {
			let ctx = new_ctx(&one_node_ds, TransactionType::Write).await;
			for i in 0..8 {
				let id = round * 8 + i;
				let v = id as f32;
				index
					.index(
						&ctx,
						&RecordIdKey::Number(id),
						None,
						Some(f32_content(&[v, v, 0.0, 0.0])),
					)
					.await?;
			}
			ctx.tx().commit().await?;
			assert!(compact_once(&index, &one_node_ds, &ikb()).await?);
			knn_len_with_k(&index, &one_node_ds, &[round as f32, 0.0, 0.0, 0.0], 4).await?;
		}
		assert_eq!(cache.clears(), 0, "a frame on a single node cleared the cache");
		assert_eq!(
			index.graph.read().await.span_opens,
			1,
			"the span was reopened after the first frame"
		);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_frames_keep_foreign_edges_and_invalidate_nothing_on_one_node() -> Result<()> {
		frames_keep_foreign_edges_and_invalidate_nothing_on_one_node(
			TestIndexStore::new().await,
			TestIndexStore::new().await,
			false,
		)
		.await
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test]
	async fn diskann_frames_keep_foreign_edges_and_invalidate_nothing_on_one_node_rocksdb()
	-> Result<()> {
		async fn store(dir: &TempDir) -> TestIndexStore {
			TestIndexStore::with_path(&format!("rocksdb:{}", dir.path().to_string_lossy())).await
		}
		let (two, one) = (TempDir::new()?, TempDir::new()?);
		frames_keep_foreign_edges_and_invalidate_nothing_on_one_node(
			store(&two).await,
			store(&one).await,
			true,
		)
		.await
	}

	/// A record key with its vector before and after one write; `None` is no vector.
	type SharedVectorWrite = (i64, Option<[f32; 4]>, Option<[f32; 4]>);

	/// A vector shared by more records than the inline document-set variants
	/// hold keeps every one of them: the set grows into, and shrinks back out
	/// of, its bitmap form without losing a write, whether each record is
	/// compacted on its own or all in one batch.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_vector_shared_by_many_records_keeps_every_record() -> Result<()> {
		for (hashed, per_record) in [(false, true), (false, false), (true, true), (true, false)] {
			let ds = TestIndexStore::new().await;
			let ikb = ikb();
			let mut p = params(VectorType::F32, Distance::Euclidean);
			p.use_hashed_vector = hashed;
			let index = new_index(ikb.clone(), TableId(4), &p, cache()).await?;
			let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
			let shared = [3.0f32; 4];
			let apply = |writes: Vec<SharedVectorWrite>| {
				let (ds, index, docids, ikb) = (&ds, &index, &docids, &ikb);
				async move {
					let mut pending = writes.into_iter().peekable();
					while pending.peek().is_some() {
						let ctx = new_ctx(ds, TransactionType::Write).await;
						for (key, old, new) in pending.by_ref().take(if per_record {
							1
						} else {
							usize::MAX
						}) {
							let id = RecordIdKey::Number(key);
							if new.is_some() {
								docids.resolve_or_assign(&ctx, &id).await?;
							}
							index
								.index(
									&ctx,
									&id,
									old.as_ref().map(|v| f32_content(v)),
									new.as_ref().map(|v| f32_content(v)),
								)
								.await?;
							if new.is_none() {
								docids.remove(&ctx.tx(), &id).await?;
							}
						}
						ctx.tx().commit().await?;
						compact_all(index, ds, ikb).await?;
					}
					anyhow::Ok(())
				}
			};
			// A distinct vector alongside, so the graph has more than one element.
			let mut writes = vec![(100i64, None, Some([9.0f32; 4]))];
			writes.extend((1..=20).map(|key| (key, None, Some(shared))));
			apply(writes).await?;
			let case = format!("hashed {hashed}, per record {per_record}");
			let mut all: Vec<i64> = (1..=20).collect();
			all.push(100);
			assert_eq!(knn_keys_with_allow(&index, &ds, &shared, 50, None).await?, all, "{case}");
			// Remove 15 of the 20, crossing back out of the bitmap form.
			apply((1..=15).map(|key| (key, Some(shared), None)).collect()).await?;
			let mut left: Vec<i64> = (16..=20).collect();
			left.push(100);
			assert_eq!(knn_keys_with_allow(&index, &ds, &shared, 50, None).await?, left, "{case}");
			// And the rest, which removes the shared element itself.
			apply((16..=20).map(|key| (key, Some(shared), None)).collect()).await?;
			assert_eq!(
				knn_keys_with_allow(&index, &ds, &shared, 50, None).await?,
				vec![100],
				"{case}"
			);
		}
		Ok(())
	}

	/// Records whose ids differ only in a number's kind (`[1]` and `[1dec]`) share
	/// one pending key and one shard. On an index stamped to keep a pending update
	/// for every record its key spells, each keeps its own: both reach the graph
	/// from one compaction, and an update to one and a delete of the other in one
	/// window leave the survivor's new vector, with no trace of the deleted record.
	///
	/// Comparisons are on encoded record keys, not `==`, which compares the
	/// numbers across kinds.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_records_sharing_a_pending_key_keep_their_own_vectors() -> Result<()> {
		fn record_key(id: &RecordIdKey) -> Vec<u8> {
			storekey::encode_vec(id).unwrap()
		}
		async fn nearest(
			index: &DiskAnnIndex,
			ds: &TestIndexStore,
			at: [f32; 4],
			k: usize,
		) -> Result<Vec<(Vec<u8>, f64)>> {
			let ctx = new_ctx(ds, TransactionType::Read).await;
			let pt: Vec<Number> = at.iter().map(|v| Number::Float(*v as f64)).collect();
			let res = index.knn_search(&ctx, &pt, k, 100, None, None).await?;
			ctx.tx().cancel().await?;
			Ok(res.iter().map(|(rid, d, _)| (record_key(&rid.key), *d)).collect())
		}

		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&p,
			cache(),
			crate::catalog::INDEX_FORMAT_VERSION,
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		let int = RecordIdKey::Array(vec![Value::Number(Number::Int(1))].into());
		let dec = RecordIdKey::Array(vec![Value::Number(Number::Decimal(1.into()))].into());
		assert_eq!(
			DiskAnnIndex::pending_state_shard(&int),
			DiskAnnIndex::pending_state_shard(&dec),
			"the pair shares a shard"
		);
		let (near, far, moved) = ([1.0f32; 4], [9.0f32; 4], [5.0f32; 4]);

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for (id, v) in [(&int, near), (&dec, far)] {
				docids.resolve_or_assign(&ctx, id).await?;
				index.index(&ctx, id, None, Some(f32_content(&v))).await?;
			}
			ctx.tx().commit().await?;
		}
		// Read from the pending queue, then from the graph.
		for compacted in [false, true] {
			if compacted {
				compact_all(&index, &ds, &ikb).await?;
			}
			let keys = |r: Vec<(Vec<u8>, f64)>| r.into_iter().map(|(k, _)| k).collect::<Vec<_>>();
			assert_eq!(
				keys(nearest(&index, &ds, near, 2).await?),
				vec![record_key(&int), record_key(&dec)],
				"compacted={compacted}"
			);
			assert_eq!(
				keys(nearest(&index, &ds, far, 2).await?),
				vec![record_key(&dec), record_key(&int)],
				"compacted={compacted}"
			);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &int, Some(f32_content(&near)), Some(f32_content(&moved))).await?;
			index.index(&ctx, &dec, Some(f32_content(&far)), None).await?;
			docids.remove(&ctx.tx(), &dec).await?;
			ctx.tx().commit().await?;
		}
		for compacted in [false, true] {
			if compacted {
				compact_all(&index, &ds, &ikb).await?;
			}
			let hits = nearest(&index, &ds, moved, 2).await?;
			assert_eq!(hits.len(), 1, "compacted={compacted}: {hits:?}");
			assert_eq!(hits[0].0, record_key(&int));
			assert_eq!(hits[0].1, 0.0, "the survivor holds its new vector, compacted={compacted}");
		}
		Ok(())
	}

	/// Checks one [`DiskAnnIndex::exact_knn_search`] against brute force over
	/// the records' current vectors: every returned record is allowed and
	/// carries its true distance, and the returned distances are exactly the
	/// `k` smallest among the allowed records.
	async fn assert_exact_matches_brute_force(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		live: &BTreeMap<i64, (DocId, [f32; 4])>,
		allow: &RoaringTreemap,
		query: [f32; 4],
		k: usize,
		tt: TransactionType,
	) -> Result<()> {
		let ctx = new_ctx(ds, tt).await;
		let pt = f32_query(&query);
		let res = index
			.exact_knn_search(&ctx, &pt, k, allow)
			.await?
			.expect("a read-only snapshot is served");
		ctx.tx().cancel().await?;
		let q = Vector::try_from_vector(VectorType::F32, &pt)?;
		let dist = |v: &[f32; 4]| {
			Distance::Euclidean
				.calculate(&q, &Vector::try_from_vector(VectorType::F32, &f32_query(v)).unwrap())
		};
		let mut truth: Vec<f64> =
			live.values().filter(|(doc, _)| allow.contains(*doc)).map(|(_, v)| dist(v)).collect();
		truth.sort_by(f64::total_cmp);
		truth.truncate(k);
		let mut seen = HashSet::new();
		let mut got = Vec::with_capacity(res.len());
		for (rid, d, _) in &res {
			let RecordIdKey::Number(key) = rid.key else {
				panic!("unexpected record key: {:?}", rid.key);
			};
			assert!(seen.insert(key), "record {key} returned twice");
			let (doc, v) = live.get(&key).unwrap_or_else(|| panic!("record {key} is not live"));
			assert!(allow.contains(*doc), "record {key} is not in the allow-list");
			assert_eq!(*d, dist(v), "distance of record {key}");
			got.push(*d);
		}
		assert!(got.is_sorted_by(|a, b| a <= b), "results not nearest first: {got:?}");
		assert_eq!(got, truth, "k = {k}, allow = {allow:?}");
		Ok(())
	}

	/// Queries a few allow-lists and `k`s against the current state of `live`.
	async fn assert_exact_round(
		index: &DiskAnnIndex,
		ds: &TestIndexStore,
		live: &BTreeMap<i64, (DocId, [f32; 4])>,
		rng: &mut SmallRng,
	) -> Result<()> {
		let docs: Vec<DocId> = live.values().map(|(d, _)| *d).collect();
		for share in [0.05, 0.3, 1.0] {
			let mut allow: RoaringTreemap =
				docs.iter().copied().filter(|_| rng.random_bool(share)).collect();
			// A doc-ID with no live record must be ignored.
			allow.insert(1_000_000);
			for k in [1, 5, 500] {
				let query = [0; 4].map(|_: u8| rng.random_range(0..8) as f32);
				assert_exact_matches_brute_force(
					index,
					ds,
					live,
					&allow,
					query,
					k,
					TransactionType::Read,
				)
				.await?;
			}
		}
		Ok(())
	}

	/// With no member to offer (an empty allow-list or `k = 0`) the exact
	/// tier answers no neighbours without reading the index, but only once
	/// the query vector has been checked: a vector of the wrong dimension
	/// still fails.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_exact_knn_search_with_nothing_to_offer() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let p = params(VectorType::F32, Distance::Euclidean);
		let index =
			DiskAnnIndex::new(ikb(), TableId(4), &p, cache(), VECTOR_DOC_ELEMENTS_FORMAT_VERSION)
				.await?;
		let some: RoaringTreemap = [1u64, 2, 3].into_iter().collect();
		let none = RoaringTreemap::new();
		let pt = f32_query(&[1.0; 4]);
		let wrong = f32_query(&[1.0; 3]);
		for (pt, k, allow, ok) in
			[(&pt, 3, &none, true), (&pt, 0, &some, true), (&wrong, 3, &none, false)]
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let res = index.exact_knn_search(&ctx, pt, k, allow).await;
			ctx.tx().cancel().await?;
			match (res, ok) {
				(Ok(Some(r)), true) => assert!(r.is_empty(), "k = {k}"),
				(Err(_), false) => {}
				(other, _) => panic!("k = {k}, dimension {}: {other:?}", pt.len()),
			}
		}
		Ok(())
	}

	/// An environment that records, at every counted check, whether the
	/// index's graph lock could be taken for writing at that moment.
	struct GraphLockProbe<'a> {
		inner: TestIndexEnv,
		graph: &'a RwLock<DiskAnnGraph>,
		checks: std::sync::atomic::AtomicUsize,
		held: std::sync::atomic::AtomicUsize,
	}

	impl crate::catalog::providers::CancellationProbe for GraphLockProbe<'_> {
		fn expect_not_timedout(&self) -> crate::catalog::providers::BoxProviderFut<'_, Result<()>> {
			self.inner.expect_not_timedout()
		}
	}

	impl IndexEnv for GraphLockProbe<'_> {
		fn tx(&self) -> Arc<Transaction> {
			IndexEnv::tx(&self.inner)
		}

		fn is_done(&self, count: Option<usize>) -> crate::env::BoxEnvFut<'_> {
			if count.is_some() {
				use std::sync::atomic::Ordering::Relaxed;
				self.checks.fetch_add(1, Relaxed);
				if self.graph.try_write().is_err() {
					self.held.fetch_add(1, Relaxed);
				}
			}
			self.inner.is_done(count)
		}

		fn index_stores(&self) -> &crate::trees::store::IndexStores {
			self.inner.index_stores()
		}

		fn node_id(&self) -> uuid::Uuid {
			self.inner.node_id()
		}

		fn sequences(&self) -> Result<&surrealdb_datastore::sequences::Sequences> {
			self.inner.sequences()
		}

		fn config(&self) -> &crate::config::IdxConfig {
			self.inner.config()
		}

		fn places(&self) -> crate::places::Ticket {
			self.inner.places()
		}
	}

	/// The exact tier holds the graph only while it reads a batch of element
	/// vectors: not while it scans pendings, reads doc → elements mappings or
	/// scores, so a compaction could take the graph at any of those checks,
	/// pending or compacted, and the members are still scored.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_exact_knn_search_leaves_the_graph_free_while_scanning_pendings() -> Result<()>
	{
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&p,
			cache(),
			VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		let mut allow = RoaringTreemap::new();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for (key, v) in [(1i64, [5.0f32; 4]), (2, [4.0; 4]), (3, [1.0; 4])] {
				let id = RecordIdKey::Number(key);
				allow.insert(docids.resolve_or_assign(&ctx, &id).await?);
				index.index(&ctx, &id, None, Some(f32_content(&v))).await?;
			}
			ctx.tx().commit().await?;
		}
		let probe = GraphLockProbe {
			inner: new_ctx(&ds, TransactionType::Read).await,
			graph: &index.graph,
			checks: Default::default(),
			held: Default::default(),
		};
		let pt = f32_query(&[5.0; 4]);
		let res = index
			.exact_knn_search(&probe, &pt, 2, &allow)
			.await?
			.expect("a read-only snapshot is served");
		probe.inner.tx().cancel().await?;
		let got: Vec<RecordIdKey> = res.iter().map(|(rid, _, _)| rid.key.clone()).collect();
		assert_eq!(got, [RecordIdKey::Number(1), RecordIdKey::Number(2)]);
		use std::sync::atomic::Ordering::Relaxed;
		assert!(probe.checks.load(Relaxed) > 0, "the pending scan checks for cancellation");
		assert_eq!(probe.held.load(Relaxed), 0, "the graph is not held while scanning pendings");
		// Compacted, the members are scored from the graph's elements: the
		// graph is taken per batch of element reads, and is free at every
		// checkpoint around the doc → elements reads and the scoring.
		compact_all(&index, &ds, &ikb).await?;
		let probe = GraphLockProbe {
			inner: new_ctx(&ds, TransactionType::Read).await,
			graph: &index.graph,
			checks: Default::default(),
			held: Default::default(),
		};
		let res = index
			.exact_knn_search(&probe, &pt, 2, &allow)
			.await?
			.expect("a read-only snapshot is served");
		probe.inner.tx().cancel().await?;
		let got: Vec<RecordIdKey> = res.iter().map(|(rid, _, _)| rid.key.clone()).collect();
		assert_eq!(got, [RecordIdKey::Number(1), RecordIdKey::Number(2)], "compacted");
		assert!(probe.checks.load(Relaxed) > 0, "scoring checks for cancellation");
		assert_eq!(probe.held.load(Relaxed), 0, "the graph is not held across the scoring");
		Ok(())
	}

	/// An index stamped before the doc-ID → elements mapping keeps no `!dk`
	/// entries, and its exact tier declines (`None`) instead of scoring from a
	/// mapping it never wrote, so the caller scores the members' records.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_exact_knn_search_declines_an_index_without_the_mapping() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let older = VECTOR_DOC_ELEMENTS_FORMAT_VERSION - 1;
		let index = DiskAnnIndex::new(ikb.clone(), TableId(4), &p, cache(), older).await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		let mut allow = RoaringTreemap::new();
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		for key in 0..4i64 {
			let id = RecordIdKey::Number(key);
			allow.insert(docids.resolve_or_assign(&ctx, &id).await?);
			index.index(&ctx, &id, None, Some(f32_content(&[key as f32; 4]))).await?;
		}
		ctx.tx().commit().await?;
		compact_all(&index, &ds, &ikb).await?;
		let ctx = new_ctx(&ds, TransactionType::Read).await;
		for doc in &allow {
			let dk = ikb.new_dk_key(doc);
			assert!(ctx.tx().get_key(&dk, None).await?.is_none(), "an older index writes no `!dk`");
		}
		let pt = f32_query(&[0.0; 4]);
		let res = index.exact_knn_search(&ctx, &pt, 3, &allow).await?;
		ctx.tx().cancel().await?;
		assert!(res.is_none(), "an index without the mapping must be declined");
		Ok(())
	}

	/// A document indexing several vectors takes one slot in the exact tier,
	/// at its nearest vector, whether its vectors are still pending or
	/// compacted into the graph. `k` leaves room for both of record 1's
	/// vectors, so a second slot for it would show.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_exact_knn_search_offers_a_multi_vector_document_once() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&p,
			cache(),
			VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		let mut allow = RoaringTreemap::new();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			// Record 1 holds the query point and a far vector; records 2 and 3
			// one vector each.
			let records: [(i64, Vec<[f32; 4]>); 3] =
				[(1, vec![[5.0; 4], [0.0; 4]]), (2, vec![[4.0; 4]]), (3, vec![[1.0; 4]])];
			for (key, vectors) in records {
				let id = RecordIdKey::Number(key);
				allow.insert(docids.resolve_or_assign(&ctx, &id).await?);
				let content = vectors.iter().flat_map(|v| f32_content(v)).collect();
				index.index(&ctx, &id, None, Some(content)).await?;
			}
			ctx.tx().commit().await?;
		}
		let pt = f32_query(&[5.0; 4]);
		for state in ["pending", "compacted"] {
			if state == "compacted" {
				compact_all(&index, &ds, &ikb).await?;
			}
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let res = index
				.exact_knn_search(&ctx, &pt, 4, &allow)
				.await?
				.expect("a read-only snapshot is served");
			ctx.tx().cancel().await?;
			let got: Vec<(RecordIdKey, f64)> =
				res.iter().map(|(rid, d, _)| (rid.key.clone(), *d)).collect();
			let expected = vec![
				(RecordIdKey::Number(1), 0.0),
				(RecordIdKey::Number(2), 2.0),
				(RecordIdKey::Number(3), 8.0),
			];
			assert_eq!(got, expected, "{state}");
			// The graph search gives the record one slot too, pending or
			// compacted alike, so neither path returns it twice.
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let res = index.knn_search(&ctx, &pt, 4, 40, None, Some(&allow)).await?;
			ctx.tx().cancel().await?;
			let got: Vec<(RecordIdKey, f64)> =
				res.iter().map(|(rid, d, _)| (rid.key.clone(), *d)).collect();
			assert_eq!(got, expected, "graph search, {state}");
		}
		Ok(())
	}

	/// A graph search returns `k` distinct records when the elements nearest
	/// the query belong to fewer: record 1's two vectors are the two nearest
	/// elements, so the `k` nearest elements name only two records, and the
	/// third result is the next record the walk reached, also when `ef` is `k`
	/// and the search list itself names only two records.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_graph_search_fills_k_distinct_records() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = ikb();
		let p = params(VectorType::F32, Distance::Euclidean);
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&p,
			cache(),
			VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
		)
		.await?;
		let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
		let mut allow = RoaringTreemap::new();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let records: [(i64, Vec<[f32; 4]>); 3] = [
				(1, vec![[5.0; 4], [5.0, 5.0, 5.0, 4.0]]),
				(2, vec![[4.0; 4]]),
				(3, vec![[1.0; 4]]),
			];
			for (key, vectors) in records {
				let id = RecordIdKey::Number(key);
				allow.insert(docids.resolve_or_assign(&ctx, &id).await?);
				let content = vectors.iter().flat_map(|v| f32_content(v)).collect();
				index.index(&ctx, &id, None, Some(content)).await?;
			}
			ctx.tx().commit().await?;
		}
		compact_all(&index, &ds, &ikb).await?;
		let pt = f32_query(&[5.0; 4]);
		let expected = vec![
			(RecordIdKey::Number(1), 0.0),
			(RecordIdKey::Number(2), 2.0),
			(RecordIdKey::Number(3), 8.0),
		];
		for ef in [40, 3] {
			for (label, allow) in [("unfiltered", None), ("allow-list", Some(&allow))] {
				let ctx = new_ctx(&ds, TransactionType::Read).await;
				let res = index.knn_search(&ctx, &pt, 3, ef, None, allow).await?;
				ctx.tx().cancel().await?;
				let got: Vec<(RecordIdKey, f64)> =
					res.iter().map(|(rid, d, _)| (rid.key.clone(), *d)).collect();
				assert_eq!(got, expected, "{label}, ef {ef}");
			}
		}
		Ok(())
	}

	/// Exact allow-list scoring from index-stored vectors returns exactly the
	/// brute-force top-k over the records' current vectors — across a
	/// compacted graph, uncompacted updates, deletions, delete → re-create
	/// under a new doc-ID, vectors shared by several records, and a doc→element
	/// map that must be re-derived after each compaction.
	#[tokio::test(flavor = "multi_thread")]
	async fn diskann_exact_knn_search_matches_brute_force() -> Result<()> {
		for hashed in [false, true] {
			let ds = TestIndexStore::new().await;
			let ikb = ikb();
			let mut p = params(VectorType::F32, Distance::Euclidean);
			p.use_hashed_vector = hashed;
			let index = DiskAnnIndex::new(
				ikb.clone(),
				TableId(4),
				&p,
				cache(),
				VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
			)
			.await?;
			let docids = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone());
			let seed: u64 = rand::random();
			info!("Seed: {seed}");
			let mut rng = SmallRng::seed_from_u64(seed);
			// A small component range makes many records share a vector.
			let random_vec = |rng: &mut SmallRng| [0; 4].map(|_: u8| rng.random_range(0..5) as f32);
			let mut live: BTreeMap<i64, (DocId, [f32; 4])> = BTreeMap::new();
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				for key in 0..150 {
					let id = RecordIdKey::Number(key);
					let doc = docids.resolve_or_assign(&ctx, &id).await?;
					let v = random_vec(&mut rng);
					index.index(&ctx, &id, None, Some(f32_content(&v))).await?;
					live.insert(key, (doc, v));
				}
				ctx.tx().commit().await?;
			}
			// Everything pending.
			assert_exact_round(&index, &ds, &live, &mut rng).await?;
			compact_all(&index, &ds, &ikb).await?;
			// Everything compacted.
			assert_exact_round(&index, &ds, &live, &mut rng).await?;
			// A write transaction reads the mapping through itself.
			{
				let allow: RoaringTreemap = live.values().map(|(d, _)| *d).collect();
				assert_exact_matches_brute_force(
					&index,
					&ds,
					&live,
					&allow,
					[1.0; 4],
					7,
					TransactionType::Write,
				)
				.await?;
			}
			for _ in 0..3 {
				// Updates, deletions and re-creations on top of the compacted
				// graph, queried before and after they are compacted.
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let mut deleted = Vec::new();
				for key in 0..150 {
					let id = RecordIdKey::Number(key);
					match (live.get(&key).copied(), rng.random_range(0..10)) {
						(Some((doc, old)), 0..=1) => {
							let v = random_vec(&mut rng);
							index
								.index(&ctx, &id, Some(f32_content(&old)), Some(f32_content(&v)))
								.await?;
							live.insert(key, (doc, v));
						}
						(Some((_, old)), 2) => {
							index.index(&ctx, &id, Some(f32_content(&old)), None).await?;
							docids.remove(&ctx.tx(), &id).await?;
							live.remove(&key);
							deleted.push(key);
						}
						(None, 3..=5) => {
							let doc = docids.resolve_or_assign(&ctx, &id).await?;
							let v = random_vec(&mut rng);
							index.index(&ctx, &id, None, Some(f32_content(&v))).await?;
							live.insert(key, (doc, v));
						}
						_ => {}
					}
				}
				ctx.tx().commit().await?;
				assert_exact_round(&index, &ds, &live, &mut rng).await?;
				// Re-create some of this round's deletions under new doc-IDs
				// before compaction.
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				for key in deleted.into_iter().step_by(2) {
					let id = RecordIdKey::Number(key);
					let doc = docids.resolve_or_assign(&ctx, &id).await?;
					let v = random_vec(&mut rng);
					index.index(&ctx, &id, None, Some(f32_content(&v))).await?;
					live.insert(key, (doc, v));
				}
				ctx.tx().commit().await?;
				assert_exact_round(&index, &ds, &live, &mut rng).await?;
				compact_all(&index, &ds, &ikb).await?;
				assert_exact_round(&index, &ds, &live, &mut rng).await?;
			}
		}
		Ok(())
	}

	/// The DiskANN guard races of #1329 and #1332 on TiKV, which runs the
	/// writer-lease protocol natively (it is not a sole writer). Gated to the
	/// `kv-tikv` backend and requiring a running cluster at 127.0.0.1:2379 (the
	/// `tikv` CI lane). Each test wipes the cluster's keyspace on entry, so the
	/// `tikv::` tests run in one serial group (`.config/nextest.toml`).
	#[cfg(feature = "kv-tikv")]
	mod tikv {
		use super::*;
		use crate::key::schema::{RootRoot, VersionKey};

		async fn fresh_tikv_ds() -> TestIndexStore {
			let ds = TestIndexStore::with_path("tikv:127.0.0.1:2379").await;
			let tx = ds.transaction(TransactionType::Write).await.unwrap();
			tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
			tx.del_key(&VersionKey {}).await.unwrap();
			tx.commit().await.unwrap();
			ds
		}

		/// #1329's reproduction: a writer open across the passes that empty its
		/// shard stays visible once it commits.
		#[tokio::test]
		async fn pending_written_while_its_shard_empties_stays_visible() -> Result<()> {
			diskann_pending_written_while_its_shard_empties_stays_visible_on(fresh_tikv_ds().await)
				.await
		}

		/// #1332's interleaving: a writer committing while the pass that empties its
		/// shard is open does not end up under an `Empty` guard.
		#[tokio::test]
		async fn writer_committing_inside_the_emptying_pass_stays_visible() -> Result<()> {
			diskann_writer_committing_inside_the_emptying_pass_stays_visible_on(
				fresh_tikv_ds().await,
			)
			.await
		}

		/// Writers into a shard whose guard reads `Empty` both commit.
		#[tokio::test]
		async fn concurrent_writers_arming_an_empty_shard_both_commit() -> Result<()> {
			diskann_concurrent_writers_arming_an_empty_shard_both_commit_on(fresh_tikv_ds().await)
				.await
		}

		/// Overlapping writers into a shard whose guard reads `NonEmpty` both
		/// commit: neither reads the guard, and the lease the first write took
		/// covers both.
		#[tokio::test]
		async fn overlapping_writers_into_an_armed_shard_both_commit() -> Result<()> {
			let ds = fresh_tikv_ds().await;
			assert!(!ds.transaction(TransactionType::Read).await?.sole_writer());
			let index = new_index(
				ikb(),
				TableId(4),
				&params(VectorType::F32, Distance::Euclidean),
				cache(),
			)
			.await?;
			let shard = DiskAnnIndex::pending_state_shard(&RecordIdKey::Number(1));
			write_one(&index, &ds, 1).await?;
			assert_eq!(stored_guard(&ds, shard).await?, Some(DiskAnnPendingStateKind::NonEmpty));
			let first = new_ctx(&ds, TransactionType::Write).await;
			let second = new_ctx(&ds, TransactionType::Write).await;
			for (ctx, id) in [(&first, 33), (&second, 65)] {
				index
					.index(
						ctx,
						&RecordIdKey::Number(id),
						None,
						Some(f32_content(&[id as f32, 0.0, 0.0, 0.0])),
					)
					.await?;
			}
			first.tx().commit().await?;
			second.tx().commit().await?;
			assert_eq!(
				knn_keys_with_allow(&index, &ds, &[33.0, 0.0, 0.0, 0.0], 3, None).await?,
				vec![1, 33, 65]
			);
			assert_eq!(ds.guard_repairs(), 0);
			Ok(())
		}
	}

	// ------------------------------------------------------------------
	// Batched compaction frames
	// ------------------------------------------------------------------
	//
	// A frame inserts its fresh vectors through upstream `multi_insert` on the
	// places it was granted, or one at a time when granted none. These pin what
	// that may and may not change: the element, mapping and state bytes are the
	// same whatever the grant, the adjacency is valid and as good, every failure
	// a task meets fails the frame, and the frame holds no more places than it
	// was granted and none once it has ended.

	mod frames {
		use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

		use surrealdb_datastore::sequences::Sequences;
		use surrealdb_datastore::{DatastoreError, is_shutdown_error};
		use surrealdb_kvs::key::RawRange;

		use super::*;
		use crate::catalog::providers::{BoxProviderFut, CancellationProbe};
		use crate::config::IdxConfig;
		use crate::env::BoxEnvFut;
		use crate::places::Ticket;
		use crate::trees::diskann::docs::DiskAnnElementDocs;
		use crate::trees::diskann::frame::{DiskAnnFrameError, FrameFaults, Park};
		use crate::trees::store::IndexStores;

		/// A starved graph budget, so a frame's back-edges append to lists with room and its
		/// prunes have candidates to choose between.
		fn small_params(dimension: u16) -> DiskAnnParams {
			graph_params(dimension, Distance::Euclidean, 8, 24)
		}

		/// Indexes `vectors` under record ids `first..`, one record each, in one write
		/// transaction.
		async fn ingest(
			index: &DiskAnnIndex,
			ds: &TestIndexStore,
			first: i64,
			vectors: &[Vec<f32>],
		) -> Result<()> {
			let ctx = new_ctx(ds, TransactionType::Write).await;
			for (i, v) in vectors.iter().enumerate() {
				let id = RecordIdKey::Number(first + i as i64);
				index.index(&ctx, &id, None, Some(f32_content(v))).await?;
			}
			ctx.tx().commit().await
		}

		/// Prepares the next plan on a read transaction.
		async fn next_plan(
			ds: &TestIndexStore,
			ikb: &IndexKeyBase,
		) -> Result<DiskAnnCompactionPlan> {
			let ctx = new_ctx(ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, ikb).await?;
			ctx.tx().cancel().await?;
			Ok(plan)
		}

		/// The pending keys the next plan would capture.
		async fn captured(ds: &TestIndexStore, ikb: &IndexKeyBase) -> Result<usize> {
			Ok(next_plan(ds, ikb).await?.captured_keys.len())
		}

		/// Points along one axis: `n` of them `step` apart, from `start`.
		fn line(dim: usize, start: f32, step: f32, n: usize) -> Vec<Vec<f32>> {
			(0..n)
				.map(|i| {
					let mut v = vec![0.0; dim];
					v[0] = start + step * i as f32;
					v[1] = (i % 3) as f32 * 0.01;
					v
				})
				.collect()
		}

		/// Every stored element of the index.
		async fn elements(
			ds: &TestIndexStore,
			ikb: &IndexKeyBase,
		) -> Result<Vec<(ElementId, DiskAnnElement)>> {
			let tx = ds.transaction(TransactionType::Read).await?;
			let mut cursor =
				tx.open_vals_cursor(ikb.new_de_range()?, Direction::Forward, 0, None).await?;
			let mut out = Vec::new();
			loop {
				let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
				if batch.is_empty() {
					break;
				}
				for (key, value) in batch.iter() {
					let key: crate::key::schema::DiskannElementKey<'_> =
						storekey::decode_borrow(key)?;
					out.push((key.element_id, DiskAnnElement::kv_decode_value(value, ())?));
				}
			}
			drop(cursor);
			tx.cancel().await?;
			Ok(out)
		}

		/// Asserts every live element has an adjacency list, non-empty once the graph holds more
		/// than one, whose neighbours are all live elements — or, once the graph has had
		/// deletions, stored elements: a delete tombstones its element and leaves the edges to it
		/// for searches to skip. Returns the live elements.
		async fn assert_adjacency_valid(ds: &TestIndexStore, ikb: &IndexKeyBase) -> Result<usize> {
			let stored = elements(ds, ikb).await?;
			let tombstoned = stored.iter().any(|(_, e)| e.deleted);
			let reachable: HashSet<ElementId> = stored
				.iter()
				.filter(|(_, e)| tombstoned || !e.deleted)
				.map(|(id, _)| *id)
				.collect();
			let live: HashSet<ElementId> =
				stored.iter().filter(|(_, e)| !e.deleted).map(|(id, _)| *id).collect();
			let tx = ds.transaction(TransactionType::Read).await?;
			for id in &live {
				let node: Option<DiskAnnNode> = tx.get_key(&ikb.new_dn_key(*id), None).await?;
				let Some(node) = node else {
					panic!("live element {id} has no adjacency list");
				};
				if live.len() > 1 {
					assert!(!node.neighbors.is_empty(), "live element {id} has no neighbour");
				}
				for n in &node.neighbors {
					assert!(
						reachable.contains(n),
						"element {id} links to {n}, which is not stored"
					);
				}
			}
			tx.cancel().await?;
			Ok(live.len())
		}

		/// The index's own keys except its adjacency lists (`!dn`), which a frame's task count may
		/// change, and the table's doc-ID mappings (`!di`, `!dd`, `!dj`). The table's doc-ID
		/// sequence state is left out: it names the node that allocated.
		async fn state_without_adjacency(
			ds: &TestIndexStore,
			ikb: &IndexKeyBase,
		) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
			let ds_key = ikb.new_ds_key().encode_key()?.as_slice().to_vec();
			assert!(ds_key.ends_with(b"!ds"), "unexpected state key layout");
			let index_prefix = &ds_key[..ds_key.len() - 3];
			let table_prefix = &index_prefix[..index_prefix.len() - 5];
			assert_eq!(index_prefix[index_prefix.len() - 5], b'+', "unexpected index key layout");
			let adjacency: Vec<u8> = [index_prefix, b"!dn"].concat();
			let tx = ds.transaction(TransactionType::Read).await?;
			let mut cursor =
				tx.open_vals_cursor_raw(RawRange::every_key(), Direction::Forward, 0, None).await?;
			let mut out = BTreeMap::new();
			loop {
				let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
				if batch.is_empty() {
					break;
				}
				for (key, value) in batch.iter() {
					let index_key = key.starts_with(index_prefix) && !key.starts_with(&adjacency);
					let mapping = [b"!di", b"!dd", b"!dj"]
						.iter()
						.any(|family| key.starts_with(&[table_prefix, &family[..]].concat()));
					if index_key || mapping {
						out.insert(key.to_vec(), value.to_vec());
					}
				}
			}
			drop(cursor);
			tx.cancel().await?;
			Ok(out)
		}

		/// Recall@`k` of `index` over `queries` against brute force over `corpus`, per `ef`.
		async fn recalls(
			index: &DiskAnnIndex,
			ds: &TestIndexStore,
			params: &DiskAnnParams,
			corpus: &[Vec<f32>],
			queries: &[Vec<f32>],
			k: usize,
			efs: &[usize],
		) -> Result<Vec<f64>> {
			let vectors: Vec<Vector> =
				corpus.iter().map(|v| Vector::F32(v.clone().into())).collect();
			let truth: Vec<Vec<i64>> = queries
				.iter()
				.map(|q| brute_force_top_k(&vectors, q, &params.distance, k))
				.collect();
			let mut out = Vec::new();
			for &ef in efs {
				let mut total = 0.0;
				for (query, truth) in queries.iter().zip(&truth) {
					total += recall(truth, &knn_ids(index, ds, query, k, ef).await?);
				}
				out.push(total / queries.len() as f64);
			}
			Ok(out)
		}

		/// The starved-degree recall dataset, its parameters and its floors.
		struct Starved {
			base: Vec<Vec<f32>>,
			queries: Vec<Vec<f32>>,
			params: DiskAnnParams,
			floors: Vec<(usize, f64)>,
		}

		fn starved_corpus() -> Starved {
			let mut rng = recall_rng();
			let mut corpus = ClusteredCorpus::new(&mut rng, 64, 1500, 0.35);
			let base = corpus.draw(&mut rng, 4000);
			let queries = corpus.draw(&mut rng, 100);
			Starved {
				base,
				queries,
				params: graph_params(64, Distance::Cosine, 8, 24),
				floors: vec![(10, 0.55), (20, 0.67), (40, 0.77), (100, 0.89), (200, 0.94)],
			}
		}

		/// One build of `corpus` on a store of `places` places.
		struct Build {
			ds: TestIndexStore,
			index: DiskAnnIndex,
		}

		async fn build(
			places: usize,
			params: &DiskAnnParams,
			corpus: &[Vec<f32>],
		) -> Result<Build> {
			let ds = TestIndexStore::new().await.with_places(places);
			let index = new_index(ikb(), TableId(4), params, cache()).await?;
			ingest(&index, &ds, 0, corpus).await?;
			compact_all(&index, &ds, &ikb()).await?;
			Ok(Build {
				ds,
				index,
			})
		}

		/// T-3: the element, mapping and state bytes of a build do not depend on the places its
		/// frames were granted, and its adjacency is valid and as good either way.
		///
		/// The sequential path (no place) spawns nothing; a frame granted `g` places never has
		/// more than `g` of its tasks alive at once, and the books show none held afterwards. The
		/// adjacency bytes are not compared: upstream's bootstrap breaks equal-distance ties by
		/// task interleaving.
		#[test_log::test(tokio::test(flavor = "multi_thread"))]
		async fn diskann_batched_build_is_independent_of_its_places() -> Result<()> {
			let Starved {
				base,
				queries,
				params,
				floors,
			} = starved_corpus();
			let efs: Vec<usize> = floors.iter().map(|(ef, _)| *ef).collect();
			let mut states = Vec::new();
			let mut measured = Vec::new();
			for places in [0, 1, 4] {
				let b = build(places, &params, &base).await?;
				let frames = b.index.frames();
				assert!(!frames.is_empty());
				for frame in &frames {
					let stats = &frame.stats;
					let granted = stats.granted.load(Ordering::Acquire);
					assert!(granted <= places, "a frame was granted {granted} of {places}");
					if places == 0 {
						assert_eq!(
							stats.spawns.load(Ordering::Acquire),
							0,
							"nothing is spawned without a place"
						);
						assert_eq!(stats.batch_calls.load(Ordering::Acquire), 0);
					} else {
						assert!(stats.peak_tasks.load(Ordering::Acquire) <= granted);
					}
					assert_eq!(frame.live_tasks(), 0);
				}
				let inserted: u64 = frames
					.iter()
					.map(|f| {
						f.stats.batched_vectors.load(Ordering::Acquire)
							+ f.stats.sequential_vectors.load(Ordering::Acquire)
					})
					.sum();
				// The first vector seeds the graph; every other one goes through a batch.
				assert_eq!(inserted, base.len() as u64 - 1);
				if places > 0 {
					assert!(
						frames
							.iter()
							.all(|f| f.stats.sequential_vectors.load(Ordering::Acquire) == 0)
					);
					assert!(b.ds.places().peak() <= places);
				}
				assert_eq!(b.ds.places().held(), 0, "a frame held places after it ended");
				assert_eq!(assert_adjacency_valid(&b.ds, &ikb()).await?, base.len());
				states.push(state_without_adjacency(&b.ds, &ikb()).await?);
				let r = recalls(&b.index, &b.ds, &params, &base, &queries, 10, &efs).await?;
				info!("places={places}: Recall@10 {r:?}");
				for ((ef, floor), got) in floors.iter().zip(&r) {
					assert!(got >= floor, "places={places} ef={ef}: {got} below floor {floor}");
				}
				measured.push(r);
			}
			for (i, state) in states.iter().enumerate().skip(1) {
				if state != &states[0] {
					let differing: Vec<_> = state
						.keys()
						.chain(states[0].keys())
						.filter(|k| state.get(*k) != states[0].get(*k))
						.take(5)
						.collect();
					panic!("build {i} differs from the sequential build at {differing:?}");
				}
			}
			// Batched builds agree with each other; the sequential build is the serial graph,
			// which a batched build must not fall below.
			for (ef, (one, four)) in efs.iter().zip(measured[1].iter().zip(&measured[2])) {
				assert!((one - four).abs() <= 0.02, "ef={ef}: 1 place {one} vs 4 places {four}");
			}
			for (ef, (serial, batched)) in efs.iter().zip(measured[0].iter().zip(&measured[2])) {
				assert!(
					batched + 0.02 >= *serial,
					"ef={ef}: batched {batched} below serial {serial}"
				);
			}
			Ok(())
		}

		/// T-2: a mixed workload — inserts, updates and deletes over several compaction rounds —
		/// built batched answers at least as well as built one vector at a time.
		#[test_log::test(tokio::test(flavor = "multi_thread"))]
		async fn diskann_batched_mixed_workload_keeps_recall() -> Result<()> {
			let Starved {
				base,
				queries,
				params,
				floors,
			} = starved_corpus();
			let efs: Vec<usize> = floors.iter().map(|(ef, _)| *ef).collect();
			let mut measured = Vec::new();
			for places in [0, 4] {
				let ds = TestIndexStore::new().await.with_places(places);
				let index = new_index(ikb(), TableId(4), &params, cache()).await?;
				let mut current: BTreeMap<i64, Vec<f32>> = BTreeMap::new();
				// Round 1: the first half. Round 2: the second half, and an update of every tenth
				// record of the first to a vector of the second half's. Round 3: a delete of every
				// seventh record.
				let half = base.len() / 2;
				ingest(&index, &ds, 0, &base[..half]).await?;
				current
					.extend(base[..half].iter().cloned().enumerate().map(|(i, v)| (i as i64, v)));
				compact_all(&index, &ds, &ikb()).await?;
				ingest(&index, &ds, half as i64, &base[half..]).await?;
				current.extend(
					base[half..].iter().cloned().enumerate().map(|(i, v)| ((half + i) as i64, v)),
				);
				{
					let ctx = new_ctx(&ds, TransactionType::Write).await;
					for id in (0..half as i64).step_by(10) {
						let old = current[&id].clone();
						let mut new = base[half + id as usize].clone();
						new[0] += 0.001;
						index
							.index(
								&ctx,
								&RecordIdKey::Number(id),
								Some(f32_content(&old)),
								Some(f32_content(&new)),
							)
							.await?;
						current.insert(id, new);
					}
					ctx.tx().commit().await?;
				}
				compact_all(&index, &ds, &ikb()).await?;
				{
					let ctx = new_ctx(&ds, TransactionType::Write).await;
					for id in (0..base.len() as i64).step_by(7) {
						let old = current.remove(&id).expect("indexed");
						index
							.index(&ctx, &RecordIdKey::Number(id), Some(f32_content(&old)), None)
							.await?;
					}
					ctx.tx().commit().await?;
				}
				compact_all(&index, &ds, &ikb()).await?;
				assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, current.len());
				// Ground truth over what is indexed now, under its record ids.
				let ids: Vec<i64> = current.keys().copied().collect();
				let corpus: Vec<Vec<f32>> = current.values().cloned().collect();
				let vectors: Vec<Vector> =
					corpus.iter().map(|v| Vector::F32(v.clone().into())).collect();
				let mut r = Vec::new();
				for &ef in &efs {
					let mut total = 0.0;
					for q in &queries {
						let truth: Vec<i64> = brute_force_top_k(&vectors, q, &params.distance, 10)
							.into_iter()
							.map(|pos| ids[pos as usize])
							.collect();
						total += recall(&truth, &knn_ids(&index, &ds, q, 10, ef).await?);
					}
					r.push(total / queries.len() as f64);
				}
				info!("mixed workload, places={places}: Recall@10 {r:?}");
				measured.push(r);
			}
			for (ef, (serial, batched)) in efs.iter().zip(measured[0].iter().zip(&measured[1])) {
				assert!(
					batched + 0.02 >= *serial,
					"ef={ef}: batched {batched} below serial {serial}"
				);
			}
			Ok(())
		}

		/// A graph of `seed` vectors, compacted, on a store of `places` places.
		async fn seeded(
			places: usize,
			params: &DiskAnnParams,
			seed: &[Vec<f32>],
		) -> Result<(TestIndexStore, DiskAnnIndex, DiskAnnCache)> {
			let ds = TestIndexStore::new().await.with_places(places);
			let cache = cache();
			let index = new_index(ikb(), TableId(4), params, cache.clone()).await?;
			ingest(&index, &ds, 0, seed).await?;
			compact_all(&index, &ds, &ikb()).await?;
			Ok((ds, index, cache))
		}

		/// T-4: a frame inserting many fresh vectors into a non-empty graph writes the graph state
		/// once, for the whole batch's element ids.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_allocates_its_ids_with_one_state_write() -> Result<()> {
			for places in [0, 4] {
				let (ds, index, _) =
					seeded(places, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				assert!(compact_once(&index, &ds, &ikb()).await?);
				let frame = index.last_frame().expect("a frame");
				assert_eq!(frame.stats.state_writes.load(Ordering::Acquire), 1, "places={places}");
				assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 50);
			}
			Ok(())
		}

		/// Asserts the failed frame left everything as it found it: the pendings still queued,
		/// the elements unchanged, nothing of the index in the cache and no place held; then that
		/// a frame without the fault folds the backlog.
		async fn assert_frame_undone(
			ds: &TestIndexStore,
			index: &DiskAnnIndex,
			cache: &DiskAnnCache,
			captured_before: usize,
			elements_before: usize,
		) -> Result<()> {
			assert_eq!(captured(ds, &ikb()).await?, captured_before, "the pendings were consumed");
			assert_eq!(
				elements(ds, &ikb()).await?.len(),
				elements_before,
				"graph elements committed"
			);
			assert_eq!(cache.weight(), 0, "the failed frame left entries in the cache");
			assert_eq!(ds.places().held(), 0, "the failed frame holds places");
			let frame = index.last_frame().expect("a frame");
			assert_eq!(frame.live_tasks(), 0);
			index.set_faults(None);
			compact_all(index, ds, &ikb()).await?;
			assert_eq!(captured(ds, &ikb()).await?, 0);
			assert_adjacency_valid(ds, &ikb()).await?;
			Ok(())
		}

		/// Applies the next plan on `index`, under `faults`.
		async fn apply_with(
			ds: &TestIndexStore,
			index: &DiskAnnIndex,
			faults: FrameFaults,
		) -> Result<CompactionApply> {
			index.set_faults(Some(Arc::new(faults)));
			let plan = next_plan(ds, &ikb()).await?;
			let ctx = new_ctx(ds, TransactionType::Write).await;
			let res = index.apply_compaction(&ctx, plan).await;
			assert!(ctx.tx().closed(), "the frame left its transaction open");
			res
		}

		/// T-7: a provider error inside the batch's back-edge phase fails the frame, although
		/// upstream swallows it, and nothing of the frame survives.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_fails_on_a_suppressed_provider_error() -> Result<()> {
			for places in [0, 4] {
				let (ds, index, cache) =
					seeded(places, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				let before = captured(&ds, &ikb()).await?;
				let faults = FrameFaults::default();
				*faults.fail_append.lock().unwrap() =
					Some(Box::new(|| anyhow::anyhow!("injected append failure")));
				let err = apply_with(&ds, &index, faults).await.expect_err("the frame must fail");
				assert!(
					err.to_string().contains("injected append failure"),
					"places={places}: {err:#}"
				);
				assert_frame_undone(&ds, &index, &cache, before, 20).await?;
			}
			Ok(())
		}

		/// T-8: the failure a task recorded keeps its type, so a shutdown is still classified as
		/// one, on the sequential path as well as the batched one.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_failure_keeps_its_classification() -> Result<()> {
			for places in [0, 4] {
				let (ds, index, cache) =
					seeded(places, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				let before = captured(&ds, &ikb()).await?;
				let faults = FrameFaults::default();
				*faults.fail_append.lock().unwrap() =
					Some(Box::new(|| anyhow::Error::new(surrealdb_kvs::Error::Shutdown)));
				let err = apply_with(&ds, &index, faults).await.expect_err("the frame must fail");
				assert!(is_shutdown_error(&err), "places={places}: not a shutdown: {err:#}");
				assert_frame_undone(&ds, &index, &cache, before, 20).await?;
			}
			Ok(())
		}

		/// T-7a: a panic in a task the batch spawned fails the frame, although upstream discards
		/// the task's `JoinError`, and the cause names the panic.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_fails_on_a_panicking_task() -> Result<()> {
			let (ds, index, cache) = seeded(4, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			let before = captured(&ds, &ikb()).await?;
			let faults = FrameFaults::default();
			faults.panic_append.store(true, Ordering::Release);
			let err = apply_with(&ds, &index, faults).await.expect_err("the frame must fail");
			assert!(
				matches!(err.downcast_ref(), Some(DiskAnnFrameError::TaskPanicked)),
				"expected a task panic, got {err:#}"
			);
			assert_frame_undone(&ds, &index, &cache, before, 20).await
		}

		/// A spawned task's guard observes its panic during the unwind, before upstream sees the
		/// `JoinError`.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_task_guard_observes_a_panic() -> Result<()> {
			use diskann::provider::ExecutionContext as _;
			let ds = TestIndexStore::new().await;
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
			let frame = DiskAnnBuildFrame::without_view(ds.places().ticket());
			let context = index
				.graph
				.read()
				.await
				.context(Arc::clone(&tx), None)
				.in_frame(Arc::clone(&frame));
			let task = tokio::spawn(context.wrap_spawn(async { panic!("injected") }));
			assert!(task.await.is_err_and(|e| e.is_panic()));
			assert!(matches!(
				frame.take_error().and_then(|e| e.downcast::<DiskAnnFrameError>().ok()),
				Some(DiskAnnFrameError::TaskPanicked)
			));
			assert_eq!(frame.live_tasks(), 0);
			tx.cancel().await
		}

		/// T-7b: a panic in one frame fails that frame alone, while another index's frame runs
		/// beside it: a task reaches its frame through its context, not through a span or a
		/// thread.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_failure_is_attributed_to_its_frame() -> Result<()> {
			let ds = TestIndexStore::new().await.with_places(8);
			// Another table, so the two frames resolve no doc-ID in common.
			let ikb_b = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb2".into(), IndexId(9));
			let a = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let b = new_index(ikb_b.clone(), TableId(4), &small_params(4), cache()).await?;
			for (index, ikb) in [(&a, ikb()), (&b, ikb_b.clone())] {
				ingest(index, &ds, 0, &line(4, 0.0, 1.0, 20)).await?;
				compact_all(index, &ds, &ikb).await?;
				ingest(index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			}
			let faults = FrameFaults::default();
			faults.panic_append.store(true, Ordering::Release);
			a.set_faults(Some(Arc::new(faults)));
			let plan_a = next_plan(&ds, &ikb()).await?;
			let plan_b = next_plan(&ds, &ikb_b).await?;
			let ctx_a = new_ctx(&ds, TransactionType::Write).await;
			let ctx_b = new_ctx(&ds, TransactionType::Write).await;
			let (ra, rb) = tokio::join!(
				a.apply_compaction(&ctx_a, plan_a),
				b.apply_compaction(&ctx_b, plan_b)
			);
			assert!(matches!(
				ra.expect_err("the faulted frame fails").downcast_ref(),
				Some(DiskAnnFrameError::TaskPanicked)
			));
			assert_eq!(rb?, CompactionApply::Applied, "the other frame commits");
			assert_eq!(captured(&ds, &ikb_b).await?, 0);
			assert!(captured(&ds, &ikb()).await? > 0);
			Ok(())
		}

		/// T-7c: an item whose candidate generation failed without reaching the provider has no
		/// adjacency list of its own, and the structural post-condition fails the frame naming it,
		/// before any mapping of the batch is written.
		///
		/// In one placement the failed item sits far out on the axis, just past a committed
		/// element, so every other item's prune keeps that element and drops it: nothing writes it
		/// a list at all. In the other it sits among the batch, so other items choose it as a
		/// neighbour and the back-edge phase writes it a list made of their edges — which is not a
		/// list of its own, and fails the frame all the same.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_post_condition_catches_a_lost_item() -> Result<()> {
			for among_the_batch in [false, true] {
				let mut seed = line(4, 0.0, 0.1, 20);
				seed.push(vec![99.9, 0.0, 0.0, 0.0]);
				let (ds, index, cache) = seeded(4, &small_params(4), &seed).await?;
				let lost = if among_the_batch {
					vec![0.42, 0.0, 0.0, 0.0]
				} else {
					vec![100.0, 0.0, 0.0, 0.0]
				};
				let mut fresh = line(4, 0.05, 0.1, 9);
				fresh.insert(4, lost.clone());
				ingest(&index, &ds, 100, &fresh).await?;
				let before = captured(&ds, &ikb()).await?;
				let faults = FrameFaults::default();
				*faults.fail_search_for.lock().unwrap() = Some(SerializedVector::F32(lost.clone()));
				let err = apply_with(&ds, &index, faults).await.expect_err("the frame must fail");
				let Some(DiskAnnFrameError::NodeNotWritten {
					element_id,
				}) = err.downcast_ref()
				else {
					panic!("expected the post-condition, got {err:#}");
				};
				let frame = index.last_frame().expect("a frame");
				let lost_element = frame
					.written_elements()
					.iter()
					.find(|(_, e)| {
						e.as_ref().is_some_and(|e| e.vector == SerializedVector::F32(lost.clone()))
					})
					.map(|(id, _)| *id);
				assert_eq!(lost_element, Some(*element_id), "the error names another element");
				if among_the_batch {
					assert!(
						frame.written_nodes().contains(element_id),
						"no back-edge reached the lost item, so this case tests nothing"
					);
				}
				// Nothing of the batch committed: no element, and no vector mapping.
				let tx = ds.transaction(TransactionType::Read).await?;
				assert!(
					tx.get_key(&ikb().new_dq_key(&SerializedVector::F32(lost)), None)
						.await?
						.is_none()
				);
				tx.cancel().await?;
				assert_frame_undone(&ds, &index, &cache, before, seed.len()).await?;
			}
			Ok(())
		}

		/// An environment whose statement is done once `done` is set, or which raises
		/// `DatastoreError::QueryBeyondMemoryThreshold` instead when `threshold` is set.
		struct StopEnv {
			inner: TestIndexEnv,
			done: Arc<AtomicBool>,
			threshold: bool,
		}

		impl CancellationProbe for StopEnv {
			fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
				self.inner.expect_not_timedout()
			}
		}

		impl IndexEnv for StopEnv {
			fn tx(&self) -> Arc<Transaction> {
				IndexEnv::tx(&self.inner)
			}

			fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
				if self.done.load(Ordering::Acquire) {
					if self.threshold {
						return Box::pin(async {
							Err(DatastoreError::QueryBeyondMemoryThreshold.into())
						});
					}
					return Box::pin(async { Ok(true) });
				}
				self.inner.is_done(count)
			}

			fn index_stores(&self) -> &IndexStores {
				self.inner.index_stores()
			}

			fn node_id(&self) -> Uuid {
				self.inner.node_id()
			}

			fn sequences(&self) -> Result<&Sequences> {
				self.inner.sequences()
			}

			fn config(&self) -> &IdxConfig {
				self.inner.config()
			}

			fn places(&self) -> Ticket {
				self.inner.places()
			}
		}

		/// T-9 / T-9a: a statement stopped while the batch's write tasks are parked mid-phase
		/// stops the frame. Each task issues no write after the stop — the one it had started is
		/// checked before it reaches KV — and the frame drains them before it ends its
		/// transaction. A cancellation ends the frame as cancelled; a memory-threshold error
		/// survives unchanged.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_stops_with_its_statement() -> Result<()> {
			for threshold in [false, true] {
				let (ds, index, cache) =
					seeded(4, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				let before = captured(&ds, &ikb()).await?;
				let park = Park::new();
				let faults = FrameFaults {
					park_set_element: Some(Arc::clone(&park)),
					..FrameFaults::default()
				};
				index.set_faults(Some(Arc::new(faults)));
				let plan = next_plan(&ds, &ikb()).await?;
				let done = Arc::new(AtomicBool::new(false));
				let env = Arc::new(StopEnv {
					inner: new_ctx(&ds, TransactionType::Write).await,
					done: Arc::clone(&done),
					threshold,
				});
				let tx = IndexEnv::tx(&*env);
				let index = Arc::new(index);
				let apply = {
					let index = Arc::clone(&index);
					let env = Arc::clone(&env);
					tokio::spawn(async move { index.apply_compaction(&*env, plan).await })
				};
				// All four write partitions are parked before their first write.
				park.wait_for(4).await;
				done.store(true, Ordering::Release);
				// The watchdog sees the stop; the frame cannot end until its tasks exit.
				common::time::sleep(DISKANN_FRAME_WATCHDOG_INTERVAL * 3).await;
				assert!(!tx.closed(), "the transaction ended while the frame's tasks ran");
				assert!(!apply.is_finished());
				park.release();
				let err = apply.await?.expect_err("a stopped frame fails");
				if threshold {
					assert!(
						matches!(
							err.downcast_ref(),
							Some(DatastoreError::QueryBeyondMemoryThreshold)
						),
						"the threshold error was not kept: {err:#}"
					);
				} else {
					assert!(
						matches!(err.downcast_ref(), Some(EngineError::QueryCancelled)),
						"expected a cancellation, got {err:#}"
					);
				}
				let frame = index.last_frame().expect("a frame");
				assert_eq!(frame.live_tasks(), 0);
				assert!(frame.written_elements().is_empty(), "a task wrote after the stop");
				assert_eq!(*frame.stats.live_at_tx_end.lock().unwrap(), vec![0]);
				assert!(tx.closed());
				let index = Arc::into_inner(index).expect("the apply has returned");
				assert_frame_undone(&ds, &index, &cache, before, 20).await?;
			}
			Ok(())
		}

		/// T-9: a write partition that fails returns from upstream at once, detaching its parked
		/// siblings. The frame still drains them before its transaction ends.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_drains_tasks_upstream_detached() -> Result<()> {
			let (ds, index, cache) = seeded(4, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			let before = captured(&ds, &ikb()).await?;
			let park = Park::new();
			// The first batch id belongs to the first partition, which upstream awaits first.
			let first_id =
				elements(&ds, &ikb()).await?.iter().map(|(id, _)| *id).max().unwrap() + 1;
			let faults = FrameFaults {
				park_set_element: Some(Arc::clone(&park)),
				..FrameFaults::default()
			};
			*faults.fail_set_element.lock().unwrap() = Some(first_id);
			index.set_faults(Some(Arc::new(faults)));
			let plan = next_plan(&ds, &ikb()).await?;
			let env = Arc::new(new_ctx(&ds, TransactionType::Write).await);
			let tx = env.tx();
			let index = Arc::new(index);
			let apply = {
				let index = Arc::clone(&index);
				let env = Arc::clone(&env);
				tokio::spawn(async move { index.apply_compaction(&*env, plan).await })
			};
			park.wait_for(3).await;
			common::time::sleep(DISKANN_FRAME_WATCHDOG_INTERVAL * 2).await;
			let frame = index.last_frame().expect("a frame");
			assert_eq!(frame.live_tasks(), 3, "the detached siblings are still running");
			assert!(!tx.closed(), "the transaction ended while detached tasks ran");
			park.release();
			let err = apply.await?.expect_err("the frame fails");
			assert!(err.to_string().contains("injected set_element failure"), "{err:#}");
			assert_eq!(frame.live_tasks(), 0);
			assert_eq!(*frame.stats.live_at_tx_end.lock().unwrap(), vec![0]);
			let index = Arc::into_inner(index).expect("the apply has returned");
			assert_frame_undone(&ds, &index, &cache, before, 20).await
		}

		/// T-11 / T-13: a frame task that panics mid-batch while its tasks are parked. The lock's
		/// cleanup runs at once, so a reader can take the graph lock and finds no entry of the
		/// batch in the cache; but the places stay on the books, and the write transaction stays
		/// open, until the parked tasks are released and have exited. The transaction is then
		/// cancelled exactly once, with no task alive, and nothing of the batch commits.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_panic_keeps_its_places_until_its_tasks_exit() -> Result<()> {
			let (ds, index, cache) = seeded(4, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			let before = captured(&ds, &ikb()).await?;
			let park = Park::new();
			let faults = FrameFaults {
				park_set_element: Some(Arc::clone(&park)),
				panic_frame_when_parked: Some(4),
				..FrameFaults::default()
			};
			index.set_faults(Some(Arc::new(faults)));
			let plan = next_plan(&ds, &ikb()).await?;
			let env = Arc::new(new_ctx(&ds, TransactionType::Write).await);
			let tx = env.tx();
			let index = Arc::new(index);
			let apply = {
				let index = Arc::clone(&index);
				let env = Arc::clone(&env);
				tokio::spawn(async move { index.apply_compaction(&*env, plan).await })
			};
			assert!(apply.await.is_err_and(|e| e.is_panic()), "the frame task panics");
			// The cleanup ran synchronously on the unwind: the lock is free and the cache clear.
			let reader = index.graph.read().await;
			assert_eq!(cache.weight(), 0, "the panicked frame left entries in the cache");
			drop(reader);
			let frame = index.last_frame().expect("a frame");
			assert!(frame.is_closed());
			assert_eq!(ds.places().held(), 4, "the parked tasks hold their places");
			common::time::sleep(DISKANN_FRAME_WATCHDOG_INTERVAL * 2).await;
			assert!(!tx.closed(), "the transaction ended while the frame's tasks ran");
			park.release();
			for _ in 0..200 {
				if tx.closed() {
					break;
				}
				common::time::sleep(std::time::Duration::from_millis(10)).await;
			}
			assert!(tx.closed(), "the backstop never cancelled the transaction");
			assert_eq!(frame.live_tasks(), 0);
			assert_eq!(ds.places().held(), 0);
			assert_eq!(
				*frame.stats.live_at_tx_end.lock().unwrap(),
				vec![0],
				"one end, after the drain"
			);
			assert!(frame.written_elements().is_empty(), "a task wrote after the frame closed");
			let index = Arc::into_inner(index).expect("the apply has returned");
			assert_frame_undone(&ds, &index, &cache, before, 20).await
		}

		/// T-13: a frame asking for more places than are free gets what is free, without waiting;
		/// a store with none runs it on one task; two indexes compacting at once hold no more
		/// than the store has between them; and every place is back once the frames end.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_places_are_bounded_by_the_pool() -> Result<()> {
			for free in [0, 3] {
				let (ds, index, _) = seeded(free, &small_params(4), &line(4, 0.0, 1.0, 20)).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				assert!(compact_once(&index, &ds, &ikb()).await?);
				let frame = index.last_frame().expect("a frame");
				assert_eq!(frame.stats.granted.load(Ordering::Acquire), free.min(30));
				if free == 0 {
					assert_eq!(frame.stats.spawns.load(Ordering::Acquire), 0);
				}
				assert!(frame.stats.peak_tasks.load(Ordering::Acquire) <= free);
				assert_eq!(ds.places().held(), 0);
			}
			let ds = TestIndexStore::new().await.with_places(5);
			// Another table, so the two frames resolve no doc-ID in common.
			let ikb_b = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb2".into(), IndexId(9));
			let a = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let b = new_index(ikb_b.clone(), TableId(4), &small_params(4), cache()).await?;
			for (index, ikb) in [(&a, ikb()), (&b, ikb_b.clone())] {
				ingest(index, &ds, 0, &line(4, 0.0, 1.0, 20)).await?;
				compact_all(index, &ds, &ikb).await?;
				ingest(index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			}
			let ikb_a = ikb();
			let (ra, rb) = tokio::join!(compact_all(&a, &ds, &ikb_a), compact_all(&b, &ds, &ikb_b));
			ra?;
			rb?;
			assert!(ds.places().peak() <= 5, "two frames held {} places", ds.places().peak());
			assert_eq!(ds.places().held(), 0);
			Ok(())
		}

		/// T-13: a task built through `wrap_spawn` holds its share of the frame's places from the
		/// moment it is built, so a future never polled keeps them past the frame's close, and
		/// gives them back when dropped.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_unpolled_task_keeps_its_places() -> Result<()> {
			use diskann::provider::ExecutionContext as _;
			let ds = TestIndexStore::new().await.with_places(2);
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
			let frame = DiskAnnBuildFrame::without_view(ds.places().ticket());
			assert_eq!(
				frame.take_places(4).map(|n| n.get()),
				Some(2),
				"a grant takes what is free"
			);
			assert_eq!(frame.take_places(4), None, "a frame asks once");
			let context = index
				.graph
				.read()
				.await
				.context(Arc::clone(&tx), None)
				.in_frame(Arc::clone(&frame));
			let task = context.wrap_spawn(async {});
			frame.close_now();
			assert_eq!(ds.places().held(), 2, "the unpolled task holds the grant");
			drop(task);
			assert_eq!(ds.places().held(), 0);
			assert_eq!(frame.live_tasks(), 0);
			tx.cancel().await
		}

		/// T-10: docs sharing a vector collapse onto one new element, a doc whose vector the graph
		/// holds is appended to it, a doc whose vector moves is linked only to its new element,
		/// and the cached doc sets of the elements a frame changed are never served stale.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_dedupes_and_keeps_both_mapping_directions() -> Result<()> {
			let p = small_params(4);
			let ds = TestIndexStore::new().await.with_places(4);
			let cache = cache();
			let index = DiskAnnIndex::new(
				ikb(),
				TableId(4),
				&p,
				cache.clone(),
				VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
			)
			.await?;
			let existing = vec![5.0, 0.0, 0.0, 0.0];
			let mut seed = line(4, 0.0, 1.0, 10);
			seed.push(existing.clone());
			ingest(&index, &ds, 0, &seed).await?;
			compact_all(&index, &ds, &ikb()).await?;
			// Prewarm the doc sets with a read-only query.
			assert!(knn_ids(&index, &ds, &existing, 3, 40).await?.contains(&10));
			let shared = vec![50.0, 1.0, 0.0, 0.0];
			let moved_to = vec![60.0, 1.0, 0.0, 0.0];
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				for id in [20, 21] {
					index
						.index(&ctx, &RecordIdKey::Number(id), None, Some(f32_content(&shared)))
						.await?;
				}
				index
					.index(&ctx, &RecordIdKey::Number(22), None, Some(f32_content(&existing)))
					.await?;
				// Record 3 moves from its vector to a fresh one.
				index
					.index(
						&ctx,
						&RecordIdKey::Number(3),
						Some(f32_content(&seed[3])),
						Some(f32_content(&moved_to)),
					)
					.await?;
				for (i, v) in line(4, 30.0, 1.0, 5).iter().enumerate() {
					index
						.index(
							&ctx,
							&RecordIdKey::Number(40 + i as i64),
							None,
							Some(f32_content(v)),
						)
						.await?;
				}
				ctx.tx().commit().await?;
			}
			let elements_before = elements(&ds, &ikb()).await?.len();
			assert!(compact_once(&index, &ds, &ikb()).await?);
			// `shared` is one element; `existing` gains no element; `moved_to` and the line are
			// new.
			assert_eq!(elements(&ds, &ikb()).await?.len(), elements_before + 1 + 1 + 5);
			let tx = ds.transaction(TransactionType::Read).await?;
			let doc_of = |id: i64| {
				let tx = &tx;
				async move {
					DiskAnnDocs::get_doc_id(
						&ikb(),
						VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
						tx,
						&RecordIdKey::Number(id),
					)
					.await
					.map(|d| d.expect("mapped"))
				}
			};
			let shared_docs: Option<DiskAnnElementDocs> =
				tx.get_key(&ikb().new_dq_key(&SerializedVector::F32(shared.clone())), None).await?;
			let shared_docs = shared_docs.expect("shared vector mapped");
			let (d20, d21, d22, d3, d10) = (
				doc_of(20).await?,
				doc_of(21).await?,
				doc_of(22).await?,
				doc_of(3).await?,
				doc_of(10).await?,
			);
			assert_eq!(shared_docs.docs.iter().collect::<Vec<_>>(), {
				let mut v = vec![d20, d21];
				v.sort();
				v
			});
			let existing_docs: DiskAnnElementDocs = tx
				.get_key(&ikb().new_dq_key(&SerializedVector::F32(existing.clone())), None)
				.await?
				.expect("existing vector mapped");
			assert_eq!(existing_docs.docs.iter().collect::<Vec<_>>(), {
				let mut v = vec![d10, d22];
				v.sort();
				v
			});
			let moved: DiskAnnElementDocs = tx
				.get_key(&ikb().new_dq_key(&SerializedVector::F32(moved_to.clone())), None)
				.await?
				.expect("moved vector mapped");
			let elements_of = index.vec_docs.elements_of(&tx, &[d20, d21, d22, d3]).await?;
			let ids = |e: &Option<Ids64>| e.as_ref().map(|e| e.iter().collect::<Vec<_>>());
			assert_eq!(ids(&elements_of[0]), Some(vec![shared_docs.e_id]));
			assert_eq!(ids(&elements_of[1]), Some(vec![shared_docs.e_id]));
			assert_eq!(ids(&elements_of[2]), Some(vec![existing_docs.e_id]));
			assert_eq!(
				ids(&elements_of[3]),
				Some(vec![moved.e_id]),
				"the moved doc kept its old element"
			);
			tx.cancel().await?;
			// The existing element's cached doc set is the new one, or not cached at all.
			let generation = {
				let tx = ds.transaction(TransactionType::Read).await?;
				let g = read_compaction_generation(&tx, &ikb().new_dg_key()).await?;
				tx.cancel().await?;
				g
			};
			let view = index.graph.read().await.reader_view(generation);
			let cache_index = (
				ikb().ns(),
				ikb().db(),
				TableId(4),
				ikb().index(),
				VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
			);
			if let Some(cached) = cache.get_doc_set(cache_index, existing_docs.e_id, &view) {
				assert_eq!(cached, existing_docs.docs, "a stale doc set is served");
			}
			// A filtered query admitting exactly the deduplicated and appended docs returns them.
			let allow: RoaringTreemap = [d20, d21, d22].into_iter().collect();
			let got = knn_keys_with_allow(&index, &ds, &shared, 3, Some(&allow)).await?;
			assert_eq!(got, vec![20, 21, 22]);
			assert_adjacency_valid(&ds, &ikb()).await?;
			Ok(())
		}

		/// T-14: multi-valued records totalling more new vectors than a plan holds, in far fewer
		/// keys and bytes, are cut by the vector cap; no frame inserts more fresh vectors than the
		/// cap, no batched call more than [`DISKANN_INSERT_BATCH_MAX_VECTORS`], and the next plans
		/// fold the rest.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_plan_caps_its_new_vectors() -> Result<()> {
			let ds = TestIndexStore::new().await.with_places(4);
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			// 600 records of 3 vectors each: 1800 vectors, under the key cap.
			let per_record = 3;
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				for r in 0..600_i64 {
					let content: Vec<Value> = (0..per_record)
						.map(|c| f32_value(&[r as f32, c as f32, 0.5, 0.25]))
						.collect();
					index.index(&ctx, &RecordIdKey::Number(r), None, Some(content)).await?;
				}
				ctx.tx().commit().await?;
			}
			let plan = next_plan(&ds, &ikb()).await?;
			assert!(plan.has_more(), "the vector cap ends the plan");
			let vectors: usize = plan.pending.iter().map(|p| p.new_vectors.len()).sum();
			assert!(
				vectors <= DISKANN_COMPACTION_MAX_PENDING_VECTORS,
				"{vectors} vectors in one plan"
			);
			assert!(vectors > DISKANN_COMPACTION_MAX_PENDING_VECTORS - per_record);
			let passes = compact_all(&index, &ds, &ikb()).await?;
			assert!(passes >= 2);
			for frame in index.frames() {
				let largest = frame.stats.largest_batch.load(Ordering::Acquire);
				assert!(largest <= DISKANN_INSERT_BATCH_MAX_VECTORS);
			}
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 1800);
			Ok(())
		}

		/// Near vectors of one frame link to each other however far apart the batch puts them.
		///
		/// Each pair below is two vectors a hair apart, one batched-insert call's length apart in
		/// the frame's batch: all records sit in one pending shard, which the plan captures in key
		/// order. A batched insert sees the rest of its call only through the intra-batch
		/// candidates positionally next to each vector, so a call holding both halves of a pair
		/// would leave them unaware of each other, and neither would list the other. The frame's
		/// calls are small enough that the second half searches a graph that already holds the
		/// first.
		///
		/// The graph is large against the batch, at `DEGREE 64`, so that upstream's bootstrap —
		/// which prunes every vector against the whole batch, and runs while a batch's back-edge
		/// sources are few — does not run and hide the gap, as it does in a young or starved graph.
		/// In one call of the whole batch, no pair links at all.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_links_near_vectors_far_apart_in_its_batch() -> Result<()> {
			let pairs = DISKANN_INSERT_BATCH_MAX_VECTORS + 4;
			assert!(pairs > DISKANN_INTRA_BATCH_CANDIDATES as usize);
			assert!(
				2 * pairs <= DISKANN_COMPACTION_MAX_PENDING_VECTORS,
				"both halves in one frame"
			);
			let mut rng = recall_rng();
			let dim = 16;
			let mut point =
				|| -> Vec<f32> { (0..dim).map(|_| rng.random_range(-1.0f32..=1.0)).collect() };
			let seed: Vec<Vec<f32>> = (0..10_000).map(|_| point()).collect();
			let params = graph_params(dim as u16, Distance::Euclidean, 64, 100);
			let firsts: Vec<Vec<f32>> = (0..pairs).map(|_| point()).collect();
			let seconds: Vec<Vec<f32>> =
				firsts.iter().map(|v| v.iter().map(|x| x + 1e-3).collect()).collect();
			// Batched only: on no place a frame inserts one vector at a time, the serial build.
			for places in [4] {
				let ds = TestIndexStore::new().await.with_places(places);
				let index = new_index(ikb(), TableId(4), &params, cache()).await?;
				ingest(&index, &ds, 0, &seed).await?;
				compact_all(&index, &ds, &ikb()).await?;
				// Every id a multiple of the shard count, so one shard holds them all, in id order.
				{
					let ctx = new_ctx(&ds, TransactionType::Write).await;
					let shards = i64::from(DISKANN_PENDING_STATE_SHARDS);
					for (pos, v) in firsts.iter().chain(&seconds).enumerate() {
						let id = RecordIdKey::Number(shards * (1000 + pos as i64));
						index.index(&ctx, &id, None, Some(f32_content(v))).await?;
					}
					ctx.tx().commit().await?;
				}
				assert!(compact_once(&index, &ds, &ikb()).await?);
				assert_eq!(captured(&ds, &ikb()).await?, 0, "both halves are in one frame");
				let tx = ds.transaction(TransactionType::Read).await?;
				let element_of = |v: &Vec<f32>| {
					let tx = &tx;
					let key = SerializedVector::F32(v.clone());
					async move {
						let docs: Option<DiskAnnElementDocs> =
							tx.get_key(&ikb().new_dq_key(&key), None).await?;
						Ok::<_, anyhow::Error>(docs.expect("mapped").e_id)
					}
				};
				let mut linked = 0;
				for (a, b) in firsts.iter().zip(&seconds) {
					let (a, b) = (element_of(a).await?, element_of(b).await?);
					let lists = |e: ElementId| {
						let tx = &tx;
						async move {
							let node: Option<DiskAnnNode> =
								tx.get_key(&ikb().new_dn_key(e), None).await?;
							Ok::<_, anyhow::Error>(node.map(|n| n.neighbors).unwrap_or_default())
						}
					};
					if lists(a).await?.contains(&b) || lists(b).await?.contains(&a) {
						linked += 1;
					}
				}
				tx.cancel().await?;
				info!("places={places}: {linked}/{pairs} near pairs linked");
				assert!(
					linked * 100 >= pairs * 95,
					"places={places}: only {linked}/{pairs} near pairs linked"
				);
			}
			Ok(())
		}

		/// T-14: a record carrying more vectors than the cap is captured alone and inserted in
		/// calls of at most [`DISKANN_INSERT_BATCH_MAX_VECTORS`], all inside one frame that commits
		/// its pending's delete, its mappings and its graph together. Such a record needs an index
		/// over more columns than the cap, so the index API is called directly.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_record_over_the_cap_is_one_frame() -> Result<()> {
			let ds = TestIndexStore::new().await.with_places(4);
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			ingest(&index, &ds, 0, &line(4, 0.0, 1.0, 10)).await?;
			compact_all(&index, &ds, &ikb()).await?;
			let n = DISKANN_COMPACTION_MAX_PENDING_VECTORS + 300;
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let content: Vec<Value> = (0..n)
					.map(|c| f32_value(&[100.0 + c as f32, (c % 7) as f32, 0.0, 1.0]))
					.collect();
				index.index(&ctx, &RecordIdKey::Number(1000), None, Some(content)).await?;
				ingest_in(&index, &ctx, 2000, &line(4, 0.5, 1.0, 5)).await?;
				ctx.tx().commit().await?;
			}
			let plan = next_plan(&ds, &ikb()).await?;
			assert_eq!(plan.captured_keys.len(), 1, "the oversized record is captured alone");
			assert!(compact_once(&index, &ds, &ikb()).await?);
			let frame = index.last_frame().expect("a frame");
			assert_eq!(
				frame.stats.batch_calls.load(Ordering::Acquire),
				n.div_ceil(DISKANN_INSERT_BATCH_MAX_VECTORS) as u64
			);
			assert_eq!(
				frame.stats.largest_batch.load(Ordering::Acquire),
				DISKANN_INSERT_BATCH_MAX_VECTORS
			);
			assert_eq!(frame.stats.batched_vectors.load(Ordering::Acquire), n as u64);
			compact_all(&index, &ds, &ikb()).await?;
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 10 + n + 5);
			Ok(())
		}

		/// [`ingest`] into an open transaction.
		async fn ingest_in(
			index: &DiskAnnIndex,
			ctx: &TestIndexEnv,
			first: i64,
			vectors: &[Vec<f32>],
		) -> Result<()> {
			for (i, v) in vectors.iter().enumerate() {
				let id = RecordIdKey::Number(first + i as i64);
				index.index(ctx, &id, None, Some(f32_content(v))).await?;
			}
			Ok(())
		}

		/// T-14: one field holding several vectors is not split into them: the write fails the
		/// dimension check.
		#[tokio::test]
		async fn diskann_array_of_vectors_in_one_field_is_rejected() -> Result<()> {
			let ds = TestIndexStore::new().await;
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let nested = Value::from(vec![
				f32_value(&[1.0, 2.0, 3.0, 4.0]),
				f32_value(&[5.0, 6.0, 7.0, 8.0]),
			]);
			assert!(
				index.index(&ctx, &RecordIdKey::Number(1), None, Some(vec![nested])).await.is_err()
			);
			ctx.tx().cancel().await
		}

		/// An environment that cancels `frame_of`'s last frame on its `at`-th probe, standing in
		/// for a statement cancelled while phase A claims doc-id windows.
		struct CancelOnProbe {
			inner: TestIndexEnv,
			index: Arc<DiskAnnIndex>,
			at: usize,
			probes: AtomicUsize,
		}

		impl CancellationProbe for CancelOnProbe {
			fn expect_not_timedout(&self) -> BoxProviderFut<'_, Result<()>> {
				Box::pin(async move {
					if self.probes.fetch_add(1, Ordering::AcqRel) + 1 == self.at {
						self.index.last_frame().expect("a frame").cancel();
					}
					self.inner.expect_not_timedout().await
				})
			}
		}

		impl IndexEnv for CancelOnProbe {
			fn tx(&self) -> Arc<Transaction> {
				IndexEnv::tx(&self.inner)
			}

			fn is_done(&self, count: Option<usize>) -> BoxEnvFut<'_> {
				self.inner.is_done(count)
			}

			fn index_stores(&self) -> &IndexStores {
				self.inner.index_stores()
			}

			fn node_id(&self) -> Uuid {
				self.inner.node_id()
			}

			fn sequences(&self) -> Result<&Sequences> {
				self.inner.sequences()
			}

			fn config(&self) -> &IdxConfig {
				self.inner.config()
			}

			fn places(&self) -> Ticket {
				self.inner.places()
			}
		}

		/// T-9: a frame cancelled while phase A claims doc-id windows resolves no further record:
		/// the window claim in flight completes, the next record's resolution checks the frame,
		/// and nothing commits.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_cancelled_in_phase_a_resolves_no_further_record() -> Result<()> {
			let ds = TestIndexStore::with_idx_config(|c| c.table_doc_ids_batch_size = 1)
				.await
				.with_places(4);
			let index = Arc::new(new_index(ikb(), TableId(4), &small_params(4), cache()).await?);
			// Records written before any doc-id exists, so each pending resolves one at compaction.
			ingest(&index, &ds, 0, &line(4, 0.0, 1.0, 12)).await?;
			let before = captured(&ds, &ikb()).await?;
			let plan = next_plan(&ds, &ikb()).await?;
			let env = CancelOnProbe {
				inner: new_ctx(&ds, TransactionType::Write).await,
				index: Arc::clone(&index),
				at: 3,
				probes: AtomicUsize::new(0),
			};
			let err = index.apply_compaction(&env, plan).await.expect_err("the frame is cancelled");
			assert!(matches!(err.downcast_ref(), Some(EngineError::QueryCancelled)), "{err:#}");
			assert_eq!(
				env.probes.load(Ordering::Acquire),
				3,
				"a record was resolved after the cancel"
			);
			assert!(IndexEnv::tx(&env).closed());
			assert_eq!(captured(&ds, &ikb()).await?, before);
			assert!(elements(&ds, &ikb()).await?.is_empty());
			Ok(())
		}

		/// T-9: a frame cancelled while the entry-point repair scans `!de` reads no further batch
		/// and persists no entry point.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_cancelled_mid_scan_reads_no_further_batch() -> Result<()> {
			let ds = TestIndexStore::new().await.with_places(4);
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			// A stored graph whose entry point and first page of elements are tombstoned, so the
			// repair has to read a second page to find a live element.
			let tombstoned = surrealdb_kvs::consts::NORMAL_BATCH_SIZE as u64 + 10;
			{
				let tx = ds.transaction(TransactionType::Write).await?;
				for id in 0..=tombstoned {
					let element = DiskAnnElement {
						vector: SerializedVector::F32(vec![id as f32, 0.0, 0.0, 0.0]),
						deleted: id < tombstoned,
					};
					tx.set_key(&ikb().new_de_key(id), &element).await?;
				}
				tx.set_key(
					&ikb().new_ds_key(),
					&DiskAnnState {
						enter_point: Some(0),
						next_element_id: tombstoned + 1,
						start_points: Vec::new(),
					},
				)
				.await?;
				tx.commit().await?;
			}
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 4)).await?;
			let faults = FrameFaults::default();
			faults.cancel_after_scan_batches.store(1, Ordering::Release);
			let err = apply_with(&ds, &index, faults).await.expect_err("the frame is cancelled");
			assert!(err.to_string().contains("cancelled"), "{err:#}");
			let frame = index.last_frame().expect("a frame");
			assert_eq!(
				frame.stats.scan_batches.load(Ordering::Acquire),
				1,
				"a batch was read after the cancel"
			);
			assert_eq!(
				frame.stats.state_writes.load(Ordering::Acquire),
				0,
				"an entry point was written"
			);
			Ok(())
		}

		/// `seed` indexed and compacted into a fresh index on `ds`.
		#[cfg(feature = "kv-rocksdb")]
		async fn seeded_on(
			ds: TestIndexStore,
			params: &DiskAnnParams,
			seed: &[Vec<f32>],
		) -> Result<(TestIndexStore, DiskAnnIndex, DiskAnnCache)> {
			let cache = cache();
			let index = new_index(ikb(), TableId(4), params, cache.clone()).await?;
			ingest(&index, &ds, 0, seed).await?;
			compact_all(&index, &ds, &ikb()).await?;
			Ok((ds, index, cache))
		}

		/// A RocksDB store, whose read-only transactions get a view, with `places` places.
		#[cfg(feature = "kv-rocksdb")]
		async fn rocksdb_store(dir: &TempDir, places: usize) -> TestIndexStore {
			TestIndexStore::with_path(&format!("rocksdb:{}", dir.path().to_string_lossy()))
				.await
				.with_places(places)
		}

		/// The record whose vector the graph's stored entry point holds, for a seed built by
		/// [`line`], whose `i`th vector starts at `i`.
		#[cfg(feature = "kv-rocksdb")]
		async fn entry_point_record(ds: &TestIndexStore) -> Result<i64> {
			let tx = ds.transaction(TransactionType::Read).await?;
			let state: DiskAnnState =
				tx.get_key(&ikb().new_ds_key(), None).await?.expect("a state");
			let entry = state.enter_point.expect("an entry point");
			let element: DiskAnnElement =
				tx.get_key(&ikb().new_de_key(entry), None).await?.expect("the entry element");
			tx.cancel().await?;
			let SerializedVector::F32(v) = element.vector else {
				panic!("an F32 vector");
			};
			Ok(v[0] as i64)
		}

		/// Asserts every entry the cache holds for the index equals what a fresh read
		/// transaction reads from KV, family by family. Returns how many entries it compared.
		#[cfg(feature = "kv-rocksdb")]
		async fn assert_cache_matches_kv(
			ds: &TestIndexStore,
			cache: &DiskAnnCache,
		) -> Result<usize> {
			use crate::trees::diskann::cache::CachedEntry;
			let ikb = ikb();
			let entries = cache.entries_of(
				ikb.ns(),
				ikb.db(),
				TableId(4),
				ikb.index(),
				DOC_IDS_FORMAT_VERSION,
			);
			let tx = ds.transaction(TransactionType::Read).await?;
			for entry in &entries {
				match entry {
					CachedEntry::State(cached) => {
						let stored: DiskAnnState =
							tx.get_key(&ikb.new_ds_key(), None).await?.expect("a stored state");
						assert_eq!(
							(cached.enter_point, cached.next_element_id, &cached.start_points),
							(stored.enter_point, stored.next_element_id, &stored.start_points),
							"the cached graph state is not the stored one"
						);
					}
					CachedEntry::Element(id, cached) => {
						let stored: DiskAnnElement = tx
							.get_key(&ikb.new_de_key(*id), None)
							.await?
							.unwrap_or_else(|| panic!("cached element {id} is not stored"));
						assert_eq!(
							(&cached.vector, cached.deleted),
							(&stored.vector, stored.deleted),
							"cached element {id} differs from the stored one"
						);
					}
					CachedEntry::Node(id, cached) => {
						let stored: DiskAnnNode = tx
							.get_key(&ikb.new_dn_key(*id), None)
							.await?
							.unwrap_or_else(|| panic!("cached adjacency list {id} is not stored"));
						assert_eq!(
							cached.neighbors, stored.neighbors,
							"cached adjacency list {id} differs from the stored one"
						);
					}
					CachedEntry::DocSet(id, cached) => {
						let element: DiskAnnElement = tx
							.get_key(&ikb.new_de_key(*id), None)
							.await?
							.expect("a stored element");
						let stored: DiskAnnElementDocs = tx
							.get_key(&ikb.new_dq_key(&element.vector), None)
							.await?
							.unwrap_or_else(|| panic!("cached doc set {id} is not stored"));
						assert_eq!(stored.e_id, *id, "the stored doc set names another element");
						assert_eq!(cached, &stored.docs, "cached doc set {id} differs from KV");
					}
					CachedEntry::DocId(doc, cached) => {
						let stored = TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone())
							.get_record_ids_batch(&tx, &[*doc])
							.await?;
						assert_eq!(
							stored,
							vec![Some(cached.as_ref().clone())],
							"cached doc-ID {doc} names another record"
						);
					}
				}
			}
			tx.cancel().await?;
			Ok(entries.len())
		}

		/// A read-only query over every vector of `seed`, so the state, elements, adjacency
		/// lists, doc sets and doc-IDs it touches are cached.
		#[cfg(feature = "kv-rocksdb")]
		async fn prewarm(
			index: &DiskAnnIndex,
			ds: &TestIndexStore,
			seed: &[Vec<f32>],
		) -> Result<()> {
			for v in seed {
				knn_ids(index, ds, v, 3, 8).await?;
			}
			Ok(())
		}

		/// T-5: on RocksDB a frame with a place reads every clean graph key through its one read
		/// view, none through its write transaction, answers the keys it wrote from its overlay,
		/// publishes what the view read, and ends its read transaction once, with no task alive.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_reads_clean_keys_through_its_view() -> Result<()> {
			let dir = TempDir::new()?;
			let (ds, index, _) =
				seeded_on(rocksdb_store(&dir, 1).await, &small_params(4), &line(4, 0.0, 1.0, 20))
					.await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			// The seeding frames wrote the whole graph through; a cold cache sends the frame's
			// reads to KV.
			index.clear_local_cache_now();
			assert!(compact_once(&index, &ds, &ikb()).await?);
			let frame = index.last_frame().expect("a frame");
			let stats = &frame.stats;
			assert_eq!(stats.read_txs_opened.load(Ordering::Acquire), 1, "one read transaction");
			assert_eq!(stats.reads_tx.load(Ordering::Acquire), 0, "a graph key read through tx");
			assert!(stats.reads_view.load(Ordering::Acquire) > 0, "nothing read through the view");
			assert!(
				stats.reads_overlay.load(Ordering::Acquire) > 0,
				"no key read from the overlay"
			);
			assert!(
				stats.cache_publishes.load(Ordering::Acquire) > 0,
				"the view published nothing"
			);
			assert_eq!(stats.granted.load(Ordering::Acquire), 1);
			assert_eq!(stats.batched_vectors.load(Ordering::Acquire), 30);
			assert_eq!(*stats.live_at_tx_end.lock().unwrap(), vec![0], "one read transaction end");
			assert!(frame.is_closed());
			assert_eq!(ds.places().held(), 0);
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 50);
			Ok(())
		}

		/// T-5: with the stored entry point tombstoned, the repair cursor reads `!de` through the
		/// write transaction, and those batches are the only reads the transaction serves.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_repair_scan_is_its_only_transaction_read() -> Result<()> {
			let dir = TempDir::new()?;
			let seed = line(4, 0.0, 1.0, 20);
			let (ds, index, _) =
				seeded_on(rocksdb_store(&dir, 1).await, &small_params(4), &seed).await?;
			let entry = entry_point_record(&ds).await?;
			// Without start points beside it, a deleted entry point leaves nothing to start from
			// but a scan for a live element.
			{
				let tx = ds.transaction(TransactionType::Write).await?;
				let mut state: DiskAnnState =
					tx.get_key(&ikb().new_ds_key(), None).await?.expect("a state");
				assert!(!state.start_points.is_empty(), "the seed kept no start point");
				state.start_points.clear();
				tx.set_key(&ikb().new_ds_key(), &state).await?;
				tx.commit().await?;
			}
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let id = RecordIdKey::Number(entry);
			index.index(&ctx, &id, Some(f32_content(&seed[entry as usize])), None).await?;
			ctx.tx().commit().await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			index.clear_local_cache_now();
			assert!(compact_once(&index, &ds, &ikb()).await?);
			let frame = index.last_frame().expect("a frame");
			let scans = frame.stats.scan_batches.load(Ordering::Acquire);
			assert!(scans > 0, "the entry point was not repaired by a scan");
			assert_eq!(frame.stats.reads_tx.load(Ordering::Acquire), scans);
			assert!(frame.stats.reads_view.load(Ordering::Acquire) > 0);
			assert_ne!(entry_point_record(&ds).await?, entry, "the tombstone is still the entry");
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 49);
			Ok(())
		}

		/// T-5 / T-3: a frame with no view opens no read transaction, reads nothing through a view
		/// and publishes nothing it read, yet takes the places that are free and inserts its batch
		/// on them, reading through its write transaction. On mem, which gives no view, and on
		/// RocksDB with its view refused by a test hook.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_without_a_view_batches_through_its_transaction() -> Result<()> {
			async fn check(ds: TestIndexStore, faults: FrameFaults) -> Result<()> {
				// Every frame of the index runs under `faults`, the seeding ones included.
				let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
				index.set_faults(Some(Arc::new(faults)));
				ingest(&index, &ds, 0, &line(4, 0.0, 1.0, 20)).await?;
				compact_all(&index, &ds, &ikb()).await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				index.clear_local_cache_now();
				assert!(compact_once(&index, &ds, &ikb()).await?);
				let frame = index.last_frame().expect("a frame");
				let stats = &frame.stats;
				assert_eq!(stats.read_txs_opened.load(Ordering::Acquire), 0);
				assert_eq!(stats.reads_view.load(Ordering::Acquire), 0);
				assert_eq!(stats.cache_publishes.load(Ordering::Acquire), 0);
				assert!(stats.reads_tx.load(Ordering::Acquire) > 0);
				assert_eq!(stats.granted.load(Ordering::Acquire), 4);
				assert!(stats.spawns.load(Ordering::Acquire) > 0, "no task was spawned");
				assert_eq!(stats.batched_vectors.load(Ordering::Acquire), 30);
				assert_eq!(stats.sequential_vectors.load(Ordering::Acquire), 0);
				assert_eq!(ds.places().held(), 0, "a place was not given back");
				assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 50);
				Ok(())
			}
			check(TestIndexStore::new().await.with_places(4), FrameFaults::default()).await?;
			#[cfg(feature = "kv-rocksdb")]
			{
				let dir = TempDir::new()?;
				let faults = FrameFaults {
					no_view: true,
					..FrameFaults::default()
				};
				check(rocksdb_store(&dir, 4).await, faults).await?;
			}
			Ok(())
		}

		/// T-6: after a committed frame every entry the cache holds for the index — state,
		/// elements, adjacency lists, doc sets and doc-IDs, prewarmed by a read-only query before
		/// the frame — equals its KV value; after a frame that fails, the cache holds nothing of
		/// the index.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_with_a_view_keeps_the_cache_coherent() -> Result<()> {
			let dir = TempDir::new()?;
			let seed = line(4, 0.0, 1.0, 20);
			let (ds, index, cache) =
				seeded_on(rocksdb_store(&dir, 2).await, &small_params(4), &seed).await?;
			let entry = entry_point_record(&ds).await?;
			// A cold cache, then a read-only query over part of the graph: the frame is served
			// some entries from the cache and reads the rest through its view.
			index.clear_local_cache_now();
			prewarm(&index, &ds, &seed[..3]).await?;
			assert!(assert_cache_matches_kv(&ds, &cache).await? > 0, "nothing was prewarmed");
			// Fresh vectors, and moves of records other than the entry point's, so the frame
			// changes elements, adjacency lists, doc sets and the state the prewarm cached.
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for i in (1..20).filter(|i| *i != entry).step_by(4) {
				let id = RecordIdKey::Number(i);
				let moved = vec![i as f32 + 0.25, 0.5, 0.0, 0.0];
				index
					.index(
						&ctx,
						&id,
						Some(f32_content(&seed[i as usize])),
						Some(f32_content(&moved)),
					)
					.await?;
			}
			ctx.tx().commit().await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			assert!(compact_once(&index, &ds, &ikb()).await?);
			let frame = index.last_frame().expect("a frame");
			assert_eq!(frame.stats.read_txs_opened.load(Ordering::Acquire), 1);
			assert!(frame.stats.cache_publishes.load(Ordering::Acquire) > 0);
			assert!(assert_cache_matches_kv(&ds, &cache).await? > 0);
			prewarm(&index, &ds, &seed).await?;
			assert!(assert_cache_matches_kv(&ds, &cache).await? > 0);
			// A frame that fails after its view has published leaves no entry of the index.
			ingest(&index, &ds, 200, &line(4, 0.75, 1.0, 30)).await?;
			index.clear_local_cache_now();
			let faults = FrameFaults::default();
			*faults.fail_append.lock().unwrap() =
				Some(Box::new(|| anyhow::anyhow!("injected append failure")));
			apply_with(&ds, &index, faults).await.expect_err("the frame fails");
			let frame = index.last_frame().expect("a frame");
			assert!(frame.stats.reads_view.load(Ordering::Acquire) > 0);
			assert_eq!(assert_cache_matches_kv(&ds, &cache).await?, 0, "the cache kept entries");
			assert_eq!(*frame.stats.live_at_tx_end.lock().unwrap(), vec![0, 0]);
			Ok(())
		}

		/// T-12: a failure while the frame opens its read view — before the read transaction
		/// opens, or once it has — cancels the write transaction, which holds the `!dg` bump and
		/// the conditional deletes, and any read transaction it opened, and commits nothing.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_view_failure_ends_both_transactions() -> Result<()> {
			use crate::trees::diskann::frame::ViewOpenFault;
			for fault in [ViewOpenFault::BeforeOpen, ViewOpenFault::AfterOpen] {
				let dir = TempDir::new()?;
				let (ds, index, cache) = seeded_on(
					rocksdb_store(&dir, 1).await,
					&small_params(4),
					&line(4, 0.0, 1.0, 20),
				)
				.await?;
				ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
				let before = captured(&ds, &ikb()).await?;
				let generation = {
					let tx = ds.transaction(TransactionType::Read).await?;
					let g = read_compaction_generation(&tx, &ikb().new_dg_key()).await?;
					tx.cancel().await?;
					g
				};
				let faults = Arc::new(FrameFaults {
					view_open: Some(fault),
					..FrameFaults::default()
				});
				index.set_faults(Some(Arc::clone(&faults)));
				let plan = next_plan(&ds, &ikb()).await?;
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let err = index.apply_compaction(&ctx, plan).await.expect_err("the frame fails");
				assert!(err.to_string().contains("injected"), "{fault:?}: {err:#}");
				assert!(ctx.tx().closed(), "{fault:?}: the write transaction was left open");
				let views = faults.opened_views.lock().unwrap().clone();
				match fault {
					ViewOpenFault::BeforeOpen => assert!(views.is_empty()),
					ViewOpenFault::AfterOpen => {
						assert_eq!(views.len(), 1);
						assert!(views[0].tx().closed(), "the read transaction was left open");
					}
				}
				let tx = ds.transaction(TransactionType::Read).await?;
				assert_eq!(
					read_compaction_generation(&tx, &ikb().new_dg_key()).await?,
					generation,
					"{fault:?}: the generation bump committed"
				);
				tx.cancel().await?;
				assert_frame_undone(&ds, &index, &cache, before, 20).await?;
			}
			Ok(())
		}

		/// T-11 on the production path: a frame with a read view whose task panics while its
		/// spawned tasks are parked. Neither transaction ends — the read transaction its lock's
		/// backstop cancels, nor the write transaction its guard's backstop cancels — until the
		/// tasks are released and have exited, and each ends once.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_frame_panic_with_a_view_drains_before_either_end() -> Result<()> {
			let dir = TempDir::new()?;
			let (ds, index, cache) =
				seeded_on(rocksdb_store(&dir, 4).await, &small_params(4), &line(4, 0.0, 1.0, 20))
					.await?;
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 30)).await?;
			let before = captured(&ds, &ikb()).await?;
			let park = Park::new();
			let faults = Arc::new(FrameFaults {
				park_set_element: Some(Arc::clone(&park)),
				panic_frame_when_parked: Some(4),
				..FrameFaults::default()
			});
			index.set_faults(Some(Arc::clone(&faults)));
			let plan = next_plan(&ds, &ikb()).await?;
			let env = Arc::new(new_ctx(&ds, TransactionType::Write).await);
			let tx = env.tx();
			let index = Arc::new(index);
			let apply = {
				let index = Arc::clone(&index);
				let env = Arc::clone(&env);
				tokio::spawn(async move { index.apply_compaction(&*env, plan).await })
			};
			assert!(apply.await.is_err_and(|e| e.is_panic()), "the frame task panics");
			let reader = index.graph.read().await;
			assert_eq!(cache.weight(), 0, "the panicked frame left entries in the cache");
			drop(reader);
			let view = faults.opened_views.lock().unwrap().last().cloned().expect("a view");
			common::time::sleep(DISKANN_FRAME_WATCHDOG_INTERVAL * 2).await;
			assert!(!tx.closed(), "the write transaction ended while the frame's tasks ran");
			assert!(!view.tx().closed(), "the read transaction ended while the frame's tasks ran");
			park.release();
			for _ in 0..200 {
				if tx.closed() && view.tx().closed() {
					break;
				}
				common::time::sleep(std::time::Duration::from_millis(10)).await;
			}
			assert!(tx.closed() && view.tx().closed(), "a backstop never ended its transaction");
			let frame = index.last_frame().expect("a frame");
			assert_eq!(frame.live_tasks(), 0);
			assert_eq!(ds.places().held(), 0);
			assert_eq!(*frame.stats.live_at_tx_end.lock().unwrap(), vec![0, 0]);
			let index = Arc::into_inner(index).expect("the apply has returned");
			assert_frame_undone(&ds, &index, &cache, before, 20).await
		}

		/// `vectors` as sink records, one record each, under record ids `first..`.
		fn sink_records(
			first: i64,
			vectors: &[Vec<f32>],
		) -> Vec<(RecordIdKey, Vec<SerializedVector>)> {
			vectors
				.iter()
				.enumerate()
				.map(|(i, v)| {
					(RecordIdKey::Number(first + i as i64), vec![SerializedVector::F32(v.clone())])
				})
				.collect()
		}

		/// T-11, sink success: the sink frame holds the graph lock until the build transaction it
		/// was applied in commits and the frame finishes; then the graph, its mappings and the
		/// span cover the commit, and nothing went through `!dw`.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_frame_commits_with_its_build_transaction() -> Result<()> {
			sink_frame_commits_with_its_build_transaction(TestIndexStore::new().await).await
		}

		/// The same on RocksDB, whose sink frame reads through a view and batches on places.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_frame_commits_with_its_build_transaction_rocksdb() -> Result<()> {
			let dir = TempDir::new()?;
			sink_frame_commits_with_its_build_transaction(rocksdb_store(&dir, 4).await).await
		}

		async fn sink_frame_commits_with_its_build_transaction(ds: TestIndexStore) -> Result<()> {
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let mut tx_guard = BuildTxGuard::new(ctx.tx());
			let frame = index
				.apply_direct(
					&ctx,
					(b"first", b"last"),
					sink_records(0, &line(4, 0.0, 1.0, 40)),
					&mut tx_guard,
				)
				.await?;
			assert!(index.graph.try_read().is_err(), "a search was let in before the commit");
			ctx.tx().commit().await?;
			tx_guard.disarm();
			frame.finish().await;
			let graph = index.graph.read().await;
			let generation = {
				let tx = ds.transaction(TransactionType::Read).await?;
				let g = read_compaction_generation(&tx, &ikb().new_dg_key()).await?;
				tx.cancel().await?;
				g
			};
			assert_eq!(
				graph.continuity.map(|span| span.head),
				Some(generation),
				"the span stops short"
			);
			drop(graph);
			assert_eq!(captured(&ds, &ikb()).await?, 0, "the sink queued pendings");
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 40);
			assert_eq!(knn_ids(&index, &ds, &[20.0, 0.0, 0.0, 0.0], 1, 40).await?, vec![20]);
			// The materialiser folds later writes on top of what the sink built.
			ingest(&index, &ds, 100, &line(4, 0.5, 1.0, 10)).await?;
			compact_all(&index, &ds, &ikb()).await?;
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 50);
			Ok(())
		}

		/// T-11, sink failure after `apply_direct`: the build transaction does not commit, the
		/// builder aborts the frame and then cancels it. A search waiting on the graph lock gets
		/// in only once the cleanup has run, and finds no entry of the batch; nothing commits.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_frame_abort_leaves_nothing() -> Result<()> {
			let ds = TestIndexStore::new().await;
			let cache = cache();
			let index =
				Arc::new(new_index(ikb(), TableId(4), &small_params(4), cache.clone()).await?);
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let mut tx_guard = BuildTxGuard::new(ctx.tx());
			let frame = index
				.apply_direct(
					&ctx,
					(b"first", b"last"),
					sink_records(0, &line(4, 0.0, 1.0, 40)),
					&mut tx_guard,
				)
				.await?;
			assert!(cache.weight() > 0, "the frame wrote nothing through");
			let reader = {
				let index = Arc::clone(&index);
				let cache = cache.clone();
				tokio::spawn(async move {
					let _graph = index.graph.read().await;
					cache.weight()
				})
			};
			common::time::sleep(std::time::Duration::from_millis(50)).await;
			assert!(!reader.is_finished(), "a search got past the frame's lock");
			frame.abort().await;
			ctx.tx().cancel().await?;
			tx_guard.disarm();
			assert_eq!(reader.await?, 0, "the search was served the aborted batch");
			assert!(elements(&ds, &ikb()).await?.is_empty(), "the aborted batch committed");
			// A later batch, in a transaction of its own, applies.
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let mut tx_guard = BuildTxGuard::new(ctx.tx());
			let frame = index
				.apply_direct(
					&ctx,
					(b"first", b"last"),
					sink_records(0, &line(4, 0.0, 1.0, 40)),
					&mut tx_guard,
				)
				.await?;
			ctx.tx().commit().await?;
			tx_guard.disarm();
			frame.finish().await;
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, 40);
			Ok(())
		}

		/// T-11, a panic between `apply_direct` and `finish`: the frame's `Drop` undoes it before
		/// the lock goes, and the build transaction's guard cancels it, once; nothing of the batch
		/// commits.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_frame_dropped_on_a_panic_commits_nothing() -> Result<()> {
			sink_frame_dropped_on_a_panic_commits_nothing(TestIndexStore::new().await, 1).await
		}

		/// The same on RocksDB: the frame's read transaction and the build transaction both end
		/// after the frame's tasks have drained.
		#[cfg(feature = "kv-rocksdb")]
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_frame_dropped_on_a_panic_commits_nothing_rocksdb() -> Result<()> {
			let dir = TempDir::new()?;
			sink_frame_dropped_on_a_panic_commits_nothing(rocksdb_store(&dir, 4).await, 2).await
		}

		/// `ends` is how many transactions the frame has: its build transaction, and a read
		/// transaction where the engine gives a view.
		async fn sink_frame_dropped_on_a_panic_commits_nothing(
			ds: TestIndexStore,
			ends: usize,
		) -> Result<()> {
			let cache = cache();
			let index =
				Arc::new(new_index(ikb(), TableId(4), &small_params(4), cache.clone()).await?);
			let ctx = Arc::new(new_ctx(&ds, TransactionType::Write).await);
			let tx = ctx.tx();
			let builder = {
				let (index, ctx) = (Arc::clone(&index), Arc::clone(&ctx));
				tokio::spawn(async move {
					let mut tx_guard = BuildTxGuard::new(ctx.tx());
					let _frame = index
						.apply_direct(
							&*ctx,
							(b"first", b"last"),
							sink_records(0, &line(4, 0.0, 1.0, 40)),
							&mut tx_guard,
						)
						.await
						.expect("the sink frame applies");
					panic!("injected panic before the build transaction commits");
				})
			};
			assert!(builder.await.is_err_and(|e| e.is_panic()), "the builder panics");
			let graph = index.graph.read().await;
			assert_eq!(cache.weight(), 0, "the dropped frame left entries in the cache");
			assert!(graph.continuity.is_none(), "the span outlived the dropped frame");
			drop(graph);
			for _ in 0..200 {
				if tx.closed() {
					break;
				}
				common::time::sleep(std::time::Duration::from_millis(10)).await;
			}
			assert!(tx.closed(), "the build transaction's guard never cancelled it");
			let frame = index.last_frame().expect("a frame");
			for _ in 0..200 {
				if frame.stats.live_at_tx_end.lock().unwrap().len() == ends {
					break;
				}
				common::time::sleep(std::time::Duration::from_millis(10)).await;
			}
			assert_eq!(*frame.stats.live_at_tx_end.lock().unwrap(), vec![0; ends]);
			assert!(elements(&ds, &ikb()).await?.is_empty(), "the batch committed");
			Ok(())
		}

		/// T-14, a record over the cap: one sink record holding more vectors than one batched
		/// insert takes is inserted in consecutive batches of at most the cap, inside the one
		/// frame, which commits them together.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_sink_inserts_a_record_over_the_cap_in_one_frame() -> Result<()> {
			let ds = TestIndexStore::new().await.with_places(4);
			let index = new_index(ikb(), TableId(4), &small_params(4), cache()).await?;
			let n = DISKANN_DIRECT_MAX_VECTORS + 77;
			let vectors: Vec<SerializedVector> = (0..n)
				.map(|i| SerializedVector::F32(vec![i as f32, (i % 7) as f32, 0.0, 0.0]))
				.collect();
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let mut tx_guard = BuildTxGuard::new(ctx.tx());
			let frame = index
				.apply_direct(
					&ctx,
					(b"first", b"last"),
					vec![(RecordIdKey::Number(1), vectors)],
					&mut tx_guard,
				)
				.await?;
			ctx.tx().commit().await?;
			tx_guard.disarm();
			frame.finish().await;
			let stats = &index.last_frame().expect("a frame").stats;
			// The first vector seeds the empty graph; the rest go in calls of at most
			// DISKANN_INSERT_BATCH_MAX_VECTORS.
			assert_eq!(
				stats.batch_calls.load(Ordering::Acquire),
				(n - 1).div_ceil(DISKANN_INSERT_BATCH_MAX_VECTORS) as u64
			);
			assert_eq!(
				stats.largest_batch.load(Ordering::Acquire),
				DISKANN_INSERT_BATCH_MAX_VECTORS
			);
			assert_eq!(stats.batched_vectors.load(Ordering::Acquire), n as u64 - 1);
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, n);
			Ok(())
		}

		/// The stored graph state.
		async fn stored_state(ds: &TestIndexStore) -> Result<DiskAnnState> {
			let tx = ds.transaction(TransactionType::Read).await?;
			let state = tx.get_key(&ikb().new_ds_key(), None).await?.expect("a state");
			tx.cancel().await?;
			Ok(state)
		}

		/// The record a [`line`] seed's element holds: its vector's first component.
		async fn record_of(ds: &TestIndexStore, id: ElementId) -> Result<i64> {
			let tx = ds.transaction(TransactionType::Read).await?;
			let element: DiskAnnElement =
				tx.get_key(&ikb().new_de_key(id), None).await?.expect("a stored element");
			tx.cancel().await?;
			let SerializedVector::F32(v) = element.vector else {
				panic!("an F32 vector");
			};
			Ok(v[0] as i64)
		}

		/// #1179: a graph seeded from a batch enters at the batch's medoid and keeps, beside it,
		/// the batch vectors spread farthest from it — on a line, its two ends.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_seed_keeps_a_medoid_entry_and_spread_start_points() -> Result<()> {
			let (ds, _, _) = seeded(0, &small_params(4), &line(4, 0.0, 1.0, 40)).await?;
			let state = stored_state(&ds).await?;
			let entry = record_of(&ds, state.enter_point.expect("an entry point")).await?;
			assert!(
				(19..=20).contains(&entry),
				"the entry point is record {entry}, not the medoid"
			);
			assert_eq!(state.start_points.len(), DISKANN_START_POINTS - 1);
			let mut starts = Vec::new();
			for id in &state.start_points {
				starts.push(record_of(&ds, *id).await?);
			}
			assert!(starts.contains(&0) && starts.contains(&39), "start points {starts:?}");
			Ok(())
		}

		/// #1179: deleting the entry point and a start point leaves the graph entering at a live
		/// start point, with the set refilled from the deleted point's neighbours, and with no
		/// scan for a survivor; every search still finds its nearest record.
		#[tokio::test(flavor = "multi_thread")]
		async fn diskann_start_points_survive_their_deletion() -> Result<()> {
			let seed = line(4, 0.0, 1.0, 40);
			let (ds, index, _) = seeded(0, &small_params(4), &seed).await?;
			let state = stored_state(&ds).await?;
			let doomed = [state.enter_point.expect("an entry point"), state.start_points[0]];
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for id in doomed {
				let record = record_of(&ds, id).await?;
				let content = f32_content(&seed[record as usize]);
				index.index(&ctx, &RecordIdKey::Number(record), Some(content), None).await?;
			}
			ctx.tx().commit().await?;
			assert!(compact_once(&index, &ds, &ikb()).await?);
			let frame = index.last_frame().expect("a frame");
			assert_eq!(
				frame.stats.scan_batches.load(Ordering::Acquire),
				0,
				"a survivor was scanned for"
			);
			let state = stored_state(&ds).await?;
			let entry = state.enter_point.expect("an entry point");
			assert!(!doomed.contains(&entry), "a deleted element is still the entry point");
			assert_eq!(
				state.start_points.len(),
				DISKANN_START_POINTS - 1,
				"deleting start points shrank the set"
			);
			let live: HashSet<ElementId> = elements(&ds, &ikb())
				.await?
				.into_iter()
				.filter(|(_, e)| !e.deleted)
				.map(|(id, _)| id)
				.collect();
			assert!(live.contains(&entry));
			assert!(state.start_points.iter().all(|p| live.contains(p) && *p != entry));
			for probe in [1, 10, 25, 38] {
				let nearest = knn_ids(&index, &ds, &seed[probe], 1, 40).await?;
				assert_eq!(nearest, vec![probe as i64]);
			}
			Ok(())
		}

		/// The adjacency list lengths of the live elements.
		async fn list_lengths(ds: &TestIndexStore) -> Result<Vec<usize>> {
			let live: Vec<ElementId> = elements(ds, &ikb())
				.await?
				.into_iter()
				.filter(|(_, e)| !e.deleted)
				.map(|(id, _)| id)
				.collect();
			let tx = ds.transaction(TransactionType::Read).await?;
			let mut lengths = Vec::with_capacity(live.len());
			for id in live {
				let node: DiskAnnNode =
					tx.get_key(&ikb().new_dn_key(id), None).await?.expect("a list");
				lengths.push(node.neighbors.len());
			}
			tx.cancel().await?;
			Ok(lengths)
		}

		/// A starved-degree corpus of `n` clustered vectors, with queries drawn from the same
		/// clusters.
		fn starved_sample(n: usize) -> (Vec<Vec<f32>>, Vec<Vec<f32>>, DiskAnnParams) {
			let mut rng = recall_rng();
			let mut corpus = ClusteredCorpus::new(&mut rng, 32, 200, 0.35);
			let base = corpus.draw(&mut rng, n);
			let queries = corpus.draw(&mut rng, 50);
			(base, queries, graph_params(32, Distance::Cosine, 8, 24))
		}

		/// #1180: every insert runs with the construction retry — the long-lived handle's
		/// single-vector inserts and every frame's batched handle alike — and a starved graph built
		/// with it answers at least as well as one built without. The build time and recall of
		/// both are logged for the comparison the retry is judged on.
		#[test_log::test(tokio::test(flavor = "multi_thread"))]
		async fn diskann_insert_retry_runs_on_every_insert() -> Result<()> {
			let (base, queries, params) = starved_sample(1500);
			let efs = [10, 40];
			let mut measured = Vec::new();
			for retry in [false, true] {
				let ds = TestIndexStore::new().await.with_places(4);
				let mut index = new_index(ikb(), TableId(4), &params, cache()).await?;
				let graph = index.graph.get_mut();
				let tasks = NonZeroUsize::new(4).expect("non-zero");
				assert!(graph.index.config.experimental_insert_retry().is_some());
				assert!(graph.frame_index(tasks)?.config.experimental_insert_retry().is_some());
				if !retry {
					graph.without_insert_retry()?;
					assert!(graph.index.config.experimental_insert_retry().is_none());
					assert!(graph.frame_index(tasks)?.config.experimental_insert_retry().is_none());
				}
				let started = web_time::Instant::now();
				ingest(&index, &ds, 0, &base).await?;
				compact_all(&index, &ds, &ikb()).await?;
				let built = started.elapsed();
				let lengths = list_lengths(&ds).await?;
				let short = lengths.iter().filter(|n| **n < params.degree as usize / 2).count();
				let mean = lengths.iter().sum::<usize>() as f64 / lengths.len() as f64;
				let r = recalls(&index, &ds, &params, &base, &queries, 10, &efs).await?;
				info!(
					"insert retry {retry}: build {built:?}, mean degree {mean:.2}, {short} short \
					 lists, Recall@10 {r:?}"
				);
				measured.push(r);
			}
			for (ef, (a, b)) in efs.iter().zip(measured[0].iter().zip(&measured[1])) {
				assert!(b + 0.02 >= *a, "ef={ef}: recall {b} with the retry, {a} without");
			}
			// Points along a line prune to their neighbours on either side: short lists, but not
			// isolated ones, so the retry leaves them as they are rather than saturating them.
			let params = graph_params(4, Distance::Euclidean, 64, 100);
			let base: Vec<Vec<f32>> = (0..128).map(|i| vec![i as f32, 0.0, 0.0, 0.0]).collect();
			let mut means = Vec::new();
			for retry in [false, true] {
				let ds = TestIndexStore::new().await;
				let mut index = new_index(ikb(), TableId(4), &params, cache()).await?;
				if !retry {
					index.graph.get_mut().without_insert_retry()?;
				}
				ingest(&index, &ds, 0, &base).await?;
				compact_all(&index, &ds, &ikb()).await?;
				let lengths = list_lengths(&ds).await?;
				means.push(lengths.iter().sum::<usize>() as f64 / lengths.len() as f64);
			}
			assert!((means[1] - means[0]).abs() < 1.0, "the retry saturated a line: {means:?}");
			Ok(())
		}

		/// Runs the re-prune sweep over the whole graph, one committed chunk per transaction.
		async fn sweep(index: &DiskAnnIndex, ds: &TestIndexStore) -> Result<usize> {
			let mut from = Some(0);
			let mut chunks = 0;
			while let Some(start) = from {
				let ctx = new_ctx(ds, TransactionType::Write).await;
				let mut tx_guard = BuildTxGuard::new(ctx.tx());
				let (frame, next) = index.reprune_direct(&ctx, start, &mut tx_guard).await?;
				match frame {
					Some(frame) => {
						ctx.tx().commit().await?;
						tx_guard.disarm();
						frame.finish().await;
						chunks += 1;
					}
					None => {
						ctx.tx().cancel().await?;
						tx_guard.disarm();
					}
				}
				from = next;
			}
			Ok(chunks)
		}

		/// #1181: after a graph is built, the re-prune sweep prunes every adjacency list that
		/// back-edges grew past the degree, in chunks of its own transactions; the graph stays
		/// valid and answers as well. A sweep interrupted mid-way and started again from the
		/// beginning ends in the same place.
		#[test_log::test(tokio::test(flavor = "multi_thread"))]
		async fn diskann_reprune_sweep_trims_lists_past_the_degree() -> Result<()> {
			let (base, queries, params) = starved_sample(1500);
			let degree = params.degree as usize;
			let ds = TestIndexStore::new().await.with_places(4);
			let index = new_index(ikb(), TableId(4), &params, cache()).await?;
			ingest(&index, &ds, 0, &base).await?;
			compact_all(&index, &ds, &ikb()).await?;
			let over = list_lengths(&ds).await?.into_iter().filter(|n| *n > degree).count();
			assert!(over > 0, "no list grew past the degree, so the sweep has nothing to do");
			let efs = [10, 40];
			let before = recalls(&index, &ds, &params, &base, &queries, 10, &efs).await?;
			// An interrupted sweep: its second chunk is abandoned and nothing of it commits.
			{
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let mut tx_guard = BuildTxGuard::new(ctx.tx());
				let (frame, next) = index.reprune_direct(&ctx, 0, &mut tx_guard).await?;
				ctx.tx().commit().await?;
				tx_guard.disarm();
				frame.expect("a chunk").finish().await;
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				let mut tx_guard = BuildTxGuard::new(ctx.tx());
				let (frame, _) = index
					.reprune_direct(&ctx, next.expect("a second chunk"), &mut tx_guard)
					.await?;
				frame.expect("a chunk").abort().await;
				ctx.tx().cancel().await?;
				tx_guard.disarm();
			}
			let started = web_time::Instant::now();
			assert_eq!(sweep(&index, &ds).await?, 2, "1500 elements are two chunks");
			let swept = started.elapsed();
			let lengths = list_lengths(&ds).await?;
			assert!(lengths.iter().all(|n| *n <= degree), "a list is still past the degree");
			assert_eq!(assert_adjacency_valid(&ds, &ikb()).await?, base.len());
			let after = recalls(&index, &ds, &params, &base, &queries, 10, &efs).await?;
			info!(
				"re-prune: {over} lists past the degree, swept in {swept:?}; Recall@10 {before:?} \
				 -> {after:?}"
			);
			for (ef, (b, a)) in efs.iter().zip(before.iter().zip(&after)) {
				assert!(a + 0.02 >= *b, "ef={ef}: recall {a} after the sweep, {b} before");
			}
			// A sweep over a graph already swept changes no list.
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let mut tx_guard = BuildTxGuard::new(ctx.tx());
			let (frame, _) = index.reprune_direct(&ctx, 0, &mut tx_guard).await?;
			let frame_stats = index.last_frame().expect("a frame");
			assert!(frame_stats.written_nodes().is_empty(), "a second sweep rewrote a list");
			frame.expect("a chunk").abort().await;
			ctx.tx().cancel().await?;
			tx_guard.disarm();
			Ok(())
		}
	}
}
