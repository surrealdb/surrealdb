use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::HashMap;
use anyhow::{Result, bail, ensure};
use common::EngineError;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
use surrealdb_datastore::pending_guard::{
	PENDING_GUARD_SHARDS, PendingGuardFamily, pending_guard_shard,
};
use surrealdb_datastore::values::diskann::{DiskAnnPendingState, DiskAnnPendingStateKind};
use surrealdb_kvs::{Direction, Val};
use tokio::sync::{RwLock, RwLockReadGuard};
use tracing::debug;
use web_time::Instant;

use crate::catalog::{
	Distance, HnswParams, TableId, VectorType, guards_hnsw_pending, keeps_pending_members,
	keeps_vector_doc_elements, may_hold_legacy_vector_pending,
};
use crate::config::BuildParallelism;
use crate::docids::{DocId, LegacyVectorDocIds};
use crate::env::IndexEnv;
use crate::key::schema::{HnswRecordPendingKey, HnswRecordPendingShardKey};
use crate::key::{KVKey, KVKeyDecode, KVValue, Key, Resumable};
use crate::trees::hnsw::batch::{
	self, GraphBase, InvalidateResident, PublishBatch, RollBackBatch, Staged, Staging,
};
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::docs::{ElementSink, HnswDocs, InPlace, VecDocs};
use crate::trees::hnsw::filter::{FilterCache, HnswTruthyDocumentFilter};
use crate::trees::hnsw::flavor::{HnswFlavor, with_hnsw};
pub use crate::trees::hnsw::outcome::{HnswCompactionError, HnswCompactionOutcome};
use crate::trees::hnsw::{
	ElementId, HnswRecordPendingMember, HnswRecordPendingUpdate, HnswSearch, VectorId,
	VectorPendingUpdate,
};
use crate::trees::knn::{KnnResultBuilder, OVERQUERY_CEILING_FACTOR};
use crate::trees::pending::{
	PendingBacklogReport, PendingScan, PendingScanStats, take_pending_record_id,
};
use crate::trees::vector::{
	DistanceExt as _, SerializedVector, SharedVector, Vector, serialized_vector_from_value,
};
use crate::trees::{KnnCondFilter, KnnIteratorResult, doc_elements};
use crate::val::{Number, RecordId, RecordIdKey, Value};
use crate::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};

/// Which record-keyed pending layout an entry was read from, which decides how
/// its key decodes.
#[derive(Clone, Copy, Debug)]
enum PendingLayout {
	/// The unsharded `!hr` range.
	Legacy,
	/// One shard of the `!hw` range.
	Sharded,
}

/// The record an HNSW pending entry belongs to.
///
/// The rule is [`take_pending_record_id`]; this supplies the key decoder it
/// falls back to, for the layout the entry was read from.
fn pending_record_id(
	key: &[u8],
	layout: PendingLayout,
	pending: &mut HnswRecordPendingUpdate,
) -> Result<RecordIdKey> {
	take_pending_record_id(&mut pending.id, || {
		Ok(match layout {
			PendingLayout::Legacy => HnswRecordPendingKey::decode_key(key)?.id.into_owned(),
			PendingLayout::Sharded => HnswRecordPendingShardKey::decode_key(key)?.id.into_owned(),
		})
	})
}

/// The state of each pending-state guard of an index, by shard: `None` for a
/// guard never written.
type PendingStateSnapshot = Vec<Option<DiskAnnPendingState>>;

/// Whether a guard of `state` lets lookup and compaction skip its shard.
fn guard_unarmed(state: &Option<DiskAnnPendingState>) -> bool {
	state.as_ref().is_none_or(|s| s.kind == DiskAnnPendingStateKind::Empty)
}

/// The record-keyed pending ranges a kNN lookup scans.
struct PendingRanges {
	/// Whether it scans the unsharded `!hr` range.
	legacy: bool,
	/// The `!hw` shards it scans, a bit per shard.
	shards: u64,
}

/// What a scan of the record-keyed pending ranges carries from one range to
/// the next.
struct RecordPendingScan<'a, 's> {
	/// The bounded scan, shared by every range read.
	scan: &'a mut PendingScan<'s>,
	/// The doc-IDs of every pending record read, which the graph search
	/// suppresses.
	all_existing_docs: &'a mut RoaringTreemap,
	/// Cancellation checkpoints so far, across every range read.
	checkpoints: &'a mut usize,
}

/// The pending entries a write to an index that guards its pending updates
/// reads for its record: its own `!hw` entry, and the `!hr` entry an unguarded
/// write at the same stamp may have left. One key type, so that one multi-get
/// reads both.
#[derive(Debug)]
enum WriterPendingKey<'a> {
	Sharded(HnswRecordPendingShardKey<'a>),
	Legacy(HnswRecordPendingKey<'a>),
}

impl KVKey for WriterPendingKey<'_> {
	type Value = HnswRecordPendingUpdate;

	fn encode_buffer(&self, buffer: &mut Vec<u8>) -> Result<()> {
		match self {
			Self::Sharded(key) => key.encode_buffer(buffer),
			Self::Legacy(key) => key.encode_buffer(buffer),
		}
	}

	fn value_context(&self) {}
}

/// Removes `id`'s state from a pending entry, returning its doc-ID and the
/// vectors the graph holds for it, or `None` when the entry holds no state of
/// `id`. A first member left behind becomes the entry's own record, so an entry
/// whose `id` is `None` and that has no others holds nothing.
fn take_record_state(
	entry: &mut HnswRecordPendingUpdate,
	id: &RecordIdKey,
) -> Option<(Option<DocId>, Vec<SerializedVector>)> {
	if entry.id.as_ref().is_none_or(|named| named.addresses_same_record(id)) {
		let taken = (entry.doc_id.take(), std::mem::take(&mut entry.old_vectors));
		entry.new_vectors.clear();
		entry.id = None;
		if !entry.others.is_empty() {
			let next = entry.others.remove(0);
			entry.doc_id = next.doc_id;
			entry.old_vectors = next.old_vectors;
			entry.new_vectors = next.new_vectors;
			entry.id = Some(next.id);
		}
		return Some(taken);
	}
	let position = entry.others.iter().position(|m| m.id.addresses_same_record(id))?;
	let member = entry.others.remove(position);
	Some((member.doc_id, member.old_vectors))
}

pub use crate::trees::hnsw::HNSW_COMPACTION_MAX_PENDING_KEYS;
/// Maximum encoded pending key/value bytes captured by one compaction plan.
const HNSW_COMPACTION_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

/// Exact pending key/value observed by an HNSW compaction read phase.
///
/// Both halves stay as the bytes the scan returned rather than as the key and
/// value they decode to. The write phase deletes conditionally on the value, and
/// the condition is a byte comparison against what is stored: a re-encode has to
/// reproduce those bytes exactly for the delete to fire, and nothing guarantees
/// it does. A decoded value re-encodes under the current revision, which is not
/// the revision an older node wrote it under, and the pending layouts are
/// precisely where entries written by an older node are found.
///
/// The cost of getting that wrong is not a failed delete. `del_compare` reports
/// a mismatch as a lost race, which compaction takes to mean another node got
/// there first — so a re-encode that differed by a byte would abandon the plan
/// silently and forever, and the backlog would never drain.
struct CapturedPendingKey {
	/// Encoded key to delete if the value still matches.
	key: Vec<u8>,
	/// Encoded value that must still be present during conditional delete.
	value: Val,
}

/// Coalesced record operation that should be applied to the HNSW graph.
#[derive(Clone)]
struct PendingOperation {
	/// Existing document ID or record key for a not-yet-resolved document.
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
	/// Graph baseline vectors to remove before applying the desired state.
	old_vectors: Vec<SerializedVector>,
	/// Desired vectors for the record after compaction.
	new_vectors: Vec<SerializedVector>,
}

/// Fixed-size lookup key for identities held by one bounded legacy scoring
/// chunk. Hash collisions are resolved against the full identity before a
/// candidate is marked superseded.
fn legacy_identity_hash(id: &VectorId) -> u64 {
	let mut hasher = DefaultHasher::new();
	id.hash(&mut hasher);
	hasher.finish()
}

/// Marks the matching candidate in a legacy chunk as superseded.
fn suppress_legacy_candidate(
	id: &VectorId,
	ids: &[VectorId],
	positions: &HashMap<u64, Vec<usize>>,
	live: &mut [bool],
) {
	let Some(candidates) = positions.get(&legacy_identity_hash(id)) else {
		return;
	};
	for &position in candidates {
		if live[position] && ids[position] == *id {
			live[position] = false;
		}
	}
}

/// Snapshot gathered by the read phase of HNSW pending compaction.
///
/// The plan stores exact pending key/value pairs for conditional deletion and
/// coalesces all observed work by record identity so replay order does not
/// determine the final graph state.
pub struct HnswCompactionPlan {
	/// Compaction generation observed while preparing the plan.
	generation: Option<u64>,
	captured_keys: Vec<CapturedPendingKey>,
	pending: Vec<PendingOperation>,
	/// The pending-state guards read before any pending range, on an index
	/// that guards its pending updates; empty on one that does not.
	pending_state: PendingStateSnapshot,
	/// The `!hw` shards whose range this plan drains, or found empty, and whose
	/// guards the step after its commit may return to `Empty`.
	cleared_shards: Vec<u16>,
	/// Whether the pending-state summary read other than `Empty` in the plan's
	/// snapshot.
	summary_armed: bool,
	/// Whether this plan reads the `!hr` range to its end, so that once it
	/// commits the range holds nothing the plan's snapshot held.
	legacy_drained: bool,
	has_more: bool,
}

impl HnswCompactionPlan {
	/// Returns whether the plan contains pending keys to apply.
	pub fn has_work(&self) -> bool {
		!self.captured_keys.is_empty()
	}

	/// Returns whether the pass should act on this plan: apply what it
	/// captured, or step guards.
	///
	/// An armed guard or summary calls for a pass even with nothing captured:
	/// the step returns a drained shard's guard to `Empty`, and the summary
	/// once every guard is, so lookup stops scanning an index with nothing
	/// pending. A guard never written holds no entry, and calls for nothing.
	pub fn requires_apply(&self) -> bool {
		self.has_work()
			|| self.summary_armed
			|| self.pending_state.iter().any(|s| !guard_unarmed(s))
	}

	/// Whether the guard step after this plan may clear the summary: it read
	/// armed, `!hr` is drained, and every guard read `Empty` or absent or is one
	/// the step steps.
	///
	/// `!hr` holds what lookup cannot find through a guard, so the summary
	/// stays armed until it is empty: lookup scans `!hr` only under an armed
	/// summary.
	fn summary_clearable(&self) -> bool {
		self.summary_armed
			&& self.legacy_drained
			&& self.pending_state.iter().enumerate().all(|(shard, state)| {
				guard_unarmed(state) || self.cleared_shards.contains(&(shard as u16))
			})
	}

	/// The guard step a compaction pass runs once this plan's transaction has
	/// committed, or, for a plan that captured nothing, been cancelled.
	pub fn guard_step(&self, ikb: &IndexKeyBase) -> HnswGuardStep {
		HnswGuardStep {
			index: ikb.guarded(PendingGuardFamily::Hnsw),
			shards: self.cleared_shards.clone(),
			summary_clearable: self.summary_clearable(),
		}
	}

	/// Returns whether the read phase stopped at the configured batch cap.
	pub fn has_more(&self) -> bool {
		self.has_more
	}

	/// Returns the number of exact pending keys captured by the plan, which
	/// applying it deletes.
	pub fn captured_len(&self) -> usize {
		self.captured_keys.len()
	}

	/// Returns the number of exact pending keys captured by the plan.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.captured_len()
	}
}

/// Mutable accumulator for building a bounded HNSW compaction plan.
struct PendingPlanBuilder {
	generation: Option<u64>,
	captured_keys: Vec<CapturedPendingKey>,
	pending: Vec<PendingOperation>,
	pending_by_id: HashMap<VectorId, usize>,
	encoded_bytes: usize,
	has_more: bool,
}

impl PendingPlanBuilder {
	/// Creates an empty compaction-plan accumulator for a generation snapshot.
	fn new(generation: Option<u64>) -> Self {
		Self {
			generation,
			captured_keys: Vec::new(),
			pending: Vec::new(),
			pending_by_id: HashMap::default(),
			encoded_bytes: 0,
			has_more: false,
		}
	}

	/// Captures one pending key/value and folds its operations into the plan:
	/// one per record the entry holds.
	///
	/// Returns `false` when adding the pending entry would exceed a batch cap.
	fn add(
		&mut self,
		key: Vec<u8>,
		value: Val,
		pending: impl IntoIterator<Item = PendingOperation>,
	) -> bool {
		if self.captured_keys.len() >= HNSW_COMPACTION_MAX_PENDING_KEYS
			|| (!self.captured_keys.is_empty()
				&& self.encoded_bytes + key.len() + value.len() > HNSW_COMPACTION_MAX_PENDING_BYTES)
		{
			self.has_more = true;
			return false;
		}
		self.encoded_bytes += key.len() + value.len();
		self.captured_keys.push(CapturedPendingKey {
			key,
			value,
		});
		for pending in pending {
			self.add_pending(pending);
		}
		if self.captured_keys.len() >= HNSW_COMPACTION_MAX_PENDING_KEYS
			|| self.encoded_bytes >= HNSW_COMPACTION_MAX_PENDING_BYTES
		{
			self.has_more = true;
		}
		true
	}

	/// Coalesces one pending operation into the record operation list.
	///
	/// Repeated operations for the same vector identity keep the first graph
	/// baseline and replace the desired vectors with the latest state.
	fn add_pending(&mut self, pending: PendingOperation) {
		if let Some(pos) = self.pending_by_id.get(&pending.id) {
			self.pending[*pos].new_vectors = pending.new_vectors;
			return;
		}
		let pos = self.pending.len();
		self.pending_by_id.insert(pending.id.clone(), pos);
		self.pending.push(pending);
	}

	/// Converts the accumulator into an immutable compaction plan.
	fn into_plan(self, guards: PlanGuards) -> HnswCompactionPlan {
		HnswCompactionPlan {
			generation: self.generation,
			captured_keys: self.captured_keys,
			pending: self.pending,
			pending_state: guards.pending_state,
			cleared_shards: guards.cleared_shards,
			summary_armed: guards.summary_armed,
			legacy_drained: guards.legacy_drained,
			has_more: self.has_more,
		}
	}
}

/// What a compaction plan's read of one record-keyed pending range found.
#[derive(Default)]
struct RangeCapture {
	/// Whether the range was read to its end within the plan's budget.
	drained: bool,
	/// The pending-state shard of the first record read from the range.
	first_shard: Option<u16>,
}

/// What a compaction plan read of its index's pending-state guards.
#[derive(Default)]
struct PlanGuards {
	pending_state: PendingStateSnapshot,
	cleared_shards: Vec<u16>,
	summary_armed: bool,
	legacy_drained: bool,
}

/// Steps the guards of the `!hw` shards a compaction plan drained to `Empty`,
/// and the summary with them where the plan allows it
/// ([`HnswCompactionPlan::guard_step`]).
///
/// The step runs in a transaction of its own, under the index's guard fence,
/// after the plan's own transaction has committed: the apply deletes the
/// entries it captured, and the step re-checks each shard's emptiness in its
/// own snapshot (`Transaction::step_pending_guards`), so a write committed in
/// between keeps its guard armed.
pub struct HnswGuardStep {
	index: surrealdb_datastore::pending_guard::GuardedIndex,
	shards: Vec<u16>,
	summary_clearable: bool,
}

impl HnswGuardStep {
	/// Runs the step through `tx`'s datastore, and reports whether it stepped
	/// a guard or the summary. A step that has nothing to step does nothing.
	///
	/// Boxed because the step holds a transaction of its own, which would
	/// otherwise grow the compaction's future by its size.
	pub async fn run(&self, tx: &Transaction) -> Result<bool> {
		if self.shards.is_empty() && !self.summary_clearable {
			return Ok(false);
		}
		Box::pin(tx.step_pending_guards(&self.index, &self.shards, self.summary_clearable)).await
	}
}

/// The resident graph, and how far it has been reconciled with the index's
/// invalidations.
///
/// A batch that reached the graph and did not commit leaves it holding state
/// the store never took, which its layer versions cannot reveal. Such a batch
/// advances [`HnswIndex::invalidations`], and a graph whose `reconciled_at`
/// trails that count reloads every layer at its next state check.
pub(super) struct ResidentGraph {
	pub(super) graph: HnswFlavor,
	/// The index's invalidation count when this graph last reloaded in full.
	reconciled_at: u64,
}

impl std::ops::Deref for ResidentGraph {
	type Target = HnswFlavor;

	fn deref(&self) -> &HnswFlavor {
		&self.graph
	}
}

impl std::ops::DerefMut for ResidentGraph {
	fn deref_mut(&mut self) -> &mut HnswFlavor {
		&mut self.graph
	}
}

/// High-level HNSW index supporting concurrent reads and writes.
///
/// Writes are handled through a two-phase approach:
/// 1. **Enqueueing**: The [`index`](Self::index) method converts document changes into record-keyed
///    [`HnswRecordPendingUpdate`] entries stored in the key-value store.
/// 2. **Applying**: Compaction prepares a bounded pending snapshot and applies it after the
///    captured keys are conditionally deleted — staged and published on commit when it only adds
///    vectors, in place under the graph write lock otherwise (see [`Self::apply_compaction`]).
///
/// Reads via [`knn_search`](Self::knn_search) scan pending updates
/// conservatively, then search the committed graph under a read lock and merge
/// both result sets into a single k-nearest neighbor response.
pub struct HnswIndex {
	/// Expected vector dimensionality.
	dim: usize,
	/// Distance metric used for similarity computation.
	distance: Distance,
	/// Stable table id used to scope process-local HNSW cache entries.
	table_id: TableId,
	/// Key base for generating index-related storage keys.
	ikb: IndexKeyBase,
	/// The type of vector stored in this index.
	vector_type: VectorType,
	/// Shared HNSW cache used for hot vector/doc mapping lookups.
	vector_cache: VectorCache,
	/// The HNSW graph, protected by a read-write lock for concurrent access.
	/// Shared, so a committed staged batch can publish into it.
	hnsw: Arc<RwLock<ResidentGraph>>,
	/// How many batches have reached the resident graph and not committed.
	/// Shared with those batches' rollback actions, which advance it.
	invalidations: Arc<AtomicU64>,
	/// Vector-to-document mappings.
	vec_docs: VecDocs,
	/// The stored index format that selects shared or legacy doc-ID mappings.
	format_version: u16,
	/// Set from the moment a kNN scan reads more of this index's pending queue
	/// than one materialisation batch holds, by entry count or by a scoring
	/// rollover. Re-armed by a scan that runs to completion inside both budgets
	/// only if no newer scan observed another crossing, so the warning is
	/// emitted once per crossing rather than once per query.
	pending_backlog_reported: PendingBacklogReport,
	/// What the kNN pending scans on this index materialised. Zero-sized
	/// outside test builds.
	pending_scan_stats: PendingScanStats,
}

/// Contextual state passed through HNSW graph operations.
///
/// Bundles the frozen query context, transaction, index key base, and
/// vector-document mappings needed by the graph and document layers.
pub(super) struct HnswContext<'a> {
	/// The environment of the query driving this operation.
	pub(super) env: &'a dyn IndexEnv,
	/// The current transaction.
	pub(super) tx: Arc<Transaction>,
	/// Key base for generating index-related storage keys.
	pub(super) ikb: IndexKeyBase,
	/// Reference to the vector-document mappings.
	pub(super) vec_docs: &'a VecDocs,
}

impl<'a> HnswContext<'a> {
	/// Creates graph-operation context backed by the environment's transaction.
	pub(super) fn new(env: &'a dyn IndexEnv, ikb: IndexKeyBase, vec_docs: &'a VecDocs) -> Self {
		Self {
			env,
			tx: env.tx(),
			ikb,
			vec_docs,
		}
	}
}

impl HnswIndex {
	pub(crate) fn format_version(&self) -> u16 {
		self.format_version
	}

	/// Creates a new HNSW index, loading existing document state from the transaction.
	pub(crate) async fn new(
		vector_cache: VectorCache,
		_tx: &Transaction,
		ikb: IndexKeyBase,
		tb: TableId,
		p: &HnswParams,
		format_version: u16,
	) -> Result<Self> {
		Ok(Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			distance: p.distance.clone(),
			table_id: tb,
			hnsw: Arc::new(RwLock::new(ResidentGraph {
				graph: HnswFlavor::new(tb, ikb.clone(), p, vector_cache.clone(), format_version)?,
				reconciled_at: 0,
			})),
			invalidations: Arc::new(AtomicU64::new(0)),
			vec_docs: VecDocs::new(
				ikb.clone(),
				tb,
				vector_cache.clone(),
				p.use_hashed_vector,
				format_version,
			),
			vector_cache,
			ikb,
			format_version,
			pending_backlog_reported: PendingBacklogReport::default(),
			pending_scan_stats: PendingScanStats::default(),
		})
	}

	/// The resident graph, for the tests that build a batch over it directly.
	#[cfg(test)]
	pub(super) fn resident(&self) -> Arc<RwLock<ResidentGraph>> {
		Arc::clone(&self.hnsw)
	}

	/// What the kNN pending scans on this index materialised.
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

	/// Converts content values into serialized vectors, validating dimensionality.
	fn content_to_vectors(&self, content: Vec<Value>) -> Result<Vec<SerializedVector>> {
		let mut vectors = Vec::with_capacity(content.len());
		// Index the values
		for value in content.into_iter().filter(|v| !v.is_nullish()) {
			// Extract the vector
			let vector = serialized_vector_from_value(self.vector_type, self.dim, value)?;
			Vector::check_expected_dimension(vector.dimension(), self.dim)?;
			// Insert the vector
			vectors.push(vector);
		}
		Ok(vectors)
	}

	/// Enqueues a vector update for later application to the HNSW graph.
	///
	/// Converts old/new document values into serialized vectors and stores them
	/// in the record-keyed pending value, as the state of `id`'s record. Repeated
	/// writes to the same record preserve the original graph baseline and replace
	/// only the desired final vectors, so compaction can apply the record's final
	/// state directly.
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
		if guards_hnsw_pending(self.format_version) {
			return self.index_guarded(&tx, id, old_vectors, new_vectors).await;
		}
		let key = self.ikb.new_hr_key(id);
		// The entry's key spells `id` without its numbers' kinds, so it may hold
		// other records. A write folds into its own record's state: the graph
		// baseline is kept and the desired vectors replaced. An index stamped
		// before entries hold every record they spell folds into whichever
		// record the entry names, as that release did.
		let pending = match tx.get_key(&key, None).await? {
			Some(mut pending) => {
				let own = !keeps_pending_members(self.format_version)
					|| pending.id.as_ref().is_none_or(|named| named.addresses_same_record(id));
				if own {
					pending.new_vectors = new_vectors;
					// Every write stamps the exact id, so compaction and lookup
					// never recover it from a key that cannot spell it.
					pending.id = Some(id.clone());
				} else if let Some(member) =
					pending.others.iter_mut().find(|m| m.id.addresses_same_record(id))
				{
					member.new_vectors = new_vectors;
				} else {
					pending.others.push(HnswRecordPendingMember {
						doc_id: HnswDocs::get_doc_id(&self.ikb, self.format_version, &tx, id)
							.await?,
						old_vectors,
						new_vectors,
						id: id.clone(),
					});
				}
				pending
			}
			None => HnswRecordPendingUpdate {
				doc_id: HnswDocs::get_doc_id(&self.ikb, self.format_version, &tx, id).await?,
				old_vectors,
				new_vectors,
				id: Some(id.clone()),
				others: Vec::new(),
			},
		};
		tx.set_key(&key, &pending).await?;
		Ok(())
	}

	/// Enqueues a vector update on an index that guards its pending updates
	/// ([`guards_hnsw_pending`]): into the `!hw` entry of `id`'s shard, whose
	/// guard the transaction's commit covers ([`Self::mark_pending_non_empty`]).
	///
	/// The entry's key spells `id` without its numbers' kinds, so it may hold
	/// other records: a write folds into its own record's state, kept apart
	/// from the others', as on `!hr`.
	///
	/// A `!hr` entry holding `id`'s state is an earlier write, made unguarded
	/// at the same stamp. That state moves into `!hw` with this write and keeps
	/// its graph baseline, so `id` is never pending under both layouts at once;
	/// any other record the `!hr` entry holds stays there, for its own write or
	/// compaction to move. Both entries are read in one multi-get.
	async fn index_guarded(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
		old_vectors: Vec<SerializedVector>,
		new_vectors: Vec<SerializedVector>,
	) -> Result<()> {
		let shard = pending_guard_shard(id);
		let key = self.ikb.new_hw_key(shard, id);
		let legacy_key = self.ikb.new_hr_key(id);
		let mut read = tx
			.get_many_key(
				vec![
					WriterPendingKey::Sharded(key.clone()),
					WriterPendingKey::Legacy(legacy_key.clone()),
				],
				None,
			)
			.await?
			.into_iter();
		let (sharded, legacy) = (read.next().flatten(), read.next().flatten());
		// The baseline `id` is pending from, when an earlier write left one in
		// `!hr`: its doc-ID and the vectors the graph holds for it.
		let baseline = match legacy {
			Some(mut legacy) => match take_record_state(&mut legacy, id) {
				Some(baseline) => {
					if legacy.id.is_none() && legacy.others.is_empty() {
						tx.del_key(&legacy_key).await?;
					} else {
						tx.set_key(&legacy_key, &legacy).await?;
					}
					Some(baseline)
				}
				None => None,
			},
			None => None,
		};
		let pending = match sharded {
			Some(mut pending) => {
				if pending.id.as_ref().is_none_or(|named| named.addresses_same_record(id)) {
					pending.new_vectors = new_vectors;
					pending.id = Some(id.clone());
				} else if let Some(member) =
					pending.others.iter_mut().find(|m| m.id.addresses_same_record(id))
				{
					member.new_vectors = new_vectors;
				} else {
					let (doc_id, old_vectors) = match baseline {
						Some(baseline) => baseline,
						None => (
							HnswDocs::get_doc_id(&self.ikb, self.format_version, tx, id).await?,
							old_vectors,
						),
					};
					pending.others.push(HnswRecordPendingMember {
						doc_id,
						old_vectors,
						new_vectors,
						id: id.clone(),
					});
				}
				pending
			}
			None => {
				let (doc_id, old_vectors) = match baseline {
					Some(baseline) => baseline,
					None => (
						HnswDocs::get_doc_id(&self.ikb, self.format_version, tx, id).await?,
						old_vectors,
					),
				};
				HnswRecordPendingUpdate {
					doc_id,
					old_vectors,
					new_vectors,
					id: Some(id.clone()),
					others: Vec::new(),
				}
			}
		};
		tx.set_key(&key, &pending).await?;
		Self::mark_pending_non_empty(tx, &self.ikb, shard);
		Ok(())
	}

	/// Covers the pending-state guard of `shard`, which this transaction wrote
	/// a `!hw` entry into.
	///
	/// Lookup and compaction skip a shard whose guard is `Empty`, so a `!hw`
	/// entry must not commit under an `Empty` guard. The writing transaction
	/// neither reads nor writes the guard, so no guard step can fail its
	/// commit: it names the shard, and its commit covers the guard under the
	/// index's guard fence (`Transaction::buffer_pending_guard`).
	///
	/// Where this datastore is the store's sole writer
	/// ([`Transaction::sole_writer`]), the commit arms the guard, and the fence
	/// orders the arming against every guard step. Elsewhere other nodes step
	/// guards too, and the commit covers the guard with this datastore's
	/// writer lease on the shard instead (`!hz`), under which no node steps it.
	fn mark_pending_non_empty(tx: &Transaction, ikb: &IndexKeyBase, shard: u16) {
		tx.buffer_pending_guard(&ikb.guarded(PendingGuardFamily::Hnsw), shard);
	}

	/// Converts an append-keyed pending value into a graph operation.
	fn append_pending_to_operation(pending: VectorPendingUpdate) -> PendingOperation {
		PendingOperation {
			id: pending.id,
			record: None,
			old_vectors: pending.old_vectors,
			new_vectors: pending.new_vectors,
		}
	}

	/// Converts a record-keyed pending value into a graph operation.
	///
	/// Existing records are addressed by their graph document ID. Records that
	/// have not reached the graph are addressed by their record key until
	/// compaction resolves a document ID for them.
	fn record_pending_to_operation(
		id: RecordIdKey,
		pending: HnswRecordPendingUpdate,
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

	/// Reads the pending-state guard of every shard, in one ordered batch.
	///
	/// The read is noted on the transaction
	/// ([`Transaction::note_pending_guards_read`]): on a store that validates
	/// every read, a guard this transaction read is covered by its own commit.
	///
	/// A shard this transaction wrote a pending update into reads `NonEmpty`
	/// whatever is stored: on a sole-writer store its guard is armed only when
	/// the transaction commits ([`Self::mark_pending_non_empty`]), and a lookup
	/// in the transaction must still see the write.
	async fn read_pending_state(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<PendingStateSnapshot> {
		let guarded = ikb.guarded(PendingGuardFamily::Hnsw);
		let mut states: PendingStateSnapshot = vec![None; usize::from(PENDING_GUARD_SHARDS)];
		let guards: Vec<(Vec<u8>, DiskAnnPendingState)> =
			tx.scan(guarded.guards()?, PENDING_GUARD_SHARDS.into(), 0, None).await?;
		for (key, state) in guards {
			let shard = usize::from(guarded.decode_guard_shard(&key)?);
			ensure!(shard < states.len(), "HNSW pending-state guard for unknown shard {shard}");
			states[shard] = Some(state);
		}
		tx.note_pending_guards_read(&guarded);
		let buffered = tx.buffered_pending_guard_shards(&guarded);
		for (shard, state) in states.iter_mut().enumerate() {
			if buffered & (1 << shard) != 0 && guard_unarmed(state) {
				*state = Some(DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: state.as_ref().map_or(0, |s| s.generation),
				});
			}
		}
		Ok(states)
	}

	/// The record-keyed pending ranges a lookup in `tx` scans.
	///
	/// An index that does not guard its pending updates
	/// ([`guards_hnsw_pending`]) keeps them all in `!hr`, which is scanned.
	///
	/// On one that does, a summary that reads `Empty` or is absent proves in
	/// this snapshot that every guard does too, so nothing is scanned. The
	/// summary is armed in the transaction that arms a guard from any other
	/// state, which commits before the entry the guard covers, so a snapshot
	/// holding the entry holds the summary armed; and compaction arms it over
	/// any `!hr` entry it finds, and steps it back only once `!hr` is empty. A
	/// shard this transaction wrote a pending update into is armed only when it
	/// commits, so while it has any, the guards are read with them overlaid
	/// ([`Self::read_pending_state`]). The summary read is noted as a guard read
	/// ([`Transaction::note_pending_guards_read`]): on a store that validates
	/// every read, a guard this transaction then writes under, and its summary,
	/// are covered by its own commit.
	///
	/// Otherwise `!hr` and every armed `!hw` shard are scanned.
	async fn read_lookup_pending_ranges(&self, tx: &Transaction) -> Result<PendingRanges> {
		if !guards_hnsw_pending(self.format_version) {
			return Ok(PendingRanges {
				legacy: true,
				shards: 0,
			});
		}
		let guarded = self.ikb.guarded(PendingGuardFamily::Hnsw);
		if tx.buffered_pending_guard_shards(&guarded) == 0 {
			let summary = tx.get_key(&guarded.summary(), None).await?;
			tx.note_pending_guards_read(&guarded);
			if summary.is_none_or(|summary| summary.kind == DiskAnnPendingStateKind::Empty) {
				return Ok(PendingRanges {
					legacy: false,
					shards: 0,
				});
			}
		}
		let states = Self::read_pending_state(tx, &self.ikb).await?;
		let shards = states
			.iter()
			.enumerate()
			.filter(|(_, state)| !guard_unarmed(state))
			.fold(0u64, |bits, (shard, _)| bits | 1 << shard);
		Ok(PendingRanges {
			legacy: true,
			shards,
		})
	}

	/// Creates an [`HnswContext`] from the current index state and an environment.
	pub(super) fn new_hnsw_context<'a>(&'a self, env: &'a dyn IndexEnv) -> HnswContext<'a> {
		HnswContext::new(env, self.ikb.clone(), &self.vec_docs)
	}

	/// Builds a bounded compaction plan for HNSW pending updates.
	///
	/// The read phase records exact key/value pairs for conditional deletion
	/// and coalesces pending work by document identity. It drains in stages:
	///
	///   1. The append-keyed `!hp` range, then the record-keyed `!hr` range. While these are not
	///      both drained within one plan, the plan stops there and reports more to come.
	///   2. On an index that guards its pending updates ([`guards_hnsw_pending`]), each `!hw` shard
	///      whose guard is armed, in shard order, until the batch budget is reached. Every shard
	///      drained within budget is recorded in the plan's cleared shards, so the guard step after
	///      it returns that shard's guard to `Empty`.
	///
	/// On such an index a guard that reads `Empty` or absent holds no
	/// committed entry, except where a repair after the fact failed or never
	/// ran, so the plan probes those shards and repairs the guard of any that
	/// holds entries (`Transaction::repair_pending_guards`). A `!hr` entry is
	/// one no guard covers, written unguarded at the same stamp: under a
	/// summary that reads `Empty`, the plan repairs the guard of that entry's
	/// shard, which arms the summary, so lookup scans `!hr` until it drains.
	pub(crate) async fn prepare_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
		format_version: u16,
	) -> Result<HnswCompactionPlan> {
		let tx = env.tx();
		let generation = read_compaction_generation(&tx, &ikb.new_hg_key()).await?;
		let guarded = guards_hnsw_pending(format_version);
		let mut guards = PlanGuards::default();
		if guarded {
			guards.pending_state = Self::read_pending_state(&tx, ikb).await?;
			guards.summary_armed = tx
				.get_key(&ikb.guarded(PendingGuardFamily::Hnsw).summary(), None)
				.await?
				.is_some_and(|summary| summary.kind != DiskAnnPendingStateKind::Empty);
		}
		let mut builder = PendingPlanBuilder::new(generation);
		let mut count = 0;
		if may_hold_legacy_vector_pending(format_version) {
			Self::collect_append_pending_for_plan(env, &tx, ikb, &mut builder, &mut count).await?;
		}
		let mut legacy = RangeCapture::default();
		if !builder.has_more {
			legacy = Self::collect_record_pending_for_plan(
				env,
				&tx,
				ikb.new_hr_range()?,
				PendingLayout::Legacy,
				&mut builder,
				&mut count,
			)
			.await?;
		}
		if !guarded {
			return Ok(builder.into_plan(guards));
		}
		let guarded_index = ikb.guarded(PendingGuardFamily::Hnsw);
		if let Some(shard) = legacy.first_shard
			&& !guards.summary_armed
		{
			tx.repair_pending_guards(&guarded_index, &[shard]).await?;
			guards.summary_armed = true;
			guards.pending_state[usize::from(shard)] = Some(DiskAnnPendingState {
				kind: DiskAnnPendingStateKind::NonEmpty,
				generation: 0,
			});
		}
		if !legacy.drained {
			// The range may hold more than the budget let in. Every pass ends
			// once a plan reports nothing more, so this one must report more.
			builder.has_more = true;
			return Ok(builder.into_plan(guards));
		}
		guards.legacy_drained = true;
		let mut orphaned = Vec::new();
		let holding = if guards.pending_state.iter().any(guard_unarmed) {
			let holding = tx.shards_holding_pending_entries(&guarded_index).await?;
			orphaned = guards
				.pending_state
				.iter()
				.enumerate()
				.filter(|(shard, state)| holding & (1 << shard) != 0 && guard_unarmed(state))
				.map(|(shard, _)| shard as u16)
				.collect();
			tx.repair_pending_guards(&guarded_index, &orphaned).await?;
			Some(holding)
		} else {
			None
		};
		for (shard, state) in guards.pending_state.iter().enumerate() {
			if guard_unarmed(state) {
				continue;
			}
			let shard = shard as u16;
			// Read in this snapshot, the probe tells which armed shards hold
			// nothing: those are cleared without a scan.
			if holding.is_some_and(|holding| holding & (1 << shard) == 0) {
				guards.cleared_shards.push(shard);
				continue;
			}
			let captured = Self::collect_record_pending_for_plan(
				env,
				&tx,
				ikb.new_hw_shard_range(shard)?,
				PendingLayout::Sharded,
				&mut builder,
				&mut count,
			)
			.await?;
			if !captured.drained {
				break;
			}
			guards.cleared_shards.push(shard);
		}
		if !orphaned.is_empty() {
			guards.summary_armed = true;
			for shard in orphaned {
				guards.pending_state[usize::from(shard)] = Some(DiskAnnPendingState {
					kind: DiskAnnPendingStateKind::NonEmpty,
					generation: 0,
				});
			}
			builder.has_more = true;
		}
		Ok(builder.into_plan(guards))
	}

	/// Adds append-keyed pending entries to a compaction plan.
	///
	/// Entries are captured as exact encoded key/value pairs so the write phase
	/// can remove only values that still match the read snapshot.
	async fn collect_append_pending_for_plan(
		env: &dyn IndexEnv,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		builder: &mut PendingPlanBuilder,
		count: &mut usize,
	) -> Result<()> {
		let rng = ikb.new_hp_range()?;
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (key, value) in batch.iter() {
				if env.is_done(Some(*count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				let pending = VectorPendingUpdate::kv_decode_value(value, ())?;
				let pending = Self::append_pending_to_operation(pending);
				// The plan captures each entry's exact bytes for its conditional
				// delete, so the copy is made here rather than for the whole batch
				// up front: the caps below stop a part-read batch as a matter of
				// course, and what the loop never reaches is never copied.
				if !builder.add(key.to_vec(), value.to_vec(), [pending]) {
					return Ok(());
				}
				*count += 1;
				if builder.has_more {
					return Ok(());
				}
			}
		}
		Ok(())
	}

	/// Adds the entries of one record-keyed pending range, `!hr` or one `!hw`
	/// shard as `layout` says, to a compaction plan, and reports whether the
	/// range was read to its end and the shard of the first record read.
	///
	/// The record identity comes from the stored pending value, which keeps it
	/// exactly. The key spells its id under a codec that omits a number's kind,
	/// so it answers only for an entry written before the value carried one.
	async fn collect_record_pending_for_plan(
		env: &dyn IndexEnv,
		tx: &Transaction,
		rng: crate::key::TypedRange<HnswRecordPendingUpdate>,
		layout: PendingLayout,
		builder: &mut PendingPlanBuilder,
		count: &mut usize,
	) -> Result<RangeCapture> {
		let mut capture = RangeCapture::default();
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				capture.drained = true;
				return Ok(capture);
			}
			for (key, value) in batch.iter() {
				if env.is_done(Some(*count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				// One operation per record the entry holds; only the first can
				// predate the id field, and so need its key to name it.
				let mut operations = Vec::with_capacity(1);
				for mut pending in
					HnswRecordPendingUpdate::kv_decode_value(value, ())?.into_records()
				{
					let id = pending_record_id(key, layout, &mut pending)?;
					if capture.first_shard.is_none() {
						capture.first_shard = Some(pending_guard_shard(&id));
					}
					operations.push(Self::record_pending_to_operation(id, pending));
				}
				// Copied per entry rather than per batch, for the reason given in
				// `collect_append_pending_for_plan`.
				if !builder.add(key.to_vec(), value.to_vec(), operations) {
					return Ok(capture);
				}
				*count += 1;
				if builder.has_more {
					return Ok(capture);
				}
			}
		}
	}

	/// Applies a prepared HNSW pending compaction plan.
	///
	/// The write phase first advances the generation and conditionally deletes
	/// only the exact pending values observed by the read phase. The graph is
	/// changed only after those guards succeed, in one of two ways:
	///
	/// * A plan whose operations only add vectors is **staged**, unless the index engines are
	///   configured for [`BuildParallelism::Serial`]: its graph changes are built on
	///   `hnsw_build_parallelism` tasks against the committed graph, written into the transaction,
	///   and taken by the resident graph when the transaction commits.
	/// * Any other plan is applied **in place** under the graph write lock: baseline removals run
	///   serially first, and when `hnsw_build_parallelism` is not [`BuildParallelism::Serial`], new
	///   elements are allocated and linked into the post-removal graph on `hnsw_build_parallelism`
	///   tasks by [`batch::build`] before installing into the resident graph.
	///
	/// A transaction may carry more than one apply for an index. Each then
	/// reads the graph back through that transaction, and every batch's
	/// rollback action accounts for what that can bring into the resident
	/// graph; see [`HnswCompactionOutcome`].
	pub(crate) async fn apply_compaction(
		&self,
		env: &dyn IndexEnv,
		plan: HnswCompactionPlan,
	) -> std::result::Result<HnswCompactionOutcome, HnswCompactionError> {
		let HnswCompactionPlan {
			generation,
			captured_keys,
			pending,
			pending_state: _,
			cleared_shards: _,
			summary_armed: _,
			legacy_drained: _,
			has_more: _,
		} = plan;
		let tx = env.tx();
		if captured_keys.is_empty() {
			return Ok(HnswCompactionOutcome::Empty);
		}
		if !bump_compaction_generation(&tx, &self.ikb.new_hg_key(), generation)
			.await
			.map_err(HnswCompactionError::untouched)?
		{
			return Ok(HnswCompactionOutcome::LostRace);
		}
		for captured in &captured_keys {
			match tx.del_compare(Key::from(&captured.key), Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => {
					return Ok(HnswCompactionOutcome::LostRace);
				}
				Err(e) => return Err(HnswCompactionError::untouched(e)),
			}
		}
		let parallelism = env.config().hnsw_build_parallelism;
		if parallelism != BuildParallelism::Serial
			&& pending.iter().all(|pending| pending.old_vectors.is_empty())
		{
			self.apply_staged(env, pending, parallelism.tasks())
				.await
				.map_err(HnswCompactionError::untouched)?;
			return Ok(HnswCompactionOutcome::Staged);
		}
		self.apply_in_place(env, pending, parallelism).await?;
		Ok(HnswCompactionOutcome::InPlace)
	}

	/// Applies a plan to the resident graph under the graph write lock.
	///
	/// Under [`BuildParallelism::Serial`], operations run one at a time via
	/// [`Self::apply_pending_operation`]. Otherwise every operation's baseline
	/// removals run first, and any remaining vector additions are staged against
	/// the post-removal graph, linked on `parallelism.tasks()` tasks by
	/// [`batch::build`], written to the transaction, and installed into the
	/// resident graph while the write lock remains held.
	async fn apply_in_place(
		&self,
		env: &dyn IndexEnv,
		pending: Vec<PendingOperation>,
		parallelism: BuildParallelism,
	) -> std::result::Result<(), HnswCompactionError> {
		let tx = env.tx();
		let mut resident = Arc::clone(&self.hnsw).write_owned().await;
		self.reconcile(&mut resident, env).await.map_err(HnswCompactionError::untouched)?;
		// Registered before the first operation reaches the graph: from here
		// on, a transaction that does not commit leaves the resident graph and
		// the vector cache holding what the store never took.
		tx.on_rollback(Box::new(InvalidateResident {
			invalidations: Arc::clone(&self.invalidations),
			elements: resident.elements().view(None),
		}))
		.await;
		async {
			let docs = HnswDocs::with_format_version(self.ikb.clone(), self.format_version);
			if parallelism == BuildParallelism::Serial {
				let ctx = self.new_hnsw_context(env);
				for pending in pending {
					self.apply_pending_operation(&ctx, &docs, &mut resident.graph, pending).await?;
				}
				return Ok(());
			}
			let mut additions = Vec::with_capacity(pending.len());
			{
				let ctx = self.new_hnsw_context(env);
				for mut pending in pending {
					if let VectorId::DocId(doc_id) = &pending.id {
						for vector in std::mem::take(&mut pending.old_vectors) {
							let vector = Vector::from(vector);
							self.vec_docs
								.remove(&ctx, &vector, *doc_id, &mut resident.graph)
								.await?;
						}
					}
					if pending.new_vectors.is_empty() {
						self.add_pending_vectors(
							&ctx,
							&docs,
							&mut InPlace(&mut resident.graph),
							pending,
						)
						.await?;
					} else {
						additions.push(pending);
					}
				}
			}
			if additions.is_empty() {
				return Ok(());
			}
			let base = with_hnsw!(&resident.graph, h => GraphBase::of(h));
			let mut staging = Staging::new(base.next_element_id());
			{
				let ctx = self.new_hnsw_context(env);
				let mut sink = Staged {
					graph: &mut resident.graph,
					staging: &mut staging,
				};
				for pending in additions {
					self.add_pending_vectors(&ctx, &docs, &mut sink, pending).await?;
				}
			}
			let adds_nothing = staging.adds_nothing();
			let (elements, doc_sets) = staging.into_parts();
			for (element, _) in doc_sets {
				self.vec_docs.remove_cached_doc_set(element).await;
			}
			if adds_nothing {
				return Ok(());
			}
			let vectors: Vec<(ElementId, SharedVector)> =
				elements.iter().map(|e| (e.id, e.vector.clone())).collect();
			let committed = resident.elements().view(None);
			let resident = Arc::new(resident);
			let staged = batch::build(
				Arc::clone(&resident),
				Arc::clone(&tx),
				base,
				elements,
				&env.places(),
				parallelism.tasks(),
			)
			.await?;
			with_hnsw!(&resident.graph, h => staged.write(h, &tx).await)?;
			let mut resident = Arc::into_inner(resident).ok_or_else(|| {
				anyhow::anyhow!("HNSW write guard remained shared after parallel batch build")
			})?;
			with_hnsw!(&mut resident.graph, h => {
				staged.install(h);
			});
			committed.cache_committed(vectors.into_iter()).await;
			Ok(())
		}
		.await
		.map_err(HnswCompactionError::in_place)
	}

	/// Stages a plan whose operations only add vectors.
	///
	/// Step 0 runs here, serially, under the graph write lock: document ids are
	/// resolved and every vector is matched against the vector-to-documents
	/// mapping through the transaction, so a vector the index already holds —
	/// committed or earlier in this batch — only gains the document, and every
	/// other vector is allocated as a new element. The lock is then downgraded,
	/// and the new elements are linked into the graph on `tasks` tasks by
	/// [`batch::build`]. The resident graph and the caches take the batch from
	/// a commit action; see the `batch` module.
	async fn apply_staged(
		&self,
		env: &dyn IndexEnv,
		pending: Vec<PendingOperation>,
		tasks: usize,
	) -> Result<()> {
		let tx = env.tx();
		let started = Instant::now();
		let mut resident = Arc::clone(&self.hnsw).write_owned().await;
		self.reconcile(&mut resident, env).await?;
		let base = with_hnsw!(&resident.graph, h => GraphBase::of(h));
		let mut staging = Staging::new(base.next_element_id());
		{
			let ctx = self.new_hnsw_context(env);
			let docs = HnswDocs::with_format_version(self.ikb.clone(), self.format_version);
			let mut sink = Staged {
				graph: &mut resident.graph,
				staging: &mut staging,
			};
			for pending in pending {
				self.add_pending_vectors(&ctx, &docs, &mut sink, pending).await?;
			}
		}
		let adds_nothing = staging.adds_nothing();
		let (elements, doc_sets) = staging.into_parts();
		let first = base.next_element_id();
		let new_elements = first..first + elements.len() as ElementId;
		let vectors: Vec<(ElementId, SharedVector)> =
			elements.iter().map(|e| (e.id, e.vector.clone())).collect();
		let committed = resident.elements().view(None);
		let allocated = started.elapsed();
		let staged = if adds_nothing {
			drop(resident);
			None
		} else {
			let started = Instant::now();
			let resident = Arc::new(resident.downgrade());
			let staged = batch::build(
				Arc::clone(&resident),
				Arc::clone(&tx),
				base.clone(),
				elements,
				&env.places(),
				tasks,
			)
			.await?;
			let built = started.elapsed();
			let started = Instant::now();
			with_hnsw!(&resident.graph, h => staged.write(h, &tx).await)?;
			debug!(
				target: "surrealdb::idx::hnsw",
				tasks = staged.tasks(),
				allocate_ms = allocated.as_millis() as u64,
				build_ms = built.as_millis() as u64,
				write_ms = started.elapsed().as_millis() as u64,
				"staged HNSW batch"
			);
			Some(staged)
		};
		tx.on_rollback(Box::new(RollBackBatch {
			resident: Arc::clone(&self.hnsw),
			invalidations: Arc::clone(&self.invalidations),
			base,
			elements: committed.view(None),
			new_elements,
			vec_docs: self.vec_docs.clone(),
			doc_sets: doc_sets.iter().map(|(element, _)| *element).collect(),
		}))
		.await;
		tx.on_commit(Box::new(PublishBatch {
			resident: Arc::clone(&self.hnsw),
			staged,
			elements: committed,
			vectors,
			vec_docs: self.vec_docs.clone(),
			doc_sets,
		}))
		.await;
		Ok(())
	}

	/// Brings the resident graph level with `env`'s view of the store.
	///
	/// A graph the index has been invalidated behind since it last reloaded in
	/// full reloads every layer; any other reloads the layers whose version
	/// moved.
	async fn reconcile(&self, resident: &mut ResidentGraph, env: &dyn IndexEnv) -> Result<()> {
		let invalidations = self.invalidations.load(Ordering::Acquire);
		if resident.reconciled_at != invalidations {
			resident.graph.reload(env).await?;
			resident.reconciled_at = invalidations;
			return Ok(());
		}
		resident.graph.check_state(env).await
	}

	/// Drains and applies one batch of pending vector updates to the HNSW graph.
	///
	/// This convenience method is used by local HNSW tests. Datastore
	/// compaction owns the split read/write transactions around the same
	/// prepare/apply methods.
	#[cfg(test)]
	pub(crate) async fn index_pendings(&self, env: &dyn IndexEnv) -> Result<usize> {
		let plan = Self::prepare_compaction(env, &self.ikb, self.format_version).await?;
		let count = plan.len();
		// The apply's own error, not its wrapper, so a caller can downcast it.
		if self.apply_compaction(env, plan).await.map_err(|e| e.error)?.applied() {
			Ok(count)
		} else {
			Ok(0)
		}
	}

	/// Applies a coalesced pending operation to the HNSW graph in place.
	///
	/// The operation removes the graph baseline for resolved documents, then
	/// adds the desired vectors as [`Self::add_pending_vectors`] does.
	async fn apply_pending_operation(
		&self,
		ctx: &HnswContext<'_>,
		docs: &HnswDocs,
		hnsw: &mut HnswFlavor,
		mut pending: PendingOperation,
	) -> Result<()> {
		if let VectorId::DocId(doc_id) = &pending.id {
			// Remove the graph baseline under the doc-ID those vectors live under.
			for vector in std::mem::take(&mut pending.old_vectors) {
				let vector = Vector::from(vector);
				self.vec_docs.remove(ctx, &vector, *doc_id, hnsw).await?;
			}
		}
		self.add_pending_vectors(ctx, docs, &mut InPlace(hnsw), pending).await
	}

	/// Everything a coalesced pending operation does apart from removing its
	/// graph baseline: resolving the document ID its desired vectors belong
	/// under — for new records only when vectors remain to index — and adding
	/// those vectors through `sink`.
	async fn add_pending_vectors<K: ElementSink>(
		&self,
		ctx: &HnswContext<'_>,
		docs: &HnswDocs,
		sink: &mut K,
		pending: PendingOperation,
	) -> Result<()> {
		match pending.id {
			VectorId::DocId(doc_id) => {
				if pending.new_vectors.is_empty() {
					if self.format_version < crate::catalog::DOC_IDS_FORMAT_VERSION {
						let legacy = LegacyVectorDocIds::Hnsw(self.ikb.clone());
						if let Some(record) = &pending.record {
							legacy.remove(&ctx.env.tx(), record).await?;
						} else if let VectorId::DocId(doc_id) = pending.id {
							legacy.remove_doc_id(&ctx.env.tx(), doc_id).await?;
						}
					}
					docs.remove(doc_id, self.table_id, &self.vector_cache).await;
				} else {
					// A delete removes the record's shared doc-ID mapping and a
					// re-create before compaction folds its vectors in here, so
					// resolve the *current* doc-ID from the record key rather than
					// reusing the captured (possibly deleted) one. For a plain
					// update the mapping is unchanged and this returns the same id.
					let insert_doc_id = match &pending.record {
						Some(record) => docs.resolve(ctx.env, record).await?,
						None => doc_id,
					};
					for vector in pending.new_vectors {
						let vector = Vector::from(vector);
						self.vec_docs.insert(ctx, vector, insert_doc_id, sink).await?;
					}
				}
			}
			VectorId::RecordKey(id) => {
				if !pending.new_vectors.is_empty() {
					let doc_id = docs.resolve(ctx.env, &id).await?;
					for vector in pending.new_vectors {
						let vector = Vector::from(vector);
						self.vec_docs.insert(ctx, vector, doc_id, sink).await?;
					}
				}
			}
		}
		Ok(())
	}

	/// Brings the in-memory graph level with `env`'s view of the store.
	///
	/// A graph search reconciles on its own (see [`Self::reconciled`]); this is
	/// for callers that want the graph current without searching it.
	pub async fn check_state(&self, env: &dyn IndexEnv) -> Result<()> {
		self.reconciled(env).await.map(drop)
	}

	/// The resident graph as of `env`'s snapshot, under a read guard the caller
	/// holds for as long as it reads the graph.
	///
	/// While the guard is held no batch can publish into the graph, no in-place
	/// apply can change it and no other reader can reload it, so a search that
	/// walks the graph under it reads the topology of the same snapshot its
	/// pendings and doc-ID lookups come from. A graph from another snapshot
	/// would pair the search's pendings with nodes for the same records, whose
	/// doc-IDs the search cannot resolve, or point it at an entry point whose
	/// vector its snapshot does not hold.
	///
	/// Validation runs under the shared read lock, so concurrent searches of an
	/// unchanged graph do not serialise. A graph that needs reloading takes the
	/// write lock, reconciles — a no-op if another task already did — and
	/// downgrades without letting a writer in between.
	async fn reconciled(&self, env: &dyn IndexEnv) -> Result<RwLockReadGuard<'_, ResidentGraph>> {
		let guard = self.hnsw.read().await;
		if guard.reconciled_at == self.invalidations.load(Ordering::Acquire)
			&& !guard.needs_state_reload(env).await?
		{
			return Ok(guard);
		}
		drop(guard);
		let mut guard = self.hnsw.write().await;
		self.reconcile(&mut guard, env).await?;
		Ok(guard.downgrade())
	}

	/// Performs a k-nearest neighbor search, combining pending and committed results.
	///
	/// The pending updates scanned are those [`Self::read_lookup_pending_ranges`]
	/// leaves: on an index that guards them, none while the pending-state
	/// summary (`!hY`) reads `Empty`, and otherwise `!hr` and the `!hw` shards
	/// whose guards (`!hy`) are armed; on one that does not, all of `!hr`.
	///
	/// `allow_list` restricts candidate admission to the given doc-IDs (over
	/// the table's shared doc-ID space) without fetching records; graph
	/// traversal still expands through non-members so the search does not
	/// disconnect. `None` leaves admission unrestricted.
	pub async fn knn_search(
		&self,
		env: &dyn IndexEnv,
		pt: &[Number],
		k: usize,
		ef: usize,
		cond_filter: Option<KnnCondFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let compaction_generation =
			read_compaction_generation(&env.tx(), &self.ikb.new_hg_key()).await?;
		// Build a filter if required
		let mut filter = cond_filter.map(|f| {
			HnswTruthyDocumentFilter::new(
				self.ikb.clone(),
				self.format_version,
				self.table_id,
				self.vector_cache.clone(),
				f.cond,
				compaction_generation,
				f.select_gate,
				f.metrics,
				f.version,
			)
		});
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, pt)?.into();
		vector.check_dimension(self.dim)?;
		let search = HnswSearch::new(vector, k, ef);
		// Doc sets are read as of this search's snapshot.
		let vec_docs = self.vec_docs.at_snapshot(&env.tx(), compaction_generation);
		// Get a new HNSW context
		let ctx = HnswContext::new(env, self.ikb.clone(), &vec_docs);
		// Collect the result
		let mut builder = KnnResultBuilder::new(k);

		// Search in the pendings if any
		let pending_docs =
			self.search_pendings(&ctx, &search, &mut filter, allow_list, &mut builder).await?;
		// Search in the graph
		self.search_graph(&ctx, &search, pending_docs, &mut filter, allow_list, &mut builder)
			.await?;

		let cache = filter.map(|filter| filter.release());
		self.resolve_results(&ctx, builder, cache.as_ref(), compaction_generation).await
	}

	/// Replaces the doc-IDs of a finished result with their record IDs,
	/// nearest first, taking the records a truthy filter already fetched from
	/// its `cache`. A doc-ID that no longer resolves takes no result slot.
	async fn resolve_results(
		&self,
		ctx: &HnswContext<'_>,
		builder: KnnResultBuilder,
		cache: Option<&FilterCache>,
		compaction_generation: Option<u64>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let result = builder.collect();
		let mut res_by_pos = vec![None; result.len()];
		let mut doc_misses = Vec::new();
		for (pos, (dist, id)) in result.into_iter().enumerate() {
			let dist: f64 = dist.into();
			// Do we have it from the cache?
			if let Some(cache) = cache
				&& let Some(Some((rid, record))) = cache.get(&id)
			{
				res_by_pos[pos] = Some((Arc::clone(rid), dist, Some(Arc::clone(record))));
				continue;
			}
			// Otherwise we get it from the state
			match id {
				VectorId::DocId(doc_id) => {
					doc_misses.push((pos, doc_id, dist));
				}
				VectorId::RecordKey(key) => {
					let rid = RecordId::new(self.ikb.table().clone(), key.as_ref().clone());
					res_by_pos[pos] = Some((Arc::new(rid), dist, None));
				}
			}
		}
		if !doc_misses.is_empty() {
			let doc_ids: Vec<_> = doc_misses.iter().map(|(_, doc_id, _)| *doc_id).collect();
			let rids = HnswDocs::get_things_batch(
				&ctx.ikb,
				self.format_version,
				self.table_id,
				&self.vector_cache,
				&ctx.tx,
				&doc_ids,
				compaction_generation,
			)
			.await?;
			for ((pos, _, dist), rid) in doc_misses.into_iter().zip(rids) {
				if let Some(rid) = rid {
					res_by_pos[pos] = Some((rid, dist, None));
				}
			}
		}
		let mut res = VecDeque::with_capacity(res_by_pos.len());
		res.extend(res_by_pos.into_iter().flatten());
		Ok(res)
	}

	/// The shape of the graph as this transaction's snapshot holds it, brought
	/// level with the snapshot as a search would be.
	pub async fn graph_shape(&self, env: &dyn IndexEnv) -> Result<crate::trees::AnnGraphShape> {
		Ok(self.reconciled(env).await?.shape())
	}

	/// Exact k-nearest neighbours among the `allow_list` members, scored from
	/// the vectors the index stores rather than from the records.
	///
	/// Every member is scored: pending (not yet compacted) vectors by the same
	/// pending scan the graph search runs, and committed vectors through the
	/// doc-ID → elements mapping, with every doc-ID the pending queue names
	/// suppressed from the committed side. No graph is walked and no record is
	/// fetched; only the final `k` doc-IDs are resolved to record IDs. A
	/// document indexing several vectors takes one slot, at its nearest vector.
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
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, pt)?.into();
		vector.check_dimension(self.dim)?;
		// No member can be offered: answered before any read, once the query
		// vector has been checked.
		if allow_list.is_empty() || k == 0 {
			return Ok(Some(VecDeque::new()));
		}
		let tx = env.tx();
		let compaction_generation = read_compaction_generation(&tx, &self.ikb.new_hg_key()).await?;
		let search = HnswSearch::new(vector, k, k);
		let vec_docs = self.vec_docs.at_snapshot(&tx, compaction_generation);
		let ctx = HnswContext::new(env, self.ikb.clone(), &vec_docs);
		let mut builder = KnnResultBuilder::new(k);
		let pending_docs =
			self.search_pendings(&ctx, &search, &mut None, Some(allow_list), &mut builder).await?;
		// The members the pendings did not already score: the allow-list itself
		// when nothing is pending, else the set difference, taken over roaring
		// containers rather than by a lookup per member.
		let unpending = pending_docs.as_ref().filter(|p| !p.is_empty()).map(|p| allow_list - p);
		let members = unpending.as_ref().unwrap_or(allow_list);
		if !members.is_empty() {
			// Element vectors come through the resident graph, whose vector
			// cache is scoped to the element ids it holds, only when that graph
			// is already level with this snapshot. Otherwise they are read from
			// the snapshot directly: a search over a few members does not load
			// every layer of a graph it never walks. The graph is held for one
			// batch of vector reads at a time, never across the doc → elements
			// reads or the scoring, so a compaction waiting for it waits for a
			// batch at most. With every member pending, no vector is read and
			// the graph is not taken.
			let (this, tx, pt) = (self, &tx, &search.pt);
			doc_elements::score_members(
				env,
				members.iter(),
				move |docs: Vec<DocId>| async move {
					let sets = this.vec_docs.elements_of(tx, &docs).await?;
					Ok((docs, sets))
				},
				move |elements: Vec<ElementId>| async move {
					let vectors = {
						let resident = this.hnsw.read().await;
						if resident.reconciled_at == this.invalidations.load(Ordering::Acquire)
							&& !resident.needs_state_reload(env).await?
						{
							resident.get_vectors(tx, &elements).await?
						} else {
							drop(resident);
							this.read_vectors(tx, &elements).await?
						}
					};
					let distances = vectors
						.into_iter()
						.map(|v| v.map(|v| this.distance.calculate_shared(pt, &v)))
						.collect();
					Ok((elements, distances))
				},
				&mut builder,
			)
			.await?;
		}
		self.resolve_results(&ctx, builder, None, compaction_generation).await.map(Some)
	}

	/// The vectors of several elements as `tx`'s snapshot stores them, in
	/// input order, read in one multi-get past the vector cache. `None` for an
	/// element the snapshot no longer holds.
	async fn read_vectors(
		&self,
		tx: &Transaction,
		e_ids: &[ElementId],
	) -> Result<Vec<Option<SharedVector>>> {
		let keys = e_ids.iter().map(|e_id| self.ikb.new_he_key(*e_id)).collect();
		let fetched: Vec<Option<SerializedVector>> = tx.get_many_key(keys, None).await?;
		// A short answer would leave the trailing elements unscored, silently.
		ensure!(
			fetched.len() == e_ids.len(),
			"{} vectors read for {} elements",
			fetched.len(),
			e_ids.len()
		);
		Ok(fetched.into_iter().map(|v| v.map(|v| Vector::from(v).into())).collect())
	}

	/// Whether the resident graph's lock is free at this instant: nothing
	/// holds it, so a compaction could take it for writing.
	#[cfg(test)]
	pub(super) fn graph_lock_is_free(&self) -> bool {
		self.hnsw.try_write().is_ok()
	}

	/// Whether the resident graph is level with `env`'s view of the store, so
	/// a search would not reload it.
	#[cfg(test)]
	pub(super) async fn resident_graph_is_current(&self, env: &dyn IndexEnv) -> Result<bool> {
		let resident = self.hnsw.read().await;
		Ok(resident.reconciled_at == self.invalidations.load(Ordering::Acquire)
			&& !resident.needs_state_reload(env).await?)
	}

	/// Searches for nearest neighbors in the committed HNSW graph.
	///
	/// Acquires a read lock on the graph and performs KNN search, optionally
	/// excluding documents that are present in `pending_docs` and restricting
	/// admission to `allow_list` members (#548).
	pub(super) async fn search_graph(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		pending_docs: Option<RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		let hnsw = self.reconciled(ctx.env).await?;
		// The builder holds one slot per record, and a record indexing several
		// vectors is reached through each of them, so the `k` nearest elements
		// can name fewer than `k` records. The walk hands over every candidate
		// it kept (up to `ef`) and the builder takes the `k` nearest distinct
		// records; an element farther than the builder's last slot is skipped
		// before its documents are read. Where the `ef` candidates still name
		// fewer than `k` records, the walk is retried with a doubled `ef` until
		// the builder holds `k`, the walk keeps fewer candidates than asked, or
		// the retry ceiling is reached. Both chains end the walk on distance only
		// once `ef` candidates are held, so a walk that keeps fewer than `ef`
		// never stopped on distance and never evicted a candidate: it has
		// expanded every reachable element and a wider pass would repeat it. A
		// retry re-offers the earlier candidates, which the builder and the
		// filter's verdict cache absorb without a second slot or a second
		// evaluation.
		let ceiling = search.ef.saturating_mul(OVERQUERY_CEILING_FACTOR);
		let mut ef = search.ef;
		loop {
			let pass = HnswSearch::new(search.pt.clone(), search.k.max(ef), ef);
			// The allow-list routes through the filtered chain even without a
			// truthy filter: only that chain admits candidates document by
			// document, which a bitmap over doc-IDs needs. Both chains expand
			// through the elements they do not admit and stop on distance only
			// once `ef` results are held.
			let neighbours = if filter.is_some() || allow_list.is_some() {
				hnsw.knn_search_with_filter(ctx, &pass, filter, pending_docs.as_ref(), allow_list)
					.await?
			} else {
				hnsw.knn_search(ctx, &pass, pending_docs.as_ref()).await?
			};
			let exhausted = neighbours.len() < ef;
			self.add_graph_results(
				ctx,
				&hnsw,
				neighbours,
				pending_docs.as_ref(),
				allow_list,
				filter,
				builder,
			)
			.await?;
			if builder.len() >= search.k || exhausted || ef >= ceiling {
				return Ok(());
			}
			ef = ef.saturating_mul(2).min(ceiling);
		}
	}

	/// Coalesces and scores one bounded chunk of the append-keyed legacy queue.
	///
	/// Entries later in this chunk win locally. A scan of the append-keyed suffix
	/// and record-keyed range then removes candidates superseded outside the
	/// chunk. The lookup stores only hashes and positions for this chunk, with an
	/// exact identity comparison on every match, so user-sized record keys are
	/// never copied into an O(backlog) liveness map.
	#[expect(clippy::too_many_arguments)]
	async fn score_legacy_pending_chunk(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
		scan: &mut PendingScan<'_>,
		chunk_end: &[u8],
		checkpoints: &mut usize,
	) -> Result<()> {
		debug_assert!(!scan.batch.is_empty());

		// Keep only the last occurrence of each identity within this chunk. The
		// lookup owns fixed-size positions, not another copy of the identities.
		let mut live = vec![false; scan.batch.ids.len()];
		let mut positions: HashMap<u64, Vec<usize>> = HashMap::default();
		for position in (0..scan.batch.ids.len()).rev() {
			let id = &scan.batch.ids[position];
			let candidates = positions.entry(legacy_identity_hash(id)).or_default();
			if candidates.iter().any(|&candidate| scan.batch.ids[candidate] == *id) {
				continue;
			}
			live[position] = true;
			candidates.push(position);
		}

		// Any matching append after this chunk supersedes its candidate. The
		// suffix begins strictly after the chunk's final encoded key.
		{
			let rng = self.ikb.new_hp_range()?.resume_after(chunk_end, Direction::Forward);
			let mut cursor = ctx.tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
			loop {
				let read = cursor.next_batch(scan.rows()).await?;
				scan.observe_page(&read);
				if read.is_empty() {
					break;
				}
				for (_, value) in &read {
					*checkpoints += 1;
					if ctx.env.is_done(Some(*checkpoints)).await? {
						bail!(EngineError::QueryCancelled)
					}
					let pending = VectorPendingUpdate::kv_decode_value(value, ())?;
					suppress_legacy_candidate(&pending.id, &scan.batch.ids, &positions, &mut live);
				}
			}
		}

		// Every record-keyed entry is later in logical scan order than every
		// append-keyed entry. Its captured identity and its record identity both
		// suppress a matching legacy candidate, whether it deletes or replaces.
		{
			let rng = self.ikb.new_hr_range()?;
			let mut cursor = ctx.tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
			loop {
				let read = cursor.next_batch(scan.rows()).await?;
				scan.observe_page(&read);
				if read.is_empty() {
					break;
				}
				for (key, value) in &read {
					*checkpoints += 1;
					if ctx.env.is_done(Some(*checkpoints)).await? {
						bail!(EngineError::QueryCancelled)
					}
					for mut pending in
						HnswRecordPendingUpdate::kv_decode_value(value, ())?.into_records()
					{
						let id = pending_record_id(key, PendingLayout::Legacy, &mut pending)?;
						let pending = Self::record_pending_to_operation(id, pending);
						suppress_legacy_candidate(
							&pending.id,
							&scan.batch.ids,
							&positions,
							&mut live,
						);
						if let Some(record) = pending.record {
							suppress_legacy_candidate(
								&VectorId::RecordKey(record),
								&scan.batch.ids,
								&positions,
								&mut live,
							);
						}
					}
				}
			}
		}

		// Keep the chunk allocation and its accounting intact until scoring, but
		// clear vectors belonging to superseded, deleting, or disallowed entries.
		// Empty vectors take no result slot.
		for (position, is_live) in live.into_iter().enumerate() {
			if !is_live || scan.batch.vectors[position].is_empty() {
				scan.batch.vectors[position].clear();
				continue;
			}
			let id = scan.batch.ids[position].clone();
			if !self.allow_list_admits(&ctx.tx, allow_list, &id).await? {
				scan.batch.vectors[position].clear();
			}
		}

		self.score_pending_batch(ctx, search, filter, builder, scan).await
	}

	/// Searches through pending (not-yet-applied) updates for nearest neighbors.
	///
	/// Scores every pending vector that is still live against the query and
	/// returns the suppression bitmap of doc-IDs seen anywhere in the pending
	/// keyspace, which the graph search uses to hide the stale entries those
	/// pendings supersede.
	///
	/// # Residency
	///
	/// The whole pending queue is read, but what the scan holds at any moment
	/// is one cursor page plus the candidates read from it and not yet scored,
	/// bounded by [`PendingScan`]: at most `PENDING_MAX_BATCH_KEYS` buffered
	/// entries, at most `PENDING_MAX_ROWS` entries in a page, and page and batch
	/// together sized for `PENDING_MAX_BYTES` — a target the page sizer
	/// converges to rather than a ceiling, for the reason given on
	/// [`crate::trees::pending`]. Candidates are buffered, scored into the
	/// result builder, and dropped before the scan advances; the page they came
	/// from shares the budget because it stays borrowed while they are scored. A
	/// deep queue therefore costs more batches, not more memory — which matters
	/// because this cost is paid per concurrent search, and the queue is deepest
	/// exactly when bulk writes are outrunning compaction.
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
	/// A pending entry is *live* if no later entry in scan order supersedes or
	/// deletes it, where scan order is the append-keyed `!hp` range, then the
	/// record-keyed `!hr` range, then each `!hw` shard the pending-state guards
	/// leave armed ([`Self::read_lookup_pending_ranges`]). The layouts coalesce
	/// differently:
	///
	/// * `!hr` and `!hw` together hold exactly one pending state per record — writes
	///   read-modify-write the record's key, whose value may hold the states of other records
	///   spelled alike, and a write to `!hw` moves its record's state out of `!hr` — and a state
	///   only ever supersedes its own record's identity. Every non-deleting record-keyed entry is
	///   therefore live the moment it is read, and is scored in the same pass that reads it.
	/// * `!hp` may hold several entries per identity, and any of them can be superseded by an `!hr`
	///   entry read afterwards. It is divided into scoring-sized chunks. Within each chunk the last
	///   entry per identity is retained, then the later `!hp` suffix and `!hr` range are scanned to
	///   discard candidates superseded outside it. This repeats legacy reads in exchange for
	///   keeping liveness bookkeeping bounded by one scoring chunk; nothing writes `!hp`, so the
	///   migration cost disappears as compaction drains it.
	async fn search_pendings(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
	) -> Result<Option<RoaringTreemap>> {
		let env = ctx.env;
		let tx = &ctx.tx;
		let mut all_existing_docs = RoaringTreemap::new();
		// One scan across every range read: only the first page is sized before
		// any entry size is known, the cancellation checks keep their back-off
		// schedule over the whole scan, and the backlog report fires on what the
		// whole scan has read.
		let mut scan = PendingScan::new(
			"hnsw",
			&self.ikb,
			&self.pending_backlog_reported,
			&self.pending_scan_stats,
		);
		// Cancellation checkpoints include the bounded legacy-validation reads,
		// while `scan.entries()` remains the logical queue depth reported to users.
		let mut checkpoints = 0usize;

		// Stream the append-keyed range into bounded chunks. Each completed chunk
		// is checked against the later append suffix and the record-keyed range
		// before it is scored, so no liveness state survives into the next chunk.
		// Only an index stamped before format versions can hold that range.
		let mut legacy_chunk_end = Vec::new();
		if may_hold_legacy_vector_pending(self.format_version) {
			let rng = self.ikb.new_hp_range()?;
			let mut cursor = Some(tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?);
			loop {
				let read = cursor
					.as_mut()
					.expect("the legacy cursor is reopened after every bounded chunk")
					.next_batch(scan.rows())
					.await?;
				scan.observe_page(&read);
				if read.is_empty() {
					break;
				}
				let mut chunk_full = false;
				for (key, value) in &read {
					// Charged before the checkpoint, so an entry the scan has
					// read is accounted — and reported, if it crosses — before an
					// expired deadline can bail out of the scan.
					scan.charge_entry(value.len());
					checkpoints += 1;
					if env.is_done(Some(checkpoints)).await? {
						bail!(EngineError::QueryCancelled)
					}
					let pending = VectorPendingUpdate::kv_decode_value(value, ())?;
					if let VectorId::DocId(doc_id) = &pending.id {
						all_existing_docs.insert(*doc_id);
					}
					chunk_full |= scan.rollover_required(value.len());
					scan.push(pending.id, pending.new_vectors, value.len());
					legacy_chunk_end.clear();
					legacy_chunk_end.extend_from_slice(key);
				}
				// Keep chunks page-aligned. The hard page-row cap bounds the entries
				// admitted after the first rollover crossing.
				if chunk_full {
					// Validation opens its own append-suffix and record-keyed cursors.
					// Release this page and its cursor arena first, so only one cursor
					// arena is resident alongside the scoring batch at any point.
					drop(cursor.take());
					scan.release_page();
					self.score_legacy_pending_chunk(
						ctx,
						search,
						filter,
						allow_list,
						builder,
						&mut scan,
						&legacy_chunk_end,
						&mut checkpoints,
					)
					.await?;
					let rng = self
						.ikb
						.new_hp_range()?
						.resume_after(&legacy_chunk_end, Direction::Forward);
					cursor = Some(tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?);
				}
			}
		}
		if !scan.batch.is_empty() {
			self.score_legacy_pending_chunk(
				ctx,
				search,
				filter,
				allow_list,
				builder,
				&mut scan,
				&legacy_chunk_end,
				&mut checkpoints,
			)
			.await?;
		}

		// Read the record-keyed ranges once each for their own suppression and
		// scoring: `!hr`, which the validation scans above already used to
		// discard legacy candidates, and then every `!hw` shard the guards
		// leave armed.
		let ranges = self.read_lookup_pending_ranges(tx).await?;
		if ranges.legacy {
			let mut records = RecordPendingScan {
				scan: &mut scan,
				all_existing_docs: &mut all_existing_docs,
				checkpoints: &mut checkpoints,
			};
			let rng = self.ikb.new_hr_range()?;
			self.scan_record_pending_range(
				ctx,
				search,
				filter,
				allow_list,
				builder,
				&mut records,
				rng,
				PendingLayout::Legacy,
			)
			.await?;
		}
		for shard in 0..PENDING_GUARD_SHARDS {
			if ranges.shards & (1 << shard) == 0 {
				continue;
			}
			let mut records = RecordPendingScan {
				scan: &mut scan,
				all_existing_docs: &mut all_existing_docs,
				checkpoints: &mut checkpoints,
			};
			let rng = self.ikb.new_hw_shard_range(shard)?;
			self.scan_record_pending_range(
				ctx,
				search,
				filter,
				allow_list,
				builder,
				&mut records,
				rng,
				PendingLayout::Sharded,
			)
			.await?;
		}
		self.score_pending_batch(ctx, search, filter, builder, &mut scan).await?;
		scan.finish();
		let keys_read = scan.entries() as u64;
		if keys_read > 0 {
			tx.observer().on_index_pending_read(&surrealdb_observe::IndexPendingReadEvent {
				safe: surrealdb_observe::IndexPendingReadEventSafe {
					kind: surrealdb_observe::IndexCompactionKind::Hnsw,
					keys: keys_read,
				},
				ctx: surrealdb_observe::IndexPendingReadEventCtx::default(),
			});
		}
		if all_existing_docs.is_empty() {
			return Ok(None);
		}
		Ok(Some(all_existing_docs))
	}

	/// Streams one record-keyed pending range into the scan: `!hr` or one
	/// `!hw` shard, as `layout` says. Every record an entry holds adds its
	/// doc-ID to the suppression bitmap, and its new vectors, if it has any and
	/// the allow-list admits it, to the scoring batch, which is scored whenever
	/// it fills.
	#[expect(clippy::too_many_arguments)]
	async fn scan_record_pending_range(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
		records: &mut RecordPendingScan<'_, '_>,
		rng: crate::key::TypedRange<HnswRecordPendingUpdate>,
		layout: PendingLayout,
	) -> Result<()> {
		let mut cursor = ctx.tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let read = cursor.next_batch(records.scan.rows()).await?;
			records.scan.observe_page(&read);
			if read.is_empty() {
				break;
			}
			for (key, value) in &read {
				// The entry's bytes go to the first of its records the batch
				// keeps; the others it holds add none.
				let mut entry_bytes = key.len() + value.len();
				// Charged before the checkpoint — see `search_pendings`.
				records.scan.charge_entry(entry_bytes);
				*records.checkpoints += 1;
				if ctx.env.is_done(Some(*records.checkpoints)).await? {
					bail!(EngineError::QueryCancelled)
				}
				for mut pending in
					HnswRecordPendingUpdate::kv_decode_value(value, ())?.into_records()
				{
					let id = pending_record_id(key, layout, &mut pending)?;
					let pending = Self::record_pending_to_operation(id, pending);
					if let VectorId::DocId(doc_id) = &pending.id {
						records.all_existing_docs.insert(*doc_id);
					}
					if pending.new_vectors.is_empty() {
						continue;
					}
					// A record-keyed pending may carry a doc-ID captured before a
					// delete → re-create of the record: that id's shared mapping is
					// gone, so a DocId-keyed hit could not be resolved back to a
					// record and would be silently dropped. Score the surviving
					// vectors under the record key — always resolvable — while the
					// captured id (inserted above) still masks the graph's stale
					// entries.
					let record = pending
						.record
						.expect("a record-keyed pending always carries its record key");
					let id = VectorId::RecordKey(record);
					// Drop candidates outside the allow-list before they take
					// batch budget, so non-members are never fetched. The
					// suppression bitmap above is deliberately NOT gated: a
					// pending doc must mask its stale graph entries whether or
					// not it is allowed.
					if !self.allow_list_admits(&ctx.tx, allow_list, &id).await? {
						continue;
					}
					let bytes = std::mem::take(&mut entry_bytes);
					if records.scan.rollover_required(bytes) {
						self.score_pending_batch(ctx, search, filter, builder, records.scan)
							.await?;
					}
					records.scan.push(id, pending.new_vectors, bytes);
				}
			}
		}
		Ok(())
	}

	/// Returns whether a pending candidate is admitted by the allow-list.
	///
	/// Membership is over the table's shared doc-ID space, so a record-keyed
	/// candidate resolves its *current* doc-ID first. A record with no doc-ID
	/// in that space has no entries in any doc-ID-carrying index, so the
	/// index-covered conjuncts cannot hold for it. No allow-list admits
	/// everything.
	async fn allow_list_admits(
		&self,
		tx: &Transaction,
		allow_list: Option<&RoaringTreemap>,
		id: &VectorId,
	) -> Result<bool> {
		let Some(allow) = allow_list else {
			return Ok(true);
		};
		Ok(match id {
			VectorId::DocId(doc_id) => allow.contains(*doc_id),
			VectorId::RecordKey(key) => {
				match HnswDocs::get_doc_id(&self.ikb, self.format_version, tx, key).await? {
					Some(doc_id) => allow.contains(doc_id),
					None => false,
				}
			}
		})
	}

	/// Scores one buffered batch of pending candidates and empties it.
	///
	/// The records behind the batch are warmed in a single multi-get before the
	/// per-candidate truthy checks, so those checks read the transaction cache
	/// instead of fetching one record at a time.
	///
	/// A candidate that takes no top-K slot has its filter-cache entry released
	/// on the way out, so the cache the search carries grows with `k` rather
	/// than with the length of the queue — the filter keeps a full record per
	/// truthy candidate and otherwise drops one only when the result builder
	/// evicts it. Releasing an entry costs at most a re-evaluation, and never
	/// happens to a candidate that holds a slot: the ids reaching this function
	/// are distinct, and every pending doc is suppressed from the graph search
	/// that follows, so no released verdict is asked for again.
	async fn score_pending_batch(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
		scan: &mut PendingScan<'_>,
	) -> Result<()> {
		if scan.batch.is_empty() {
			return Ok(());
		}
		scan.begin_scoring();
		scan.batch.retain_non_empty_vectors();
		if scan.batch.is_empty() {
			return Ok(());
		}
		if let Some(filter) = filter.as_mut() {
			filter.prefetch_records(ctx, &scan.batch.ids).await?;
		}
		let batch = &mut scan.batch;
		for (id, vectors) in batch.ids.drain(..).zip(batch.vectors.drain(..)) {
			let truthy = match filter.as_mut() {
				Some(filter) => filter.check_vector_id_truthy(ctx, id.clone()).await?,
				None => true,
			};
			if truthy {
				// A document indexing several vectors takes one slot, at its
				// nearest, as the exact tier offers a compacted one.
				let nearest = vectors
					.into_iter()
					.map(|vector| {
						self.distance.calculate_against(&search.pt, &Vector::from(vector))
					})
					.min_by(f64::total_cmp);
				if let Some(d) = nearest
					&& builder.check_add(d)
					&& let Some(evicted_id) = builder.add_vector_id_result(d, id.clone())
					&& let Some(filter) = filter.as_mut()
				{
					filter.expire(&evicted_id);
				}
			}
			if let Some(filter) = filter.as_mut()
				&& !builder.contains(&id)
			{
				filter.expire(&id);
			}
		}
		Ok(())
	}

	/// Converts graph search results (element IDs) into document-level results and adds them to
	/// the KNN result builder.
	///
	/// `pending_docs` suppresses compacted graph hits for documents with newer record-keyed pending
	/// updates, so the exact pending scan remains the source of truth for those records.
	///
	/// `allow_list` (#548) and the truthy `filter` gate each doc individually:
	/// element admission only requires *one* qualifying doc, so a multi-doc
	/// element (identical vectors) may carry sibling docs that are outside the
	/// bitmap, hidden by the SELECT permission, or failing the residual
	/// condition — none of which may surface or consume top-K slots here.
	/// Sibling verification is cache-first: the doc that admitted the element
	/// is already verdict-cached, so single-doc elements (the common case)
	/// re-check for free.
	#[expect(clippy::too_many_arguments)]
	async fn add_graph_results(
		&self,
		ctx: &HnswContext<'_>,
		hnsw: &HnswFlavor,
		neighbors: Vec<(f64, ElementId)>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		// Neighbours arrive nearest first, and `check_add` only tightens as results are added.
		// They are resolved in chunks of as many as the builder has free slots (at least one):
		// each chunk's vectors and doc sets are read in one batch each, and once the builder is
		// full a chunk is a single neighbour that may still displace a farther result.
		let mut neighbors = neighbors.into_iter();
		let mut chunk: Vec<(f64, ElementId)> = Vec::new();
		loop {
			let want = builder.remaining().max(1);
			chunk.clear();
			for (e_dist, e_id) in neighbors.by_ref() {
				if builder.check_add(e_dist) {
					chunk.push((e_dist, e_id));
					if chunk.len() == want {
						break;
					}
				}
			}
			if chunk.is_empty() {
				return Ok(());
			}
			self.add_graph_chunk(ctx, hnsw, &chunk, pending_docs, allow_list, filter, builder)
				.await?;
		}
	}

	/// Resolves one chunk of [`Self::add_graph_results`] into `builder`.
	#[expect(clippy::too_many_arguments)]
	async fn add_graph_chunk(
		&self,
		ctx: &HnswContext<'_>,
		hnsw: &HnswFlavor,
		neighbors: &[(f64, ElementId)],
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		let ids: Vec<ElementId> = neighbors.iter().map(|(_, e_id)| *e_id).collect();
		// An index that resolves documents by element id needs no vectors; any other keys
		// each lookup by the element's vector.
		let vectors = if ctx.vec_docs.reads_docs_by_element() {
			Vec::new()
		} else {
			hnsw.get_vectors(&ctx.tx, &ids).await?
		};
		let elements: Vec<(ElementId, Option<&Vector>)> = ids
			.iter()
			.enumerate()
			.map(|(i, e_id)| (*e_id, vectors.get(i).and_then(|v| v.as_deref())))
			.collect();
		let doc_sets = ctx.vec_docs.get_docs_by_elements(&ctx.tx, &elements).await?;
		for (&(e_dist, _), docs) in neighbors.iter().zip(doc_sets) {
			if !builder.check_add(e_dist) {
				continue;
			}
			if let Some(docs) = docs {
				if pending_docs.is_some() || allow_list.is_some() || filter.is_some() {
					for doc_id in docs.iter() {
						if let Some(pending_docs) = pending_docs
							&& pending_docs.contains(doc_id)
						{
							continue;
						}
						if let Some(allow) = allow_list
							&& !allow.contains(doc_id)
						{
							continue;
						}
						let id = VectorId::DocId(doc_id);
						if let Some(filter) = filter.as_mut()
							&& !filter.check_vector_id_truthy(ctx, id.clone()).await?
						{
							continue;
						}
						if let Some(evicted_id) = builder.add_vector_id_result(e_dist, id)
							&& let Some(filter) = filter.as_mut()
						{
							filter.expire(&evicted_id);
						}
					}
				} else {
					builder.add_graph_result(e_dist, &docs);
				}
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		self.hnsw.read().await.check_hnsw_properties(expected_count).await
	}

	/// See [`VecDocs::assert_element_docs_match`].
	#[cfg(test)]
	pub(super) async fn assert_element_docs_match(&self, tx: &Transaction) -> Result<usize> {
		self.vec_docs.assert_element_docs_match(tx).await
	}

	/// Each resident layer's node count and mean out-degree, layer 0 first.
	#[cfg(test)]
	pub(super) async fn degree_profile(&self) -> Vec<(usize, f64)> {
		let resident = self.hnsw.read().await;
		with_hnsw!(&resident.graph, h => h.degree_profile())
	}

	/// Adds an edge from `from` to `to` in the resident graph's layer 0 only.
	#[cfg(test)]
	pub(super) async fn plant_layer0_edge(&self, from: ElementId, to: ElementId) {
		let mut resident = self.hnsw.write().await;
		with_hnsw!(&mut resident.graph, h => h.plant_layer0_edge(from, to))
	}

	/// The resident graph's entry point and every layer's adjacency, layer 0
	/// first: two indexes hold the same graph exactly when these are equal.
	#[cfg(test)]
	pub(super) async fn adjacency(&self) -> super::Adjacency {
		let resident = self.hnsw.read().await;
		with_hnsw!(&resident.graph, h => h.adjacency())
	}
}
