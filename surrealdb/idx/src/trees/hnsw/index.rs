use std::borrow::Cow;
use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ahash::HashMap;
use anyhow::{Result, bail};
use common::EngineError;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::{Direction, Val};
use tokio::sync::{RwLock, RwLockReadGuard};
use tracing::debug;
use web_time::Instant;

use crate::catalog::{Distance, HnswParams, TableId, VectorType};
use crate::config::BuildParallelism;
use crate::docids::{LegacyVectorDocIds, TableDocIds};
use crate::env::IndexEnv;
use crate::key::schema::{HnswRecordPendingKey, HnswRecordPendingPrefix};
use crate::key::{KVKeyDecode, KVSubspace, KVValue, Key, Resumable};
use crate::trees::hnsw::batch::{
	self, GraphBase, InvalidateResident, PublishBatch, RollBackBatch, Staged, Staging,
};
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::docs::{ElementSink, HnswDocs, InPlace, VecDocs};
use crate::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::trees::hnsw::flavor::{HnswFlavor, with_hnsw};
use crate::trees::hnsw::{
	ElementId, HnswRecordPendingUpdate, HnswSearch, VectorId, VectorPendingUpdate,
};
use crate::trees::knn::KnnResultBuilder;
use crate::trees::pending::{
	PendingBacklogReport, PendingScan, PendingScanStats, take_compacted_record_id,
	take_pending_record_id,
};
use crate::trees::vector::{
	DistanceExt as _, SerializedVector, SharedVector, Vector, serialized_vector_from_value,
};
use crate::trees::{KnnCondFilter, KnnIteratorResult};
use crate::val::{Number, RecordId, RecordIdKey, Value};
use crate::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};

/// The record an HNSW pending entry belongs to.
///
/// The rule is [`take_pending_record_id`]; this supplies the key decoder it
/// falls back to.
fn pending_record_id(key: &[u8], pending: &mut HnswRecordPendingUpdate) -> Result<RecordIdKey> {
	take_pending_record_id(&mut pending.id, || {
		Ok(HnswRecordPendingKey::decode_key(key)?.id.into_owned())
	})
}

/// [`pending_record_id`] for compaction, which maps the record to a doc-ID; the
/// rule is [`take_compacted_record_id`].
async fn compacted_record_id(
	tx: &Transaction,
	ikb: &IndexKeyBase,
	key: &[u8],
	pending: &mut HnswRecordPendingUpdate,
) -> Result<RecordIdKey> {
	take_compacted_record_id(
		tx,
		&mut pending.id,
		|| {
			let prefix = HnswRecordPendingPrefix {
				ns: ikb.ns(),
				db: ikb.db(),
				tb: Cow::Borrowed(ikb.table()),
				ix: ikb.index(),
			}
			.encode_bound()?;
			Ok(key.strip_prefix(&*prefix).map(|spelled| {
				(TableDocIds::new(ikb.ns(), ikb.db(), ikb.table().clone()), spelled.to_vec())
			}))
		},
		|| Ok(HnswRecordPendingKey::decode_key(key)?.id.into_owned()),
	)
	.await
}

/// Maximum number of pending key/value pairs captured by one compaction plan.
const HNSW_COMPACTION_MAX_PENDING_KEYS: usize = 1024;
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
	has_more: bool,
}

impl HnswCompactionPlan {
	/// Returns whether the plan contains pending keys to apply.
	pub fn has_work(&self) -> bool {
		!self.captured_keys.is_empty()
	}

	/// Returns whether the read phase stopped at the configured batch cap.
	pub fn has_more(&self) -> bool {
		self.has_more
	}

	/// Returns the number of exact pending keys captured by the plan.
	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.captured_keys.len()
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

	/// Captures one pending key/value and folds its operation into the plan.
	///
	/// Returns `false` when adding the pending entry would exceed a batch cap.
	fn add(&mut self, key: Vec<u8>, value: Val, pending: PendingOperation) -> bool {
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
		self.add_pending(pending);
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
	fn into_plan(self) -> HnswCompactionPlan {
		HnswCompactionPlan {
			generation: self.generation,
			captured_keys: self.captured_keys,
			pending: self.pending,
			has_more: self.has_more,
		}
	}
}

/// What applying a compaction plan did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HnswCompactionOutcome {
	/// Nothing was applied: the plan was empty, another compactor advanced the
	/// generation first, or a captured pending changed before the write.
	Skipped,
	/// The plan was applied into the transaction only, and the resident graph
	/// takes it when that transaction commits. A transaction that does not
	/// commit leaves the resident graph as it stood, so nothing needs
	/// discarding.
	Staged,
	/// The plan was applied to the resident graph in place, as it was written
	/// into the transaction. A transaction that does not commit invalidates the
	/// resident graph for every holder of the index; a caller still discards
	/// the process-local index, so the next access starts from a new one.
	InPlace,
}

impl HnswCompactionOutcome {
	/// Whether the plan was applied, so the transaction has a batch to commit.
	pub fn applied(self) -> bool {
		!matches!(self, Self::Skipped)
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
	/// Converts old/new document values into serialized vectors and stores a
	/// single record-keyed pending value. Repeated writes to the same record
	/// preserve the original graph baseline and replace only the desired final
	/// vectors, so compaction can apply the record's final state directly.
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
		let key = self.ikb.new_hr_key(id);
		let mut pending = if let Some(mut pending) = tx.get_key(&key, None).await? {
			pending.new_vectors = new_vectors;
			pending
		} else {
			HnswRecordPendingUpdate {
				doc_id: HnswDocs::get_doc_id(&self.ikb, self.format_version, &tx, id).await?,
				old_vectors,
				new_vectors,
				id: None,
			}
		};
		// Every write stamps the exact id, whichever branch built the entry, so
		// compaction and lookup never have to recover it from a key that cannot
		// spell it. Stamping here rather than per branch keeps that true of a
		// branch added later.
		pending.id = Some(id.clone());
		tx.set_key(&key, &pending).await?;
		Ok(())
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

	/// Creates an [`HnswContext`] from the current index state and an environment.
	pub(super) fn new_hnsw_context<'a>(&'a self, env: &'a dyn IndexEnv) -> HnswContext<'a> {
		HnswContext::new(env, self.ikb.clone(), &self.vec_docs)
	}

	/// Builds a bounded compaction plan for HNSW pending updates.
	///
	/// The read phase scans append-keyed `!hp` entries first, then record-keyed
	/// `!hr` entries. It records exact key/value pairs for conditional deletion
	/// and coalesces pending work by document identity.
	pub(crate) async fn prepare_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
	) -> Result<HnswCompactionPlan> {
		let tx = env.tx();
		let generation = read_compaction_generation(&tx, &ikb.new_hg_key()).await?;
		let mut builder = PendingPlanBuilder::new(generation);
		let mut count = 0;
		Self::collect_append_pending_for_plan(env, &tx, ikb, &mut builder, &mut count).await?;
		if !builder.has_more {
			Self::collect_record_pending_for_plan(env, &tx, ikb, &mut builder, &mut count).await?;
		}
		Ok(builder.into_plan())
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
				if !builder.add(key.to_vec(), value.to_vec(), pending) {
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

	/// Adds record-keyed pending entries to a compaction plan.
	///
	/// The record identity comes from the stored pending value, which keeps it
	/// exactly. The key spells its id under a codec that omits a number's kind,
	/// so it answers only for an entry written before the value carried one.
	async fn collect_record_pending_for_plan(
		env: &dyn IndexEnv,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		builder: &mut PendingPlanBuilder,
		count: &mut usize,
	) -> Result<()> {
		let rng = ikb.new_hr_range()?;
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
				let mut pending = HnswRecordPendingUpdate::kv_decode_value(value, ())?;
				let id = compacted_record_id(tx, ikb, key, &mut pending).await?;
				let pending = Self::record_pending_to_operation(id, pending);
				// Copied per entry rather than per batch, for the reason given in
				// `collect_append_pending_for_plan`.
				if !builder.add(key.to_vec(), value.to_vec(), pending) {
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
	/// * Any other plan is applied **in place**, one operation at a time, under the graph write
	///   lock.
	///
	/// A transaction may carry more than one apply for an index. Each then
	/// reads the graph back through that transaction, and every batch's
	/// rollback action accounts for what that can bring into the resident
	/// graph; see [`HnswCompactionOutcome`].
	pub(crate) async fn apply_compaction(
		&self,
		env: &dyn IndexEnv,
		plan: HnswCompactionPlan,
	) -> Result<HnswCompactionOutcome> {
		let HnswCompactionPlan {
			generation,
			captured_keys,
			pending,
			has_more: _,
		} = plan;
		let tx = env.tx();
		if captured_keys.is_empty() {
			return Ok(HnswCompactionOutcome::Skipped);
		}
		if !bump_compaction_generation(&tx, &self.ikb.new_hg_key(), generation).await? {
			return Ok(HnswCompactionOutcome::Skipped);
		}
		for captured in &captured_keys {
			match tx.del_compare(Key::from(&captured.key), Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => {
					return Ok(HnswCompactionOutcome::Skipped);
				}
				Err(e) => return Err(e),
			}
		}
		let parallelism = env.config().hnsw_build_parallelism;
		if parallelism != BuildParallelism::Serial
			&& pending.iter().all(|pending| pending.old_vectors.is_empty())
		{
			self.apply_staged(env, pending, parallelism.tasks()).await?;
			return Ok(HnswCompactionOutcome::Staged);
		}
		self.apply_in_place(env, pending).await?;
		Ok(HnswCompactionOutcome::InPlace)
	}

	/// Applies a plan to the resident graph, one operation at a time, under
	/// the graph write lock.
	async fn apply_in_place(
		&self,
		env: &dyn IndexEnv,
		pending: Vec<PendingOperation>,
	) -> Result<()> {
		let mut hnsw = self.hnsw.write().await;
		self.reconcile(&mut hnsw, env).await?;
		// Registered before the first operation reaches the graph: from here
		// on, a transaction that does not commit leaves the resident graph and
		// the vector cache holding what the store never took.
		env.tx()
			.on_rollback(Box::new(InvalidateResident {
				invalidations: Arc::clone(&self.invalidations),
				elements: hnsw.elements().view(None),
			}))
			.await;
		let ctx = self.new_hnsw_context(env);
		let docs = HnswDocs::with_format_version(self.ikb.clone(), self.format_version);
		for pending in pending {
			self.apply_pending_operation(&ctx, &docs, &mut hnsw.graph, pending).await?;
		}
		Ok(())
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
			let staged =
				batch::build(Arc::clone(&resident), Arc::clone(&tx), base.clone(), elements, tasks)
					.await?;
			let built = started.elapsed();
			let started = Instant::now();
			with_hnsw!(&resident.graph, h => staged.write(h, &tx).await)?;
			debug!(
				target: "surrealdb::idx::hnsw",
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
		let plan = Self::prepare_compaction(env, &self.ikb).await?;
		let count = plan.len();
		if self.apply_compaction(env, plan).await?.applied() {
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
	/// HNSW pending updates remain on the hot write path, so lookup scans them
	/// conservatively instead of relying on a shared pending-state key that can
	/// create write contention under concurrent indexing.
	///
	/// `allow_list` restricts candidate admission to the given doc-IDs (over
	/// the table's shared doc-ID space) without fetching records; graph
	/// traversal still expands through non-members so the search does not
	/// disconnect. `None` leaves admission unrestricted.
	#[expect(clippy::too_many_arguments)]
	pub async fn knn_search(
		&self,
		env: &dyn IndexEnv,
		stk: &mut Stk,
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
		// Get a new HNSW context
		let ctx = self.new_hnsw_context(env);
		// Collect the result
		let mut builder = KnnResultBuilder::new(k);

		// Search in the pendings if any
		let pending_docs =
			self.search_pendings(&ctx, stk, &search, &mut filter, allow_list, &mut builder).await?;
		// Search in the graph
		self.search_graph(&ctx, stk, &search, pending_docs, &mut filter, allow_list, &mut builder)
			.await?;

		// We build the final result: replacing DocId with RecordIds
		let result = builder.collect();

		let cache = if let Some(filter) = filter {
			// If there is a filter, retrieve the record cache
			let cache = filter.release();
			Some(cache)
		} else {
			None
		};
		let mut res_by_pos = vec![None; result.len()];
		let mut doc_misses = Vec::new();
		for (pos, (dist, id)) in result.into_iter().enumerate() {
			let dist: f64 = dist.into();
			// Do we have it from the cache?
			if let Some(cache) = &cache
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

	/// Searches for nearest neighbors in the committed HNSW graph.
	///
	/// Acquires a read lock on the graph and performs KNN search, optionally
	/// excluding documents that are present in `pending_docs` and restricting
	/// admission to `allow_list` members (#548).
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn search_graph(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		search: &HnswSearch,
		pending_docs: Option<RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		let hnsw = self.reconciled(ctx.env).await?;
		// Do the search. The allow-list routes through the filtered chain even
		// without a truthy filter: only that chain admits candidates document
		// by document, which a bitmap over doc-IDs needs. Both chains expand
		// through the elements they do not admit. They differ in when distance
		// ends the walk: the unfiltered chain only once it holds `ef` results,
		// the filtered chain as soon as its furthest admitted result is nearer
		// than the next candidate, so a selective filter costs one bounded
		// walk rather than a filter evaluation of every reachable record, at
		// the price of returning fewer than `k` when matches lie beyond the
		// rejected elements.
		if filter.is_some() || allow_list.is_some() {
			let neighbours = hnsw
				.knn_search_with_filter(ctx, search, stk, filter, pending_docs.as_ref(), allow_list)
				.await?;
			self.add_graph_results(
				ctx,
				stk,
				&hnsw,
				neighbours,
				pending_docs.as_ref(),
				allow_list,
				filter,
				builder,
			)
			.await
		} else {
			let neighbours = hnsw.knn_search(ctx, search, pending_docs.as_ref()).await?;
			self.add_graph_results(
				ctx,
				stk,
				&hnsw,
				neighbours,
				pending_docs.as_ref(),
				None,
				filter,
				builder,
			)
			.await
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
		stk: &mut Stk,
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
					let mut pending = HnswRecordPendingUpdate::kv_decode_value(value, ())?;
					let id = pending_record_id(key, &mut pending)?;
					let pending = Self::record_pending_to_operation(id, pending);
					suppress_legacy_candidate(&pending.id, &scan.batch.ids, &positions, &mut live);
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

		self.score_pending_batch(ctx, stk, search, filter, builder, scan).await
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
	/// deletes it, where scan order is the append-keyed `!hp` range followed by
	/// the record-keyed `!hr` range. The two layouts coalesce differently:
	///
	/// * `!hr` holds exactly one entry per record — writes read-modify-write that single key — and
	///   an entry only ever supersedes its own record's identity. Every non-deleting `!hr` entry is
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
		stk: &mut Stk,
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
		let mut legacy_chunk_end = Vec::new();
		{
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
						stk,
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
				stk,
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

		// Read the record-keyed range once for its own suppression and scoring.
		// The validation scans above already used it to discard legacy candidates.
		{
			let rng = self.ikb.new_hr_range()?;
			let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
			loop {
				let read = cursor.next_batch(scan.rows()).await?;
				scan.observe_page(&read);
				if read.is_empty() {
					break;
				}
				for (key, value) in &read {
					let entry_bytes = key.len() + value.len();
					// Charged before the checkpoint — see the legacy pass above.
					scan.charge_entry(entry_bytes);
					checkpoints += 1;
					if env.is_done(Some(checkpoints)).await? {
						bail!(EngineError::QueryCancelled)
					}
					let mut pending = HnswRecordPendingUpdate::kv_decode_value(value, ())?;
					let id = pending_record_id(key, &mut pending)?;
					let pending = Self::record_pending_to_operation(id, pending);
					if let VectorId::DocId(doc_id) = &pending.id {
						all_existing_docs.insert(*doc_id);
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
					if !self.allow_list_admits(tx, allow_list, &id).await? {
						continue;
					}
					if scan.rollover_required(entry_bytes) {
						self.score_pending_batch(ctx, stk, search, filter, builder, &mut scan)
							.await?;
					}
					scan.push(id, pending.new_vectors, entry_bytes);
				}
			}
		}
		self.score_pending_batch(ctx, stk, search, filter, builder, &mut scan).await?;
		scan.finish();
		if all_existing_docs.is_empty() {
			return Ok(None);
		}
		Ok(Some(all_existing_docs))
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
		stk: &mut Stk,
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
				Some(filter) => filter.check_vector_id_truthy(ctx, stk, id.clone()).await?,
				None => true,
			};
			if truthy {
				for vector in vectors {
					let vector = Vector::from(vector);
					let d = self.distance.calculate(&search.pt, &vector);
					if builder.check_add(d)
						&& let Some(evicted_id) = builder.add_vector_id_result(d, id.clone())
						&& let Some(filter) = filter.as_mut()
					{
						filter.expire(&evicted_id);
					}
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
		stk: &mut Stk,
		hnsw: &HnswFlavor,
		neighbors: Vec<(f64, ElementId)>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		for (e_dist, e_id) in neighbors {
			if !builder.check_add(e_dist) {
				continue;
			}
			let docs = if let Some(docs) = self.vec_docs.get_cached_doc_set(e_id).await {
				Some(docs)
			} else if let Some(v) = hnsw.get_vector(&ctx.tx, &e_id).await? {
				self.vec_docs.get_docs_by_element(&ctx.tx, e_id, &v).await?
			} else {
				None
			};
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
							&& !filter.check_vector_id_truthy(ctx, stk, id.clone()).await?
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

	/// Each resident layer's node count and mean out-degree, layer 0 first.
	#[cfg(test)]
	pub(super) async fn degree_profile(&self) -> Vec<(usize, f64)> {
		let resident = self.hnsw.read().await;
		with_hnsw!(&resident.graph, h => h.degree_profile())
	}

	/// The resident graph's entry point and every layer's adjacency, layer 0
	/// first: two indexes hold the same graph exactly when these are equal.
	#[cfg(test)]
	pub(super) async fn adjacency(&self) -> super::Adjacency {
		let resident = self.hnsw.read().await;
		with_hnsw!(&resident.graph, h => h.adjacency())
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionType;

	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::key::KVKey;
	use crate::key::schema::{DocKeyKey, DocLookupKey};
	use crate::test_env::TestIndexStore;

	/// Compaction takes the record of an entry whose value carries no id from
	/// the table's mappings, not from the id its key decodes to.
	///
	/// The key spells the id under the index format, so `[42, 'intro']` decodes
	/// as `[42dec, 'intro']`, a record that does not exist; resolving that would
	/// mint it a doc-ID of its own.
	#[tokio::test]
	async fn compaction_names_the_mapped_record_of_an_entry_without_an_id() {
		let (ns, db) = (NamespaceId(1), DatabaseId(2));
		let ikb = IndexKeyBase::new(ns, db, "tb".into(), IndexId(3));
		let id =
			RecordIdKey::Array(vec![Value::Number(Number::Int(42)), Value::from("intro")].into());
		let key = ikb.new_hr_key(&id).encode_key().unwrap().to_vec();
		let decoded = HnswRecordPendingKey::decode_key(&key).unwrap().id.into_owned();
		assert!(
			!decoded.addresses_same_record(&id),
			"the premise: the key's id is not the record's"
		);
		let entry = || HnswRecordPendingUpdate {
			doc_id: None,
			old_vectors: Vec::new(),
			new_vectors: Vec::new(),
			id: None,
		};

		let ds = TestIndexStore::new().await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(
			compacted_record_id(&tx, &ikb, &key, &mut entry())
				.await
				.unwrap()
				.addresses_same_record(&decoded),
			"with no mapping at that spelling, the decoded id is all there is"
		);
		let tb = crate::val::TableName::from("tb");
		tx.set_key(&DocLookupKey::new(ns, db, Cow::Borrowed(&tb), Cow::Borrowed(&id)), &7u64)
			.await
			.unwrap();
		tx.set_key(&DocKeyKey::new(ns, db, Cow::Borrowed(&tb), 7), &id).await.unwrap();
		let named = compacted_record_id(&tx, &ikb, &key, &mut entry()).await.unwrap();
		assert!(
			named.addresses_same_record(&id),
			"the mapping names the record exactly: {named:?}"
		);
		tx.cancel().await.unwrap();
	}
}
