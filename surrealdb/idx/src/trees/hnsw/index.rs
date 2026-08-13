use std::collections::VecDeque;
use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, bail};
use common::EngineError;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::{Direction, Val};
use tokio::sync::RwLock;

use crate::catalog::{Distance, HnswParams, TableId, VectorType};
use crate::env::IndexEnv;
use crate::key::schema::HnswRecordPendingKey;
use crate::key::{KVKeyDecode, KVValue, Key};
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::trees::hnsw::flavor::HnswFlavor;
use crate::trees::hnsw::{
	ElementId, HnswRecordPendingUpdate, HnswSearch, VectorId, VectorPendingUpdate,
};
use crate::trees::knn::KnnResultBuilder;
use crate::trees::vector::{
	DistanceExt as _, SerializedVector, SharedVector, Vector, serialized_vector_from_value,
};
use crate::trees::{KnnCondFilter, KnnIteratorResult};
use crate::val::{Number, RecordId, RecordIdKey, Value};
use crate::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};

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

/// High-level HNSW index supporting concurrent reads and writes.
///
/// Writes are handled through a two-phase approach:
/// 1. **Enqueueing**: The [`index`](Self::index) method converts document changes into record-keyed
///    [`HnswRecordPendingUpdate`] entries stored in the key-value store.
/// 2. **Applying**: Compaction prepares a bounded pending snapshot and applies it under a graph
///    write lock after the captured keys are conditionally deleted.
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
	hnsw: RwLock<HnswFlavor>,
	/// Vector-to-document mappings.
	vec_docs: VecDocs,
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
	/// Creates a new HNSW index, loading existing document state from the transaction.
	pub(crate) async fn new(
		vector_cache: VectorCache,
		_tx: &Transaction,
		ikb: IndexKeyBase,
		tb: TableId,
		p: &HnswParams,
	) -> Result<Self> {
		Ok(Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			distance: p.distance.clone(),
			table_id: tb,
			hnsw: RwLock::new(HnswFlavor::new(tb, ikb.clone(), p, vector_cache.clone())?),
			vec_docs: VecDocs::new(ikb.clone(), tb, vector_cache.clone(), p.use_hashed_vector),
			vector_cache,
			ikb,
		})
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
		let pending = if let Some(mut pending) = tx.get_key(&key, None).await? {
			pending.new_vectors = new_vectors;
			pending
		} else {
			HnswRecordPendingUpdate {
				doc_id: HnswDocs::get_doc_id(&self.ikb, &tx, id).await?,
				old_vectors,
				new_vectors,
			}
		};
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
	/// The record identity is decoded from each key and combined with the
	/// stored pending value before being coalesced into the plan.
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
				let hr = HnswRecordPendingKey::decode_key(key)?;
				let pending = HnswRecordPendingUpdate::kv_decode_value(value, ())?;
				let pending = Self::record_pending_to_operation(hr.id.into_owned(), pending);
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
	/// mutated only after those guards succeed.
	pub(crate) async fn apply_compaction(
		&self,
		env: &dyn IndexEnv,
		plan: HnswCompactionPlan,
	) -> Result<bool> {
		let HnswCompactionPlan {
			generation,
			captured_keys,
			pending,
			has_more: _,
		} = plan;
		let tx = env.tx();
		if captured_keys.is_empty() {
			return Ok(false);
		}
		if !bump_compaction_generation(&tx, &self.ikb.new_hg_key(), generation).await? {
			return Ok(false);
		}
		for captured in &captured_keys {
			match tx.del_compare(Key::from(&captured.key), Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => return Ok(false),
				Err(e) => return Err(e),
			}
		}
		let mut hnsw = self.hnsw.write().await;
		hnsw.check_state(env).await?;
		let mut ctx = self.new_hnsw_context(env);
		let docs = HnswDocs::new(self.ikb.clone());
		for pending in pending {
			self.apply_pending_operation(&mut ctx, &docs, &mut hnsw, pending).await?;
		}
		Ok(true)
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
		if self.apply_compaction(env, plan).await? {
			Ok(count)
		} else {
			Ok(0)
		}
	}

	/// Applies a coalesced pending operation to the HNSW graph.
	///
	/// The operation removes the graph baseline for resolved documents and
	/// inserts the desired vectors, resolving a document ID for new records only
	/// when vectors remain to index.
	async fn apply_pending_operation(
		&self,
		ctx: &mut HnswContext<'_>,
		docs: &HnswDocs,
		hnsw: &mut HnswFlavor,
		pending: PendingOperation,
	) -> Result<()> {
		match pending.id {
			VectorId::DocId(doc_id) => {
				// Remove the graph baseline under the doc-ID those vectors live under.
				for vector in pending.old_vectors {
					let vector = Vector::from(vector);
					self.vec_docs.remove(ctx, &vector, doc_id, hnsw).await?;
				}
				if pending.new_vectors.is_empty() {
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
						self.vec_docs.insert(ctx, vector, insert_doc_id, hnsw).await?;
					}
				}
			}
			VectorId::RecordKey(id) => {
				if !pending.new_vectors.is_empty() {
					let doc_id = docs.resolve(ctx.env, &id).await?;
					for vector in pending.new_vectors {
						let vector = Vector::from(vector);
						self.vec_docs.insert(ctx, vector, doc_id, hnsw).await?;
					}
				}
			}
		}
		Ok(())
	}

	/// Ensures the in-memory graph layers are up-to-date with the persisted state.
	///
	/// Concurrent kNN searches all invoke this before reading the graph. To keep
	/// them from serialising on a single write lock, we validate under a shared
	/// read lock first and only escalate to a write lock when an actual reload
	/// is required. The write-lock branch double-checks because another task may
	/// have refreshed the state while we waited.
	pub async fn check_state(&self, env: &dyn IndexEnv) -> Result<()> {
		// Fast path: validate under a read lock. Multiple readers run concurrently,
		// so steady-state (no peer writer bumped the version) no longer serialises.
		{
			let guard = self.hnsw.read().await;
			if !guard.needs_state_reload(env).await? {
				return Ok(());
			}
		}
		// Slow path: a reload is required. Acquire the write lock and re-validate
		// before reloading — a concurrent task may have already refreshed.
		let mut guard = self.hnsw.write().await;
		if guard.needs_state_reload(env).await? {
			guard.check_state(env).await?;
		}
		Ok(())
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
				self.table_id,
				self.vector_cache.clone(),
				f.cond,
				compaction_generation,
				f.select_gate,
				f.metrics,
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
		let hnsw = self.hnsw.read().await;
		// Do the search. The allow-list routes through the filtered chain even
		// without a truthy filter: the unfiltered chain suppresses elements
		// from the traversal frontier itself, which is fine for a handful of
		// pending docs but would disconnect the walk under a selective bitmap.
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

	/// Searches through pending (not-yet-applied) updates for nearest neighbors.
	///
	/// Scans all pending updates to identify active (non-deleted) vectors,
	/// computes distances against the search query, and adds matches to the
	/// result builder. Returns a bitmap of doc IDs seen in pending updates
	/// so the graph search can exclude them to avoid duplicate results.
	async fn search_pendings(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		allow_list: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
	) -> Result<Option<RoaringTreemap>> {
		let mut all_existing_docs = RoaringTreemap::new();
		let mut non_deleted_docs = HashMap::default();
		// First pass, identify deleted doc
		self.collect_pending(ctx.env, &ctx.tx, |pending| {
			if let VectorId::DocId(doc_id) = &pending.id {
				all_existing_docs.insert(*doc_id);
			};
			if pending.new_vectors.is_empty() {
				non_deleted_docs.remove(&pending.id);
			} else {
				// A record-keyed pending may carry a doc-ID captured before a
				// delete → re-create of the record: that id's shared mapping is
				// gone, so a DocId-keyed hit could not be resolved back to a
				// record and would be silently dropped. Emit the surviving
				// vectors under the record key — always resolvable — while the
				// captured id (inserted above) still masks the graph's stale
				// entries. An entry coalesced earlier under the captured id is
				// superseded.
				let id = if let Some(record) = &pending.record {
					non_deleted_docs.remove(&pending.id);
					VectorId::RecordKey(Arc::clone(record))
				} else {
					pending.id
				};
				non_deleted_docs.insert(id, pending.new_vectors);
			}
		})
		.await?;
		if all_existing_docs.is_empty() && non_deleted_docs.is_empty() {
			return Ok(None);
		}
		// #548: drop pending candidates outside the allow-list before the
		// prefetch below, so non-members are never fetched. Note the
		// `all_existing_docs` suppression bitmap is deliberately NOT filtered:
		// a pending doc must mask its stale graph entries whether or not it is
		// allowed.
		if let Some(allow) = allow_list {
			let mut allowed = HashMap::default();
			for (id, vectors) in non_deleted_docs {
				let member = match &id {
					VectorId::DocId(doc_id) => allow.contains(*doc_id),
					VectorId::RecordKey(key) => {
						match HnswDocs::get_doc_id(&self.ikb, &ctx.tx, key).await? {
							Some(doc_id) => allow.contains(doc_id),
							// A record with no doc-ID in the shared space has no
							// entries in any doc-ID-carrying index, so the
							// index-covered conjuncts cannot hold for it.
							None => false,
						}
					}
				};
				if member {
					allowed.insert(id, vectors);
				}
			}
			non_deleted_docs = allowed;
		}
		// Warm the transaction record cache for the pending candidates in one
		// batch, so the per-doc truthy checks below hit the cache instead of
		// fetching each record individually.
		if let Some(filter) = filter.as_mut() {
			let ids: Vec<VectorId> = non_deleted_docs.keys().cloned().collect();
			filter.prefetch_records(ctx, &ids).await?;
		}
		// Second pass, we build the KNN result for non-deleted documents
		for (id, vectors) in non_deleted_docs {
			// If there is a filter, we need to check if the record is truthy
			if let Some(filter) = filter
				&& !filter.check_vector_id_truthy(ctx, stk, id.clone()).await?
			{
				continue;
			}
			for vector in vectors {
				let vector = Vector::from(vector);
				let d = self.distance.calculate(&search.pt, &vector);
				if builder.check_add(d)
					&& let Some(evicted_id) = builder.add_vector_id_result(d, id.clone())
					&& let Some(filter) = filter
				{
					filter.expire(&evicted_id);
				}
			}
		}
		if all_existing_docs.is_empty() {
			return Ok(None);
		}
		Ok(Some(all_existing_docs))
	}

	/// Streams all pending updates and passes graph operations to a collector.
	///
	/// Append-keyed entries are streamed before record-keyed entries. The
	/// collector decides whether and how to coalesce operations.
	async fn collect_pending<F>(
		&self,
		env: &dyn IndexEnv,
		tx: &Transaction,
		mut collector: F,
	) -> Result<()>
	where
		F: FnMut(PendingOperation),
	{
		let rng = self.ikb.new_hp_range()?;
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		let mut count = 0;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (_, v) in &batch {
				if env.is_done(Some(count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				let pending = VectorPendingUpdate::kv_decode_value(v, ())?;
				collector(Self::append_pending_to_operation(pending));
				count += 1;
			}
		}
		drop(cursor);

		let rng = self.ikb.new_hr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for (key, value) in &batch {
				if env.is_done(Some(count)).await? {
					bail!(EngineError::QueryCancelled)
				}
				let hr = HnswRecordPendingKey::decode_key(key)?;
				let pending = HnswRecordPendingUpdate::kv_decode_value(value, ())?;
				collector(Self::record_pending_to_operation(hr.id.into_owned(), pending));
				count += 1;
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
}
