use std::collections::VecDeque;
use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::catalog::{Distance, HnswParams, TableId, VectorType};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Cond;
use crate::idx::planner::ScanDirection;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::idx::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::idx::trees::hnsw::flavor::HnswFlavor;
use crate::idx::trees::hnsw::{
	ElementId, HnswRecordPendingUpdate, HnswSearch, VectorId, VectorPendingUpdate,
};
use crate::idx::trees::knn::KnnResultBuilder;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::idx::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};
use crate::key::index::hr::HnswRecordPending;
use crate::kvs::{KVValue, Key, Transaction, Val};
use crate::val::{Number, RecordId, RecordIdKey, Value};

/// Maximum number of pending key/value pairs captured by one compaction plan.
const HNSW_COMPACTION_MAX_PENDING_KEYS: usize = 1024;
/// Maximum encoded pending key/value bytes captured by one compaction plan.
const HNSW_COMPACTION_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;
/// Exact pending key/value observed by an HNSW compaction read phase.
struct CapturedPendingKey {
	/// Encoded key to delete if the value still matches.
	key: Key,
	/// Encoded value that must still be present during conditional delete.
	value: Val,
}

/// Coalesced record operation that should be applied to the HNSW graph.
#[derive(Clone)]
struct PendingOperation {
	/// Existing document ID or record key for a not-yet-resolved document.
	id: VectorId,
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
pub(crate) struct HnswCompactionPlan {
	/// Compaction generation observed while preparing the plan.
	generation: Option<u64>,
	captured_keys: Vec<CapturedPendingKey>,
	pending: Vec<PendingOperation>,
	has_more: bool,
}

impl HnswCompactionPlan {
	/// Returns whether the plan contains pending keys to apply.
	pub(crate) fn has_work(&self) -> bool {
		!self.captured_keys.is_empty()
	}

	/// Returns whether the read phase stopped at the configured batch cap.
	pub(crate) fn has_more(&self) -> bool {
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
	fn add(&mut self, key: Key, value: Val, pending: PendingOperation) -> bool {
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
pub(crate) struct HnswIndex {
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
	/// The frozen query context.
	pub(super) ctx: &'a FrozenContext,
	/// The current transaction.
	pub(super) tx: Arc<Transaction>,
	/// Key base for generating index-related storage keys.
	pub(super) ikb: IndexKeyBase,
	/// Reference to the vector-document mappings.
	pub(super) vec_docs: &'a VecDocs,
}

impl<'a> HnswContext<'a> {
	/// Creates graph-operation context backed by the frozen transaction context.
	pub(super) fn new(ctx: &'a FrozenContext, ikb: IndexKeyBase, vec_docs: &'a VecDocs) -> Self {
		Self {
			ctx,
			tx: ctx.tx(),
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
			let vector = SerializedVector::try_from_value(self.vector_type, self.dim, value)?;
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
		ctx: &Context,
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
		let tx = ctx.tx();
		let key = self.ikb.new_hr_key(id);
		let pending = if let Some(mut pending) = tx.get(&key, None).await? {
			pending.new_vectors = new_vectors;
			pending
		} else {
			HnswRecordPendingUpdate {
				doc_id: HnswDocs::get_doc_id(&self.ikb, &tx, id).await?,
				old_vectors,
				new_vectors,
			}
		};
		tx.set(&key, &pending).await?;
		Ok(())
	}

	/// Converts an append-keyed pending value into a graph operation.
	fn append_pending_to_operation(pending: VectorPendingUpdate) -> PendingOperation {
		PendingOperation {
			id: pending.id,
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
		let id = if let Some(doc_id) = pending.doc_id {
			VectorId::DocId(doc_id)
		} else {
			VectorId::RecordKey(Arc::new(id))
		};
		PendingOperation {
			id,
			old_vectors: pending.old_vectors,
			new_vectors: pending.new_vectors,
		}
	}

	/// Creates an [`HnswContext`] from the current index state and a frozen context.
	pub(super) fn new_hnsw_context<'a>(&'a self, ctx: &'a FrozenContext) -> HnswContext<'a> {
		HnswContext::new(ctx, self.ikb.clone(), &self.vec_docs)
	}

	/// Builds a bounded compaction plan for HNSW pending updates.
	///
	/// The read phase scans append-keyed `!hp` entries first, then record-keyed
	/// `!hr` entries. It records exact key/value pairs for conditional deletion
	/// and coalesces pending work by document identity.
	pub(in crate::idx) async fn prepare_compaction(
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
	) -> Result<HnswCompactionPlan> {
		let tx = ctx.tx();
		let generation = read_compaction_generation(&tx, &ikb.new_hg_key()).await?;
		let mut builder = PendingPlanBuilder::new(generation);
		let mut count = 0;
		Self::collect_append_pending_for_plan(ctx, &tx, ikb, &mut builder, &mut count).await?;
		if !builder.has_more {
			Self::collect_record_pending_for_plan(ctx, &tx, ikb, &mut builder, &mut count).await?;
		}
		Ok(builder.into_plan())
	}

	/// Adds append-keyed pending entries to a compaction plan.
	///
	/// Entries are captured as exact encoded key/value pairs so the write phase
	/// can remove only values that still match the read snapshot.
	async fn collect_append_pending_for_plan(
		ctx: &FrozenContext,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		builder: &mut PendingPlanBuilder,
		count: &mut usize,
	) -> Result<()> {
		let rng = ikb.new_hp_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		loop {
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(crate::kvs::NORMAL_BATCH_SIZE))
				.await?;
			if batch.is_empty() {
				break;
			}
			let owned: Vec<(Vec<u8>, Vec<u8>)> =
				batch.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
			for (key, value) in owned {
				if ctx.is_done(Some(*count)).await? {
					bail!(Error::QueryCancelled)
				}
				let pending = VectorPendingUpdate::kv_decode_value(&value, ())?;
				let pending = Self::append_pending_to_operation(pending);
				if !builder.add(key, value, pending) {
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
		ctx: &FrozenContext,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		builder: &mut PendingPlanBuilder,
		count: &mut usize,
	) -> Result<()> {
		let rng = ikb.new_hr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		loop {
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(crate::kvs::NORMAL_BATCH_SIZE))
				.await?;
			if batch.is_empty() {
				break;
			}
			let owned: Vec<(Vec<u8>, Vec<u8>)> =
				batch.iter().map(|(k, v)| (k.to_vec(), v.to_vec())).collect();
			for (key, value) in owned {
				if ctx.is_done(Some(*count)).await? {
					bail!(Error::QueryCancelled)
				}
				let hr = HnswRecordPending::decode_key(&key)?;
				let pending = HnswRecordPendingUpdate::kv_decode_value(&value, ())?;
				let pending = Self::record_pending_to_operation(hr.id.into_owned(), pending);
				if !builder.add(key, value, pending) {
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
	pub(in crate::idx) async fn apply_compaction(
		&self,
		ctx: &FrozenContext,
		plan: HnswCompactionPlan,
	) -> Result<bool> {
		let HnswCompactionPlan {
			generation,
			captured_keys,
			pending,
			has_more: _,
		} = plan;
		let tx = ctx.tx();
		if captured_keys.is_empty() {
			return Ok(false);
		}
		if !bump_compaction_generation(&tx, &self.ikb.new_hg_key(), generation).await? {
			return Ok(false);
		}
		for captured in &captured_keys {
			match tx.delc(&captured.key, Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => return Ok(false),
				Err(e) => return Err(e),
			}
		}
		let mut hnsw = self.hnsw.write().await;
		hnsw.check_state(ctx).await?;
		let mut ctx = self.new_hnsw_context(ctx);
		let mut docs = HnswDocs::new(&tx, self.ikb.clone()).await?;
		for pending in pending {
			self.apply_pending_operation(&mut ctx, &mut docs, &mut hnsw, pending).await?;
		}
		docs.finish(&tx).await?;
		Ok(true)
	}

	/// Drains and applies one batch of pending vector updates to the HNSW graph.
	///
	/// This convenience method is used by local HNSW tests. Datastore
	/// compaction owns the split read/write transactions around the same
	/// prepare/apply methods.
	#[cfg(test)]
	pub(in crate::idx) async fn index_pendings(&self, ctx: &FrozenContext) -> Result<usize> {
		let plan = Self::prepare_compaction(ctx, &self.ikb).await?;
		let count = plan.len();
		if self.apply_compaction(ctx, plan).await? {
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
		docs: &mut HnswDocs,
		hnsw: &mut HnswFlavor,
		pending: PendingOperation,
	) -> Result<()> {
		match pending.id {
			VectorId::DocId(doc_id) => {
				for vector in pending.old_vectors {
					let vector = Vector::from(vector);
					self.vec_docs.remove(ctx, &vector, doc_id, hnsw).await?;
				}
				if pending.new_vectors.is_empty() {
					docs.remove(&ctx.tx, doc_id, self.table_id, &self.vector_cache).await?;
				} else {
					for vector in pending.new_vectors {
						let vector = Vector::from(vector);
						self.vec_docs.insert(ctx, vector, doc_id, hnsw).await?;
					}
				}
			}
			VectorId::RecordKey(id) => {
				if !pending.new_vectors.is_empty() {
					let doc_id = docs.resolve(&ctx.tx, &id).await?;
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
	pub(crate) async fn check_state(&self, ctx: &FrozenContext) -> Result<()> {
		// Fast path: validate under a read lock. Multiple readers run concurrently,
		// so steady-state (no peer writer bumped the version) no longer serialises.
		{
			let guard = self.hnsw.read().await;
			if !guard.needs_state_reload(ctx).await? {
				return Ok(());
			}
		}
		// Slow path: a reload is required. Acquire the write lock and re-validate
		// before reloading — a concurrent task may have already refreshed.
		let mut guard = self.hnsw.write().await;
		if guard.needs_state_reload(ctx).await? {
			guard.check_state(ctx).await?;
		}
		Ok(())
	}

	/// Performs a k-nearest neighbor search, combining pending and committed results.
	///
	/// HNSW pending updates remain on the hot write path, so lookup scans them
	/// conservatively instead of relying on a shared pending-state key that can
	/// create write contention under concurrent indexing.
	pub(crate) async fn knn_search(
		&self,
		ctx: &FrozenContext,
		stk: &mut Stk,
		pt: &[Number],
		k: usize,
		ef: usize,
		cond_filter: Option<(&Options, Arc<Cond>)>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let compaction_generation =
			read_compaction_generation(&ctx.tx(), &self.ikb.new_hg_key()).await?;
		// Build a filter if required
		let mut filter = if let Some((opt, cond)) = cond_filter {
			Some(HnswTruthyDocumentFilter::new(
				opt,
				self.ikb.clone(),
				self.table_id,
				self.vector_cache.clone(),
				cond,
				compaction_generation,
			))
		} else {
			None
		};
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, pt)?.into();
		vector.check_dimension(self.dim)?;
		let search = HnswSearch::new(vector, k, ef);
		// Get a new HNSW context
		let ctx = self.new_hnsw_context(ctx);
		// Collect the result
		let mut builder = KnnResultBuilder::new(k);

		// Search in the pendings if any
		let pending_docs =
			self.search_pendings(&ctx, stk, &search, &mut filter, &mut builder).await?;
		// Search in the graph
		self.search_graph(&ctx, stk, &search, pending_docs, &mut filter, &mut builder).await?;

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
	/// excluding documents that are present in `pending_docs`.
	pub(super) async fn search_graph(
		&self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		search: &HnswSearch,
		pending_docs: Option<RoaringTreemap>,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<()> {
		let hnsw = self.hnsw.read().await;
		// Do the search
		if let Some(filter) = filter {
			let neighbours = hnsw
				.knn_search_with_filter(ctx, search, stk, filter, pending_docs.as_ref())
				.await?;
			self.add_graph_results(
				&ctx.tx,
				&hnsw,
				neighbours,
				pending_docs.as_ref(),
				builder,
				|evicted_docs| filter.expires(&evicted_docs),
			)
			.await
		} else {
			let neighbours = hnsw.knn_search(ctx, search, pending_docs.as_ref()).await?;
			self.add_graph_results(
				&ctx.tx,
				&hnsw,
				neighbours,
				pending_docs.as_ref(),
				builder,
				|_| {},
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
		builder: &mut KnnResultBuilder,
	) -> Result<Option<RoaringTreemap>> {
		let mut all_existing_docs = RoaringTreemap::new();
		let mut non_deleted_docs = HashMap::default();
		// First pass, identify deleted doc
		self.collect_pending(ctx.ctx, &ctx.tx, |pending| {
			if let VectorId::DocId(doc_id) = &pending.id {
				all_existing_docs.insert(*doc_id);
			};
			if pending.new_vectors.is_empty() {
				non_deleted_docs.remove(&pending.id);
			} else {
				non_deleted_docs.insert(pending.id, pending.new_vectors);
			}
		})
		.await?;
		if all_existing_docs.is_empty() && non_deleted_docs.is_empty() {
			return Ok(None);
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
		ctx: &Context,
		tx: &Transaction,
		mut collector: F,
	) -> Result<()>
	where
		F: FnMut(PendingOperation),
	{
		let rng = self.ikb.new_hp_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		let mut count = 0;
		loop {
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(crate::kvs::NORMAL_BATCH_SIZE))
				.await?;
			if batch.is_empty() {
				break;
			}
			for (_, v) in &batch {
				if ctx.is_done(Some(count)).await? {
					bail!(Error::QueryCancelled)
				}
				let pending = VectorPendingUpdate::kv_decode_value(v, ())?;
				collector(Self::append_pending_to_operation(pending));
				count += 1;
			}
		}
		drop(cursor);

		let rng = self.ikb.new_hr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		loop {
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(crate::kvs::NORMAL_BATCH_SIZE))
				.await?;
			if batch.is_empty() {
				break;
			}
			for (key, value) in &batch {
				if ctx.is_done(Some(count)).await? {
					bail!(Error::QueryCancelled)
				}
				let hr = HnswRecordPending::decode_key(key)?;
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
	async fn add_graph_results<F>(
		&self,
		tx: &Transaction,
		hnsw: &HnswFlavor,
		neighbors: Vec<(f64, ElementId)>,
		pending_docs: Option<&RoaringTreemap>,
		builder: &mut KnnResultBuilder,
		mut evicted_docs_func: F,
	) -> Result<()>
	where
		F: FnMut(Vec<VectorId>),
	{
		for (e_dist, e_id) in neighbors {
			if !builder.check_add(e_dist) {
				continue;
			}
			let docs = if let Some(docs) = self.vec_docs.get_cached_doc_set(e_id).await {
				Some(docs)
			} else if let Some(v) = hnsw.get_vector(tx, &e_id).await? {
				self.vec_docs.get_docs_by_element(tx, e_id, &v).await?
			} else {
				None
			};
			if let Some(docs) = docs {
				let evicted_docs = if let Some(pending_docs) = pending_docs {
					let mut evicted_docs = Vec::with_capacity(1);
					for doc_id in docs.iter() {
						if pending_docs.contains(doc_id) {
							continue;
						}
						if let Some(evicted_id) =
							builder.add_vector_id_result(e_dist, VectorId::DocId(doc_id))
						{
							evicted_docs.push(evicted_id);
						}
					}
					evicted_docs
				} else {
					builder.add_graph_result(e_dist, &docs)
				};
				evicted_docs_func(evicted_docs);
			}
		}
		Ok(())
	}

	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		self.hnsw.read().await.check_hnsw_properties(expected_count).await
	}
}
