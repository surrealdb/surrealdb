//! DiskANN index orchestration.
//!
//! This module connects SurrealDB index writes, background compaction, and KNN lookup to the
//! KV-backed DiskANN graph provider. User writes append record-keyed pending updates (`!dr`) and
//! mark the pending-state guard (`!dp`) non-empty. Compaction consumes a bounded pending batch,
//! mutates the graph/document mappings, and moves `!dp` toward empty only after empty-range
//! confirmation. Lookup uses `!dp` to skip pending scans only when every compute node can safely
//! agree that no committed pending keys exist.

use std::collections::VecDeque;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, bail};
use diskann::graph::DiskANNIndex as RawDiskAnnIndex;
use diskann::graph::config::{Builder, MaxDegree, PruneKind};
use diskann::graph::search::Knn;
use diskann::graph::search_output_buffer::IdDistance;
use diskann::provider::{Delete, Guard, SetElement};
use diskann_vector::Half;
use diskann_vector::distance::Metric;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use tokio::sync::RwLock;

use crate::catalog::{DiskAnnParams, Distance, TableId, VectorType};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::Cond;
use crate::idx::planner::ScanDirection;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::diskann::docs::{DiskAnnDocs, DiskAnnVecDocs};
use crate::idx::trees::diskann::filter::DiskAnnTruthyDocumentFilter;
use crate::idx::trees::diskann::provider::{
	DiskAnnProvider, DiskAnnProviderContext, DiskAnnStrategy, DiskAnnVectorElement,
};
use crate::idx::trees::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnPendingStateKind,
	DiskAnnRecordPendingUpdate, ElementId,
};
use crate::idx::trees::hnsw::VectorId;
use crate::idx::trees::knn::KnnResultBuilder;
use crate::idx::trees::vector::{SerializedVector, Vector};
use crate::idx::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
	read_compaction_generation,
};
use crate::key::index::dr::DiskAnnRecordPending;
use crate::kvs::{KVValue, Key, Transaction, Val};
use crate::val::{Number, RecordId, RecordIdKey, Value};

/// Soft per-batch limits for [`DiskAnnIndex::prepare_compaction`]. When either cap fires,
/// `has_more = true` is set on the [`DiskAnnCompactionPlan`] and the caller is expected to run
/// another compaction iteration.
///
/// Note (#7318 review followup, C7): while `has_more = true`, [`apply_compaction`] keeps the
/// `!dp` pending-state shards `NonEmpty` (`should_clear_pending_state = !has_more`). KNN
/// lookups therefore scan the full `!dr` range on every query for the duration of the backlog
/// — this is unavoidable today because `!dr` is keyed by `RecordIdKey` directly and there is
/// no efficient per-shard scan. A proper fix would prefix `!dr` keys with the pending-state
/// shard id (or maintain a parallel `!dr`-by-shard index) so that `prepare_compaction` can
/// drain one shard at a time and advance only that shard's `!dp` after a successful commit.
/// Until then, sustained write load saturates these limits and KNN search pays the
/// full-range scan on every query.
const DISKANN_COMPACTION_MAX_PENDING_KEYS: usize = 1024;
const DISKANN_COMPACTION_MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

struct CapturedPendingKey {
	/// Exact pending key captured during the read phase.
	key: Key,
	/// Value observed for the key; apply deletes it conditionally before mutating the graph.
	value: Val,
}

#[derive(Clone)]
struct PendingOperation {
	/// Owning record/document ID after coalescing record-keyed pending updates.
	id: VectorId,
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
/// snapshot used to reject stale plans and to clear `!dp` shards conservatively.
pub(crate) struct DiskAnnCompactionPlan {
	/// Compaction generation observed while preparing the plan.
	generation: Option<u64>,
	/// Pending-state guard shards observed before scanning `!dr`.
	pending_state: PendingStateSnapshot,
	/// Pending keys captured for conditional deletion.
	captured_keys: Vec<CapturedPendingKey>,
	/// Coalesced graph/document operations derived from captured pending records.
	pending: Vec<PendingOperation>,
	/// True when prepare stopped because the bounded batch limit was reached.
	has_more: bool,
}

impl DiskAnnCompactionPlan {
	/// Returns whether the plan captured pending keys to apply.
	pub(crate) fn has_work(&self) -> bool {
		!self.captured_keys.is_empty()
	}

	/// Returns whether the write phase should run for this plan.
	pub(crate) fn requires_apply(&self) -> bool {
		self.has_work()
			|| self.pending_state.iter().any(|state| {
				state.as_ref().is_none_or(|state| state.kind != DiskAnnPendingStateKind::Empty)
			})
	}

	/// Returns whether another compaction pass should be scheduled for remaining pending keys.
	pub(crate) fn has_more(&self) -> bool {
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
			has_more: false,
		}
	}

	fn add(&mut self, key: Key, value: Val, pending: PendingOperation) -> bool {
		if self.captured_keys.len() >= DISKANN_COMPACTION_MAX_PENDING_KEYS
			|| (!self.captured_keys.is_empty()
				&& self.encoded_bytes + key.len() + value.len()
					> DISKANN_COMPACTION_MAX_PENDING_BYTES)
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
		if self.captured_keys.len() >= DISKANN_COMPACTION_MAX_PENDING_KEYS
			|| self.encoded_bytes >= DISKANN_COMPACTION_MAX_PENDING_BYTES
		{
			self.has_more = true;
		}
		true
	}

	fn add_pending(&mut self, pending: PendingOperation) {
		if let Some(pos) = self.pending_by_id.get(&pending.id) {
			self.pending[*pos].new_vectors = pending.new_vectors;
			return;
		}
		let pos = self.pending.len();
		self.pending_by_id.insert(pending.id.clone(), pos);
		self.pending.push(pending);
	}

	fn into_plan(self) -> DiskAnnCompactionPlan {
		DiskAnnCompactionPlan {
			generation: self.generation,
			pending_state: self.pending_state,
			captured_keys: self.captured_keys,
			pending: self.pending,
			has_more: self.has_more,
		}
	}
}

/// One DiskANN index instance cached inside [`IndexStores`](crate::idx::trees::store::IndexStores).
pub(crate) struct DiskAnnIndex {
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
}

/// Context passed from SurrealDB execution into DiskANN provider calls.
pub(super) struct DiskAnnContext<'a> {
	/// Frozen query context for cancellation and condition evaluation.
	pub(super) ctx: &'a FrozenContext,
	/// Transaction used by the current graph/search/compaction operation.
	pub(super) tx: Arc<Transaction>,
	/// Index key builder copied into provider-facing calls.
	pub(super) ikb: IndexKeyBase,
	/// DiskANN provider context containing transaction/key state.
	pub(super) provider_context: DiskAnnProviderContext,
}

impl<'a> DiskAnnContext<'a> {
	fn new(
		ctx: &'a FrozenContext,
		ikb: IndexKeyBase,
		provider_context: DiskAnnProviderContext,
	) -> Self {
		let tx = ctx.tx();
		Self {
			ctx,
			tx,
			ikb,
			provider_context,
		}
	}
}

/// Thin wrapper around the upstream DiskANN graph using SurrealDB's provider implementation.
pub(super) struct DiskAnnGraph {
	index: RawDiskAnnIndex<DiskAnnProvider>,
}

/// Raw graph-search output before document filtering.
///
/// The distance is the value returned by the DiskANN distance computer for the index metric. It is
/// converted to SurrealDB's public distance semantics before it reaches the KNN result builder.
type DiskAnnSearchResult = (ElementId, f64);

impl DiskAnnGraph {
	/// Builds the upstream DiskANN graph configuration and provider for one SurrealDB index.
	fn new(ikb: IndexKeyBase, tb: TableId, p: &DiskAnnParams, cache: DiskAnnCache) -> Result<Self> {
		let metric = distance_to_metric(&p.distance)?;
		let alpha = p.alpha.to_float() as f32;
		if !alpha.is_finite() || alpha <= 0.0 {
			bail!("DISKANN ALPHA must be finite and greater than 0")
		}
		let mut builder = Builder::new(
			p.degree as usize,
			MaxDegree::default_slack(),
			p.l_build as usize,
			PruneKind::from_metric(metric),
		);
		builder.alpha(alpha);
		let config = builder.build()?;
		let provider = DiskAnnProvider::new(ikb, tb, cache, p.dimension as usize, metric);
		Ok(Self {
			index: RawDiskAnnIndex::new(config, provider, None),
		})
	}

	/// Inserts one vector into the graph and returns its new element ID.
	pub(super) async fn insert(
		&mut self,
		ctx: &DiskAnnContext<'_>,
		vector: Vector,
	) -> Result<ElementId> {
		match vector {
			Vector::F32(values) => {
				let Some(values) = values.as_slice() else {
					bail!("DISKANN vector storage must be contiguous")
				};
				self.insert_typed(ctx, values).await
			}
			Vector::F16(values) => {
				let Some(values) = values.as_slice() else {
					bail!("DISKANN vector storage must be contiguous")
				};
				self.insert_typed(ctx, values).await
			}
			Vector::I8(values) => {
				let Some(values) = values.as_slice() else {
					bail!("DISKANN vector storage must be contiguous")
				};
				self.insert_typed(ctx, values).await
			}
			Vector::U8(values) => {
				let Some(values) = values.as_slice() else {
					bail!("DISKANN vector storage must be contiguous")
				};
				self.insert_typed(ctx, values).await
			}
			_ => bail!("DISKANN supports TYPE F32, F16, I8, and U8"),
		}
	}

	/// Inserts a typed vector slice through the upstream DiskANN insertion strategy.
	async fn insert_typed<T>(&mut self, ctx: &DiskAnnContext<'_>, values: &[T]) -> Result<ElementId>
	where
		T: DiskAnnVectorElement,
		for<'a> DiskAnnProvider: SetElement<&'a [T], SetError = diskann::ANNError>,
	{
		let provider = self.index.provider();
		let element_id = provider.allocate_element_id(&ctx.provider_context).await?;
		if provider.valid_starting_points(&ctx.provider_context).await?.is_empty() {
			let guard = provider.set_element(&ctx.provider_context, &element_id, values).await?;
			guard.complete().await;
			let node: crate::idx::trees::diskann::DiskAnnNode = Default::default();
			ctx.tx.set(&ctx.ikb.new_dn_key(element_id), &node).await?;
			provider.set_entry_point(&ctx.provider_context, Some(element_id)).await?;
		} else {
			self.index
				.insert(DiskAnnStrategy::<T>::default(), &ctx.provider_context, &element_id, values)
				.await?;
			provider.ensure_entry_point(&ctx.provider_context, element_id).await?;
		}
		Ok(element_id)
	}

	/// Marks one graph element deleted and refreshes the entry point if needed.
	pub(super) async fn remove(
		&mut self,
		ctx: &DiskAnnContext<'_>,
		element_id: ElementId,
	) -> Result<()> {
		let provider = self.index.provider();
		provider.delete(&ctx.provider_context, &element_id).await?;
		let next = provider.valid_starting_points(&ctx.provider_context).await?.into_iter().next();
		provider.set_entry_point(&ctx.provider_context, next).await?;
		Ok(())
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
		let params = Knn::new_default(limit, limit)?;
		let mut ids = vec![0; limit];
		let mut distances = vec![0.0; limit];
		let mut output = IdDistance::new(&mut ids, &mut distances);
		let stats = self
			.index
			.search(
				params,
				&DiskAnnStrategy::<T>::default(),
				&ctx.provider_context,
				query,
				&mut output,
			)
			.await?;
		let result_count = stats.result_count as usize;
		Ok(ids
			.into_iter()
			.zip(distances)
			.take(result_count)
			.map(|(id, distance)| (id, distance as f64))
			.collect())
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
	/// Query vector in the shared SurrealDB representation, used for exact pending scoring.
	pt: Vector,
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
			pt,
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
	/// Optional condition filter applied before admitting candidates to the result builder.
	filter: &'a mut Option<DiskAnnTruthyDocumentFilter<'b>>,
	/// Shared result builder combining pending and graph candidates.
	builder: &'a mut KnnResultBuilder,
}

impl DiskAnnIndex {
	/// Creates a DiskANN index wrapper and validates the configured type/metric combination.
	pub(crate) async fn new(
		ikb: IndexKeyBase,
		tb: TableId,
		p: &DiskAnnParams,
		cache: DiskAnnCache,
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
			graph: RwLock::new(DiskAnnGraph::new(ikb.clone(), tb, p, cache.clone())?),
			vec_docs: DiskAnnVecDocs::new(ikb.clone(), tb, cache, p.use_hashed_vector),
			ikb,
		})
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
			let vector = SerializedVector::try_from_value(self.vector_type, self.dim, value)?;
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

	/// Reads every DiskANN pending-state shard in one ordered batch.
	async fn read_pending_state(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<PendingStateSnapshot> {
		let keys: Vec<_> =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|shard| ikb.new_dp_key(shard)).collect();
		tx.getm(keys, None).await
	}

	/// Returns true when lookup must scan `!dr` because at least one pending-state shard is not
	/// explicitly confirmed empty.
	fn pending_state_requires_scan(state: &[Option<DiskAnnPendingState>]) -> bool {
		state.iter().any(|state| {
			state.as_ref().is_none_or(|state| state.kind != DiskAnnPendingStateKind::Empty)
		})
	}

	/// Marks the distributed pending-state guard as non-empty after writing a pending update.
	///
	/// Single-shot: within one transaction the `tx.get` snapshot and the `tx.putc` condition check
	/// see the same value, so a retry on `TransactionConditionNotMet` would deterministically
	/// reach the same outcome (sister function [`Self::clear_pending_state_if_current`] uses the
	/// same single-shot shape for the inverse direction). The old code wrapped this in a
	/// 32-iteration retry loop on the same tx, which couldn't help and would just spin to the
	/// `bail!` at the end.
	async fn mark_pending_non_empty(
		tx: &Transaction,
		ikb: &IndexKeyBase,
		id: &RecordIdKey,
	) -> Result<()> {
		let key = ikb.new_dp_key(Self::pending_state_shard(id));
		let current: Option<DiskAnnPendingState> = tx.get(&key, None).await?;
		if current.as_ref().is_some_and(|state| state.kind == DiskAnnPendingStateKind::NonEmpty) {
			return Ok(());
		}
		let next = DiskAnnPendingState {
			kind: DiskAnnPendingStateKind::NonEmpty,
			generation: current.as_ref().map_or(0, |state| state.generation).saturating_add(1),
		};
		tx.putc(&key, &next, current.as_ref()).await
	}

	/// Conditionally advances `!dp` toward empty after compaction consumed its planned range.
	async fn clear_pending_state_if_current(
		tx: &Transaction,
		ikb: &IndexKeyBase,
		current: &[Option<DiskAnnPendingState>],
	) -> Result<bool> {
		let mut changed = false;
		for (shard, current) in current.iter().enumerate() {
			if current.as_ref().is_some_and(|state| state.kind == DiskAnnPendingStateKind::Empty) {
				continue;
			}
			let key = ikb.new_dp_key(shard as u16);
			let kind = match current.as_ref().map(|state| state.kind) {
				Some(DiskAnnPendingStateKind::NonEmpty) => DiskAnnPendingStateKind::MaybeEmpty,
				Some(DiskAnnPendingStateKind::MaybeEmpty) | None => DiskAnnPendingStateKind::Empty,
				Some(DiskAnnPendingStateKind::Empty) => unreachable!(),
			};
			let next = DiskAnnPendingState {
				kind,
				generation: current.as_ref().map_or(0, |state| state.generation.saturating_add(1)),
			};
			match tx.putc(&key, &next, current.as_ref()).await {
				Ok(()) => changed = true,
				Err(e) if is_transaction_condition_not_met(&e) => return Ok(false),
				Err(e) => return Err(e),
			}
		}
		Ok(changed)
	}

	/// Re-checks the DiskANN pending range inside the apply transaction before clearing state.
	async fn pending_range_empty(
		ctx: &FrozenContext,
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<bool> {
		let rng = ikb.new_dr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		// The first non-empty batch is conclusive; we just need to know
		// whether *any* entry exists in the range.
		let batch = cursor.next_batch(crate::kvs::ScanLimit::Count(1)).await?;
		if !batch.is_empty() {
			return Ok(false);
		}
		drop(cursor);
		if ctx.is_done(None).await? {
			bail!(Error::QueryCancelled)
		}
		Ok(true)
	}

	/// Records a transaction's old/new vectors as a coalesced pending update.
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
		let key = self.ikb.new_dr_key(id);
		let pending = if let Some(mut pending) = tx.get(&key, None).await? {
			pending.new_vectors = new_vectors;
			pending
		} else {
			DiskAnnRecordPendingUpdate {
				doc_id: DiskAnnDocs::get_doc_id(&self.ikb, &tx, id).await?,
				old_vectors,
				new_vectors,
			}
		};
		tx.set(&key, &pending).await?;
		Self::mark_pending_non_empty(&tx, &self.ikb, id).await?;
		Ok(())
	}

	/// Converts a persisted record-keyed pending value into a graph compaction operation.
	fn record_pending_to_operation(
		id: RecordIdKey,
		pending: DiskAnnRecordPendingUpdate,
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

	/// Builds a context that shares the current transaction with the DiskANN provider.
	fn new_diskann_context<'a>(
		&'a self,
		ctx: &'a FrozenContext,
		provider_context: DiskAnnProviderContext,
	) -> DiskAnnContext<'a> {
		DiskAnnContext::new(ctx, self.ikb.clone(), provider_context)
	}

	/// Scans a bounded `!dr` range and prepares a conditional compaction batch.
	pub(in crate::idx) async fn prepare_compaction(
		ctx: &FrozenContext,
		ikb: &IndexKeyBase,
	) -> Result<DiskAnnCompactionPlan> {
		let tx = ctx.tx();
		let generation = read_compaction_generation(&tx, &ikb.new_dg_key()).await?;
		let pending_state = Self::read_pending_state(&tx, ikb).await?;
		let mut builder = PendingPlanBuilder::new(generation, pending_state);
		let rng = ikb.new_dr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		let mut count = 0;
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
				if ctx.is_done(Some(count)).await? {
					bail!(Error::QueryCancelled)
				}
				let dr = DiskAnnRecordPending::decode_key(&key)?;
				let pending = DiskAnnRecordPendingUpdate::kv_decode_value(&value, ())?;
				let pending = Self::record_pending_to_operation(dr.id.into_owned(), pending);
				if !builder.add(key, value, pending) {
					return Ok(builder.into_plan());
				}
				count += 1;
				if builder.has_more {
					return Ok(builder.into_plan());
				}
			}
		}
		Ok(builder.into_plan())
	}

	/// Applies a prepared compaction plan if its generation and captured keys are still current.
	///
	/// The transaction lifecycle is owned by this method:
	///   * `Ok(true)` — mutations were applied and the transaction has been committed.
	///   * `Ok(false)` — the plan was stale (generation drift or captured-key mismatch); the
	///     transaction has been cancelled and no mutations are in KV or in the process-local cache.
	///   * `Err(_)` — the apply or commit step failed; the transaction has been cancelled and, if
	///     any graph mutations had been buffered, the per-index [`DiskAnnCache`] has been cleared
	///     while the graph lock was still held so concurrent KNN searches cannot observe a cache
	///     state that disagrees with KV.
	///
	/// This frame is what rule (2) of the
	/// [cache coherency invariant](crate::idx::trees::diskann::provider) refers to: writable-tx
	/// cache write-throughs in the provider are sound only because they happen inside it.
	pub(in crate::idx) async fn apply_compaction(
		&self,
		ctx: &FrozenContext,
		plan: DiskAnnCompactionPlan,
	) -> Result<bool> {
		let DiskAnnCompactionPlan {
			generation,
			pending_state,
			captured_keys,
			pending,
			has_more,
		} = plan;
		let should_clear_pending_state = !has_more;
		let tx = ctx.tx();
		if captured_keys.is_empty() {
			// No graph mutations possible; the only KV writes here are to the !dp
			// shards. Commit (or cancel) without touching the graph lock or cache.
			if should_clear_pending_state
				&& Self::pending_range_empty(ctx, &tx, &self.ikb).await?
				&& Self::clear_pending_state_if_current(&tx, &self.ikb, &pending_state).await?
			{
				return tx.commit().await.map(|()| true);
			}
			cancel_silently(&tx).await;
			return Ok(false);
		}
		if !bump_compaction_generation(&tx, &self.ikb.new_dg_key(), generation).await? {
			cancel_silently(&tx).await;
			return Ok(false);
		}
		for captured in &captured_keys {
			match tx.delc(&captured.key, Some(&captured.value)).await {
				Ok(()) => {}
				Err(e) if is_transaction_condition_not_met(&e) => {
					cancel_silently(&tx).await;
					return Ok(false);
				}
				Err(e) => {
					cancel_silently(&tx).await;
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
		let apply_result: Result<()> = async {
			let mut docs = DiskAnnDocs::new(&tx, self.ikb.clone()).await?;
			let provider_context = graph.index.provider().context(Arc::clone(&tx));
			let diskann_ctx = self.new_diskann_context(ctx, provider_context);
			for pending in pending {
				self.apply_pending_operation(&diskann_ctx, &mut docs, &mut graph, pending).await?;
			}
			docs.finish(&tx).await?;
			if should_clear_pending_state && Self::pending_range_empty(ctx, &tx, &self.ikb).await? {
				Self::clear_pending_state_if_current(&tx, &self.ikb, &pending_state).await?;
			}
			Ok(())
		}
		.await;
		if let Err(e) = apply_result {
			cancel_silently(&tx).await;
			self.clear_local_cache().await;
			return Err(e);
		}
		if let Err(e) = tx.commit().await {
			self.clear_local_cache().await;
			return Err(e);
		}
		// Lock is released as `graph` goes out of scope. By the time any
		// concurrent reader can acquire `graph.read()` the cache and KV are
		// consistent (commit succeeded) — or, on the error path above, the
		// cache has been cleared (commit failed) before the lock was released.
		Ok(true)
	}

	/// Drops every entry in the process-local [`DiskAnnCache`] that is scoped to
	/// this index, keeping the [`DiskAnnIndex`] registration intact so the graph
	/// `RwLock` continues to serialise compaction and KNN search.
	async fn clear_local_cache(&self) {
		self.cache
			.remove_index(self.ikb.ns(), self.ikb.db(), self.table_id, self.ikb.index())
			.await;
	}

	/// Applies one coalesced pending operation to document mappings and the DiskANN graph.
	async fn apply_pending_operation(
		&self,
		ctx: &DiskAnnContext<'_>,
		docs: &mut DiskAnnDocs,
		graph: &mut DiskAnnGraph,
		pending: PendingOperation,
	) -> Result<()> {
		match pending.id {
			VectorId::DocId(doc_id) => {
				for vector in pending.old_vectors {
					let vector = Vector::from(vector);
					self.vec_docs.remove(ctx, &vector, doc_id, graph).await?;
				}
				if pending.new_vectors.is_empty() {
					docs.remove(&ctx.tx, doc_id, self.table_id, &self.cache).await?;
				} else {
					for vector in pending.new_vectors {
						self.vec_docs.insert(ctx, Vector::from(vector), doc_id, graph).await?;
					}
				}
			}
			VectorId::RecordKey(id) => {
				if !pending.new_vectors.is_empty() {
					let doc_id = docs.resolve(&ctx.tx, &id).await?;
					for vector in pending.new_vectors {
						self.vec_docs.insert(ctx, Vector::from(vector), doc_id, graph).await?;
					}
				}
			}
		}
		Ok(())
	}

	/// Placeholder consistency hook matching the HNSW index-store interface.
	pub(crate) async fn check_state(&self) -> Result<()> {
		Ok(())
	}

	/// Executes a DiskANN KNN lookup and returns ordered iterator results.
	///
	/// Lookup scans pending updates unless the distributed pending-state guard is explicitly empty.
	/// Compacted graph candidates are resolved through process-local caches before any remaining KV
	/// reads, and final document IDs are materialized in one batch.
	pub(crate) async fn knn_search(
		&self,
		ctx: &FrozenContext,
		stk: &mut Stk,
		pt: &[Number],
		k: usize,
		ef: usize,
		cond_filter: Option<(&Options, Arc<Cond>)>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		let pending_state = Self::read_pending_state(&ctx.tx(), &self.ikb).await?;
		let compaction_generation =
			read_compaction_generation(&ctx.tx(), &self.ikb.new_dg_key()).await?;
		let mut filter = cond_filter.map(|(opt, cond)| {
			DiskAnnTruthyDocumentFilter::new(
				opt,
				self.ikb.clone(),
				self.table_id,
				self.cache.clone(),
				compaction_generation,
				cond,
			)
		});
		let vector = Vector::try_from_vector(self.vector_type, pt)?;
		vector.check_dimension(self.dim)?;
		let search = DiskAnnSearch::new(vector, k, ef)?;
		let graph = self.graph.read().await;
		let provider_context = graph.index.provider().context(ctx.tx());
		let ctx = self.new_diskann_context(ctx, provider_context);
		let mut builder = KnnResultBuilder::new(k);
		let pending_docs = if Self::pending_state_requires_scan(&pending_state) {
			self.search_pendings(&ctx, stk, &search, &mut filter, &mut builder).await?
		} else {
			None
		};
		self.search_graph(
			&ctx,
			stk,
			DiskAnnGraphSearch {
				graph: &graph,
				search: &search,
				pending_docs,
				filter: &mut filter,
				builder: &mut builder,
			},
		)
		.await?;
		let result = builder.collect();
		let cache = filter.map(DiskAnnTruthyDocumentFilter::release);
		let doc_ids: Vec<_> = result
			.iter()
			.filter_map(|(_, id)| match id {
				VectorId::DocId(doc_id) => Some(*doc_id),
				VectorId::RecordKey(_) => None,
			})
			.collect();
		let mut doc_rids = DiskAnnDocs::get_things_batch(
			&ctx.ikb,
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
			let cached = cache.as_ref().and_then(|cache| cache.get(&id)).cloned();
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

	/// Searches the compacted graph and adds visible candidate documents to the result builder.
	async fn search_graph(
		&self,
		ctx: &DiskAnnContext<'_>,
		stk: &mut Stk,
		state: DiskAnnGraphSearch<'_, '_>,
	) -> Result<()> {
		let results =
			state.graph.search(ctx, &state.search.query, state.search.k, state.search.l).await?;
		// Keep the distances returned by graph search instead of re-reading each vector only to
		// recompute the same score. The remaining vector reads are only needed to resolve
		// vector-to-document keys.
		let candidates: Vec<_> = results
			.into_iter()
			.map(|(element_id, distance)| (element_id, self.graph_distance(distance)))
			.filter(|(_, distance)| state.builder.check_add(*distance))
			.collect();
		if candidates.is_empty() {
			return Ok(());
		}
		// Resolve candidate graph elements to document id sets before applying pending-update
		// suppression and optional truthy filtering. Warm doc-set cache hits avoid re-reading the
		// graph vector; misses fetch only the missing vectors before falling back to Dq/Dh
		// mappings.
		let docs = self.vec_docs.get_docs_by_element_batch(&ctx.tx, &candidates).await?;
		for (_, distance, docs) in docs {
			if !state.builder.check_add(distance) {
				continue;
			}
			let Some(docs) = docs else {
				continue;
			};
			for doc_id in docs.iter() {
				if state.pending_docs.as_ref().is_some_and(|pending| pending.contains(doc_id)) {
					continue;
				}
				let id = VectorId::DocId(doc_id);
				if let Some(filter) = state.filter.as_mut()
					&& !filter.check_vector_id_truthy(ctx, stk, id.clone()).await?
				{
					continue;
				}
				if let Some(evicted_id) = state.builder.add_vector_id_result(distance, id)
					&& let Some(filter) = state.filter.as_mut()
				{
					filter.expire(&evicted_id);
				}
			}
		}
		Ok(())
	}

	/// Scores pending vectors exactly and returns document IDs that should suppress graph results.
	async fn search_pendings(
		&self,
		ctx: &DiskAnnContext<'_>,
		stk: &mut Stk,
		search: &DiskAnnSearch,
		filter: &mut Option<DiskAnnTruthyDocumentFilter<'_>>,
		builder: &mut KnnResultBuilder,
	) -> Result<Option<RoaringTreemap>> {
		let mut all_existing_docs = RoaringTreemap::new();
		let mut non_deleted_docs = HashMap::default();
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
		for (id, vectors) in non_deleted_docs {
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

	/// Streams all pending updates for conservative lookup merging.
	async fn collect_pending<F>(
		&self,
		ctx: &Context,
		tx: &Transaction,
		mut collector: F,
	) -> Result<()>
	where
		F: FnMut(PendingOperation),
	{
		let rng = self.ikb.new_dr_range()?;
		let mut cursor = tx.open_vals_cursor(rng, ScanDirection::Forward, 0, None).await?;
		let mut count = 0;
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
				let dr = DiskAnnRecordPending::decode_key(key)?;
				let pending = DiskAnnRecordPendingUpdate::kv_decode_value(value, ())?;
				collector(Self::record_pending_to_operation(dr.id.into_owned(), pending));
				count += 1;
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	#[cfg(feature = "kv-rocksdb")]
	use temp_dir::TempDir;

	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId};
	use crate::idx::trees::diskann::cache::DiskAnnCache;
	use crate::kvs::{Datastore, LockType, TransactionType};

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	fn cache() -> DiskAnnCache {
		DiskAnnCache::new(1024 * 1024)
	}

	fn params(vector_type: VectorType, distance: Distance) -> DiskAnnParams {
		DiskAnnParams {
			dimension: 4,
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
			has_more: false,
		}
	}

	async fn new_ctx(ds: &Datastore, tt: TransactionType) -> FrozenContext {
		let tx = Arc::new(ds.transaction(tt, LockType::Optimistic).await.unwrap());
		let mut ctx = Context::new_test();
		ctx.set_transaction(tx);
		ctx.freeze()
	}

	async fn diskann_pending_states(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<Vec<Option<DiskAnnPendingState>>> {
		let keys: Vec<_> =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|shard| ikb.new_dp_key(shard)).collect();
		tx.getm(keys, None).await
	}

	fn diskann_any_pending_state_non_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().flatten().any(|state| state.kind == DiskAnnPendingStateKind::NonEmpty)
	}

	fn diskann_any_pending_state_maybe_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().flatten().any(|state| state.kind == DiskAnnPendingStateKind::MaybeEmpty)
	}

	fn diskann_pending_states_require_scan(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().any(|state| {
			state.as_ref().is_none_or(|state| state.kind != DiskAnnPendingStateKind::Empty)
		})
	}

	fn diskann_all_pending_states_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().all(|state| {
			state.as_ref().is_some_and(|state| state.kind == DiskAnnPendingStateKind::Empty)
		})
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
		}
	}

	fn f32_query(values: &[f32]) -> Vec<Number> {
		values.iter().map(|v| Number::from(*v)).collect()
	}

	async fn knn_len_with_k(
		index: &DiskAnnIndex,
		ds: &Datastore,
		values: &[f32],
		k: usize,
	) -> Result<usize> {
		let ctx = new_ctx(ds, TransactionType::Read).await;
		let query = f32_query(values);
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async { index.knn_search(&ctx, stk, &query, k, 8, None).await })
			.finish()
			.await?;
		ctx.tx().cancel().await?;
		Ok(res.len())
	}

	async fn knn_len(index: &DiskAnnIndex, ds: &Datastore, values: &[f32]) -> Result<usize> {
		knn_len_with_k(index, ds, values, 1).await
	}

	async fn compact_once(
		index: &DiskAnnIndex,
		ds: &Datastore,
		ikb: &IndexKeyBase,
	) -> Result<bool> {
		let plan = {
			let ctx = new_ctx(ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let ctx = new_ctx(ds, TransactionType::Write).await;
		// `apply_compaction` now commits the tx itself when it returns Ok(true)
		// and cancels it on Ok(false) / Err, so the test must not double-commit.
		let applied = index.apply_compaction(&ctx, plan).await?;
		Ok(applied)
	}

	fn cached_doc_ids(
		cache: &DiskAnnCache,
		ikb: &IndexKeyBase,
		element_id: ElementId,
	) -> Option<Vec<u64>> {
		cache
			.get_doc_set((ikb.ns(), ikb.db(), TableId(4), ikb.index()), element_id)
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
	fn diskann_compaction_plan_requires_apply_for_uncleared_pending_state() {
		let mut missing = diskann_empty_pending_states();
		missing[0] = None;

		let mut maybe_empty = diskann_empty_pending_states();
		maybe_empty[0] = Some(diskann_pending_state(DiskAnnPendingStateKind::MaybeEmpty));

		let mut non_empty = diskann_empty_pending_states();
		non_empty[0] = Some(diskann_pending_state(DiskAnnPendingStateKind::NonEmpty));

		for pending_state in [missing, maybe_empty, non_empty] {
			let plan = diskann_compaction_plan(pending_state, Vec::new());
			assert!(!plan.has_work());
			assert!(plan.requires_apply());
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
			DiskAnnIndex::new(ikb(), TableId(4), &params(vector_type, distance), cache()).await?;
		}
		Ok(())
	}

	#[tokio::test]
	async fn diskann_rejects_unsupported_type_metric_combinations() -> Result<()> {
		assert!(
			DiskAnnIndex::new(
				ikb(),
				TableId(4),
				&params(VectorType::I16, Distance::Euclidean),
				cache()
			)
			.await
			.is_err()
		);
		assert!(
			DiskAnnIndex::new(
				ikb(),
				TableId(4),
				&params(VectorType::U8, Distance::CosineNormalized),
				cache()
			)
			.await
			.is_err()
		);
		assert!(
			DiskAnnIndex::new(
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
		let index = DiskAnnIndex::new(
			ikb(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		assert_eq!(index.graph_distance(9.0), 3.0);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_doc_set_cache_evicted_and_refilled_for_duplicate_vector_updates() -> Result<()>
	{
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = cache();
		let index = DiskAnnIndex::new(
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
		assert!(cached_doc_ids(&cache, &ikb, 0).is_none());

		assert_eq!(knn_len(&index, &ds, &vector).await?, 1);
		assert_eq!(cached_doc_ids(&cache, &ikb, 0), Some(vec![0, 1]));

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &first_id, Some(f32_content(&vector)), None).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert!(cached_doc_ids(&cache, &ikb, 0).is_none());

		assert_eq!(knn_len(&index, &ds, &vector).await?, 1);
		assert_eq!(cached_doc_ids(&cache, &ikb, 0), Some(vec![1]));

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			index.index(&ctx, &second_id, Some(f32_content(&vector)), None).await?;
			ctx.tx().commit().await?;
		}
		assert!(compact_once(&index, &ds, &ikb).await?);
		assert!(cached_doc_ids(&cache, &ikb, 0).is_none());
		assert_eq!(knn_len(&index, &ds, &vector).await?, 0);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_index_write_marks_pending_state_non_empty() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
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
			tx.get(&ikb.new_dr_key(&id), None).await?.unwrap();
		let states = diskann_pending_states(&tx, &ikb).await?;
		let state = states
			.iter()
			.flatten()
			.find(|state| state.kind == DiskAnnPendingStateKind::NonEmpty)
			.unwrap();
		assert!(pending.old_vectors.is_empty());
		assert_eq!(pending.new_vectors, vec![SerializedVector::F32(vec![1.0, 2.0, 3.0, 4.0])]);
		assert_eq!(state.kind, DiskAnnPendingStateKind::NonEmpty);
		assert_eq!(state.generation, 1);

		index
			.index(
				&ctx,
				&id,
				Some(f32_content(&[1.0, 2.0, 3.0, 4.0])),
				Some(f32_content(&[4.0, 3.0, 2.0, 1.0])),
			)
			.await?;
		let pending: DiskAnnRecordPendingUpdate =
			tx.get(&ikb.new_dr_key(&id), None).await?.unwrap();
		let updated_states = diskann_pending_states(&tx, &ikb).await?;
		let updated_state = updated_states
			.iter()
			.flatten()
			.find(|state| state.kind == DiskAnnPendingStateKind::NonEmpty)
			.unwrap();
		assert_eq!(pending.new_vectors, vec![SerializedVector::F32(vec![4.0, 3.0, 2.0, 1.0])]);
		assert_eq!(updated_state.kind, DiskAnnPendingStateKind::NonEmpty);
		assert!(updated_state.generation >= state.generation);
		tx.cancel().await?;
		Ok(())
	}

	#[tokio::test]
	async fn diskann_lookup_skips_pendings_only_when_pending_state_is_empty() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache(),
		)
		.await?;
		let id = RecordIdKey::Number(1);
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			tx.set(&ikb.new_dr_key(&id), &f32_pending(&[1.0, 2.0, 3.0, 4.0])).await?;
			tx.commit().await?;
		}

		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for shard in 0..DISKANN_PENDING_STATE_SHARDS {
				ctx.tx()
					.set(
						&ikb.new_dp_key(shard),
						&DiskAnnPendingState {
							kind: DiskAnnPendingStateKind::MaybeEmpty,
							generation: 0,
						},
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}

		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 1);

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			for shard in 0..DISKANN_PENDING_STATE_SHARDS {
				ctx.tx()
					.set(
						&ikb.new_dp_key(shard),
						&DiskAnnPendingState {
							kind: DiskAnnPendingStateKind::Empty,
							generation: 1,
						},
					)
					.await?;
			}
			ctx.tx().commit().await?;
		}

		assert_eq!(knn_len(&index, &ds, &[1.0, 2.0, 3.0, 4.0]).await?, 0);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_compaction_clears_pending_state_after_empty_confirmation() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
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
			// apply_compaction commits the tx internally on Ok(true).
			assert!(index.apply_compaction(&ctx, plan).await?);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_pending_states_require_scan(&states));
			assert!(diskann_any_pending_state_maybe_empty(&states));
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}

		assert!(compact_once(&index, &ds, &ikb).await?);

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_all_pending_states_empty(&states));
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[cfg(feature = "kv-rocksdb")]
	#[tokio::test]
	async fn diskann_rocksdb_clear_race_keeps_concurrent_writer_visible() -> Result<()> {
		let dir = TempDir::new()?;
		let path = format!("rocksdb:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
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

		// apply_compaction commits the tx internally on Ok(true).
		assert!(index.apply_compaction(&apply_ctx, plan).await?);

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			let shard = DiskAnnIndex::pending_state_shard(&second_id) as usize;
			assert_eq!(
				states[shard].as_ref().map(|state| state.kind),
				Some(DiskAnnPendingStateKind::MaybeEmpty)
			);
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&second_id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}

		assert_eq!(knn_len_with_k(&index, &ds, &[4.0, 3.0, 2.0, 1.0], 2).await?, 2);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_empty_compaction_plan_does_not_clear_concurrent_pending_write() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
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
			// apply_compaction now cancels the tx itself when it returns Ok(false).
			assert!(!index.apply_compaction(&ctx, plan).await?);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_any_pending_state_non_empty(&states));
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[tokio::test]
	async fn diskann_final_compaction_plan_preserves_concurrent_pending_write() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let index = DiskAnnIndex::new(
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
			// apply_compaction commits the tx internally on Ok(true).
			assert!(index.apply_compaction(&ctx, plan).await?);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_any_pending_state_non_empty(&states));
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&first_id), None).await?.is_none());
			assert!(ctx.tx().get::<_>(&ikb.new_dr_key(&second_id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	/// Regression for the #7318 class of bug: two compactors race on the same `!dr` plan,
	/// the late one's commit conflicts on `!dg` after it has already mutated the shared
	/// cache during the apply phase. `apply_compaction` must clear the per-index cache
	/// before returning the error so subsequent KNN searches can't observe element ids
	/// from the rolled-back tx.
	#[tokio::test]
	async fn diskann_failed_compaction_clears_cache_and_keeps_knn_working() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = cache();
		let index = DiskAnnIndex::new(
			ikb.clone(),
			TableId(4),
			&params(VectorType::F32, Distance::Euclidean),
			cache.clone(),
		)
		.await?;

		// Seed a handful of records so the captured plan is non-trivial.
		for i in 0..4_i64 {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let v = [i as f32, 0.0, 0.0, 0.0];
			index.index(&ctx, &RecordIdKey::Number(i), None, Some(f32_content(&v))).await?;
			ctx.tx().commit().await?;
		}

		// Two identical plans, both capturing every `!dr` key.
		let plan_a = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let plan_b = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan_a.has_work());
		assert!(plan_b.has_work());

		// Open both apply contexts before either commits, so `ctx_b`'s snapshot sees the
		// captured `!dr` keys and the pre-bump `!dg` even after `ctx_a` commits.
		let ctx_a = new_ctx(&ds, TransactionType::Write).await;
		let ctx_b = new_ctx(&ds, TransactionType::Write).await;

		// Apply A first — succeeds and commits.
		assert!(index.apply_compaction(&ctx_a, plan_a).await?);

		// Apply B — passes the write-time checks (its snapshot still sees the captured
		// values and the pre-A generation), mutates the cache during the apply phase,
		// then must fail on commit because OCC catches the snapshot violation.
		let res = index.apply_compaction(&ctx_b, plan_b).await;
		assert!(res.is_err(), "expected commit failure, got {res:?}");

		// Cache must be clean for this index: the post-failure `clear_local_cache` was
		// triggered while the graph write lock was still held.
		let cache_index = (ikb.ns(), ikb.db(), TableId(4), ikb.index());
		assert!(cache.get_state(cache_index).is_none(), "state cache should be empty");
		// And no element/node entries either — the retain-based cleanup is authoritative.
		for id in 0..4 {
			assert!(cache.get_element(cache_index, id).is_none(), "element {id} cached");
			assert!(cache.get_node(cache_index, id).is_none(), "node {id} cached");
		}

		// KNN now goes to KV, populates the cache fresh, and returns the elements
		// committed by A.
		assert_eq!(knn_len_with_k(&index, &ds, &[2.0, 0.0, 0.0, 0.0], 4).await?, 4);
		Ok(())
	}

	/// T3 — end-to-end compaction + KNN in `use_hashed_vector` mode. The existing
	/// `docs.rs` tests cover the in-bucket disambiguation only on synthetic
	/// pre-seeded buckets; this exercises the real compaction → graph build → search
	/// path with hashed-vector storage enabled.
	#[tokio::test]
	async fn diskann_hashed_vector_compaction_and_knn() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = ikb();
		let cache = cache();
		let params = DiskAnnParams {
			use_hashed_vector: true,
			..params(VectorType::F32, Distance::Euclidean)
		};
		let index = DiskAnnIndex::new(ikb.clone(), TableId(4), &params, cache.clone()).await?;

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
}
