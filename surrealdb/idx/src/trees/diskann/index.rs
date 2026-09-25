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

use std::borrow::Cow;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, bail};
use common::EngineError;
use diskann::graph::DiskANNIndex as RawDiskAnnIndex;
use diskann::graph::config::{Builder, MaxDegree, PruneKind};
use diskann::graph::search::Knn;
use diskann::graph::search_output_buffer::IdDistance;
use diskann::provider::{Delete, Guard, SetElement};
use diskann_vector::Half;
use diskann_vector::distance::Metric;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;
use surrealdb_kvs::api::ValsBatch;
use surrealdb_kvs::{Direction, Val};
use tokio::sync::RwLock;

use crate::catalog::{DiskAnnParams, Distance, TableId, VectorType};
use crate::docids::{LegacyVectorDocIds, TableDocIds};
use crate::env::IndexEnv;
use crate::key::schema::{
	DiskannRecordPendingKey, DiskannRecordPendingShardKey, DiskannRecordPendingShardShardPrefix,
};
use crate::key::{KVKey, KVKeyDecode, KVSubspace, KVValue, Key, TypedRange};
use crate::trees::diskann::cache::DiskAnnCache;
use crate::trees::diskann::docs::{DiskAnnDocs, DiskAnnVecDocs};
use crate::trees::diskann::filter::DiskAnnTruthyDocumentFilter;
use crate::trees::diskann::provider::{
	DiskAnnProvider, DiskAnnProviderContext, DiskAnnStrategy, DiskAnnVectorElement,
};
use crate::trees::diskann::{
	DISKANN_PENDING_STATE_SHARDS, DiskAnnPendingState, DiskAnnPendingStateKind,
	DiskAnnRecordPendingUpdate, ElementId,
};
use crate::trees::hnsw::VectorId;
use crate::trees::knn::KnnResultBuilder;
use crate::trees::pending::{
	PendingBacklogReport, PendingScan, PendingScanStats, take_compacted_record_id,
	take_pending_record_id,
};
use crate::trees::vector::{
	DistanceExt as _, SerializedVector, Vector, serialized_vector_from_value,
};
use crate::trees::{KnnCondFilter, KnnIteratorResult};
use crate::val::{Number, RecordId, RecordIdKey, Value};
use crate::{
	IndexKeyBase, bump_compaction_generation, is_transaction_condition_not_met,
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

/// Ceiling for the allow-list over-query retries in [`DiskAnnIndex::search_graph`],
/// as a multiple of the initial (already ef-boosted) search list. The list
/// doubles per retry, so the worst case is three extra graph searches —
/// bounded work that converts "the top-`l` overall contained too few allow-list
/// members" from a truncated result into a converging one.
const ALLOWLIST_OVERQUERY_CEILING_FACTOR: usize = 8;

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

	fn add(&mut self, key: Vec<u8>, value: Val, pending: PendingOperation) -> bool {
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

	/// Whether the bounded batch can still admit `keys` more captured keys totalling `bytes`,
	/// mirroring the reject condition in [`Self::add`]. Used to admit a legacy `!dr` entry and its
	/// folded sharded `!dw` counterpart as a single atomic pair, so the budget never splits the
	/// pair across compaction passes (which would reintroduce the phantom the fold prevents).
	fn has_room_for(&self, keys: usize, bytes: usize) -> bool {
		if self.captured_keys.len() + keys > DISKANN_COMPACTION_MAX_PENDING_KEYS {
			return false;
		}
		// An empty batch always admits its first entries (mirrors `add`'s `!is_empty()` guard) so a
		// single oversized pair can still make progress.
		self.captured_keys.is_empty()
			|| self.encoded_bytes + bytes <= DISKANN_COMPACTION_MAX_PENDING_BYTES
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
	fn add_authorized(&mut self, key: Vec<u8>, value: Val, pending: PendingOperation) {
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
		let mut builder = Builder::new(
			p.degree as usize,
			MaxDegree::default_slack(),
			p.l_build as usize,
			PruneKind::from_metric(metric),
		);
		builder.alpha(alpha);
		let config = builder.build()?;
		let provider =
			DiskAnnProvider::new(ikb, tb, format_version, cache, p.dimension as usize, metric);
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
			let node: crate::trees::diskann::DiskAnnNode = Default::default();
			ctx.tx.set_key(&ctx.ikb.new_dn_key(element_id), &node).await?;
			provider.set_entry_point(&ctx.provider_context, Some(element_id)).await?;
		} else {
			let strategy = DiskAnnStrategy::<T>::default();
			self.index.insert(&strategy, &ctx.provider_context, &element_id, values).await?;
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
	/// Doc-IDs admitted by the index-covered WHERE conjuncts (#548). When set,
	/// candidates outside the bitmap are rejected without any record fetch.
	allow_list: Option<&'a RoaringTreemap>,
	/// Optional condition filter applied before admitting candidates to the result builder.
	filter: &'a mut Option<DiskAnnTruthyDocumentFilter<'b>>,
	/// Shared result builder combining pending and graph candidates.
	builder: &'a mut KnnResultBuilder,
}

/// Which record-keyed pending layout a scanned range belongs to.
///
/// Both hold at most one entry per record; they differ in how the key is decoded
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

/// [`pending_record_id`] for compaction of a sharded entry, which maps the
/// record to a doc-ID; the rule is [`take_compacted_record_id`]. Only the
/// sharded layout can belong to an index on the table's doc-ID space: nothing
/// that writes the legacy layout knows that space.
async fn compacted_record_id(
	tx: &Transaction,
	key: &[u8],
	pending: &mut DiskAnnRecordPendingUpdate,
) -> Result<RecordIdKey> {
	take_compacted_record_id(
		tx,
		&mut pending.id,
		|| {
			let entry = DiskannRecordPendingShardKey::decode_key(key)?;
			let prefix = DiskannRecordPendingShardShardPrefix {
				ns: entry.ns,
				db: entry.db,
				tb: Cow::Borrowed(entry.tb.as_ref()),
				ix: entry.ix,
				shard: entry.shard,
			}
			.encode_bound()?;
			Ok(key.strip_prefix(&*prefix).map(|spelled| {
				(TableDocIds::new(entry.ns, entry.db, entry.tb.as_ref().clone()), spelled.to_vec())
			}))
		},
		|| Ok(DiskannRecordPendingShardKey::decode_key(key)?.id.into_owned()),
	)
	.await
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
		})
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

	/// Reads every DiskANN pending-state shard in one ordered batch.
	/// Reads the sharded `!dy` pending-state guard for every shard.
	///
	/// This guard tracks the sharded `!dw` layout only. It is deliberately separate from the legacy
	/// `!dp` guard so a pre-change node's compactor (which clears `!dp` on `!dr`-emptiness, unaware
	/// of `!dw`) can never mark a shard empty while a `!dw` entry still exists. Legacy `!dr`
	/// records are not reflected here; lookup scans the legacy range unconditionally instead.
	async fn read_pending_state(
		tx: &Transaction,
		ikb: &IndexKeyBase,
	) -> Result<PendingStateSnapshot> {
		let keys: Vec<_> =
			(0..DISKANN_PENDING_STATE_SHARDS).map(|shard| ikb.new_dy_key(shard)).collect();
		tx.get_many_key(keys, None).await
	}

	/// Marks the sharded `!dy` pending-state guard non-empty after writing a sharded `!dw` update.
	///
	/// Lookup and compaction skip a shard whose guard is `Empty`, so a transaction that writes a
	/// `!dw` entry must not commit while its shard's guard is `Empty`. A guard that is not
	/// `NonEmpty` is set `NonEmpty` with a compare-and-swap, which conflicts with a concurrent
	/// compaction step of that guard. A guard that already reads `NonEmpty` is left unwritten, so
	/// writers into one shard never conflict with one another over it. Compaction can still step
	/// that guard `NonEmpty → MaybeEmpty → Empty` over two passes while the writer is open:
	///
	/// * Where locked reads are shared ([`Transaction::shared_locked_reads`]), the guard is read
	///   locked. A step committed after this transaction's snapshot then fails its commit with a
	///   retryable conflict, and the retry finds the stepped guard and sets it `NonEmpty`. The
	///   refusal has to cover the first step, to `MaybeEmpty`, and not only the step to `Empty`:
	///   the pass that empties the shard checks its `!dw` range in a snapshot, and nothing
	///   validates that check at commit, so the pass cannot see a writer that commits while it is
	///   open. The first step always commits after the snapshot of a writer that read the guard
	///   `NonEmpty`, so refusing the writer over it covers that interleaving as well.
	/// * Elsewhere a locked read would make the writers into a shard conflict with one another, so
	///   the guard is read plain. A writer is then covered by the engine's own read validation
	///   where it has one, and otherwise only while at most one compaction pass steps its shard's
	///   guard: that pass leaves it `MaybeEmpty`, which lookup and compaction still scan.
	///
	/// Single-shot: within one transaction the read and the `putc` condition check see the same
	/// value, so retrying on `TransactionConditionNotMet` inside the transaction would reach the
	/// same outcome ([`Self::clear_pending_state_if_current`] has the same shape for the inverse
	/// direction).
	async fn mark_pending_non_empty(
		tx: &Transaction,
		ikb: &IndexKeyBase,
		id: &RecordIdKey,
	) -> Result<()> {
		let key = ikb.new_dy_key(Self::pending_state_shard(id));
		let current: Option<DiskAnnPendingState> = if tx.shared_locked_reads() {
			tx.getu_key(&key).await?
		} else {
			tx.get_key(&key, None).await?
		};
		if current.as_ref().is_some_and(|state| state.kind == DiskAnnPendingStateKind::NonEmpty) {
			return Ok(());
		}
		let next = DiskAnnPendingState {
			kind: DiskAnnPendingStateKind::NonEmpty,
			generation: current.as_ref().map_or(0, |state| state.generation).saturating_add(1),
		};
		tx.put_compare_key(&key, &next, current.as_ref()).await
	}

	/// Conditionally advances the given sharded `!dy` guard shards toward empty after compaction
	/// consumed their planned `!dw` ranges.
	///
	/// Only the shards in `shards` are stepped (NonEmpty → MaybeEmpty → Empty). Each `putc` is
	/// conditioned on the snapshot value observed during prepare, so a concurrent writer that
	/// bumped a shard between prepare and apply aborts that shard's clear without losing its
	/// update. A writer that found the guard `NonEmpty` and left it unwritten is covered from its
	/// own side instead; see [`Self::mark_pending_non_empty`].
	async fn clear_pending_state_if_current(
		tx: &Transaction,
		ikb: &IndexKeyBase,
		current: &[Option<DiskAnnPendingState>],
		shards: &[u16],
	) -> Result<bool> {
		let mut changed = false;
		for &shard in shards {
			let current = current.get(shard as usize).and_then(|state| state.as_ref());
			if current.is_some_and(|state| state.kind == DiskAnnPendingStateKind::Empty) {
				continue;
			}
			let key = ikb.new_dy_key(shard);
			let kind = match current.map(|state| state.kind) {
				Some(DiskAnnPendingStateKind::NonEmpty) => DiskAnnPendingStateKind::MaybeEmpty,
				Some(DiskAnnPendingStateKind::MaybeEmpty) | None => DiskAnnPendingStateKind::Empty,
				// Already filtered by the `continue` guard above; degrade to a no-op skip rather
				// than panic on the compaction path if that guard ever drifts from this match.
				Some(DiskAnnPendingStateKind::Empty) => continue,
			};
			let next = DiskAnnPendingState {
				kind,
				generation: current.map_or(0, |state| state.generation.saturating_add(1)),
			};
			match tx.put_compare_key(&key, &next, current).await {
				Ok(()) => changed = true,
				Err(e) if is_transaction_condition_not_met(&e) => return Ok(false),
				Err(e) => return Err(e),
			}
		}
		Ok(changed)
	}

	/// Returns whether a pending-update range holds no entries. Used to re-check emptiness inside
	/// the apply transaction before advancing pending state.
	async fn range_empty(
		env: &dyn IndexEnv,
		tx: &Transaction,
		rng: TypedRange<DiskAnnRecordPendingUpdate>,
	) -> Result<bool> {
		let mut cursor = tx.open_vals_cursor(rng, Direction::Forward, 0, None).await?;
		// The first non-empty batch is conclusive; we just need to know
		// whether *any* entry exists in the range.
		let batch = cursor.next_batch(1).await?;
		if !batch.is_empty() {
			return Ok(false);
		}
		drop(cursor);
		if env.is_done(None).await? {
			bail!(EngineError::QueryCancelled)
		}
		Ok(true)
	}

	/// Re-checks that each cleared shard's `!dw` range is empty before advancing the shard's `!dy`
	/// guard toward `Empty`.
	///
	/// Only the per-shard `!dw` range matters: the `!dy` guard tracks the sharded layout, and
	/// lookup scans the legacy `!dr` range unconditionally, so a shard's guard can clear as soon
	/// as its `!dw` range drains — independent of how far the one-time legacy `!dr` backlog has
	/// drained.
	async fn pending_shard_ranges_empty(
		env: &dyn IndexEnv,
		tx: &Transaction,
		ikb: &IndexKeyBase,
		shards: &[u16],
	) -> Result<bool> {
		for &shard in shards {
			if !Self::range_empty(env, tx, ikb.new_dw_shard_range(shard)?).await? {
				return Ok(false);
			}
		}
		Ok(true)
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
		let legacy = Self::take_legacy_pending(&tx, &self.ikb, id).await?;
		let mut pending = match (tx.get_key(&key, None).await?, legacy) {
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
				}
			}
			(None, None) => DiskAnnRecordPendingUpdate {
				doc_id: DiskAnnDocs::get_doc_id(&self.ikb, self.format_version, &tx, id).await?,
				old_vectors,
				new_vectors,
				id: None,
			},
		};
		// Every write stamps the exact id, whichever arm built the entry, so
		// compaction and lookup never have to recover it from a key that cannot
		// spell it. Stamping here rather than per arm keeps that true of an arm
		// added later.
		pending.id = Some(id.clone());
		tx.set_key(&key, &pending).await?;
		Self::mark_pending_non_empty(&tx, &self.ikb, id).await?;
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
		// in a transaction that read or set its `!dy` shard `NonEmpty`, and
		// `mark_pending_non_empty` keeps that transaction from committing once the guard is
		// stepped to `Empty`, within the engine limits it documents. Absent/Empty shards
		// therefore hold no committed entry.
		let mut cleared_shards = Vec::new();
		for (shard, state) in pending_state.iter().enumerate() {
			if state.as_ref().is_none_or(|s| s.kind == DiskAnnPendingStateKind::Empty) {
				continue;
			}
			let shard = shard as u16;
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
				let mut shard_update = shard_value
					.as_ref()
					.map(|v| DiskAnnRecordPendingUpdate::kv_decode_value(v, ()))
					.transpose()?;
				// Nothing writes the legacy layout any more, so its entries never carry the id and
				// the spelling above is whatever the key could express — decimalised, for an id
				// holding a number. Both entries name one record, so when the sharded counterpart
				// carries the exact id, that is the one the pair folds under.
				if let Some(exact) = shard_update.as_mut().and_then(|u| u.id.take()) {
					id = exact;
				}
				let legacy_op = Self::record_pending_to_operation(id.clone(), legacy_update);
				let shard_entry = match (shard_key, shard_value, shard_update) {
					(Some(shard_key), Some(shard_value), Some(update)) => {
						let op = Self::record_pending_to_operation(id.clone(), update);
						Some((shard_key, shard_value, op))
					}
					_ => None,
				};
				// Admit the legacy entry and its counterpart atomically: deferring only one of the
				// pair to a later pass would reintroduce the cross-pass split the fold prevents.
				let pair_keys = 1 + usize::from(shard_entry.is_some());
				let pair_bytes = legacy_key.len()
					+ legacy_value.len()
					+ shard_entry.as_ref().map_or(0, |(k, v, _)| k.len() + v.len());
				if !builder.has_room_for(pair_keys, pair_bytes) {
					return Ok(false);
				}
				// The pair is authorized; capture both halves unconditionally so the byte guard can
				// never admit one and reject the other (which would orphan the sharded half). The
				// plan needs each entry's exact bytes for its conditional delete, so the copy is
				// made here rather than for the whole batch up front: the caps above stop a
				// part-read batch as a matter of course, and what the loop never reaches is never
				// copied.
				builder.add_authorized(legacy_key.to_vec(), legacy_value.to_vec(), legacy_op);
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
				let mut pending = DiskAnnRecordPendingUpdate::kv_decode_value(value, ())?;
				let id = compacted_record_id(tx, key, &mut pending).await?;
				let pending = Self::record_pending_to_operation(id, pending);
				// Copied per entry rather than per batch, for the reason given in
				// `collect_legacy_record_pending`.
				if !builder.add(key.to_vec(), value.to_vec(), pending) {
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
	///   * `Ok(true)` — mutations were applied and the transaction has been committed.
	///   * `Ok(false)` — the plan was stale (generation drift or captured-key mismatch); the
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
	) -> Result<bool> {
		let DiskAnnCompactionPlan {
			generation,
			pending_state,
			captured_keys,
			pending,
			cleared_shards,
			has_more: _,
		} = plan;
		let tx = env.tx();
		if captured_keys.is_empty() {
			// No graph mutations possible; the only KV writes here are to the !dy
			// guard shards. Commit (or cancel) without touching the graph lock or cache.
			if !cleared_shards.is_empty()
				&& Self::pending_shard_ranges_empty(env, &tx, &self.ikb, &cleared_shards).await?
				&& Self::clear_pending_state_if_current(
					&tx,
					&self.ikb,
					&pending_state,
					&cleared_shards,
				)
				.await?
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
			match tx.del_compare(Key::from(&captured.key), Some(&captured.value)).await {
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
			let docs = DiskAnnDocs::with_format_version(self.ikb.clone(), self.format_version);
			let provider_context = graph.index.provider().context(Arc::clone(&tx));
			let diskann_ctx = self.new_diskann_context(env, provider_context);
			for pending in pending {
				self.apply_pending_operation(&diskann_ctx, &docs, &mut graph, pending).await?;
			}
			if !cleared_shards.is_empty()
				&& Self::pending_shard_ranges_empty(env, &tx, &self.ikb, &cleared_shards).await?
			{
				Self::clear_pending_state_if_current(
					&tx,
					&self.ikb,
					&pending_state,
					&cleared_shards,
				)
				.await?;
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
			.remove_index(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.ikb.index(),
				self.format_version,
			)
			.await;
	}

	/// Applies one coalesced pending operation to document mappings and the DiskANN graph.
	async fn apply_pending_operation(
		&self,
		ctx: &DiskAnnContext<'_>,
		docs: &DiskAnnDocs,
		graph: &mut DiskAnnGraph,
		pending: PendingOperation,
	) -> Result<()> {
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
						Some(record) => docs.resolve(ctx.env, record).await?,
						None => doc_id,
					};
					for vector in pending.new_vectors {
						self.vec_docs
							.insert(ctx, Vector::from(vector), insert_doc_id, graph)
							.await?;
					}
				}
			}
			VectorId::RecordKey(id) => {
				if !pending.new_vectors.is_empty() {
					let doc_id = docs.resolve(ctx.env, &id).await?;
					for vector in pending.new_vectors {
						self.vec_docs.insert(ctx, Vector::from(vector), doc_id, graph).await?;
					}
				}
			}
		}
		Ok(())
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
			)
		});
		let vector = Vector::try_from_vector(self.vector_type, pt)?;
		vector.check_dimension(self.dim)?;
		let search = DiskAnnSearch::new(vector, k, ef)?;
		let graph = self.graph.read().await;
		let provider_context = graph.index.provider().context(env.tx());
		let ctx = self.new_diskann_context(env, provider_context);
		let mut builder = KnnResultBuilder::new(k);
		// `pending_state` reflects only the sharded `!dy` guard; legacy `!dr` records are not
		// tracked there, so we always scan pendings. `search_pendings` sweeps the legacy `!dr`
		// range unconditionally and the per-shard `!dw` ranges of the non-empty `!dy` shards;
		// when nothing is pending the cost is a single empty legacy range probe.
		let pending_docs = self
			.search_pendings(
				&ctx,
				stk,
				&search,
				&mut filter,
				allow_list,
				&mut builder,
				&pending_state,
			)
			.await?;
		self.search_graph(
			&ctx,
			stk,
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
		// #548: with an allow-list, this is a *gated post-filter*, not filtered
		// traversal — `graph.search` walks unaware of the bitmap and returns the
		// top-`l` candidates overall, so a selective allow-list can leave fewer
		// than k admissible docs in that list even when the graph holds plenty
		// (HNSW, by contrast, gates admission during traversal and keeps walking
		// until `ef` members are found). Compensate by over-querying: retry with
		// a doubled search list until k allow-list members have been admitted,
		// the graph is exhausted, the builder's distance gate closes (every
		// remaining candidate is farther than the k already held), or the retry
		// ceiling is reached. `admitted` guards retries against re-adding docs
		// the builder already holds. The pure-residual path (no allow-list)
		// keeps its pre-existing single-pass semantics.
		let mut l = state.search.l;
		let retry_ceiling = l.saturating_mul(ALLOWLIST_OVERQUERY_CEILING_FACTOR);
		let mut admitted: HashSet<u64> = HashSet::default();
		loop {
			let results = state.graph.search(ctx, &state.search.query, state.search.k, l).await?;
			// Fewer results than requested means the traversal ran dry: a wider
			// list cannot surface anything new.
			let graph_exhausted = results.len() < l;
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
				let mut docs =
					self.vec_docs.get_docs_by_element_batch(&ctx.tx, &candidates).await?;
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
							// of the result builder. A doc admitted by an earlier
							// over-query pass is already in the builder.
							if state.allow_list.is_some_and(|allow| !allow.contains(doc_id))
								|| admitted.contains(&doc_id)
							{
								continue;
							}
							let id = VectorId::DocId(doc_id);
							if let Some(filter) = state.filter.as_mut()
								&& !filter.check_vector_id_truthy(ctx, stk, id.clone()).await?
							{
								continue;
							}
							if state.allow_list.is_some() {
								admitted.insert(doc_id);
							}
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
			// Over-query only applies to allow-list-gated searches, and stops as
			// soon as retrying cannot improve the result set.
			if state.allow_list.is_none()
				|| admitted.len() >= state.search.k
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
	/// shard, so each range holds at most one entry per record and every entry read
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
	#[expect(clippy::too_many_arguments)]
	async fn search_pendings(
		&self,
		ctx: &DiskAnnContext<'_>,
		stk: &mut Stk,
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
		self.scan_pending_range(ctx, stk, &mut scan, rng, PendingLayout::Legacy, false).await?;
		// Phase 2: the sharded ranges of every shard that may hold data. A write
		// commits its `!dw` key only in a transaction that read or set its `!dy`
		// shard `NonEmpty`, and `mark_pending_non_empty` keeps that transaction from
		// committing once the guard is stepped to `Empty`, within the engine limits
		// it documents. A shard that is absent or confirmed `Empty` therefore holds
		// no committed `!dw` entry. Skipping those shards keeps the scan proportional
		// to the un-compacted backlog of the few active shards rather than the whole
		// index.
		for shard in 0..DISKANN_PENDING_STATE_SHARDS {
			let state = pending_state.get(usize::from(shard)).and_then(|state| state.as_ref());
			if state.is_none_or(|state| state.kind == DiskAnnPendingStateKind::Empty) {
				continue;
			}
			let probe_legacy = scan.legacy_shards & (1u32 << u32::from(shard)) != 0;
			let rng = self.ikb.new_dw_shard_range(shard)?;
			self.scan_pending_range(ctx, stk, &mut scan, rng, PendingLayout::Sharded, probe_legacy)
				.await?;
		}
		self.score_pending_batch(ctx, stk, &mut scan).await?;
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
		stk: &mut Stk,
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
				self.score_pending_batch(ctx, stk, scan).await?;
				self.legacy_owned_rows(ctx, &read, &scan.pending).await?
			} else {
				Vec::new()
			};
			for (row, (key, value)) in read.iter().enumerate() {
				let entry_bytes = key.len() + value.len();
				// Charged before the checkpoint, so an entry the scan has read is
				// accounted — and reported, if it crosses — before an expired
				// deadline can bail out of the scan.
				scan.pending.charge_entry(entry_bytes);
				if ctx.env.is_done(Some(scan.pending.entries())).await? {
					bail!(EngineError::QueryCancelled)
				}
				let mut pending = DiskAnnRecordPendingUpdate::kv_decode_value(value, ())?;
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
					PendingLayout::Sharded if probe_legacy => {
						if legacy_owned[row] {
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
				if scan.pending.rollover_required(entry_bytes) {
					self.score_pending_batch(ctx, stk, scan).await?;
				}
				scan.pending.push(
					VectorId::RecordKey(Arc::new(id)),
					pending.new_vectors,
					entry_bytes,
				);
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
		stk: &mut Stk,
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
				Some(filter) => filter.check_vector_id_truthy(ctx, stk, id.clone()).await?,
				None => true,
			};
			if truthy {
				for vector in vectors {
					let vector = Vector::from(vector);
					let d = self.distance.calculate(&scan.search.pt, &vector);
					if scan.builder.check_add(d)
						&& let Some(evicted_id) = scan.builder.add_vector_id_result(d, id.clone())
						&& let Some(filter) = scan.filter.as_mut()
					{
						filter.expire(&evicted_id);
					}
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
	use surrealdb_datastore::is_retryable_transaction_conflict;
	use surrealdb_kvs::TransactionType;
	#[cfg(feature = "kv-rocksdb")]
	use temp_dir::TempDir;

	use super::*;
	use crate::catalog::{DOC_IDS_FORMAT_VERSION, DatabaseId, IndexId, NamespaceId};
	use crate::docids::TableDocIds;
	use crate::test_env::{CancelAtCheckpoint, TestIndexEnv, TestIndexStore};
	use crate::trees::diskann::cache::DiskAnnCache;
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

	fn diskann_any_pending_state_maybe_empty(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().flatten().any(|state| state.kind == DiskAnnPendingStateKind::MaybeEmpty)
	}

	fn diskann_pending_states_require_scan(states: &[Option<DiskAnnPendingState>]) -> bool {
		states.iter().any(|state| {
			state.as_ref().is_none_or(|state| state.kind != DiskAnnPendingStateKind::Empty)
		})
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
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async { index.knn_search(&ctx, stk, &query, k, 8, None, None).await })
			.finish()
			.await?;
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
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async { index.knn_search(&ctx, stk, &query, 1, 8, None, None).await })
			.finish()
			.await?;
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
		let mut stack = reblessive::tree::TreeStack::new();
		let res = stack
			.enter(|stk| async {
				index.knn_search(&ctx, stk, &query, k, 8, None, allow_list).await
			})
			.finish()
			.await?;
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
			.get_doc_set(
				(ikb.ns(), ikb.db(), TableId(4), ikb.index(), DOC_IDS_FORMAT_VERSION),
				element_id,
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
			tx.get_key(&dw_key(&ikb, &id), None).await?.unwrap();
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
		let mut stack = reblessive::tree::TreeStack::new();
		let err = stack
			.enter(|stk| async { index.knn_search(&ctx, stk, &query, 4, 8, None, None).await })
			.finish()
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
		let mut stack = reblessive::tree::TreeStack::new();
		let err = stack
			.enter(|stk| async { index.knn_search(&ctx, stk, &query, 1, 8, None, None).await })
			.finish()
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
			// apply_compaction commits the tx internally on Ok(true).
			assert!(index.apply_compaction(&ctx, plan).await?);
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_pending_states_require_scan(&states));
			assert!(diskann_any_pending_state_maybe_empty(&states));
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}

		assert!(compact_once(&index, &ds, &ikb).await?);

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let states = diskann_pending_states(&ctx.tx(), &ikb).await?;
			assert!(diskann_all_pending_states_empty(&states));
			assert!(ctx.tx().get_key::<_>(&dw_key(&ikb, &id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}
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
			// apply_compaction now cancels the tx itself when it returns Ok(false).
			assert!(!index.apply_compaction(&ctx, plan).await?);
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
			// apply_compaction commits the tx internally on Ok(true).
			assert!(index.apply_compaction(&ctx, plan).await?);
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

	/// A writer that finds its shard's `!dy` guard `NonEmpty` leaves the guard
	/// alone. Compaction passes that step the shard to `Empty` while the writer
	/// is still open must not hide the writer's pending once it commits: search
	/// and compaction both skip a shard whose guard is `Empty`. The writer's
	/// commit may instead be refused as retryable, in which case the retried
	/// write must be the one that stays visible.
	#[tokio::test]
	async fn diskann_pending_written_while_its_shard_empties_stays_visible() -> Result<()> {
		let ds = TestIndexStore::new().await;
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
		match writer.tx().commit().await {
			Ok(()) => {}
			// Retried as a client does, in a fresh transaction.
			Err(e) if is_retryable_transaction_conflict(&e) => {
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				index
					.index(
						&ctx,
						&RecordIdKey::Number(33),
						None,
						Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
					)
					.await?;
				ctx.tx().commit().await?;
			}
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
		Ok(())
	}

	/// A writer that found its shard's guard `NonEmpty` and commits while the
	/// pass that empties the shard is open, after that pass has checked the
	/// shard's `!dw` range but before it commits, must not end up under an
	/// `Empty` guard. The pass's range check never sees the writer's pending,
	/// so one of the two transactions has to be refused.
	#[tokio::test]
	async fn diskann_writer_committing_inside_the_emptying_pass_stays_visible() -> Result<()> {
		let ds = TestIndexStore::new().await;
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
		// The first pass drains record 1 and steps the guard to `MaybeEmpty`.
		assert!(compact_once(&index, &ds, &ikb).await?);
		// The emptying pass plans and opens its apply before the writer commits.
		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = DiskAnnIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let emptying = new_ctx(&ds, TransactionType::Write).await;
		match writer.tx().commit().await {
			Ok(()) => {}
			// Retried as a client does, in a fresh transaction.
			Err(e) if is_retryable_transaction_conflict(&e) => {
				let ctx = new_ctx(&ds, TransactionType::Write).await;
				index
					.index(
						&ctx,
						&RecordIdKey::Number(33),
						None,
						Some(f32_content(&[33.0, 0.0, 0.0, 0.0])),
					)
					.await?;
				ctx.tx().commit().await?;
			}
			Err(e) => return Err(e),
		}
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
		Ok(())
	}

	/// Overlapping writers into a shard whose `!dy` guard already reads
	/// `NonEmpty` all commit: each takes a locked read of the guard and leaves
	/// it unwritten, and locked reads do not conflict with one another.
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
			first.tx().shared_locked_reads(),
			"the store must share locked reads for the writers to take the locked path"
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
		assert!(builder.has_room_for(2, 2 + half + half), "empty batch admits the oversized pair");
		builder.add_authorized(vec![0u8; 1], vec![0u8; half], op(1));
		builder.add_authorized(vec![1u8; 1], vec![0u8; half], op(2));
		assert_eq!(builder.captured_keys.len(), 2, "both halves captured");

		// Contrast: plain `add` rejects the second half once the batch is non-empty and over the
		// byte budget — the orphaning path this fix closes.
		let mut naive = PendingPlanBuilder::new(None, empty_state);
		assert!(naive.add(vec![0u8; 1], vec![0u8; half], op(1)));
		assert!(!naive.add(vec![1u8; 1], vec![0u8; half], op(2)));
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

	/// Regression for the #7318 class of bug: two compactors race on the same `!dr` plan,
	/// the late one's commit conflicts on `!dg` after it has already mutated the shared
	/// cache during the apply phase. `apply_compaction` must clear the per-index cache
	/// before returning the error so subsequent KNN searches can't observe element ids
	/// from the rolled-back tx.
	#[tokio::test]
	async fn diskann_failed_compaction_clears_cache_and_keeps_knn_working() -> Result<()> {
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
		let cache_index = (ikb.ns(), ikb.db(), TableId(4), ikb.index(), DOC_IDS_FORMAT_VERSION);
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

	/// Compaction takes the record of a sharded entry whose value carries no id
	/// from the table's mappings, not from the id its key decodes to, which is
	/// decimalised once it holds a number and names no record.
	#[tokio::test]
	async fn compaction_names_the_mapped_record_of_an_entry_without_an_id() {
		use crate::key::schema::{DocKeyKey, DocLookupKey};
		let ikb = ikb();
		let id =
			RecordIdKey::Array(vec![Value::Number(Number::Int(42)), Value::from("intro")].into());
		let key = ikb
			.new_dw_key(DiskAnnIndex::pending_state_shard(&id), &id)
			.encode_key()
			.unwrap()
			.to_vec();
		let decoded = DiskannRecordPendingShardKey::decode_key(&key).unwrap().id.into_owned();
		assert!(
			!decoded.addresses_same_record(&id),
			"the premise: the key's id is not the record's"
		);
		let entry = || DiskAnnRecordPendingUpdate {
			doc_id: None,
			old_vectors: Vec::new(),
			new_vectors: Vec::new(),
			id: None,
		};

		let ds = TestIndexStore::new().await;
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		assert!(
			compacted_record_id(&tx, &key, &mut entry())
				.await
				.unwrap()
				.addresses_same_record(&decoded),
			"with no mapping at that spelling, the decoded id is all there is"
		);
		let tb = crate::val::TableName::from("tb");
		let (ns, db) = (ikb.ns(), ikb.db());
		tx.set_key(&DocLookupKey::new(ns, db, Cow::Borrowed(&tb), Cow::Borrowed(&id)), &7u64)
			.await
			.unwrap();
		tx.set_key(&DocKeyKey::new(ns, db, Cow::Borrowed(&tb), 7), &id).await.unwrap();
		let named = compacted_record_id(&tx, &key, &mut entry()).await.unwrap();
		assert!(
			named.addresses_same_record(&id),
			"the mapping names the record exactly: {named:?}"
		);
		tx.cancel().await.unwrap();
	}
}
