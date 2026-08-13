use std::sync::Arc;

use ahash::{HashMap, HashSet};
use anyhow::Result;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use surrealdb_catalog::providers::CachePolicy;

use crate::IndexKeyBase;
use crate::catalog::providers::TableProvider;
use crate::catalog::{Record, TableId};
use crate::docids::DocId;
use crate::trees::gate::{
	CachedTableSelect, CandidateCondition, CandidateFetchCounter, check_cached_table_select_for_doc,
};
use crate::trees::hnsw::VectorId;
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::docs::HnswDocs;
use crate::trees::hnsw::index::HnswContext;
use crate::trees::knn::Ids64;
use crate::val::RecordId;

/// Cache of evaluated filter results, mapping vector IDs to their record data
/// (if truthy) or `None` (if not truthy or not found).
pub(super) type FilterCache = HashMap<VectorId, Option<(Arc<RecordId>, Arc<Record>)>>;

/// Filter that evaluates a `WHERE` condition against documents during KNN search.
///
/// Uses [`HnswDocs`] static methods to look up records directly from the
/// key-value store (without holding a lock on `HnswDocs`), and caches
/// evaluation results to avoid redundant record lookups and condition
/// evaluations across candidates.
pub(super) struct HnswTruthyDocumentFilter<'a> {
	/// Key base for record lookups.
	ikb: IndexKeyBase,
	/// Stable table id used to scope HNSW doc-id cache entries.
	table_id: TableId,
	/// Shared HNSW cache used for compact document ID resolution.
	vector_cache: VectorCache,
	/// The filter condition to evaluate. `None` runs the filter in
	/// permission-only mode: a candidate the permission gate admits is truthy
	/// by definition.
	cond: Option<Arc<dyn CandidateCondition + 'a>>,
	/// Pending generation captured at lookup start, used to reject stale doc-id cache entries.
	pending_generation: Option<u64>,
	/// Cache of previously evaluated filter results.
	cache: FilterCache,
	/// Table SELECT permission gate, supplied by the executor driving the
	/// search (`KnnCondFilter::select_gate`). All candidates from this filter
	/// share the indexed table, so one resolution serves the lifetime of the
	/// filter.
	permission: CachedTableSelect<'a>,
	/// Counter for the candidates this filter fetches and evaluates
	/// in-traversal. `None` when the driving executor reports no metrics.
	metrics: Option<Arc<dyn CandidateFetchCounter>>,
}

impl<'a> HnswTruthyDocumentFilter<'a> {
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		vector_cache: VectorCache,
		cond: Option<Arc<dyn CandidateCondition + 'a>>,
		pending_generation: Option<u64>,
		select_gate: CachedTableSelect<'a>,
		metrics: Option<Arc<dyn CandidateFetchCounter>>,
	) -> Self {
		Self {
			ikb,
			table_id,
			vector_cache,
			cond,
			pending_generation,
			cache: Default::default(),
			permission: select_gate,
			metrics,
		}
	}

	/// Returns `true` if any of the given document IDs satisfies the filter condition.
	///
	/// When an `allow_list` is provided (#548), document IDs outside it are
	/// skipped without fetching their record: an element must be admitted by
	/// a document that itself passes the bitmap, never by a sibling document
	/// that happens to share the same vector. Docs in `pending_docs` are
	/// skipped for the same reason — the pending scan is their source of
	/// truth and result materialization suppresses them, so they must not
	/// admit their (stale) graph element either.
	pub(super) async fn check_any_doc_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		doc_ids: Ids64,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<bool> {
		for doc_id in doc_ids.iter() {
			if pending_docs.is_some_and(|pending| pending.contains(doc_id)) {
				continue;
			}
			if let Some(allow) = allow_list
				&& !allow.contains(doc_id)
			{
				continue;
			}
			if self.check_vector_id_truthy(ctx, stk, VectorId::DocId(doc_id)).await? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	/// Warms the transaction record cache for a batch of candidate document IDs,
	/// and records a not-found verdict for any that are missing.
	///
	/// During a filtered KNN search the condition is evaluated against the full
	/// record of every visited candidate, which [`Self::is_record_truthy`]
	/// fetches one at a time. This resolves the given document IDs to record IDs
	/// and issues a single batched multi-get ([`TableProvider::get_records`],
	/// cache-aware with a native multi-get on misses) so those later
	/// per-candidate `get_record` lookups hit the transaction cache instead of
	/// each making an individual round-trip.
	///
	/// Document IDs already present in the filter cache are skipped — their
	/// verdict, and record, are already known. The condition is never evaluated
	/// here; the evaluation loop runs afterwards unchanged, with `get_record`
	/// reading the warmed cache. `get_records` does not cache *misses*, so a
	/// genuinely missing or deleted candidate would otherwise still cost an
	/// individual `get_record`; we therefore mark such ids not-found (`None`) in
	/// the filter cache directly. That is safe and result-preserving: a nullish
	/// record is never truthy and [`Self::is_record_truthy`] returns for it before
	/// the SELECT-permission / condition checks, so pre-marking changes neither
	/// the verdict nor the permission-before-cond ordering.
	pub(super) async fn prefetch_records(
		&mut self,
		ctx: &HnswContext<'_>,
		ids: &[VectorId],
	) -> Result<()> {
		// Partition the not-yet-cached ids, keeping each paired (via the parallel
		// `ids_out` / `rids` vecs) with the record id it resolves to. Compact
		// `DocId`s (committed graph) are resolved in one batch; `RecordKey`s
		// (pending updates) map straight to a record id. De-duplicate by
		// `VectorId` so a repeated candidate is collected — and fetched — once.
		let mut seen: HashSet<VectorId> = HashSet::default();
		let mut doc_ids: Vec<DocId> = Vec::new();
		let mut doc_id_ids: Vec<VectorId> = Vec::new();
		let mut ids_out: Vec<VectorId> = Vec::new();
		let mut rids: Vec<RecordId> = Vec::new();
		for id in ids {
			if self.cache.contains_key(id) || !seen.insert(id.clone()) {
				continue;
			}
			match id {
				VectorId::DocId(doc_id) => {
					doc_ids.push(*doc_id);
					doc_id_ids.push(id.clone());
				}
				VectorId::RecordKey(key) => {
					ids_out.push(id.clone());
					rids.push(RecordId::new(self.ikb.table().clone(), key.as_ref().clone()));
				}
			}
		}
		if !doc_ids.is_empty() {
			let resolved = HnswDocs::get_things_batch(
				&self.ikb,
				self.table_id,
				&self.vector_cache,
				&ctx.tx,
				&doc_ids,
				self.pending_generation,
			)
			.await?;
			// `resolved[i]` corresponds to `doc_id_ids[i]`; a `None` means the doc
			// id has no record-id mapping (deleted) — mark it not-found.
			for (id, rid) in doc_id_ids.into_iter().zip(resolved) {
				match rid {
					Some(rid) => {
						ids_out.push(id);
						rids.push(rid.as_ref().clone());
					}
					None => {
						self.cache.insert(id, None);
					}
				}
			}
		}
		if rids.is_empty() {
			return Ok(());
		}
		// Warm the transaction record cache with a single multi-get, then mark any
		// missing/nullish record not-found (`get_records` does not cache misses)
		// so the eval loop skips the otherwise-redundant `get_record` for it.
		// `records` is returned in `rids` order, which matches `ids_out`.
		let records = ctx
			.tx
			.get_records(ctx.ikb.ns(), ctx.ikb.db(), &rids, None, CachePolicy::ReadWrite)
			.await?;
		for (id, record) in ids_out.into_iter().zip(records) {
			if record.data.is_nullish() {
				self.cache.insert(id, None);
			}
		}
		Ok(())
	}

	/// Checks whether the document identified by a vector ID satisfies the filter condition.
	///
	/// Results are cached so repeated checks for the same vector ID are free.
	pub(super) async fn check_vector_id_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		id: VectorId,
	) -> Result<bool> {
		if let Some(cached) = self.cache.get(&id) {
			return Ok(cached.is_some());
		}
		// Resolve the RecordId
		let rid = match &id {
			VectorId::DocId(doc_id) => {
				let Some(rid) = HnswDocs::get_thing_cached(
					&self.ikb,
					self.table_id,
					&self.vector_cache,
					&ctx.tx,
					*doc_id,
					self.pending_generation,
				)
				.await?
				else {
					self.cache.insert(id, None);
					// No record ID ? It is not truthy
					return Ok(false);
				};
				rid
			}
			VectorId::RecordKey(key) => {
				Arc::new(RecordId::new(self.ikb.table().clone(), key.as_ref().clone()))
			}
		};
		// One in-traversal candidate record evaluation (filter-cache misses
		// only — a cached verdict returned above involved no new fetch). This
		// counts evaluations, not KV reads: the record read below may be a
		// transaction-cache hit (prefetch batches warm it), and the candidate
		// is counted even if that read subsequently errors.
		if let Some(metrics) = &self.metrics {
			metrics.record_fetch();
		}
		let record = Self::is_record_truthy(
			ctx,
			stk,
			self.cond.as_deref(),
			Arc::clone(&rid),
			&self.permission,
		)
		.await?;
		let truthy = record.is_some();
		self.cache.insert(id, record.map(|r| (rid, r)));
		Ok(truthy)
	}

	/// Fetches a record and evaluates the filter condition against it.
	/// Returns the record data if truthy, or `None` otherwise.
	///
	/// A `None` condition means permission-only mode: a record that passes the
	/// SELECT-permission gate is truthy by definition.
	async fn is_record_truthy(
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		cond: Option<&dyn CandidateCondition>,
		rid: Arc<RecordId>,
		permission: &CachedTableSelect<'_>,
	) -> Result<Option<Arc<Record>>> {
		let val = ctx.tx.get_record(ctx.ikb.ns(), ctx.ikb.db(), &rid.table, &rid.key, None).await?;
		if val.data.is_nullish() {
			return Ok(None);
		}
		// SECURITY: apply the table's SELECT permission BEFORE evaluating the
		// caller-controlled WHERE condition. Without this guard the cond is
		// evaluated against records the caller cannot see, so result counts /
		// ordering / pre-decode hits can leak field values from restricted
		// rows.
		if !check_cached_table_select_for_doc(stk, permission, &rid, &val).await? {
			return Ok(None);
		}
		let truthy = match cond {
			Some(cond) => cond.matches(stk, &rid, &val).await?,
			None => true,
		};
		if truthy {
			return Ok(Some(val));
		}
		Ok(None)
	}

	/// Remove a vector id that has been evicted from the knn result
	pub(super) fn expire(&mut self, id: &VectorId) {
		self.cache.remove(id);
	}

	/// Consumes the filter and returns the accumulated result cache.
	pub(super) fn release(self) -> FilterCache {
		self.cache
	}
}
