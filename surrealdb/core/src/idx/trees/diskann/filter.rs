//! Truthy-condition filtering for DiskANN KNN candidates.
//!
//! KNN lookup may include a SQL condition that must be evaluated against candidate records before
//! they are admitted to the result builder. This filter resolves DiskANN vector/document IDs back
//! to records, caches condition results while the lookup is running, and shares the DiskANN
//! doc-id-to-record-id cache for compact document IDs.

use std::sync::Arc;

use ahash::{HashMap, HashSet};
use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Record, TableId};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::permission::{
	CachedTableSelect, check_cached_table_select_for_doc, ensure_cached_table_select,
};
use crate::expr::{Cond, FlowResultExt as _};
use crate::idx::IndexKeyBase;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::diskann::docs::DiskAnnDocs;
use crate::idx::trees::diskann::index::DiskAnnContext;
use crate::idx::trees::hnsw::VectorId;
use crate::kvs::CachePolicy;
use crate::val::RecordId;

/// Query-local condition results for vector owners.
pub(super) type FilterCache = HashMap<VectorId, Option<(Arc<RecordId>, Arc<Record>)>>;

/// Per-query cache for condition checks applied to DiskANN candidate documents.
pub(super) struct DiskAnnTruthyDocumentFilter<'a> {
	/// Query options used when evaluating the SQL condition.
	opt: &'a Options,
	/// Index key identity used to resolve record IDs.
	ikb: IndexKeyBase,
	/// Stable table id used to scope DiskANN doc-id cache entries.
	table_id: TableId,
	/// Shared DiskANN cache used for compact document ID resolution.
	diskann_cache: DiskAnnCache,
	/// Pending generation captured at lookup start, used to reject stale doc-id cache entries.
	pending_generation: Option<u64>,
	/// Condition applied to candidate records.
	cond: Arc<Cond>,
	/// Query-local truthy/missing cache keyed by vector owner.
	cache: FilterCache,
	/// Table SELECT permission gate. Pre-seeded by the streaming executor
	/// (`KnnCondFilter::select_gate`), or resolved lazily on the first
	/// candidate when driven by the legacy executor. All candidates from
	/// this filter share the indexed table, so this caches once for the
	/// lifetime of the filter and is reused across candidates.
	permission: Option<CachedTableSelect>,
}

impl<'a> DiskAnnTruthyDocumentFilter<'a> {
	/// Creates a per-query truthy filter for one DiskANN lookup.
	pub(super) fn new(
		opt: &'a Options,
		ikb: IndexKeyBase,
		table_id: TableId,
		diskann_cache: DiskAnnCache,
		pending_generation: Option<u64>,
		cond: Arc<Cond>,
		select_gate: Option<CachedTableSelect>,
	) -> Self {
		Self {
			opt,
			ikb,
			table_id,
			diskann_cache,
			pending_generation,
			cond,
			cache: Default::default(),
			permission: select_gate,
		}
	}

	/// Resolves a candidate owner and evaluates the condition, caching the result for the query.
	pub(super) async fn check_vector_id_truthy(
		&mut self,
		ctx: &DiskAnnContext<'_>,
		stk: &mut Stk,
		id: VectorId,
	) -> Result<bool> {
		if let Some(cached) = self.cache.get(&id) {
			return Ok(cached.is_some());
		}
		let rid = match &id {
			VectorId::DocId(doc_id) => {
				let Some(rid) = DiskAnnDocs::get_thing_cached(
					&self.ikb,
					self.table_id,
					&self.diskann_cache,
					&ctx.tx,
					*doc_id,
					self.pending_generation,
				)
				.await?
				else {
					self.cache.insert(id, None);
					return Ok(false);
				};
				rid
			}
			VectorId::RecordKey(key) => {
				Arc::new(RecordId::new(self.ikb.table().clone(), key.as_ref().clone()))
			}
		};
		let permission =
			ensure_cached_table_select(ctx.ctx, self.opt, &ctx.tx, &self.ikb, &mut self.permission)
				.await?;
		let record = Self::is_record_truthy(
			ctx,
			self.opt,
			stk,
			Arc::clone(&self.cond),
			Arc::clone(&rid),
			permission,
		)
		.await?;
		let truthy = record.is_some();
		self.cache.insert(id, record.map(|record| (rid, record)));
		Ok(truthy)
	}

	/// Warms the transaction record cache for a batch of candidate vector ids,
	/// and records a not-found verdict for any that are missing.
	///
	/// The DiskANN graph search materializes its full candidate list before
	/// filtering, then evaluates the condition against each candidate's record,
	/// which [`Self::is_record_truthy`] fetches one at a time. This resolves the
	/// given ids to record ids — compact `DocId`s in one batch, `RecordKey`s
	/// (pending updates) directly — and issues a single batched multi-get
	/// ([`TableProvider::get_records`], cache-aware with a native multi-get on
	/// misses) so those later per-candidate `get_record` lookups hit the
	/// transaction cache instead of each making an individual round-trip.
	///
	/// Ids already present in the filter cache are skipped — their verdict, and
	/// record, are already known. The condition is never evaluated here; the
	/// evaluation loop runs afterwards unchanged, with `get_record` reading the
	/// warmed cache. `get_records` does not cache *misses*, so a genuinely missing
	/// or deleted candidate would otherwise still cost an individual `get_record`;
	/// we therefore mark such ids not-found (`None`) in the filter cache directly.
	/// That is safe and result-preserving: a nullish record is never truthy and
	/// [`Self::is_record_truthy`] returns for it before the SELECT-permission /
	/// condition checks, so pre-marking changes neither the verdict nor the
	/// permission-before-cond ordering.
	pub(super) async fn prefetch_records(
		&mut self,
		ctx: &DiskAnnContext<'_>,
		ids: &[VectorId],
	) -> Result<()> {
		// Partition the not-yet-cached ids, keeping each paired (via the parallel
		// `ids_out` / `rids` vecs) with the record id it resolves to. Compact
		// `DocId`s are resolved in one batch; `RecordKey`s map straight to a record
		// id. De-duplicate by `VectorId` so a repeated candidate is collected —
		// and fetched — at most once.
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
			let resolved = DiskAnnDocs::get_things_batch(
				&self.ikb,
				self.table_id,
				&self.diskann_cache,
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

	/// Evaluates the SQL condition against a fetched record and returns the record on success.
	async fn is_record_truthy(
		ctx: &DiskAnnContext<'_>,
		opt: &Options,
		stk: &mut Stk,
		cond: Arc<Cond>,
		rid: Arc<RecordId>,
		permission: &CachedTableSelect,
	) -> Result<Option<Arc<Record>>> {
		let val = ctx.tx.get_record(ctx.ikb.ns(), ctx.ikb.db(), &rid.table, &rid.key, None).await?;
		if val.data.is_nullish() {
			return Ok(None);
		}
		let cursor_doc = CursorDoc {
			rid: Some(Arc::clone(&rid)),
			ir: None,
			doc: val.into(),
			fields_computed: false,
		};
		// SECURITY: apply the table's SELECT permission BEFORE evaluating the
		// caller-controlled WHERE condition. The cond pre-filter runs inside
		// the ANN search and influences which candidates are admitted to the
		// topK; without this guard a caller can probe restricted fields by
		// crafting a WHERE on them and observing the resulting count / order /
		// timing.
		if !check_cached_table_select_for_doc(stk, ctx.ctx, opt, permission, &cursor_doc).await? {
			return Ok(None);
		}
		let truthy = stk
			.run(|stk| cond.0.compute(stk, ctx.ctx, opt, Some(&cursor_doc)))
			.await
			.catch_return()?
			.is_truthy();
		if truthy {
			return Ok(Some(cursor_doc.doc.into_read_only()));
		}
		Ok(None)
	}

	/// Drops a cached condition result after its candidate has been evicted from the result
	/// builder.
	pub(super) fn expire(&mut self, id: &VectorId) {
		self.cache.remove(id);
	}

	/// Returns the query-local cache so final materialization can reuse already fetched records.
	pub(super) fn release(self) -> FilterCache {
		self.cache
	}
}
