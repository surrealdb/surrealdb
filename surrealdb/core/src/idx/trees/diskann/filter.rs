//! Truthy-condition filtering for DiskANN KNN candidates.
//!
//! KNN lookup may include a SQL condition that must be evaluated against candidate records before
//! they are admitted to the result builder. This filter resolves DiskANN vector/document IDs back
//! to records, caches condition results while the lookup is running, and shares the DiskANN
//! doc-id-to-record-id cache for compact document IDs.

use std::sync::Arc;

use ahash::HashMap;
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
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::diskann::docs::DiskAnnDocs;
use crate::idx::trees::diskann::index::DiskAnnContext;
use crate::idx::trees::hnsw::VectorId;
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
	/// Table SELECT permission, resolved lazily on first candidate. All
	/// candidates from this filter share the indexed table, so this caches
	/// once for the lifetime of the filter and is reused across candidates.
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
	) -> Self {
		Self {
			opt,
			ikb,
			table_id,
			diskann_cache,
			pending_generation,
			cond,
			cache: Default::default(),
			permission: None,
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
