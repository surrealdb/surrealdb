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
use crate::idx::trees::hnsw::VectorId;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::knn::Ids64;
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
	/// Query options for condition evaluation.
	opt: &'a Options,
	/// Key base for record lookups.
	ikb: IndexKeyBase,
	/// Stable table id used to scope HNSW doc-id cache entries.
	table_id: TableId,
	/// Shared HNSW cache used for compact document ID resolution.
	vector_cache: VectorCache,
	/// The filter condition to evaluate.
	cond: Arc<Cond>,
	/// Pending generation captured at lookup start, used to reject stale doc-id cache entries.
	pending_generation: Option<u64>,
	/// Cache of previously evaluated filter results.
	cache: FilterCache,
	/// Table SELECT permission, resolved lazily on first candidate. All
	/// candidates from this filter share the indexed table, so this caches
	/// once for the lifetime of the filter and is reused across candidates.
	permission: Option<CachedTableSelect>,
}

impl<'a> HnswTruthyDocumentFilter<'a> {
	pub(super) fn new(
		opt: &'a Options,
		ikb: IndexKeyBase,
		table_id: TableId,
		vector_cache: VectorCache,
		cond: Arc<Cond>,
		pending_generation: Option<u64>,
	) -> Self {
		Self {
			opt,
			ikb,
			table_id,
			vector_cache,
			cond,
			pending_generation,
			cache: Default::default(),
			permission: None,
		}
	}

	/// Returns `true` if any of the given document IDs satisfies the filter condition.
	pub(super) async fn check_any_doc_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		doc_ids: Ids64,
	) -> Result<bool> {
		for doc_id in doc_ids.iter() {
			if self.check_vector_id_truthy(ctx, stk, VectorId::DocId(doc_id)).await? {
				return Ok(true);
			}
		}
		Ok(false)
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
		self.cache.insert(id, record.map(|r| (rid, r)));
		Ok(truthy)
	}

	/// Fetches a record and evaluates the filter condition against it.
	/// Returns the record data if truthy, or `None` otherwise.
	async fn is_record_truthy(
		ctx: &HnswContext<'_>,
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
		// caller-controlled WHERE condition. Without this guard the cond is
		// evaluated against records the caller cannot see, so result counts /
		// ordering / pre-decode hits can leak field values from restricted
		// rows.
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

	/// Remove a vector id that has been evicted from the knn result
	pub(super) fn expire(&mut self, id: &VectorId) {
		self.cache.remove(id);
	}

	/// Remove a list of vector ids that have been evicted from the knn result
	pub(super) fn expires(&mut self, ids: &[VectorId]) {
		for id in ids {
			self.cache.remove(id);
		}
	}

	/// Consumes the filter and returns the accumulated result cache.
	pub(super) fn release(self) -> FilterCache {
		self.cache
	}
}
