use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use ahash::HashMap;
use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Record;
use crate::catalog::providers::TableProvider;
use crate::doc::CursorDoc;
use crate::expr::{Cond, FlowResultExt as _};
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::knn::Ids64;
use crate::val::RecordId;

type FilterCache = HashMap<DocId, Option<(Arc<RecordId>, Arc<Record>)>>;

pub(super) struct HnswTruthyDocumentFilter {
	cond: Arc<Cond>,
	cache: FilterCache,
}

impl HnswTruthyDocumentFilter {
	pub(super) fn new(cond: Arc<Cond>) -> Self {
		Self {
			cond,
			cache: Default::default(),
		}
	}

	pub(super) fn convert_result(
		mut self,
		res: VecDeque<(DocId, f64)>,
	) -> VecDeque<KnnIteratorResult> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if let Some(e) = self.cache.remove(&doc_id)
				&& let Some((rid, value)) = e
			{
				result.push_back((rid, dist, Some(value)))
			}
		}
		result
	}

	pub(super) async fn check_any_doc_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		docs: &HnswDocs,
		stk: &mut Stk,
		doc_ids: Ids64,
	) -> Result<bool> {
		for doc_id in doc_ids.iter() {
			if self.check_doc_id_truthy(ctx, docs, stk, doc_id).await? {
				return Ok(true);
			}
		}
		Ok(false)
	}

	pub(super) async fn check_doc_id_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		docs: &HnswDocs,
		stk: &mut Stk,
		doc_id: DocId,
	) -> Result<bool> {
		match self.cache.entry(doc_id) {
			Entry::Occupied(e) => Ok(e.get().is_some()),
			Entry::Vacant(e) => {
				let Some(rid) = docs.get_thing(&ctx.tx, doc_id).await? else {
					// No record ID ? It is not truthy
					return Ok(false);
				};
				let rid = Arc::new(rid);
				// Is the record truthy?
				let record =
					Self::is_record_truthy(ctx, stk, self.cond.clone(), rid.clone()).await?;
				let truthy = record.is_some();
				// Store the result in the cache
				let entry = record.map(|r| (rid, r));
				e.insert(entry);
				// Return the result
				Ok(truthy)
			}
		}
	}

	pub(super) async fn check_record_is_truthy(
		&mut self,
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		rid: Arc<RecordId>,
	) -> Result<bool> {
		Ok(Self::is_record_truthy(ctx, stk, self.cond.clone(), rid).await?.is_some())
	}

	async fn is_record_truthy(
		ctx: &HnswContext<'_>,
		stk: &mut Stk,
		cond: Arc<Cond>,
		rid: Arc<RecordId>,
	) -> Result<Option<Arc<Record>>> {
		let val = ctx
			.tx
			.get_record(ctx.db.namespace_id, ctx.db.database_id, &rid.table, &rid.key, None)
			.await?;
		if val.data.as_ref().is_nullish() {
			return Ok(None);
		}
		let cursor_doc = CursorDoc {
			rid: Some(rid.clone()),
			ir: None,
			doc: val.into(),
			fields_computed: false,
		};
		let truthy = stk
			.run(|stk| cond.0.compute(stk, ctx.ctx, ctx.opt, Some(&cursor_doc)))
			.await
			.catch_return()?
			.is_truthy();
		if truthy {
			return Ok(Some(cursor_doc.doc.into_read_only()));
		}
		Ok(None)
	}

	pub(super) fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}

	pub(super) fn expires(&mut self, doc_ids: Ids64) {
		for doc_id in doc_ids.iter() {
			self.expire(doc_id);
		}
	}
}
