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
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::knn::Ids64;
use crate::val::RecordId;

struct FilterCacheEntry {
	record: Option<(Arc<RecordId>, Arc<Record>)>,
	truthy: bool,
}

impl FilterCacheEntry {
	fn convert_result(
		res: VecDeque<(DocId, f64)>,
		mut cache: HashMap<DocId, FilterCacheEntry>,
	) -> VecDeque<KnnIteratorResult> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if let Some(e) = cache.remove(&doc_id)
				&& e.truthy && let Some((rid, value)) = e.record
			{
				result.push_back((rid, dist, Some(value)))
			}
		}
		result
	}

	async fn build(
		search_ctx: &HnswCheckedSearchContext<'_>,
		stk: &mut Stk,
		rid: Option<RecordId>,
		cond: &Cond,
	) -> Result<Self> {
		if let Some(rid) = rid {
			let rid = Arc::new(rid);
			let val = search_ctx
				.tx
				.get_record(
					search_ctx.db.namespace_id,
					search_ctx.db.database_id,
					&rid.table,
					&rid.key,
					None,
				)
				.await?;
			if !val.data.as_ref().is_nullish() {
				let (record, truthy) = {
					let cursor_doc = CursorDoc {
						rid: Some(rid.clone()),
						ir: None,
						doc: val.into(),
						fields_computed: false,
					};
					let truthy = stk
						.run(|stk| {
							cond.0.compute(stk, search_ctx.ctx, search_ctx.opt, Some(&cursor_doc))
						})
						.await
						.catch_return()?
						.is_truthy();
					(cursor_doc.doc.into_read_only(), truthy)
				};
				return Ok(FilterCacheEntry {
					record: Some((rid, record)),
					truthy,
				});
			}
		}
		Ok(FilterCacheEntry {
			record: None,
			truthy: false,
		})
	}
}

pub(super) struct HnswTruthyDocumentFilter {
	cond: Arc<Cond>,
	cache: HashMap<DocId, FilterCacheEntry>,
}

impl HnswTruthyDocumentFilter {
	pub(in crate::idx) fn new(cond: Arc<Cond>) -> Self {
		Self {
			cond,
			cache: Default::default(),
		}
	}

	pub(super) fn convert_result(self, res: VecDeque<(DocId, f64)>) -> VecDeque<KnnIteratorResult> {
		FilterCacheEntry::convert_result(res, self.cache)
	}

	pub(in crate::idx) async fn check_any_doc_truthy(
		&mut self,
		search_ctx: &HnswCheckedSearchContext<'_>,
		stk: &mut Stk,
		doc_ids: Ids64,
	) -> Result<bool> {
		let mut res = false;
		for doc_id in doc_ids.iter() {
			if match self.cache.entry(doc_id) {
				Entry::Occupied(e) => e.get().truthy,
				Entry::Vacant(e) => {
					let rid = search_ctx.docs.get_thing(&search_ctx.tx, doc_id).await?;
					let ent =
						FilterCacheEntry::build(search_ctx, stk, rid, self.cond.as_ref()).await?;
					let truthy = ent.truthy;
					e.insert(ent);
					truthy
				}
			} {
				res = true;
			}
		}
		Ok(res)
	}

	pub(in crate::idx) fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}

	pub(in crate::idx) fn expires(&mut self, doc_ids: Ids64) {
		for doc_id in doc_ids.iter() {
			self.expire(doc_id);
		}
	}
}
