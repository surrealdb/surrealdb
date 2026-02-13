use std::collections::VecDeque;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use ahash::HashMap;
use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseDefinition, Record};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Cond, FlowResultExt as _};
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::knn::Ids64;
use crate::kvs::Transaction;
use crate::val::RecordId;

pub enum HnswConditionChecker<'a> {
	Hnsw(HnswChecker),
	HnswCondition(HnswCondChecker<'a>),
}

impl<'a> HnswConditionChecker<'a> {
	pub(crate) fn new() -> Self {
		Self::Hnsw(HnswChecker {})
	}

	pub(in crate::idx) fn new_cond(
		ctx: &'a FrozenContext,
		opt: &'a Options,
		cond: Arc<Cond>,
	) -> Self {
		Self::HnswCondition(HnswCondChecker {
			ctx,
			opt,
			cond,
			cache: Default::default(),
		})
	}

	pub(in crate::idx) async fn check_truthy(
		&mut self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		docs: &HnswDocs,
		doc_ids: Ids64,
	) -> Result<bool> {
		match self {
			Self::HnswCondition(c) => c.check_any_truthy(db, tx, stk, docs, doc_ids).await,
			Self::Hnsw(_) => Ok(true),
		}
	}

	pub(in crate::idx) fn expire(&mut self, doc_id: u64) {
		if let Self::HnswCondition(c) = self {
			c.expire(doc_id)
		}
	}

	pub(in crate::idx) fn expires(&mut self, doc_ids: Ids64) {
		if let Self::HnswCondition(c) = self {
			c.expires(doc_ids)
		}
	}

	pub(in crate::idx) async fn convert_result(
		&mut self,
		tx: &Transaction,
		docs: &HnswDocs,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		match self {
			Self::Hnsw(c) => c.convert_result(tx, docs, res).await,
			Self::HnswCondition(c) => Ok(c.convert_result(res)),
		}
	}
}

struct CheckerCacheEntry {
	record: Option<(Arc<RecordId>, Arc<Record>)>,
	truthy: bool,
}

impl CheckerCacheEntry {
	fn convert_result(
		res: VecDeque<(DocId, f64)>,
		cache: &mut HashMap<DocId, CheckerCacheEntry>,
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
		stk: &mut Stk,
		db: &DatabaseDefinition,
		ctx: &FrozenContext,
		opt: &Options,
		rid: Option<RecordId>,
		cond: &Cond,
	) -> Result<Self> {
		if let Some(rid) = rid {
			let rid = Arc::new(rid);
			let txn = ctx.tx();
			let val =
				txn.get_record(db.namespace_id, db.database_id, &rid.table, &rid.key, None).await?;
			if !val.data.as_ref().is_nullish() {
				let (record, truthy) = {
					let cursor_doc = CursorDoc {
						rid: Some(rid.clone()),
						ir: None,
						doc: val.into(),
						fields_computed: false,
					};
					let truthy = stk
						.run(|stk| cond.0.compute(stk, ctx, opt, Some(&cursor_doc)))
						.await
						.catch_return()?
						.is_truthy();
					(cursor_doc.doc.into_read_only(), truthy)
				};
				return Ok(CheckerCacheEntry {
					record: Some((rid, record)),
					truthy,
				});
			}
		}
		Ok(CheckerCacheEntry {
			record: None,
			truthy: false,
		})
	}
}

pub struct HnswChecker {}

impl HnswChecker {
	async fn convert_result(
		&self,
		tx: &Transaction,
		docs: &HnswDocs,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if let Some(rid) = docs.get_thing(tx, doc_id).await? {
				result.push_back((rid.clone().into(), dist, None));
			}
		}
		Ok(result)
	}
}

pub struct HnswCondChecker<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	cond: Arc<Cond>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl HnswCondChecker<'_> {
	fn convert_result(&mut self, res: VecDeque<(DocId, f64)>) -> VecDeque<KnnIteratorResult> {
		CheckerCacheEntry::convert_result(res, &mut self.cache)
	}

	async fn check_any_truthy(
		&mut self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		docs: &HnswDocs,
		doc_ids: Ids64,
	) -> Result<bool> {
		let mut res = false;
		for doc_id in doc_ids.iter() {
			if match self.cache.entry(doc_id) {
				Entry::Occupied(e) => e.get().truthy,
				Entry::Vacant(e) => {
					let rid = docs.get_thing(tx, doc_id).await?;
					let ent = CheckerCacheEntry::build(
						stk,
						db,
						self.ctx,
						self.opt,
						rid,
						self.cond.as_ref(),
					)
					.await?;
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

	fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}

	fn expires(&mut self, doc_ids: Ids64) {
		for doc_id in doc_ids.iter() {
			self.expire(doc_id);
		}
	}
}
