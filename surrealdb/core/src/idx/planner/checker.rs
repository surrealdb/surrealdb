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
use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultDoc};
use crate::kvs::Transaction;
use crate::val::RecordId;

pub enum HnswConditionChecker<'a> {
	Hnsw(HnswChecker),
	HnswCondition(HnswCondChecker<'a>),
}

impl<'a> HnswConditionChecker<'a> {
	pub(in crate::idx) fn new() -> Self {
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
		res: KnnResult,
	) -> Result<VecDeque<KnnIteratorResult>> {
		match self {
			Self::Hnsw(c) => c.convert_result(tx, docs, res).await,
			Self::HnswCondition(c) => Ok(c.convert_result(res)),
		}
	}
}

enum CheckerCacheEntry {
	Truthy(Arc<RecordId>, Arc<Record>),
	NonTruthy,
}

impl CheckerCacheEntry {
	fn is_truthy(&self) -> bool {
		matches!(self, Self::Truthy(..))
	}

	fn convert_result(
		res: KnnResult,
		cache: &mut HashMap<KnnResultDoc, CheckerCacheEntry>,
	) -> VecDeque<KnnIteratorResult> {
		let mut result = VecDeque::with_capacity(res.0.len());
		for (doc, dist) in res.0 {
			if let Some(e) = cache.remove(&doc)
				&& let CheckerCacheEntry::Truthy(rid, value) = e
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
		tx: &Transaction,
		opt: &Options,
		rid: Option<Arc<RecordId>>,
		cond: &Cond,
	) -> Result<Self> {
		if let Some(rid) = rid {
			let val =
				tx.get_record(db.namespace_id, db.database_id, &rid.table, &rid.key, None).await?;
			if !val.data.as_ref().is_nullish() {
				let cursor_doc = CursorDoc {
					rid: Some(rid.clone()),
					ir: None,
					doc: val.into(),
					fields_computed: false,
				};
				if stk
					.run(|stk| cond.0.compute(stk, ctx, opt, Some(&cursor_doc)))
					.await
					.catch_return()?
					.is_truthy()
				{
					let record = cursor_doc.doc.into_read_only();
					return Ok(CheckerCacheEntry::Truthy(rid, record));
				}
			}
		}
		Ok(CheckerCacheEntry::NonTruthy)
	}
}

pub struct HnswChecker {}

impl HnswChecker {
	async fn convert_result(
		&self,
		tx: &Transaction,
		docs: &HnswDocs,
		res: KnnResult,
	) -> Result<VecDeque<KnnIteratorResult>> {
		if res.0.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.0.len());
		for (doc, dist) in res.0 {
			let rid = match doc {
				KnnResultDoc::DocId(doc_id) => {
					docs.get_thing(tx, doc_id).await?.map(|r| Arc::new(r))
				}
				KnnResultDoc::RecordId(rid) => Some(rid),
			};
			if let Some(rid) = rid {
				result.push_back((rid, dist, None));
			}
		}
		Ok(result)
	}
}

pub struct HnswCondChecker<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	cond: Arc<Cond>,
	cache: HashMap<KnnResultDoc, CheckerCacheEntry>,
}

impl HnswCondChecker<'_> {
	fn convert_result(&mut self, res: KnnResult) -> VecDeque<KnnIteratorResult> {
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
		for doc_id in doc_ids.iter() {
			match self.cache.entry(KnnResultDoc::DocId(doc_id)) {
				Entry::Occupied(e) => {
					if e.get().is_truthy() {
						return Ok(true);
					}
				}
				Entry::Vacant(e) => {
					let rid = docs.get_thing(tx, doc_id).await?.map(Arc::new);
					let ent = CheckerCacheEntry::build(
						stk,
						db,
						self.ctx,
						tx,
						self.opt,
						rid,
						self.cond.as_ref(),
					)
					.await?;
					if e.insert(ent).is_truthy() {
						return Ok(true);
					}
				}
			}
		}
		Ok(false)
	}

	async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		db: &DatabaseDefinition,
		tx: &Transaction,
		rid: Arc<RecordId>,
	) -> Result<bool> {
		match self.cache.entry(KnnResultDoc::RecordId(rid.clone())) {
			Entry::Occupied(e) => Ok(e.get().is_truthy()),
			Entry::Vacant(e) => {
				let ent = CheckerCacheEntry::build(
					stk,
					db,
					self.ctx,
					tx,
					self.opt,
					Some(rid),
					self.cond.as_ref(),
				)
				.await?;
				Ok(e.insert(ent).is_truthy())
			}
		}
	}

	fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&KnnResultDoc::DocId(doc_id));
	}

	fn expires(&mut self, doc_ids: Ids64) {
		for doc_id in doc_ids.iter() {
			self.expire(doc_id);
		}
	}
}
