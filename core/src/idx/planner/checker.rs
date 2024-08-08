use crate::ctx::Context;
use crate::dbs::{Iterable, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::knn::Ids64;
use crate::sql::{Cond, Thing, Value};
use ahash::HashMap;
use reblessive::tree::Stk;
use std::collections::hash_map::Entry;
use std::collections::VecDeque;
use std::sync::Arc;

pub enum HnswConditionChecker<'a> {
	Hnsw(HnswChecker),
	HnswCondition(HnswCondChecker<'a>),
}

pub enum MTreeConditionChecker<'a> {
	MTree(MTreeChecker<'a>),
	MTreeCondition(MTreeCondChecker<'a>),
}

impl<'a> Default for HnswConditionChecker<'a> {
	fn default() -> Self {
		Self::Hnsw(HnswChecker {})
	}
}

impl<'a> HnswConditionChecker<'a> {
	pub(in crate::idx) fn new_cond(ctx: &'a Context, opt: &'a Options, cond: Arc<Cond>) -> Self {
		Self::HnswCondition(HnswCondChecker {
			ctx,
			opt,
			cond,
			cache: Default::default(),
		})
	}

	pub(in crate::idx) async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		docs: &HnswDocs,
		doc_ids: &Ids64,
	) -> Result<bool, Error> {
		match self {
			Self::HnswCondition(c) => c.check_any_truthy(stk, docs, doc_ids).await,
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
		docs: &HnswDocs,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		match self {
			Self::Hnsw(c) => c.convert_result(docs, res).await,
			Self::HnswCondition(c) => Ok(c.convert_result(res)),
		}
	}
}

impl<'a> MTreeConditionChecker<'a> {
	pub fn new_cond(ctx: &'a Context, opt: &'a Options, cond: Arc<Cond>) -> Self {
		if Cond(Value::Bool(true)).ne(cond.as_ref()) {
			return Self::MTreeCondition(MTreeCondChecker {
				ctx,
				opt,
				cond,
				cache: Default::default(),
			});
		} else {
			Self::new(ctx)
		}
	}

	pub fn new(ctx: &'a Context) -> Self {
		Self::MTree(MTreeChecker {
			ctx,
		})
	}

	pub(in crate::idx) async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		doc_ids: &DocIds,
		doc_id: DocId,
	) -> Result<bool, Error> {
		match self {
			Self::MTreeCondition(c) => c.check_truthy(stk, doc_ids, doc_id).await,
			Self::MTree(_) => Ok(true),
		}
	}

	pub(in crate::idx) fn expires(&mut self, ids: Ids64) {
		if let Self::MTreeCondition(c) = self {
			c.expires(ids)
		}
	}

	pub(in crate::idx) async fn convert_result(
		&mut self,
		doc_ids: &DocIds,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		match self {
			Self::MTree(c) => c.convert_result(doc_ids, res).await,
			Self::MTreeCondition(c) => Ok(c.convert_result(res)),
		}
	}
}

pub struct MTreeChecker<'a> {
	ctx: &'a Context,
}

impl<'a> MTreeChecker<'a> {
	async fn convert_result(
		&self,
		doc_ids: &DocIds,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		let txn = self.ctx.tx();
		for (doc_id, dist) in res {
			if let Some(key) = doc_ids.get_doc_key(&txn, doc_id).await? {
				let rid: Thing = key.into();
				result.push_back((rid.into(), dist, None));
			}
		}
		Ok(result)
	}
}

struct CheckerCacheEntry {
	record: Option<(Arc<Thing>, Arc<Value>)>,
	truthy: bool,
}

impl CheckerCacheEntry {
	fn convert_result(
		res: VecDeque<(DocId, f64)>,
		cache: &mut HashMap<DocId, CheckerCacheEntry>,
	) -> VecDeque<KnnIteratorResult> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if let Some(e) = cache.remove(&doc_id) {
				if e.truthy {
					if let Some((rid, value)) = e.record {
						result.push_back((rid, dist, Some(value)))
					}
				}
			}
		}
		result
	}

	async fn build(
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		rid: Option<Thing>,
		cond: &Cond,
	) -> Result<Self, Error> {
		if let Some(rid) = rid {
			let rid = Arc::new(rid);
			let txn = ctx.tx();
			let val = Iterable::fetch_thing(&txn, opt, &rid).await?;
			if !val.is_none_or_null() {
				let (value, truthy) = {
					let mut cursor_doc = CursorDoc {
						rid: Some(rid.clone()),
						ir: None,
						doc: val.into(),
					};
					let truthy = cond.compute(stk, ctx, opt, Some(&cursor_doc)).await?.is_truthy();
					(cursor_doc.doc.as_arc(), truthy)
				};
				return Ok(CheckerCacheEntry {
					record: Some((rid, value)),
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

pub struct MTreeCondChecker<'a> {
	ctx: &'a Context,
	opt: &'a Options,
	cond: Arc<Cond>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl<'a> MTreeCondChecker<'a> {
	async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		doc_ids: &DocIds,
		doc_id: u64,
	) -> Result<bool, Error> {
		match self.cache.entry(doc_id) {
			Entry::Occupied(e) => Ok(e.get().truthy),
			Entry::Vacant(e) => {
				let txn = self.ctx.tx();
				let rid = doc_ids.get_doc_key(&txn, doc_id).await?.map(|k| k.into());
				let ent =
					CheckerCacheEntry::build(stk, self.ctx, self.opt, rid, self.cond.as_ref())
						.await?;
				let truthy = ent.truthy;
				e.insert(ent);
				Ok(truthy)
			}
		}
	}

	fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}

	fn expires(&mut self, doc_ids: Ids64) {
		for doc_id in doc_ids.iter() {
			self.expire(doc_id);
		}
	}

	fn convert_result(&mut self, res: VecDeque<(DocId, f64)>) -> VecDeque<KnnIteratorResult> {
		CheckerCacheEntry::convert_result(res, &mut self.cache)
	}
}

pub struct HnswChecker {}

impl<'a> HnswChecker {
	async fn convert_result(
		&self,
		docs: &HnswDocs,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if let Some(rid) = docs.get_thing(doc_id) {
				result.push_back((rid.clone().into(), dist, None));
			}
		}
		Ok(result)
	}
}

pub struct HnswCondChecker<'a> {
	ctx: &'a Context,
	opt: &'a Options,
	cond: Arc<Cond>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl<'a> HnswCondChecker<'a> {
	fn convert_result(&mut self, res: VecDeque<(DocId, f64)>) -> VecDeque<KnnIteratorResult> {
		CheckerCacheEntry::convert_result(res, &mut self.cache)
	}

	async fn check_any_truthy(
		&mut self,
		stk: &mut Stk,
		docs: &HnswDocs,
		doc_ids: &Ids64,
	) -> Result<bool, Error> {
		let mut res = false;
		for doc_id in doc_ids.iter() {
			if match self.cache.entry(doc_id) {
				Entry::Occupied(e) => e.get().truthy,
				Entry::Vacant(e) => {
					let rid: Option<Thing> = docs.get_thing(doc_id).cloned();
					let ent =
						CheckerCacheEntry::build(stk, self.ctx, self.opt, rid, self.cond.as_ref())
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
