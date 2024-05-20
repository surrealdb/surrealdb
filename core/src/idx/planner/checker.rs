use crate::ctx::Context;
use crate::dbs::{Iterable, Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::knn::Ids64;
use crate::idx::trees::store::hnsw::SharedHnswIndex;
use crate::sql::{Cond, Thing, Value};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub enum ConditionChecker<'a> {
	Hnsw(HnswChecker<'a>),
	HnswCondition(HnswConditionChecker<'a>),
	MTree(MTreeChecker<'a>),
	MTreeCondition(MTreeConditionChecker<'a>),
	#[cfg(test)]
	None,
}

impl<'a> ConditionChecker<'a> {
	pub(in crate::idx) fn new_mtree(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		cond: Option<Arc<Cond>>,
		doc_ids: Arc<RwLock<DocIds>>,
	) -> Self {
		if let Some(cond) = cond {
			if Cond(Value::Bool(true)).ne(cond.as_ref()) {
				return Self::MTreeCondition(MTreeConditionChecker {
					ctx,
					opt,
					txn,
					cond,
					doc_ids,
					cache: Default::default(),
				});
			}
		}
		Self::MTree(MTreeChecker {
			ctx,
			txn,
			doc_ids,
		})
	}

	pub(in crate::idx) fn new_hnsw(
		ctx: &'a Context<'_>,
		opt: &'a Options,
		txn: &'a Transaction,
		cond: Option<Arc<Cond>>,
		h: SharedHnswIndex,
	) -> Self {
		if let Some(cond) = cond {
			if Cond(Value::Bool(true)).ne(cond.as_ref()) {
				return Self::HnswCondition(HnswConditionChecker {
					ctx,
					opt,
					txn,
					cond,
					h,
					cache: Default::default(),
				});
			}
		}
		Self::Hnsw(HnswChecker {
			ctx,
			h,
		})
	}

	pub(in crate::idx) async fn check_mtree_truthy(
		&mut self,
		stk: &mut Stk,
		doc_id: DocId,
	) -> Result<bool, Error> {
		match self {
			Self::MTreeCondition(c) => c.check_truthy(stk, doc_id).await,
			Self::MTree(_) => Ok(true),
			_ => unreachable!(),
		}
	}

	pub(in crate::idx) async fn check_hnsw_truthy(
		&mut self,
		stk: &mut Stk,
		doc_ids: &Ids64,
	) -> Result<bool, Error> {
		match self {
			Self::HnswCondition(c) => c.check_any_truthy(stk, doc_ids).await,
			Self::Hnsw(_) => Ok(true),
			_ => unreachable!(),
		}
	}

	pub(in crate::idx) fn expire(&mut self, id: u64) {
		match self {
			Self::MTreeCondition(c) => c.expire(id),
			Self::HnswCondition(c) => c.expire(id),
			_ => {}
		}
	}

	pub(in crate::idx) fn expires(&mut self, ids: Ids64) {
		match self {
			Self::MTreeCondition(c) => c.expires(ids),
			Self::HnswCondition(_) => unreachable!(),
			_ => {}
		}
	}

	pub(in crate::idx) async fn convert_result(
		&mut self,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		match self {
			ConditionChecker::MTree(c) => c.convert_result(res).await,
			ConditionChecker::MTreeCondition(c) => Ok(c.convert_result(res)),
			ConditionChecker::Hnsw(c) => c.convert_result(res).await,
			ConditionChecker::HnswCondition(c) => Ok(c.convert_result(res)),
			#[cfg(test)]
			ConditionChecker::None => Ok(VecDeque::from([])),
		}
	}
}

pub struct MTreeChecker<'a> {
	ctx: &'a Context<'a>,
	txn: &'a Transaction,
	doc_ids: Arc<RwLock<DocIds>>,
}

impl<'a> MTreeChecker<'a> {
	async fn convert_result(
		&self,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		let doc_ids = self.doc_ids.read().await;
		let mut tx = self.txn.lock().await;
		for (doc_id, dist) in res {
			if self.ctx.is_done() {
				break;
			}
			if let Some(key) = doc_ids.get_doc_key(&mut tx, doc_id).await? {
				result.push_back((key.into(), dist, None));
			}
		}
		Ok(result)
	}
}

struct CheckerCacheEntry {
	record: Option<(Thing, Value)>,
	truthy: bool,
}

impl CheckerCacheEntry {
	fn convert_result(
		ctx: &Context<'_>,
		res: VecDeque<(DocId, f64)>,
		cache: &mut HashMap<DocId, CheckerCacheEntry>,
	) -> VecDeque<KnnIteratorResult> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if ctx.is_done() {
				break;
			}
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		rid: Option<Thing>,
		cond: &Cond,
	) -> Result<Self, Error> {
		if let Some(rid) = rid {
			let val = {
				let mut tx = txn.lock().await;
				Iterable::fetch_thing(&mut tx, opt, &rid).await?
			};
			if !val.is_none_or_null() {
				let (value, truthy) = {
					let cursor_doc = CursorDoc {
						rid: Some(&rid),
						ir: None,
						doc: Cow::Owned(val),
					};
					let truthy =
						cond.compute(stk, ctx, opt, txn, Some(&cursor_doc)).await?.is_truthy();
					(cursor_doc.doc.into_owned(), truthy)
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

pub struct MTreeConditionChecker<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	cond: Arc<Cond>,
	doc_ids: Arc<RwLock<DocIds>>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl<'a> MTreeConditionChecker<'a> {
	async fn check_truthy(&mut self, stk: &mut Stk, doc_id: u64) -> Result<bool, Error> {
		match self.cache.entry(doc_id) {
			Entry::Occupied(e) => Ok(e.get().truthy),
			Entry::Vacant(e) => {
				let rid = {
					let mut tx = self.txn.lock().await;
					self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await?.map(|k| k.into())
				};
				let ent = CheckerCacheEntry::build(
					stk,
					self.ctx,
					self.opt,
					self.txn,
					rid,
					self.cond.as_ref(),
				)
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
		CheckerCacheEntry::convert_result(self.ctx, res, &mut self.cache)
	}
}

pub struct HnswChecker<'a> {
	ctx: &'a Context<'a>,
	h: SharedHnswIndex,
}

impl<'a> HnswChecker<'a> {
	async fn convert_result(
		&self,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		let h = self.h.read().await;
		for (doc_id, dist) in res {
			if self.ctx.is_done() {
				break;
			}
			if let Some(rid) = h.get_thing(doc_id) {
				result.push_back((rid.clone(), dist, None));
			}
		}
		Ok(result)
	}
}

pub struct HnswConditionChecker<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	cond: Arc<Cond>,
	h: SharedHnswIndex,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl<'a> HnswConditionChecker<'a> {
	fn convert_result(&mut self, res: VecDeque<(DocId, f64)>) -> VecDeque<KnnIteratorResult> {
		CheckerCacheEntry::convert_result(self.ctx, res, &mut self.cache)
	}

	async fn check_any_truthy(&mut self, stk: &mut Stk, doc_ids: &Ids64) -> Result<bool, Error> {
		let mut res = false;
		for doc_id in doc_ids.iter() {
			if match self.cache.entry(doc_id) {
				Entry::Occupied(e) => e.get().truthy,
				Entry::Vacant(e) => {
					let rid: Option<Thing> = self.h.read().await.get_thing(doc_id).cloned();
					let ent = CheckerCacheEntry::build(
						stk,
						self.ctx,
						self.opt,
						self.txn,
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
}
