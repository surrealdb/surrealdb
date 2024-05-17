use crate::ctx::Context;
use crate::dbs::{Iterable, Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::idx::planner::iterators::KnnIteratorResult;
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
	Hnsw(HnswChecker),
	HnswCondition(HnswConditionChecker<'a>),
	MTree(MTreeChecker<'a>),
	MTreeCondition(MTreeConditionChecker<'a>),
	#[cfg(debug_assertions)]
	#[allow(dead_code)]
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
			h,
		})
	}

	pub(in crate::idx) async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		doc_id: DocId,
	) -> Result<bool, Error> {
		match self {
			Self::MTreeCondition(c) => c.check_truthy(stk, doc_id).await,
			Self::HnswCondition(c) => c.check_truthy(stk, doc_id).await,
			_ => Ok(true),
		}
	}

	pub(in crate::idx) fn expire(&mut self, doc_id: DocId) {
		match self {
			Self::MTreeCondition(c) => c.expire(doc_id),
			Self::HnswCondition(c) => c.expire(doc_id),
			_ => {}
		}
	}

	pub(in crate::idx) async fn convert_result(
		&mut self,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		match self {
			ConditionChecker::MTree(c) => c.convert_result(res).await,
			ConditionChecker::MTreeCondition(c) => c.convert_result(res).await,
			ConditionChecker::Hnsw(c) => c.convert_result(res).await,
			ConditionChecker::HnswCondition(c) => c.convert_result(res).await,
			#[cfg(debug_assertions)]
			#[allow(dead_code)]
			_ => Ok(VecDeque::from([])),
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

pub struct MTreeConditionChecker<'a> {
	ctx: &'a Context<'a>,
	opt: &'a Options,
	txn: &'a Transaction,
	cond: Arc<Cond>,
	doc_ids: Arc<RwLock<DocIds>>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl<'a> MTreeConditionChecker<'a> {
	async fn check_truthy(&mut self, stk: &mut Stk, doc_id: DocId) -> Result<bool, Error> {
		match self.cache.entry(doc_id) {
			Entry::Occupied(e) => Ok(e.get().truthy),
			Entry::Vacant(e) => {
				let mut tx = self.txn.lock().await;
				if let Some(key) = self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await? {
					let rid: Thing = key.into();
					let val = Iterable::fetch_thing(&mut tx, self.opt, &rid).await?;
					let (record, truthy) = if !val.is_none_or_null() {
						let (value, truthy) = {
							let cursor_doc = CursorDoc {
								rid: Some(&rid),
								ir: None,
								doc: Cow::Owned(val),
							};
							let truthy = self
								.cond
								.compute(stk, self.ctx, self.opt, self.txn, Some(&cursor_doc))
								.await?
								.is_truthy();
							(cursor_doc.doc.into_owned(), truthy)
						};
						(Some((rid, value)), truthy)
					} else {
						(None, false)
					};
					e.insert(CheckerCacheEntry {
						record,
						truthy,
					});
					Ok(truthy)
				} else {
					e.insert(CheckerCacheEntry {
						record: None,
						truthy: false,
					});
					Ok(false)
				}
			}
		}
	}

	fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}

	async fn convert_result(
		&mut self,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if self.ctx.is_done() {
				break;
			}
			if let Some(e) = self.cache.remove(&doc_id) {
				if e.truthy {
					if let Some((rid, value)) = e.record {
						result.push_back((rid, dist, Some(value)))
					}
				}
			}
		}
		Ok(result)
	}
}

pub struct HnswChecker {
	h: SharedHnswIndex,
}

impl HnswChecker {
	async fn convert_result(
		&self,
		_res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		todo!()
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
	async fn convert_result(
		&self,
		_res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		todo!()
	}

	async fn check_truthy(&mut self, _stk: &mut Stk, _doc_id: DocId) -> Result<bool, Error> {
		todo!()
	}

	fn expire(&mut self, doc_id: DocId) {
		self.cache.remove(&doc_id);
	}
}
