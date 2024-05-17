use crate::ctx::Context;
use crate::dbs::{Iterable, Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::idx::docids::{DocId, DocIds};
use crate::sql::{Cond, Thing, Value};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use reblessive::tree::Stk;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

pub enum ConditionChecker {
	Hnsw,
	MTree(MTreeChecker),
	MTreeCondition(MTreeConditionChecker),
}
impl ConditionChecker {
	pub(in crate::idx) fn new_mtree(cond: Option<Arc<Cond>>, doc_ids: Arc<RwLock<DocIds>>) -> Self {
		if let Some(cond) = cond {
			if Cond(Value::Bool(true)).ne(cond.as_ref()) {
				return Self::MTreeCondition(MTreeConditionChecker {
					cond,
					doc_ids,
					cache: Default::default(),
				});
			}
		}
		Self::MTree(MTreeChecker {
			doc_ids,
		})
	}
	pub(in crate::idx) async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc_id: DocId,
	) -> Result<bool, Error> {
		match self {
			Self::MTree(_) | Self::Hnsw => Ok(true),
			Self::MTreeCondition(c) => c.check_truthy(stk, ctx, opt, txn, doc_id).await,
		}
	}

	pub(in crate::idx) fn expire(&mut self, doc_id: DocId) {
		match self {
			Self::MTree(_) | Self::Hnsw => {}
			Self::MTreeCondition(c) => c.expire(doc_id),
		}
	}

	pub(in crate::idx) async fn convert_result(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<(Thing, f64, Option<Value>)>, Error> {
		match self {
			ConditionChecker::Hnsw => todo!(),
			ConditionChecker::MTree(c) => c.convert_result(ctx, txn, res).await,
			ConditionChecker::MTreeCondition(c) => c.convert_result(ctx, res).await,
		}
	}
}

pub struct MTreeChecker {
	doc_ids: Arc<RwLock<DocIds>>,
}

impl MTreeChecker {
	async fn convert_result(
		&self,
		ctx: &Context<'_>,
		txn: &Transaction,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<(Thing, f64, Option<Value>)>, Error> {
		if res.is_empty() {
			return Ok(VecDeque::from([]));
		}
		let mut result = VecDeque::with_capacity(res.len());
		let doc_ids = self.doc_ids.read().await;
		let mut tx = txn.lock().await;
		for (doc_id, dist) in res {
			if ctx.is_done() {
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

pub struct MTreeConditionChecker {
	cond: Arc<Cond>,
	doc_ids: Arc<RwLock<DocIds>>,
	cache: HashMap<DocId, CheckerCacheEntry>,
}

impl MTreeConditionChecker {
	async fn check_truthy(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc_id: DocId,
	) -> Result<bool, Error> {
		match self.cache.entry(doc_id) {
			Entry::Occupied(e) => Ok(e.get().truthy),
			Entry::Vacant(e) => {
				let mut tx = txn.lock().await;
				if let Some(key) = self.doc_ids.read().await.get_doc_key(&mut tx, doc_id).await? {
					let rid: Thing = key.into();
					let val = Iterable::fetch_thing(&mut tx, opt, &rid).await?;
					let (record, truthy) = if !val.is_none_or_null() {
						let (value, truthy) = {
							let cursor_doc = CursorDoc {
								rid: Some(&rid),
								ir: None,
								doc: Cow::Owned(val),
							};
							let truthy = self
								.cond
								.compute(stk, ctx, opt, txn, Some(&cursor_doc))
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
		ctx: &Context<'_>,
		res: VecDeque<(DocId, f64)>,
	) -> Result<VecDeque<(Thing, f64, Option<Value>)>, Error> {
		let mut result = VecDeque::with_capacity(res.len());
		for (doc_id, dist) in res {
			if ctx.is_done() {
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
