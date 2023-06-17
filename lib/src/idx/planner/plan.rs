use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::docids::{DocId, NO_DOC_ID};
use crate::idx::ft::terms::TermId;
use crate::idx::ft::{FtIndex, HitsIterator, MatchRef};
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::tree::IndexMap;
use crate::idx::IndexKeyBase;
use crate::key;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Expression, Ident, Object, Operator, Table, Thing, Value};
use async_trait::async_trait;
use roaring::RoaringTreemap;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default)]
pub(super) struct PlanBuilder {
	indexes: Vec<IndexOption>,
}

impl PlanBuilder {
	pub(super) fn add_index_option(&mut self, i: IndexOption) {
		self.indexes.push(i);
	}

	pub(super) fn build(mut self) -> Result<Plan, Error> {
		// TODO select the best option if there are several (cost based)
		if let Some(index) = self.indexes.pop() {
			Ok(Plan::new(index))
		} else {
			Err(Error::BypassQueryPlanner)
		}
	}
}

pub(crate) struct Plan {
	pub(super) i: IndexOption,
}

impl Plan {
	pub(super) fn new(i: IndexOption) -> Self {
		Self {
			i,
		}
	}

	pub(crate) async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
		exe: &QueryExecutor,
	) -> Result<Box<dyn ThingIterator>, Error> {
		self.i.new_iterator(opt, txn, exe).await
	}

	pub(crate) fn explain(&self) -> Value {
		match &self.i {
			IndexOption {
				ix,
				v,
				op,
				..
			} => Value::Object(Object::from(HashMap::from([
				("index", Value::from(ix.name.0.to_owned())),
				("operator", Value::from(op.to_string())),
				("value", v.clone()),
			]))),
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct IndexOption {
	pub(super) ix: DefineIndexStatement,
	pub(super) v: Value,
	pub(super) op: Operator,
	ep: Expression,
	mr: Option<MatchRef>,
}

impl IndexOption {
	pub(super) fn new(ix: DefineIndexStatement, op: Operator, v: Value, ep: Expression) -> Self {
		let mr = if let Operator::Matches(mr) = ep.o {
			mr
		} else {
			None
		};
		Self {
			ix,
			op,
			v,
			ep,
			mr,
		}
	}

	pub(super) async fn new_query_executor(
		&self,
		opt: &Options,
		txn: &Transaction,
		t: &Table,
		i: IndexMap,
	) -> Result<QueryExecutor, Error> {
		QueryExecutor::new(opt, txn, t, i, Some(self.ep.clone())).await
	}

	async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
		exe: &QueryExecutor,
	) -> Result<Box<dyn ThingIterator>, Error> {
		match &self.ix.index {
			Index::Idx => match self.op {
				Operator::Equal => {
					Ok(Box::new(NonUniqueEqualThingIterator::new(opt, &self.ix, &self.v)?))
				}
				_ => Err(Error::BypassQueryPlanner),
			},
			Index::Uniq => match self.op {
				Operator::Equal => {
					Ok(Box::new(UniqueEqualThingIterator::new(opt, &self.ix, &self.v)?))
				}
				_ => Err(Error::BypassQueryPlanner),
			},
			Index::Search {
				az,
				hl,
				sc,
				order,
			} => match self.op {
				Operator::Matches(_) => {
					let td = exe.pre_match_terms_docs();
					Ok(Box::new(
						MatchesThingIterator::new(opt, txn, &self.ix, az, *hl, sc, *order, td)
							.await?,
					))
				}
				_ => Err(Error::BypassQueryPlanner),
			},
		}
	}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(crate) trait ThingIterator: Send {
	async fn next_batch(
		&mut self,
		tx: &Transaction,
		size: u32,
	) -> Result<Vec<(Thing, DocId)>, Error>;
}

struct NonUniqueEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl NonUniqueEqualThingIterator {
	fn new(opt: &Options, ix: &DefineIndexStatement, v: &Value) -> Result<Self, Error> {
		let v = Array::from(v.clone());
		let beg = key::index::prefix_all_ids(opt.ns(), opt.db(), &ix.what, &ix.name, &v);
		let end = key::index::suffix_all_ids(opt.ns(), opt.db(), &ix.what, &ix.name, &v);
		Ok(Self {
			beg,
			end,
		})
	}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ThingIterator for NonUniqueEqualThingIterator {
	async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let min = self.beg.clone();
		let max = self.end.clone();
		let res = txn.lock().await.scan(min..max, limit).await?;
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
			self.beg.push(0x00);
		}
		let res = res.iter().map(|(_, val)| (val.into(), NO_DOC_ID)).collect();
		Ok(res)
	}
}

struct UniqueEqualThingIterator {
	key: Option<Key>,
}

impl UniqueEqualThingIterator {
	fn new(opt: &Options, ix: &DefineIndexStatement, v: &Value) -> Result<Self, Error> {
		let v = Array::from(v.clone());
		let key = key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &v, None).into();
		Ok(Self {
			key: Some(key),
		})
	}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ThingIterator for UniqueEqualThingIterator {
	async fn next_batch(
		&mut self,
		txn: &Transaction,
		_limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				return Ok(vec![(val.into(), NO_DOC_ID)]);
			}
		}
		Ok(vec![])
	}
}

struct MatchesThingIterator {
	hits: Option<HitsIterator>,
}

impl MatchesThingIterator {
	async fn new(
		opt: &Options,
		txn: &Transaction,
		ix: &DefineIndexStatement,
		az: &Ident,
		hl: bool,
		sc: &Scoring,
		order: u32,
		terms_docs: Option<Arc<Vec<(TermId, RoaringTreemap)>>>,
	) -> Result<Self, Error> {
		let ikb = IndexKeyBase::new(opt, ix);
		if let Scoring::Bm {
			..
		} = sc
		{
			let mut run = txn.lock().await;
			let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
			let fti = FtIndex::new(&mut run, az, ikb, order, sc, hl).await?;
			if let Some(terms_docs) = terms_docs {
				let hits = fti.new_hits_iterator(&mut run, terms_docs).await?;
				Ok(Self {
					hits,
				})
			} else {
				Ok(Self {
					hits: None,
				})
			}
		} else {
			Err(Error::FeatureNotYetImplemented {
				feature: "Vector Search",
			})
		}
	}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl ThingIterator for MatchesThingIterator {
	async fn next_batch(
		&mut self,
		txn: &Transaction,
		mut limit: u32,
	) -> Result<Vec<(Thing, DocId)>, Error> {
		let mut res = vec![];
		if let Some(hits) = &mut self.hits {
			let mut run = txn.lock().await;
			while limit > 0 {
				if let Some(hit) = hits.next(&mut run).await? {
					res.push(hit);
				} else {
					break;
				}
				limit -= 1;
			}
		}
		Ok(res)
	}
}
