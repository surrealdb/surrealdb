use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::{FtIndex, HitsIterator};
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::tree::{IndexMap, Node};
use crate::idx::IndexKeyBase;
use crate::key;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Array, Expression, Ident, Object, Operator, Table, Thing, Value};
use async_trait::async_trait;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct PlanBuilder {
	indexes: Vec<IndexOption>,
}

impl PlanBuilder {
	pub(super) fn add(&mut self, i: IndexOption) {
		self.indexes.push(i);
	}

	pub(super) fn build(mut self) -> Result<Plan, Error> {
		// TODO select the best option if there are several (cost based)
		if let Some(index) = self.indexes.pop() {
			Ok(index.into())
		} else {
			Err(Error::BypassQueryPlanner)
		}
	}
}

pub(crate) struct Plan {
	pub(super) i: IndexOption,
}

impl Plan {
	pub(crate) async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Box<dyn ThingIterator>, Error> {
		self.i.new_iterator(opt, txn).await
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

impl From<IndexOption> for Plan {
	fn from(i: IndexOption) -> Self {
		Self {
			i,
		}
	}
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct IndexOption {
	pub(super) ix: DefineIndexStatement,
	pub(super) v: Value,
	pub(super) op: Operator,
	ep: Expression,
}

impl IndexOption {
	fn new(ix: DefineIndexStatement, op: Operator, v: Value, ep: Expression) -> Self {
		Self {
			ix,
			op,
			v,
			ep,
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

	pub(super) fn found(
		ix: &DefineIndexStatement,
		op: &Operator,
		v: &Node,
		ep: &Expression,
	) -> Option<Self> {
		if let Some(v) = v.is_scalar() {
			if match ix.index {
				Index::Idx => Operator::Equal.eq(op),
				Index::Uniq => Operator::Equal.eq(op),
				Index::Search {
					..
				} => {
					matches!(op, Operator::Matches(_))
				}
			} {
				return Some(IndexOption::new(ix.clone(), op.to_owned(), v.clone(), ep.clone()));
			}
		}
		None
	}

	async fn new_iterator(
		&self,
		opt: &Options,
		txn: &Transaction,
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
				Operator::Matches(_) => Ok(Box::new(
					MatchesThingIterator::new(opt, txn, &self.ix, az, *hl, sc, *order, &self.v)
						.await?,
				)),
				_ => Err(Error::BypassQueryPlanner),
			},
		}
	}
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub(crate) trait ThingIterator: Send {
	async fn next_batch(&mut self, tx: &Transaction, size: u32) -> Result<Vec<Thing>, Error>;
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
	async fn next_batch(&mut self, txn: &Transaction, limit: u32) -> Result<Vec<Thing>, Error> {
		let min = self.beg.clone();
		let max = self.end.clone();
		let res = txn.lock().await.scan(min..max, limit).await?;
		if let Some((key, _)) = res.last() {
			self.beg = key.clone();
			self.beg.push(0x00);
		}
		let res = res.iter().map(|(_, val)| val.into()).collect();
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
	async fn next_batch(&mut self, txn: &Transaction, _limit: u32) -> Result<Vec<Thing>, Error> {
		if let Some(key) = self.key.take() {
			if let Some(val) = txn.lock().await.get(key).await? {
				return Ok(vec![val.into()]);
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
		_hl: bool,
		sc: &Scoring,
		order: u32,
		v: &Value,
	) -> Result<Self, Error> {
		let ikb = IndexKeyBase::new(opt, ix);
		let mut run = txn.lock().await;
		if let Scoring::Bm {
			..
		} = sc
		{
			let query_string = v.clone().convert_to_string()?;
			let az = run.get_az(opt.ns(), opt.db(), az.as_str()).await?;
			let fti = FtIndex::new(&mut run, az, ikb, order).await?;
			let hits = fti.search(&mut run, query_string).await?;
			Ok(Self {
				hits,
			})
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
	async fn next_batch(&mut self, txn: &Transaction, mut limit: u32) -> Result<Vec<Thing>, Error> {
		let mut res = vec![];
		if let Some(hits) = &mut self.hits {
			let mut run = txn.lock().await;
			while limit > 0 {
				if let Some((hit, _)) = hits.next(&mut run).await? {
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
