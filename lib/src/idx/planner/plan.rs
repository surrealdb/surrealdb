use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::ft::FtIndex;
use crate::idx::planner::tree::Node;
use crate::idx::IndexKeyBase;
use crate::key;
use crate::kvs::Key;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Ident, Object, Operator, Thing, Value};
use async_trait::async_trait;
use std::collections::HashMap;

pub(super) struct IndexOption {
	ix: DefineIndexStatement,
	v: Node,
	op: Operator,
}

impl IndexOption {
	fn new(ix: &DefineIndexStatement, op: &Operator, v: &Node) -> Self {
		Self {
			ix: ix.clone(),
			op: op.clone(),
			v: v.clone(),
		}
	}

	pub(super) fn found(ix: &DefineIndexStatement, op: &Operator, v: &Node) -> Option<Self> {
		let supported = v.is_scalar()
			&& match ix.index {
				Index::Idx => Operator::Equal.eq(op),
				Index::Uniq => Operator::Equal.eq(op),
				Index::Search {
					..
				} => {
					if let Operator::Matches(_) = op {
						true
					} else {
						false
					}
				}
			};
		if supported {
			Some(IndexOption::new(ix, op, v))
		} else {
			None
		}
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
			} => match self.op {
				Operator::Matches(_) => Ok(Box::new(
					MatchesThingIterator::new(opt, txn, &self.ix, az, *hl, &sc, &self.v).await?,
				)),
				_ => Err(Error::BypassQueryPlanner),
			},
		}
	}
}

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
	i: IndexOption,
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
			} => Value::Object(Object::from(HashMap::from([
				("index", Value::from(ix.name.0.to_owned())),
				("operator", Value::from(op.to_string())),
				("value", v.explain()),
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

#[async_trait]
pub(crate) trait ThingIterator: Send {
	async fn next_batch(&mut self, tx: &Transaction, size: u32) -> Result<Vec<Thing>, Error>;
}

struct NonUniqueEqualThingIterator {
	beg: Vec<u8>,
	end: Vec<u8>,
}

impl NonUniqueEqualThingIterator {
	fn new(opt: &Options, ix: &DefineIndexStatement, v: &Node) -> Result<Self, Error> {
		let v = v.to_array()?;
		let beg = key::index::prefix_all_ids(opt.ns(), opt.db(), &ix.what, &ix.name, &v);
		let end = key::index::suffix_all_ids(opt.ns(), opt.db(), &ix.what, &ix.name, &v);
		Ok(Self {
			beg,
			end,
		})
	}
}

#[async_trait]
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
	fn new(opt: &Options, ix: &DefineIndexStatement, v: &Node) -> Result<Self, Error> {
		let v = v.to_array()?;
		let key = key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, &v, None).into();
		Ok(Self {
			key: Some(key),
		})
	}
}

#[async_trait]
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
	_fti: FtIndex,
	_q: String,
}

impl MatchesThingIterator {
	async fn new(
		opt: &Options,
		txn: &Transaction,
		ix: &DefineIndexStatement,
		_az: &Ident,
		_hl: bool,
		sc: &Scoring,
		v: &Node,
	) -> Result<Self, Error> {
		let ikb = IndexKeyBase::new(opt, ix);
		let mut run = txn.lock().await;
		if let Scoring::Bm {
			b,
			..
		} = sc
		{
			let _fti = FtIndex::new(&mut run, ikb, b.to_usize()).await?;
			let _q = v.to_string()?;
			Ok(Self {
				_fti,
				_q,
			})
		} else {
			Err(Error::FeatureNotYetImplemented {
				feature: "Vector Search",
			})
		}
	}
}

#[async_trait]
impl ThingIterator for MatchesThingIterator {
	async fn next_batch(&mut self, _txn: &Transaction, _limit: u32) -> Result<Vec<Thing>, Error> {
		todo!();
		// let mut run = txn.lock().await;
		// self.fti.search(&mut run, &self.q)
		// Ok(vec![])
	}
}
