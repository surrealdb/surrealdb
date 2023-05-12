use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::idx::planner::tree::Node;
use crate::key;
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Operator, Thing};
use async_trait::async_trait;

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

	fn new_iterator(&self, opt: &Options) -> Result<Box<dyn ThingIterator>, Error> {
		match self.ix.index {
			Index::Idx => match self.op {
				Operator::Equal => {
					Ok(Box::new(NonUniqueEqualThingIterator::new(opt, &self.ix, &self.v)?))
				}
				_ => Err(Error::BypassQueryPlanner),
			},
			Index::Uniq => {
				todo!()
			}
			Index::Search {
				..
			} => {
				todo!()
			}
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
	pub(crate) fn new_iterator(&self, opt: &Options) -> Result<Box<dyn ThingIterator>, Error> {
		self.i.new_iterator(opt)
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
