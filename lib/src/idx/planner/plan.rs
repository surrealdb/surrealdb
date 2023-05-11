use crate::err::Error;
use crate::idx::planner::tree::Node;
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Id, Operator};

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

	fn get_id_iterator(&self) -> Result<Box<dyn IdIterator>, Error> {
		match self.ix.index {
			Index::Idx => match self.op {
				Operator::Equal => Ok(Box::new(NonUniqueEqualIdIterator {})),
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

pub(super) struct PlanBuilder {
	indexes: Vec<IndexOption>,
}

impl PlanBuilder {
	pub(super) fn new() -> Self {
		Self {
			indexes: vec![],
		}
	}

	pub(super) fn add(&mut self, i: IndexOption) {
		self.indexes.push(i);
	}

	pub(super) fn build(mut self) -> Result<Plan, Error> {
		// TODO select the best option if there is several (cost based)
		if let Some(index) = self.indexes.pop() {
			Ok(index.get_id_iterator()?.into())
		} else {
			Err(Error::BypassQueryPlanner)
		}
	}
}

pub(crate) struct Plan {
	i: Box<dyn IdIterator>,
}

impl Plan {
	pub(crate) fn next(&mut self) -> Result<Option<Id>, Error> {
		self.i.next()
	}
}

impl From<Box<dyn IdIterator>> for Plan {
	fn from(i: Box<dyn IdIterator>) -> Self {
		Self {
			i,
		}
	}
}

pub(super) trait IdIterator: Send {
	fn next(&mut self) -> Result<Option<Id>, Error>;
}

struct NonUniqueEqualIdIterator {}

impl IdIterator for NonUniqueEqualIdIterator {
	fn next(&mut self) -> Result<Option<Id>, Error> {
		todo!()
	}
}
