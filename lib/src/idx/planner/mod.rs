pub(crate) mod plan;
mod tree;

use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::Plan;
use crate::idx::planner::tree::{Node, TreeBuilder};
use crate::sql::index::Index;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Cond, Operator, Table};

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			cond,
			opt,
		}
	}

	pub(crate) async fn get_iterable(
		&self,
		txn: &Transaction,
		t: Table,
	) -> Result<Iterable, Error> {
		if let Some(node) = TreeBuilder::parse(self.opt, txn, &t, self.cond).await? {
			if let Some(plan) = AllAndStrategy::build(&node)? {
				return Ok(Iterable::Index(plan));
			}
		}
		Ok(Iterable::Table(t))
	}
}

struct AllAndStrategy {
	plan: Plan,
}

/// Successful if every boolean operators are AND
/// and there is at least one condition covered by an index
impl AllAndStrategy {
	fn build(node: &Node) -> Result<Option<Plan>, Error> {
		let mut s = AllAndStrategy {
			plan: Plan::new(),
		};
		match s.eval_node(node) {
			Ok(_) => Ok(Some(s.plan)),
			Err(Error::BypassQueryPlanner) => Ok(None),
			Err(e) => Err(e),
		}
	}

	fn eval_node(&mut self, node: &Node) -> Result<(), Error> {
		match node {
			Node::Expression {
				left,
				right,
				operator,
			} => self.eval_expression(left, right, operator),
			Node::Unsupported => Err(Error::BypassQueryPlanner),
			_ => Ok(()),
		}
	}

	fn eval_expression(&mut self, left: &Node, right: &Node, op: &Operator) -> Result<(), Error> {
		if op.eq(&Operator::Or) {
			return Err(Error::BypassQueryPlanner);
		}
		if let Some(ix) = left.is_indexed_field() {
			if right.is_scalar() && Self::index_supported_operator(&ix, op) {
				self.plan.add(ix.clone(), right.clone(), op.clone());
				return Ok(());
			}
			self.eval_node(right)?;
		} else if let Some(ix) = right.is_indexed_field() {
			if left.is_scalar() && Self::index_supported_operator(&ix, op) {
				self.plan.add(ix.clone(), left.clone(), op.clone());
				return Ok(());
			}
			self.eval_node(left)?;
		} else {
			self.eval_node(left)?;
			self.eval_node(right)?;
		}
		Ok(())
	}

	fn index_supported_operator(ix: &DefineIndexStatement, op: &Operator) -> bool {
		match ix.index {
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
		}
	}
}
