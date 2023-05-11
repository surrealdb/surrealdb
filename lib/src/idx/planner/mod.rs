pub(crate) mod plan;
mod tree;

use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::{IndexOption, Plan, PlanBuilder};
use crate::idx::planner::tree::{Node, TreeBuilder};
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
				return Ok(Iterable::Index(t, plan));
			}
		}
		Ok(Iterable::Table(t))
	}
}

struct AllAndStrategy {
	b: PlanBuilder,
}

/// Successful if every boolean operators are AND
/// and there is at least one condition covered by an index
impl AllAndStrategy {
	fn build(node: &Node) -> Result<Option<Plan>, Error> {
		let mut s = AllAndStrategy {
			b: PlanBuilder::new(),
		};
		match s.eval_node(node) {
			Ok(_) => Ok(Some(s.b.build()?)),
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
			if let Some(index_option) = IndexOption::found(ix, op, right) {
				self.b.add(index_option);
				return Ok(());
			}
			self.eval_node(right)?;
		} else if let Some(ix) = right.is_indexed_field() {
			if let Some(index_option) = IndexOption::found(ix, op, left) {
				self.b.add(index_option);
				return Ok(());
			}
			self.eval_node(left)?;
		} else {
			self.eval_node(left)?;
			self.eval_node(right)?;
		}
		Ok(())
	}
}
