pub(crate) mod executor;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::idx::planner::plan::{Plan, PlanBuilder};
use crate::idx::planner::tree::{Node, Tree};
use crate::sql::{Cond, Operator, Table};
use std::collections::HashMap;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
	/// There is one executor per table
	executors: HashMap<String, QueryExecutor>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			opt,
			cond,
			executors: HashMap::default(),
		}
	}

	pub(crate) async fn get_iterable(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		t: Table,
	) -> Result<Iterable, Error> {
		let res = Tree::build(ctx, self.opt, txn, &t, self.cond).await?;
		if let Some((node, im)) = res {
			if let Some(plan) = AllAndStrategy::build(&node)? {
				let e = QueryExecutor::new(self.opt, txn, &t, im, Some(plan.e.clone())).await?;
				self.executors.insert(t.0.clone(), e);
				return Ok(Iterable::Index(t, plan));
			}
			let e = QueryExecutor::new(self.opt, txn, &t, im, None).await?;
			self.executors.insert(t.0.clone(), e);
		}
		Ok(Iterable::Table(t))
	}

	pub(crate) fn finish(self) -> Option<HashMap<String, QueryExecutor>> {
		if self.executors.is_empty() {
			None
		} else {
			Some(self.executors)
		}
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
			b: PlanBuilder::default(),
		};
		match s.eval_node(node) {
			Ok(_) => match s.b.build() {
				Ok(p) => Ok(Some(p)),
				Err(Error::BypassQueryPlanner) => Ok(None),
				Err(e) => Err(e),
			},
			Err(Error::BypassQueryPlanner) => Ok(None),
			Err(e) => Err(e),
		}
	}

	fn eval_node(&mut self, node: &Node) -> Result<(), Error> {
		match node {
			Node::Expression {
				io: index_option,
				left,
				right,
				exp: expression,
			} => {
				if let Some(io) = index_option {
					self.b.add_index_option(expression.clone(), io.clone());
				}
				self.eval_expression(left, right, expression.operator())
			}
			Node::Unsupported => Err(Error::BypassQueryPlanner),
			_ => Ok(()),
		}
	}

	fn eval_expression(&mut self, left: &Node, right: &Node, op: &Operator) -> Result<(), Error> {
		if op.eq(&Operator::Or) {
			return Err(Error::BypassQueryPlanner);
		}
		self.eval_node(left)?;
		self.eval_node(right)?;
		Ok(())
	}
}
