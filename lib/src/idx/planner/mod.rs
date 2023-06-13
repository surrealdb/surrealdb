pub(crate) mod executor;
pub(crate) mod plan;
mod tree;

use crate::ctx::Context;
use crate::dbs::{Iterable, Options};
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
	#[inline]
	pub(crate) fn add_query_executor(
		pla: Option<&QueryPlanner<'_>>,
		tb: &str,
		ctx: &mut Context<'_>,
	) {
		if let Some(p) = pla {
			if let Some(exe) = p.get_query_executor(tb) {
				ctx.add_query_executor(exe);
			}
		}
	}

	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			opt,
			cond,
			executors: HashMap::default(),
		}
	}

	pub(crate) fn get_query_executor(&self, tb: &str) -> Option<QueryExecutor> {
		self.executors.get(tb).cloned()
	}

	pub(crate) async fn get_iterable(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		t: Table,
	) -> Result<Iterable, Error> {
		let txn = ctx.clone_transaction()?;
		let res = Tree::build(self.opt, &txn, &t, self.cond).await?;
		if let Some((node, im)) = res {
			if let Some(plan) = AllAndStrategy::build(&node)? {
				let e = plan.i.new_query_executor(opt, &txn, &t, im).await?;
				self.executors.insert(t.0.clone(), e);
				return Ok(Iterable::Index(t, plan));
			}
			let e = QueryExecutor::new(opt, &txn, &t, im, None).await?;
			self.executors.insert(t.0.clone(), e);
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
				index_option,
				left,
				right,
				operator,
			} => {
				if let Some(io) = index_option {
					self.b.add(io.clone());
				}
				self.eval_expression(left, right, operator)
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
