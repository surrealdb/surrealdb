pub(crate) mod executor;
pub(crate) mod plan;
mod tree;

use crate::dbs::{Iterable, Options, Transaction};
use crate::err::Error;
use crate::idx::planner::plan::{IndexOption, Plan, PlanBuilder};
use crate::idx::planner::tree::{IndexMap, Node, Tree};
use crate::sql::{Cond, Operator, Table};
use std::collections::HashMap;

pub(crate) struct QueryPlanner<'a> {
	opt: &'a Options,
	cond: &'a Option<Cond>,
	indexes: HashMap<Table, IndexMap>,
}

impl<'a> QueryPlanner<'a> {
	pub(crate) fn new(opt: &'a Options, cond: &'a Option<Cond>) -> Self {
		Self {
			opt,
			cond,
			indexes: HashMap::default(),
		}
	}

	pub(crate) async fn get_iterable(
		&mut self,
		txn: &Transaction,
		t: Table,
	) -> Result<Iterable, Error> {
		let (root, indexes) = Tree::build(self.opt, txn, &t, self.cond).await?;
		if !indexes.is_empty() {
			if let Some(node) = root {
				if let Some(plan) = AllAndStrategy::build(&node, &indexes)? {
					self.indexes.insert(t.clone(), indexes);
					return Ok(Iterable::Index(t, plan));
				}
			}
			self.indexes.insert(t.clone(), indexes);
		}
		Ok(Iterable::Table(t))
	}
}

struct AllAndStrategy<'a> {
	i: &'a IndexMap,
	b: PlanBuilder,
}

/// Successful if every boolean operators are AND
/// and there is at least one condition covered by an index
impl<'a> AllAndStrategy<'a> {
	fn build(node: &Node, i: &IndexMap) -> Result<Option<Plan>, Error> {
		let mut s = AllAndStrategy {
			i,
			b: PlanBuilder::default(),
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
		if let Some(idiom) = left.is_indexed_field() {
			if let Some(ix) = self.i.get(idiom) {
				if let Some(index_option) = IndexOption::found(ix, op, right) {
					self.b.add(index_option);
					return Ok(());
				}
			}
			self.eval_node(right)?;
		} else if let Some(idiom) = right.is_indexed_field() {
			if let Some(ix) = self.i.get(idiom) {
				if let Some(index_option) = IndexOption::found(ix, op, left) {
					self.b.add(index_option);
					return Ok(());
				}
			}
			self.eval_node(left)?;
		} else {
			self.eval_node(left)?;
			self.eval_node(right)?;
		}
		Ok(())
	}
}
