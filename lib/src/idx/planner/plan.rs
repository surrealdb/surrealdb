use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::Node;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Object;
use crate::sql::{Expression, Idiom, Operator, Value};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub(super) struct PlanBuilder {
	indexes: Vec<(Expression, IndexOption)>,
	all_and: bool,
	all_exp_with_index: bool,
}

impl PlanBuilder {
	pub(super) fn build(root: Node) -> Result<Plan, Error> {
		let mut b = PlanBuilder {
			indexes: Vec::new(),
			all_and: true,
			all_exp_with_index: true,
		};
		// Browse the AST and collect information
		b.eval_node(root)?;
		// If we didn't found any index, we're done with no index plan
		if b.indexes.is_empty() {
			return Err(Error::BypassQueryPlanner);
		}
		// If every boolean operator are AND then we can use the single index plan
		if b.all_and {
			if let Some((e, i)) = b.indexes.pop() {
				return Ok(Plan::SingleIndex(e, i));
			}
		}
		// If every expression is backed by an index with can use the MultiIndex plan
		if b.all_exp_with_index {
			return Ok(Plan::MultiIndex(b.indexes));
		}
		Err(Error::BypassQueryPlanner)
	}

	fn eval_node(&mut self, node: Node) -> Result<(), Error> {
		match node {
			Node::Expression {
				io,
				left,
				right,
				exp,
			} => {
				if self.all_and && Operator::Or.eq(exp.operator()) {
					self.all_and = false;
				}
				let is_bool = self.check_boolean_operator(exp.operator());
				if let Some(io) = io {
					self.add_index_option(exp, io);
				} else if self.all_exp_with_index && !is_bool {
					self.all_exp_with_index = false;
				}
				self.eval_expression(*left, *right)
			}
			Node::Unsupported => Err(Error::BypassQueryPlanner),
			_ => Ok(()),
		}
	}

	fn check_boolean_operator(&mut self, op: &Operator) -> bool {
		match op {
			Operator::Neg | Operator::Or => {
				if self.all_and {
					self.all_and = false;
				}
				true
			}
			Operator::And => true,
			_ => false,
		}
	}

	fn eval_expression(&mut self, left: Node, right: Node) -> Result<(), Error> {
		self.eval_node(left)?;
		self.eval_node(right)?;
		Ok(())
	}

	fn add_index_option(&mut self, e: Expression, i: IndexOption) {
		self.indexes.push((e, i));
	}
}

pub(super) enum Plan {
	SingleIndex(Expression, IndexOption),
	MultiIndex(Vec<(Expression, IndexOption)>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	v: Value,
	qs: Option<String>,
	op: Operator,
	mr: Option<MatchRef>,
}

impl IndexOption {
	pub(super) fn new(
		ix: DefineIndexStatement,
		id: Idiom,
		op: Operator,
		v: Value,
		qs: Option<String>,
		mr: Option<MatchRef>,
	) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
			v,
			qs,
			mr,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &Operator {
		&self.0.op
	}

	pub(super) fn value(&self) -> &Value {
		&self.0.v
	}

	pub(super) fn qs(&self) -> Option<&String> {
		self.0.qs.as_ref()
	}

	pub(super) fn id(&self) -> &Idiom {
		&self.0.id
	}

	pub(super) fn match_ref(&self) -> Option<&MatchRef> {
		self.0.mr.as_ref()
	}

	pub(crate) fn explain(&self) -> Value {
		Value::Object(Object::from(HashMap::from([
			("index", Value::from(self.ix().name.0.to_owned())),
			("operator", Value::from(self.op().to_string())),
			("value", self.value().clone()),
		])))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::IndexOption;
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Idiom, Operator, Value};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Value::from("test"),
			None,
			None,
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Value::from("test"),
			None,
			None,
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}
}
