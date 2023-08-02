use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::Node;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::with::With;
use crate::sql::{Array, Object};
use crate::sql::{Expression, Idiom, Operator, Value};
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

pub(super) struct PlanBuilder<'a> {
	indexes: Vec<(Expression, IndexOption)>,
	with: &'a Option<With>,
	all_and: bool,
	all_exp_with_index: bool,
}

impl<'a> PlanBuilder<'a> {
	pub(super) fn build(root: Node, with: &'a Option<With>) -> Result<Plan, Error> {
		if let Some(with) = with {
			if matches!(with, With::NoIndex) {
				return Ok(Plan::TableIterator);
			}
		}
		let mut b = PlanBuilder {
			indexes: Vec::new(),
			with,
			all_and: true,
			all_exp_with_index: true,
		};
		// Browse the AST and collect information
		if !b.eval_node(root)? {
			return Ok(Plan::TableIterator);
		}
		// If we didn't found any index, we're done with no index plan
		if b.indexes.is_empty() {
			return Ok(Plan::TableIterator);
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
		Ok(Plan::TableIterator)
	}

	// Check if we have an explicit list of index we can use
	fn filter_index_option(&self, io: Option<IndexOption>) -> Option<IndexOption> {
		if let Some(io) = &io {
			if let Some(With::Index(ixs)) = self.with {
				if !ixs.contains(&io.ix().name.0) {
					return None;
				}
			}
		}
		io
	}

	fn eval_node(&mut self, node: Node) -> Result<bool, Error> {
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
				if let Some(io) = self.filter_index_option(io) {
					self.add_index_option(exp, io);
				} else if self.all_exp_with_index && !is_bool {
					self.all_exp_with_index = false;
				}
				self.eval_expression(*left, *right)
			}
			Node::Unsupported => Ok(false),
			_ => Ok(true),
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

	fn eval_expression(&mut self, left: Node, right: Node) -> Result<bool, Error> {
		if !self.eval_node(left)? {
			return Ok(false);
		}
		if !self.eval_node(right)? {
			return Ok(false);
		}
		Ok(true)
	}

	fn add_index_option(&mut self, e: Expression, i: IndexOption) {
		self.indexes.push((e, i));
	}
}

pub(super) enum Plan {
	TableIterator,
	SingleIndex(Expression, IndexOption),
	MultiIndex(Vec<(Expression, IndexOption)>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	op: Operator,
	lo: Lookup,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum Lookup {
	FtMatches {
		qs: String,
		mr: Option<MatchRef>,
	},
	MtKnn {
		a: Array,
		k: u32,
	},
	IdxEqual(Value),
	UniqEqual(Value),
}

impl Lookup {
	pub(super) fn value(&self) -> Value {
		match self {
			Lookup::FtMatches {
				qs,
				..
			} => Value::from(qs.to_string()),
			Lookup::IdxEqual(v) | Lookup::UniqEqual(v) => v.to_owned(),
			Lookup::MtKnn {
				a,
				..
			} => Value::Array(a.to_owned()),
		}
	}
}

impl IndexOption {
	pub(super) fn new(ix: DefineIndexStatement, id: Idiom, op: Operator, lo: Lookup) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
			lo,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &Operator {
		&self.0.op
	}

	pub(super) fn lo(&self) -> &Lookup {
		&self.0.lo
	}

	pub(super) fn id(&self) -> &Idiom {
		&self.0.id
	}

	pub(crate) fn explain(&self) -> Value {
		Value::Object(Object::from(HashMap::from([
			("index", Value::from(self.ix().name.0.to_owned())),
			("operator", Value::from(self.op().to_string())),
			("value", self.lo().value()),
		])))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOption, Lookup};
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Array, Idiom, Operator, Value};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Lookup::MtKnn {
				a: Array(vec![Value::from(1)]),
				k: 0,
			},
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			Operator::Equal,
			Lookup::MtKnn {
				a: Array(vec![Value::from(1)]),
				k: 0,
			},
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}
}
