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
				return Ok(Plan::TableIterator(Some("WITH NOINDEX".to_string())));
			}
		}
		let mut b = PlanBuilder {
			indexes: Vec::new(),
			with,
			all_and: true,
			all_exp_with_index: true,
		};
		// Browse the AST and collect information
		if let Err(e) = b.eval_node(root) {
			return Ok(Plan::TableIterator(Some(e.to_string())));
		}
		// If we didn't found any index, we're done with no index plan
		if b.indexes.is_empty() {
			return Ok(Plan::TableIterator(Some("NO INDEX FOUND".to_string())));
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
		Ok(Plan::TableIterator(None))
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

	fn eval_node(&mut self, node: Node) -> Result<(), String> {
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
				self.eval_node(*left)?;
				self.eval_node(*right)?;
				Ok(())
			}
			Node::Unsupported(reason) => Err(reason),
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

	fn add_index_option(&mut self, e: Expression, i: IndexOption) {
		self.indexes.push((e, i));
	}
}

pub(super) enum Plan {
	TableIterator(Option<String>),
	SingleIndex(Expression, IndexOption),
	MultiIndex(Vec<(Expression, IndexOption)>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	qs: Option<String>,
	op: IndexOperation,
	mr: Option<MatchRef>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperation {
	Operator(Operator, Array),
	Range(RangeValue, RangeValue),
}

impl IndexOption {
	pub(super) fn new(
		ix: DefineIndexStatement,
		id: Idiom,
		op: IndexOperation,
		qs: Option<String>,
		mr: Option<MatchRef>,
	) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
			qs,
			mr,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &IndexOperation {
		&self.0.op
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
		let mut r = HashMap::from([("index", Value::from(self.ix().name.0.to_owned()))]);
		match self.op() {
			IndexOperation::Operator(op, a) => {
				let v = if a.len() == 1 {
					a[0].clone()
				} else {
					Value::Array(a.clone())
				};
				r.insert("operator", Value::from(op.to_string()));
				r.insert("value", v);
			}
			IndexOperation::Range(from, to) => {
				r.insert("operator", Value::from("Range"));
				r.insert("from", from.into());
				r.insert("to", to.into());
			}
		};
		Value::Object(Object::from(r))
	}
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct RangeValue {
	value: Value,
	inclusive: bool,
}

impl From<&RangeValue> for Value {
	fn from(rv: &RangeValue) -> Self {
		Value::from(Object::from(HashMap::from([
			("value", rv.value.to_owned()),
			("inclusive", Value::from(rv.inclusive)),
		])))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOperation, IndexOption};
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Array, Idiom, Operator};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperation::Operator(Operator::Equal, Array::from(vec!["test"])),
			None,
			None,
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperation::Operator(Operator::Equal, Array::from(vec!["test"])),
			None,
			None,
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}
}
