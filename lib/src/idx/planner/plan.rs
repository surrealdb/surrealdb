use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::Node;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::with::With;
use crate::sql::{Array, Object};
use crate::sql::{Expression, Idiom, Operator, Value};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

pub(super) struct PlanBuilder<'a> {
	indexes: Vec<(Arc<Expression>, IndexOption)>,
	range_queries: HashMap<String, RangeQueryBuilder>,
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
			range_queries: HashMap::new(),
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
			// TODO: This is currently pretty arbitrary
			// We take the "first" range query if one is available
			if let Some((_, rb)) = b.range_queries.iter().next() {
				return Ok(Plan::SingleIndexMultiExpression(rb.exps));
			}
			// Otherwise we take the first single index option
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

	fn add_index_option(&mut self, exp: Arc<Expression>, io: IndexOption) {
		if let OperatorType::Range(o, v) = io.op_type() {
			match self.range_queries.entry(io.ix().name.0.to_owned()) {
				Entry::Occupied(mut e) => {
					e.get_mut().add(exp.clone(), o, v);
				}
				Entry::Vacant(e) => {
					let mut b = RangeQueryBuilder::default();
					b.add(exp.clone(), o, v);
					e.insert(b);
				}
			}
		}
		self.indexes.push((exp, io));
	}
}

pub(super) enum Plan {
	TableIterator(Option<String>),
	SingleIndex(Arc<Expression>, IndexOption),
	MultiIndex(Vec<(Arc<Expression>, IndexOption)>),
	SingleIndexMultiExpression(HashSet<Arc<Expression>>),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	op: IndexOperation,
	op_type: OperatorType,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperation {
	Operator(Operator, Array),
	Range(RangeValue, RangeValue),
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum OperatorType {
	Equality(Value),
	Range(Operator, Value),
	Matches(String, Option<MatchRef>),
}

impl IndexOption {
	pub(super) fn new(
		ix: DefineIndexStatement,
		id: Idiom,
		op: IndexOperation,
		op_type: OperatorType,
	) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
			op_type,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &IndexOperation {
		&self.0.op
	}

	pub(super) fn op_type(&self) -> &OperatorType {
		&self.0.op_type
	}

	pub(super) fn id(&self) -> &Idiom {
		&self.0.id
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

#[derive(Debug, Default, Eq, PartialEq, Hash)]
pub(super) struct RangeValue {
	value: Value,
	inclusive: bool,
}

impl RangeValue {
	fn set_less_than(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.inclusive {
			if self.value.ge(v) {
				self.value = v.clone();
			}
		} else {
			if self.value.gt(v) {
				self.value = v.clone();
			}
		}
	}

	fn set_less_than_inclusive(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.gt(v) {
			self.value = v.clone();
		}
	}

	fn set_more_than(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.lt(v) {
			self.value = v.clone();
		}
	}

	fn set_more_than_inclusive(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.inclusive {
			if self.value.le(v) {
				self.value = v.clone();
			}
		} else {
			if self.value.lt(v) {
				self.value = v.clone();
			}
		}
	}
}

impl From<&RangeValue> for Value {
	fn from(rv: &RangeValue) -> Self {
		Value::from(Object::from(HashMap::from([
			("value", rv.value.to_owned()),
			("inclusive", Value::from(rv.inclusive)),
		])))
	}
}

#[derive(Default)]
struct RangeQueryBuilder {
	exps: HashSet<Arc<Expression>>,
	from: RangeValue,
	to: RangeValue,
}

impl RangeQueryBuilder {
	fn add(&mut self, exp: Arc<Expression>, op: &Operator, v: &Value) {
		match op {
			Operator::LessThan => self.to.set_less_than(v),
			Operator::LessThanOrEqual => self.to.set_less_than_inclusive(v),
			Operator::MoreThan => self.from.set_more_than(v),
			Operator::MoreThanOrEqual => self.from.set_more_than_inclusive(v),
			_ => return,
		}
		self.exps.insert(exp);
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOperation, IndexOption, OperatorType, RangeValue};
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Array, Idiom, Operator, Value};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperation::Operator(Operator::Equal, Array::from(vec!["test"])),
			OperatorType::Equality(Value::None),
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperation::Operator(Operator::Equal, Array::from(vec!["test"])),
			OperatorType::Equality(Value::None),
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}

	#[test]
	fn test_range_value() {
		let r = RangeValue::default();
		assert_eq!(r.value, Value::None);
		assert_eq!(r.inclusive, false);
	}
}
