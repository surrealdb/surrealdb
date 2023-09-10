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
			if let Some((ixn, rq)) = b.range_queries.drain().take(1).next() {
				return Ok(Plan::SingleIndexMultiExpression(ixn, rq));
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
		if let IndexOperator::RangePart(o, v) = io.op() {
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
	SingleIndexMultiExpression(String, RangeQueryBuilder),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ix: DefineIndexStatement,
	id: Idiom,
	op: IndexOperator,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperator {
	Equality(Array),
	RangePart(Operator, Value),
	Matches(String, Option<MatchRef>),
}

impl IndexOption {
	pub(super) fn new(ix: DefineIndexStatement, id: Idiom, op: IndexOperator) -> Self {
		Self(Arc::new(Inner {
			ix,
			id,
			op,
		}))
	}

	pub(super) fn ix(&self) -> &DefineIndexStatement {
		&self.0.ix
	}

	pub(super) fn op(&self) -> &IndexOperator {
		&self.0.op
	}

	pub(super) fn id(&self) -> &Idiom {
		&self.0.id
	}

	pub(crate) fn explain(&self) -> Value {
		let mut r = HashMap::from([("index", Value::from(self.ix().name.0.to_owned()))]);
		match self.op() {
			IndexOperator::Equality(a) => {
				let v = if a.len() == 1 {
					a[0].clone()
				} else {
					Value::Array(a.clone())
				};
				r.insert("operator", Value::from(Operator::Equal.to_string()));
				r.insert("value", v);
			}
			IndexOperator::Matches(qs, a) => {
				r.insert("operator", Value::from(Operator::Matches(a.clone()).to_string()));
				r.insert("value", Value::from(qs.to_owned()));
			}
			IndexOperator::RangePart(op, v) => {
				r.insert("operator", Value::from(op.to_string()));
				r.insert("value", v.to_owned());
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
	fn set_to(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.lt(v) {
			self.value = v.clone();
			self.inclusive = false;
		}
	}

	fn set_to_inclusive(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			self.inclusive = true;
			return;
		}
		if self.inclusive {
			if self.value.lt(v) {
				self.value = v.clone();
			}
		} else {
			if self.value.le(v) {
				self.value = v.clone();
				self.inclusive = true;
			}
		}
	}

	fn set_from(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.gt(v) {
			self.value = v.clone();
			self.inclusive = false;
		}
	}

	fn set_from_inclusive(&mut self, v: &Value) {
		if self.value.is_none() {
			self.value = v.clone();
			self.inclusive = true;
			return;
		}
		if self.inclusive {
			if self.value.gt(v) {
				self.value = v.clone();
			}
		} else {
			if self.value.ge(v) {
				self.value = v.clone();
				self.inclusive = true;
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

#[derive(Default, Debug)]
pub(super) struct RangeQueryBuilder {
	pub(super) exps: HashSet<Arc<Expression>>,
	pub(super) from: RangeValue,
	pub(super) to: RangeValue,
}

impl RangeQueryBuilder {
	fn add(&mut self, exp: Arc<Expression>, op: &Operator, v: &Value) {
		match op {
			Operator::LessThan => self.to.set_to(v),
			Operator::LessThanOrEqual => self.to.set_to_inclusive(v),
			Operator::MoreThan => self.from.set_from(v),
			Operator::MoreThanOrEqual => self.from.set_from_inclusive(v),
			_ => return,
		}
		self.exps.insert(exp);
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOperator, IndexOperator, IndexOption, RangeValue};
	use crate::sql::statements::DefineIndexStatement;
	use crate::sql::{Array, Idiom, Operator, Value};
	use std::collections::HashSet;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperator::Equality(Array::from(vec!["test"])),
		);

		let io2 = IndexOption::new(
			DefineIndexStatement::default(),
			Idiom::from("a.b".to_string()),
			IndexOperator::Equality(Array::from(vec!["test"])),
		);

		set.insert(io1);
		set.insert(io2.clone());
		set.insert(io2);

		assert_eq!(set.len(), 1);
	}

	#[test]
	fn test_range_default_value() {
		let r = RangeValue::default();
		assert_eq!(r.value, Value::None);
		assert_eq!(r.inclusive, false);
	}
	#[test]
	fn test_range_value_from_inclusive() {
		let mut r = RangeValue::default();
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
		r.set_from_inclusive(&10.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, true);
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, true);
	}

	#[test]
	fn test_range_value_from() {
		let mut r = RangeValue::default();
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, false);
		r.set_from(&10.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, false);
		r.set_from(&20.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, false);
	}

	#[test]
	fn test_range_value_to_inclusive() {
		let mut r = RangeValue::default();
		r.set_to_inclusive(&10.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, true);
		r.set_to_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
		r.set_to_inclusive(&10.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
	}

	#[test]
	fn test_range_value_to() {
		let mut r = RangeValue::default();
		r.set_to(&10.into());
		assert_eq!(r.value, 10.into());
		assert_eq!(r.inclusive, false);
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, false);
		r.set_to(&10.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, false);
	}

	#[test]
	fn test_range_value_to_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, false);
		r.set_to_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
	}

	#[test]
	fn test_range_value_from_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, false);
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert_eq!(r.inclusive, true);
	}
}
