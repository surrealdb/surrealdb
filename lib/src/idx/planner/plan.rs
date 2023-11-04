use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::{IndexRef, Node};
use crate::sql::with::With;
use crate::sql::{Array, Idiom, Object};
use crate::sql::{Expression, Operator, Value};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

pub(super) struct PlanBuilder {
	indexes: Vec<IndexOption>,
	indexed_expressions: HashMap<Arc<Expression>, IndexOption>,
	range_queries: HashMap<IndexRef, RangeQueryBuilder>,
	with_indexes: Vec<IndexRef>,
	all_and: bool,
	all_exp_with_index: bool,
}

impl PlanBuilder {
	pub(super) fn build(
		root: Node,
		with: &Option<With>,
		with_indexes: Vec<IndexRef>,
	) -> Result<Plan, Error> {
		if let Some(With::NoIndex) = with {
			return Ok(Plan::TableIterator(Some("WITH NOINDEX".to_string())));
		}
		let mut b = PlanBuilder {
			indexes: Default::default(),
			indexed_expressions: Default::default(),
			range_queries: Default::default(),
			with_indexes,
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
			if let Some((ir, rq)) = b.range_queries.drain().take(1).next() {
				return Ok(Plan::SingleIndexMultiExpression(ir, rq));
			}
			// Otherwise we take the first single index option
			if let Some(i) = b.indexes.pop() {
				return Ok(Plan::SingleIndex(i));
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
			if !self.with_indexes.is_empty() && !self.with_indexes.contains(&io.ir()) {
				return None;
			}
		}
		io
	}

	fn eval_node(&mut self, node: Node) -> Result<(), String> {
		match node {
			Node::IndexedFilter(io) => {
				self.add_index_option(None, io);
				Ok(())
			}
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
					self.add_index_option(Some(exp), io);
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

	fn add_index_option(&mut self, exp: Option<Arc<Expression>>, io: IndexOption) {
		if let IndexOperator::RangePart(o, v) = io.op() {
			match self.range_queries.entry(io.ir()) {
				Entry::Occupied(mut e) => {
					e.get_mut().add(io.cloned_id(), o, v);
				}
				Entry::Vacant(e) => {
					let mut b = RangeQueryBuilder::default();
					b.add(io.cloned_id(), o, v);
					e.insert(b);
				}
			}
		}
		if let Some(exp) = exp {
			self.indexed_expressions.insert(exp, io.clone());
		}
		self.indexes.push(io);
	}
}

pub(super) enum Plan {
	TableIterator(Option<String>),
	SingleIndex(IndexOption),
	MultiIndex(Vec<IndexOption>),
	SingleIndexMultiExpression(IndexRef, RangeQueryBuilder),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(crate) struct IndexOption(Arc<Inner>);

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) struct Inner {
	ir: IndexRef,
	id: Arc<Idiom>,
	op: IndexOperator,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperator {
	Equality(Value),
	Union(Array),
	RangePart(Operator, Value),
	Matches(String, Option<MatchRef>),
	Knn(Array, u32),
}

impl IndexOption {
	pub(super) fn new(ir: IndexRef, id: Arc<Idiom>, op: IndexOperator) -> Self {
		Self(Arc::new(Inner {
			ir,
			id,
			op,
		}))
	}

	pub(super) fn require_distinct(&self) -> bool {
		matches!(self.0.op, IndexOperator::Union(_))
	}

	pub(super) fn ir(&self) -> IndexRef {
		self.0.ir
	}

	pub(super) fn op(&self) -> &IndexOperator {
		&self.0.op
	}

	pub(super) fn id(&self) -> &Idiom {
		self.0.id.as_ref()
	}

	pub(super) fn cloned_id(&self) -> Arc<Idiom> {
		self.0.id.clone()
	}

	fn reduce_array(v: &Value) -> Value {
		if let Value::Array(a) = v {
			if a.len() == 1 {
				return a[0].clone();
			}
		}
		v.clone()
	}

	pub(crate) fn explain(&self, e: &mut HashMap<&str, Value>) {
		match self.op() {
			IndexOperator::Equality(v) => {
				e.insert("operator", Value::from(Operator::Equal.to_string()));
				e.insert("value", Self::reduce_array(v));
			}
			IndexOperator::Union(a) => {
				e.insert("operator", Value::from("union"));
				e.insert("value", Value::Array(a.clone()));
			}
			IndexOperator::Matches(qs, a) => {
				e.insert("operator", Value::from(Operator::Matches(*a).to_string()));
				e.insert("value", Value::from(qs.to_owned()));
			}
			IndexOperator::RangePart(op, v) => {
				e.insert("operator", Value::from(op.to_string()));
				e.insert("value", v.to_owned());
			}
			IndexOperator::Knn(a, k) => {
				e.insert("operator", Value::from(format!("<{}>", k)));
				e.insert("value", Value::Array(a.clone()));
			}
		};
	}
}

#[derive(Debug, Default, Eq, PartialEq, Hash)]
pub(super) struct RangeValue {
	pub(super) value: Value,
	pub(super) inclusive: bool,
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
		} else if self.value.le(v) {
			self.value = v.clone();
			self.inclusive = true;
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
		} else if self.value.ge(v) {
			self.value = v.clone();
			self.inclusive = true;
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
	pub(super) idioms: HashSet<Arc<Idiom>>,
	pub(super) from: RangeValue,
	pub(super) to: RangeValue,
}

impl RangeQueryBuilder {
	fn add(&mut self, id: Arc<Idiom>, op: &Operator, v: &Value) {
		match op {
			Operator::LessThan => self.to.set_to(v),
			Operator::LessThanOrEqual => self.to.set_to_inclusive(v),
			Operator::MoreThan => self.from.set_from(v),
			Operator::MoreThanOrEqual => self.from.set_from_inclusive(v),
			_ => return,
		}
		self.idioms.insert(id);
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
	use crate::sql::test::Parse;
	use crate::sql::{Array, Idiom, Value};
	use std::collections::HashSet;
	use std::sync::Arc;

	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			1,
			Arc::new(Idiom::parse("test")),
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"]))),
		);

		let io2 = IndexOption::new(
			1,
			Arc::new(Idiom::parse("test")),
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"]))),
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
		assert!(!r.inclusive);
	}
	#[test]
	fn test_range_value_from_inclusive() {
		let mut r = RangeValue::default();
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
		r.set_from_inclusive(&10.into());
		assert_eq!(r.value, 10.into());
		assert!(r.inclusive);
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 10.into());
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_from() {
		let mut r = RangeValue::default();
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(!r.inclusive);
		r.set_from(&10.into());
		assert_eq!(r.value, 10.into());
		assert!(!r.inclusive);
		r.set_from(&20.into());
		assert_eq!(r.value, 10.into());
		assert!(!r.inclusive);
	}

	#[test]
	fn test_range_value_to_inclusive() {
		let mut r = RangeValue::default();
		r.set_to_inclusive(&10.into());
		assert_eq!(r.value, 10.into());
		assert!(r.inclusive);
		r.set_to_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
		r.set_to_inclusive(&10.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_to() {
		let mut r = RangeValue::default();
		r.set_to(&10.into());
		assert_eq!(r.value, 10.into());
		assert!(!r.inclusive);
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(!r.inclusive);
		r.set_to(&10.into());
		assert_eq!(r.value, 20.into());
		assert!(!r.inclusive);
	}

	#[test]
	fn test_range_value_to_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(!r.inclusive);
		r.set_to_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
		r.set_to(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_from_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(!r.inclusive);
		r.set_from_inclusive(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
		r.set_from(&20.into());
		assert_eq!(r.value, 20.into());
		assert!(r.inclusive);
	}
}
