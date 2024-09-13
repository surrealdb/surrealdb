use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::{GroupRef, IdiomPosition, IndexRef, Node};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::with::With;
use crate::sql::{Array, Expression, Idiom, Number, Object};
use crate::sql::{Operator, Value};
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

/// The `PlanBuilder` struct represents a builder for constructing query plans.
pub(super) struct PlanBuilder {
	/// Do we have at least one index?
	has_indexes: bool,
	/// List of expressions that are not ranges, backed by an index
	non_range_indexes: Vec<(Arc<Expression>, IndexOption)>,
	/// List of indexes involved in this plan
	with_indexes: Vec<IndexRef>,
	/// Group each possible optimisations local to a SubQuery
	groups: BTreeMap<GroupRef, Group>, // The order matters because we want the plan to be consistent across repeated queries.
	/// Does a group contains only AND relations?
	all_and_groups: HashMap<GroupRef, bool>,
	/// Does the whole query contains only AND relations?
	all_and: bool,
	/// Is every expression backed by an index?
	all_exp_with_index: bool,
}

impl PlanBuilder {
	pub(super) fn build(
		root: Option<Node>,
		with: Option<&With>,
		with_indexes: Vec<IndexRef>,
		order: Option<IndexOption>,
	) -> Result<Plan, Error> {
		if let Some(With::NoIndex) = with {
			return Ok(Plan::TableIterator(Some("WITH NOINDEX".to_string())));
		}
		let mut b = PlanBuilder {
			has_indexes: false,
			non_range_indexes: Default::default(),
			groups: Default::default(),
			with_indexes,
			all_and_groups: Default::default(),
			all_and: true,
			all_exp_with_index: true,
		};
		// Browse the AST and collect information
		if let Some(root) = &root {
			if let Err(e) = b.eval_node(root) {
				return Ok(Plan::TableIterator(Some(e.to_string())));
			}
		}

		// If every boolean operator are AND then we can use the single index plan
		if b.all_and {
			// TODO: This is currently pretty arbitrary
			// We take the "first" range query if one is available
			if let Some((_, group)) = b.groups.into_iter().next() {
				if let Some((ir, rq)) = group.take_first_range() {
					return Ok(Plan::SingleIndexRange(ir, rq));
				}
			}
			// Otherwise we take the first single index option
			if let Some((e, i)) = b.non_range_indexes.pop() {
				return Ok(Plan::SingleIndex(Some(e), i));
			}
			// If there is an order option
			if let Some(o) = order {
				return Ok(Plan::SingleIndex(None, o.clone()));
			}
		}
		// If every expression is backed by an index with can use the MultiIndex plan
		else if b.all_exp_with_index {
			let mut ranges = Vec::with_capacity(b.groups.len());
			for (depth, group) in b.groups {
				if b.all_and_groups.get(&depth) == Some(&true) {
					group.take_union_ranges(&mut ranges);
				} else {
					group.take_intersect_ranges(&mut ranges);
				}
			}
			return Ok(Plan::MultiIndex(b.non_range_indexes, ranges));
		}
		Ok(Plan::TableIterator(None))
	}

	// Check if we have an explicit list of index we can use
	fn filter_index_option(&self, io: Option<&IndexOption>) -> Option<IndexOption> {
		if let Some(io) = &io {
			if !self.with_indexes.is_empty() && !self.with_indexes.contains(&io.ix_ref()) {
				return None;
			}
		}
		io.cloned()
	}

	fn eval_node(&mut self, node: &Node) -> Result<(), String> {
		match node {
			Node::Expression {
				group,
				io,
				left,
				right,
				exp,
			} => {
				let is_bool = self.check_boolean_operator(*group, exp.operator());
				if let Some(io) = self.filter_index_option(io.as_ref()) {
					self.add_index_option(*group, exp.clone(), io);
				} else if self.all_exp_with_index && !is_bool {
					self.all_exp_with_index = false;
				}
				self.eval_node(left)?;
				self.eval_node(right)?;
				Ok(())
			}
			Node::Unsupported(reason) => Err(reason.to_owned()),
			_ => Ok(()),
		}
	}

	fn check_boolean_operator(&mut self, gr: GroupRef, op: &Operator) -> bool {
		match op {
			Operator::Neg | Operator::Or => {
				if self.all_and {
					self.all_and = false;
				}
				self.all_and_groups.entry(gr).and_modify(|b| *b = false).or_insert(false);
				true
			}
			Operator::And => {
				self.all_and_groups.entry(gr).or_insert(true);
				true
			}
			_ => {
				self.all_and_groups.entry(gr).or_insert(true);
				false
			}
		}
	}

	fn add_index_option(&mut self, group_ref: GroupRef, exp: Arc<Expression>, io: IndexOption) {
		if let IndexOperator::RangePart(_, _) = io.op() {
			let level = self.groups.entry(group_ref).or_default();
			match level.ranges.entry(io.ix_ref()) {
				Entry::Occupied(mut e) => {
					e.get_mut().push((exp, io));
				}
				Entry::Vacant(e) => {
					e.insert(vec![(exp, io)]);
				}
			}
		} else {
			self.non_range_indexes.push((exp, io));
		}
		self.has_indexes = true;
	}
}

pub(super) enum Plan {
	/// Table full scan
	TableIterator(Option<String>),
	/// Index scan filtered on records matching a given expression
	SingleIndex(Option<Arc<Expression>>, IndexOption),
	/// Union of filtered index scans
	MultiIndex(Vec<(Arc<Expression>, IndexOption)>, Vec<(IndexRef, UnionRangeQueryBuilder)>),
	/// Index scan for record matching a given range
	SingleIndexRange(IndexRef, UnionRangeQueryBuilder),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(super) struct IndexOption {
	/// A reference to the index definition
	ix_ref: IndexRef,
	id: Idiom,
	id_pos: IdiomPosition,
	op: Arc<IndexOperator>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperator {
	Equality(Arc<Value>),
	Exactness(Arc<Value>),
	Union(Arc<Value>),
	Join(Vec<IndexOption>),
	RangePart(Operator, Arc<Value>),
	Matches(String, Option<MatchRef>),
	Knn(Arc<Vec<Number>>, u32),
	Ann(Arc<Vec<Number>>, u32, u32),
	Order,
}

impl IndexOption {
	pub(super) fn new(
		ix_ref: IndexRef,
		id: Idiom,
		id_pos: IdiomPosition,
		op: IndexOperator,
	) -> Self {
		Self {
			ix_ref,
			id,
			id_pos,
			op: Arc::new(op),
		}
	}

	pub(super) fn require_distinct(&self) -> bool {
		matches!(self.op.as_ref(), IndexOperator::Union(_))
	}

	pub(super) fn ix_ref(&self) -> IndexRef {
		self.ix_ref
	}

	pub(super) fn op(&self) -> &IndexOperator {
		self.op.as_ref()
	}

	pub(super) fn id_ref(&self) -> &Idiom {
		&self.id
	}

	pub(super) fn id_pos(&self) -> IdiomPosition {
		self.id_pos
	}

	fn reduce_array(v: &Value) -> Value {
		if let Value::Array(a) = v {
			if a.len() == 1 {
				return a[0].clone();
			}
		}
		v.clone()
	}

	pub(crate) fn explain(&self, ix_def: &[DefineIndexStatement]) -> Value {
		let mut e = HashMap::new();
		if let Some(ix) = ix_def.get(self.ix_ref as usize) {
			e.insert("index", Value::from(ix.name.0.to_owned()));
		}
		match self.op() {
			IndexOperator::Equality(v) => {
				e.insert("operator", Value::from(Operator::Equal.to_string()));
				e.insert("value", Self::reduce_array(v));
			}
			IndexOperator::Exactness(v) => {
				e.insert("operator", Value::from(Operator::Exact.to_string()));
				e.insert("value", Self::reduce_array(v));
			}
			IndexOperator::Union(v) => {
				e.insert("operator", Value::from("union"));
				e.insert("value", v.as_ref().clone());
			}
			IndexOperator::Join(ios) => {
				e.insert("operator", Value::from("join"));
				let mut joins = Vec::with_capacity(ios.len());
				for io in ios {
					joins.push(io.explain(ix_def));
				}
				let joins = Value::from(joins);
				e.insert("joins", joins);
			}
			IndexOperator::Matches(qs, a) => {
				e.insert("operator", Value::from(Operator::Matches(*a).to_string()));
				e.insert("value", Value::from(qs.to_owned()));
			}
			IndexOperator::RangePart(op, v) => {
				e.insert("operator", Value::from(op.to_string()));
				e.insert("value", v.as_ref().to_owned());
			}
			IndexOperator::Knn(a, k) => {
				let op = Value::from(Operator::Knn(*k, None).to_string());
				let val = Value::Array(Array::from(a.as_ref().clone()));
				e.insert("operator", op);
				e.insert("value", val);
			}
			IndexOperator::Ann(a, k, ef) => {
				let op = Value::from(Operator::Ann(*k, *ef).to_string());
				let val = Value::Array(Array::from(a.as_ref().clone()));
				e.insert("operator", op);
				e.insert("value", val);
			}
			IndexOperator::Order => {
				e.insert("operator", Value::from("Order"));
			}
		};
		Value::from(e)
	}
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
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

#[derive(Default)]
pub(super) struct Group {
	ranges: HashMap<IndexRef, Vec<(Arc<Expression>, IndexOption)>>,
}

impl Group {
	fn take_first_range(self) -> Option<(IndexRef, UnionRangeQueryBuilder)> {
		if let Some((ir, ri)) = self.ranges.into_iter().take(1).next() {
			UnionRangeQueryBuilder::new_aggregate(ri).map(|rb| (ir, rb))
		} else {
			None
		}
	}

	fn take_union_ranges(self, r: &mut Vec<(IndexRef, UnionRangeQueryBuilder)>) {
		for (ir, ri) in self.ranges {
			if let Some(rb) = UnionRangeQueryBuilder::new_aggregate(ri) {
				r.push((ir, rb));
			}
		}
	}

	fn take_intersect_ranges(self, r: &mut Vec<(IndexRef, UnionRangeQueryBuilder)>) {
		for (ir, ri) in self.ranges {
			for (exp, io) in ri {
				if let Some(rb) = UnionRangeQueryBuilder::new(exp, io) {
					r.push((ir, rb));
				}
			}
		}
	}
}

#[derive(Default, Debug)]
pub(super) struct UnionRangeQueryBuilder {
	pub(super) exps: HashSet<Arc<Expression>>,
	pub(super) from: RangeValue,
	pub(super) to: RangeValue,
}

impl UnionRangeQueryBuilder {
	fn new_aggregate(exp_ios: Vec<(Arc<Expression>, IndexOption)>) -> Option<Self> {
		if exp_ios.is_empty() {
			return None;
		}
		let mut b = Self::default();
		for (exp, io) in exp_ios {
			b.add(exp, io);
		}
		Some(b)
	}

	fn new(exp: Arc<Expression>, io: IndexOption) -> Option<Self> {
		let mut b = Self::default();
		if b.add(exp, io) {
			Some(b)
		} else {
			None
		}
	}

	fn add(&mut self, exp: Arc<Expression>, io: IndexOption) -> bool {
		if let IndexOperator::RangePart(op, val) = io.op() {
			match op {
				Operator::LessThan => self.to.set_to(val),
				Operator::LessThanOrEqual => self.to.set_to_inclusive(val),
				Operator::MoreThan => self.from.set_from(val),
				Operator::MoreThanOrEqual => self.from.set_from_inclusive(val),
				_ => return false,
			}
			self.exps.insert(exp);
		}
		true
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
	use crate::idx::planner::tree::IdiomPosition;
	use crate::sql::{Array, Idiom, Value};
	use crate::syn::Parse;
	use std::collections::HashSet;

	#[allow(clippy::mutable_key_type)]
	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			1,
			Idiom::parse("test"),
			IdiomPosition::Right,
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"])).into()),
		);

		let io2 = IndexOption::new(
			1,
			Idiom::parse("test"),
			IdiomPosition::Right,
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"])).into()),
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
