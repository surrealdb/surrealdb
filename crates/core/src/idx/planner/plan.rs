use crate::err::Error;
use crate::idx::ft::MatchRef;
use crate::idx::planner::tree::{
	CompoundIndexes, GroupRef, IdiomCol, IdiomPosition, IndexReference, Node,
};
use crate::idx::planner::{GrantedPermission, RecordStrategy, ScanDirection, StatementContext};
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
	/// List of indexes allowed in this plan
	with_indexes: Option<Vec<IndexReference>>,
	/// Group each possible optimisations local to a SubQuery
	groups: BTreeMap<GroupRef, Group>, // The order matters because we want the plan to be consistent across repeated queries.
}

pub(super) struct PlanBuilderParameters {
	pub(super) root: Option<Node>,
	pub(super) gp: GrantedPermission,
	pub(super) compound_indexes: CompoundIndexes,
	pub(super) order_limit: Option<IndexOption>,
	pub(super) with_indexes: Option<Vec<IndexReference>>,
	pub(super) all_and: bool,
	pub(super) all_expressions_with_index: bool,
	pub(super) all_and_groups: HashMap<GroupRef, bool>,
	pub(super) reverse_scan: bool,
}

impl PlanBuilder {
	#[allow(clippy::mutable_key_type)]
	pub(super) async fn build(
		ctx: &StatementContext<'_>,
		p: PlanBuilderParameters,
	) -> Result<Plan, Error> {
		let mut b = PlanBuilder {
			has_indexes: false,
			non_range_indexes: Default::default(),
			groups: Default::default(),
			with_indexes: p.with_indexes,
		};

		if let Some(With::NoIndex) = ctx.with {
			return Self::table_iterator(ctx, Some("WITH NOINDEX"), p.gp).await;
		}

		// Browse the AST and collect information
		if let Some(root) = &p.root {
			if let Err(e) = b.eval_node(root) {
				return Self::table_iterator(ctx, Some(&e), p.gp).await;
			}
		}

		// If all boolean operators are AND, we can use the single index plan
		if p.all_and {
			// We try first the largest compound indexed
			let mut compound_index = None;
			for (ixr, vals) in p.compound_indexes {
				if let Some((cols, io)) = b.check_compound_index(ixr, vals) {
					if let Some((c, _)) = &compound_index {
						if cols <= *c {
							continue;
						}
					}
					if cols > 1 {
						compound_index = Some((cols, io));
					}
				}
			}
			if let Some((_, io)) = compound_index {
				// Evaluate if we can use keys only
				let record_strategy = ctx.check_record_strategy(true, p.gp)?;
				// Return the plan
				return Ok(Plan::SingleIndex(None, io, record_strategy));
			}

			// We take the "first" range query if one is available
			if let Some((_, group)) = b.groups.into_iter().next() {
				if let Some((ir, rq)) = group.take_first_range() {
					// Evaluate the record strategy
					let record_strategy = ctx.check_record_strategy(true, p.gp)?;
					// Return the plan
					return Ok(Plan::SingleIndexRange(ir, rq, record_strategy));
				}
			}

			// Otherwise, we try to find the most interesting (todo: TBD) single index option
			if let Some((e, i)) = b.non_range_indexes.pop() {
				// Evaluate the record strategy
				let record_strategy = ctx.check_record_strategy(true, p.gp)?;
				// Return the plan
				return Ok(Plan::SingleIndex(Some(e), i, record_strategy));
			}
			// If there is an order option
			if let Some(o) = p.order_limit {
				// Evaluate the record strategy
				let record_strategy = ctx.check_record_strategy(true, p.gp)?;
				// Check it is compatible with the reverse scan capability
				if Self::check_order_scan(p.reverse_scan, o.op()) {
					// Return the plan
					return Ok(Plan::SingleIndex(None, o.clone(), record_strategy));
				}
			}
		}
		// If every expression is backed by an index with can use the MultiIndex plan
		else if p.all_expressions_with_index {
			let mut ranges = Vec::with_capacity(b.groups.len());
			for (gr, group) in b.groups {
				if p.all_and_groups.get(&gr) == Some(&true) {
					group.take_union_ranges(&mut ranges);
				} else {
					group.take_intersect_ranges(&mut ranges);
				}
			}
			// Evaluate the record strategy
			let record_strategy = ctx.check_record_strategy(true, p.gp)?;
			// Return the plan
			return Ok(Plan::MultiIndex(b.non_range_indexes, ranges, record_strategy));
		}
		Self::table_iterator(ctx, None, p.gp).await
	}

	async fn table_iterator(
		ctx: &StatementContext<'_>,
		reason: Option<&str>,
		granted_permission: GrantedPermission,
	) -> Result<Plan, Error> {
		// Evaluate the record strategy
		let rs = ctx.check_record_strategy(false, granted_permission)?;
		// Evaluate the scan direction
		let sc = ctx.check_scan_direction();
		// Collect the reason if any
		let reason = reason.map(|s| s.to_string());
		Ok(Plan::TableIterator(reason, rs, sc))
	}

	/// Check if we have an explicit list of index that we should use
	fn filter_index_option(&self, io: Option<&IndexOption>) -> Option<IndexOption> {
		if let Some(io) = io {
			if !self.allowed_index(io.ix_ref()) {
				return None;
			}
		}
		io.cloned()
	}

	/// Check if an index is allowed to be used
	fn allowed_index(&self, ixr: &IndexReference) -> bool {
		if let Some(wi) = &self.with_indexes {
			if !wi.contains(ixr) {
				return false;
			}
		}
		true
	}

	/// Check if the ordering is compatible with the datastore transaction capabilities
	fn check_order_scan(has_reverse_scan: bool, op: &IndexOperator) -> bool {
		has_reverse_scan || matches!(op, IndexOperator::Order(false))
	}

	/// Check if a compound index can be used.
	fn check_compound_index(
		&self,
		ixr: IndexReference,
		columns: Vec<Vec<Arc<Value>>>,
	) -> Option<(IdiomCol, IndexOption)> {
		// Check the index can be used
		if !self.allowed_index(&ixr) {
			return None;
		}
		// Count continues values (from the left)
		let mut cont = 0;
		for vals in &columns {
			if vals.is_empty() {
				break;
			}
			if vals.iter().all(|v| v.is_none_or_null()) {
				break;
			}
			cont += 1;
		}
		if cont == 0 {
			return None;
		}
		let combinations = Self::cartesian_product(&columns);
		if combinations.len() == 1 {
			let val: Vec<Value> = combinations[0].iter().map(|v| v.as_ref().clone()).collect();
			return Some((
				cont,
				IndexOption::new(
					ixr,
					None,
					IdiomPosition::None,
					IndexOperator::Equality(Arc::new(Value::Array(Array(val)))),
				),
			));
		}
		let vals: Vec<Value> = combinations
			.iter()
			.map(|v| {
				let a: Vec<Value> = v.iter().map(|v| v.as_ref().clone()).collect();
				Value::Array(Array(a))
			})
			.collect();
		Some((
			cont,
			IndexOption::new(
				ixr,
				None,
				IdiomPosition::None,
				IndexOperator::Union(Arc::new(Value::Array(Array(vals)))),
			),
		))
	}

	fn cartesian_product(values: &[Vec<Arc<Value>>]) -> Vec<Vec<Arc<Value>>> {
		values.iter().fold(vec![vec![]], |acc, v| {
			acc.iter()
				.flat_map(|prev| {
					v.iter().map(move |x| {
						let mut new_vec = prev.clone();
						new_vec.push(x.clone());
						new_vec
					})
				})
				.collect()
		})
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
				if let Some(io) = self.filter_index_option(io.as_ref()) {
					self.add_index_option(*group, exp.clone(), io);
				}
				self.eval_node(left)?;
				self.eval_node(right)?;
				Ok(())
			}
			Node::Unsupported(reason) => Err(reason.to_owned()),
			_ => Ok(()),
		}
	}

	fn add_index_option(&mut self, group_ref: GroupRef, exp: Arc<Expression>, io: IndexOption) {
		if let IndexOperator::RangePart(_, _) = io.op() {
			let level = self.groups.entry(group_ref).or_default();
			match level.ranges.entry(io.ixr.clone()) {
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
	/// 1: An optional reason
	/// 2: A record strategy
	TableIterator(Option<String>, RecordStrategy, ScanDirection),
	/// Index scan filtered on records matching a given expression
	/// 1: The optional expression associated with the index
	/// 2: A record strategy
	SingleIndex(Option<Arc<Expression>>, IndexOption, RecordStrategy),
	/// Union of filtered index scans
	/// 1: A list of expression and index options
	/// 2: A list of index ranges
	/// 3: A record strategy
	MultiIndex(
		Vec<(Arc<Expression>, IndexOption)>,
		Vec<(IndexReference, UnionRangeQueryBuilder)>,
		RecordStrategy,
	),
	/// Index scan for record matching a given range
	/// 1. The reference to index
	/// 2. The index range
	/// 3. A record strategy
	SingleIndexRange(IndexReference, UnionRangeQueryBuilder, RecordStrategy),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(super) struct IndexOption {
	/// A reference to the index definition
	ixr: IndexReference,
	/// The idiom matching this index and its index
	id: Option<Arc<Idiom>>,
	/// The position of the idiom in the expression (Left or Right)
	id_pos: IdiomPosition,
	/// The index operator
	op: Arc<IndexOperator>,
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(super) enum IndexOperator {
	Equality(Arc<Value>),
	Union(Arc<Value>),
	Join(Vec<IndexOption>),
	RangePart(Operator, Arc<Value>),
	Matches(String, Option<MatchRef>),
	Knn(Arc<Vec<Number>>, u32),
	Ann(Arc<Vec<Number>>, u32, u32),
	/// false = ascending, true = descending
	Order(bool),
}

impl IndexOption {
	pub(super) fn new(
		ixr: IndexReference,
		id: Option<Arc<Idiom>>,
		id_pos: IdiomPosition,
		op: IndexOperator,
	) -> Self {
		Self {
			ixr,
			id,
			id_pos,
			op: Arc::new(op),
		}
	}

	pub(super) fn require_distinct(&self) -> bool {
		matches!(self.op.as_ref(), IndexOperator::Union(_))
	}

	pub(super) fn ix_ref(&self) -> &IndexReference {
		&self.ixr
	}

	pub(super) fn op(&self) -> &IndexOperator {
		self.op.as_ref()
	}

	pub(super) fn id_ref(&self) -> Option<&Idiom> {
		self.id.as_ref().map(|id| id.as_ref())
	}

	pub(super) fn id_pos(&self) -> IdiomPosition {
		self.id_pos
	}

	fn reduce_array(value: &Value) -> Value {
		if let Value::Array(a) = value {
			if a.len() == 1 {
				return a[0].clone();
			}
		}
		value.clone()
	}

	pub(crate) fn explain(&self) -> Value {
		let mut e = HashMap::new();
		e.insert("index", Value::from(self.ix_ref().name.0.to_owned()));
		match self.op() {
			IndexOperator::Equality(v) => {
				e.insert("operator", Value::from(Operator::Equal.to_string()));
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
					joins.push(io.explain());
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
			IndexOperator::Order(reverse) => {
				e.insert(
					"operator",
					Value::from(if *reverse {
						"ReverseOrder"
					} else {
						"Order"
					}),
				);
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
	ranges: HashMap<IndexReference, Vec<(Arc<Expression>, IndexOption)>>,
}

impl Group {
	fn take_first_range(self) -> Option<(IndexReference, UnionRangeQueryBuilder)> {
		if let Some((ir, ri)) = self.ranges.into_iter().take(1).next() {
			UnionRangeQueryBuilder::new_aggregate(ri).map(|rb| (ir, rb))
		} else {
			None
		}
	}

	fn take_union_ranges(self, r: &mut Vec<(IndexReference, UnionRangeQueryBuilder)>) {
		for (ir, ri) in self.ranges {
			if let Some(rb) = UnionRangeQueryBuilder::new_aggregate(ri) {
				r.push((ir, rb));
			}
		}
	}

	fn take_intersect_ranges(self, r: &mut Vec<(IndexReference, UnionRangeQueryBuilder)>) {
		for (ir, ri) in self.ranges {
			for (exp, io) in ri {
				if let Some(rb) = UnionRangeQueryBuilder::new(exp, io) {
					r.push((ir.clone(), rb));
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
	use crate::idx::planner::tree::{IdiomPosition, IndexReference};
	use crate::sql::{Array, Idiom, Value};
	use crate::syn::Parse;
	use std::collections::HashSet;
	use std::sync::Arc;

	#[allow(clippy::mutable_key_type)]
	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			IndexReference::new(Arc::new([]), 1),
			Some(Idiom::parse("test").into()),
			IdiomPosition::Right,
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"])).into()),
		);

		let io2 = IndexOption::new(
			IndexReference::new(Arc::new([]), 1),
			Some(Idiom::parse("test").into()),
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
