use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use anyhow::Result;

use crate::expr::operator::{MatchesOperator, NearestNeighbor};
use crate::expr::with::With;
use crate::expr::{BinaryOperator, Expr, Idiom};
use crate::idx::planner::tree::{
	CompoundIndexes, GroupRef, IdiomCol, IdiomPosition, IndexReference, Node,
};
use crate::idx::planner::{GrantedPermission, RecordStrategy, ScanDirection, StatementContext};
use crate::key::value::StoreKeyValue;
use crate::val::{Array, Number, Object, Value};

/// The `PlanBuilder` struct represents a builder for constructing query plans.
pub(super) struct PlanBuilder {
	/// Do we have at least one index?
	has_indexes: bool,
	/// List of expressions that are not ranges, backed by an index
	non_range_indexes: Vec<(Arc<Expr>, IndexOption)>,
	/// List of indexes allowed in this plan
	with_indexes: Option<Vec<IndexReference>>,
	/// Group each possible optimisations local to a SubQuery
	groups: BTreeMap<GroupRef, Group>, /* The order matters because we want the plan to be
	                                    * consistent across repeated queries. */
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
	/// Builds an optimal query execution plan by analyzing available indexes
	/// and query conditions.
	///
	/// This method implements a sophisticated cost-based optimizer that chooses
	/// between different execution strategies:
	/// 1. Table scan (fallback when no indexes are suitable)
	/// 2. Single index scan (most common, using one optimal index)
	/// 3. Multi-index scan (when multiple indexes can be combined)
	/// 4. Range scan (for range queries with optional ordering)
	///
	/// The optimizer considers factors like:
	/// - Available indexes and their selectivity
	/// - Boolean operator types (AND vs OR affects index combination strategies)
	/// - Compound index opportunities for multi-column queries
	/// - Range query optimization with proper scan direction
	/// - Order clause compatibility with index ordering
	pub(super) async fn build(
		ctx: &StatementContext<'_>,
		p: PlanBuilderParameters,
	) -> Result<Plan> {
		let mut b = PlanBuilder {
			has_indexes: false,
			non_range_indexes: Default::default(),
			groups: Default::default(),
			with_indexes: p.with_indexes,
		};

		// Handle explicit NO INDEX directive
		if let Some(With::NoIndex) = ctx.with {
			return Self::table_iterator(ctx, Some("WITH NOINDEX"), p.gp).await;
		}

		// Analyze the query AST to discover indexable conditions and collect
		// optimization opportunities
		if let Some(root) = &p.root {
			if let Err(e) = b.eval_node(root) {
				// Fall back to table scan if analysis fails
				return Self::table_iterator(ctx, Some(&e), p.gp).await;
			}
		}

		// Optimization path 1: All conditions connected by AND operators
		// This enables single-index optimizations and compound index usage
		if p.all_and {
			// Priority 1: Find the best compound index that covers multiple query
			// conditions Compound indexes are highly efficient as they can satisfy
			// multiple WHERE clauses in a single index scan, significantly reducing I/O
			// operations
			let mut compound_index = None;
			for (ixr, vals) in p.compound_indexes {
				if let Some((cols, io)) = b.check_compound_index_all_and(ixr, vals) {
					// Prefer indexes that cover more columns (higher selectivity)
					if let Some((c, _)) = &compound_index {
						if cols <= *c {
							continue; // Skip if this index covers fewer columns
						}
					}
					// Only consider true compound indexes (multiple columns)
					if cols > 1 {
						compound_index = Some((cols, io));
					}
				}
			}

			if let Some((_, io)) = compound_index {
				// Evaluate whether we can use index-only access (no table lookups needed)
				let record_strategy =
					ctx.check_record_strategy(p.all_expressions_with_index, p.gp)?;
				// Return optimized single compound index plan
				return Ok(Plan::SingleIndex(None, io, record_strategy));
			}

			// Select the first available range query (deterministic group order)
			if let Some((_, group)) = b.groups.into_iter().next() {
				if let Some((ir, rq)) = group.take_first_range() {
					// Evaluate the record strategy
					let record_strategy =
						ctx.check_record_strategy(p.all_expressions_with_index, p.gp)?;
					let (is_order, sc) = if let Some(io) = p.order_limit {
						#[cfg(not(any(feature = "kv-rocksdb", feature = "kv-tikv")))]
						{
							(io.ixr == ir, ScanDirection::Forward)
						}
						#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
						{
							(
								io.ixr == ir,
								Self::check_range_scan_direction(p.reverse_scan, io.op()),
							)
						}
					} else {
						(false, ScanDirection::Forward)
					};
					// Return the plan
					return Ok(Plan::SingleIndexRange(ir, rq, record_strategy, sc, is_order));
				}
			}

			// Otherwise, pick a non-range single-index
			// option (heuristic)
			if let Some((e, i)) = b.non_range_indexes.pop() {
				// Evaluate the record strategy
				let record_strategy =
					ctx.check_record_strategy(p.all_expressions_with_index, p.gp)?;
				// Return the plan
				return Ok(Plan::SingleIndex(Some(e), i, record_strategy));
			}
			// If there is an order option
			if let Some(o) = p.order_limit {
				// Evaluate the record strategy
				let record_strategy =
					ctx.check_record_strategy(p.all_expressions_with_index, p.gp)?;
				// Check compatibility with reverse-scan capability
				if Self::check_order_scan(p.reverse_scan, o.op()) {
					// Return the plan
					return Ok(Plan::SingleIndex(None, o.clone(), record_strategy));
				}
			}
		}
		// If every expression is backed by an index we can use the MultiIndex plan
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
			let record_strategy = ctx.check_record_strategy(p.all_expressions_with_index, p.gp)?;
			// Return the plan
			return Ok(Plan::MultiIndex(b.non_range_indexes, ranges, record_strategy));
		}
		Self::table_iterator(ctx, None, p.gp).await
	}

	async fn table_iterator(
		ctx: &StatementContext<'_>,
		reason: Option<&str>,
		granted_permission: GrantedPermission,
	) -> Result<Plan> {
		// Evaluate the record strategy
		let rs = ctx.check_record_strategy(false, granted_permission)?;
		// Evaluate the scan direction
		let sc = ctx.check_scan_direction();
		// Collect the reason if any
		let reason = reason.map(|s| s.to_string());
		Ok(Plan::TableIterator(reason, rs, sc))
	}

	/// Check if we have an explicit list of indices that we should use
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

	/// Check if the ordering is compatible with the datastore transaction
	/// capabilities
	fn check_order_scan(has_reverse_scan: bool, op: &IndexOperator) -> bool {
		has_reverse_scan || matches!(op, IndexOperator::Order(false))
	}

	#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
	fn check_range_scan_direction(has_reverse_scan: bool, op: &IndexOperator) -> ScanDirection {
		if has_reverse_scan && matches!(op, IndexOperator::Order(true)) {
			return ScanDirection::Backward;
		}
		ScanDirection::Forward
	}

	/// Check if a compound index can be used.
	/// Returns the number of columns involved, and the index option
	fn check_compound_index_all_and(
		&self,
		ixr: IndexReference,
		columns: Vec<Vec<IndexOperator>>,
	) -> Option<(IdiomCol, IndexOption)> {
		// Check the index can be used
		if !self.allowed_index(&ixr) {
			return None;
		}
		// Count contiguous values (from the left) that will be part of an equal search
		let mut continues_equals_values = 0;
		// Collect the range parts for any column
		let mut range_parts = vec![];
		for vals in &columns {
			// If the column is empty, we can stop here.
			if vals.is_empty() {
				break;
			}
			let mut is_equality = false;
			for iop in vals {
				match iop {
					IndexOperator::Equality(val) => {
						if !val.is_nullish() {
							is_equality = true;
						}
					}
					IndexOperator::RangePart(bo, val) => {
						if !val.is_nullish() {
							range_parts.push((bo.clone(), val.clone()));
						}
					}
					_ => {
						return None;
					}
				}
			}
			if !is_equality {
				break;
			}
			continues_equals_values += 1;
		}

		if continues_equals_values == 0 {
			if !range_parts.is_empty() {
				return Some((
					continues_equals_values + 1,
					IndexOption::new(
						ixr,
						None,
						IdiomPosition::None,
						IndexOperator::Range(vec![], range_parts),
					),
				));
			}
			return None;
		}

		let equal_combinations = Self::cartesian_equals_product(&columns, continues_equals_values);
		if equal_combinations.len() == 1 {
			let equals: Vec<Value> =
				equal_combinations[0].iter().map(|v| v.as_ref().clone()).collect();
			if !range_parts.is_empty() {
				return Some((
					continues_equals_values + 1,
					IndexOption::new(
						ixr,
						None,
						IdiomPosition::None,
						IndexOperator::Range(equals, range_parts),
					),
				));
			}
			return Some((
				continues_equals_values,
				IndexOption::new(
					ixr,
					None,
					IdiomPosition::None,
					IndexOperator::Equality(Arc::new(Value::Array(Array(equals)))),
				),
			));
		}
		let vals: Vec<Value> = equal_combinations
			.iter()
			.map(|v| {
				let a: Vec<Value> = v.iter().map(|v| v.as_ref().clone()).collect();
				Value::Array(Array(a))
			})
			.collect();
		Some((
			continues_equals_values,
			IndexOption::new(
				ixr,
				None,
				IdiomPosition::None,
				IndexOperator::Union(Arc::new(Value::Array(Array(vals)))),
			),
		))
	}

	fn cartesian_equals_product(
		columns: &[Vec<IndexOperator>],
		contigues_equals_value: usize,
	) -> Vec<Vec<Arc<Value>>> {
		columns.iter().take(contigues_equals_value).fold(vec![vec![]], |acc, v| {
			acc.iter()
				.flat_map(|prev| {
					v.iter().map(move |iop| {
						let mut new_vec = prev.clone();
						let val = if let IndexOperator::Equality(val) = iop {
							val.clone()
						} else {
							Arc::new(Value::None)
						};
						new_vec.push(val);
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

	fn add_index_option(&mut self, group_ref: GroupRef, exp: Arc<Expr>, io: IndexOption) {
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
	SingleIndex(Option<Arc<Expr>>, IndexOption, RecordStrategy),
	/// Union of filtered index scans
	/// 1: A list of expression and index options
	/// 2: A list of index ranges
	/// 3: A record strategy
	MultiIndex(
		Vec<(Arc<Expr>, IndexOption)>,
		Vec<(IndexReference, UnionRangeQueryBuilder)>,
		RecordStrategy,
	),
	/// Index scan for records matching a given range
	/// 1. The index reference
	/// 2. The index range
	/// 3. A record strategy
	/// 4. The scan direction
	/// 5. True if it matches an order option
	SingleIndexRange(IndexReference, UnionRangeQueryBuilder, RecordStrategy, ScanDirection, bool),
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub(super) struct IndexOption {
	/// A reference to the index definition
	ixr: IndexReference,
	/// The idiom matched by this index
	id: Option<Arc<Idiom>>,
	/// The position of the idiom in the expression (Left or Right)
	id_pos: IdiomPosition,
	/// The index operator
	op: Arc<IndexOperator>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) enum IndexOperator {
	Equality(Arc<Value>),
	Union(Arc<Value>),
	Join(Vec<IndexOption>),
	RangePart(BinaryOperator, Arc<Value>),
	Range(Vec<Value>, Vec<(BinaryOperator, Arc<Value>)>),
	Matches(String, MatchesOperator),
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

	pub(super) fn is_order(&self) -> bool {
		matches!(self.op.as_ref(), IndexOperator::Order(_))
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
		e.insert("index", Value::from(self.ix_ref().name.clone()));
		match self.op() {
			IndexOperator::Equality(v) => {
				e.insert("operator", Value::from(BinaryOperator::Equal.to_string()));
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
			IndexOperator::Matches(qs, op) => {
				e.insert("operator", Value::from(BinaryOperator::Matches(op.clone()).to_string()));
				e.insert("value", Value::from(qs.to_owned()));
			}
			IndexOperator::RangePart(op, v) => {
				e.insert("operator", Value::from(op.to_string()));
				e.insert("value", v.as_ref().to_owned());
			}
			IndexOperator::Range(equals, ranges) => {
				e.insert("prefix", Value::from(Array::from(equals.clone())));
				let a: Vec<Value> = ranges
					.iter()
					.map(|(o, v)| {
						let o = Object::from(BTreeMap::from([
							("operator", Value::from(o.to_string())),
							("value", v.as_ref().to_owned()),
						]));
						Value::from(o)
					})
					.collect();
				e.insert("ranges", Value::from(a));
			}
			IndexOperator::Knn(a, k) => {
				let expr = NearestNeighbor::KTree(*k).to_string();
				let op = Value::from(expr);
				let val = Value::Array(Array::from(a.as_ref().clone()));
				e.insert("operator", op);
				e.insert("value", val);
			}
			IndexOperator::Ann(a, k, ef) => {
				let expr = NearestNeighbor::Approximate(*k, *ef).to_string();
				let op = Value::from(expr);
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
	pub(super) value: Arc<Value>,
	pub(super) inclusive: bool,
}

impl RangeValue {
	fn set_to(&mut self, v: &Arc<Value>) {
		// Merge an exclusive upper bound (e.g., < v). We choose the maximum 'to' value.
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.lt(v) {
			self.value = v.clone();
			// A stricter (exclusive) bound dominates when we move the upper limit up.
			self.inclusive = false;
		}
	}

	fn set_to_inclusive(&mut self, v: &Arc<Value>) {
		// Merge an inclusive upper bound (e.g., <= v). Prefer the highest value; if
		// values are equal, inclusive wins over exclusive.
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

	fn set_from(&mut self, v: &Arc<Value>) {
		// Merge an exclusive lower bound (e.g., > v). We choose the minimum 'from' value
		// that is still >= all constraints; moving the bound down uses exclusive.
		if self.value.is_none() {
			self.value = v.clone();
			return;
		}
		if self.value.as_ref().gt(v.as_ref()) {
			self.value = v.clone();
			self.inclusive = false;
		}
	}

	fn set_from_inclusive(&mut self, v: &Arc<Value>) {
		// Merge an inclusive lower bound (e.g., >= v). If multiple constraints target
		// the same value, inclusive should override exclusive.
		if self.value.as_ref().is_none() {
			self.value = v.clone();
			self.inclusive = true;
			return;
		}
		if self.inclusive {
			if self.value.as_ref().gt(v.as_ref()) {
				self.value = v.clone();
			}
		} else if self.value.as_ref().ge(v.as_ref()) {
			self.value = v.clone();
			self.inclusive = true;
		}
	}
}

impl From<&RangeValue> for Value {
	fn from(rv: &RangeValue) -> Self {
		Value::from(Object::from(HashMap::from([
			("value", rv.value.as_ref().clone()),
			("inclusive", Value::from(rv.inclusive)),
		])))
	}
}

#[derive(Clone)]
pub(super) struct StoreRangeValue {
	pub(super) value: StoreKeyValue,
	pub(super) inclusive: bool,
}

impl Default for StoreRangeValue {
	fn default() -> Self {
		Self {
			value: StoreKeyValue::None,
			inclusive: false,
		}
	}
}

impl From<&RangeValue> for StoreRangeValue {
	fn from(rv: &RangeValue) -> Self {
		Self {
			value: rv.value.as_ref().clone().into(),
			inclusive: rv.inclusive,
		}
	}
}

#[derive(Default)]
pub(super) struct Group {
	ranges: HashMap<IndexReference, Vec<(Arc<Expr>, IndexOption)>>,
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
	pub(super) exps: HashSet<Arc<Expr>>,
	pub(super) from: RangeValue,
	pub(super) to: RangeValue,
}

impl UnionRangeQueryBuilder {
	fn new_aggregate(exp_ios: Vec<(Arc<Expr>, IndexOption)>) -> Option<Self> {
		if exp_ios.is_empty() {
			return None;
		}
		let mut b = Self::default();
		for (exp, io) in exp_ios {
			b.add(exp, io);
		}
		Some(b)
	}

	fn new(exp: Arc<Expr>, io: IndexOption) -> Option<Self> {
		let mut b = Self::default();
		if b.add(exp, io) {
			Some(b)
		} else {
			None
		}
	}

	fn add(&mut self, exp: Arc<Expr>, io: IndexOption) -> bool {
		if let IndexOperator::RangePart(op, val) = io.op() {
			match op {
				BinaryOperator::LessThan => self.to.set_to(val),
				BinaryOperator::LessThanEqual => self.to.set_to_inclusive(val),
				BinaryOperator::MoreThan => self.from.set_from(val),
				BinaryOperator::MoreThanEqual => self.from.set_from_inclusive(val),
				_ => return false,
			}
			self.exps.insert(exp);
		}
		true
	}
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;
	use std::sync::Arc;

	use crate::expr::{Ident, Idiom};
	use crate::idx::planner::plan::{IndexOperator, IndexOption, RangeValue};
	use crate::idx::planner::tree::{IdiomPosition, IndexReference};
	use crate::val::{Array, Value};

	#[expect(clippy::mutable_key_type)]
	#[test]
	fn test_hash_index_option() {
		let mut set = HashSet::new();
		let io1 = IndexOption::new(
			IndexReference::new(Arc::new([]), 1),
			Some(Idiom::field(Ident::new("test".to_owned()).unwrap()).into()),
			IdiomPosition::Right,
			IndexOperator::Equality(Value::Array(Array::from(vec!["test"])).into()),
		);

		let io2 = IndexOption::new(
			IndexReference::new(Arc::new([]), 1),
			Some(Idiom::field(Ident::new("test".to_owned()).unwrap()).into()),
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
		assert!(r.value.is_none());
		assert!(!r.inclusive);
		assert!(!r.inclusive);
	}
	#[test]
	fn test_range_value_from_inclusive() {
		let mut r = RangeValue::default();
		r.set_from_inclusive(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
		r.set_from_inclusive(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(r.inclusive);
		r.set_from_inclusive(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_from() {
		let mut r = RangeValue::default();
		r.set_from(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(!r.inclusive);
		r.set_from(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(!r.inclusive);
		r.set_from(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(!r.inclusive);
	}

	#[test]
	fn test_range_value_to_inclusive() {
		let mut r = RangeValue::default();
		r.set_to_inclusive(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(r.inclusive);
		r.set_to_inclusive(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
		r.set_to_inclusive(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_to() {
		let mut r = RangeValue::default();
		r.set_to(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(10));
		assert!(!r.inclusive);
		r.set_to(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(!r.inclusive);
		r.set_to(&Arc::new(10.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(!r.inclusive);
	}

	#[test]
	fn test_range_value_to_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_to(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(!r.inclusive);
		r.set_to_inclusive(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
		r.set_to(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
	}

	#[test]
	fn test_range_value_from_switch_inclusive() {
		let mut r = RangeValue::default();
		r.set_from(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(!r.inclusive);
		r.set_from_inclusive(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
		r.set_from(&Arc::new(20.into()));
		assert_eq!(r.value.as_ref(), &Value::from(20));
		assert!(r.inclusive);
	}
}
