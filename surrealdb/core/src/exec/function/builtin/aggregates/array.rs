//! Array aggregate functions.
//!
//! Provides aggregates for collecting values into arrays: group, join, and distinct.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use anyhow::Result;

use super::saved::{Loading, Saving};
use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::val::Value;

// ============================================================================
// ArrayGroup
// ============================================================================

/// array::group - flattens the group's values and returns the unique ones.
///
/// Returns the same values as the scalar `array::group`. The aggregate must not
/// keep values that the scalar form would collapse, otherwise `array::len` over
/// it counts values that the group's rows do not support.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayGroup;

impl AggregateFunction for ArrayGroup {
	fn name(&self) -> &'static str {
		"array::group"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::group", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ArrayGroupAccumulator::deduping())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Array(Box::new(Kind::Any), None))
	}
}

/// array::collect - the group's values in row order, duplicates kept.
///
/// The fold that answers "every value this group saw", as distinct from
/// `array::group`'s "the distinct values, flattened". One accumulator cannot
/// honour both contracts, so they are separate functions, and three callers
/// depend on this one:
///
/// - the SurrealQL planner substitutes it for a projected expression that carries no aggregate of
///   its own but does read the row, so that `SELECT name, count() ... GROUP BY type` reports one
///   element per row;
/// - the GQL lowering targets it for `collect`/`collect_list`, which owe one entry per matched row;
/// - the SQL front ends target it for `array_agg`, `json_agg` and `jsonb_agg`, which keep the NULLs
///   they are given.
///
/// The name is not in the parser's builtin path table, so SurrealQL text
/// cannot call it. It names the fold in plan output and is the target of the
/// GQL `collect` lowering. The SurrealQL form of this fold is the projected
/// expression itself.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayCollect;

impl AggregateFunction for ArrayCollect {
	fn name(&self) -> &'static str {
		"array::collect"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::collect", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ArrayGroupAccumulator::collecting())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Array(Box::new(Kind::Any), None))
	}
}

#[derive(Debug, Clone, Default)]
struct ArrayGroupAccumulator {
	values: Vec<Value>,
	/// Whether `finalize` applies the scalar `array::group` reduction
	/// (flatten, then unique) or returns what it collected verbatim.
	dedupe: bool,
}

impl ArrayGroupAccumulator {
	fn deduping() -> Self {
		Self {
			values: Vec::new(),
			dedupe: true,
		}
	}

	fn collecting() -> Self {
		Self {
			values: Vec::new(),
			dedupe: false,
		}
	}
}

impl Accumulator for ArrayGroupAccumulator {
	fn holds_inputs(&self) -> bool {
		true
	}

	fn save(&self) -> Option<Value> {
		Some(Saving::default().values(&self.values).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.values = state.values()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		self.values.push(value);
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<ArrayGroupAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.values.extend(other.values.iter().cloned());
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		let array = self.values.clone().into();
		if self.dedupe {
			// Calls the scalar function directly, so the aggregate and the
			// scalar form always return the same value.
			crate::fnc::array::group((array,))
		} else {
			Ok(Value::Array(array))
		}
	}

	fn reset(&mut self) {
		self.values.clear();
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// ArrayJoin
// ============================================================================

/// array::join - collects the group's values other than `NONE` and `NULL` and joins them with a
/// separator
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayJoin;

impl AggregateFunction for ArrayJoin {
	fn name(&self) -> &'static str {
		"array::join"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::join", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		// The separator is the group's own argument and is bound through
		// `Accumulator::set_extra_args` before the group is finalized; a group
		// whose statement supplies none joins on nothing.
		Box::new(ArrayJoinAccumulator {
			values: Vec::new(),
			separator: String::new(),
		})
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("value", Kind::Any)
			.arg("separator", Kind::String)
			.returns(Kind::String)
	}
}

#[derive(Debug, Clone)]
struct ArrayJoinAccumulator {
	values: Vec<Value>,
	separator: String,
}

impl Accumulator for ArrayJoinAccumulator {
	fn holds_inputs(&self) -> bool {
		true
	}

	fn save(&self) -> Option<Value> {
		Some(Saving::default().values(&self.values).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.values = state.values()?;
		Ok(())
	}

	/// `NONE` and `NULL` have no text to join and are skipped, so a column with missing values
	/// joins the ones it has.
	fn update(&mut self, value: Value) -> Result<()> {
		if !matches!(value, Value::None | Value::Null) {
			self.values.push(value);
		}
		Ok(())
	}

	/// The first argument after the accumulated one is the separator.
	fn set_extra_args(&mut self, args: &[Value]) {
		self.separator = args.first().map(|v| v.clone().into_raw_string()).unwrap_or_default();
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<ArrayJoinAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.values.extend(other.values.iter().cloned());
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		let joined = self
			.values
			.iter()
			.map(|v| v.clone().into_raw_string())
			.collect::<Vec<_>>()
			.join(&self.separator);
		Ok(Value::String(joined.into()))
	}

	fn reset(&mut self) {
		self.values.clear();
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// ArrayDistinct
// ============================================================================

/// array::distinct - collects unique values into an array (preserving insertion order)
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayDistinct;

impl AggregateFunction for ArrayDistinct {
	fn name(&self) -> &'static str {
		"array::distinct"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::distinct", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ArrayDistinctAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Array(Box::new(Kind::Any), None))
	}
}

#[derive(Debug, Clone, Default)]
struct ArrayDistinctAccumulator {
	/// Values in insertion order (for maintaining order)
	values: Vec<Value>,
	/// Hashes of values we've seen (for O(1) distinctness checking)
	seen_hashes: HashSet<u64>,
}

impl ArrayDistinctAccumulator {
	/// Compute hash of a value for distinctness checking
	fn hash_value(value: &Value) -> u64 {
		let mut hasher = std::collections::hash_map::DefaultHasher::new();
		value.hash(&mut hasher);
		hasher.finish()
	}
}

impl Accumulator for ArrayDistinctAccumulator {
	fn holds_inputs(&self) -> bool {
		true
	}

	fn save(&self) -> Option<Value> {
		Some(Saving::default().values(&self.values).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.values = state.values()?;
		self.seen_hashes = self.values.iter().map(Self::hash_value).collect();
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		let hash = Self::hash_value(&value);
		// Only add if we haven't seen this hash before
		if self.seen_hashes.insert(hash) {
			self.values.push(value);
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<ArrayDistinctAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		// Add values from other that we haven't seen
		for value in &other.values {
			let hash = Self::hash_value(value);
			if self.seen_hashes.insert(hash) {
				self.values.push(value.clone());
			}
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Array(self.values.clone().into()))
	}

	fn reset(&mut self) {
		self.values.clear();
		self.seen_hashes.clear();
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// ArrayMin / ArrayMax
// ============================================================================

/// Whether an [`ExtremumAccumulator`] keeps the least or the greatest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Extremum {
	Min,
	Max,
}

/// array::min - the least of the group's values, by SurrealDB's value order.
///
/// The orderable counterpart to `math::min`, which folds only numbers. Ordering
/// is `Value`'s own `Ord`, the same comparison the scalar `array::min` uses, so
/// the aggregate and scalar forms cannot disagree about which value is least —
/// across strings, datetimes, durations, and mixed types alike.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayMin;

impl AggregateFunction for ArrayMin {
	fn name(&self) -> &'static str {
		"array::min"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::min", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ExtremumAccumulator::new(Extremum::Min))
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Any)
	}
}

/// array::max - the greatest of the group's values, by SurrealDB's value order.
///
/// See [`ArrayMin`] for the ordering contract.
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayMax;

impl AggregateFunction for ArrayMax {
	fn name(&self) -> &'static str {
		"array::max"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "array::max", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ExtremumAccumulator::new(Extremum::Max))
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Any)
	}
}

/// Running least-or-greatest value for [`ArrayMin`] and [`ArrayMax`].
///
/// Every value is orderable against every other, so unlike the numeric
/// aggregates there is no wrong-kind input to reject and no identity to
/// mistake for an answer: a group that folded nothing reports `NONE`, which is
/// what the scalar forms return for an empty array.
///
/// `NONE` and `NULL` are skipped rather than ordered. The scalar form would
/// order them (they sort below every other value, so a single missing field
/// would decide the whole group's minimum), but an aggregate that ignores nulls
/// is what SQL specifies and what a sparsely populated column needs.
#[derive(Debug, Clone)]
struct ExtremumAccumulator {
	extremum: Extremum,
	current: Option<Value>,
}

impl ExtremumAccumulator {
	fn new(extremum: Extremum) -> Self {
		Self {
			extremum,
			current: None,
		}
	}

	/// Fold one candidate into the running value.
	fn fold(&mut self, value: &Value) {
		if matches!(value, Value::None | Value::Null) {
			return;
		}
		let replace = match &self.current {
			None => true,
			Some(current) => match self.extremum {
				Extremum::Min => value < current,
				Extremum::Max => value > current,
			},
		};
		if replace {
			self.current = Some(value.clone());
		}
	}
}

impl Accumulator for ExtremumAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().option_value(&self.current).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.current = state.option_value()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		self.fold(&value);
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			self.fold(value);
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<ExtremumAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		// Both aggregates share this accumulator, so the downcast above does
		// not distinguish them; finalising a max as a min would silently return
		// the wrong end of the group.
		anyhow::ensure!(
			self.extremum == other.extremum,
			"Cannot merge a {:?} accumulator into a {:?} one",
			other.extremum,
			self.extremum
		);
		if let Some(value) = &other.current {
			self.fold(value);
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(self.current.clone().unwrap_or(Value::None))
	}

	fn reset(&mut self) {
		self.current = None;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::val::{Array, Number};

	// Helper to extract array from Value
	fn as_array(v: &Value) -> &Array {
		let Value::Array(a) = v else {
			panic!("Expected Array, got {:?}", v);
		};
		a
	}

	// Helper to extract string from Value
	fn as_string(v: &Value) -> &str {
		let Value::String(s) = v else {
			panic!("Expected String, got {:?}", v);
		};
		s.as_str()
	}

	// -------------------------------------------------------------------------
	// ArrayGroup tests
	// -------------------------------------------------------------------------

	#[test]
	fn array_group_zero_items() {
		let func = ArrayGroup;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(as_array(&result).is_empty());
	}

	#[test]
	fn array_group_single_item() {
		let func = ArrayGroup;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 1);
		assert_eq!(arr[0], Value::Number(Number::Int(42)));
	}

	#[test]
	fn array_group_multiple_items() {
		let func = ArrayGroup;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::String(Strand::new_static("hello"))).unwrap();
		acc.update(Value::Bool(true)).unwrap();
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 3);
		assert_eq!(arr[0], Value::Number(Number::Int(1)));
		assert_eq!(arr[1], Value::String(Strand::new_static("hello")));
		assert_eq!(arr[2], Value::Bool(true));
	}

	#[test]
	fn array_group_merge() {
		let func = ArrayGroup;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(2))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 2);
	}

	#[test]
	fn array_group_reset() {
		let func = ArrayGroup;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert!(as_array(&result).is_empty());
	}

	// -------------------------------------------------------------------------
	// ArrayJoin tests
	// -------------------------------------------------------------------------

	#[test]
	fn array_join_zero_items() {
		let func = ArrayJoin;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "");
	}

	#[test]
	fn array_join_single_item() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String(Strand::new_static("hello"))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "hello");
	}

	#[test]
	fn array_join_multiple_items_no_separator() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String(Strand::new_static("a"))).unwrap();
		acc.update(Value::String(Strand::new_static("b"))).unwrap();
		acc.update(Value::String(Strand::new_static("c"))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "abc");
	}

	#[test]
	fn array_join_with_separator() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String(Strand::new_static("a"))).unwrap();
		acc.update(Value::String(Strand::new_static("b"))).unwrap();
		acc.update(Value::String(Strand::new_static("c"))).unwrap();
		acc.set_extra_args(&[Value::String(Strand::new_static(", "))]);
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "a, b, c");
	}

	#[test]
	fn array_join_with_numbers() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.set_extra_args(&[Value::String(Strand::new_static("-"))]);
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "1-2-3");
	}

	#[test]
	fn array_join_merge() {
		let func = ArrayJoin;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::String(Strand::new_static("a"))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::String(Strand::new_static("b"))).unwrap();

		// The separator belongs to the merged group, not to either partial
		// accumulator, so it is bound after the merge.
		acc1.merge(acc2).unwrap();
		acc1.set_extra_args(&[Value::String(Strand::new_static(","))]);
		let result = acc1.finalize().unwrap();
		assert_eq!(as_string(&result), "a,b");
	}

	#[test]
	fn array_join_reset() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String(Strand::new_static("hello"))).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "");
	}

	// -------------------------------------------------------------------------
	// ArrayDistinct tests
	// -------------------------------------------------------------------------

	#[test]
	fn array_distinct_zero_items() {
		let func = ArrayDistinct;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(as_array(&result).is_empty());
	}

	#[test]
	fn array_distinct_single_item() {
		let func = ArrayDistinct;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 1);
		assert_eq!(arr[0], Value::Number(Number::Int(42)));
	}

	#[test]
	fn array_distinct_deduplicates() {
		let func = ArrayDistinct;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap(); // duplicate
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap(); // duplicate
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 3);
		// Should preserve insertion order
		assert_eq!(arr[0], Value::Number(Number::Int(1)));
		assert_eq!(arr[1], Value::Number(Number::Int(2)));
		assert_eq!(arr[2], Value::Number(Number::Int(3)));
	}

	#[test]
	fn array_distinct_mixed_types() {
		let func = ArrayDistinct;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::String(Strand::new_static("hello"))).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap(); // duplicate
		acc.update(Value::String(Strand::new_static("hello"))).unwrap(); // duplicate
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 2);
	}

	#[test]
	fn array_distinct_merge() {
		let func = ArrayDistinct;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();
		acc1.update(Value::Number(Number::Int(2))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(2))).unwrap(); // duplicate across accumulators
		acc2.update(Value::Number(Number::Int(3))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 3);
		assert_eq!(arr[0], Value::Number(Number::Int(1)));
		assert_eq!(arr[1], Value::Number(Number::Int(2)));
		assert_eq!(arr[2], Value::Number(Number::Int(3)));
	}

	#[test]
	fn array_distinct_reset() {
		let func = ArrayDistinct;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert!(as_array(&result).is_empty());
	}

	// -------------------------------------------------------------------------
	// ArrayMin / ArrayMax tests
	// -------------------------------------------------------------------------

	fn strand(s: &'static str) -> Value {
		Value::String(Strand::new_static(s))
	}

	#[test]
	fn orderable_extremes_order_any_comparable_value() {
		for (func, expected) in
			[(ArrayMin.create_accumulator(), "apple"), (ArrayMax.create_accumulator(), "pear")]
		{
			let mut acc = func;
			acc.update(strand("pear")).unwrap();
			acc.update(strand("apple")).unwrap();
			acc.update(strand("fig")).unwrap();
			assert_eq!(acc.finalize().unwrap(), strand(expected));
		}
	}

	#[test]
	fn orderable_extremes_report_none_for_an_empty_group() {
		// Not the numeric aggregates' `Infinity` identity, which a caller cannot
		// tell from a real extreme.
		assert_eq!(ArrayMin.create_accumulator().finalize().unwrap(), Value::None);
		assert_eq!(ArrayMax.create_accumulator().finalize().unwrap(), Value::None);
	}

	#[test]
	fn orderable_extremes_skip_absent_values_rather_than_ordering_them() {
		// `NONE` sorts below every other value, so ordering it would make the
		// minimum of any sparsely populated column `NONE`.
		let mut acc = ArrayMin.create_accumulator();
		acc.update(Value::None).unwrap();
		acc.update(strand("pear")).unwrap();
		acc.update(Value::Null).unwrap();
		assert_eq!(acc.finalize().unwrap(), strand("pear"));
	}

	#[test]
	fn orderable_extremes_merge_across_partial_groups() {
		let mut left = ArrayMin.create_accumulator();
		left.update(strand("pear")).unwrap();
		let mut right = ArrayMin.create_accumulator();
		right.update(strand("apple")).unwrap();

		left.merge(right.clone_box()).unwrap();
		assert_eq!(left.finalize().unwrap(), strand("apple"));
	}

	#[test]
	fn orderable_extremes_refuse_to_merge_opposite_ends() {
		// Both share one accumulator, so the downcast cannot tell them apart;
		// finalising a max as a min would silently return the wrong end.
		let mut min = ArrayMin.create_accumulator();
		min.update(strand("pear")).unwrap();
		let max = ArrayMax.create_accumulator();
		assert!(min.merge(max.clone_box()).is_err());
	}

	#[test]
	fn orderable_extremes_reset_forgets_the_running_value() {
		let mut acc = ArrayMax.create_accumulator();
		acc.update(strand("pear")).unwrap();
		acc.reset();
		assert_eq!(acc.finalize().unwrap(), Value::None);
	}
}
