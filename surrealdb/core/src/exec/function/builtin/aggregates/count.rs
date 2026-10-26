//! Count aggregate functions.
//!
//! Provides `COUNT()` for counting all rows, `COUNT(field)` for counting
//! truthy values, and `count::present` for counting the values that are
//! neither `NULL` nor `NONE`.

use anyhow::Result;

use super::saved::{Loading, Saving};
use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::val::{Number, Value};

// ============================================================================
// Count (no args)
// ============================================================================

/// COUNT() - counts all rows in a group
#[derive(Debug, Clone, Copy, Default)]
pub struct Count;

impl AggregateFunction for Count {
	fn name(&self) -> &'static str {
		"count"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "count", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(CountAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::Count(CountAccumulator::default())
	}

	fn signature(&self) -> Signature {
		// count() takes no arguments. Evaluation passes a placeholder value that update() ignores.
		Signature::new().returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CountAccumulator {
	count: i64,
}

impl CountAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.count += other.count;
	}
}

impl Accumulator for CountAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().int(self.count).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.count = state.int()?;
		Ok(())
	}

	fn update(&mut self, _value: Value) -> Result<()> {
		self.count += 1;
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		self.count += values.len() as i64;
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<CountAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(Number::Int(self.count)))
	}

	fn reset(&mut self) {
		self.count = 0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// CountField (with field arg)
// ============================================================================

/// COUNT(field) - counts truthy (non-null, non-false) values
#[derive(Debug, Clone, Copy, Default)]
pub struct CountField;

impl AggregateFunction for CountField {
	fn name(&self) -> &'static str {
		"count"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "count", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(CountFieldAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::CountField(CountFieldAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CountFieldAccumulator {
	count: i64,
}

impl CountFieldAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.count += other.count;
	}
}

impl Accumulator for CountFieldAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().int(self.count).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.count = state.int()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		// Only truthy values are counted. None and false are skipped.
		if value.is_truthy() {
			self.count += 1;
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		self.count += values.iter().filter(|v| v.is_truthy()).count() as i64;
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<CountFieldAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(Number::Int(self.count)))
	}

	fn reset(&mut self) {
		self.count = 0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// CountPresent (SQL `COUNT(column)`)
// ============================================================================

/// `count::present`: the number of values of the group that are neither `NULL`
/// nor `NONE`, which is SQL's `COUNT(column)`. Not callable from SurrealQL; the
/// SQL front ends name it so that the folded argument is the column as
/// written, with no per-row test around it.
#[derive(Debug, Clone, Copy, Default)]
pub struct CountPresent;

impl AggregateFunction for CountPresent {
	fn name(&self) -> &'static str {
		"count::present"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "count::present", Fallibility::INFALLIBLE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(CountPresentAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::CountPresent(CountPresentAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CountPresentAccumulator {
	count: i64,
}

impl CountPresentAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.count += other.count;
	}
}

impl Accumulator for CountPresentAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().int(self.count).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.count = state.int()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if !value.is_nullish() {
			self.count += 1;
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		self.count += values.iter().filter(|v| !v.is_nullish()).count() as i64;
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<CountPresentAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(Number::Int(self.count)))
	}

	fn reset(&mut self) {
		self.count = 0;
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

	// Helper to extract i64 from Value
	fn as_int(v: &Value) -> i64 {
		let Value::Number(Number::Int(i)) = v else {
			panic!("Expected Int, got {:?}", v);
		};
		*i
	}

	// -------------------------------------------------------------------------
	// Count tests
	// -------------------------------------------------------------------------

	#[test]
	fn count_zero_items() {
		let func = Count;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn count_single_item() {
		let func = Count;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 1);
	}

	#[test]
	fn count_multiple_items() {
		let func = Count;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	#[test]
	fn count_merge() {
		let func = Count;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();
		acc1.update(Value::Number(Number::Int(2))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(3))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	#[test]
	fn count_reset() {
		let func = Count;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	// -------------------------------------------------------------------------
	// CountField tests
	// -------------------------------------------------------------------------

	#[test]
	fn count_field_zero_items() {
		let func = CountField;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn count_field_truthy_value() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 1);
	}

	#[test]
	fn count_field_falsy_values() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		// None is falsy
		acc.update(Value::None).unwrap();
		// False is falsy
		acc.update(Value::Bool(false)).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn count_field_mixed_values() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap(); // truthy
		acc.update(Value::None).unwrap(); // falsy
		acc.update(Value::String(Strand::new_static("hello"))).unwrap(); // truthy
		acc.update(Value::Bool(false)).unwrap(); // falsy
		acc.update(Value::Bool(true)).unwrap(); // truthy
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	#[test]
	fn count_field_merge() {
		let func = CountField;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();
		acc1.update(Value::None).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::String(Strand::new_static("test"))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_int(&result), 2);
	}

	// -------------------------------------------------------------------------
	// Count update_batch tests
	// -------------------------------------------------------------------------

	#[test]
	fn count_batch_empty() {
		let func = Count;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn count_batch_multiple() {
		let func = Count;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(1)),
			Value::None,
			Value::String(Strand::new_static("test")),
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	#[test]
	fn count_batch_then_single() {
		let func = Count;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	// -------------------------------------------------------------------------
	// CountField update_batch tests
	// -------------------------------------------------------------------------

	#[test]
	fn count_field_batch_empty() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn count_field_batch_mixed() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(1)),              // truthy
			Value::None,                                // falsy
			Value::String(Strand::new_static("hello")), // truthy
			Value::Bool(false),                         // falsy
			Value::Bool(true),                          // truthy
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 3);
	}

	#[test]
	fn count_field_batch_then_single() {
		let func = CountField;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(1)), Value::None];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Bool(true)).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 2);
	}

	// -------------------------------------------------------------------------
	// CountPresent tests
	// -------------------------------------------------------------------------

	#[test]
	fn count_present_skips_null_and_none_only() {
		let mut acc = CountPresent.create_accumulator();
		for value in [
			Value::Null,
			Value::None,
			Value::Bool(false),
			Value::Number(Number::Int(0)),
			Value::String(Strand::new_static("")),
			Value::Bool(true),
		] {
			acc.update(value).unwrap();
		}
		assert_eq!(as_int(&acc.finalize().unwrap()), 4);
	}

	#[test]
	fn count_present_merge() {
		let mut acc1 = CountPresent.create_accumulator();
		acc1.update(Value::Number(Number::Int(0))).unwrap();
		acc1.update(Value::Null).unwrap();
		let mut acc2 = CountPresent.create_accumulator();
		acc2.update(Value::Bool(false)).unwrap();
		acc1.merge(acc2).unwrap();
		assert_eq!(as_int(&acc1.finalize().unwrap()), 2);
	}
}
