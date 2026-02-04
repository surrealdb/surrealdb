//! Count aggregate functions.
//!
//! Provides `COUNT()` for counting all rows and `COUNT(field)` for counting
//! truthy values.

use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
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

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(CountAccumulator::default())
	}

	fn signature(&self) -> Signature {
		// count() takes no arguments - we pass a dummy value during evaluation
		Signature::new().returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
struct CountAccumulator {
	count: i64,
}

impl Accumulator for CountAccumulator {
	fn update(&mut self, _value: Value) -> Result<()> {
		self.count += 1;
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<CountAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.count += other.count;
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

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(CountFieldAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
struct CountFieldAccumulator {
	count: i64,
}

impl Accumulator for CountFieldAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		// Count truthy values (matches old behavior)
		if value.is_truthy() {
			self.count += 1;
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<CountFieldAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.count += other.count;
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
	use super::*;

	// Helper to extract i64 from Value
	fn as_int(v: &Value) -> i64 {
		match v {
			Value::Number(Number::Int(i)) => *i,
			_ => panic!("Expected Int, got {:?}", v),
		}
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
		acc.update(Value::String("hello".into())).unwrap(); // truthy
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
		acc2.update(Value::String("test".into())).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_int(&result), 2);
	}
}
