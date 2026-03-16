//! Array aggregate functions.
//!
//! Provides aggregates for collecting values into arrays: group, join, and distinct.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::expr::Kind;
use crate::val::Value;

// ============================================================================
// ArrayGroup
// ============================================================================

/// array::group - collects values into an array
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayGroup;

impl AggregateFunction for ArrayGroup {
	fn name(&self) -> &'static str {
		"array::group"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(ArrayGroupAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Array(Box::new(Kind::Any), None))
	}
}

#[derive(Debug, Clone, Default)]
struct ArrayGroupAccumulator {
	values: Vec<Value>,
}

impl Accumulator for ArrayGroupAccumulator {
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
		Ok(Value::Array(self.values.clone().into()))
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

/// array::join - collects values and joins them with a separator
#[derive(Debug, Clone, Copy, Default)]
pub struct ArrayJoin;

impl AggregateFunction for ArrayJoin {
	fn name(&self) -> &'static str {
		"array::join"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		// Default accumulator with empty separator
		Box::new(ArrayJoinAccumulator {
			values: Vec::new(),
			separator: String::new(),
		})
	}

	fn create_accumulator_with_args(&self, args: &[Value]) -> Box<dyn Accumulator> {
		// Extract separator from extra args (first extra arg after the accumulated value)
		let separator = args.first().map(|v| v.clone().into_raw_string()).unwrap_or_default();
		Box::new(ArrayJoinAccumulator {
			values: Vec::new(),
			separator,
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
	fn update(&mut self, value: Value) -> Result<()> {
		self.values.push(value);
		Ok(())
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
		Ok(Value::String(joined))
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{Array, Number};

	// Helper to extract array from Value
	fn as_array(v: &Value) -> &Array {
		match v {
			Value::Array(a) => a,
			_ => panic!("Expected Array, got {:?}", v),
		}
	}

	// Helper to extract string from Value
	fn as_string(v: &Value) -> &str {
		match v {
			Value::String(s) => s.as_str(),
			_ => panic!("Expected String, got {:?}", v),
		}
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
		acc.update(Value::String("hello".into())).unwrap();
		acc.update(Value::Bool(true)).unwrap();
		let result = acc.finalize().unwrap();
		let arr = as_array(&result);
		assert_eq!(arr.len(), 3);
		assert_eq!(arr[0], Value::Number(Number::Int(1)));
		assert_eq!(arr[1], Value::String("hello".into()));
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
		acc.update(Value::String("hello".into())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "hello");
	}

	#[test]
	fn array_join_multiple_items_no_separator() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String("a".into())).unwrap();
		acc.update(Value::String("b".into())).unwrap();
		acc.update(Value::String("c".into())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "abc");
	}

	#[test]
	fn array_join_with_separator() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator_with_args(&[Value::String(", ".into())]);
		acc.update(Value::String("a".into())).unwrap();
		acc.update(Value::String("b".into())).unwrap();
		acc.update(Value::String("c".into())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "a, b, c");
	}

	#[test]
	fn array_join_with_numbers() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator_with_args(&[Value::String("-".into())]);
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_string(&result), "1-2-3");
	}

	#[test]
	fn array_join_merge() {
		let func = ArrayJoin;
		let mut acc1 = func.create_accumulator_with_args(&[Value::String(",".into())]);
		acc1.update(Value::String("a".into())).unwrap();

		let mut acc2 = func.create_accumulator_with_args(&[Value::String(",".into())]);
		acc2.update(Value::String("b".into())).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_string(&result), "a,b");
	}

	#[test]
	fn array_join_reset() {
		let func = ArrayJoin;
		let mut acc = func.create_accumulator();
		acc.update(Value::String("hello".into())).unwrap();
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
		acc.update(Value::String("hello".into())).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap(); // duplicate
		acc.update(Value::String("hello".into())).unwrap(); // duplicate
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
}
