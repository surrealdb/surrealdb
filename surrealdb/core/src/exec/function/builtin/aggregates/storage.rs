use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::expr::Kind;
use crate::val::{Number, Value};

#[derive(Debug, Clone, Copy, Default)]
pub struct Storage;

impl AggregateFunction for Storage {
	fn name(&self) -> &'static str {
		"storage"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(StorageAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
struct StorageAccumulator {
	total_bytes: i64,
}

impl Accumulator for StorageAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		let bytes = revision::to_vec(&value)?;
		self.total_bytes += bytes.len() as i64;
		Ok(())
	}

	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			let bytes = revision::to_vec(value)?;
			self.total_bytes += bytes.len() as i64;
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<StorageAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.total_bytes += other.total_bytes;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(Number::Int(self.total_bytes)))
	}

	fn reset(&mut self) {
		self.total_bytes = 0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StorageField;

impl AggregateFunction for StorageField {
	fn name(&self) -> &'static str {
		"storage"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(StorageFieldAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Any).returns(Kind::Int)
	}
}

#[derive(Debug, Clone, Default)]
struct StorageFieldAccumulator {
	total_bytes: i64,
}

impl Accumulator for StorageFieldAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		let bytes = revision::to_vec(&value)?;
		self.total_bytes += bytes.len() as i64;
		Ok(())
	}

	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			let bytes = revision::to_vec(value)?;
			self.total_bytes += bytes.len() as i64;
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<StorageFieldAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.total_bytes += other.total_bytes;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(Number::Int(self.total_bytes)))
	}

	fn reset(&mut self) {
		self.total_bytes = 0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn as_int(v: &Value) -> i64 {
		match v {
			Value::Number(Number::Int(i)) => *i,
			_ => panic!("Expected Int, got {:?}", v),
		}
	}

	fn byte_size(v: &Value) -> i64 {
		revision::to_vec(v).unwrap().len() as i64
	}

	#[test]
	fn storage_zero_items() {
		let func = Storage;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn storage_single_item() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		let val = Value::Number(Number::Int(42));
		let expected = byte_size(&val);
		acc.update(val).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_multiple_items() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		let v1 = Value::Number(Number::Int(1));
		let v2 = Value::String("hello".into());
		let v3 = Value::Bool(true);
		let expected = byte_size(&v1) + byte_size(&v2) + byte_size(&v3);
		acc.update(v1).unwrap();
		acc.update(v2).unwrap();
		acc.update(v3).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_merge() {
		let func = Storage;
		let mut acc1 = func.create_accumulator();
		let v1 = Value::Number(Number::Int(1));
		let v2 = Value::Number(Number::Int(2));
		let expected1 = byte_size(&v1) + byte_size(&v2);
		acc1.update(v1).unwrap();
		acc1.update(v2).unwrap();

		let mut acc2 = func.create_accumulator();
		let v3 = Value::String("test".into());
		let expected2 = byte_size(&v3);
		acc2.update(v3).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_int(&result), expected1 + expected2);
	}

	#[test]
	fn storage_reset() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn storage_field_zero_items() {
		let func = StorageField;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn storage_field_single_value() {
		let func = StorageField;
		let mut acc = func.create_accumulator();
		let val = Value::String("hello world".into());
		let expected = byte_size(&val);
		acc.update(val).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_field_none_value() {
		let func = StorageField;
		let mut acc = func.create_accumulator();
		let val = Value::None;
		let expected = byte_size(&val);
		acc.update(val).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_field_mixed_values() {
		let func = StorageField;
		let mut acc = func.create_accumulator();
		let v1 = Value::Number(Number::Int(1));
		let v2 = Value::None;
		let v3 = Value::String("hello".into());
		let expected = byte_size(&v1) + byte_size(&v2) + byte_size(&v3);
		acc.update(v1).unwrap();
		acc.update(v2).unwrap();
		acc.update(v3).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_field_merge() {
		let func = StorageField;
		let mut acc1 = func.create_accumulator();
		let v1 = Value::Number(Number::Int(1));
		let expected1 = byte_size(&v1);
		acc1.update(v1).unwrap();

		let mut acc2 = func.create_accumulator();
		let v2 = Value::String("test".into());
		let expected2 = byte_size(&v2);
		acc2.update(v2).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_int(&result), expected1 + expected2);
	}

	#[test]
	fn storage_batch_empty() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), 0);
	}

	#[test]
	fn storage_batch_multiple() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(1)), Value::None, Value::String("test".into())];
		let expected: i64 = values.iter().map(byte_size).sum();
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_batch_then_single() {
		let func = Storage;
		let mut acc = func.create_accumulator();
		let batch_values = vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))];
		let single_value = Value::Number(Number::Int(3));
		let expected: i64 =
			batch_values.iter().map(byte_size).sum::<i64>() + byte_size(&single_value);
		acc.update_batch(&batch_values).unwrap();
		acc.update(single_value).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}

	#[test]
	fn storage_field_batch_mixed() {
		let func = StorageField;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(1)),
			Value::None,
			Value::String("hello".into()),
			Value::Bool(false),
			Value::Bool(true),
		];
		let expected: i64 = values.iter().map(byte_size).sum();
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_int(&result), expected);
	}
}
