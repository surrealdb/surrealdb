//! Time aggregate functions.
//!
//! Provides aggregates for datetime operations: min and max.

use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::expr::Kind;
use crate::val::{Datetime, Value};

// ============================================================================
// TimeMin
// ============================================================================

/// time::min - finds minimum datetime value
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeMin;

impl AggregateFunction for TimeMin {
	fn name(&self) -> &'static str {
		"time::min"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(TimeMinAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Datetime).returns(Kind::Datetime)
	}
}

#[derive(Debug, Clone, Default)]
struct TimeMinAccumulator {
	min: Option<Datetime>,
}

impl Accumulator for TimeMinAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Datetime(d) = value {
			self.min = Some(match &self.min {
				None => d,
				Some(current) => {
					if d < *current {
						d
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<TimeMinAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		if let Some(other_min) = &other.min {
			self.min = Some(match &self.min {
				None => other_min.clone(),
				Some(current) => {
					if *other_min < *current {
						other_min.clone()
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		match &self.min {
			Some(d) => Ok(Value::Datetime(d.clone())),
			None => Ok(Value::Datetime(Datetime::MAX_UTC)),
		}
	}

	fn reset(&mut self) {
		self.min = None;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// TimeMax
// ============================================================================

/// time::max - finds maximum datetime value
#[derive(Debug, Clone, Copy, Default)]
pub struct TimeMax;

impl AggregateFunction for TimeMax {
	fn name(&self) -> &'static str {
		"time::max"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(TimeMaxAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Datetime).returns(Kind::Datetime)
	}
}

#[derive(Debug, Clone, Default)]
struct TimeMaxAccumulator {
	max: Option<Datetime>,
}

impl Accumulator for TimeMaxAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Datetime(d) = value {
			self.max = Some(match &self.max {
				None => d,
				Some(current) => {
					if d > *current {
						d
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<TimeMaxAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		if let Some(other_max) = &other.max {
			self.max = Some(match &self.max {
				None => other_max.clone(),
				Some(current) => {
					if *other_max > *current {
						other_max.clone()
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		match &self.max {
			Some(d) => Ok(Value::Datetime(d.clone())),
			None => Ok(Value::Datetime(Datetime::MIN_UTC)),
		}
	}

	fn reset(&mut self) {
		self.max = None;
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
	use chrono::{TimeZone, Utc};

	use super::*;

	// Helper to extract Datetime from Value
	fn as_datetime(v: &Value) -> &Datetime {
		match v {
			Value::Datetime(d) => d,
			_ => panic!("Expected Datetime, got {:?}", v),
		}
	}

	// Helper to create a test datetime
	fn make_datetime(year: i32, month: u32, day: u32) -> Datetime {
		Datetime::from(Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).unwrap())
	}

	// -------------------------------------------------------------------------
	// TimeMin tests
	// -------------------------------------------------------------------------

	#[test]
	fn time_min_zero_items() {
		let func = TimeMin;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), Datetime::MAX_UTC);
	}

	#[test]
	fn time_min_single_item() {
		let func = TimeMin;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt.clone())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_min_multiple_items() {
		let func = TimeMin;
		let mut acc = func.create_accumulator();
		let dt1 = make_datetime(2024, 6, 15);
		let dt2 = make_datetime(2024, 1, 1);
		let dt3 = make_datetime(2024, 12, 31);
		acc.update(Value::Datetime(dt1)).unwrap();
		acc.update(Value::Datetime(dt2.clone())).unwrap();
		acc.update(Value::Datetime(dt3)).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt2);
	}

	#[test]
	fn time_min_merge() {
		let func = TimeMin;
		let mut acc1 = func.create_accumulator();
		let dt1 = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt1)).unwrap();

		let mut acc2 = func.create_accumulator();
		let dt2 = make_datetime(2024, 1, 1);
		acc2.update(Value::Datetime(dt2.clone())).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt2);
	}

	#[test]
	fn time_min_merge_empty() {
		let func = TimeMin;
		let mut acc1 = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt.clone())).unwrap();

		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_min_reset() {
		let func = TimeMin;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), Datetime::MAX_UTC);
	}

	// -------------------------------------------------------------------------
	// TimeMax tests
	// -------------------------------------------------------------------------

	#[test]
	fn time_max_zero_items() {
		let func = TimeMax;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), Datetime::MIN_UTC);
	}

	#[test]
	fn time_max_single_item() {
		let func = TimeMax;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt.clone())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_max_multiple_items() {
		let func = TimeMax;
		let mut acc = func.create_accumulator();
		let dt1 = make_datetime(2024, 6, 15);
		let dt2 = make_datetime(2024, 1, 1);
		let dt3 = make_datetime(2024, 12, 31);
		acc.update(Value::Datetime(dt1)).unwrap();
		acc.update(Value::Datetime(dt2)).unwrap();
		acc.update(Value::Datetime(dt3.clone())).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt3);
	}

	#[test]
	fn time_max_merge() {
		let func = TimeMax;
		let mut acc1 = func.create_accumulator();
		let dt1 = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt1)).unwrap();

		let mut acc2 = func.create_accumulator();
		let dt2 = make_datetime(2024, 12, 31);
		acc2.update(Value::Datetime(dt2.clone())).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt2);
	}

	#[test]
	fn time_max_merge_empty() {
		let func = TimeMax;
		let mut acc1 = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt.clone())).unwrap();

		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_max_reset() {
		let func = TimeMax;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(*as_datetime(&result), Datetime::MIN_UTC);
	}
}
