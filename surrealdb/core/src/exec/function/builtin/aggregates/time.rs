//! Time aggregate functions.
//!
//! Provides aggregates for datetime operations: min and max.

use anyhow::Result;

use super::Rejected;
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
	rejected: Rejected,
}

impl Accumulator for TimeMinAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(d) = self.rejected.datetime("time::min", &value) {
			self.min = Some(match &self.min {
				None => d,
				Some(current) => {
					if d < *current {
						d
					} else {
						*current
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
		self.rejected.merge_from(&other.rejected);
		if let Some(other_min) = &other.min {
			self.min = Some(match &self.min {
				None => *other_min,
				Some(current) => {
					if *other_min < *current {
						*other_min
					} else {
						*current
					}
				}
			});
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		// Streaming executor only. The materialised-view path
		// (`catalog/aggregation.rs`) still emits `Datetime::MAX_UTC` for empty
		// groups; aligning it requires bumping `AggregationStat`'s revision.
		self.rejected.check()?;
		match &self.min {
			Some(d) => Ok(Value::Datetime(*d)),
			None => Ok(Value::None),
		}
	}

	fn reset(&mut self) {
		self.min = None;
		self.rejected.reset();
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
	rejected: Rejected,
}

impl Accumulator for TimeMaxAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(d) = self.rejected.datetime("time::max", &value) {
			self.max = Some(match &self.max {
				None => d,
				Some(current) => {
					if d > *current {
						d
					} else {
						*current
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
		self.rejected.merge_from(&other.rejected);
		if let Some(other_max) = &other.max {
			self.max = Some(match &self.max {
				None => *other_max,
				Some(current) => {
					if *other_max > *current {
						*other_max
					} else {
						*current
					}
				}
			});
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		// Streaming executor only. The materialised-view path
		// (`catalog/aggregation.rs`) still emits `Datetime::MIN_UTC` for empty
		// groups; aligning it requires bumping `AggregationStat`'s revision.
		self.rejected.check()?;
		match &self.max {
			Some(d) => Ok(Value::Datetime(*d)),
			None => Ok(Value::None),
		}
	}

	fn reset(&mut self) {
		self.max = None;
		self.rejected.reset();
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
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_min_single_item() {
		let func = TimeMin;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
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
		acc.update(Value::Datetime(dt2)).unwrap();
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
		acc2.update(Value::Datetime(dt2)).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt2);
	}

	#[test]
	fn time_min_merge_empty() {
		let func = TimeMin;
		let mut acc1 = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt)).unwrap();

		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_min_merge_into_empty() {
		// Exercise the `self.min == None, other.min == Some` branch of merge.
		let func = TimeMin;
		let mut acc1 = func.create_accumulator();

		let mut acc2 = func.create_accumulator();
		let dt = make_datetime(2024, 1, 1);
		acc2.update(Value::Datetime(dt)).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_min_merge_both_empty() {
		let func = TimeMin;
		let mut acc1 = func.create_accumulator();
		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_min_reset() {
		let func = TimeMin;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_min_skips_non_datetime_only_when_something_folded() {
		// A value of the wrong kind is tolerated when the group has an answer
		// without it: the datetime folds and the array is ignored.
		let func = TimeMin;
		let sample = Value::Datetime(Datetime::from(Utc.timestamp_opt(1_700_000_000, 0).unwrap()));
		let mut acc = func.create_accumulator();
		acc.update(Value::Array(Default::default())).unwrap();
		acc.update(Value::None).unwrap();
		acc.update(sample.clone()).unwrap();
		assert_eq!(acc.finalize().unwrap(), sample);

		// `NONE` alone is absent rather than wrong, so it still finalises to NONE.
		let mut absent = func.create_accumulator();
		absent.update(Value::None).unwrap();
		assert_eq!(absent.finalize().unwrap(), Value::None);

		// With nothing folded, reporting NONE would present the absence of an
		// answer as an answer, so the wrong-kind value raises instead.
		let mut wrong = func.create_accumulator();
		wrong.update(Value::Array(Default::default())).unwrap();
		let err = wrong.finalize().unwrap_err().to_string();
		assert!(
			err.contains("time::min") && err.contains("Expected `datetime`"),
			"unexpected error: {err}"
		);
	}

	// -------------------------------------------------------------------------
	// TimeMax tests
	// -------------------------------------------------------------------------

	#[test]
	fn time_max_zero_items() {
		let func = TimeMax;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_max_single_item() {
		let func = TimeMax;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
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
		acc.update(Value::Datetime(dt3)).unwrap();
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
		acc2.update(Value::Datetime(dt2)).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt2);
	}

	#[test]
	fn time_max_merge_empty() {
		let func = TimeMax;
		let mut acc1 = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc1.update(Value::Datetime(dt)).unwrap();

		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_max_merge_into_empty() {
		// Exercise the `self.max == None, other.max == Some` branch of merge.
		let func = TimeMax;
		let mut acc1 = func.create_accumulator();

		let mut acc2 = func.create_accumulator();
		let dt = make_datetime(2024, 12, 31);
		acc2.update(Value::Datetime(dt)).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(*as_datetime(&result), dt);
	}

	#[test]
	fn time_max_merge_both_empty() {
		let func = TimeMax;
		let mut acc1 = func.create_accumulator();
		let acc2 = func.create_accumulator();
		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_max_reset() {
		let func = TimeMax;
		let mut acc = func.create_accumulator();
		let dt = make_datetime(2024, 6, 15);
		acc.update(Value::Datetime(dt)).unwrap();
		acc.reset();
		let result = acc.finalize().unwrap();
		assert_eq!(result, Value::None);
	}

	#[test]
	fn time_max_skips_non_datetime_only_when_something_folded() {
		// A value of the wrong kind is tolerated when the group has an answer
		// without it: the datetime folds and the array is ignored.
		let func = TimeMax;
		let sample = Value::Datetime(Datetime::from(Utc.timestamp_opt(1_700_000_000, 0).unwrap()));
		let mut acc = func.create_accumulator();
		acc.update(Value::Array(Default::default())).unwrap();
		acc.update(Value::None).unwrap();
		acc.update(sample.clone()).unwrap();
		assert_eq!(acc.finalize().unwrap(), sample);

		// `NONE` alone is absent rather than wrong, so it still finalises to NONE.
		let mut absent = func.create_accumulator();
		absent.update(Value::None).unwrap();
		assert_eq!(absent.finalize().unwrap(), Value::None);

		// With nothing folded, reporting NONE would present the absence of an
		// answer as an answer, so the wrong-kind value raises instead.
		let mut wrong = func.create_accumulator();
		wrong.update(Value::Array(Default::default())).unwrap();
		let err = wrong.finalize().unwrap_err().to_string();
		assert!(
			err.contains("time::max") && err.contains("Expected `datetime`"),
			"unexpected error: {err}"
		);
	}
}
