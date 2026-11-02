//! Time aggregate functions.
//!
//! Provides aggregates over the temporal kinds: min and max of `date`, `time`, `timestamp` or
//! `datetime` values, one kind per group, answering that kind.

use anyhow::Result;

use super::Rejected;
use super::saved::{Loading, Saving};
use crate::exec::function::{Accumulator, AggregateFunction, ReturnRule, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::val::Value;

/// The kinds a `time::min` or `time::max` argument may have.
fn temporal_kind() -> Kind {
	Kind::Either(vec![Kind::Datetime, Kind::Date, Kind::Time, Kind::Timestamp])
}

/// Whether `candidate` replaces `held`, a value of the same temporal kind, in a fold that keeps the
/// smaller (`smallest`) or the larger value.
fn replaces(candidate: &Value, held: &Value, smallest: bool) -> bool {
	if smallest {
		candidate < held
	} else {
		candidate > held
	}
}

/// Folds `candidate` into the value `held` keeps, the smallest (`smallest`) or the largest of a
/// group's values of one temporal kind. A value of another kind is recorded in `rejected`.
fn fold(
	rejected: &mut Rejected,
	func: &str,
	candidate: &Value,
	held: &mut Option<Value>,
	smallest: bool,
) {
	if let Some(v) = rejected.temporal(func, candidate, held.as_ref())
		&& held.as_ref().is_none_or(|current| replaces(&v, current, smallest))
	{
		*held = Some(v);
	}
}

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

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "time::min", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(TimeMinAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("value", temporal_kind())
			.returns(Kind::Datetime)
			.rule(ReturnRule::SameAsArg(0))
	}
}

#[derive(Debug, Clone, Default)]
struct TimeMinAccumulator {
	min: Option<Value>,
	rejected: Rejected,
}

impl Accumulator for TimeMinAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().option_value(&self.min).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.min = state.option_value()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		fold(&mut self.rejected, "time::min", &value, &mut self.min, true);
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<TimeMinAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.rejected.merge_from(&other.rejected);
		if let Some(other_min) = &other.min {
			fold(&mut self.rejected, "time::min", other_min, &mut self.min, true);
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		// An empty group returns NONE. The materialised-view path in
		// `catalog/aggregation.rs` returns `Datetime::MAX_UTC` for empty groups
		// instead. Changing that path needs a new revision of `AggregationStat`.
		self.rejected.check()?;
		Ok(self.min.clone().unwrap_or(Value::None))
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

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "time::max", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(TimeMaxAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("value", temporal_kind())
			.returns(Kind::Datetime)
			.rule(ReturnRule::SameAsArg(0))
	}
}

#[derive(Debug, Clone, Default)]
struct TimeMaxAccumulator {
	max: Option<Value>,
	rejected: Rejected,
}

impl Accumulator for TimeMaxAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().option_value(&self.max).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.max = state.option_value()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		fold(&mut self.rejected, "time::max", &value, &mut self.max, false);
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<TimeMaxAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.rejected.merge_from(&other.rejected);
		if let Some(other_max) = &other.max {
			fold(&mut self.rejected, "time::max", other_max, &mut self.max, false);
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		// An empty group returns NONE. The materialised-view path in
		// `catalog/aggregation.rs` returns `Datetime::MIN_UTC` for empty groups
		// instead. Changing that path needs a new revision of `AggregationStat`.
		self.rejected.check()?;
		Ok(self.max.clone().unwrap_or(Value::None))
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
	use crate::val::{Date, Datetime, Time, Timestamp};

	// Helper to extract Datetime from Value
	fn as_datetime(v: &Value) -> &Datetime {
		let Value::Datetime(d) = v else {
			panic!("Expected Datetime, got {:?}", v);
		};
		d
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
		// Covers merge when self has no minimum and other has one.
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
		// Covers merge when self has no maximum and other has one.
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

	// -------------------------------------------------------------------------
	// The other temporal kinds
	// -------------------------------------------------------------------------

	fn date(days: i32) -> Value {
		Value::Date(Date::from_days(days).expect("a date"))
	}

	#[test]
	fn min_and_max_of_dates_answer_a_date() {
		let (min, max) = (TimeMin, TimeMax);
		let (mut lo, mut hi) = (min.create_accumulator(), max.create_accumulator());
		for days in [20_000, 19_000, 21_000] {
			lo.update(date(days)).unwrap();
			hi.update(date(days)).unwrap();
		}
		assert_eq!(lo.finalize().unwrap(), date(19_000));
		assert_eq!(hi.finalize().unwrap(), date(21_000));
	}

	#[test]
	fn min_and_max_of_times_and_timestamps_answer_their_own_kind() {
		let time = |n: u64| Value::Time(Time::from_nanos(n).expect("a time"));
		let stamp = |s: i64| Value::Timestamp(Timestamp::from_parts(s, 0).expect("a timestamp"));
		let mut lo = TimeMin.create_accumulator();
		let mut hi = TimeMax.create_accumulator();
		for n in [5, 1, 9] {
			lo.update(time(n)).unwrap();
			hi.update(time(n)).unwrap();
		}
		assert_eq!(lo.finalize().unwrap(), time(1));
		assert_eq!(hi.finalize().unwrap(), time(9));
		let mut lo = TimeMin.create_accumulator();
		for s in [50, 10, 90] {
			lo.update(stamp(s)).unwrap();
		}
		assert_eq!(lo.finalize().unwrap(), stamp(10));
	}

	#[test]
	fn a_value_of_another_temporal_kind_is_skipped_once_the_group_holds_one() {
		let mut acc = TimeMin.create_accumulator();
		acc.update(date(5)).unwrap();
		acc.update(Value::Datetime(make_datetime(1970, 1, 2))).unwrap();
		acc.update(date(3)).unwrap();
		assert_eq!(acc.finalize().unwrap(), date(3));
	}

	#[test]
	fn a_group_with_no_temporal_value_is_refused() {
		let mut acc = TimeMax.create_accumulator();
		acc.update(Value::String("2024-01-01".into())).unwrap();
		assert!(acc.finalize().is_err());
	}

	#[test]
	fn a_saved_date_state_loads_back() {
		let mut acc = TimeMax.create_accumulator();
		acc.update(date(7)).unwrap();
		let state = acc.save().expect("a state");
		let mut loaded = TimeMax.create_accumulator();
		loaded.load(state).unwrap();
		assert_eq!(loaded.finalize().unwrap(), date(7));
	}
}
