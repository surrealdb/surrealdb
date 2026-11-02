//! Distinct numeric aggregates: `math::sum_distinct` and `math::mean_distinct`.
//!
//! Each folds every distinct number of a group once, into the accumulator of the plain aggregate
//! of the same kind, in the order the numbers first appear. Equal numbers are one number whatever
//! their type: `1`, `1.0` and `1dec` are the same value, and the first of them to appear is the
//! one folded. As for the plain aggregates, a value that is not a number, `NULL` and `NONE`
//! included, is skipped, and an array is a value that is not a number, whatever it holds.
//!
//! An aggregate cannot take another aggregate's answer as its argument, so a distinct sum is not
//! the plain sum of `array::group`: distinctness is part of the accumulator. These functions are
//! not callable from SurrealQL, which has no `DISTINCT` aggregate syntax; the SQL front ends name
//! them for `SUM(DISTINCT x)` and `AVG(DISTINCT x)`.

use std::collections::HashSet;

use anyhow::Result;

use super::math::{MeanAccumulator, SumAccumulator};
use super::saved::{Loading, Saving};
use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::val::{Number, Value};

/// `math::sum_distinct`: the sum of the distinct numbers of the group, or `NULL` where it holds
/// none. An integer total that leaves `i64` fails the group, as the plain sum's does.
#[derive(Debug, Clone, Copy, Default)]
pub struct MathSumDistinct;

impl AggregateFunction for MathSumDistinct {
	fn name(&self) -> &'static str {
		"math::sum_distinct"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::sum_distinct", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(DistinctAccumulator::<SumAccumulator>::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// `math::mean_distinct`: the mean, as a float, of the distinct numbers of the group, or `NULL`
/// where it holds none. The total is exact, as the plain mean's is.
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMeanDistinct;

impl AggregateFunction for MathMeanDistinct {
	fn name(&self) -> &'static str {
		"math::mean_distinct"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::mean_distinct", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(DistinctAccumulator::<MeanAccumulator>::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// The distinct numbers of a group, folded once each into `inner` as they first appear.
///
/// The group keeps one copy of each distinct number, so its size grows with the number of
/// distinct numbers and not with the number of rows. The operator that runs the accumulator
/// counts every value an update is handed against the query's memory (`holds_inputs`), repeats
/// included, so the count is never below what the group holds.
///
/// The three fields always agree: `seen` holds the numbers of `numbers`, and `inner` is the state a
/// fresh accumulator reaches by folding `numbers` in order. An accumulator that holds no number is
/// a fresh one. A `fold` or a `load` that fails leaves the accumulator as it was.
#[derive(Debug, Clone, Default)]
pub(crate) struct DistinctAccumulator<A> {
	/// The distinct numbers in the order each first appeared, which is the order they were folded
	/// into `inner` and the state a spilled group saves.
	numbers: Vec<Number>,
	/// The same numbers, to find out whether one has appeared.
	seen: HashSet<Number>,
	inner: A,
}

impl<A: Accumulator> DistinctAccumulator<A> {
	/// Fold `number` into `inner` unless an equal number was folded before.
	///
	/// The number is recorded only once `inner` has folded it: `numbers` takes it after the update
	/// succeeds, and `seen` gives it up where the update fails. A fold that fails therefore leaves
	/// the accumulator as it was, provided `inner.update` leaves `inner` unchanged when it fails.
	fn fold(&mut self, number: Number) -> Result<()> {
		if self.seen.insert(number.clone()) {
			if let Err(e) = self.inner.update(Value::Number(number.clone())) {
				self.seen.remove(&number);
				return Err(e);
			}
			self.numbers.push(number);
		}
		Ok(())
	}
}

impl<A: Accumulator + Clone + Default + 'static> Accumulator for DistinctAccumulator<A> {
	fn holds_inputs(&self) -> bool {
		true
	}

	fn save(&self) -> Option<Value> {
		Some(Saving::default().numbers(&self.numbers).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		// The state is built aside and takes the place of this accumulator's only once every number
		// folded: a state that cannot be read or folded leaves the accumulator as it was, and one
		// that can leaves nothing of what the accumulator held.
		let mut loaded = Self::default();
		for number in state.numbers()? {
			loaded.fold(number)?;
		}
		*self = loaded;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(number) = value {
			self.fold(number)
		} else {
			Ok(())
		}
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<Self>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		if self.numbers.is_empty() {
			// Folding the numbers of `other` into an accumulator that holds none reaches the
			// state `other` is in, so that state is copied rather than reached number by number.
			self.numbers.clone_from(&other.numbers);
			self.seen.clone_from(&other.seen);
			self.inner.clone_from(&other.inner);
		} else {
			for number in &other.numbers {
				self.fold(number.clone())?;
			}
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		if self.numbers.is_empty() {
			Ok(Value::Null)
		} else {
			self.inner.finalize()
		}
	}

	fn reset(&mut self) {
		self.numbers.clear();
		self.seen.clear();
		self.inner.reset();
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
	use rust_decimal::Decimal;
	use surrealdb_strand::Strand;

	use super::*;

	fn int(i: i64) -> Value {
		Value::Number(Number::Int(i))
	}

	fn float(f: f64) -> Value {
		Value::Number(Number::Float(f))
	}

	fn decimal(d: i64) -> Value {
		Value::Number(Number::Decimal(Decimal::from(d)))
	}

	fn finalize(function: &dyn AggregateFunction, values: Vec<Value>) -> anyhow::Result<Value> {
		let mut accumulator = function.create_accumulator();
		for value in values {
			accumulator.update(value)?;
		}
		accumulator.finalize()
	}

	#[test]
	fn a_group_with_no_number_answers_null() {
		for function in [&MathSumDistinct as &dyn AggregateFunction, &MathMeanDistinct] {
			assert_eq!(finalize(function, vec![]).unwrap(), Value::Null);
			let skipped = vec![
				Value::None,
				Value::Null,
				Value::Bool(true),
				Value::String(Strand::new_static("a")),
				Value::Array(vec![int(1)].into()),
			];
			assert_eq!(finalize(function, skipped).unwrap(), Value::Null);
		}
	}

	#[test]
	fn each_distinct_number_counts_once_and_other_values_are_skipped() {
		let values =
			|| vec![int(1), Value::None, int(1), Value::Null, Value::Bool(true), int(3), int(3)];
		assert_eq!(finalize(&MathSumDistinct, values()).unwrap(), int(4));
		assert_eq!(finalize(&MathMeanDistinct, values()).unwrap(), float(2.0));
	}

	#[test]
	fn an_array_value_is_skipped_and_not_flattened() {
		let values = vec![int(1), Value::Array(vec![int(5), int(6)].into())];
		assert_eq!(finalize(&MathSumDistinct, values).unwrap(), int(1));
	}

	#[test]
	fn equal_numbers_of_different_types_are_one_number() {
		let values = || vec![int(1), float(1.0), decimal(1), int(2)];
		assert_eq!(finalize(&MathSumDistinct, values()).unwrap(), int(3));
		assert_eq!(finalize(&MathMeanDistinct, values()).unwrap(), float(1.5));
		let first_decimal = vec![decimal(1), int(1), float(1.0)];
		assert_eq!(finalize(&MathSumDistinct, first_decimal).unwrap(), decimal(1));
	}

	#[test]
	fn numbers_that_cancel_sum_to_zero_and_not_null() {
		assert_eq!(finalize(&MathSumDistinct, vec![int(1), int(-1)]).unwrap(), int(0));
		assert_eq!(finalize(&MathMeanDistinct, vec![int(1), int(-1)]).unwrap(), float(0.0));
	}

	#[test]
	fn the_mean_is_the_plain_means_over_the_distinct_numbers() {
		let distinct = vec![float(0.1), float(0.2), float(0.3)];
		let repeated = vec![float(0.1), float(0.2), float(0.1), float(0.3), float(0.3)];
		let mut plain = crate::exec::function::builtin::aggregates::MathMean.create_accumulator();
		for value in distinct {
			plain.update(value).unwrap();
		}
		assert_eq!(finalize(&MathMeanDistinct, repeated).unwrap(), plain.finalize().unwrap());
	}

	#[test]
	fn an_integer_total_that_leaves_i64_fails_the_sum_and_not_the_mean() {
		let values = || vec![int(i64::MAX), int(i64::MAX - 1)];
		assert!(finalize(&MathSumDistinct, values()).is_err());
		assert_eq!(finalize(&MathMeanDistinct, values()).unwrap(), float(i64::MAX as f64));
		// The boundary itself is a sum.
		assert_eq!(finalize(&MathSumDistinct, vec![int(i64::MAX), int(0)]).unwrap(), int(i64::MAX));
		assert_eq!(
			finalize(&MathSumDistinct, vec![int(i64::MIN), int(i64::MAX)]).unwrap(),
			int(-1)
		);
	}

	#[test]
	fn infinity_and_nan_are_numbers() {
		let infinite = vec![float(f64::INFINITY), int(1)];
		assert_eq!(finalize(&MathSumDistinct, infinite.clone()).unwrap(), float(f64::INFINITY));
		assert_eq!(finalize(&MathMeanDistinct, infinite).unwrap(), float(f64::INFINITY));
		let Value::Number(Number::Float(nan)) =
			finalize(&MathSumDistinct, vec![float(f64::NAN), int(1), float(f64::NAN)]).unwrap()
		else {
			panic!("a float");
		};
		assert!(nan.is_nan());
	}

	#[test]
	fn merging_folds_the_numbers_the_other_part_saw_that_this_one_did_not() {
		let function = MathSumDistinct;
		let mut first = function.create_accumulator();
		let mut second = function.create_accumulator();
		for value in [int(1), int(2)] {
			first.update(value).unwrap();
		}
		for value in [int(2), int(3)] {
			second.update(value).unwrap();
		}
		first.merge(second).unwrap();
		assert_eq!(first.finalize().unwrap(), int(6));
	}

	#[test]
	fn a_saved_state_keeps_the_numbers_and_no_repeats() {
		let function = MathSumDistinct;
		let mut saved = function.create_accumulator();
		for value in [int(1), int(1), int(2)] {
			saved.update(value).unwrap();
		}
		let mut loaded = function.create_accumulator();
		loaded.load(saved.save().expect("saves")).unwrap();
		loaded.update(int(2)).unwrap();
		loaded.update(int(4)).unwrap();
		assert_eq!(loaded.finalize().unwrap(), int(7));
	}

	#[test]
	fn loading_a_state_replaces_what_the_accumulator_held() {
		for function in [&MathSumDistinct as &dyn AggregateFunction, &MathMeanDistinct] {
			let mut saved = function.create_accumulator();
			for value in [int(5), int(6)] {
				saved.update(value).unwrap();
			}
			let state = saved.save().expect("saves");

			let mut used = function.create_accumulator();
			for value in [int(1), int(2), int(5)] {
				used.update(value).unwrap();
			}
			used.load(state).unwrap();
			assert_eq!(used.finalize().unwrap(), saved.finalize().unwrap());
			// A number the accumulator held before the load is a new number again.
			used.update(int(1)).unwrap();
			saved.update(int(1)).unwrap();
			assert_eq!(used.finalize().unwrap(), saved.finalize().unwrap());
		}
	}

	#[test]
	fn a_state_that_cannot_be_read_leaves_the_accumulator_as_it_was() {
		let mut accumulator = MathSumDistinct.create_accumulator();
		accumulator.update(int(1)).unwrap();
		assert!(accumulator.load(Value::Null).is_err());
		assert!(accumulator.load(Value::Array(vec![Value::Bool(true)].into())).is_err());
		assert_eq!(accumulator.finalize().unwrap(), int(1));
	}

	/// An accumulator whose `update` refuses a negative integer and changes nothing when it does.
	/// The sum and the mean accept every number, so this is what exercises a fold that fails.
	#[derive(Debug, Clone, Default)]
	struct NonNegativeTotal {
		total: i64,
	}

	impl Accumulator for NonNegativeTotal {
		fn update(&mut self, value: Value) -> Result<()> {
			if let Value::Number(Number::Int(n)) = value {
				anyhow::ensure!(n >= 0, "negative");
				self.total += n;
			}
			Ok(())
		}

		fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
			let other = other
				.as_any()
				.downcast_ref::<Self>()
				.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
			self.total += other.total;
			Ok(())
		}

		fn finalize(&self) -> Result<Value> {
			Ok(int(self.total))
		}

		fn reset(&mut self) {
			self.total = 0;
		}

		fn clone_box(&self) -> Box<dyn Accumulator> {
			Box::new(self.clone())
		}

		fn as_any(&self) -> &dyn std::any::Any {
			self
		}
	}

	#[test]
	fn a_number_the_inner_accumulator_refuses_is_not_recorded() {
		let mut accumulator = DistinctAccumulator::<NonNegativeTotal>::default();
		accumulator.update(int(2)).unwrap();
		assert!(accumulator.update(int(-1)).is_err());
		// The refused number is not remembered, so it is refused again and not skipped as a repeat.
		assert!(accumulator.update(int(-1)).is_err());
		accumulator.update(int(2)).unwrap();
		accumulator.update(int(5)).unwrap();
		assert_eq!(accumulator.numbers, vec![Number::Int(2), Number::Int(5)]);
		assert_eq!(accumulator.seen.len(), 2);
		assert_eq!(accumulator.finalize().unwrap(), int(7));
		// A state saved after the refusal holds the numbers that were folded.
		let mut loaded = DistinctAccumulator::<NonNegativeTotal>::default();
		loaded.load(accumulator.save().expect("saves")).unwrap();
		assert_eq!(loaded.finalize().unwrap(), int(7));
	}

	#[test]
	fn a_load_that_fails_leaves_the_accumulator_as_it_was() {
		let mut accumulator = DistinctAccumulator::<NonNegativeTotal>::default();
		accumulator.update(int(1)).unwrap();
		accumulator.update(int(2)).unwrap();
		let refused = Saving::default().numbers(&[Number::Int(5), Number::Int(-1)]).done();
		assert!(accumulator.load(refused).is_err());
		assert_eq!(accumulator.numbers, vec![Number::Int(1), Number::Int(2)]);
		assert_eq!(accumulator.seen.len(), 2);
		assert_eq!(accumulator.finalize().unwrap(), int(3));
		// It goes on as it was: the numbers it held are still repeats, and 5 is still new.
		accumulator.update(int(2)).unwrap();
		accumulator.update(int(5)).unwrap();
		assert_eq!(accumulator.finalize().unwrap(), int(8));
	}

	#[test]
	fn an_overflowing_total_is_reported_when_the_group_is_finalised_and_the_state_stays_whole() {
		let function = MathSumDistinct;
		let mut accumulator = function.create_accumulator();
		for value in [int(i64::MAX), int(1), int(1), int(2)] {
			accumulator.update(value).expect("an update does not fail; the total reports it");
		}
		let error = accumulator.finalize().expect_err("the total leaves i64").to_string();
		// The numbers that were folded are all saved, and a state loaded from them fails alike.
		let mut loaded = function.create_accumulator();
		loaded.load(accumulator.save().expect("saves")).unwrap();
		assert_eq!(loaded.finalize().expect_err("the total leaves i64").to_string(), error);
	}

	/// The parts of a group and what folding every number of them into one accumulator answers.
	fn merged_and_folded(
		function: &dyn AggregateFunction,
		parts: &[&[Value]],
		probe: &[Value],
	) -> (Vec<String>, Vec<String>) {
		let outcome = |accumulator: &dyn Accumulator| match accumulator.finalize() {
			Ok(value) => format!("{value:?}"),
			Err(e) => format!("error: {e}"),
		};
		let mut merged = function.create_accumulator();
		let mut folded = function.create_accumulator();
		for part in parts {
			let mut accumulator = function.create_accumulator();
			for value in *part {
				accumulator.update(value.clone()).unwrap();
				folded.update(value.clone()).unwrap();
			}
			merged.merge(accumulator).unwrap();
		}
		let mut merged_outcomes = vec![outcome(merged.as_ref())];
		let mut folded_outcomes = vec![outcome(folded.as_ref())];
		// What each goes on to do with the numbers that follow is the same too.
		for value in probe {
			merged.update(value.clone()).unwrap();
			folded.update(value.clone()).unwrap();
			merged_outcomes.push(outcome(merged.as_ref()));
			folded_outcomes.push(outcome(folded.as_ref()));
		}
		(merged_outcomes, folded_outcomes)
	}

	#[test]
	fn merging_answers_what_folding_every_number_into_one_accumulator_answers() {
		let probe = [int(1), int(7), float(0.2), int(7)];
		let cases: Vec<Vec<Vec<Value>>> = vec![
			// Into an empty accumulator, and an empty part into one that holds numbers.
			vec![vec![], vec![int(1), int(2), int(2), int(3)]],
			vec![vec![int(1), int(2)], vec![]],
			vec![vec![], vec![]],
			vec![vec![Value::Null, Value::None], vec![int(4), int(4)]],
			// Parts that overlap in some numbers, in all of them, and in none.
			vec![vec![int(1), int(2)], vec![int(2), int(3)]],
			vec![vec![int(1), int(2)], vec![int(2), int(1)]],
			vec![vec![int(1), int(2)], vec![int(3), int(4)]],
			// Three parts, the first of them empty.
			vec![vec![], vec![int(1), int(2)], vec![int(2), int(3)]],
			// A float total depends on the order its terms are folded in: the numbers of a part
			// that the earlier ones did not see follow them in the order of the part.
			vec![vec![float(0.1), float(0.2)], vec![float(0.3), float(0.2), float(0.4)]],
			vec![vec![], vec![float(0.1), float(0.2), float(0.3), float(1e16), float(-1e16)]],
			// Equal numbers of different types are one number, the first of them folded.
			vec![vec![int(1)], vec![float(1.0), decimal(1), int(2)]],
			vec![vec![decimal(1)], vec![int(1), float(2.0)]],
			// A total that leaves `i64` fails in the part and in the merge alike.
			vec![vec![], vec![int(i64::MAX), int(i64::MAX - 1)]],
			vec![vec![int(i64::MAX)], vec![int(i64::MAX - 1), int(i64::MAX)]],
			vec![vec![float(f64::NAN)], vec![float(f64::NAN), float(f64::INFINITY)]],
		];
		for function in [&MathSumDistinct as &dyn AggregateFunction, &MathMeanDistinct] {
			for case in &cases {
				let parts: Vec<&[Value]> = case.iter().map(Vec::as_slice).collect();
				let (merged, folded) = merged_and_folded(function, &parts, &probe);
				assert_eq!(merged, folded, "{} over {case:?}", function.name());
			}
		}
	}

	#[test]
	fn merging_an_accumulator_of_another_aggregate_is_refused() {
		let mut sum = MathSumDistinct.create_accumulator();
		for other in [
			MathMeanDistinct.create_accumulator(),
			crate::exec::function::builtin::aggregates::MathSum.create_accumulator(),
		] {
			assert!(sum.merge(other).is_err());
		}
		assert_eq!(sum.finalize().unwrap(), Value::Null);
	}

	#[test]
	fn a_reset_accumulator_starts_a_new_group() {
		let function = MathMeanDistinct;
		let mut accumulator = function.create_accumulator();
		accumulator.update(int(10)).unwrap();
		accumulator.reset();
		assert_eq!(accumulator.finalize().unwrap(), Value::Null);
		accumulator.update(int(10)).unwrap();
		assert_eq!(accumulator.finalize().unwrap(), float(10.0));
	}
}
