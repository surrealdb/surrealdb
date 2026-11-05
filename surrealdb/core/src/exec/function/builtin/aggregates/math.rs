//! Math aggregate functions.
//!
//! Provides aggregates for mathematical operations: sum, mean, min, max,
//! stddev, variance, and median.

use anyhow::Result;

use super::Rejected;
use super::saved::{Loading, Saving};
use crate::catalog::aggregation;
use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::expr::Kind;
use crate::val::{Number, TryAdd as _, Value};

// ============================================================================
// Sum
// ============================================================================

/// math::sum - sums numeric values
#[derive(Debug, Clone, Copy, Default)]
pub struct MathSum;

impl AggregateFunction for MathSum {
	fn name(&self) -> &'static str {
		"math::sum"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::sum", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(SumAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::Sum(SumAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SumAccumulator {
	sum: Number,
	rejected: Rejected,
}

impl SumAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
		self.rejected.merge_from(&other.rejected);
		self.add(other.sum.clone());
		Ok(())
	}
}

impl SumAccumulator {
	/// Fold `n` into the total. A total leaving `i64` fails the group when it
	/// is finalised: wrapping would answer with a number that is not the sum.
	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; this takes its operand by value like the arithmetic it feeds"
	)]
	fn add(&mut self, n: Number) {
		match self.sum.clone().try_add(n.widened()) {
			Ok(sum) => self.sum = sum,
			Err(e) => self.rejected.fail(&e),
		}
	}
}

impl Accumulator for SumAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().number(self.sum.clone()).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.sum = state.number()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number("math::sum", &value) {
			self.add(n);
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			if let Some(n) = self.rejected.number("math::sum", value) {
				self.add(n);
			}
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<SumAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other)
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		Ok(Value::Number(self.sum.clone()))
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
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
// Mean
// ============================================================================

/// math::mean - calculates average of numeric values
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMean;

impl AggregateFunction for MathMean {
	fn name(&self) -> &'static str {
		"math::mean"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::mean", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MeanAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::Mean(MeanAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MeanAccumulator {
	sum: Number,
	count: i64,
	rejected: Rejected,
}

impl MeanAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) -> Result<()> {
		self.rejected.merge_from(&other.rejected);
		self.add(other.sum.clone());
		self.count += other.count;
		Ok(())
	}
}

impl MeanAccumulator {
	/// Fold `n` into the total, which stays exact: integers that leave `i64`
	/// continue as a decimal. A total no decimal holds fails the group when it
	/// is finalised.
	#[allow(
		clippy::needless_pass_by_value,
		reason = "Number is not Copy; this takes its operand by value like the arithmetic it feeds"
	)]
	fn add(&mut self, n: Number) {
		match self.sum.clone().add_exact(n.widened()) {
			Ok(sum) => self.sum = sum,
			Err(e) => self.rejected.fail(&e),
		}
	}
}

impl Accumulator for MeanAccumulator {
	fn save(&self) -> Option<Value> {
		Some(
			Saving::default()
				.number(self.sum.clone())
				.int(self.count)
				.rejected(&self.rejected)
				.done(),
		)
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.sum = state.number()?;
		self.count = state.int()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number("math::mean", &value) {
			self.add(n);
			self.count += 1;
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			if let Some(n) = self.rejected.number("math::mean", value) {
				self.add(n);
				self.count += 1;
			}
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MeanAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other)
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		if self.count == 0 {
			// An empty group has no mean, so it finalises to NaN.
			Ok(Value::Number(Number::Float(f64::NAN)))
		} else if let Number::BigDecimal(sum) = &self.sum {
			// A total that reached a bigdecimal has an exact mean, rounded half away from zero
			// as a bigdecimal division is, as the scalar math::mean has.
			let count = surrealdb_types::BigDecimal::from(self.count);
			Ok(Value::Number(Number::BigDecimal(Box::new(sum.div_default(&count)?))))
		} else {
			let mean = self.sum.to_float() / self.count as f64;
			Ok(Value::Number(Number::Float(mean)))
		}
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
		self.count = 0;
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
// Min
// ============================================================================

/// math::min - finds minimum numeric value
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMin;

impl AggregateFunction for MathMin {
	fn name(&self) -> &'static str {
		"math::min"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::min", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MinAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::Min(MinAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MinAccumulator {
	min: Option<Number>,
	rejected: Rejected,
}

impl MinAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.rejected.merge_from(&other.rejected);
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
	}
}

impl Accumulator for MinAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().option_number(self.min.clone()).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.min = state.option_number()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number("math::min", &value) {
			self.min = Some(match &self.min {
				None => n,
				Some(current) => {
					if n < *current {
						n
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			if let Some(n) = self.rejected.number("math::min", value) {
				self.min = Some(match &self.min {
					None => n,
					Some(current) => {
						if n < *current {
							n
						} else {
							current.clone()
						}
					}
				});
			}
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MinAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		match &self.min {
			Some(n) => Ok(Value::Number(n.clone())),
			None => Ok(Value::Number(Number::Float(f64::INFINITY))),
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
// Max
// ============================================================================

/// math::max - finds maximum numeric value
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMax;

impl AggregateFunction for MathMax {
	fn name(&self) -> &'static str {
		"math::max"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::max", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MaxAccumulator::default())
	}

	fn accumulator_state(&self) -> super::AccumulatorState {
		super::AccumulatorState::Max(MaxAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MaxAccumulator {
	max: Option<Number>,
	rejected: Rejected,
}

impl MaxAccumulator {
	/// Fold `other`, a partial state over the same group, into this one.
	pub(crate) fn absorb(&mut self, other: &Self) {
		self.rejected.merge_from(&other.rejected);
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
	}
}

impl Accumulator for MaxAccumulator {
	fn save(&self) -> Option<Value> {
		Some(Saving::default().option_number(self.max.clone()).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.max = state.option_number()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number("math::max", &value) {
			self.max = Some(match &self.max {
				None => n,
				Some(current) => {
					if n > *current {
						n
					} else {
						current.clone()
					}
				}
			});
		}
		Ok(())
	}

	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			if let Some(n) = self.rejected.number("math::max", value) {
				self.max = Some(match &self.max {
					None => n,
					Some(current) => {
						if n > *current {
							n
						} else {
							current.clone()
						}
					}
				});
			}
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MaxAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.absorb(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		match &self.max {
			Some(n) => Ok(Value::Number(n.clone())),
			None => Ok(Value::Number(Number::Float(f64::NEG_INFINITY))),
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
// Stddev / Variance (shifted sum-of-squares)
// ============================================================================

/// math::stddev - sample standard deviation.
#[derive(Debug, Clone, Copy, Default)]
pub struct MathStddev;

impl AggregateFunction for MathStddev {
	fn name(&self) -> &'static str {
		"math::stddev"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::stddev", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(SpreadAccumulator::deviation())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// math::variance - sample variance.
#[derive(Debug, Clone, Copy, Default)]
pub struct MathVariance;

impl AggregateFunction for MathVariance {
	fn name(&self) -> &'static str {
		"math::variance"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::variance", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(SpreadAccumulator::variance())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// Whether a [`SpreadAccumulator`] finalises to a variance or its square root.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Spread {
	Variance,
	Deviation,
}

impl Spread {
	/// The function name to report a rejected input against.
	fn function_name(self) -> &'static str {
		match self {
			Spread::Variance => "math::variance",
			Spread::Deviation => "math::stddev",
		}
	}
}

/// Shared accumulator for `math::variance` and `math::stddev`.
///
/// Delegates to [`aggregation::shifted_accumulate`] and
/// [`aggregation::shifted_sample_variance`], the same functions materialized-view
/// maintenance uses, so both paths produce bit-identical results over the same input order.
/// Arithmetic stays in `Number`, so a group of `Decimal` inputs finalises as `Decimal`.
#[derive(Debug, Clone)]
struct SpreadAccumulator {
	spread: Spread,
	shift: Number,
	sum: Number,
	sum_of_squares: Number,
	count: i64,
	rejected: Rejected,
}

impl SpreadAccumulator {
	fn new(spread: Spread) -> Self {
		Self {
			spread,
			shift: 0.0.into(),
			sum: 0.0.into(),
			sum_of_squares: 0.0.into(),
			count: 0,
			rejected: Rejected::default(),
		}
	}

	fn variance() -> Self {
		Self::new(Spread::Variance)
	}

	fn deviation() -> Self {
		Self::new(Spread::Deviation)
	}
}

impl Accumulator for SpreadAccumulator {
	fn save(&self) -> Option<Value> {
		Some(
			Saving::default()
				.number(self.shift.clone())
				.number(self.sum.clone())
				.number(self.sum_of_squares.clone())
				.int(self.count)
				.rejected(&self.rejected)
				.done(),
		)
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.shift = state.number()?;
		self.sum = state.number()?;
		self.sum_of_squares = state.number()?;
		self.count = state.int()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number(self.spread.function_name(), &value)
			&& let Err(e) = aggregation::shifted_accumulate(
				&mut self.shift,
				&mut self.sum,
				&mut self.sum_of_squares,
				&mut self.count,
				n.widened(),
			) {
			self.rejected.fail(&e);
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<SpreadAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		// Both spread kinds share this accumulator, so the downcast alone cannot tell them apart.
		// The check below rejects a merge across kinds.
		// Finalising a mixed merge would return a variance where a deviation was asked for.
		// Such a merge is an error, never a silent result.
		anyhow::ensure!(
			self.spread == other.spread,
			"Cannot merge a {:?} accumulator into a {:?} one",
			other.spread,
			self.spread
		);
		self.rejected.merge_from(&other.rejected);
		if other.count == 0 {
			return Ok(());
		}
		if self.count == 0 {
			self.shift = other.shift.clone();
			self.sum = other.sum.clone();
			self.sum_of_squares = other.sum_of_squares.clone();
			self.count = other.count;
			return Ok(());
		}

		// The two halves were shifted against different assumed means, so
		// re-base `other` onto ours before adding.
		let self_sum = self.sum.clone();
		let self_sum_of_squares = self.sum_of_squares.clone();
		let rebased = aggregation::shifted_rebase(
			other.sum.clone(),
			other.sum_of_squares.clone(),
			other.count,
			other.shift.clone(),
			self.shift.clone(),
		)
		.and_then(|(rebased_sum, rebased_sum_of_squares)| {
			let next_sum = self_sum.try_add(rebased_sum)?;
			let next_sum_of_squares = self_sum_of_squares.try_add(rebased_sum_of_squares)?;
			Ok((next_sum, next_sum_of_squares))
		});
		match rebased {
			Ok((next_sum, next_sum_of_squares)) => {
				self.sum = next_sum;
				self.sum_of_squares = next_sum_of_squares;
				self.count += other.count;
			}
			Err(e) => self.rejected.fail(&e),
		}
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		let num = match self.spread {
			Spread::Variance => aggregation::shifted_sample_variance(
				self.sum.clone(),
				self.sum_of_squares.clone(),
				self.count,
			),
			Spread::Deviation => aggregation::shifted_sample_deviation(
				self.sum.clone(),
				self.sum_of_squares.clone(),
				self.count,
			),
		};
		Ok(Value::Number(num))
	}

	fn reset(&mut self) {
		*self = Self::new(self.spread);
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// Median
// ============================================================================

/// math::median - calculates median of values
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMedian;

impl AggregateFunction for MathMedian {
	fn name(&self) -> &'static str {
		"math::median"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Aggregate, "math::median", Fallibility::VALUE)
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MedianAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct MedianAccumulator {
	values: Vec<Number>,
	rejected: Rejected,
}

impl Accumulator for MedianAccumulator {
	fn holds_inputs(&self) -> bool {
		true
	}

	fn save(&self) -> Option<Value> {
		Some(Saving::default().numbers(&self.values).rejected(&self.rejected).done())
	}

	fn load(&mut self, state: Value) -> Result<()> {
		let mut state = Loading::new(state)?;
		self.values = state.numbers()?;
		self.rejected = state.rejected()?;
		Ok(())
	}

	fn update(&mut self, value: Value) -> Result<()> {
		if let Some(n) = self.rejected.number("math::median", &value) {
			self.values.push(n);
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MedianAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.rejected.merge_from(&other.rejected);
		self.values.extend(other.values.iter().cloned());
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		self.rejected.check()?;
		if self.values.is_empty() {
			return Ok(Value::None);
		}

		// Sort the values
		let mut sorted = self.values.clone();
		sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

		let len = sorted.len();
		let median = if len.is_multiple_of(2) {
			// Even number of elements: average of the two middle values
			let mid = len / 2;
			let a = sorted[mid - 1].to_float();
			let b = sorted[mid].to_float();
			Number::Float((a + b) / 2.0)
		} else {
			// Odd number of elements: middle value
			sorted[len / 2].widened()
		};

		Ok(Value::Number(median))
	}

	fn reset(&mut self) {
		self.values.clear();
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
	use surrealdb_strand::Strand;

	use super::*;

	// Helper to extract f64 from Value
	fn as_float(v: &Value) -> f64 {
		if let Value::Number(Number::Float(f)) = v {
			*f
		} else if let Value::Number(Number::Int(i)) = v {
			*i as f64
		} else {
			panic!("Expected Number, got {:?}", v);
		}
	}

	// Helper to check approximate equality for floats
	fn approx_eq(a: f64, b: f64, epsilon: f64) -> bool {
		(a - b).abs() < epsilon
	}

	// -------------------------------------------------------------------------
	// Sum tests
	// -------------------------------------------------------------------------

	#[test]
	fn sum_zero_items() {
		let func = MathSum;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
	}

	#[test]
	fn sum_single_item() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 42.0);
	}

	#[test]
	fn sum_multiple_items() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 6.0);
	}

	#[test]
	fn sum_merge() {
		let func = MathSum;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();
		acc1.update(Value::Number(Number::Int(2))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(3))).unwrap();
		acc2.update(Value::Number(Number::Int(4))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_float(&result), 10.0);
	}

	// -------------------------------------------------------------------------
	// Mean tests
	// -------------------------------------------------------------------------

	#[test]
	fn mean_zero_items() {
		let func = MathMean;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(as_float(&result).is_nan());
	}

	#[test]
	fn mean_single_item() {
		let func = MathMean;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 42.0);
	}

	#[test]
	fn mean_multiple_items() {
		let func = MathMean;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(4))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 3.0);
	}

	#[test]
	fn mean_merge() {
		let func = MathMean;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(2))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(4))).unwrap();
		acc2.update(Value::Number(Number::Int(6))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_float(&result), 4.0);
	}

	// -------------------------------------------------------------------------
	// Min tests
	// -------------------------------------------------------------------------

	#[test]
	fn min_zero_items() {
		let func = MathMin;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), f64::INFINITY);
	}

	#[test]
	fn min_single_item() {
		let func = MathMin;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 42.0);
	}

	#[test]
	fn min_multiple_items() {
		let func = MathMin;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 1.0);
	}

	#[test]
	fn min_merge() {
		let func = MathMin;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(5))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(2))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_float(&result), 2.0);
	}

	// -------------------------------------------------------------------------
	// Max tests
	// -------------------------------------------------------------------------

	#[test]
	fn max_zero_items() {
		let func = MathMax;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), f64::NEG_INFINITY);
	}

	#[test]
	fn max_single_item() {
		let func = MathMax;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 42.0);
	}

	#[test]
	fn max_multiple_items() {
		let func = MathMax;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 3.0);
	}

	#[test]
	fn max_merge() {
		let func = MathMax;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(5))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(10))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert_eq!(as_float(&result), 10.0);
	}

	// -------------------------------------------------------------------------
	// Stddev tests
	// -------------------------------------------------------------------------

	#[test]
	fn stddev_zero_items() {
		// Empty group has no defined deviation; matches scalar `math::stddev`.
		let func = MathStddev;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(as_float(&result).is_nan());
	}

	#[test]
	fn stddev_single_item() {
		let func = MathStddev;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
	}

	#[test]
	fn stddev_multiple_items() {
		let func = MathStddev;
		let mut acc = func.create_accumulator();
		// Values: 2, 4, 6 -> mean = 4, variance = 4, stddev = 2
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(4))).unwrap();
		acc.update(Value::Number(Number::Int(6))).unwrap();
		let result = acc.finalize().unwrap();
		assert!(approx_eq(as_float(&result), 2.0, 1e-10));
	}

	#[test]
	fn stddev_merge() {
		let func = MathStddev;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(2))).unwrap();
		acc1.update(Value::Number(Number::Int(4))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(6))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert!(approx_eq(as_float(&result), 2.0, 1e-10));
	}

	#[test]
	fn stddev_numerical_stability() {
		// Test with large numbers that would cause issues with naive algorithm
		let func = MathStddev;
		let mut acc = func.create_accumulator();
		let base = 1e9;
		acc.update(Value::Number(Number::Float(base + 1.0))).unwrap();
		acc.update(Value::Number(Number::Float(base + 2.0))).unwrap();
		acc.update(Value::Number(Number::Float(base + 3.0))).unwrap();
		let result = acc.finalize().unwrap();
		// stddev of [1, 2, 3] offset by 1e9 should still be 1.0
		assert!(approx_eq(as_float(&result), 1.0, 1e-6));
	}

	#[test]
	fn stddev_extreme_i64_spread() {
		let func = MathStddev;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(i64::MIN))).unwrap();
		acc.update(Value::Number(Number::Int(i64::MAX))).unwrap();
		let result = acc.finalize().unwrap();
		assert!(as_float(&result).is_finite() && as_float(&result) > 0.0);
	}

	#[test]
	fn variance_decimal_overflow_propagates_to_finalize() {
		use rust_decimal::Decimal;
		let func = MathVariance;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Decimal(Decimal::ZERO))).unwrap();
		// Updating with Decimal::MAX causes delta * delta to overflow Decimal in
		// shifted_accumulate. Even when callers ignore the update() result (as the VM does),
		// finalize() must fail.
		let _ = acc.update(Value::Number(Number::Decimal(Decimal::MAX)));
		assert!(acc.finalize().is_err());
	}

	// -------------------------------------------------------------------------
	// Variance tests
	// -------------------------------------------------------------------------

	#[test]
	fn variance_zero_items() {
		// Empty group has no defined variance; matches scalar `math::variance`.
		let func = MathVariance;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(as_float(&result).is_nan());
	}

	#[test]
	fn variance_single_item() {
		let func = MathVariance;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
	}

	#[test]
	fn variance_multiple_items() {
		let func = MathVariance;
		let mut acc = func.create_accumulator();
		// Values: 2, 4, 6 -> mean = 4, variance = 4
		acc.update(Value::Number(Number::Int(2))).unwrap();
		acc.update(Value::Number(Number::Int(4))).unwrap();
		acc.update(Value::Number(Number::Int(6))).unwrap();
		let result = acc.finalize().unwrap();
		assert!(approx_eq(as_float(&result), 4.0, 1e-10));
	}

	#[test]
	fn variance_merge() {
		let func = MathVariance;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(2))).unwrap();
		acc1.update(Value::Number(Number::Int(4))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(6))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		assert!(approx_eq(as_float(&result), 4.0, 1e-10));
	}

	// -------------------------------------------------------------------------
	// Median tests
	// -------------------------------------------------------------------------

	#[test]
	fn median_zero_items() {
		let func = MathMedian;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert!(matches!(result, Value::None));
	}

	#[test]
	fn median_single_item() {
		let func = MathMedian;
		let mut acc = func.create_accumulator();
		acc.update(Value::Number(Number::Int(42))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 42.0);
	}

	#[test]
	fn median_odd_count() {
		let func = MathMedian;
		let mut acc = func.create_accumulator();
		// Values: 3, 1, 2 -> sorted: 1, 2, 3 -> median = 2
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 2.0);
	}

	#[test]
	fn median_even_count() {
		let func = MathMedian;
		let mut acc = func.create_accumulator();
		// Values: 4, 1, 3, 2 -> sorted: 1, 2, 3, 4 -> median = (2+3)/2 = 2.5
		acc.update(Value::Number(Number::Int(4))).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		acc.update(Value::Number(Number::Int(3))).unwrap();
		acc.update(Value::Number(Number::Int(2))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 2.5);
	}

	#[test]
	fn median_merge() {
		let func = MathMedian;
		let mut acc1 = func.create_accumulator();
		acc1.update(Value::Number(Number::Int(1))).unwrap();
		acc1.update(Value::Number(Number::Int(5))).unwrap();

		let mut acc2 = func.create_accumulator();
		acc2.update(Value::Number(Number::Int(3))).unwrap();

		acc1.merge(acc2).unwrap();
		let result = acc1.finalize().unwrap();
		// [1, 5, 3] -> sorted [1, 3, 5] -> median = 3
		assert_eq!(as_float(&result), 3.0);
	}

	// -------------------------------------------------------------------------
	// update_batch tests - Sum
	// -------------------------------------------------------------------------

	#[test]
	fn sum_batch_empty() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
	}

	#[test]
	fn sum_batch_multiple() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
			Value::Number(Number::Int(3)),
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 6.0);
	}

	#[test]
	fn sum_batch_skips_non_numbers() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(5)),
			Value::None,
			Value::String(Strand::new_static("test")),
			Value::Number(Number::Int(3)),
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 8.0);
	}

	#[test]
	fn sum_batch_then_single() {
		let func = MathSum;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Number(Number::Int(7))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 10.0);
	}

	// -------------------------------------------------------------------------
	// update_batch tests - Mean
	// -------------------------------------------------------------------------

	#[test]
	fn mean_batch_empty() {
		let func = MathMean;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert!(as_float(&result).is_nan());
	}

	#[test]
	fn mean_batch_multiple() {
		let func = MathMean;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(2)), Value::Number(Number::Int(4))];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 3.0);
	}

	#[test]
	fn mean_batch_then_single() {
		let func = MathMean;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(2))];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Number(Number::Int(4))).unwrap();
		acc.update(Value::Number(Number::Int(6))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 4.0);
	}

	// -------------------------------------------------------------------------
	// update_batch tests - Min
	// -------------------------------------------------------------------------

	#[test]
	fn min_batch_empty() {
		let func = MathMin;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), f64::INFINITY);
	}

	#[test]
	fn min_batch_multiple() {
		let func = MathMin;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(3)),
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(2)),
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 1.0);
	}

	#[test]
	fn min_batch_then_single() {
		let func = MathMin;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(5)), Value::Number(Number::Int(3))];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Number(Number::Int(1))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 1.0);
	}

	// -------------------------------------------------------------------------
	// update_batch tests - Max
	// -------------------------------------------------------------------------

	#[test]
	fn max_batch_empty() {
		let func = MathMax;
		let mut acc = func.create_accumulator();
		acc.update_batch(&[]).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), f64::NEG_INFINITY);
	}

	#[test]
	fn max_batch_multiple() {
		let func = MathMax;
		let mut acc = func.create_accumulator();
		let values = vec![
			Value::Number(Number::Int(1)),
			Value::Number(Number::Int(3)),
			Value::Number(Number::Int(2)),
		];
		acc.update_batch(&values).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 3.0);
	}

	#[test]
	fn max_batch_then_single() {
		let func = MathMax;
		let mut acc = func.create_accumulator();
		let values = vec![Value::Number(Number::Int(5)), Value::Number(Number::Int(3))];
		acc.update_batch(&values).unwrap();
		acc.update(Value::Number(Number::Int(10))).unwrap();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 10.0);
	}
}
