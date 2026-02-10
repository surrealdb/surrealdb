//! Math aggregate functions.
//!
//! Provides aggregates for mathematical operations: sum, mean, min, max,
//! stddev, variance, and median.

use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, Signature};
use crate::expr::Kind;
use crate::val::{Number, Value};

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

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(SumAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct SumAccumulator {
	sum: Number,
}

impl Accumulator for SumAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.sum = self.sum + n;
		}
		// Skip non-numbers (matches old behavior)
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<SumAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.sum = self.sum + other.sum;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		Ok(Value::Number(self.sum))
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
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

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MeanAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct MeanAccumulator {
	sum: Number,
	count: i64,
}

impl Accumulator for MeanAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.sum = self.sum + n;
			self.count += 1;
		}
		// Skip non-numbers
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MeanAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.sum = self.sum + other.sum;
		self.count += other.count;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		if self.count == 0 {
			// Return NaN for empty groups (matches old behavior)
			Ok(Value::Number(Number::Float(f64::NAN)))
		} else {
			let mean = self.sum.to_float() / self.count as f64;
			Ok(Value::Number(Number::Float(mean)))
		}
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
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
// Min
// ============================================================================

/// math::min - finds minimum numeric value
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMin;

impl AggregateFunction for MathMin {
	fn name(&self) -> &'static str {
		"math::min"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MinAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct MinAccumulator {
	min: Option<Number>,
}

impl Accumulator for MinAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.min = Some(match &self.min {
				None => n,
				Some(current) => {
					if n < *current {
						n
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
			.downcast_ref::<MinAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
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
		match &self.min {
			Some(n) => Ok(Value::Number(*n)),
			None => Ok(Value::Number(Number::Float(f64::INFINITY))),
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
// Max
// ============================================================================

/// math::max - finds maximum numeric value
#[derive(Debug, Clone, Copy, Default)]
pub struct MathMax;

impl AggregateFunction for MathMax {
	fn name(&self) -> &'static str {
		"math::max"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(MaxAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct MaxAccumulator {
	max: Option<Number>,
}

impl Accumulator for MaxAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.max = Some(match &self.max {
				None => n,
				Some(current) => {
					if n > *current {
						n
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
			.downcast_ref::<MaxAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
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
		match &self.max {
			Some(n) => Ok(Value::Number(*n)),
			None => Ok(Value::Number(Number::Float(f64::NEG_INFINITY))),
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
// Stddev (using Welford's online algorithm)
// ============================================================================

/// math::stddev - calculates sample standard deviation using Welford's algorithm
///
/// Welford's online algorithm is numerically stable and avoids catastrophic
/// cancellation that can occur with the naive sum-of-squares approach for
/// large numbers or numbers close in value.
#[derive(Debug, Clone, Copy, Default)]
pub struct MathStddev;

impl AggregateFunction for MathStddev {
	fn name(&self) -> &'static str {
		"math::stddev"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(WelfordAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// Welford's online algorithm accumulator for computing variance/stddev.
///
/// This algorithm maintains a running mean and sum of squared differences
/// from the mean (M2), which provides better numerical stability than
/// the naive sum and sum-of-squares approach.
///
/// Reference: Welford, B. P. (1962). "Note on a method for calculating
/// corrected sums of squares and products"
#[derive(Debug, Clone, Default)]
struct WelfordAccumulator {
	count: i64,
	mean: f64,
	m2: f64, // Sum of squared differences from mean
}

impl WelfordAccumulator {
	/// Update with a new value using Welford's algorithm
	fn update_value(&mut self, x: f64) {
		self.count += 1;
		let delta = x - self.mean;
		self.mean += delta / self.count as f64;
		let delta2 = x - self.mean;
		self.m2 += delta * delta2;
	}

	/// Merge another Welford accumulator using parallel algorithm.
	///
	/// Uses Chan's parallel algorithm for combining partial results.
	/// Reference: Chan et al. (1979) "Updating Formulae and a Pairwise
	/// Algorithm for Computing Sample Variances"
	#[allow(unused)]
	fn merge_welford(&mut self, other: &WelfordAccumulator) {
		if other.count == 0 {
			return;
		}
		if self.count == 0 {
			self.count = other.count;
			self.mean = other.mean;
			self.m2 = other.m2;
			return;
		}

		let total_count = self.count + other.count;
		let delta = other.mean - self.mean;

		// Combined mean
		let new_mean = self.mean + delta * (other.count as f64 / total_count as f64);

		// Combined M2 using Chan's formula
		let new_m2 = self.m2
			+ other.m2
			+ delta * delta * (self.count as f64 * other.count as f64 / total_count as f64);

		self.count = total_count;
		self.mean = new_mean;
		self.m2 = new_m2;
	}

	/// Compute sample variance (using n-1 divisor)
	fn sample_variance(&self) -> f64 {
		if self.count <= 1 {
			0.0
		} else {
			self.m2 / (self.count - 1) as f64
		}
	}
}

impl Accumulator for WelfordAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.update_value(n.to_float());
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<WelfordAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.merge_welford(other);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		let stddev = self.sample_variance().sqrt();
		Ok(Value::Number(Number::Float(stddev)))
	}

	fn reset(&mut self) {
		self.count = 0;
		self.mean = 0.0;
		self.m2 = 0.0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

// ============================================================================
// Variance (using Welford's online algorithm)
// ============================================================================

/// math::variance - calculates sample variance using Welford's algorithm
#[derive(Debug, Clone, Copy, Default)]
pub struct MathVariance;

impl AggregateFunction for MathVariance {
	fn name(&self) -> &'static str {
		"math::variance"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(VarianceAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

/// Variance accumulator using Welford's algorithm (same as stddev, different finalize)
#[derive(Debug, Clone, Default)]
struct VarianceAccumulator {
	welford: WelfordAccumulator,
}

impl Accumulator for VarianceAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.welford.update_value(n.to_float());
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<VarianceAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.welford.merge_welford(&other.welford);
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		let variance = self.welford.sample_variance();
		Ok(Value::Number(Number::Float(variance)))
	}

	fn reset(&mut self) {
		self.welford.reset();
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
}

impl Accumulator for MedianAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.values.push(n);
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<MedianAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.values.extend(other.values.iter().copied());
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
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
			sorted[len / 2]
		};

		Ok(Value::Number(median))
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
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
	use super::*;

	// Helper to extract f64 from Value
	fn as_float(v: &Value) -> f64 {
		match v {
			Value::Number(Number::Float(f)) => *f,
			Value::Number(Number::Int(i)) => *i as f64,
			_ => panic!("Expected Number, got {:?}", v),
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
		let func = MathStddev;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
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

	// -------------------------------------------------------------------------
	// Variance tests
	// -------------------------------------------------------------------------

	#[test]
	fn variance_zero_items() {
		let func = MathVariance;
		let acc = func.create_accumulator();
		let result = acc.finalize().unwrap();
		assert_eq!(as_float(&result), 0.0);
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
}
