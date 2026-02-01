//! Built-in aggregate function implementations.
//!
//! This module provides aggregate functions that operate over groups of values
//! during GROUP BY query execution.

use anyhow::Result;

use crate::exec::function::{Accumulator, AggregateFunction, FunctionRegistry, Signature};
use crate::expr::Kind;
use crate::val::{Datetime, Number, Value};

// ============================================================================
// Count Aggregates
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

/// COUNT(field) - counts non-null values
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
// Math Aggregates
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

/// math::stddev - calculates standard deviation
#[derive(Debug, Clone, Copy, Default)]
pub struct MathStddev;

impl AggregateFunction for MathStddev {
	fn name(&self) -> &'static str {
		"math::stddev"
	}

	fn create_accumulator(&self) -> Box<dyn Accumulator> {
		Box::new(StddevAccumulator::default())
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("value", Kind::Number).returns(Kind::Number)
	}
}

#[derive(Debug, Clone, Default)]
struct StddevAccumulator {
	sum: Number,
	sum_of_squares: Number,
	count: i64,
}

impl Accumulator for StddevAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.sum = self.sum + n;
			self.sum_of_squares = self.sum_of_squares + (n * n);
			self.count += 1;
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<StddevAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.sum = self.sum + other.sum;
		self.sum_of_squares = self.sum_of_squares + other.sum_of_squares;
		self.count += other.count;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		if self.count <= 1 {
			Ok(Value::Number(Number::Float(0.0)))
		} else {
			let mean = self.sum / Number::from(self.count);
			let variance = (self.sum_of_squares - (self.sum * mean)) / Number::from(self.count - 1);
			let stddev = if variance == Number::Float(0.0) {
				Number::Float(0.0)
			} else {
				variance.sqrt()
			};
			Ok(Value::Number(stddev))
		}
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
		self.sum_of_squares = Number::Int(0);
		self.count = 0;
	}

	fn clone_box(&self) -> Box<dyn Accumulator> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}
}

/// math::variance - calculates variance
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

#[derive(Debug, Clone, Default)]
struct VarianceAccumulator {
	sum: Number,
	sum_of_squares: Number,
	count: i64,
}

impl Accumulator for VarianceAccumulator {
	fn update(&mut self, value: Value) -> Result<()> {
		if let Value::Number(n) = value {
			self.sum = self.sum + n;
			self.sum_of_squares = self.sum_of_squares + (n * n);
			self.count += 1;
		}
		Ok(())
	}

	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()> {
		let other = other
			.as_any()
			.downcast_ref::<VarianceAccumulator>()
			.ok_or_else(|| anyhow::anyhow!("Cannot merge incompatible accumulators"))?;
		self.sum = self.sum + other.sum;
		self.sum_of_squares = self.sum_of_squares + other.sum_of_squares;
		self.count += other.count;
		Ok(())
	}

	fn finalize(&self) -> Result<Value> {
		if self.count <= 1 {
			Ok(Value::Number(Number::Float(0.0)))
		} else {
			let mean = self.sum / Number::from(self.count);
			let variance = (self.sum_of_squares - (self.sum * mean)) / Number::from(self.count - 1);
			Ok(Value::Number(variance))
		}
	}

	fn reset(&mut self) {
		self.sum = Number::Int(0);
		self.sum_of_squares = Number::Int(0);
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
// Time Aggregates
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
// Array Aggregates
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
// Registration
// ============================================================================

/// Register all built-in aggregate functions with the registry.
pub fn register(registry: &mut FunctionRegistry) {
	// Count aggregates
	registry.register_aggregate(Count);
	// Note: CountField is handled specially - "count" with args becomes CountField

	// Math aggregates
	registry.register_aggregate(MathSum);
	registry.register_aggregate(MathMean);
	registry.register_aggregate(MathMin);
	registry.register_aggregate(MathMax);
	registry.register_aggregate(MathStddev);
	registry.register_aggregate(MathVariance);

	// Time aggregates
	registry.register_aggregate(TimeMin);
	registry.register_aggregate(TimeMax);

	// Array aggregates
	registry.register_aggregate(ArrayGroup);
}
