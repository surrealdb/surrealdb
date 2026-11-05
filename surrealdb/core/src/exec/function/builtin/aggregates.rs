//! Built-in aggregate function implementations.
//!
//! This module provides aggregate functions that operate over groups of values
//! during GROUP BY query execution.
//!
//! Aggregates are organized by category:
//! - [`count`]: Row and value counting (COUNT(), COUNT(field), the SQL front ends' COUNT(column))
//! - [`math`]: Mathematical aggregations (sum, mean, min, max, stddev, variance, median)
//! - [`distinct`]: Sum and mean of the distinct numbers of a group
//! - [`time`]: Datetime aggregations (min, max)
//! - [`array`]: Array collection operations (group, join, distinct, orderable min/max)
//! - [`vector`]: Elementwise vector aggregations (sum)

mod array;
mod count;
mod distinct;
mod math;
mod reject;
mod saved;
mod time;
mod vector;

// Re-export all aggregate functions
pub use array::{ArrayCollect, ArrayDistinct, ArrayGroup, ArrayJoin, ArrayMax, ArrayMin};
pub use count::{Count, CountField, CountPresent};
pub use distinct::{MathMeanDistinct, MathSumDistinct};
pub use math::{MathMax, MathMean, MathMedian, MathMin, MathStddev, MathSum, MathVariance};
use reject::Rejected;
pub use time::{TimeMax, TimeMin};
pub use vector::VectorSum;

use crate::exec::function::{Accumulator, FunctionRegistry};
use crate::val::Value;

/// An aggregate's accumulator held by value: a built-in whose state is
/// plain data as that data, and any other as its boxed accumulator.
///
/// A caller that keeps the states of many groups side by side in one vector
/// holds a plain state inline, so opening a group allocates nothing for it.
/// Each variant answers exactly what the accumulator its function creates
/// answers.
#[derive(Debug)]
pub(crate) enum AccumulatorState {
	Count(count::CountAccumulator),
	CountField(count::CountFieldAccumulator),
	CountPresent(count::CountPresentAccumulator),
	Sum(math::SumAccumulator),
	Mean(math::MeanAccumulator),
	Min(math::MinAccumulator),
	Max(math::MaxAccumulator),
	Boxed(Box<dyn Accumulator>),
}

impl AccumulatorState {
	/// A state left behind where one is moved out, which holds nothing.
	pub(crate) fn placeholder() -> Self {
		Self::Count(count::CountAccumulator::default())
	}

	/// The accumulator this state is.
	pub(crate) fn as_accumulator(&self) -> &dyn Accumulator {
		match self {
			Self::Count(a) => a,
			Self::CountField(a) => a,
			Self::CountPresent(a) => a,
			Self::Sum(a) => a,
			Self::Mean(a) => a,
			Self::Min(a) => a,
			Self::Max(a) => a,
			Self::Boxed(a) => a.as_ref(),
		}
	}

	/// The accumulator this state is, to fold into.
	pub(crate) fn as_accumulator_mut(&mut self) -> &mut dyn Accumulator {
		match self {
			Self::Count(a) => a,
			Self::CountField(a) => a,
			Self::CountPresent(a) => a,
			Self::Sum(a) => a,
			Self::Mean(a) => a,
			Self::Min(a) => a,
			Self::Max(a) => a,
			Self::Boxed(a) => a.as_mut(),
		}
	}

	/// [`Accumulator::update`].
	#[inline]
	pub(crate) fn update(&mut self, value: Value) -> anyhow::Result<()> {
		self.as_accumulator_mut().update(value)
	}

	/// The state as a value, for an aggregate that spills its groups:
	/// [`Accumulator::save`].
	pub(crate) fn save(&self) -> Option<Value> {
		match self {
			Self::Count(a) => a.save(),
			Self::CountField(a) => a.save(),
			Self::CountPresent(a) => a.save(),
			Self::Sum(a) => a.save(),
			Self::Mean(a) => a.save(),
			Self::Min(a) => a.save(),
			Self::Max(a) => a.save(),
			Self::Boxed(a) => a.save(),
		}
	}

	/// [`Accumulator::holds_inputs`].
	pub(crate) fn holds_inputs(&self) -> bool {
		matches!(self, Self::Boxed(a) if a.holds_inputs())
	}

	/// Load a state [`Self::save`] answered into this fresh state:
	/// [`Accumulator::load`].
	pub(crate) fn load(&mut self, state: Value) -> anyhow::Result<()> {
		self.as_accumulator_mut().load(state)
	}

	/// [`Accumulator::finalize`].
	pub(crate) fn finalize(&self) -> anyhow::Result<Value> {
		self.as_accumulator().finalize()
	}

	/// [`Accumulator::set_extra_args`].
	pub(crate) fn set_extra_args(&mut self, args: &[Value]) {
		self.as_accumulator_mut().set_extra_args(args)
	}

	/// Fold `other`, a partial state of the same aggregate over the same
	/// group, into this one: [`Accumulator::merge`], without boxing a plain
	/// state.
	pub(crate) fn merge(&mut self, other: Self) -> anyhow::Result<()> {
		match (self, other) {
			(Self::Count(a), Self::Count(b)) => a.absorb(&b),
			(Self::CountField(a), Self::CountField(b)) => a.absorb(&b),
			(Self::CountPresent(a), Self::CountPresent(b)) => a.absorb(&b),
			(Self::Sum(a), Self::Sum(b)) => return a.absorb(&b),
			(Self::Mean(a), Self::Mean(b)) => return a.absorb(&b),
			(Self::Min(a), Self::Min(b)) => a.absorb(&b),
			(Self::Max(a), Self::Max(b)) => a.absorb(&b),
			(Self::Boxed(a), Self::Boxed(b)) => return a.merge(b),
			_ => anyhow::bail!("Cannot merge incompatible accumulators"),
		}
		Ok(())
	}
}

/// Register all built-in aggregate functions with the registry.
pub fn register(registry: &mut FunctionRegistry) {
	// Count aggregates
	registry.register_aggregate(Count);
	// Note: CountField is handled specially - "count" with args becomes CountField
	registry.register_aggregate(CountPresent);

	// Math aggregates
	registry.register_aggregate(MathSum);
	registry.register_aggregate(MathMean);
	registry.register_aggregate(MathMin);
	registry.register_aggregate(MathMax);
	registry.register_aggregate(MathStddev);
	registry.register_aggregate(MathVariance);
	registry.register_aggregate(MathMedian);
	registry.register_aggregate(MathSumDistinct);
	registry.register_aggregate(MathMeanDistinct);

	// Time aggregates
	registry.register_aggregate(TimeMin);
	registry.register_aggregate(TimeMax);

	// Array aggregates
	registry.register_aggregate(ArrayGroup);
	registry.register_aggregate(ArrayCollect);
	registry.register_aggregate(ArrayJoin);
	registry.register_aggregate(ArrayDistinct);
	registry.register_aggregate(ArrayMin);
	registry.register_aggregate(ArrayMax);

	// Vector aggregates
	registry.register_aggregate(VectorSum);
}
