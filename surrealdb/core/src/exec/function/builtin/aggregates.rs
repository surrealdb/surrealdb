//! Built-in aggregate function implementations.
//!
//! This module provides aggregate functions that operate over groups of values
//! during GROUP BY query execution.
//!
//! Aggregates are organized by category:
//! - [`count`]: Row and value counting (COUNT(), COUNT(field))
//! - [`math`]: Mathematical aggregations (sum, mean, min, max, stddev, variance, median)
//! - [`time`]: Datetime aggregations (min, max)
//! - [`array`]: Array collection operations (group, join, distinct)

mod array;
mod count;
mod math;
mod time;

// Re-export all aggregate functions
pub use array::{ArrayDistinct, ArrayGroup, ArrayJoin};
pub use count::{Count, CountField};
pub use math::{MathMax, MathMean, MathMedian, MathMin, MathStddev, MathSum, MathVariance};
pub use time::{TimeMax, TimeMin};

use crate::exec::function::FunctionRegistry;

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
	registry.register_aggregate(MathMedian);

	// Time aggregates
	registry.register_aggregate(TimeMin);
	registry.register_aggregate(TimeMax);

	// Array aggregates
	registry.register_aggregate(ArrayGroup);
	registry.register_aggregate(ArrayJoin);
	registry.register_aggregate(ArrayDistinct);
}
