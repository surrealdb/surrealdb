//! Aggregate function system for the streaming executor.
//!
//! This module provides traits for aggregate functions that operate over
//! groups of values (used with GROUP BY clauses).
//!
//! Aggregate functions differ from scalar functions in that they:
//! - Accumulate state across multiple input values
//! - Produce a single output value per group
//! - Support incremental computation via accumulators

use std::any::Any;
use std::fmt::Debug;

use anyhow::Result;

use super::Signature;
use crate::expr::Kind;
use crate::val::Value;

/// Accumulator for incremental aggregate computation.
///
/// Each accumulator maintains state for computing an aggregate over
/// a sequence of input values. Accumulators are created per-group
/// when executing GROUP BY queries.
pub trait Accumulator: Send + Sync + Debug {
	/// Update the accumulator with a new value.
	///
	/// Called once for each row in the group with the evaluated
	/// argument expression result.
	fn update(&mut self, value: Value) -> Result<()>;

	/// Merge another accumulator into this one.
	///
	/// Used for parallel execution where partial aggregates from
	/// different partitions need to be combined.
	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()>;

	/// Compute the final aggregate value.
	///
	/// Called after all values have been accumulated to produce
	/// the final result for the group.
	fn finalize(&self) -> Result<Value>;

	/// Reset the accumulator to its initial state.
	///
	/// Allows reusing the same accumulator instance for multiple groups.
	fn reset(&mut self);

	/// Clone the accumulator into a boxed trait object.
	fn clone_box(&self) -> Box<dyn Accumulator>;

	/// Returns self as Any for downcasting in merge operations.
	fn as_any(&self) -> &dyn Any;
}

/// An aggregate function that operates over groups of values.
///
/// Aggregate functions are registered in the function registry and
/// are detected during query planning when a GROUP BY clause is present.
///
/// Examples: count(), math::sum(), math::mean(), array::group()
pub trait AggregateFunction: Send + Sync + Debug {
	/// The fully qualified function name (e.g., "math::mean", "count").
	fn name(&self) -> &'static str;

	/// Create a new accumulator instance for this aggregate.
	///
	/// Called once per group during query execution.
	fn create_accumulator(&self) -> Box<dyn Accumulator>;

	/// Create a new accumulator instance with additional arguments.
	///
	/// Called once per group during query execution. The `args` parameter
	/// contains the evaluated values of any extra arguments (beyond the first
	/// accumulated argument). For example, `array::join(txt, " ")` would
	/// receive `[" "]` as args.
	///
	/// The default implementation ignores the args and delegates to `create_accumulator`.
	fn create_accumulator_with_args(&self, _args: &[Value]) -> Box<dyn Accumulator> {
		self.create_accumulator()
	}

	/// The function signature describing arguments and return type.
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// The default implementation returns the signature's return type.
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}
}

/// Helper macro to define simple aggregate functions with their accumulators.
///
/// # Usage
///
/// ```ignore
/// define_aggregate!(
///     MathSum,                        // Struct name for the function
///     "math::sum",                    // Function name
///     (value: Number) -> Number,      // Signature
///     SumAccumulator                  // Accumulator type
/// );
/// ```
#[macro_export]
macro_rules! define_aggregate {
	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$accumulator:ty
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::AggregateFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn create_accumulator(&self) -> Box<dyn $crate::exec::function::Accumulator> {
				Box::new(<$accumulator>::default())
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new()
					.arg(stringify!($arg_name), $crate::expr::Kind::$arg_type)
					.returns($crate::expr::Kind::$ret)
			}
		}
	};

	// No arguments (for count())
	(
		$struct_name:ident,
		$func_name:literal,
		() -> $ret:ident,
		$accumulator:ty
	) => {
		#[derive(Debug, Clone, Copy, Default)]
		pub struct $struct_name;

		impl $crate::exec::function::AggregateFunction for $struct_name {
			fn name(&self) -> &'static str {
				$func_name
			}

			fn create_accumulator(&self) -> Box<dyn $crate::exec::function::Accumulator> {
				Box::new(<$accumulator>::default())
			}

			fn signature(&self) -> $crate::exec::function::Signature {
				$crate::exec::function::Signature::new().returns($crate::expr::Kind::$ret)
			}
		}
	};
}

/// Helper macro to register multiple aggregate functions at once.
#[macro_export]
macro_rules! register_aggregates {
	($registry:expr, $($func:ty),* $(,)?) => {
		$(
			$registry.register_aggregate(<$func>::default());
		)*
	};
}
