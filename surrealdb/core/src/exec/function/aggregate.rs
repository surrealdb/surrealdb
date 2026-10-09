//! Aggregate function system for the engine.
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

	/// Update the accumulator with a batch of values.
	///
	/// The default implementation calls `update()` for each value.
	/// Accumulators can override this for better performance (e.g.,
	/// `CountAccumulator` can just add `values.len()` without iterating).
	#[cfg(test)]
	fn update_batch(&mut self, values: &[Value]) -> Result<()> {
		for value in values {
			self.update(value.clone())?;
		}
		Ok(())
	}

	/// Merge another accumulator into this one.
	///
	/// Used for parallel execution where partial aggregates from
	/// different partitions need to be combined.
	#[allow(unused)]
	fn merge(&mut self, other: Box<dyn Accumulator>) -> Result<()>;

	/// The accumulator's partial state as a value, for an aggregate that
	/// spills its groups, or `None` where the state cannot be saved. A state
	/// saved here and loaded into a fresh accumulator of the same function
	/// ([`Self::load`]) folds and finalizes as this one does.
	fn save(&self) -> Option<Value> {
		None
	}

	/// Whether the accumulator keeps its inputs, so what it holds grows with
	/// each value it folds: a breaker counts those values against its
	/// query's memory.
	fn holds_inputs(&self) -> bool {
		false
	}

	/// Replace this fresh accumulator's state with `state`, which
	/// [`Self::save`] answered for an accumulator of the same function.
	fn load(&mut self, state: Value) -> Result<()> {
		let _ = state;
		anyhow::bail!("This aggregate's state cannot be restored")
	}

	/// Compute the final aggregate value.
	///
	/// Called after all values have been accumulated to produce
	/// the final result for the group.
	fn finalize(&self) -> Result<Value>;

	/// Reset the accumulator to its initial state.
	///
	/// Allows reusing the same accumulator instance for multiple groups.
	#[allow(unused)]
	fn reset(&mut self);

	/// Clone the accumulator into a boxed trait object.
	#[allow(unused)]
	fn clone_box(&self) -> Box<dyn Accumulator>;

	/// Returns self as Any for downcasting in merge operations.
	#[allow(unused)]
	fn as_any(&self) -> &dyn Any;

	/// Bind the aggregate's arguments after the first.
	///
	/// `array::join(txt, sep)`'s separator describes the group rather than any
	/// one row, so it is evaluated once for the group and bound here rather
	/// than at construction: a group is not known to exist until a row opens
	/// it, and the groups are bound in group-key order, which is where a
	/// separator that writes, draws or reads the clock has its effect.
	///
	/// Called once per group, immediately before [`Self::finalize`]. The
	/// default implementation ignores the arguments, which is what every
	/// aggregate taking only the accumulated one wants.
	fn set_extra_args(&mut self, _args: &[Value]) {}
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

	/// A new accumulator held by value: the same accumulator
	/// [`Self::create_accumulator`] makes, as plain state where the function
	/// has one.
	fn accumulator_state(&self) -> super::builtin::aggregates::AccumulatorState {
		super::builtin::aggregates::AccumulatorState::Boxed(self.create_accumulator())
	}

	/// The function signature describing arguments and return type.
	#[allow(unused)]
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// The default implementation returns the signature's return type.
	#[allow(unused)]
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
