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
use crate::exec::ir::iface::Fallibility;
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

	/// Whether the accumulator keeps its inputs, so its size grows with
	/// each value it folds. The operator that runs it counts those values
	/// against the query's memory.
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
	/// than at construction. A group exists only once a row opens it. Groups
	/// are bound in group-key order, which decides the effect of a separator
	/// that writes, draws random values or reads the clock.
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

	/// What a group's failure can depend on: the failure classes of the
	/// accumulator's `update`, `merge` and `finalize` for arguments of the
	/// signature's kinds. Argument coercion is the call's `VALUE`, not the
	/// aggregate's.
	///
	/// A class is a superset when unsure. Every implementor declares a
	/// class; there is no default, so an undeclared one does not compile.
	#[allow(unused)]
	fn fallibility(&self) -> Fallibility;
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
///
/// Every aggregate names what its failure depends on last, as
/// `, fallibility: Fallibility::VALUE`
/// ([`AggregateFunction::fallibility`]). The argument is required: an
/// aggregate without it does not compile.
#[macro_export]
macro_rules! define_aggregate {
	// Single required argument: (name: Type) -> ReturnType
	(
		$struct_name:ident,
		$func_name:literal,
		($arg_name:ident : $arg_type:ident) -> $ret:ident,
		$accumulator:ty
		, fallibility: $fallibility:expr
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

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				$crate::declare_fallibility!(Aggregate, $func_name, $fallibility)
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
		, fallibility: $fallibility:expr
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

			fn fallibility(&self) -> $crate::exec::ir::iface::Fallibility {
				$crate::declare_fallibility!(Aggregate, $func_name, $fallibility)
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
