//! Function system for the streaming executor.
//!
//! This module provides a trait-based function system that supports:
//! - Pure scalar functions (no context needed)
//! - Context-aware scalar functions (need execution context)
//! - Async scalar functions (HTTP, crypto, etc.)
//! - Aggregate functions (operate over groups of values)
//! - Projection functions (produce field bindings for output objects)
//!
//! Functions are registered in a `FunctionRegistry` which can be accessed
//! through the execution context.

mod aggregate;
mod builtin;
mod macros;
mod method;
mod projection;
mod registry;
mod signature;

use std::fmt::Debug;

pub use aggregate::{Accumulator, AggregateFunction};
use anyhow::Result;
pub use method::{MethodDescriptor, MethodRegistry};
pub use projection::ProjectionFunction;
pub use registry::FunctionRegistry;
pub use signature::Signature;

use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::val::Value;

/// A scalar function that can be invoked during query execution.
///
/// Scalar functions operate on individual values and return a single value.
/// They may be:
/// - Pure: operate only on their arguments with no side effects
/// - Context-aware: need access to session/database state
/// - Async: perform I/O operations
pub trait ScalarFunction: Send + Sync + Debug {
	/// The fully qualified function name (e.g., "math::abs", "string::len")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// This is used during planning for type inference. The default
	/// implementation returns the signature's return type.
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}

	/// Whether this function is pure (no context needed).
	///
	/// Pure functions can be evaluated with just their arguments.
	/// Non-pure functions need access to the execution context.
	fn is_pure(&self) -> bool {
		true
	}

	/// Whether this function requires async execution.
	///
	/// Async functions perform I/O or other blocking operations.
	fn is_async(&self) -> bool {
		false
	}

	/// Synchronous invocation for pure functions.
	///
	/// This is the primary entry point for pure scalar functions.
	/// The default implementation returns an error.
	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires context or async execution", self.name()))
	}

	/// Async invocation with context access.
	///
	/// This is used for context-aware or async functions.
	/// The default implementation delegates to `invoke()`.
	#[allow(unused_variables)]
	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move { self.invoke(args) })
	}
}
