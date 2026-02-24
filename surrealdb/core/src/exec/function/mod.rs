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
mod index;
mod macros;
mod method;
mod projection;
mod registry;
mod signature;

use std::fmt::Debug;

pub use aggregate::{Accumulator, AggregateFunction};
use anyhow::Result;
pub use index::{
	IndexContext, IndexContextKind, IndexFunction, KnnContext, MatchInfo, MatchesContext,
};
pub use method::MethodDescriptor;
pub use projection::ProjectionFunction;
pub use registry::FunctionRegistry;
pub use signature::Signature;

use crate::exec::physical_expr::EvalContext;
use crate::exec::{BoxFut, SendSyncRequirement};
use crate::expr::Kind;
use crate::val::Value;

/// A scalar function that can be invoked during query execution.
///
/// Scalar functions operate on individual values and return a single value.
/// They may be:
/// - Pure: operate only on their arguments with no side effects
/// - Context-aware: need access to session/database state
/// - Async: perform I/O operations
pub trait ScalarFunction: SendSyncRequirement + Debug {
	/// The fully qualified function name (e.g., "math::abs", "string::len")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	#[allow(unused)]
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// This is used during planning for type inference. The default
	/// implementation returns the signature's return type.
	#[allow(unused)]
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}

	/// The minimum context level required to execute this function.
	///
	/// Functions that access database state (e.g., analyzers, custom functions)
	/// should override this to return `ContextLevel::Database`.
	/// The default is `Root` (no namespace/database context needed).
	fn required_context(&self) -> crate::exec::ContextLevel {
		crate::exec::ContextLevel::Root
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
	) -> BoxFut<'a, Result<Value>> {
		Box::pin(async move { self.invoke(args) })
	}
}
