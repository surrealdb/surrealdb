//! Projection function system for the streaming executor.
//!
//! This module provides traits for projection functions that produce field bindings
//! rather than single values. These functions affect the structure of output objects
//! in SELECT projections.
//!
//! Projection functions differ from scalar functions in that they:
//! - Return multiple (Idiom, Value) pairs that become output fields
//! - Derive output field names from their arguments at runtime
//! - Are "transparent" to the projection - the function name doesn't appear in output
//!
//! Examples: type::field, type::fields

use std::fmt::Debug;
use std::pin::Pin;

use anyhow::Result;

use super::Signature;
use crate::exec::ContextLevel;
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::expr::idiom::Idiom;
use crate::val::Value;

/// A projection function that produces field bindings for output objects.
///
/// Unlike scalar functions which return a single value, projection functions
/// return a list of (Idiom, Value) pairs that become fields in the output object.
///
/// For example, `SELECT type::field("name") FROM person` produces `{ name: "value" }`
/// rather than `{ "type::field": "value" }`.
pub trait ProjectionFunction: Send + Sync + Debug {
	/// The fully qualified function name (e.g., "type::field", "type::fields")
	fn name(&self) -> &'static str;

	/// The function signature describing arguments and return type.
	fn signature(&self) -> Signature;

	/// Infer the return type given the argument types.
	///
	/// The default implementation returns the signature's return type.
	fn return_type(&self, _arg_types: &[Kind]) -> Result<Kind> {
		Ok(self.signature().returns)
	}

	/// Whether this function requires async execution.
	///
	/// Projection functions typically need to evaluate idioms against documents,
	/// which may require async operations.
	fn is_async(&self) -> bool {
		true
	}

	/// The minimum context level required to execute this function.
	///
	/// Projection functions typically need database context to evaluate field paths.
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	/// Evaluate and return field-value bindings for the output object.
	///
	/// Each (Idiom, Value) pair becomes a field in the output object.
	/// The Idiom specifies the field path (e.g., "name" or "address.city"),
	/// and the Value is the field's value.
	///
	/// # Arguments
	/// * `ctx` - The evaluation context with access to current row and parameters
	/// * `args` - The evaluated function arguments
	///
	/// # Returns
	/// A vector of (Idiom, Value) pairs to set on the output object
	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Vec<(Idiom, Value)>>> + Send + 'a>>;
}
