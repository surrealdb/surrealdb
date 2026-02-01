//! Closure operator for capturing context and creating closure values.
//!
//! The ClosurePlan operator captures the current execution context and returns
//! a `Value::Closure` that can be called later. Unlike other operators, closures
//! don't execute their body immediately - they capture variables and return
//! a callable value.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::FrozenContext;
use crate::dbs::ParameterCapturePass;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, FlowResult, OperatorPlan, ValueBatch, ValueBatchStream};
use crate::expr::{ControlFlow, Expr, Kind, Param};
use crate::val::{Closure, Value};

/// Closure operator that captures context and returns a closure value.
///
/// When executed, this operator:
/// 1. Builds a `FrozenContext` from the current `ExecutionContext`
/// 2. Uses `ParameterCapturePass` to identify variables referenced in the body
/// 3. Returns a `Value::Closure` with the captured variables
///
/// The closure body is NOT executed at this point - execution happens when
/// the closure is invoked (e.g., in array methods like `.map()` or `.filter()`).
///
/// Example:
/// ```surql
/// -- Creates a closure value
/// LET $double = |$x: int| -> int { $x * 2 };
///
/// -- Closure is invoked here with each array element
/// [1, 2, 3].map($double);
/// ```
#[derive(Debug)]
pub struct ClosurePlan {
	/// Closure arguments with optional types
	pub args: Vec<(Param, Kind)>,
	/// Optional return type
	pub returns: Option<Kind>,
	/// Closure body expression (not executed, just captured)
	pub body: Expr,
}

/// Create a FrozenContext for parameter capture from the current execution context.
fn create_capture_context(exec_ctx: &ExecutionContext) -> FrozenContext {
	let mut ctx = crate::ctx::Context::background();
	ctx.set_transaction(exec_ctx.txn().clone());
	for (name, value) in exec_ctx.params().iter() {
		ctx.add_value(name.clone(), value.clone());
	}
	ctx.freeze()
}

#[async_trait]
impl OperatorPlan for ClosurePlan {
	fn name(&self) -> &'static str {
		"Closure"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("args".to_string(), self.args.len().to_string())];
		if self.returns.is_some() {
			attrs.push(("has_return_type".to_string(), "true".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Closures can be created at any context level - they don't need
		// database access themselves (body execution happens later)
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// Creating a closure is always read-only - the body isn't executed
		// The actual access mode depends on what the closure does when invoked
		AccessMode::ReadOnly
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let args = self.args.clone();
		let returns = self.returns.clone();
		let body = self.body.clone();
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_closure(&args, &returns, &body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		// Closures don't have children - body is captured, not planned
		vec![]
	}

	fn is_scalar(&self) -> bool {
		// Closures return a single closure value
		true
	}
}

/// Execute the closure operator - capture context and return a closure value.
async fn execute_closure(
	args: &[(Param, Kind)],
	returns: &Option<Kind>,
	body: &Expr,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	// Create a frozen context for parameter capture
	let frozen_ctx = create_capture_context(ctx);

	// Capture all parameters referenced in the body from the current context
	let captures = ParameterCapturePass::capture(&frozen_ctx, body);

	// Create the closure value with captured context
	let closure_value = Value::Closure(Box::new(Closure::Expr {
		args: args.to_vec(),
		returns: returns.clone(),
		captures,
		body: body.clone(),
	}));

	Ok(ValueBatch {
		values: vec![closure_value],
	})
}

impl ToSql for ClosurePlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('|');
		for (i, (param, kind)) in self.args.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			param.fmt_sql(f, fmt);
			if !kind.is_any() {
				f.push_str(": ");
				kind.fmt_sql(f, fmt);
			}
		}
		f.push('|');
		if let Some(ref returns) = self.returns {
			f.push_str(" -> ");
			returns.fmt_sql(f, fmt);
		}
		f.push(' ');
		self.body.fmt_sql(f, fmt);
	}
}

impl From<ControlFlow> for crate::expr::FlowResult<ValueBatch> {
	fn from(ctrl: ControlFlow) -> Self {
		Err(ctrl)
	}
}
