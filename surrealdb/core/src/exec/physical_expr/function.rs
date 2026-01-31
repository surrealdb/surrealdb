use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::val::Value;

/// Function call - count(), string::concat(a, b), etc.
#[derive(Debug, Clone)]
pub struct FunctionCallExpr {
	pub(crate) function: crate::expr::Function,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[async_trait]
impl PhysicalExpr for FunctionCallExpr {
	fn name(&self) -> &'static str {
		"FunctionCall"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// TODO: Function calls need full execution context with Stk, Options, and CursorDoc
		// These are not yet available in EvalContext for physical expressions
		// This will need to be implemented when the execution context is extended
		Err(anyhow::anyhow!(
			"Function call evaluation not yet supported in physical expressions - need Stk and Options in EvalContext"
		))
	}

	fn references_current_value(&self) -> bool {
		// Check if any argument references the current value
		self.arguments.iter().any(|e| e.references_current_value())
	}

	fn access_mode(&self) -> AccessMode {
		// Function calls may be read-write depending on the function
		// For now, check if the function itself is read-only
		let func_mode = if self.function.read_only() {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		};

		// Combine with argument access modes
		let args_mode = self.arguments.iter().map(|e| e.access_mode()).combine_all();
		func_mode.combine(args_mode)
	}
}

impl ToSql for FunctionCallExpr {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		// Convert to FunctionCall for formatting
		// Note: We can't easily convert physical exprs back to logical exprs,
		// so we just show the function name without arguments
		f.push_str(&self.function.to_idiom().to_sql());
		f.push_str("(...)");
	}
}

/// Closure expression - |$x| $x * 2
#[derive(Debug, Clone)]
pub struct ClosurePhysicalExpr {
	pub(crate) closure: crate::expr::ClosureExpr,
}

#[async_trait]
impl PhysicalExpr for ClosurePhysicalExpr {
	fn name(&self) -> &'static str {
		"Closure"
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Closures evaluate to a Value::Closure
		// This is similar to how the old executor handles it
		// TODO: Need to capture parameters from context
		Err(anyhow::anyhow!(
			"Closure evaluation not yet fully implemented - need parameter capture from context"
		))
	}

	fn references_current_value(&self) -> bool {
		// Closures capture their environment, but don't directly reference current value
		// The body might, but that's evaluated later when the closure is called
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Closures themselves are read-only (they're values)
		// What they do when called is a different matter
		AccessMode::ReadOnly
	}
}

impl ToSql for ClosurePhysicalExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.closure.fmt_sql(f, fmt);
	}
}
