use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, CombineAccessModes};
use crate::expr::Function;
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Get the function name - only Normal (built-in) functions supported for now
		let name = match &self.function {
			Function::Normal(name) => name.as_str(),
			Function::Custom(name) => {
				return Err(anyhow::anyhow!(
					"Custom function 'fn::{}' requires execution context - not yet supported in streaming executor",
					name
				));
			}
			Function::Script(_) => {
				return Err(anyhow::anyhow!(
					"Script functions require execution context - not yet supported in streaming executor"
				));
			}
			Function::Model(m) => {
				return Err(anyhow::anyhow!(
					"Model function '{:?}' requires execution context - not yet supported in streaming executor",
					m
				));
			}
			Function::Module(m, s) => {
				let name = match s {
					Some(s) => format!("mod::{}::{}", m, s),
					None => format!("mod::{}", m),
				};
				return Err(anyhow::anyhow!(
					"Module function '{}' requires execution context - not yet supported in streaming executor",
					name
				));
			}
			Function::Silo {
				org,
				pkg,
				..
			} => {
				return Err(anyhow::anyhow!(
					"Silo function 'silo::{}::{}' requires execution context - not yet supported in streaming executor",
					org,
					pkg
				));
			}
		};

		// Look up the function in the registry
		let registry = ctx.exec_ctx.function_registry();
		let func = registry.get(name).ok_or_else(|| {
			anyhow::anyhow!("Unknown function '{}' - not found in function registry", name)
		})?;

		// Evaluate all arguments first
		let mut args = Vec::with_capacity(self.arguments.len());
		for arg_expr in &self.arguments {
			args.push(arg_expr.evaluate(ctx.clone()).await?);
		}

		// Invoke the function based on whether it's pure or needs context
		if func.is_pure() && !func.is_async() {
			func.invoke(args)
		} else {
			func.invoke_async(&ctx, args).await
		}
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
