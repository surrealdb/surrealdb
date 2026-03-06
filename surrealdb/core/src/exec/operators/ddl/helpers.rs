use anyhow::Result;

use crate::dbs::Options;
use crate::exec::context::ExecutionContext;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResultExt;
use crate::val::Value;

/// Extract [`Options`] from the execution context.
pub(crate) fn get_opt(ctx: &ExecutionContext) -> Result<&Options> {
	ctx.root()
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))
}

/// Evaluate a [`PhysicalExpr`] to a [`Value`], catching RETURN control flow.
pub(crate) async fn eval_value(expr: &dyn PhysicalExpr, ctx: &ExecutionContext) -> Result<Value> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	expr.evaluate(eval_ctx).await.catch_return()
}

/// Evaluate a [`PhysicalExpr`] as a string identifier.
pub(crate) async fn eval_ident(expr: &dyn PhysicalExpr, ctx: &ExecutionContext) -> Result<String> {
	eval_value(expr, ctx).await?.coerce_to::<String>().map_err(anyhow::Error::from)
}

/// Evaluate a [`PhysicalExpr`] as an optional comment (`Option<String>`).
pub(crate) async fn eval_comment(
	expr: &dyn PhysicalExpr,
	ctx: &ExecutionContext,
) -> Result<Option<String>> {
	eval_value(expr, ctx).await?.cast_to().map_err(anyhow::Error::from)
}

/// Wrap an async `Result<Value>` DDL execution function into the
/// `execute` method pattern used by DDL [`ExecOperator`] impls.
///
/// Returns a single-element stream that either yields `Value::None`
/// or propagates the error as `ControlFlow::Err`.
pub(crate) fn ddl_stream<F>(
	ctx: &ExecutionContext,
	f: F,
) -> crate::exec::FlowResult<crate::exec::ValueBatchStream>
where
	F: FnOnce(ExecutionContext) -> crate::exec::BoxFut<'static, Result<Value>> + Send + 'static,
{
	let ctx = ctx.clone();
	Ok(Box::pin(futures::stream::once(async move {
		match f(ctx).await {
			Ok(value) => Ok(crate::exec::ValueBatch {
				values: vec![value],
			}),
			Err(e) => Err(crate::expr::ControlFlow::Err(e)),
		}
	})))
}

/// Shorthand for the common set of DDL operator [`ExecOperator`] trait impls.
///
/// Given the large number of DDL operators that share identical trait
/// method implementations (access_mode, cardinality, scalar, metrics),
/// this macro reduces per-file boilerplate.
macro_rules! ddl_operator_common {
	($name_str:expr, $ctx_level:expr) => {
		fn name(&self) -> &'static str {
			$name_str
		}

		fn required_context(&self) -> crate::exec::context::ContextLevel {
			$ctx_level
		}

		fn access_mode(&self) -> crate::exec::AccessMode {
			crate::exec::AccessMode::ReadWrite
		}

		fn cardinality_hint(&self) -> crate::exec::CardinalityHint {
			crate::exec::CardinalityHint::AtMostOne
		}

		fn is_scalar(&self) -> bool {
			true
		}

		fn metrics(&self) -> Option<&crate::exec::OperatorMetrics> {
			Some(self.metrics.as_ref())
		}
	};
}

pub(crate) use ddl_operator_common;
