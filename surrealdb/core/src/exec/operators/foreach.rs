//! Foreach operator with deferred planning.
//!
//! The ForeachPlan operator implements FOR loop iteration over arrays/ranges,
//! using deferred planning like SequencePlan. It handles BREAK/CONTINUE control
//! flow signals within the loop body.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::{SqlFormat, ToSql};

use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::plan_or_compute::{
	block_required_context, evaluate_body_expr, evaluate_expr, expr_required_context,
};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::{Block, ControlFlow, ControlFlowExt, Expr, Param};
use crate::val::Value;
use crate::val::range::IntegerRangeIter;

/// Foreach operator with deferred planning.
///
/// Iterates over an array or integer range, executing the body block for
/// each element with the loop variable bound in the context.
///
/// Example:
/// ```surql
/// FOR $item IN [1, 2, 3] {
///     CREATE foo SET value = $item;
/// }
/// ```
#[derive(Debug)]
pub struct ForeachPlan {
	/// Loop variable parameter
	pub param: Param,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// Range expression (evaluates to Array or Range)
	pub range: Expr,
	/// Loop body block
	pub body: Block,
}

impl ForeachPlan {
	pub(crate) fn new(param: Param, range: Expr, body: Block) -> Self {
		Self {
			param,
			range,
			body,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

/// Iterator enum for foreach - handles both arrays and integer ranges.
enum ForeachIter {
	Array(std::vec::IntoIter<Value>),
	Range(std::iter::Map<IntegerRangeIter, fn(i64) -> Value>),
}

impl Iterator for ForeachIter {
	type Item = Value;

	fn next(&mut self) -> Option<Self::Item> {
		match self {
			ForeachIter::Array(iter) => iter.next(),
			ForeachIter::Range(iter) => iter.next(),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ForeachPlan {
	fn name(&self) -> &'static str {
		"Foreach"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("param".to_string(), self.param.to_string()),
			("statements".to_string(), self.body.0.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		// Derive the required context from the range expression and body block
		expr_required_context(&self.range).max(block_required_context(&self.body))
	}

	fn access_mode(&self) -> AccessMode {
		// Check if range and body require write access
		let range_read_only = self.range.read_only();
		let body_read_only = self.body.read_only();

		if range_read_only && body_read_only {
			AccessMode::ReadOnly
		} else {
			AccessMode::ReadWrite
		}
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let param = self.param.clone();
		let range = self.range.clone();
		let body = self.body.clone();
		let ctx = ctx.clone();

		let stream =
			stream::once(async move { execute_foreach(&param, &range, &body, &ctx).await });

		Ok(Box::pin(stream))
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// With deferred planning, we don't have pre-built children
		vec![]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		// FOR loops return a single value (NONE)
		true
	}
}

/// Execute the FOR loop with deferred planning.
async fn execute_foreach(
	param: &Param,
	range: &Expr,
	body: &Block,
	ctx: &ExecutionContext,
) -> crate::expr::FlowResult<ValueBatch> {
	// First, evaluate the range expression
	let range_value = evaluate_expr(range, ctx).await?;

	// Create the iterator based on the range value
	let iter = match range_value {
		Value::Array(arr) => ForeachIter::Array(arr.into_iter()),
		Value::Range(r) => {
			let r = r.coerce_to_typed::<i64>().map_err(Error::from).context("Invalid FOR range")?;
			ForeachIter::Range(r.iter().map(Value::from))
		}
		v => {
			return Err(ControlFlow::Err(anyhow::Error::new(Error::InvalidStatementTarget {
				value: v.to_raw_string(),
			})));
		}
	};

	// Loop variable name
	let param_name = param.as_str().to_owned();

	// Iterate over each value
	for v in iter {
		// Check timeout (also yields for cooperative scheduling)
		ctx.ctx().expect_not_timedout().await.map_err(ControlFlow::Err)?;
		// Check for cancellation via the streaming executor's token
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(crate::err::Error::QueryCancelled)));
		}

		// Create a new context with the loop variable bound
		// This is the base context for this iteration - LET statements will build on this
		let mut current_ctx = ctx.with_param(param_name.clone(), v.clone());

		// Execute each statement in the body
		for expr in body.0.iter() {
			let result = evaluate_body_expr(expr, &mut current_ctx, &param_name, &v).await;

			// Handle control flow signals
			match result {
				Ok(_) => {
					// Continue to next statement
				}
				Err(ControlFlow::Continue) => {
					// Skip remaining statements, move to next iteration
					break;
				}
				Err(ControlFlow::Break) => {
					// Exit the loop entirely
					return Ok(ValueBatch {
						values: vec![Value::None],
					});
				}
				Err(ctrl) => {
					// Propagate RETURN and errors upward
					return Err(ctrl);
				}
			}
		}
	}

	// Loop completed normally - return NONE
	Ok(ValueBatch {
		values: vec![Value::None],
	})
}

impl ToSql for ForeachPlan {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("FOR ");
		self.param.fmt_sql(f, fmt);
		f.push_str(" IN ");
		self.range.fmt_sql(f, fmt);
		f.push(' ');
		self.body.fmt_sql(f, fmt);
	}
}
