//! ComputeFields operator for evaluating COMPUTED fields.
//!
//! This operator is mandatory after every Scan or RecordIdLookup. It evaluates
//! COMPUTED field expressions defined on the table schema and injects the results
//! into each record.
//!
//! The table name can be an expression (e.g., `"prefix" + $param`), so field
//! definitions are resolved lazily at execution time.

use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::OnceCell;

use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, OperatorPlan, PhysicalExpr, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::val::{TableName, Value};

/// Computes COMPUTED fields for each record in the stream.
///
/// This operator is mandatory after Scan/RecordIdLookup. It:
/// - Evaluates the table expression to get the table name (lazy resolution)
/// - Fetches field definitions for the table
/// - For each record: evaluates COMPUTED field expressions and injects results
/// - Caches field definitions after first batch (table name won't change mid-stream)
#[derive(Debug, Clone)]
pub struct ComputeFields {
	/// The input plan to compute fields for
	pub(crate) input: Arc<dyn OperatorPlan>,
	/// Table name expression (evaluated at runtime for lazy resolution)
	pub(crate) table: Arc<dyn PhysicalExpr>,
}

/// Cached state for computed field evaluation.
/// Initialized on first batch and reused for subsequent batches.
#[derive(Debug)]
struct ComputeFieldsState {
	/// Resolved table name (kept for debugging/error messages)
	#[allow(dead_code)]
	table_name: TableName,
	/// Computed field definitions converted to physical expressions
	computed_fields: Vec<ComputedFieldDef>,
}

/// A computed field definition ready for evaluation.
#[derive(Debug)]
struct ComputedFieldDef {
	/// The field name where to store the result
	field_name: String,
	/// The physical expression to evaluate
	expr: Arc<dyn PhysicalExpr>,
	/// Optional type coercion
	kind: Option<crate::expr::Kind>,
}

impl OperatorPlan for ComputeFields {
	fn name(&self) -> &'static str {
		"ComputeFields"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("table".to_string(), self.table.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// ComputeFields needs Database for field definition lookup
		// Also inherits child requirements
		ContextLevel::Database.max(self.input.required_context())
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let input_stream = self.input.execute(ctx)?;
		let table_expr = self.table.clone();
		let ctx = ctx.clone();

		// Lazy-initialized state - OnceCell avoids locking after first initialization
		let cached_state: Arc<OnceCell<ComputeFieldsState>> = Arc::new(OnceCell::new());

		let computed = input_stream.then(move |batch_result| {
			let table_expr = table_expr.clone();
			let ctx = ctx.clone();
			let cached_state = cached_state.clone();

			async move {
				let mut batch = batch_result?;

				// Empty batches pass through unchanged
				if batch.values.is_empty() {
					return Ok(batch);
				}

				// Initialize state on first batch, or get cached reference
				let state = cached_state.get_or_try_init(|| build_state(&ctx, &table_expr)).await?;

				// No computed fields - pass through unchanged
				if state.computed_fields.is_empty() {
					return Ok(batch);
				}

				// Process each value in the batch
				compute_fields_for_batch(&ctx, state, &mut batch.values).await?;

				Ok(batch)
			}
		});

		Ok(Box::pin(computed))
	}
}

/// Build the state by resolving table name and fetching field definitions.
async fn build_state(
	ctx: &ExecutionContext,
	table_expr: &Arc<dyn PhysicalExpr>,
) -> Result<ComputeFieldsState, ControlFlow> {
	let table_name = resolve_table_name(ctx, table_expr).await?;
	let computed_fields = fetch_computed_fields(ctx, &table_name).await?;

	Ok(ComputeFieldsState {
		table_name,
		computed_fields,
	})
}

/// Resolve the table name from the table expression.
async fn resolve_table_name(
	ctx: &ExecutionContext,
	table_expr: &Arc<dyn PhysicalExpr>,
) -> Result<TableName, ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let table_value = table_expr.evaluate(eval_ctx).await.map_err(|e| {
		ControlFlow::Err(anyhow::anyhow!("Failed to evaluate table expression: {}", e))
	})?;

	match table_value {
		Value::String(s) => Ok(TableName::from(s)),
		Value::Table(t) => Ok(t),
		_ => Err(ControlFlow::Err(anyhow::anyhow!(
			"Table expression must evaluate to a string or table, got: {:?}",
			table_value
		))),
	}
}

/// Fetch computed field definitions for the table and convert to physical expressions.
async fn fetch_computed_fields(
	ctx: &ExecutionContext,
	table_name: &TableName,
) -> Result<Vec<ComputedFieldDef>, ControlFlow> {
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let txn = ctx.txn();

	let field_defs = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, table_name, None)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get field definitions: {}", e)))?;

	// Convert computed field expressions to PhysicalExpr
	let mut computed_fields = Vec::new();
	for fd in field_defs.iter() {
		if let Some(ref expr) = fd.computed {
			let physical_expr = expr_to_physical_expr(expr.clone()).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(
					"Computed field '{}' has unsupported expression: {}",
					fd.name.to_raw_string(),
					e
				))
			})?;

			computed_fields.push(ComputedFieldDef {
				field_name: fd.name.to_raw_string(),
				expr: physical_expr,
				kind: fd.field_kind.clone(),
			});
		}
	}

	Ok(computed_fields)
}

/// Compute fields for all values in the batch.
async fn compute_fields_for_batch(
	ctx: &ExecutionContext,
	state: &ComputeFieldsState,
	values: &mut [Value],
) -> Result<(), ControlFlow> {
	for value in values.iter_mut() {
		compute_fields_for_value(ctx, state, value).await?;
	}

	Ok(())
}

/// Compute all computed fields for a single value.
async fn compute_fields_for_value(
	ctx: &ExecutionContext,
	state: &ComputeFieldsState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx);

	for cf in &state.computed_fields {
		// Evaluate with the current value as context
		let row_ctx = eval_ctx.with_value(value);
		let computed_value = cf.expr.evaluate(row_ctx).await.map_err(|e| {
			ControlFlow::Err(anyhow::anyhow!("Failed to compute field '{}': {}", cf.field_name, e))
		})?;

		// Apply type coercion if specified
		let final_value = if let Some(ref kind) = cf.kind {
			computed_value.coerce_to_kind(kind).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(
					"Failed to coerce computed field '{}': {}",
					cf.field_name,
					e
				))
			})?
		} else {
			computed_value
		};

		// Inject the computed value into the document
		if let Value::Object(obj) = value {
			obj.insert(cf.field_name.clone(), final_value);
		} else {
			return Err(ControlFlow::Err(anyhow::anyhow!("Value is not an object: {:?}", value)));
		}
	}

	Ok(())
}
