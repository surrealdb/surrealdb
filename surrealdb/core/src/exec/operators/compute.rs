//! Compute operator for evaluating expressions and adding results as fields.
//!
//! The Compute operator is the central place where complex expressions are evaluated.
//! It takes a list of (internal_name, expression) pairs, evaluates each expression
//! against the current record, and adds the results as new fields.
//!
//! This enables the "compute once, reference by name" pattern:
//! 1. Complex expressions are identified during planning
//! 2. They're registered with internal names (e.g., "_e0" or output aliases)
//! 3. Compute evaluates them once and adds results as fields
//! 4. Downstream operators (Sort, Project) reference them by name

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::{Object, Value};

/// Evaluates expressions and adds results as fields to each record.
///
/// This operator is the single point of evaluation for complex expressions
/// in a query. By centralizing computation here, we ensure expressions are
/// evaluated exactly once, avoiding duplicate work in Sort and Project.
#[derive(Debug, Clone)]
pub struct Compute {
	/// The input plan to compute from
	pub input: Arc<dyn ExecOperator>,
	/// Fields to compute: (internal_name, expression)
	pub fields: Vec<(String, Arc<dyn PhysicalExpr>)>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Compute {
	/// Create a new Compute operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields: Vec<(String, Arc<dyn PhysicalExpr>)>,
	) -> Self {
		Self {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Compute {
	fn name(&self) -> &'static str {
		"Compute"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let fields_str = self
			.fields
			.iter()
			.map(|(name, expr)| format!("{} = {}", name, expr.to_sql()))
			.collect::<Vec<_>>()
			.join(", ");
		vec![("fields".to_string(), fields_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// Combine field expression contexts with child operator context
		let expr_ctx = self
			.fields
			.iter()
			.map(|(_, expr)| expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		self.input.required_context().max(expr_ctx)
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with all expression access modes
		// An expression could contain a mutation subquery!
		let expr_mode = self.fields.iter().map(|(_, expr)| expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.fields.iter().map(|(name, expr)| (name.as_str(), expr)).collect()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// If there are no fields to compute, just pass through
		if self.fields.is_empty() {
			return self.input.execute(ctx);
		}

		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let fields = self.fields.clone();
		let ctx = ctx.clone();

		// Create a stream that computes fields for each batch
		let computed = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let eval_ctx = EvalContext::from_exec_ctx(&ctx);

				compute_batch(&batch.values, &fields, eval_ctx).await
			}
		});

		Ok(monitor_stream(Box::pin(computed), "Compute", &self.metrics))
	}
}

/// Compute all fields across a batch of values using per-field batch evaluation.
///
/// For each field expression, evaluates it across all rows in one `evaluate_batch` call,
/// then merges the results into the per-row output objects.
///
/// If a field's batch evaluation hits a `ControlFlow::Return` signal (rare -- only from
/// explicit RETURN statements in function bodies), that field falls back to per-row
/// evaluation where RETURN values are caught and used as field values.
async fn compute_batch(
	values: &[Value],
	fields: &[(String, Arc<dyn PhysicalExpr>)],
	eval_ctx: EvalContext<'_>,
) -> Result<ValueBatch, ControlFlow> {
	// Initialize output objects from input values.
	// Geometry values are converted to their GeoJSON object representation
	// so that downstream operators (SelectProject) can access fields like
	// `type` and `coordinates` directly.
	let mut objects: Vec<Object> = Vec::with_capacity(values.len());

	for v in values.iter() {
		let o = match v {
			Value::Object(o) => o.clone(),
			Value::Geometry(geo) => geo.as_object(),
			Value::RecordId(rid) => {
				if let Value::Object(v) = super::fetch::fetch_record(eval_ctx.exec_ctx, rid).await?
				{
					v
				} else {
					Object::default()
				}
			}
			_ => Object::default(),
		};
		objects.push(o);
	}

	// Batch each field expression across all rows
	for (name, expr) in fields {
		match expr.evaluate_batch(eval_ctx.clone(), values).await {
			Ok(computed_values) => {
				for (i, computed) in computed_values.into_iter().enumerate() {
					objects[i].insert(name.clone(), computed);
				}
			}
			Err(ControlFlow::Return(_)) => {
				// Batch evaluation hit a RETURN signal. Fall back to per-row
				// evaluation for this field only, catching RETURN as a value.
				for (i, value) in values.iter().enumerate() {
					let computed = match expr.evaluate(eval_ctx.with_value(value)).await {
						Ok(v) => v,
						Err(ControlFlow::Return(v)) => v,
						Err(e) => return Err(e),
					};
					objects[i].insert(name.clone(), computed);
				}
			}
			Err(e) => return Err(e),
		}
	}

	Ok(ValueBatch {
		values: objects.into_iter().map(Value::Object).collect(),
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::physical_expr::Literal;
	use crate::val::Number;

	/// Helper to create a simple literal expression
	fn literal_expr(value: i64) -> Arc<dyn PhysicalExpr> {
		Arc::new(Literal(Value::Number(Number::Int(value))))
	}

	#[test]
	fn test_compute_attrs() {
		// We can't easily test execute without a full context,
		// but we can test the operator's metadata methods
		use crate::exec::operators::SourceExpr;

		let source = Arc::new(SourceExpr::new(literal_expr(1)));

		let compute = Compute::new(
			source,
			vec![("a".to_string(), literal_expr(42)), ("b".to_string(), literal_expr(100))],
		);

		assert_eq!(compute.name(), "Compute");
		assert_eq!(compute.fields.len(), 2);
		assert!(!compute.fields.is_empty());

		let attrs = compute.attrs();
		assert_eq!(attrs.len(), 1);
		assert!(attrs[0].1.contains("a = 42"));
		assert!(attrs[0].1.contains("b = 100"));
	}

	#[test]
	fn test_compute_empty() {
		use crate::exec::operators::SourceExpr;

		let source = Arc::new(SourceExpr::new(literal_expr(1)));

		let compute = Compute::new(source, vec![]);

		assert!(compute.fields.is_empty());
		assert_eq!(compute.fields.len(), 0);
	}
}
