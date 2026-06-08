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

use futures::StreamExt;

use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::val::{Object, Strand, Value};

/// Evaluates expressions and adds results as fields to each record.
///
/// This operator is the single point of evaluation for complex expressions
/// in a query. By centralizing computation here, we ensure expressions are
/// evaluated exactly once, avoiding duplicate work in Sort and Project.
///
/// Field names are stored as [`Strand`] rather than `String` so per-row
/// inserts into the output [`Object`] only pay a 24-byte bitwise copy for
/// short names (which covers virtually every SurrealQL identifier), avoiding
/// the heap allocation that `String::clone` would do on every (row × field).
#[derive(Debug, Clone)]
pub struct Compute {
	/// The input plan to compute from
	pub input: Arc<dyn ExecOperator>,
	/// Fields to compute: (internal_name, expression)
	pub fields: Vec<(Strand, Arc<dyn PhysicalExpr>)>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Compute {
	/// Create a new Compute operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields: Vec<(Strand, Arc<dyn PhysicalExpr>)>,
	) -> Self {
		Self {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
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
			ctx.root().ctx.config.operator_buffer_size,
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

				compute_batch(batch.values, &fields, eval_ctx).await
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
/// Takes `values` by value so per-row Objects can be moved out of the input batch
/// instead of deep-cloned. The seed pass for non-Object inputs (RecordId fetches,
/// Geometry conversion) runs first to preserve side-effect ordering relative to
/// field expression evaluation; the Object payloads themselves are moved out of
/// the input batch in a final pass once expression evaluation is complete.
///
/// If a field's batch evaluation hits a `ControlFlow::Return` signal (rare -- only from
/// explicit RETURN statements in function bodies), that field falls back to per-row
/// evaluation where RETURN values are caught and used as field values.
async fn compute_batch(
	values: Vec<Value>,
	fields: &[(Strand, Arc<dyn PhysicalExpr>)],
	eval_ctx: EvalContext<'_>,
) -> Result<ValueBatch, ControlFlow> {
	// Run side-effectful per-row work (RecordId fetches, Geometry → object
	// conversion) in input order, before field-expression evaluation, so that
	// any mutating subqueries in field expressions don't reorder relative to
	// the upstream record fetch. `Value::Object` inputs are deferred with
	// `None`; their payload is moved out of the input batch later to avoid a
	// deep clone.
	let mut seeded: Vec<Option<Object>> = Vec::with_capacity(values.len());
	for v in values.iter() {
		let row = match v {
			Value::Object(_) => None,
			Value::Geometry(geo) => Some(geo.as_object()),
			Value::RecordId(rid) => {
				let o = if let Value::Object(v) =
					super::fetch::fetch_record(eval_ctx.exec_ctx, rid).await?
				{
					v
				} else {
					Object::default()
				};
				Some(o)
			}
			_ => Some(Object::default()),
		};
		seeded.push(row);
	}

	// Evaluate each field expression across all rows while we still hold the
	// input values by reference; results are merged into the output objects
	// after the values have been consumed.
	let mut field_results: Vec<Vec<Value>> = Vec::with_capacity(fields.len());
	for (_name, expr) in fields {
		let computed = match expr.evaluate_batch(eval_ctx.clone(), &values).await {
			Ok(v) => v,
			Err(ControlFlow::Return(_)) => {
				// Batch evaluation hit a RETURN signal. Fall back to per-row
				// evaluation for this field only, catching RETURN as a value.
				let mut per_row = Vec::with_capacity(values.len());
				for value in values.iter() {
					let v = match expr.evaluate(eval_ctx.with_value(value)).await {
						Ok(v) => v,
						Err(ControlFlow::Return(v)) => v,
						Err(e) => return Err(e),
					};
					per_row.push(v);
				}
				per_row
			}
			Err(e) => return Err(e),
		};
		field_results.push(computed);
	}

	// Consume the input values and materialize the output Objects, moving any
	// `Value::Object` payload out of the input batch instead of cloning it.
	let mut objects: Vec<Object> = Vec::with_capacity(values.len());
	for (v, sr) in values.into_iter().zip(seeded) {
		let o = match sr {
			Some(o) => o,
			None => {
				if let Value::Object(o) = v {
					o
				} else {
					Object::default()
				}
			}
		};
		objects.push(o);
	}

	// Merge each field's computed values into the corresponding row.
	for ((name, _expr), computed_values) in fields.iter().zip(field_results) {
		for (i, computed) in computed_values.into_iter().enumerate() {
			objects[i].insert(name.clone(), computed);
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
			vec![(Strand::new("a"), literal_expr(42)), (Strand::new("b"), literal_expr(100))],
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
