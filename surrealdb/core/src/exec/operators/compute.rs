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
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, PhysicalExpr, ValueBatch, ValueBatchStream,
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
}

#[async_trait]
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
		// Compute needs Database for expression evaluation, but also
		// inherits child requirements (take the maximum)
		let expr_ctx = self
			.fields
			.iter()
			.map(|(_, expr)| expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		ContextLevel::Database.max(self.input.required_context()).max(expr_ctx)
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with all expression access modes
		// An expression could contain a mutation subquery!
		let expr_mode = self.fields.iter().map(|(_, expr)| expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// If there are no fields to compute, just pass through
		if self.fields.is_empty() {
			return self.input.execute(ctx);
		}

		let input_stream = self.input.execute(ctx)?;
		let fields = self.fields.clone();
		let ctx = ctx.clone();

		// Create a stream that computes fields for each batch
		let computed = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut computed_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let computed_value = compute_fields_for_value(&value, &fields, &ctx).await?;
					computed_values.push(computed_value);
				}

				Ok(ValueBatch {
					values: computed_values,
				})
			}
		});

		Ok(Box::pin(computed))
	}
}

/// Compute all fields for a single value and return a new value with fields added.
async fn compute_fields_for_value(
	value: &Value,
	fields: &[(String, Arc<dyn PhysicalExpr>)],
	ctx: &ExecutionContext,
) -> Result<Value, ControlFlow> {
	let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(value);

	// Start with the original value's fields
	let mut obj = match value {
		Value::Object(o) => o.clone(),
		_ => Object::default(),
	};

	// Compute each expression and add to object
	for (name, expr) in fields {
		let computed = match expr.evaluate(eval_ctx.clone()).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(e) => return Err(e),
		};
		obj.insert(name.clone(), computed);
	}

	Ok(Value::Object(obj))
}

impl Compute {
	/// Create a new Compute operator.
	pub fn new(input: Arc<dyn ExecOperator>, fields: Vec<(String, Arc<dyn PhysicalExpr>)>) -> Self {
		Self {
			input,
			fields,
		}
	}
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

		let source = Arc::new(SourceExpr {
			expr: literal_expr(1),
		});

		let compute = Compute::new(
			source,
			vec![("a".to_string(), literal_expr(42)), ("b".to_string(), literal_expr(100))],
		);

		assert_eq!(compute.name(), "Compute");
		assert_eq!(compute.len(), 2);
		assert!(!compute.is_empty());

		let attrs = compute.attrs();
		assert_eq!(attrs.len(), 1);
		assert!(attrs[0].1.contains("a = 42"));
		assert!(attrs[0].1.contains("b = 100"));
	}

	#[test]
	fn test_compute_empty() {
		use crate::exec::operators::SourceExpr;

		let source = Arc::new(SourceExpr {
			expr: literal_expr(1),
		});

		let compute = Compute::new(source, vec![]);

		assert!(compute.is_empty());
		assert_eq!(compute.len(), 0);
	}
}
