//! Project operator for field selection and transformation.
//!
//! The Project operator selects and transforms fields from input records.
//! It is a pure transformation operator that evaluates expressions and builds
//! output objects. Permissions are handled in Scan.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecutionContext, FlowResult,
	OperatorPlan, PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::{Object, Value};

/// Field selection specification.
#[derive(Debug, Clone)]
pub struct FieldSelection {
	/// The output name for this field
	pub output_name: String,
	/// The expression to evaluate for this field's value
	pub expr: Arc<dyn PhysicalExpr>,
}

/// Project operator - selects and transforms fields from input records.
///
/// This is a pure transformation operator that evaluates expressions and builds
/// output objects. All permission checking occurs in the Scan operator.
#[derive(Debug, Clone)]
pub struct Project {
	/// The input plan to project from
	pub input: Arc<dyn OperatorPlan>,
	/// The fields to select/project
	pub fields: Vec<FieldSelection>,
	/// Fields to omit from output (for SELECT * OMIT)
	pub omit: Vec<Idiom>,
	/// Whether to include all fields from input (for SELECT *, field1, field2)
	pub include_all: bool,
}

#[async_trait]
impl OperatorPlan for Project {
	fn name(&self) -> &'static str {
		"Project"
	}

	fn required_context(&self) -> ContextLevel {
		// Project is a pure transformation operator - inherits child requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with all field expressions
		// This is critical: a projection like `SELECT *, (UPSERT person) FROM person`
		// must return ReadWrite because the subquery mutates!
		let expr_mode = self.fields.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let fields = self.fields.clone();
		let omit = self.omit.clone();
		let include_all = self.include_all;
		let ctx = ctx.clone();

		// Create a stream that projects fields
		let projected = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let omit = omit.clone();
			let ctx = ctx.clone();

			async move {
				use crate::expr::ControlFlow;

				let batch = batch_result?;
				let mut projected_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					// Build evaluation context with current value
					let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&value);

					// Build the projected object
					// If include_all is true, start with the original object's fields
					let mut obj = if include_all {
						match &value {
							Value::Object(original) => original.clone(),
							_ => Object::default(),
						}
					} else {
						Object::default()
					};

					// Add/override with explicit field selections
					for field in &fields {
						// Evaluate the field expression
						let field_value =
							field.expr.evaluate(eval_ctx.clone()).await.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!(
									"Failed to evaluate field expression: {}",
									e
								))
							})?;
						obj.insert(field.output_name.clone(), field_value);
					}

					let mut output_value = Value::Object(obj);

					// Apply omit fields if present
					for field in &omit {
						omit_field_sync(&mut output_value, field);
					}

					projected_values.push(output_value);
				}

				Ok(ValueBatch {
					values: projected_values,
				})
			}
		});

		Ok(Box::pin(projected))
	}
}

/// Synchronously remove a field from a value by idiom path.
fn omit_field_sync(value: &mut Value, idiom: &Idiom) {
	// For simple single-part idioms, directly remove from object
	if idiom.len() == 1 {
		if let Some(Part::Field(field_name)) = idiom.first() {
			if let Value::Object(obj) = value {
				obj.remove(&**field_name);
			}
		}
	} else {
		// For nested paths, traverse and remove
		omit_nested_field(value, idiom, 0);
	}
}

/// Recursively traverse and remove a nested field.
fn omit_nested_field(value: &mut Value, idiom: &Idiom, depth: usize) {
	if depth >= idiom.len() {
		return;
	}

	let Some(part) = idiom.get(depth) else {
		return;
	};

	match part {
		Part::Field(field_name) => {
			if let Value::Object(obj) = value {
				if depth == idiom.len() - 1 {
					// Last part - remove the field
					obj.remove(&**field_name);
				} else {
					// Not last part - recurse into the field
					if let Some(nested) = obj.get_mut(&**field_name) {
						omit_nested_field(nested, idiom, depth + 1);
					}
				}
			}
		}
		Part::All => {
			// Apply to all elements
			match value {
				Value::Object(obj) => {
					for (_, v) in obj.iter_mut() {
						omit_nested_field(v, idiom, depth + 1);
					}
				}
				Value::Array(arr) => {
					for v in arr.iter_mut() {
						omit_nested_field(v, idiom, depth + 1);
					}
				}
				_ => {}
			}
		}
		Part::Value(expr) => {
			// Handle array index access: [0], [1], etc.
			if let crate::expr::Expr::Literal(crate::expr::Literal::Integer(idx)) = expr {
				if let Value::Array(arr) = value {
					if let Some(nested) = arr.get_mut(*idx as usize) {
						if depth == idiom.len() - 1 {
							// Can't "remove" an array element by index, set to None
							*nested = Value::None;
						} else {
							omit_nested_field(nested, idiom, depth + 1);
						}
					}
				}
			}
		}
		_ => {
			// Other part types are not supported for omit
		}
	}
}
