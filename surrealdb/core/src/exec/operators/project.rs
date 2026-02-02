//! Project operator for field selection and transformation.
//!
//! This module provides two operators:
//!
//! - [`Project`]: The original operator that evaluates expressions for field values
//! - [`SelectProject`]: A simplified operator that only does field selection/renaming
//!
//! The `SelectProject` operator is designed for the consolidated expression evaluation
//! approach, where complex expressions are pre-computed by a `Compute` operator and
//! Project only needs to select/rename fields for the final output.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::{Object, Value};

/// Field selection specification.
#[derive(Debug, Clone)]
pub struct FieldSelection {
	/// The output path for this field (e.g., ["tags", "id"] for "tags.id")
	/// This allows proper nested object construction.
	pub output_path: Vec<String>,
	/// The expression to evaluate for this field's value
	pub expr: Arc<dyn PhysicalExpr>,
}

impl FieldSelection {
	/// Create a new field selection from an output name string.
	/// If the name contains dots and represents a simple field path (no special characters),
	/// it will be split into path components for nested object construction.
	pub fn new(output_name: String, expr: Arc<dyn PhysicalExpr>) -> Self {
		// Parse the output name into path components
		// For simple dot-separated paths like "tags.id", split into ["tags", "id"]
		// For complex names with special chars, keep as single element
		let output_path = if output_name.contains('.') && !output_name.contains(['[', '(', ' ']) {
			output_name.split('.').map(|s| s.to_string()).collect()
		} else {
			vec![output_name]
		};
		Self {
			output_path,
			expr,
		}
	}

	/// Get the display name for this field (for debugging/attrs).
	pub fn display_name(&self) -> String {
		self.output_path.join(".")
	}
}

/// Project operator - selects and transforms fields from input records.
///
/// This is a pure transformation operator that evaluates expressions and builds
/// output objects. All permission checking occurs in the Scan operator.
#[derive(Debug, Clone)]
pub struct Project {
	/// The input plan to project from
	pub input: Arc<dyn ExecOperator>,
	/// The fields to select/project
	pub fields: Vec<FieldSelection>,
	/// Fields to omit from output (for SELECT * OMIT)
	pub omit: Vec<Idiom>,
	/// Whether to include all fields from input (for SELECT *, field1, field2)
	pub include_all: bool,
}

#[async_trait]
impl ExecOperator for Project {
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

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
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
						// Use nested setting to properly construct nested objects
						// e.g., path ["tags", "id"] creates { tags: { id: value } }
						set_nested_value(&mut obj, &field.output_path, field_value);
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

/// Set a value at a nested path in an object.
///
/// For a path like ["tags", "id"], this creates or updates:
/// `{ tags: { id: value } }`
///
/// If intermediate paths already exist as objects, they are updated.
/// If they exist as non-objects, they are replaced with objects.
fn set_nested_value(obj: &mut Object, path: &[String], value: Value) {
	if path.is_empty() {
		return;
	}

	if path.len() == 1 {
		// Simple case: just insert at this level
		obj.insert(path[0].clone(), value);
		return;
	}

	// Need to traverse/create nested structure
	let key = &path[0];
	let rest = &path[1..];

	// Get or create the nested object
	let nested = obj.entry(key.clone()).or_insert_with(|| Value::Object(Object::default()));

	match nested {
		Value::Object(nested_obj) => {
			set_nested_value(nested_obj, rest, value);
		}
		_ => {
			// Replace non-object with new object containing the nested path
			let mut new_obj = Object::default();
			set_nested_value(&mut new_obj, rest, value);
			*nested = Value::Object(new_obj);
		}
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
			if depth == idiom.len() - 1 {
				// Last part - remove all fields from current object
				match value {
					Value::Object(obj) => {
						obj.clear();
					}
					Value::Array(arr) => {
						arr.clear();
					}
					_ => {}
				}
			} else {
				// Not last part - recurse into all elements
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

// ============================================================================
// SelectProject - Simplified projection without expression evaluation
// ============================================================================

/// Specifies how to handle a field in SelectProject.
#[derive(Debug, Clone)]
pub enum Projection {
	/// Include a field with its original name
	Include(String),
	/// Rename a field (from internal name to output name)
	Rename {
		from: String,
		to: String,
	},
	/// Include all fields from input (SELECT *)
	All,
	/// Exclude a field (for OMIT)
	Omit(String),
}

/// Simplified project operator that only does field selection and renaming.
///
/// This is designed for the consolidated expression evaluation approach:
/// 1. Complex expressions are pre-computed by a `Compute` operator
/// 2. `SelectProject` only needs to select/rename fields for output
/// 3. No expression evaluation occurs in this operator
///
/// Benefits:
/// - Clearer separation of concerns (Compute evaluates, SelectProject shapes)
/// - Simpler and faster execution (just field manipulation)
/// - Easier to reason about when expressions are evaluated
#[derive(Debug, Clone)]
pub struct SelectProject {
	/// The input plan to project from
	pub input: Arc<dyn ExecOperator>,
	/// The projections to apply
	pub projections: Vec<Projection>,
}

impl SelectProject {
	/// Create a new SelectProject operator.
	pub fn new(input: Arc<dyn ExecOperator>, projections: Vec<Projection>) -> Self {
		Self {
			input,
			projections,
		}
	}

	/// Create a SelectProject that includes all fields.
	pub fn all(input: Arc<dyn ExecOperator>) -> Self {
		Self {
			input,
			projections: vec![Projection::All],
		}
	}

	/// Create a SelectProject with specific field includes.
	pub fn include_fields(input: Arc<dyn ExecOperator>, fields: Vec<String>) -> Self {
		Self {
			input,
			projections: fields.into_iter().map(Projection::Include).collect(),
		}
	}
}

#[async_trait]
impl ExecOperator for SelectProject {
	fn name(&self) -> &'static str {
		"SelectProject"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let proj_str = self
			.projections
			.iter()
			.map(|p| match p {
				Projection::Include(name) => name.clone(),
				Projection::Rename {
					from,
					to,
				} => format!("{} AS {}", from, to),
				Projection::All => "*".to_string(),
				Projection::Omit(name) => format!("OMIT {}", name),
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("projections".to_string(), proj_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// SelectProject doesn't evaluate expressions - inherits child requirements
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// SelectProject is pure field manipulation - inherits input's access mode
		self.input.access_mode()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let projections = self.projections.clone();

		// Create a stream that applies projections
		let projected = input_stream.map(move |batch_result| {
			let projections = projections.clone();

			let batch = batch_result?;
			let mut projected_values = Vec::with_capacity(batch.values.len());

			for value in batch.values {
				let projected = apply_projections(&value, &projections);
				projected_values.push(projected);
			}

			Ok(ValueBatch {
				values: projected_values,
			})
		});

		Ok(Box::pin(projected))
	}
}

/// Apply projections to a single value.
fn apply_projections(value: &Value, projections: &[Projection]) -> Value {
	let input_obj = match value {
		Value::Object(obj) => obj,
		_ => return value.clone(),
	};

	// Determine if we have an All projection
	let has_all = projections.iter().any(|p| matches!(p, Projection::All));

	// Collect fields to omit
	let omit_fields: Vec<&str> = projections
		.iter()
		.filter_map(|p| match p {
			Projection::Omit(name) => Some(name.as_str()),
			_ => None,
		})
		.collect();

	// Build output object
	let mut output = if has_all {
		// Start with all fields from input
		let mut obj = input_obj.clone();
		// Apply omits
		for field in &omit_fields {
			obj.remove(*field);
		}
		obj
	} else {
		Object::default()
	};

	// Apply includes and renames
	for projection in projections {
		match projection {
			Projection::Include(name) => {
				if let Some(v) = input_obj.get(name) {
					output.insert(name.clone(), v.clone());
				}
			}
			Projection::Rename {
				from,
				to,
			} => {
				if let Some(v) = input_obj.get(from) {
					output.insert(to.clone(), v.clone());
					// If we had All, we need to remove the original name
					// to avoid having both `from` and `to` in output
					if has_all {
						output.remove(from);
					}
				}
			}
			Projection::All | Projection::Omit(_) => {
				// Already handled above
			}
		}
	}

	Value::Object(output)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_apply_projections_include() {
		let obj = Object::from(vec![
			("a".to_string(), Value::from(1)),
			("b".to_string(), Value::from(2)),
			("c".to_string(), Value::from(3)),
		]);
		let value = Value::Object(obj);

		let projections =
			vec![Projection::Include("a".to_string()), Projection::Include("c".to_string())];

		let result = apply_projections(&value, &projections);
		if let Value::Object(obj) = result {
			assert!(obj.contains_key("a"));
			assert!(!obj.contains_key("b"));
			assert!(obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}

	#[test]
	fn test_apply_projections_rename() {
		let obj = Object::from(vec![("old_name".to_string(), Value::from(42))]);
		let value = Value::Object(obj);

		let projections = vec![Projection::Rename {
			from: "old_name".to_string(),
			to: "new_name".to_string(),
		}];

		let result = apply_projections(&value, &projections);
		if let Value::Object(obj) = result {
			assert!(!obj.contains_key("old_name"));
			assert!(obj.contains_key("new_name"));
			assert_eq!(obj.get("new_name"), Some(&Value::from(42)));
		} else {
			panic!("Expected Object");
		}
	}

	#[test]
	fn test_apply_projections_all_with_omit() {
		let obj = Object::from(vec![
			("a".to_string(), Value::from(1)),
			("b".to_string(), Value::from(2)),
			("c".to_string(), Value::from(3)),
		]);
		let value = Value::Object(obj);

		let projections = vec![Projection::All, Projection::Omit("b".to_string())];

		let result = apply_projections(&value, &projections);
		if let Value::Object(obj) = result {
			assert!(obj.contains_key("a"));
			assert!(!obj.contains_key("b"));
			assert!(obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}
}
