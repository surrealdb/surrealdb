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

use super::fetch::fetch_record;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::val::{Object, RecordId, Value};

/// Field selection specification.
#[derive(Debug, Clone)]
pub struct FieldSelection {
	/// The output path for this field - determines where the value goes in the result.
	/// Uses FieldPath for proper nested object construction and array iteration.
	pub output_path: FieldPath,
	/// The expression to evaluate for this field's value
	pub expr: Arc<dyn PhysicalExpr>,
	/// Whether the output_path came from an explicit alias.
	/// When true, projection functions should use output_path instead of dynamic field names.
	pub has_explicit_alias: bool,
}

impl FieldSelection {
	/// Create a new field selection from an output name string.
	/// If the name contains dots and represents a simple field path (no special characters),
	/// it will be split into path components for nested object construction.
	pub fn new(output_name: String, expr: Arc<dyn PhysicalExpr>) -> Self {
		// Parse the output name into path components
		// For simple dot-separated paths like "tags.id", split into Field parts
		// For complex names with special chars, keep as single Field
		let output_path = if output_name.contains('.') && !output_name.contains(['[', '(', ' ']) {
			FieldPath(output_name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect())
		} else {
			FieldPath::field(output_name)
		};
		Self {
			output_path,
			expr,
			has_explicit_alias: false,
		}
	}

	/// Create a new field selection with an explicit alias.
	/// Used when the user specified an alias in the query (e.g., `SELECT expr AS alias`).
	/// For projection functions, the alias takes precedence over dynamic field names.
	pub fn with_alias(output_name: String, expr: Arc<dyn PhysicalExpr>) -> Self {
		let output_path = if output_name.contains('.') && !output_name.contains(['[', '(', ' ']) {
			FieldPath(output_name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect())
		} else {
			FieldPath::field(output_name)
		};
		Self {
			output_path,
			expr,
			has_explicit_alias: true,
		}
	}

	/// Create a new field selection from a FieldPath directly.
	/// Used for graph traversals without aliases where the path represents nested structure.
	pub fn from_field_path(output_path: FieldPath, expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			output_path,
			expr,
			has_explicit_alias: false,
		}
	}

	/// Get the display name for this field (for debugging/attrs).
	pub fn display_name(&self) -> String {
		self.output_path.to_string()
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
		// When include_all is true, we may need to dereference RecordIds,
		// which requires database access
		if self.include_all {
			ContextLevel::Database.max(self.input.required_context())
		} else {
			self.input.required_context()
		}
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
					// If the value is a RecordId, dereference it to get the full record
					// If the value is a scalar (not Object/RecordId), pass through as-is
					let mut output_value = if include_all {
						match &value {
							Value::Object(original) => {
								let mut obj = original.clone();
								// Add/override with explicit field selections
								for field in &fields {
									evaluate_and_set_field(&mut obj, field, eval_ctx.clone())
										.await?;
								}
								Value::Object(obj)
							}
							Value::RecordId(rid) => {
								// Dereference RecordId to full record
								let fetched = fetch_record(&ctx, rid).await?;
								match fetched {
									Value::Object(mut obj) => {
										// Add/override with explicit field selections
										for field in &fields {
											evaluate_and_set_field(
												&mut obj,
												field,
												eval_ctx.clone(),
											)
											.await?;
										}
										Value::Object(obj)
									}
									other => other, // Pass through None or other values
								}
							}
							// For scalars (integers, strings, etc.), pass through as-is
							// unless there are explicit fields to add
							other => {
								if fields.is_empty() {
									other.clone()
								} else {
									// If there are explicit fields, we need to create an object
									let mut obj = Object::default();
									for field in &fields {
										evaluate_and_set_field(&mut obj, field, eval_ctx.clone())
											.await?;
									}
									Value::Object(obj)
								}
							}
						}
					} else {
						// Not include_all - build object from explicit fields only
						let mut obj = Object::default();
						for field in &fields {
							evaluate_and_set_field(&mut obj, field, eval_ctx.clone()).await?;
						}
						Value::Object(obj)
					};

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

/// Evaluate a field expression and set the resulting value(s) on the output object.
///
/// For regular expressions, evaluates the expression and sets the result at the output_path.
/// For projection functions:
/// - If has_explicit_alias is true, use the alias (output_path) as the field name
/// - Otherwise, use the dynamic field names from the function result
async fn evaluate_and_set_field(
	obj: &mut Object,
	field: &FieldSelection,
	eval_ctx: EvalContext<'_>,
) -> Result<(), crate::expr::ControlFlow> {
	// Check if this is a projection function
	if field.expr.is_projection_function() {
		// Evaluate as projection function to get field bindings
		match field.expr.evaluate_projection(eval_ctx.clone()).await {
			Ok(Some(bindings)) => {
				if field.has_explicit_alias {
					// User provided an alias - use it as the field name
					// For multiple bindings, collect values into an array
					let value = if bindings.len() == 1 {
						bindings.into_iter().next().unwrap().1
					} else {
						// Multiple bindings with alias - collect as array
						Value::Array(
							bindings.into_iter().map(|(_, v)| v).collect::<Vec<_>>().into(),
						)
					};
					let mut target = Value::Object(std::mem::take(obj));
					target.set_at_field_path(&field.output_path, value);
					if let Value::Object(new_obj) = target {
						*obj = new_obj;
					}
				} else {
					// No alias - use the dynamic field names from the function
					for (idiom, value) in bindings {
						// Convert idiom to FieldPath
						if let Ok(path) = FieldPath::try_from(&idiom) {
							if !path.is_empty() {
								let mut target = Value::Object(std::mem::take(obj));
								target.set_at_field_path(&path, value);
								if let Value::Object(new_obj) = target {
									*obj = new_obj;
								}
							}
						}
					}
				}
				Ok(())
			}
			Ok(None) => {
				// Not actually a projection function (shouldn't happen), fall back to regular eval
				let field_value = field.expr.evaluate(eval_ctx).await.map_err(|e| {
					crate::expr::ControlFlow::Err(anyhow::anyhow!(
						"Failed to evaluate field expression: {}",
						e
					))
				})?;
				let mut target = Value::Object(std::mem::take(obj));
				target.set_at_field_path(&field.output_path, field_value);
				if let Value::Object(new_obj) = target {
					*obj = new_obj;
				}
				Ok(())
			}
			Err(e) => Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
				"Failed to evaluate projection function: {}",
				e
			))),
		}
	} else {
		// Regular expression - evaluate and set at output_path
		let field_value = field.expr.evaluate(eval_ctx).await.map_err(|e| {
			crate::expr::ControlFlow::Err(anyhow::anyhow!(
				"Failed to evaluate field expression: {}",
				e
			))
		})?;
		let mut target = Value::Object(std::mem::take(obj));
		target.set_at_field_path(&field.output_path, field_value);
		if let Value::Object(new_obj) = target {
			*obj = new_obj;
		}
		Ok(())
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
		// When projections include All, we may need to dereference RecordIds,
		// which requires database access
		let has_all = self.projections.iter().any(|p| matches!(p, Projection::All));
		if has_all {
			ContextLevel::Database.max(self.input.required_context())
		} else {
			self.input.required_context()
		}
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
		let ctx = ctx.clone();

		// Create a stream that applies projections
		let projected = input_stream.then(move |batch_result| {
			let projections = projections.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut projected_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let projected = apply_projections(&value, &projections, &ctx).await?;
					projected_values.push(projected);
				}

				Ok(ValueBatch {
					values: projected_values,
				})
			}
		});

		Ok(Box::pin(projected))
	}
}

/// Apply projections to a single value.
async fn apply_projections(
	value: &Value,
	projections: &[Projection],
	ctx: &ExecutionContext,
) -> Result<Value, crate::expr::ControlFlow> {
	// Determine if we have an All projection
	let has_all = projections.iter().any(|p| matches!(p, Projection::All));

	// Get the input object, dereferencing RecordId if needed for SELECT *
	let input_obj = match value {
		Value::Object(obj) => obj.clone(),
		Value::RecordId(rid) if has_all => {
			// Dereference RecordId to full record for SELECT *
			match fetch_record(ctx, rid).await? {
				Value::Object(obj) => obj,
				Value::None => return Ok(Value::None),
				other => return Ok(other),
			}
		}
		_ => return Ok(value.clone()),
	};

	Ok(apply_projections_to_object(&input_obj, projections))
}

/// Apply projections to an already-resolved object (sync version).
/// This is the core projection logic used by both the async apply_projections
/// and by tests.
fn apply_projections_to_object(input_obj: &Object, projections: &[Projection]) -> Value {
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

		let projections =
			vec![Projection::Include("a".to_string()), Projection::Include("c".to_string())];

		let result = apply_projections_to_object(&obj, &projections);
		if let Value::Object(result_obj) = result {
			assert!(result_obj.contains_key("a"));
			assert!(!result_obj.contains_key("b"));
			assert!(result_obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}

	#[test]
	fn test_apply_projections_rename() {
		let obj = Object::from(vec![("old_name".to_string(), Value::from(42))]);

		let projections = vec![Projection::Rename {
			from: "old_name".to_string(),
			to: "new_name".to_string(),
		}];

		let result = apply_projections_to_object(&obj, &projections);
		if let Value::Object(result_obj) = result {
			assert!(!result_obj.contains_key("old_name"));
			assert!(result_obj.contains_key("new_name"));
			assert_eq!(result_obj.get("new_name"), Some(&Value::from(42)));
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

		let projections = vec![Projection::All, Projection::Omit("b".to_string())];

		let result = apply_projections_to_object(&obj, &projections);
		if let Value::Object(result_obj) = result {
			assert!(result_obj.contains_key("a"));
			assert!(!result_obj.contains_key("b"));
			assert!(result_obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}
}
