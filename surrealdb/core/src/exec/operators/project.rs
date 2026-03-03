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
use tracing::instrument;

use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::idiom::Idiom;
use crate::expr::part::{DestructurePart, Part};
use crate::val::{Object, Value};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Parse an output name string into a [`FieldPath`].
///
/// Simple dot-separated names (e.g. `"tags.id"`) are split into nested
/// [`FieldPathPart::Field`] components so that the projection can build nested
/// objects.  Names containing `[`, `(`, or spaces are kept as a single field.
fn parse_output_path(name: &str) -> FieldPath {
	if name.contains('.') && !name.contains(['[', '(', ' ']) {
		FieldPath(name.split('.').map(|s| FieldPathPart::Field(s.to_string())).collect())
	} else {
		FieldPath::field(name.to_string())
	}
}

/// Set a value at the given [`FieldPath`] on an [`Object`].
///
/// Internally wraps the object in a `Value::Object` so that
/// `Value::set_at_field_path` can be used, then unwraps the result back.
#[inline]
fn set_field_on_object(obj: &mut Object, path: &FieldPath, value: Value) {
	let mut target = Value::Object(std::mem::take(obj));
	target.set_at_field_path(path, value);
	if let Value::Object(new_obj) = target {
		*obj = new_obj;
	}
}

// ---------------------------------------------------------------------------
// FieldSelection
// ---------------------------------------------------------------------------

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
		Self {
			output_path: parse_output_path(&output_name),
			expr,
			has_explicit_alias: false,
		}
	}

	/// Create a new field selection with an explicit alias.
	/// Used when the user specified an alias in the query (e.g., `SELECT expr AS alias`).
	/// For projection functions, the alias takes precedence over dynamic field names.
	pub fn with_alias(output_name: String, expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			output_path: parse_output_path(&output_name),
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
}

// ---------------------------------------------------------------------------
// Project operator
// ---------------------------------------------------------------------------

/// Project operator - selects and transforms fields from input records.
///
/// This is a pure transformation operator that evaluates expressions and builds
/// output objects. All permission checking occurs in the Scan operator.
#[derive(Debug, Clone)]
pub struct Project {
	/// The input plan to project from
	pub input: Arc<dyn ExecOperator>,
	/// The fields to select/project (shared across batches via Arc)
	pub fields: Arc<[FieldSelection]>,
	/// Fields to omit from output (for SELECT * OMIT) (shared across batches via Arc)
	pub omit: Arc<[Idiom]>,
	/// Whether to include all fields from input (for SELECT *, field1, field2)
	pub include_all: bool,
	/// Per-operator metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Project {
	/// Create a new Project operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields: Vec<FieldSelection>,
		omit: Vec<Idiom>,
		include_all: bool,
	) -> Self {
		Self {
			input,
			fields: fields.into(),
			omit: omit.into(),
			include_all,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Project {
	fn name(&self) -> &'static str {
		"Project"
	}

	fn required_context(&self) -> ContextLevel {
		// Combine field expression contexts with child operator context.
		// When include_all is true, we additionally need database access
		// to dereference RecordIds.
		let fields_ctx = self
			.fields
			.iter()
			.map(|f| f.expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		let base = self.input.required_context().max(fields_ctx);
		if self.include_all {
			base.max(ContextLevel::Database)
		} else {
			base
		}
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's mode with all field expressions
		// This is critical: a projection like `SELECT *, (UPSERT person) FROM person`
		// must return ReadWrite because the subquery mutates!
		let expr_mode = self.fields.iter().map(|f| f.expr.access_mode()).combine_all();
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
		self.fields.iter().map(|f| ("field", &f.expr)).collect()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	#[instrument(level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let fields = Arc::clone(&self.fields);
		let omit = Arc::clone(&self.omit);
		let include_all = self.include_all;
		let ctx = ctx.clone();

		// Create a stream that projects fields
		let projected = input_stream.then(move |batch_result| {
			let fields = Arc::clone(&fields);
			let omit = Arc::clone(&omit);
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let eval_ctx = EvalContext::from_exec_ctx(&ctx);

				let projected_values = if include_all {
					// --- include_all path: per-row processing ---
					// RecordId dereferencing and row-skipping requires per-row handling.
					let mut values = Vec::with_capacity(batch.values.len());
					for value in batch.values {
						let row_ctx = eval_ctx.with_value_and_doc(&value);

						let mut output_value = match &value {
							Value::Object(original) => {
								let mut obj = original.clone();
								for field in fields.iter() {
									evaluate_and_set_field(&mut obj, field, row_ctx.clone())
										.await?;
								}
								Value::Object(obj)
							}
							Value::RecordId(rid) => {
								let fetched =
									super::fetch::fetch_record(eval_ctx.exec_ctx, rid).await?;
								match fetched {
									Value::Object(mut obj) => {
										for field in fields.iter() {
											evaluate_and_set_field(
												&mut obj,
												field,
												row_ctx.clone(),
											)
											.await?;
										}
										Value::Object(obj)
									}
									Value::None => {
										// Record doesn't exist - skip this row.
										continue;
									}
									other => other,
								}
							}
							other => {
								if fields.is_empty() {
									other.clone()
								} else {
									let mut obj = Object::default();
									for field in fields.iter() {
										evaluate_and_set_field(&mut obj, field, row_ctx.clone())
											.await?;
									}
									Value::Object(obj)
								}
							}
						};

						for field in omit.iter() {
							omit_field_sync(&mut output_value, field);
						}
						values.push(output_value);
					}
					values
				} else {
					// --- Batch per-field evaluation for non-include_all ---
					// Evaluate each field expression across all rows in one batch call,
					// then assemble per-row objects from the results.
					let batch_len = batch.values.len();
					let mut objects: Vec<Object> =
						(0..batch_len).map(|_| Object::default()).collect();

					for field in fields.iter() {
						if field.expr.is_projection_function() {
							// Projection functions return multiple field bindings;
							// handle per-row since they need special object assembly.
							for (i, value) in batch.values.iter().enumerate() {
								evaluate_and_set_field(
									&mut objects[i],
									field,
									eval_ctx.with_value_and_doc(value),
								)
								.await?;
							}
						} else {
							// Batch evaluation: use evaluate_batch which allows
							// I/O-bound expressions (subqueries, lookups) to
							// parallelize. For simple field accesses, the default
							// sequential implementation is used.
							let field_values =
								field.expr.evaluate_batch(eval_ctx.clone(), &batch.values).await?;
							for (i, field_value) in field_values.into_iter().enumerate() {
								set_field_on_object(
									&mut objects[i],
									&field.output_path,
									field_value,
								);
							}
						}
					}

					let mut values: Vec<Value> = objects.into_iter().map(Value::Object).collect();
					if !omit.is_empty() {
						for val in &mut values {
							for field in omit.iter() {
								omit_field_sync(val, field);
							}
						}
					}
					values
				};

				Ok(ValueBatch {
					values: projected_values,
				})
			}
		});

		Ok(monitor_stream(Box::pin(projected), "Project", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// evaluate_and_set_field
// ---------------------------------------------------------------------------

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
	if field.expr.is_projection_function()
		&& let Some(bindings) = field.expr.evaluate_projection(eval_ctx.clone()).await?
	{
		if field.has_explicit_alias {
			// User provided an alias - use it as the field name.
			// For multiple bindings, collect values into an array.
			let value = if bindings.len() == 1 {
				bindings.into_iter().next().expect("bindings verified non-empty").1
			} else {
				Value::Array(bindings.into_iter().map(|(_, v)| v).collect::<Vec<_>>().into())
			};
			set_field_on_object(obj, &field.output_path, value);
		} else {
			// No alias - use the dynamic field names from the function
			for (idiom, value) in bindings {
				if let Ok(path) = FieldPath::try_from(&idiom)
					&& !path.is_empty()
				{
					set_field_on_object(obj, &path, value);
				}
			}
		}
		return Ok(());
		// Fall through to regular evaluation if not actually a projection function
	}

	let field_value = field.expr.evaluate(eval_ctx).await?;
	set_field_on_object(obj, &field.output_path, field_value);
	Ok(())
}

// ---------------------------------------------------------------------------
// OMIT helpers
// ---------------------------------------------------------------------------

/// Synchronously remove a field from a value by idiom path.
#[inline]
fn omit_field_sync(value: &mut Value, idiom: &Idiom) {
	// For simple single-part idioms, directly remove from object
	if idiom.len() == 1 {
		if let Some(Part::Field(field_name)) = idiom.first()
			&& let Value::Object(obj) = value
		{
			obj.remove(&**field_name);
		}
	} else {
		// For nested paths, traverse and remove
		omit_nested_field(value, idiom, 0);
	}
}

/// Recursively traverse and remove a nested field.
fn omit_nested_field(value: &mut Value, idiom: &Idiom, depth: usize) {
	let Some(part) = idiom.get(depth) else {
		return;
	};
	let is_last = depth == idiom.len() - 1;

	match part {
		Part::Field(field_name) => {
			if let Value::Object(obj) = value {
				if is_last {
					obj.remove(&**field_name);
				} else if let Some(nested) = obj.get_mut(&**field_name) {
					omit_nested_field(nested, idiom, depth + 1);
				}
			}
		}
		Part::All => {
			if is_last {
				match value {
					Value::Object(obj) => obj.clear(),
					Value::Array(arr) => arr.clear(),
					_ => {}
				}
			} else {
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
			if let crate::expr::Expr::Literal(crate::expr::Literal::Integer(idx)) = expr
				&& let Value::Array(arr) = value
				&& let Some(nested) = arr.get_mut(*idx as usize)
			{
				if is_last {
					// Can't "remove" an array element by index, set to None
					*nested = Value::None;
				} else {
					omit_nested_field(nested, idiom, depth + 1);
				}
			}
		}
		Part::Destructure(destructure_parts) => {
			// Destructure in OMIT: remove the listed fields from the current object.
			// E.g., OMIT obj.c.{ d, f } removes d and f from obj.c.
			if let Value::Object(obj) = value {
				omit_destructure_fields(obj, destructure_parts);
			}
		}
		_ => {
			// Other part types are not supported for omit
		}
	}
}

/// Recursively remove fields described by destructure parts from an object.
fn omit_destructure_fields(obj: &mut Object, parts: &[DestructurePart]) {
	for dp in parts {
		match dp {
			DestructurePart::Field(name) | DestructurePart::All(name) => {
				obj.remove(name.as_str());
			}
			DestructurePart::Destructure(name, nested) => {
				if let Some(Value::Object(inner)) = obj.get_mut(name.as_str()) {
					omit_destructure_fields(inner, nested);
				}
			}
			DestructurePart::Aliased(name, _) => {
				obj.remove(name.as_str());
			}
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
	/// The projections to apply (shared across batches via Arc)
	pub projections: Arc<[Projection]>,
	/// Per-operator metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SelectProject {
	/// Create a new SelectProject operator.
	pub fn new(
		input: Arc<dyn ExecOperator>,
		projections: Vec<Projection>,
		metrics: Arc<OperatorMetrics>,
	) -> Self {
		Self {
			input,
			projections: projections.into(),
			metrics,
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	#[instrument(level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let projections = Arc::clone(&self.projections);
		let ctx = ctx.clone();

		// Create a stream that applies projections
		let projected = input_stream.then(move |batch_result| {
			let projections = Arc::clone(&projections);
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut projected_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let projected = apply_projections(&value, &projections, &ctx).await?;
					// Skip NONE values from non-existent records (e.g. mock ranges)
					if matches!(&value, Value::RecordId(_)) && matches!(&projected, Value::None) {
						continue;
					}
					projected_values.push(projected);
				}

				Ok(ValueBatch {
					values: projected_values,
				})
			}
		});

		Ok(monitor_stream(Box::pin(projected), "SelectProject", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// SelectProject helpers
// ---------------------------------------------------------------------------

/// Apply projections to a single value.
async fn apply_projections(
	value: &Value,
	projections: &[Projection],
	ctx: &ExecutionContext,
) -> Result<Value, crate::expr::ControlFlow> {
	let has_all = projections.iter().any(|p| matches!(p, Projection::All));
	let has_includes =
		projections.iter().any(|p| matches!(p, Projection::Include(_) | Projection::Rename { .. }));

	// Get the input object, dereferencing RecordId when needed.
	// RecordIds must be dereferenced for SELECT * (all fields) and for
	// specific field selections (e.g. SELECT id FROM [person:1]).
	let input_obj = match value {
		Value::Object(obj) => obj.clone(),
		Value::RecordId(rid) if has_all || has_includes => {
			match super::fetch::fetch_record(ctx, rid).await? {
				Value::Object(obj) => obj,
				Value::None => return Ok(Value::None),
				other => return Ok(other),
			}
		}
		// Geometry values expose GeoJSON fields (type, coordinates, etc.)
		Value::Geometry(geo) if has_all || has_includes => geo.as_object(),
		_ => return Ok(value.clone()),
	};

	Ok(apply_projections_to_object(&input_obj, projections, has_all))
}

/// Apply projections to an already-resolved object (sync version).
/// This is the core projection logic used by both the async apply_projections
/// and by tests.
fn apply_projections_to_object(
	input_obj: &Object,
	projections: &[Projection],
	has_all: bool,
) -> Value {
	// Build output object
	let mut output = if has_all {
		input_obj.clone()
	} else {
		Object::default()
	};

	// Apply includes and renames.
	// Fields that don't exist on the input default to Value::None,
	// matching SQL projection semantics (SELECT v FROM t always
	// produces a `v` column, even when the record lacks it).
	for projection in projections {
		match projection {
			Projection::Include(name) => {
				let v = input_obj.get(name).cloned().unwrap_or(Value::None);
				output.insert(name.clone(), v);
			}
			Projection::Rename {
				from,
				to,
			} => {
				let v = input_obj.get(from).cloned().unwrap_or(Value::None);
				output.insert(to.clone(), v);
				// If we had All, remove the original name to avoid
				// having both `from` and `to` in output.
				if has_all {
					output.remove(from);
				}
			}
			Projection::Omit(name) => {
				output.remove(name.as_str());
			}
			Projection::All => {
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

		let result = apply_projections_to_object(&obj, &projections, false);
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

		let result = apply_projections_to_object(&obj, &projections, false);
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

		let result = apply_projections_to_object(&obj, &projections, true);
		if let Value::Object(result_obj) = result {
			assert!(result_obj.contains_key("a"));
			assert!(!result_obj.contains_key("b"));
			assert!(result_obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}
}
