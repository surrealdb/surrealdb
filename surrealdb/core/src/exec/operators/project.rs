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

use futures::StreamExt;

use crate::exec::field_path::FieldPath;
use crate::exec::operators::OmitPath;
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::val::{Object, Strand, Value};

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Wrap an output name string into a single-part [`FieldPath`].
///
/// The name is treated as an opaque flat key. Callers that need a nested
/// output path must supply a structurally multi-part [`FieldPath`] via
/// [`FieldSelection::from_field_path`] or [`FieldSelection::with_alias_path`],
/// built from the alias or from `LogicalExpr::output_path` of the field, which
/// preserves the distinction between `AS foo.bar` (nested) and
/// `` AS `foo.bar` `` (flat, a single key whose identifier contains a dot).
fn parse_output_path(name: &str) -> FieldPath {
	FieldPath::field(name.to_string())
}

/// Set a value at the given [`FieldPath`] on an [`Object`], consuming `value`.
///
/// Internally wraps the object in a `Value::Object` so that
/// `Value::set_at_field_path_owned` can be used, then unwraps the result back.
/// Taking `value` by value avoids the per-leaf clone that the borrowed form
/// pays inside the recursive path walk.
#[inline]
fn set_field_on_object(obj: &mut Object, path: &FieldPath, value: Value) {
	let mut target = Value::Object(std::mem::take(obj));
	target.set_at_field_path_owned(path, value);
	if let Value::Object(new_obj) = target {
		*obj = new_obj;
	}
}

/// Drain `writes` into `obj` and return it as a `Value::Object`.
///
/// The drain leaves `writes` empty (capacity retained) so the caller can reuse
/// it across rows without re-allocating.
#[inline]
fn apply_field_writes(mut obj: Object, writes: &mut Vec<(FieldPath, Value)>) -> Value {
	for (path, v) in writes.drain(..) {
		set_field_on_object(&mut obj, &path, v);
	}
	Value::Object(obj)
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
	///
	/// The name is treated as an opaque flat key (a single-part
	/// [`FieldPath`]). Callers that need a nested output path must supply
	/// a structurally multi-part [`FieldPath`] via
	/// [`FieldSelection::from_field_path`] or
	/// [`FieldSelection::with_alias_path`], which preserves the distinction
	/// between `AS foo.bar` (nested) and `` AS `foo.bar` `` (flat).
	pub fn new(output_name: &str, expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			output_path: parse_output_path(output_name),
			expr,
			has_explicit_alias: false,
		}
	}

	/// Create a new field selection with an explicit alias whose output path
	/// was built from the alias's output path.
	///
	/// Used when the user specified an alias in the query
	/// (e.g., `SELECT expr AS alias`). Preserves the single-part vs
	/// multi-part distinction (`` AS `foo.bar` `` vs `AS foo.bar`) without
	/// round-tripping through a string. For projection functions, the
	/// alias takes precedence over dynamic field names.
	pub fn with_alias_path(output_path: FieldPath, expr: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			output_path,
			expr,
			has_explicit_alias: true,
		}
	}

	/// Create a new field selection from a [`FieldPath`] directly: the path an
	/// unaliased expression names itself by (`LogicalExpr::output_path`),
	/// where a multi-part path lands the value in a nested object.
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
	/// Paths to omit from output (for SELECT * OMIT) (shared across batches via Arc)
	pub omit: Arc<[OmitPath]>,
	/// Whether to include all fields from input (for SELECT *, field1, field2)
	pub include_all: bool,
	/// Whether a batch may be evaluated one field at a time — each field for
	/// every row before the next — rather than a row at a time with the
	/// fields in written order.
	///
	/// It decides how a batch is evaluated, never what the answer is. `true`
	/// licenses the faster column-at-a-time shape, which is what lets a field
	/// reading through a lookup overlap its rows; `false` — the safe answer,
	/// and the default — evaluates a row at a time, each field in written
	/// order, so a call that writes or advances a sequence is made in the
	/// statement's own order. This operator passes a failed field call on as
	/// the batch's, so it repeats nothing, and the planner answers it with
	/// [`BatchRetry::None`](crate::exec::logical::BatchRetry::None).
	///
	/// Read only by the `include_all == false` path. The `include_all` path is
	/// a row at a time whatever this says, because dereferencing a record id
	/// and dropping the rows that miss are per-row decisions.
	pub(crate) column_major: bool,
	/// Per-operator metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Project {
	/// Create a new Project operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields: Vec<FieldSelection>,
		omit: Vec<OmitPath>,
		include_all: bool,
	) -> Self {
		Self {
			input,
			fields: fields.into(),
			omit: omit.into(),
			include_all,
			column_major: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Declare the batch free to run a field at a time; see
	/// [`Self::column_major`]. Only the planner, which holds the expressions
	/// as written, can answer this.
	pub(crate) fn with_column_major(mut self) -> Self {
		self.column_major = true;
		self
	}
}
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

	#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", skip_all))]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let fields = Arc::clone(&self.fields);
		let omit = Arc::clone(&self.omit);
		let include_all = self.include_all;
		let column_major = self.column_major;
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
					let mut values = Vec::with_capacity(batch.len());
					if fields.is_empty() && omit.is_empty() {
						// Pure SELECT * — move values directly, no clone needed
						for value in batch.into_values() {
							match value {
								Value::RecordId(rid) => {
									let fetched =
										super::fetch::fetch_record(eval_ctx.exec_ctx, &rid).await?;
									if !matches!(fetched, Value::None) {
										values.push(fetched);
									}
								}
								other => values.push(other),
							}
						}
					} else {
						// Reused across rows; `drain(..)` empties it for the next
						// iteration while keeping the backing buffer.
						let mut field_writes: Vec<(FieldPath, Value)> =
							Vec::with_capacity(fields.len());
						for value in batch.into_values() {
							// For RecordId inputs, resolve the target record FIRST.
							// When the record is missing or resolves to a non-Object,
							// field expressions must not run — field expressions can
							// have side effects (mutating subqueries) that must not
							// fire for rows we'll then drop or pass through raw.
							let fetched_obj: Option<Object> = if let Value::RecordId(rid) = &value {
								match super::fetch::fetch_record(eval_ctx.exec_ctx, rid).await? {
									Value::Object(obj) => Some(obj),
									Value::None => continue,
									mut raw => {
										for field in omit.iter() {
											field.apply(&mut raw, &eval_ctx).await?;
										}
										values.push(raw);
										continue;
									}
								}
							} else {
								None
							};

							// Evaluate every field expression against the original
							// `value` (still borrowed by `row_ctx`); writes are
							// applied later, after `value` is freely movable.
							field_writes.clear();
							if !fields.is_empty() {
								let row_ctx = eval_ctx.with_value_and_doc(&value);
								for field in fields.iter() {
									compute_field_writes(field, row_ctx.clone(), &mut field_writes)
										.await?;
								}
							}

							// Materialize the output. `value` is no longer borrowed,
							// so its Object payload can be moved out instead of
							// deep-cloned.
							let mut output_value = if let Some(obj) = fetched_obj {
								apply_field_writes(obj, &mut field_writes)
							} else {
								match value {
									Value::Object(obj) => {
										apply_field_writes(obj, &mut field_writes)
									}
									other if fields.is_empty() => other,
									_ => apply_field_writes(Object::default(), &mut field_writes),
								}
							};

							for field in omit.iter() {
								field.apply(&mut output_value, &eval_ctx).await?;
							}
							values.push(output_value);
						}
					}
					values
				} else {
					// --- non-include_all: build one output object per row ---
					//
					// A field at a time is the faster shape — `evaluate_batch`
					// lets an I/O-bound expression (a subquery, a lookup)
					// overlap its rows — but it evaluates one field for the
					// whole batch before the next, so two fields' effects
					// interleave differently from the statement as written.
					// That is invisible where the fields have no effect to
					// reorder, which is what `column_major` answers;
					// where they may, the batch is evaluated a row at a time,
					// each field in written order, which is the per-row
					// engine's order.
					let batch_len = batch.len();
					let mut objects: Vec<Object> =
						(0..batch_len).map(|_| Object::default()).collect();

					if column_major {
						for field in fields.iter() {
							if field.expr.is_projection_function() {
								// Projection functions return multiple field bindings;
								// handle per-row since they need special object assembly.
								for (i, value) in batch.values().iter().enumerate() {
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
								let field_values = field
									.expr
									.evaluate_batch(eval_ctx.clone(), batch.values())
									.await?;
								for (i, field_value) in field_values.into_iter().enumerate() {
									set_field_on_object(
										&mut objects[i],
										&field.output_path,
										field_value,
									);
								}
							}
						}
					} else {
						for (i, value) in batch.values().iter().enumerate() {
							let row_ctx = eval_ctx.with_value_and_doc(value);
							for field in fields.iter() {
								evaluate_and_set_field(&mut objects[i], field, row_ctx.clone())
									.await?;
							}
						}
					}

					let mut values: Vec<Value> = objects.into_iter().map(Value::Object).collect();
					if !omit.is_empty() {
						for val in &mut values {
							for field in omit.iter() {
								field.apply(val, &eval_ctx).await?;
							}
						}
					}
					values
				};

				Ok(ValueBatch::new(projected_values))
			}
		});

		Ok(monitor_stream(Box::pin(projected), "Project", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// evaluate_and_set_field
// ---------------------------------------------------------------------------

/// Evaluate a field expression and collect the resulting `(FieldPath, Value)`
/// writes into `writes`, without touching any output object.
///
/// Used by the `include_all` path so that field expressions can be evaluated
/// while the input row is still borrowed (for `row_ctx`), then applied later
/// to an output Object that has been moved out of the input value — avoiding
/// the deep clone of the input Object that mutating-in-place would force.
///
/// Semantics mirror [`evaluate_and_set_field`]:
/// - Projection functions with an explicit alias collapse to one write at the alias path (single
///   binding) or one write of an Array (multiple bindings).
/// - Projection functions without an alias produce one write per binding, keyed by the binding's
///   idiom.
/// - Regular expressions produce one write at `field.output_path`.
pub(crate) async fn compute_field_writes(
	field: &FieldSelection,
	eval_ctx: EvalContext<'_>,
	writes: &mut Vec<(FieldPath, Value)>,
) -> Result<(), crate::expr::ControlFlow> {
	if field.expr.is_projection_function()
		&& let Some(bindings) = field.expr.evaluate_projection(eval_ctx.clone()).await?
	{
		projection_writes(bindings, field.has_explicit_alias, &field.output_path, writes);
		return Ok(());
	}

	let field_value = field.expr.evaluate(eval_ctx).await?;
	writes.push((field.output_path.clone(), field_value));
	Ok(())
}

/// The writes a projection function's `bindings` make at a projection's
/// root: with an explicit alias, one write at `output_path` — the one
/// binding's value, or every binding's value in an array; otherwise one write
/// per binding, keyed by the binding's idiom, skipping an idiom that names no
/// field path.
pub(crate) fn projection_writes(
	bindings: Vec<(crate::expr::idiom::Idiom, Value)>,
	aliased: bool,
	output_path: &FieldPath,
	writes: &mut Vec<(FieldPath, Value)>,
) {
	if aliased {
		let value = if bindings.len() == 1 {
			bindings.into_iter().next().expect("bindings verified non-empty").1
		} else {
			Value::Array(bindings.into_iter().map(|(_, v)| v).collect::<Vec<_>>().into())
		};
		writes.push((output_path.clone(), value));
	} else {
		for (idiom, value) in bindings {
			if let Ok(path) = crate::exec::field_path_convert::field_path_from_idiom(&idiom)
				&& !path.is_empty()
			{
				writes.push((path, value));
			}
		}
	}
}

/// Evaluate a field expression and set the resulting value(s) on the output object.
///
/// For regular expressions, evaluates the expression and sets the result at the output_path.
/// For projection functions:
/// - If has_explicit_alias is true, use the alias (output_path) as the field name
/// - Otherwise, use the dynamic field names from the function result
pub(crate) async fn evaluate_and_set_field(
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
				if let Ok(path) = crate::exec::field_path_convert::field_path_from_idiom(&idiom)
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

// ============================================================================
// SelectProject - Simplified projection without expression evaluation
// ============================================================================

/// Specifies how to handle a field in SelectProject.
///
/// Field names are stored as [`Strand`] rather than `String` so that per-row
/// inserts into the output [`Object`] only pay a 24-byte bitwise copy for
/// short names (the common case), avoiding the heap allocation that
/// `String::clone` would do on every row × projection.
#[derive(Debug, Clone)]
pub enum Projection {
	/// Include a field with its original name
	Include(Strand),
	/// Rename a field (from internal name to output name)
	Rename {
		from: Strand,
		to: Strand,
	},
	/// Include all fields from input (SELECT *)
	All,
	/// Exclude a field (for OMIT)
	Omit(Strand),
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
impl ExecOperator for SelectProject {
	fn name(&self) -> &'static str {
		"SelectProject"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let proj_str = self
			.projections
			.iter()
			.map(|p| match p {
				Projection::Include(name) => name.to_string(),
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

	#[cfg_attr(feature = "observability", tracing::instrument(level = "trace", skip_all))]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let projections = Arc::clone(&self.projections);
		let ctx = ctx.clone();

		// Create a stream that applies projections
		let projected = input_stream.then(move |batch_result| {
			let projections = Arc::clone(&projections);
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut projected_values = Vec::with_capacity(batch.len());

				for value in batch.into_values() {
					let is_record_id = matches!(&value, Value::RecordId(_));
					let projected = apply_projections(value, &projections, &ctx).await?;
					if is_record_id && matches!(&projected, Value::None) {
						continue;
					}
					projected_values.push(projected);
				}

				Ok(ValueBatch::new(projected_values))
			}
		});

		Ok(monitor_stream(Box::pin(projected), "SelectProject", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// SelectProject helpers
// ---------------------------------------------------------------------------

/// Apply projections to a single value, taking ownership to avoid cloning.
async fn apply_projections(
	value: Value,
	projections: &[Projection],
	ctx: &ExecutionContext,
) -> Result<Value, crate::expr::ControlFlow> {
	let has_all = projections.iter().any(|p| matches!(p, Projection::All));
	let has_includes =
		projections.iter().any(|p| matches!(p, Projection::Include(_) | Projection::Rename { .. }));

	let input_obj = match value {
		Value::Object(obj) => obj,
		Value::RecordId(rid) if has_all || has_includes => {
			match super::fetch::fetch_record(ctx, &rid).await? {
				Value::Object(obj) => obj,
				Value::None => return Ok(Value::None),
				other => return Ok(other),
			}
		}
		// A geometry answers `type` and `coordinates` like an object, so a
		// projection naming fields reads them off one. `*` does not expand it:
		// the row is the geometry, and `SELECT *`, `SELECT * OMIT …` and
		// `SELECT *, type` all yield the geometry as it stands.
		Value::Geometry(geo) if has_includes && !has_all => geo.as_object(),
		other => return Ok(other),
	};

	Ok(apply_projections_to_object(input_obj, projections, has_all))
}

/// Apply projections to an already-resolved object (sync version).
/// Takes ownership of the input object to avoid cloning in the `has_all` path.
fn apply_projections_to_object(
	input_obj: Object,
	projections: &[Projection],
	has_all: bool,
) -> Value {
	if has_all {
		// `*` keeps every field of the row, and a renamed field reads the row as
		// it was stored: its value is taken before any projection writes, so
		// `*, a AS b, b AS c` gives `c` the stored `b`.
		let mut renamed = projections
			.iter()
			.filter_map(|projection| match projection {
				Projection::Rename {
					from,
					..
				} => Some(input_obj.get(from).cloned().unwrap_or(Value::None)),
				_ => None,
			})
			.collect::<Vec<_>>()
			.into_iter();
		let mut output = input_obj;
		for projection in projections {
			match projection {
				Projection::Include(name) => {
					if !output.contains_key(name) {
						output.insert(name.clone(), Value::None);
					}
				}
				Projection::Rename {
					to,
					..
				} => {
					let v = renamed.next().expect("one value per rename");
					output.insert(to.clone(), v);
				}
				Projection::Omit(name) => {
					output.remove(name.as_str());
				}
				Projection::All => {}
			}
		}
		Value::Object(output)
	} else {
		let mut output = Object::default();
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
				}
				Projection::Omit(name) => {
					output.remove(name.as_str());
				}
				Projection::All => {}
			}
		}
		Value::Object(output)
	}
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
			vec![Projection::Include(Strand::new("a")), Projection::Include(Strand::new("c"))];

		let result = apply_projections_to_object(obj, &projections, false);
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
			from: Strand::new("old_name"),
			to: Strand::new("new_name"),
		}];

		let result = apply_projections_to_object(obj, &projections, false);
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

		let projections = vec![Projection::All, Projection::Omit(Strand::new("b"))];

		let result = apply_projections_to_object(obj, &projections, true);
		if let Value::Object(result_obj) = result {
			assert!(result_obj.contains_key("a"));
			assert!(!result_obj.contains_key("b"));
			assert!(result_obj.contains_key("c"));
		} else {
			panic!("Expected Object");
		}
	}
}
