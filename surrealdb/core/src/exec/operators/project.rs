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
use tracing::instrument;

use crate::exec::field_path::FieldPath;
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::logical::{LogicalDestructureField, LogicalExpr, LogicalLiteral};
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
	pub omit: Arc<[LogicalExpr]>,
	/// Whether to include all fields from input (for SELECT *, field1, field2)
	pub include_all: bool,
	/// Whether every field expression is free of effects that evaluating it
	/// out of the order it was written in would expose: it stages no write,
	/// advances no counter, reaches nothing outside the transaction and runs
	/// no body the plan cannot see.
	///
	/// It decides how a batch is evaluated, never what the answer is. `true`
	/// licenses the faster column-at-a-time shape, which evaluates one field
	/// for every row of the batch before the next; `false` — the safe answer,
	/// and the default — evaluates a row at a time, each field in written
	/// order, so a call that writes or advances a sequence is made in the
	/// statement's own order.
	///
	/// Read only by the `include_all == false` path. The `include_all` path is
	/// a row at a time whatever this says, because dereferencing a record id
	/// and dropping the rows that miss are per-row decisions.
	pub(crate) effect_free_columns: bool,
	/// Per-operator metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Project {
	/// Create a new Project operator with fresh metrics.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields: Vec<FieldSelection>,
		omit: Vec<LogicalExpr>,
		include_all: bool,
	) -> Self {
		Self {
			input,
			fields: fields.into(),
			omit: omit.into(),
			include_all,
			effect_free_columns: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Declare every field expression effect-free; see
	/// [`Self::effect_free_columns`]. Only the planner, which holds the
	/// expressions as written, can answer this.
	pub(crate) fn with_effect_free_columns(mut self) -> Self {
		self.effect_free_columns = true;
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

	#[instrument(level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let fields = Arc::clone(&self.fields);
		let omit = Arc::clone(&self.omit);
		let include_all = self.include_all;
		let effect_free_columns = self.effect_free_columns;
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
											omit_field_sync(&mut raw, field);
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
								omit_field_sync(&mut output_value, field);
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
					// reorder, which is what `effect_free_columns` answers;
					// where they may, the batch is evaluated a row at a time,
					// each field in written order, which is the per-row
					// engine's order.
					let batch_len = batch.len();
					let mut objects: Vec<Object> =
						(0..batch_len).map(|_| Object::default()).collect();

					if effect_free_columns {
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
								omit_field_sync(val, field);
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
async fn compute_field_writes(
	field: &FieldSelection,
	eval_ctx: EvalContext<'_>,
	writes: &mut Vec<(FieldPath, Value)>,
) -> Result<(), crate::expr::ControlFlow> {
	if field.expr.is_projection_function()
		&& let Some(bindings) = field.expr.evaluate_projection(eval_ctx.clone()).await?
	{
		if field.has_explicit_alias {
			let value = if bindings.len() == 1 {
				bindings.into_iter().next().expect("bindings verified non-empty").1
			} else {
				Value::Array(bindings.into_iter().map(|(_, v)| v).collect::<Vec<_>>().into())
			};
			writes.push((field.output_path.clone(), value));
		} else {
			for (idiom, value) in bindings {
				if let Ok(path) = crate::exec::field_path_convert::field_path_from_idiom(&idiom)
					&& !path.is_empty()
				{
					writes.push((path, value));
				}
			}
		}
		return Ok(());
	}

	let field_value = field.expr.evaluate(eval_ctx).await?;
	writes.push((field.output_path.clone(), field_value));
	Ok(())
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

/// Synchronously remove the location a path expression denotes from a value.
///
/// The path is applied from its root, following the value through field
/// accesses, array indices and `.*` fan-out, and the outermost step decides
/// what is removed: a field is deleted from its object, an index removes the
/// element it numbers or the key it spells, `[$]` removes the last element,
/// `.*` empties the object or array, and a destructure deletes the listed
/// fields. A `Broadcast` applies its body to each value its base reaches.
///
/// The walk is structural, so it only reaches what a path names without
/// evaluating any of its steps. [`omit_path_is_expressible`] is the statement
/// of which paths those are, and the planner declines the rest: a path this
/// walker cannot perform would remove nothing at all, where the per-row
/// engine evaluates the step and removes what it names.
#[inline]
pub(crate) fn omit_field_sync(value: &mut Value, path: &LogicalExpr) {
	match path {
		LogicalExpr::Field(name) => {
			if let Value::Object(obj) = value {
				obj.remove(name.as_str());
			}
		}
		LogicalExpr::Access {
			base,
			field,
		} => for_each_target(value, base, &mut |target| {
			if let Value::Object(obj) = target {
				obj.remove(field.as_str());
			}
		}),
		LogicalExpr::Each(base) => for_each_target(value, base, &mut |target| match target {
			Value::Object(obj) => obj.clear(),
			Value::Array(arr) => arr.clear(),
			_ => {}
		}),
		LogicalExpr::Index {
			base,
			index,
		} => {
			let Some(idx) = literal_index(index) else {
				return;
			};
			// An index numbers a position in an array and spells a key in an
			// object, and removes what it names in either.
			for_each_target(value, base, &mut |target| match target {
				Value::Array(arr) if idx < arr.len() => {
					arr.remove(idx);
				}
				Value::Object(obj) => {
					obj.remove(idx.to_string().as_str());
				}
				_ => {}
			})
		}
		LogicalExpr::First(base) => for_each_target(value, base, &mut |target| {
			if let Value::Array(arr) = target
				&& !arr.is_empty()
			{
				arr.remove(0);
			}
		}),
		LogicalExpr::Last(base) => for_each_target(value, base, &mut |target| {
			if let Value::Array(arr) = target
				&& let Some(last) = arr.len().checked_sub(1)
			{
				arr.remove(last);
			}
		}),
		LogicalExpr::Destructure {
			base,
			fields,
		} => for_each_target(value, base, &mut |target| {
			// OMIT obj.c.{ d, f } removes d and f from obj.c.
			if let Value::Object(obj) = target {
				omit_destructure_fields(obj, fields);
			}
		}),
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => for_each_target(value, base, &mut |target| omit_field_sync(target, body)),
		_ => {}
	}
}

/// Whether [`omit_field_sync`] can perform the removal `path` describes.
///
/// The walker is structural: it follows a path through a value without
/// evaluating any of its steps. A step whose meaning comes from evaluation —
/// a filter, a method, a lookup, a recursion, an index that is not written
/// down as a non-negative integer — therefore names no location it can reach,
/// and applying such a path would remove nothing at all. The planner declines
/// a statement holding one so it runs on the per-row engine, which evaluates
/// the step and removes what it names.
pub(crate) fn omit_path_is_expressible(path: &LogicalExpr) -> bool {
	match path {
		LogicalExpr::Field(_) => true,
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Each(base)
		| LogicalExpr::First(base)
		| LogicalExpr::Last(base) => omit_target_is_reachable(base),
		LogicalExpr::Index {
			base,
			index,
		} => literal_index(index).is_some() && omit_target_is_reachable(base),
		LogicalExpr::Destructure {
			base,
			fields,
		} => omit_target_is_reachable(base) && omit_destructure_is_expressible(fields),
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => omit_target_is_reachable(base) && omit_path_is_expressible(body),
		_ => false,
	}
}

/// Whether [`for_each_target`] can reach the values `path` names.
fn omit_target_is_reachable(path: &LogicalExpr) -> bool {
	match path {
		LogicalExpr::Current | LogicalExpr::Field(_) => true,
		LogicalExpr::Access {
			base,
			..
		}
		| LogicalExpr::Each(base)
		| LogicalExpr::First(base)
		| LogicalExpr::Last(base) => omit_target_is_reachable(base),
		LogicalExpr::Index {
			base,
			index,
		} => literal_index(index).is_some() && omit_target_is_reachable(base),
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => omit_target_is_reachable(base) && omit_target_is_reachable(body),
		_ => false,
	}
}

/// Whether every entry of a destructure step names a location
/// [`omit_destructure_fields`] removes. An aliased entry does not: it names an
/// expression, which the per-row engine rejects outright.
fn omit_destructure_is_expressible(fields: &[LogicalDestructureField]) -> bool {
	fields.iter().all(|field| match field {
		LogicalDestructureField::Field(_) | LogicalDestructureField::All(_) => true,
		LogicalDestructureField::Destructure(_, nested) => omit_destructure_is_expressible(nested),
		LogicalDestructureField::Aliased(..) => false,
	})
}

/// The non-negative literal integer an index step holds, if any.
fn literal_index(index: &LogicalExpr) -> Option<usize> {
	match index {
		LogicalExpr::Literal(LogicalLiteral::Integer(idx)) => usize::try_from(*idx).ok(),
		_ => None,
	}
}

/// Calls `f` on every value `path` reaches inside `value`, following field
/// accesses into objects, literal indices into arrays and objects, `[$]` into
/// the last element of an array, `.*` into every child of an object or array,
/// and a `Broadcast` body into each value its base reaches. `Current` is
/// `value` itself. Any other step reaches nothing, and the planner declines a
/// path holding one — see [`omit_path_is_expressible`].
fn for_each_target(value: &mut Value, path: &LogicalExpr, f: &mut dyn FnMut(&mut Value)) {
	match path {
		LogicalExpr::Current => f(value),
		LogicalExpr::Field(name) => {
			if let Value::Object(obj) = value
				&& let Some(nested) = obj.get_mut(name.as_str())
			{
				f(nested);
			}
		}
		LogicalExpr::Access {
			base,
			field,
		} => for_each_target(value, base, &mut |target| {
			if let Value::Object(obj) = target
				&& let Some(nested) = obj.get_mut(field.as_str())
			{
				f(nested);
			}
		}),
		LogicalExpr::Each(base) => for_each_target(value, base, &mut |target| match target {
			Value::Object(obj) => {
				for (_, v) in obj.iter_mut() {
					f(v);
				}
			}
			Value::Array(arr) => {
				for v in arr.iter_mut() {
					f(v);
				}
			}
			_ => {}
		}),
		LogicalExpr::Index {
			base,
			index,
		} => {
			let Some(idx) = literal_index(index) else {
				return;
			};
			for_each_target(value, base, &mut |target| match target {
				Value::Array(arr) => {
					if let Some(nested) = arr.get_mut(idx) {
						f(nested);
					}
				}
				Value::Object(obj) => {
					if let Some(nested) = obj.get_mut(idx.to_string().as_str()) {
						f(nested);
					}
				}
				_ => {}
			})
		}
		LogicalExpr::First(base) => for_each_target(value, base, &mut |target| {
			if let Value::Array(arr) = target
				&& let Some(nested) = arr.first_mut()
			{
				f(nested);
			}
		}),
		LogicalExpr::Last(base) => for_each_target(value, base, &mut |target| {
			if let Value::Array(arr) = target
				&& let Some(nested) = arr.last_mut()
			{
				f(nested);
			}
		}),
		LogicalExpr::Broadcast {
			base,
			body,
			..
		} => for_each_target(value, base, &mut |target| for_each_target(target, body, f)),
		_ => {}
	}
}

/// Recursively remove fields described by destructure fields from an object.
fn omit_destructure_fields(obj: &mut Object, fields: &[LogicalDestructureField]) {
	for field in fields {
		match field {
			LogicalDestructureField::Field(name) => {
				obj.remove(name.as_str());
			}
			// `{ a.* }` names the contents of `a` rather than `a` itself, so
			// the container it holds is emptied and the field stays.
			LogicalDestructureField::All(name) => match obj.get_mut(name.as_str()) {
				Some(Value::Object(inner)) => inner.clear(),
				Some(Value::Array(inner)) => inner.clear(),
				_ => {}
			},
			LogicalDestructureField::Destructure(name, nested) => {
				if let Some(Value::Object(inner)) = obj.get_mut(name.as_str()) {
					omit_destructure_fields(inner, nested);
				}
			}
			// An aliased entry names an expression rather than a location to
			// remove. The planner declines such a path, leaving the per-row
			// engine to raise the error this walker has no way to report.
			LogicalDestructureField::Aliased(..) => {}
		}
	}
}

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

	#[instrument(level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
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
		let mut output = input_obj;
		for projection in projections {
			match projection {
				Projection::Include(name) => {
					if !output.contains_key(name) {
						output.insert(name.clone(), Value::None);
					}
				}
				Projection::Rename {
					from,
					to,
				} => {
					let v = output.remove(from).unwrap_or(Value::None);
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
	use surrealdb_types::ToSql;

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

	fn omit_path(sql: &str) -> LogicalExpr {
		crate::expr::logical::lower_expr(crate::sql::Expr::Idiom(
			crate::syn::idiom(sql).expect("idiom"),
		))
		.expect("lowers")
	}

	fn omitted(value: Value, sql: &str) -> String {
		let mut value = value;
		omit_field_sync(&mut value, &omit_path(sql));
		value.to_sql()
	}

	#[test]
	fn omit_removes_the_location_a_path_denotes() {
		let row = || {
			crate::val::convert_public::convert_public_value_to_internal(
				crate::syn::value(
					"{ a: { b: 1, c: [10, 20] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }",
				)
				.expect("value"),
			)
		};
		assert_eq!(omitted(row(), "a"), "{ d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }");
		assert_eq!(
			omitted(row(), "a.b"),
			"{ a: { c: [10, 20] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }"
		);
		assert_eq!(
			omitted(row(), "a.c[0]"),
			"{ a: { b: 1, c: [20] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }"
		);
		assert_eq!(
			omitted(row(), "a.c[$]"),
			"{ a: { b: 1, c: [10] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }"
		);
		assert_eq!(
			omitted(row(), "d.*.e"),
			"{ a: { b: 1, c: [10, 20] }, d: [{ f: 2 }, { f: 4 }] }"
		);
		assert_eq!(
			omitted(row(), "d[1].f"),
			"{ a: { b: 1, c: [10, 20] }, d: [{ e: 1, f: 2 }, { e: 3 }] }"
		);
		assert_eq!(omitted(row(), "a.*"), "{ a: {  }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }");
		assert_eq!(
			omitted(row(), "a.{ b }"),
			"{ a: { c: [10, 20] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }"
		);
		// A destructure entry naming the contents of a field empties that
		// field rather than removing it.
		assert_eq!(
			omitted(row(), "a.{ c.* }"),
			"{ a: { b: 1, c: [] }, d: [{ e: 1, f: 2 }, { e: 3, f: 4 }] }"
		);
	}

	#[test]
	fn omit_declines_a_path_the_walk_cannot_reach() {
		// Every shape the walk performs.
		for sql in [
			"a",
			"a.b",
			"a.c[0]",
			"a.c[$]",
			"d.*.e",
			"d[1].f",
			"a.*",
			"a.{ b }",
			"a.{ c.* }",
			"a.{ b.{ c } }",
		] {
			assert!(omit_path_is_expressible(&omit_path(sql)), "{sql} is expressible");
		}
		// Every shape it cannot: a step whose meaning comes from evaluating
		// it, and a destructure entry naming an expression rather than a
		// location.
		for sql in [
			"d[WHERE e = 1].f",
			"d[WHERE e = 1]",
			"a.c[$p]",
			"a.c.len()",
			"a.{ b }.c",
			"a.{ b: c }",
			"a.{1}.b",
			"a.b.@",
		] {
			assert!(!omit_path_is_expressible(&omit_path(sql)), "{sql} is not expressible");
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
