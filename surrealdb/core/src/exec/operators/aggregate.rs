use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::function::{Accumulator, AggregateFunction};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::val::{Object, Value};

/// Aggregates values by grouping keys.
///
/// GROUP BY collects all values into groups based on the specified keys,
/// then applies aggregate functions (COUNT, SUM, array::group, etc.) to each group.
///
/// This is a pipeline breaking operator - it must consume the entire input stream
/// before producing any output.
#[derive(Debug, Clone)]
pub struct Aggregate {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The original GROUP BY idioms (for display/debugging purposes).
	pub(crate) group_by: Vec<Idiom>,
	/// Physical expressions to evaluate for computing group keys.
	/// These are the actual expressions that determine grouping.
	/// For `GROUP BY country, year` where `year` is an alias for `time::year(time)`,
	/// this would contain expressions for `country` and `time::year(time)`.
	pub(crate) group_by_exprs: Vec<Arc<dyn PhysicalExpr>>,
	/// The aggregate expressions to compute for each group.
	/// These are the selected fields that may contain aggregate functions.
	pub(crate) aggregates: Vec<AggregateField>,
}

/// Represents a field in the SELECT that may be an aggregate.
#[derive(Debug, Clone)]
pub struct AggregateField {
	/// The output path for this field (e.g., ["address", "city"] for "address.city")
	/// This allows proper nested object construction.
	pub output_path: Vec<String>,
	/// Whether this field is a group-by key (passed through unchanged)
	pub is_group_key: bool,
	/// If this is a group-by key, the index into the group key vector.
	/// This allows retrieving the computed group key value directly.
	pub group_key_index: Option<usize>,
	/// Information about aggregate functions in this expression (if any).
	/// When set, the accumulator-based evaluation is used.
	/// Supports multiple aggregates per expression (e.g., `SUM(a) + AVG(a)`).
	pub aggregate_expr_info: Option<AggregateExprInfo>,
	/// Expression to evaluate for non-aggregate fields (e.g., group-by keys or first-value
	/// fields). This is used when aggregate_expr_info is None.
	pub fallback_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl AggregateField {
	/// Create a new AggregateField from an output name string.
	/// If the name contains dots and represents a simple field path (no special characters),
	/// it will be split into path components for nested object construction.
	pub fn new(
		name: String,
		is_group_key: bool,
		group_key_index: Option<usize>,
		aggregate_expr_info: Option<AggregateExprInfo>,
		fallback_expr: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		let output_path = if name.contains('.') && !name.contains(['[', '(', ' ']) {
			name.split('.').map(|s| s.to_string()).collect()
		} else {
			vec![name]
		};
		Self {
			output_path,
			is_group_key,
			group_key_index,
			aggregate_expr_info,
			fallback_expr,
		}
	}

	/// Check if this is an empty name (used for SELECT VALUE with GROUP BY)
	pub fn is_empty_name(&self) -> bool {
		self.output_path.len() == 1 && self.output_path[0].is_empty()
	}
}

/// Information about all aggregates extracted from a single SELECT expression.
///
/// Supports expressions with multiple aggregates like `SUM(a) + AVG(a)`.
/// Each aggregate is extracted and assigned a synthetic field name (`_a0`, `_a1`, etc.).
/// The original expression is transformed to reference these fields.
#[derive(Clone)]
pub struct AggregateExprInfo {
	/// All extracted aggregate functions, indexed by their position.
	/// For `SUM(a) + AVG(a)`, this would contain `[SUM(a), AVG(a)]`.
	pub aggregates: Vec<ExtractedAggregate>,

	/// The transformed expression with aggregates replaced by field references.
	/// Uses synthetic field names like `_a0`, `_a1` that correspond to
	/// indices in the `aggregates` vector.
	/// None if the expression is a direct single aggregate (no transformation needed).
	pub post_expr: Option<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for AggregateExprInfo {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AggregateExprInfo")
			.field("num_aggregates", &self.aggregates.len())
			.field("has_post_expr", &self.post_expr.is_some())
			.finish()
	}
}

/// A single aggregate function extracted from an expression.
#[derive(Clone)]
pub struct ExtractedAggregate {
	/// The aggregate function from the registry.
	pub function: Arc<dyn AggregateFunction>,
	/// The expression to evaluate per-row to get the value to accumulate.
	/// For `math::mean(a)`, this would be the expression for `a`.
	pub argument_expr: Arc<dyn PhysicalExpr>,
	/// Additional arguments (evaluated once per group, not per-row).
	/// For `array::join(txt, " ")`, this would contain the separator expression.
	pub extra_args: Vec<Arc<dyn PhysicalExpr>>,
}

impl std::fmt::Debug for ExtractedAggregate {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ExtractedAggregate").field("function", &self.function.name()).finish()
	}
}

/// Generate a synthetic field name for an aggregate at the given index.
/// These names are used in the transformed expression to reference aggregate results.
pub fn aggregate_field_name(idx: usize) -> String {
	format!("_a{}", idx)
}

/// Key for grouping - a tuple of values corresponding to GROUP BY expressions
type GroupKey = Vec<Value>;

/// Per-group aggregate state using accumulators
struct GroupState {
	/// Accumulators for each aggregate field.
	/// For fields with multiple aggregates (e.g., `SUM(a) + AVG(a)`),
	/// this contains a Vec of accumulators, one per extracted aggregate.
	/// Empty Vec for non-aggregate fields.
	accumulators: Vec<Vec<Box<dyn Accumulator>>>,
	/// First values seen for non-aggregate fields
	first_values: Vec<Value>,
}

#[async_trait]
impl ExecOperator for Aggregate {
	fn name(&self) -> &'static str {
		"Aggregate"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		if self.group_by.is_empty() {
			vec![("mode".to_string(), "GROUP ALL".to_string())]
		} else {
			vec![(
				"by".to_string(),
				self.group_by.iter().map(|i| i.to_sql()).collect::<Vec<_>>().join(", "),
			)]
		}
	}

	fn required_context(&self) -> ContextLevel {
		// Aggregate needs database context for expression evaluation
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with aggregate expression modes
		let mut mode = self.input.access_mode();
		// Include group-by expressions
		for expr in &self.group_by_exprs {
			mode = mode.combine(expr.access_mode());
		}
		for agg in &self.aggregates {
			if let Some(info) = &agg.aggregate_expr_info {
				// Check all extracted aggregates
				for extracted in &info.aggregates {
					mode = mode.combine(extracted.argument_expr.access_mode());
					for extra_arg in &extracted.extra_args {
						mode = mode.combine(extra_arg.access_mode());
					}
				}
				// Check post-expression
				if let Some(post_expr) = &info.post_expr {
					mode = mode.combine(post_expr.access_mode());
				}
			}
			if let Some(expr) = &agg.fallback_expr {
				mode = mode.combine(expr.access_mode());
			}
		}
		mode
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let group_by_exprs = self.group_by_exprs.clone();
		let aggregates = self.aggregates.clone();
		let ctx = ctx.clone();

		// Collect all input batches, then group and aggregate
		let aggregate_stream = async_stream::try_stream! {
			// Pre-evaluate extra_args for each aggregate (evaluated once, not per-row)
			// This is needed for functions like array::join(txt, " ") where " " is evaluated once
			// Structure: evaluated_extra_args[field_idx][aggregate_idx] = Vec<Value>
			let eval_ctx_for_args = EvalContext::from_exec_ctx(&ctx);
			let mut evaluated_extra_args: Vec<Vec<Vec<Value>>> = Vec::with_capacity(aggregates.len());
			for agg in &aggregates {
				if let Some(info) = &agg.aggregate_expr_info {
					let mut field_args = Vec::with_capacity(info.aggregates.len());
					for extracted in &info.aggregates {
						let mut args = Vec::with_capacity(extracted.extra_args.len());
						for extra_arg in &extracted.extra_args {
							let value = match extra_arg.evaluate(eval_ctx_for_args.clone()).await {
								Ok(v) => v,
								Err(e) => {
									tracing::debug!(error = %e, "Extra arg evaluation failed, using None");
									Value::None
								}
							};
							args.push(value);
						}
						field_args.push(args);
					}
					evaluated_extra_args.push(field_args);
				} else {
					evaluated_extra_args.push(vec![]);
				}
			}

			// Accumulate all values into groups
			let mut groups: BTreeMap<GroupKey, GroupState> = BTreeMap::new();

			// Consume all input batches
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				let batch = batch_result?;
				for value in batch.values {
					// Create evaluation context for this row
					let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&value);

					// Compute the group key by evaluating group-by expressions
					let key = compute_group_key_async(&group_by_exprs, eval_ctx.clone()).await;

					// Get or create the group state
					let state = groups.entry(key).or_insert_with(|| create_group_state(&aggregates, &evaluated_extra_args));

					// Update aggregate states with this value
					for (i, agg) in aggregates.iter().enumerate() {
						if agg.is_group_key {
							// Group keys are extracted from the key, not the value
							continue;
						}

						if let Some(info) = &agg.aggregate_expr_info {
							// Evaluate and update ALL aggregates for this field
							for (agg_idx, extracted) in info.aggregates.iter().enumerate() {
								match extracted.argument_expr.evaluate(eval_ctx.clone()).await {
									Ok(arg_value) => {
										if let Some(acc) = state.accumulators[i].get_mut(agg_idx)
											&& let Err(e) = acc.update(arg_value)
										{
											tracing::debug!(error = %e, "Accumulator update failed, skipping value");
										}
									}
									Err(e) => {
										tracing::debug!(error = %e, "Aggregate argument evaluation failed, skipping value");
									}
								}
							}
						} else if let Some(expr) = &agg.fallback_expr {
							// Non-aggregate field - store first value
							if state.first_values[i].is_none() {
								match expr.evaluate(eval_ctx.clone()).await {
									Ok(field_value) => {
										state.first_values[i] = field_value;
									}
									Err(e) => {
										tracing::debug!(error = %e, "Fallback expression evaluation failed");
									}
								}
							}
						}
					}
				}
			}

			// Now compute final results for each group
			let mut results = Vec::with_capacity(groups.len());
			for (group_key, state) in groups {
				let result = compute_group_result_async(
					&group_key,
					state,
					&aggregates,
					&ctx,
				).await;
				results.push(result);
			}

			yield ValueBatch { values: results };
		};

		Ok(Box::pin(aggregate_stream))
	}
}

/// Create initial group state with accumulators for each aggregate field.
///
/// The `evaluated_extra_args` parameter contains the pre-evaluated extra arguments
/// for each aggregate field: `evaluated_extra_args[field_idx][aggregate_idx] = Vec<Value>`.
fn create_group_state(
	aggregates: &[AggregateField],
	evaluated_extra_args: &[Vec<Vec<Value>>],
) -> GroupState {
	let accumulators = aggregates
		.iter()
		.enumerate()
		.map(|(i, agg)| {
			if let Some(info) = &agg.aggregate_expr_info {
				// Create an accumulator for each extracted aggregate
				info.aggregates
					.iter()
					.enumerate()
					.map(|(agg_idx, extracted)| {
						let extra_args = evaluated_extra_args
							.get(i)
							.and_then(|field_args| field_args.get(agg_idx))
							.map(|v| v.as_slice())
							.unwrap_or(&[]);
						extracted.function.create_accumulator_with_args(extra_args)
					})
					.collect()
			} else {
				// Non-aggregate field - no accumulators
				vec![]
			}
		})
		.collect();

	let first_values = aggregates.iter().map(|_| Value::None).collect();

	GroupState {
		accumulators,
		first_values,
	}
}

/// Compute the group key by evaluating GROUP BY expressions.
///
/// Unlike simple idiom picking, this supports computed expressions like `time::year(time)`.
async fn compute_group_key_async(
	group_by_exprs: &[Arc<dyn PhysicalExpr>],
	eval_ctx: EvalContext<'_>,
) -> GroupKey {
	let mut key = Vec::with_capacity(group_by_exprs.len());
	for expr in group_by_exprs {
		let value = match expr.evaluate(eval_ctx.clone()).await {
			Ok(v) => v,
			Err(e) => {
				tracing::debug!(error = %e, "Group key expression evaluation failed, using None");
				Value::None
			}
		};
		key.push(value);
	}
	key
}

/// Compute the value for a single aggregate field.
///
/// This handles three cases:
/// 1. Group-by key: return the key value from the group key vector
/// 2. Aggregate expression: finalize accumulators and optionally evaluate post-expression
/// 3. Non-aggregate field: return the first value seen
async fn compute_single_field_value(
	agg: &AggregateField,
	group_key: &GroupKey,
	accumulators: &[Box<dyn Accumulator>],
	first_value: Value,
	ctx: &ExecutionContext,
) -> Value {
	if let Some(idx) = agg.group_key_index {
		// For group-by keys, use the key value directly by index
		group_key.get(idx).cloned().unwrap_or(Value::None)
	} else if let Some(info) = &agg.aggregate_expr_info {
		// Compute the aggregate value(s)
		compute_aggregate_field_value(info, accumulators, ctx).await
	} else {
		// Return first value for non-aggregate fields
		first_value
	}
}

/// Compute the result value for a single group, with support for multiple aggregates per field.
///
/// For expressions like `SUM(a) + AVG(a)`:
/// 1. Finalize all accumulators to get `{ _a0: sum_value, _a1: avg_value }`
/// 2. Evaluate the post-expression against this document to get the final value
async fn compute_group_result_async(
	group_key: &GroupKey,
	state: GroupState,
	aggregates: &[AggregateField],
	ctx: &ExecutionContext,
) -> Value {
	// Special case: SELECT VALUE with GROUP BY
	// If there's exactly one aggregate with an empty name, return the raw value
	if aggregates.len() == 1 && aggregates[0].is_empty_name() {
		let agg = &aggregates[0];
		let first_value = state.first_values.into_iter().next().unwrap_or(Value::None);
		let accumulators = state.accumulators.into_iter().next().unwrap_or_default();
		return compute_single_field_value(agg, group_key, &accumulators, first_value, ctx).await;
	}

	// Normal case: return an object with field names
	let mut result = Object::default();

	// Zip aggregates with their corresponding accumulators and first values
	let field_data = aggregates.iter().zip(state.accumulators).zip(state.first_values);

	for ((agg, accumulators), first_value) in field_data {
		let field_value =
			compute_single_field_value(agg, group_key, &accumulators, first_value, ctx).await;

		// Use nested setting to properly construct nested objects
		// e.g., path ["address", "city"] creates { address: { city: value } }
		set_nested_value(&mut result, &agg.output_path, field_value);
	}

	Value::Object(result)
}

/// Set a value at a nested path in an object.
///
/// For a path like ["address", "city"], this creates or updates:
/// `{ address: { city: value } }`
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

/// Compute the final value for a field with aggregate expressions.
///
/// If there's a post_expr, builds a document with all aggregate results
/// and evaluates the expression against it. Otherwise returns the single
/// aggregate value directly.
async fn compute_aggregate_field_value(
	info: &AggregateExprInfo,
	accumulators: &[Box<dyn Accumulator>],
	ctx: &ExecutionContext,
) -> Value {
	if info.aggregates.is_empty() {
		return Value::Null;
	}

	// Finalize all accumulators and build the aggregate document
	let mut agg_doc = Object::default();
	for (idx, acc) in accumulators.iter().enumerate() {
		let value = match acc.finalize() {
			Ok(v) => v,
			Err(e) => {
				debug!(error = %e, idx, "Accumulator finalize failed, using Null");
				Value::Null
			}
		};
		agg_doc.insert(aggregate_field_name(idx), value);
	}

	// If there's a post-expression, evaluate it against the aggregate document
	if let Some(post_expr) = &info.post_expr {
		let doc_value = Value::Object(agg_doc);
		let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&doc_value);
		match post_expr.evaluate(eval_ctx).await {
			Ok(v) => v,
			Err(e) => {
				debug!(error = %e, "Post-expression evaluation failed, using Null");
				Value::Null
			}
		}
	} else {
		// No post-expression means direct single aggregate - return first value
		agg_doc.0.into_values().next().unwrap_or(Value::Null)
	}
}
