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
/// This is a blocking operator - it must consume the entire input stream
/// before producing any output.
#[derive(Debug, Clone)]
pub struct Aggregate {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The fields/expressions to group by. Empty means GROUP ALL.
	pub(crate) group_by: Vec<Idiom>,
	/// The aggregate expressions to compute for each group.
	/// These are the selected fields that may contain aggregate functions.
	pub(crate) aggregates: Vec<AggregateField>,
}

/// Represents a field in the SELECT that may be an aggregate.
#[derive(Debug, Clone)]
pub struct AggregateField {
	/// The output field name (alias or computed)
	pub name: String,
	/// Whether this field is a group-by key (passed through unchanged)
	pub is_group_key: bool,
	/// Information about aggregate functions in this expression (if any).
	/// When set, the accumulator-based evaluation is used.
	/// Supports multiple aggregates per expression (e.g., `SUM(a) + AVG(a)`).
	pub aggregate_expr_info: Option<AggregateExprInfo>,
	/// Expression to evaluate for non-aggregate fields (e.g., group-by keys or first-value
	/// fields). This is used when aggregate_expr_info is None.
	pub fallback_expr: Option<Arc<dyn PhysicalExpr>>,
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
		let group_by = self.group_by.clone();
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
							let value = extra_arg.evaluate(eval_ctx_for_args.clone()).await.unwrap_or(Value::None);
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
					// Compute the group key from the value
					let key = compute_group_key(&value, &group_by);

					// Get or create the group state
					let state = groups.entry(key).or_insert_with(|| create_group_state(&aggregates, &evaluated_extra_args));

					// Update aggregate states with this value
					for (i, agg) in aggregates.iter().enumerate() {
						if agg.is_group_key {
							// Group keys are extracted from the key, not the value
							continue;
						}

						let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&value);

						if let Some(info) = &agg.aggregate_expr_info {
							// Evaluate and update ALL aggregates for this field
							for (agg_idx, extracted) in info.aggregates.iter().enumerate() {
								match extracted.argument_expr.evaluate(eval_ctx.clone()).await {
									Ok(arg_value) => {
										if let Some(acc) = state.accumulators[i].get_mut(agg_idx) {
											// Update the accumulator with the argument value
											if let Err(_e) = acc.update(arg_value) {
												// On error, continue (value is effectively skipped)
											}
										}
									}
									Err(_) => {
										// On error, skip this value for this aggregate
									}
								}
							}
						} else if let Some(expr) = &agg.fallback_expr {
							// Non-aggregate field - store first value
							if state.first_values[i].is_none() {
								match expr.evaluate(eval_ctx).await {
									Ok(field_value) => {
										state.first_values[i] = field_value;
									}
									Err(_) => {
										// On error, leave as None
									}
								}
							}
						}
					}
				}
			}

			// If no groups and we have GROUP ALL, create an empty group
			if groups.is_empty() && group_by.is_empty() {
				groups.insert(vec![], create_group_state(&aggregates, &evaluated_extra_args));
			}

			// Now compute final results for each group
			let mut results = Vec::new();
			for (group_key, state) in groups {
				let result = compute_group_result_async(
					&group_key,
					state,
					&group_by,
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

/// Compute the group key for a value based on GROUP BY expressions.
fn compute_group_key(value: &Value, group_by: &[Idiom]) -> GroupKey {
	if group_by.is_empty() {
		// GROUP ALL - single group for everything
		vec![]
	} else {
		group_by.iter().map(|idiom| value.pick(idiom)).collect()
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
	group_by: &[Idiom],
	aggregates: &[AggregateField],
	ctx: &ExecutionContext,
) -> Value {
	// Special case: SELECT VALUE with GROUP BY
	// If there's exactly one aggregate with an empty name, return the raw value
	if aggregates.len() == 1 && aggregates[0].name.is_empty() {
		let agg = &aggregates[0];
		return if agg.is_group_key {
			// For group-by keys, use the key value directly
			find_matching_group_key_value(group_key, group_by, &agg.name)
		} else if let Some(info) = &agg.aggregate_expr_info {
			// Compute the aggregate value(s)
			compute_aggregate_field_value(info, &state.accumulators[0], ctx).await
		} else {
			// Return first value
			state.first_values.into_iter().next().unwrap_or(Value::None)
		};
	}

	// Normal case: return an object with field names
	let mut result = Object::default();

	let mut accumulators_iter = state.accumulators.into_iter();
	let mut first_values = state.first_values.into_iter();

	for agg in aggregates.iter() {
		let field_accumulators = accumulators_iter.next().unwrap_or_default();
		let first_value = first_values.next().unwrap_or(Value::None);

		let field_value = if agg.is_group_key {
			// For group-by keys, use the key value directly
			find_matching_group_key_value(group_key, group_by, &agg.name)
		} else if let Some(info) = &agg.aggregate_expr_info {
			// Compute the aggregate value(s)
			compute_aggregate_field_value(info, &field_accumulators, ctx).await
		} else {
			// Return first value for non-aggregate fields
			first_value
		};
		result.insert(agg.name.clone(), field_value);
	}

	Value::Object(result)
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
		let value = acc.finalize().unwrap_or(Value::Null);
		agg_doc.insert(aggregate_field_name(idx), value);
	}

	// If there's a post-expression, evaluate it against the aggregate document
	if let Some(post_expr) = &info.post_expr {
		let doc_value = Value::Object(agg_doc);
		let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&doc_value);
		post_expr.evaluate(eval_ctx).await.unwrap_or(Value::Null)
	} else {
		// No post-expression means direct single aggregate - return first value
		agg_doc.0.into_values().next().unwrap_or(Value::Null)
	}
}

/// Find the group key value that matches the given field name.
fn find_matching_group_key_value(group_key: &GroupKey, group_by: &[Idiom], name: &str) -> Value {
	// For empty name (VALUE queries), use the first group key
	if name.is_empty() && !group_key.is_empty() {
		return group_key[0].clone();
	}

	// Otherwise find by matching field name
	let key_idx = group_by.iter().position(|g| idiom_to_field_name(g) == name).unwrap_or(0);
	group_key.get(key_idx).cloned().unwrap_or(Value::None)
}

/// Extract a simple field name from an idiom for matching.
fn idiom_to_field_name(idiom: &Idiom) -> String {
	use surrealdb_types::ToSql;

	use crate::expr::part::Part;
	if let Some(Part::Field(f)) = idiom.first() {
		f.to_string()
	} else {
		idiom.to_sql()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_compute_group_key_empty() {
		let value = Value::from(42);
		let key = compute_group_key(&value, &[]);
		assert!(key.is_empty());
	}
}
