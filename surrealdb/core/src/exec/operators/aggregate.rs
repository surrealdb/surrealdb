use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecutionContext, FlowResult, OperatorPlan,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::idiom::Idiom;
use crate::val::{Number, Object, Value};

/// Aggregates values by grouping keys.
///
/// GROUP BY collects all values into groups based on the specified keys,
/// then applies aggregate functions (COUNT, SUM, array::group, etc.) to each group.
///
/// This is a blocking operator - it must consume the entire input stream
/// before producing any output.
#[derive(Debug, Clone)]
pub struct Aggregate {
	pub(crate) input: Arc<dyn OperatorPlan>,
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
	/// The expression to evaluate. May contain aggregate functions.
	pub expr: Arc<dyn PhysicalExpr>,
	/// Whether this field is a group-by key (passed through unchanged)
	pub is_group_key: bool,
	/// Optional aggregate function type (for simple aggregate detection)
	pub aggregate_type: Option<AggregateType>,
}

/// Known aggregate function types for simple aggregate handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateType {
	/// COUNT() - counts all values
	Count,
	/// COUNT(field) - counts non-null values
	CountField,
	/// SUM(field) - sums numeric values
	Sum,
	/// MIN(field) - finds minimum
	Min,
	/// MAX(field) - finds maximum
	Max,
	/// AVG(field) or math::mean(field) - calculates average
	Avg,
	/// array::group(field) - collects values into an array
	ArrayGroup,
}

/// Running aggregate state for a single field.
#[derive(Debug, Clone)]
enum AggregateState {
	Count(i64),
	CountField(i64),
	Sum(Number),
	Min(Value),
	Max(Value),
	Avg {
		sum: Number,
		count: i64,
	},
	ArrayGroup(Vec<Value>),
	/// For non-aggregate fields, just store the first value seen
	FirstValue(Value),
}

impl AggregateState {
	fn new(agg_type: Option<AggregateType>) -> Self {
		match agg_type {
			Some(AggregateType::Count) => AggregateState::Count(0),
			Some(AggregateType::CountField) => AggregateState::CountField(0),
			Some(AggregateType::Sum) => AggregateState::Sum(Number::Int(0)),
			Some(AggregateType::Min) => AggregateState::Min(Value::None),
			Some(AggregateType::Max) => AggregateState::Max(Value::None),
			Some(AggregateType::Avg) => AggregateState::Avg {
				sum: Number::Int(0),
				count: 0,
			},
			Some(AggregateType::ArrayGroup) => AggregateState::ArrayGroup(Vec::new()),
			None => AggregateState::FirstValue(Value::None),
		}
	}

	fn update(&mut self, value: Value) {
		match self {
			AggregateState::Count(count) => *count += 1,
			AggregateState::CountField(count) => {
				if !value.is_none() && !value.is_null() {
					*count += 1;
				}
			}
			AggregateState::Sum(sum) => {
				if let Some(num) = value.as_number() {
					*sum = sum.clone() + num.clone();
				}
			}
			AggregateState::Min(min) => {
				if !value.is_none() && !value.is_null() {
					if min.is_none() || value < *min {
						*min = value;
					}
				}
			}
			AggregateState::Max(max) => {
				if !value.is_none() && !value.is_null() {
					if max.is_none() || value > *max {
						*max = value;
					}
				}
			}
			AggregateState::Avg {
				sum,
				count,
			} => {
				if let Some(num) = value.as_number() {
					*sum = sum.clone() + num.clone();
					*count += 1;
				}
			}
			AggregateState::ArrayGroup(values) => {
				values.push(value);
			}
			AggregateState::FirstValue(first) => {
				if first.is_none() {
					*first = value;
				}
			}
		}
	}

	fn finalize(self) -> Value {
		match self {
			AggregateState::Count(count) => Value::Number(Number::Int(count)),
			AggregateState::CountField(count) => Value::Number(Number::Int(count)),
			AggregateState::Sum(sum) => Value::Number(sum),
			AggregateState::Min(min) => {
				if min.is_none() {
					Value::Null
				} else {
					min
				}
			}
			AggregateState::Max(max) => {
				if max.is_none() {
					Value::Null
				} else {
					max
				}
			}
			AggregateState::Avg {
				sum,
				count,
			} => {
				if count == 0 {
					Value::Null
				} else {
					// Convert to float division
					let sum_f64 = sum.to_float();
					Value::Number(Number::Float(sum_f64 / count as f64))
				}
			}
			AggregateState::ArrayGroup(values) => Value::Array(values.into()),
			AggregateState::FirstValue(value) => value,
		}
	}
}

/// Key for grouping - a tuple of values corresponding to GROUP BY expressions
type GroupKey = Vec<Value>;

/// Per-group aggregate state
struct GroupState {
	/// State for each aggregate field
	field_states: Vec<AggregateState>,
}

#[async_trait]
impl OperatorPlan for Aggregate {
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
			mode = mode.combine(agg.expr.access_mode());
		}
		mode
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = self.input.execute(ctx)?;
		let group_by = self.group_by.clone();
		let aggregates = self.aggregates.clone();
		let ctx = ctx.clone();

		// Collect all input batches, then group and aggregate
		let aggregate_stream = async_stream::try_stream! {
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
					let state = groups.entry(key).or_insert_with(|| GroupState {
						field_states: aggregates.iter().map(|a| AggregateState::new(a.aggregate_type)).collect(),
					});

					// Update aggregate states with this value
					for (i, agg) in aggregates.iter().enumerate() {
						if agg.is_group_key {
							// Group keys are extracted from the key, not the value
							continue;
						}

						// Evaluate the expression to get the value to aggregate
						let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value(&value);
						match agg.expr.evaluate(eval_ctx).await {
							Ok(field_value) => {
								state.field_states[i].update(field_value);
							}
							Err(_) => {
								// On error, update with None
								state.field_states[i].update(Value::None);
							}
						}
					}
				}
			}

			// If no groups and we have GROUP ALL, create an empty group
			if groups.is_empty() && group_by.is_empty() {
				groups.insert(vec![], GroupState {
					field_states: aggregates.iter().map(|a| AggregateState::new(a.aggregate_type)).collect(),
				});
			}

			// Now compute final results for each group
			let mut results = Vec::new();
			for (group_key, state) in groups {
				let result = compute_group_result(&group_key, state, &group_by, &aggregates);
				results.push(result);
			}

			yield ValueBatch { values: results };
		};

		Ok(Box::pin(aggregate_stream))
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

/// Compute the result value for a single group.
fn compute_group_result(
	group_key: &GroupKey,
	state: GroupState,
	group_by: &[Idiom],
	aggregates: &[AggregateField],
) -> Value {
	// Special case: SELECT VALUE with GROUP BY
	// If there's exactly one aggregate with an empty name, return the raw value
	if aggregates.len() == 1 && aggregates[0].name.is_empty() {
		let agg = &aggregates[0];
		return if agg.is_group_key {
			// For group-by keys, use the key value directly
			// Find which group key this corresponds to
			find_matching_group_key_value(group_key, group_by, &agg.name)
		} else {
			// Finalize the aggregate state
			state.field_states[0].clone().finalize()
		};
	}

	// Normal case: return an object with field names
	let mut result = Object::default();

	for (i, agg) in aggregates.iter().enumerate() {
		let field_value = if agg.is_group_key {
			// For group-by keys, use the key value directly
			find_matching_group_key_value(group_key, group_by, &agg.name)
		} else {
			// Finalize the aggregate state
			state.field_states[i].clone().finalize()
		};
		result.insert(agg.name.clone(), field_value);
	}

	Value::Object(result)
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
