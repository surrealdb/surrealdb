//! Sort operator - applies ORDER BY to a stream.

use std::cmp::Ordering;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::err::Error;
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecutionContext, OperatorPlan,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::val::Value;

/// Sort direction for ORDER BY
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
	/// Ascending order (default)
	Asc,
	/// Descending order
	Desc,
}

impl Default for SortDirection {
	fn default() -> Self {
		Self::Asc
	}
}

/// Where to place NULL values in sort order
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullsOrder {
	/// NULLs sort first (before all other values)
	First,
	/// NULLs sort last (after all other values)
	Last,
}

impl Default for NullsOrder {
	fn default() -> Self {
		// Default: nulls last for ASC, nulls first for DESC
		// This will be applied based on direction in the sort comparison
		Self::Last
	}
}

/// A single field in an ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderByField {
	/// Expression to evaluate for each row
	pub expr: Arc<dyn PhysicalExpr>,
	/// Sort direction
	pub direction: SortDirection,
	/// Where to place nulls
	pub nulls: NullsOrder,
}

/// Sorts the input stream by the specified ORDER BY fields.
///
/// This is a blocking operator - it must collect all input before
/// producing any output, since sorting requires seeing all values.
#[derive(Debug, Clone)]
pub struct Sort {
	pub(crate) input: Arc<dyn OperatorPlan>,
	pub(crate) order_by: Vec<OrderByField>,
}

#[async_trait]
impl OperatorPlan for Sort {
	fn name(&self) -> &'static str {
		"Sort"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let order_str = self
			.order_by
			.iter()
			.map(|f| {
				let dir = match f.direction {
					SortDirection::Asc => "ASC",
					SortDirection::Desc => "DESC",
				};
				let nulls = match f.nulls {
					NullsOrder::First => "NULLS FIRST",
					NullsOrder::Last => "NULLS LAST",
				};
				format!("{} {} {}", f.expr.to_sql(), dir, nulls)
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("order_by".to_string(), order_str)]
	}

	fn required_context(&self) -> ContextLevel {
		// Sort needs Database for expression evaluation
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Combine input's access mode with all ORDER BY expressions
		let expr_mode = self.order_by.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let input_stream = self.input.execute(ctx)?;
		let order_by = self.order_by.clone();
		let ctx = ctx.clone();

		// Sort requires collecting all input first, then sorting, then emitting
		let sorted_stream = futures::stream::once(async move {
			// Collect all values from input
			let mut all_values: Vec<Value> = Vec::new();
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				match batch_result {
					Ok(batch) => all_values.extend(batch.values),
					Err(e) => return Err(e),
				}
			}

			if all_values.is_empty() {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Pre-compute sort keys for each value
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let mut keyed: Vec<(Vec<Value>, Value)> = Vec::with_capacity(all_values.len());

			for value in all_values {
				let row_ctx = eval_ctx.clone().with_value(&value);
				let mut keys = Vec::with_capacity(order_by.len());

				for field in &order_by {
					let key = field.expr.evaluate(row_ctx.clone()).await.map_err(|e| {
						crate::expr::ControlFlow::Err(anyhow::anyhow!(
							"Sort key evaluation error: {}",
							e
						))
					})?;
					keys.push(key);
				}

				keyed.push((keys, value));
			}

			// Sort by keys
			keyed.sort_by(|(keys_a, _), (keys_b, _)| {
				for (i, field) in order_by.iter().enumerate() {
					let a = &keys_a[i];
					let b = &keys_b[i];

					let ordering = compare_values(a, b, field.nulls);
					let ordering = match field.direction {
						SortDirection::Asc => ordering,
						SortDirection::Desc => ordering.reverse(),
					};

					if ordering != Ordering::Equal {
						return ordering;
					}
				}
				Ordering::Equal
			});

			// Extract sorted values and emit as a single batch
			let sorted: Vec<Value> = keyed.into_iter().map(|(_, v)| v).collect();
			Ok(ValueBatch {
				values: sorted,
			})
		});

		// Filter out empty batches
		let filtered = sorted_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(Box::pin(filtered))
	}
}

/// Compare two values for sorting, handling nulls according to the specified order
fn compare_values(a: &Value, b: &Value, nulls: NullsOrder) -> Ordering {
	let a_is_null = matches!(a, Value::None | Value::Null);
	let b_is_null = matches!(b, Value::None | Value::Null);

	match (a_is_null, b_is_null) {
		(true, true) => Ordering::Equal,
		(true, false) => match nulls {
			NullsOrder::First => Ordering::Less,
			NullsOrder::Last => Ordering::Greater,
		},
		(false, true) => match nulls {
			NullsOrder::First => Ordering::Greater,
			NullsOrder::Last => Ordering::Less,
		},
		(false, false) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
	}
}
