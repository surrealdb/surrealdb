//! SortTopK operator - heap-based top-k selection for ORDER BY + LIMIT.
//!
//! This operator is optimized for queries with ORDER BY and a small LIMIT.
//! Instead of sorting all values, it maintains a heap of the top-k values,
//! which is more efficient when k << n.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::common::{OrderByField, SortDirection, compare_keys};
use crate::err::Error;
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecutionContext, OperatorPlan,
	ValueBatch, ValueBatchStream,
};
use crate::val::Value;

/// A value with pre-computed sort keys for heap comparison.
struct KeyedValue {
	/// Pre-computed sort keys
	keys: Vec<Value>,
	/// The original value
	value: Value,
	/// Reference to the order-by specification for comparison
	order_by: Arc<Vec<OrderByField>>,
}

impl PartialEq for KeyedValue {
	fn eq(&self, other: &Self) -> bool {
		self.keys == other.keys
	}
}

impl Eq for KeyedValue {}

impl PartialOrd for KeyedValue {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for KeyedValue {
	fn cmp(&self, other: &Self) -> Ordering {
		// Note: We compare other to self (reversed) because we want a min-heap
		// of the "worst" values in the top-k. When we pop, we get the worst,
		// allowing us to keep the best k values.
		compare_keys(&other.keys, &self.keys, &self.order_by)
	}
}

/// Selects the top-k values from the input stream using a heap.
///
/// This is more efficient than full sorting when the limit is small relative
/// to the total number of values. It maintains a heap of size `limit` and
/// only keeps track of the top-k values.
///
/// Use this operator when `limit <= MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE` (default 1000).
#[derive(Debug, Clone)]
pub struct SortTopK {
	pub(crate) input: Arc<dyn OperatorPlan>,
	pub(crate) order_by: Vec<OrderByField>,
	/// The effective limit (start + limit from query)
	pub(crate) limit: usize,
}

#[async_trait]
impl OperatorPlan for SortTopK {
	fn name(&self) -> &'static str {
		"SortTopK"
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
				format!("{} {}", f.expr.to_sql(), dir)
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("order_by".to_string(), order_str), ("limit".to_string(), self.limit.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		let expr_mode = self.order_by.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		let input_stream = self.input.execute(ctx)?;
		let order_by = Arc::new(self.order_by.clone());
		let limit = self.limit;
		let ctx = ctx.clone();

		let sorted_stream = futures::stream::once(async move {
			// Use a min-heap to track the top-k values
			// We use Reverse to turn BinaryHeap's max-heap into a min-heap
			let mut heap: BinaryHeap<Reverse<KeyedValue>> = BinaryHeap::with_capacity(limit + 1);

			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			// Process all input values
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				for value in batch.values {
					// Pre-compute sort keys
					let row_ctx = eval_ctx.clone().with_value(&value);
					let mut keys = Vec::with_capacity(order_by.len());

					for field in order_by.iter() {
						let key = field.expr.evaluate(row_ctx.clone()).await.map_err(|e| {
							crate::expr::ControlFlow::Err(anyhow::anyhow!(
								"Sort key evaluation error: {}",
								e
							))
						})?;
						keys.push(key);
					}

					let keyed = KeyedValue {
						keys,
						value,
						order_by: order_by.clone(),
					};

					if heap.len() >= limit {
						// Heap is full - only add if better than the worst in heap
						if let Some(worst) = heap.peek() {
							// Compare new value against worst in heap
							let cmp = compare_keys(&keyed.keys, &worst.0.keys, &order_by);
							if cmp == Ordering::Less {
								// New value is better, push and pop the worst
								heap.push(Reverse(keyed));
								heap.pop();
							}
							// Otherwise, skip this value
						}
					} else {
						// Heap not full, always push
						heap.push(Reverse(keyed));
					}
				}
			}

			// Extract sorted values from heap
			// Pop gives us worst-first, so we need to reverse
			let mut sorted: Vec<Value> = Vec::with_capacity(heap.len());
			while let Some(Reverse(keyed)) = heap.pop() {
				sorted.push(keyed.value);
			}
			sorted.reverse();

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
