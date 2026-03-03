//! SortTopK operator - heap-based top-k selection for ORDER BY + LIMIT.
//!
//! This module provides two TopK implementations:
//!
//! - [`SortTopK`]: Expression-evaluation variant that pre-computes sort keys. Used by the
//!   non-consolidated sort path (`plan_sort`).
//! - [`SortTopKByKey`]: Field-path extraction variant that compares values inline. Used by the
//!   consolidated sort path (`plan_sort_consolidated`). This is the preferred variant because it
//!   avoids pre-computing sort keys for every row and instead extracts fields only during
//!   comparison -- matching the old executor's `MemoryOrderedLimit` strategy.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::common::{OrderByField, SortDirection, SortKey, compare_keys, compare_records_by_keys};
use crate::exec::{
	AccessMode, CardinalityHint, CombineAccessModes, ContextLevel, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
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
	/// Insertion sequence number for stable sorting — earlier entries win ties.
	seq: u64,
}

impl PartialEq for KeyedValue {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
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
		//
		// The seq tiebreaker ensures stability: for equal keys, earlier entries
		// (lower seq) are considered "better" and remain in the heap.
		compare_keys(&other.keys, &self.keys, &self.order_by).then_with(|| other.seq.cmp(&self.seq))
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
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) order_by: Vec<OrderByField>,
	/// The effective limit (start + limit from query)
	pub(crate) limit: usize,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SortTopK {
	/// Create a new SortTopK operator.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		order_by: Vec<OrderByField>,
		limit: usize,
	) -> Self {
		Self {
			input,
			order_by,
			limit,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SortTopK {
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
		// Combine order-by expression contexts with child operator context
		let order_ctx = self
			.order_by
			.iter()
			.map(|f| f.expr.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		order_ctx.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		let expr_mode = self.order_by.iter().map(|f| f.expr.access_mode()).combine_all();
		self.input.access_mode().combine(expr_mode)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(self.limit)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.order_by.iter().map(|f| ("order_by", &f.expr)).collect()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		use crate::exec::ordering::SortProperty;
		crate::exec::OutputOrdering::Sorted(
			self.order_by
				.iter()
				.map(|f| {
					let sql = f.expr.to_sql();
					let path = crate::exec::field_path::FieldPath::field(sql);
					SortProperty {
						path,
						direction: f.direction,
						collate: f.collate,
						numeric: f.numeric,
					}
				})
				.collect(),
		)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let order_by = Arc::new(self.order_by.clone());
		let limit = self.limit;
		let ctx = ctx.clone();

		let sorted_stream = futures::stream::once(async move {
			// Use a min-heap to track the top-k values
			// We use Reverse to turn BinaryHeap's max-heap into a min-heap
			let mut heap: BinaryHeap<Reverse<KeyedValue>> = BinaryHeap::with_capacity(limit + 1);
			let mut seq: u64 = 0;

			let eval_ctx = EvalContext::from_exec_ctx(&ctx);

			// Process all input values
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if ctx.cancellation().is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				// Batch evaluate sort key expressions per-field
				let num_fields = order_by.len();
				let mut key_columns: Vec<Vec<Value>> = Vec::with_capacity(num_fields);
				for field in order_by.iter() {
					let keys = field.expr.evaluate_batch(eval_ctx.clone(), &batch.values).await?;
					key_columns.push(keys);
				}

				// Transpose column-oriented keys to per-row, then insert into heap
				let mut key_iters: Vec<std::vec::IntoIter<Value>> =
					key_columns.into_iter().map(|col| col.into_iter()).collect();

				for value in batch.values {
					let keys: Vec<Value> = key_iters
						.iter_mut()
						.map(|iter| iter.next().expect("key column length matches batch size"))
						.collect();

					let keyed = KeyedValue {
						keys,
						value,
						order_by: order_by.clone(),
						seq,
					};
					seq += 1;

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

		Ok(monitor_stream(Box::pin(filtered), "SortTopK", &self.metrics))
	}
}

// ============================================================================
// SortTopKByKey - Field-path extraction variant (consolidated approach)
// ============================================================================

/// A heap entry that stores the value and a shared reference to the sort keys.
///
/// Uses `Arc<Vec<SortKey>>` per entry so that `BinaryHeap` can use the `Ord`
/// trait for comparison. The Arc clone is ~1ns and this mirrors the proven
/// pattern from the old executor's `MemoryOrderedLimit` (`Arc<OrderList>`).
struct TopKByKeyEntry {
	/// The original record value.
	value: Value,
	/// Shared reference to the sort key specification.
	sort_keys: Arc<Vec<SortKey>>,
	/// Insertion sequence number for stable sorting — earlier entries win ties.
	seq: u64,
}

impl PartialEq for TopKByKeyEntry {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for TopKByKeyEntry {}

impl PartialOrd for TopKByKeyEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for TopKByKeyEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		// Reversed: we want a min-heap of the "worst" values so that pop()
		// removes the worst, keeping the best k values in the heap.
		//
		// The seq tiebreaker ensures stability: for equal keys, earlier entries
		// (lower seq) are considered "better" and remain in the heap.
		compare_records_by_keys(&other.value, &self.value, &self.sort_keys)
			.then_with(|| other.seq.cmp(&self.seq))
	}
}

/// Heap-based top-k selection using field-path extraction for comparison.
///
/// This is the consolidated-sort counterpart of [`SortTopK`]. Instead of
/// pre-computing sort keys for every row through expression evaluation, it
/// extracts field values inline during comparison via [`FieldPath::extract`].
///
/// This is significantly faster for ORDER BY + small LIMIT because:
/// - No per-row `Vec<Value>` allocation for sort keys
/// - No expression evaluation for rows that will be immediately rejected
/// - Field extraction is a cheap O(1) path lookup on the `Value` object
///
/// Use when `limit <= MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE` (default 1000).
#[derive(Debug, Clone)]
pub struct SortTopKByKey {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) sort_keys: Vec<SortKey>,
	/// The effective limit (start + limit from query)
	pub(crate) limit: usize,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SortTopKByKey {
	/// Create a new SortTopKByKey operator.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, sort_keys: Vec<SortKey>, limit: usize) -> Self {
		Self {
			input,
			sort_keys,
			limit,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SortTopKByKey {
	fn name(&self) -> &'static str {
		"SortTopKByKey"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let order_str = self
			.sort_keys
			.iter()
			.map(|k| {
				let dir = match k.direction {
					SortDirection::Asc => "ASC",
					SortDirection::Desc => "DESC",
				};
				format!("{} {}", k.path, dir)
			})
			.collect::<Vec<_>>()
			.join(", ");
		vec![("sort_keys".to_string(), order_str), ("limit".to_string(), self.limit.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		// SortTopKByKey does not evaluate expressions; it only extracts fields.
		// Inherit the child's requirement.
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		// Pure comparison — inherits input's access mode.
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(self.limit)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		use crate::exec::ordering::SortProperty;
		crate::exec::OutputOrdering::Sorted(
			self.sort_keys
				.iter()
				.map(|k| SortProperty {
					path: k.path.clone(),
					direction: k.direction,
					collate: k.collate,
					numeric: k.numeric,
				})
				.collect(),
		)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let sort_keys = Arc::new(self.sort_keys.clone());
		let limit = self.limit;
		let cancellation = ctx.cancellation().clone();

		let sorted_stream = futures::stream::once(async move {
			let mut heap: BinaryHeap<Reverse<TopKByKeyEntry>> =
				BinaryHeap::with_capacity(limit + 1);
			let mut seq: u64 = 0;

			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				if cancellation.is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				for value in batch.values {
					if heap.len() >= limit {
						// Heap is full — compare inline before allocating anything.
						if let Some(worst) = heap.peek() {
							let cmp = compare_records_by_keys(&value, &worst.0.value, &sort_keys);
							if cmp == Ordering::Less {
								heap.push(Reverse(TopKByKeyEntry {
									value,
									sort_keys: sort_keys.clone(),
									seq,
								}));
								seq += 1;
								heap.pop();
							}
							// Otherwise skip — the value is worse than everything
							// already in the heap.
						}
					} else {
						// Heap not full yet — always push.
						heap.push(Reverse(TopKByKeyEntry {
							value,
							sort_keys: sort_keys.clone(),
							seq,
						}));
						seq += 1;
					}
				}
			}

			// Extract sorted values from heap (pop gives worst-first, so reverse).
			let mut sorted: Vec<Value> = Vec::with_capacity(heap.len());
			while let Some(Reverse(entry)) = heap.pop() {
				sorted.push(entry.value);
			}
			sorted.reverse();

			Ok(ValueBatch {
				values: sorted,
			})
		});

		let filtered = sorted_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "SortTopKByKey", &self.metrics))
	}
}
