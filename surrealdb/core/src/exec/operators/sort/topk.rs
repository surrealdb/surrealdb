//! SortTopK operator - heap-based top-k selection for ORDER BY + LIMIT.
//!
//! This module provides two TopK implementations:
//!
//! - [`SortTopK`]: Expression-evaluation variant that pre-computes sort keys. Used by the
//!   non-consolidated sort path (`plan_sort`).
//! - [`SortTopKByKey`]: Field-path extraction variant used by the consolidated sort path
//!   (`plan_sort_consolidated`). This is the preferred variant because it extracts each row's keys
//!   once as cheap borrowed views (no expression evaluation), materialising them only for rows that
//!   actually enter the heap, where they are cached for all later comparisons.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::Arc;

use futures::StreamExt;

use super::common::{OrderByField, SortDirection, SortKey, compare_keys, compare_keys_by_sort_key};
use crate::exec::topk_pushdown::TopKThresholdCell;
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
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.operator_buffer_size,
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
						order_by: Arc::clone(&order_by),
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

/// A heap entry that stores the value with its pre-extracted sort keys.
///
/// Keys are extracted exactly once when the entry is created, so heap
/// comparisons are pure value comparisons — no repeated `FieldPath::extract`
/// walks against the record (the heap's worst entry is compared against
/// every candidate, so re-extracting it per candidate is O(n) extra walks).
///
/// Uses `Arc<Vec<SortKey>>` per entry so that `BinaryHeap` can use the `Ord`
/// trait for comparison. The Arc clone is ~1ns and this mirrors the proven
/// pattern from the old executor's `MemoryOrderedLimit` (`Arc<OrderList>`).
struct TopKByKeyEntry {
	/// Sort keys extracted from the value, one per `SortKey`.
	keys: Vec<Value>,
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
		compare_keys_by_sort_key(&other.keys, &self.keys, &self.sort_keys)
			.then_with(|| other.seq.cmp(&self.seq))
	}
}

/// Bounded "keep the best k" heap with an optional TopK threshold publisher.
///
/// Factored out of [`SortTopKByKey::execute`] so the admission and publish
/// logic is unit-testable without an [`ExecutionContext`]. Admission requires
/// a strict [`Ordering::Less`] against the current worst entry — equal keys
/// never displace earlier rows (insertion-stable via `seq`), which is the
/// invariant the scan-side threshold probe's tie rejection relies on.
struct TopKByKeyAccumulator {
	heap: BinaryHeap<Reverse<TopKByKeyEntry>>,
	sort_keys: Arc<Vec<SortKey>>,
	limit: usize,
	seq: u64,
	/// TopK threshold pushdown publish side (see
	/// [`crate::exec::topk_pushdown`]). The heap-worst's first sort key is
	/// already cached on its entry, so publishing is a single clone — no
	/// re-extraction. `None` (the common case) makes every publish site a
	/// no-op.
	threshold_cell: Option<Arc<TopKThresholdCell>>,
}

impl TopKByKeyAccumulator {
	fn new(
		sort_keys: Arc<Vec<SortKey>>,
		limit: usize,
		threshold_cell: Option<Arc<TopKThresholdCell>>,
	) -> Self {
		Self {
			heap: BinaryHeap::with_capacity(limit + 1),
			sort_keys,
			limit,
			seq: 0,
			threshold_cell,
		}
	}

	/// Publish the heap's worst entry's first sort key as the scan-side
	/// rejection threshold. Only called when the heap is full, so the scan
	/// never rejects against a threshold the heap could still admit
	/// unconditionally.
	fn publish_threshold(&self) {
		if let Some(cell) = self.threshold_cell.as_ref()
			&& let Some(worst) = self.heap.peek()
			&& let Some(first_key) = worst.0.keys.first()
		{
			cell.publish(first_key.clone());
		}
	}

	fn insert(&mut self, value: Value) {
		// Extract this row's sort keys once, as borrowed views — no clone
		// unless the row is actually kept.
		let keys: Vec<std::borrow::Cow<Value>> =
			self.sort_keys.iter().map(|k| k.path.extract(&value)).collect();

		if self.heap.len() >= self.limit {
			// Heap is full — compare against the worst entry's cached keys
			// before materialising anything.
			if let Some(worst) = self.heap.peek() {
				let cmp = compare_keys_by_sort_key(&keys, &worst.0.keys, &self.sort_keys);
				if cmp == Ordering::Less {
					let keys: Vec<Value> = keys.into_iter().map(|k| k.into_owned()).collect();
					self.heap.push(Reverse(TopKByKeyEntry {
						keys,
						value,
						sort_keys: Arc::clone(&self.sort_keys),
						seq: self.seq,
					}));
					self.seq += 1;
					self.heap.pop();
					// The admission replaced the worst entry with a strictly
					// better one — the threshold tightened.
					self.publish_threshold();
				}
				// Otherwise skip — the value is worse than everything
				// already in the heap.
			}
		} else {
			// Heap not full yet — always push.
			let keys: Vec<Value> = keys.into_iter().map(|k| k.into_owned()).collect();
			self.heap.push(Reverse(TopKByKeyEntry {
				keys,
				value,
				sort_keys: Arc::clone(&self.sort_keys),
				seq: self.seq,
			}));
			self.seq += 1;
			if self.heap.len() == self.limit {
				// Fill transition: the first complete top-K exists, so a
				// rejection threshold is now meaningful.
				self.publish_threshold();
			}
		}
	}

	/// Extract sorted values from the heap (pop gives worst-first, so reverse).
	fn into_sorted(mut self) -> Vec<Value> {
		let mut sorted: Vec<Value> = Vec::with_capacity(self.heap.len());
		while let Some(Reverse(entry)) = self.heap.pop() {
			sorted.push(entry.value);
		}
		sorted.reverse();
		sorted
	}
}

/// Heap-based top-k selection using field-path extraction.
///
/// This is the consolidated-sort counterpart of [`SortTopK`]. Instead of
/// evaluating order-by expressions, it extracts each row's sort keys via
/// [`FieldPath::extract`] — once per row, as borrowed views. A rejected row
/// (worse than everything in the full heap) never has its keys cloned; keys
/// are materialised only when a row actually enters the heap, where they are
/// cached so heap comparisons never re-extract from the record.
///
/// Use when `limit <= MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE` (default 1000).
#[derive(Debug, Clone)]
pub struct SortTopKByKey {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) sort_keys: Vec<SortKey>,
	/// The effective limit (start + limit from query)
	pub(crate) limit: usize,
	/// TopK threshold pushdown publish side (see [`crate::exec::topk_pushdown`]).
	///
	/// When installed by the planner, the heap publishes its worst entry's
	/// **first** sort key whenever the heap is full — on the fill transition
	/// and after every admission — so the upstream KV scan can reject rows
	/// that cannot beat it without decoding them. Publishing only ever
	/// tightens the threshold: admissions replace the worst entry with a
	/// strictly better one, so the new worst is at least as good as the old.
	pub(crate) threshold_cell: Option<Arc<TopKThresholdCell>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SortTopKByKey {
	/// Create a new SortTopKByKey operator.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, sort_keys: Vec<SortKey>, limit: usize) -> Self {
		Self {
			input,
			sort_keys,
			limit,
			threshold_cell: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Install the TopK threshold pushdown publish side. Only the planner's
	/// `plan_sort_consolidated` calls this, after verifying the built
	/// `sort_keys` match the probe the scan was compiled against.
	pub(crate) fn with_threshold_cell(mut self, cell: Arc<TopKThresholdCell>) -> Self {
		self.threshold_cell = Some(cell);
		self
	}
}
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
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.operator_buffer_size,
		);
		let sort_keys = Arc::new(self.sort_keys.clone());
		let limit = self.limit;
		let cancellation = ctx.cancellation().clone();
		let threshold_cell = self.threshold_cell.clone();

		let sorted_stream = futures::stream::once(async move {
			let mut acc = TopKByKeyAccumulator::new(sort_keys, limit, threshold_cell);

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
					acc.insert(value);
				}
			}

			Ok(ValueBatch {
				values: acc.into_sorted(),
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::field_path::FieldPath;
	use crate::val::{Number, Object};

	fn row(n: i64) -> Value {
		Value::Object(Object::from(BTreeMap::from([(
			Strand::from("a"),
			Value::Number(Number::Int(n)),
		)])))
	}

	fn desc_keys() -> Arc<Vec<SortKey>> {
		let mut key = SortKey::new(FieldPath::field("a"));
		key.direction = SortDirection::Desc;
		Arc::new(vec![key])
	}

	fn acc_with_cell(limit: usize) -> (TopKByKeyAccumulator, Arc<TopKThresholdCell>) {
		let cell = Arc::new(TopKThresholdCell::default());
		let acc = TopKByKeyAccumulator::new(desc_keys(), limit, Some(Arc::clone(&cell)));
		(acc, cell)
	}

	#[test]
	fn publishes_on_fill_transition_only() {
		let (mut acc, cell) = acc_with_cell(2);
		acc.insert(row(10));
		assert!(cell.snapshot().is_none(), "no threshold before the heap fills");
		acc.insert(row(20));
		// Heap full: the worst of {10, 20} under DESC is 10.
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(10))));
	}

	#[test]
	fn tightens_on_admission_and_ignores_rejects() {
		let (mut acc, cell) = acc_with_cell(2);
		acc.insert(row(10));
		acc.insert(row(20));
		// 5 is worse than the worst (10) under DESC — rejected, threshold unchanged.
		acc.insert(row(5));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(10))));
		// 30 displaces 10 — the new worst (and threshold) is 20.
		acc.insert(row(30));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(20))));
	}

	#[test]
	fn equal_keys_do_not_displace_or_tighten() {
		let (mut acc, cell) = acc_with_cell(2);
		acc.insert(row(10));
		acc.insert(row(20));
		// Ties with the worst entry are not admitted (insertion-stable), so
		// the threshold stays put — matching the scan probe's tie rejection.
		acc.insert(row(10));
		assert_eq!(cell.snapshot().as_deref(), Some(&Value::Number(Number::Int(10))));
		assert_eq!(acc.into_sorted(), vec![row(20), row(10)]);
	}

	#[test]
	fn never_publishes_when_heap_never_fills() {
		let (mut acc, cell) = acc_with_cell(5);
		acc.insert(row(1));
		acc.insert(row(2));
		assert!(cell.snapshot().is_none());
		assert_eq!(acc.into_sorted(), vec![row(2), row(1)]);
	}

	#[test]
	fn zero_limit_never_publishes() {
		let (mut acc, cell) = acc_with_cell(0);
		acc.insert(row(1));
		assert!(cell.snapshot().is_none());
		assert!(acc.into_sorted().is_empty());
	}

	#[test]
	fn output_identical_with_and_without_publisher() {
		let values = [5i64, 3, 9, 1, 7, 9, 2, 8];
		let (mut with_cell, _cell) = acc_with_cell(3);
		let mut without_cell = TopKByKeyAccumulator::new(desc_keys(), 3, None);
		for v in values {
			with_cell.insert(row(v));
			without_cell.insert(row(v));
		}
		assert_eq!(with_cell.into_sorted(), without_cell.into_sorted());
	}
}
