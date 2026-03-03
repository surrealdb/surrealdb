//! KnnTopK operator - brute-force nearest-neighbor search as a pipeline-breaking
//! aggregate.
//!
//! When no HNSW index is available, KNN queries (`field <|k, EUCLIDEAN|> vec`)
//! must scan all records, compute distances, and return the top-K nearest.
//!
//! This operator consumes its entire input stream (pipeline-breaking, like
//! `Sort`), maintains a bounded min-heap of size `k`, and emits the top-K
//! records ordered by ascending distance.
//!
//! Pipeline shape:
//! ```text
//! TableScan -> KnnTopK(field, query_vector, k, distance_fn) -> Filter -> Project
//! ```

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use surrealdb_types::ToSql;

use crate::catalog::Distance;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::Idiom;
use crate::val::{Number, Value};

/// A heap entry storing a record with its computed distance.
///
/// Uses `Reverse` wrapping + reversed `Ord` so that `BinaryHeap` acts as a
/// min-heap of the **worst** (farthest) distances, matching the `SortTopK`
/// pattern. When the heap is full, the worst entry is evicted when a closer
/// record arrives.
struct DistanceEntry {
	/// Computed distance from the query vector (sort key).
	distance: Number,
	/// The full record value.
	value: Value,
	/// Insertion sequence number for stable tie-breaking.
	seq: u64,
}

impl PartialEq for DistanceEntry {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl Eq for DistanceEntry {}

impl PartialOrd for DistanceEntry {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for DistanceEntry {
	fn cmp(&self, other: &Self) -> Ordering {
		// Reversed: we want a min-heap of the worst (farthest) distances.
		// `BinaryHeap::peek()` returns the largest element, so by reversing
		// comparison we ensure the farthest record is at the top and gets
		// evicted first.  Ties are broken by insertion order (stability).
		other
			.distance
			.partial_cmp(&self.distance)
			.unwrap_or(Ordering::Equal)
			.then_with(|| other.seq.cmp(&self.seq))
	}
}

/// Brute-force KNN operator: scans all input, computes vector distances,
/// and returns the top-K nearest records.
///
/// This is a **pipeline-breaking** operator. It must consume the entire input
/// stream before producing any output.
#[derive(Debug)]
pub struct KnnTopK {
	/// Child operator providing the input stream (typically a table scan).
	pub(crate) input: Arc<dyn ExecOperator>,
	/// Idiom path to the vector field on each record (e.g., `embedding`).
	pub(crate) field: Idiom,
	/// The query vector to compute distances against.
	pub(crate) query_vector: Vec<Number>,
	/// Number of nearest neighbors to return.
	pub(crate) k: usize,
	/// Distance metric to use for computing distances.
	pub(crate) distance: Distance,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// KNN distance context, shared with IndexFunctionExec for vector::distance::knn().
	/// Populated after computing top-K results so that downstream projection
	/// evaluation can look up per-record distances.
	pub(crate) knn_context: Option<Arc<crate::exec::function::KnnContext>>,
}

impl KnnTopK {
	/// Create a new KnnTopK operator.
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		field: Idiom,
		query_vector: Vec<Number>,
		k: usize,
		distance: Distance,
	) -> Self {
		Self {
			input,
			field,
			query_vector,
			k,
			distance,
			metrics: Arc::new(OperatorMetrics::new()),
			knn_context: None,
		}
	}

	/// Set the KNN context for distance propagation.
	pub(crate) fn with_knn_context(
		mut self,
		knn_context: Option<Arc<crate::exec::function::KnnContext>>,
	) -> Self {
		self.knn_context = knn_context;
		self
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for KnnTopK {
	fn name(&self) -> &'static str {
		"KnnTopK"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("field".to_string(), self.field.to_sql()),
			("k".to_string(), self.k.to_string()),
			("distance".to_string(), format!("{:?}", self.distance)),
			("dimension".to_string(), self.query_vector.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(self.k)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let field = self.field.clone();
		let query_vector = self.query_vector.clone();
		let k = self.k;
		let distance = self.distance.clone();
		let cancellation = ctx.cancellation().clone();
		let knn_context = self.knn_context.clone();

		let result_stream = futures::stream::once(async move {
			let mut heap: BinaryHeap<std::cmp::Reverse<DistanceEntry>> =
				BinaryHeap::with_capacity(k + 1);
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
					// Extract the vector field from the record
					let record_vec = match extract_vector(&value, &field) {
						Some(v) => v,
						None => continue, // Skip records without a valid vector field
					};

					// Compute the distance
					let dist = match distance.compute(&record_vec, &query_vector) {
						Ok(d) => d,
						Err(_) => continue, // Skip on dimension mismatch etc.
					};

					let entry = DistanceEntry {
						distance: dist,
						value,
						seq,
					};
					seq += 1;

					if heap.len() >= k {
						// Heap is full -- only insert if closer than the farthest
						if let Some(worst) = heap.peek()
							&& entry.distance < worst.0.distance
						{
							heap.push(std::cmp::Reverse(entry));
							heap.pop();
						}
					} else {
						heap.push(std::cmp::Reverse(entry));
					}
				}
			}

			// Extract results ordered by distance (nearest first).
			// Pop yields farthest-first, so reverse after collecting.
			let mut entries: Vec<DistanceEntry> = Vec::with_capacity(heap.len());
			while let Some(std::cmp::Reverse(entry)) = heap.pop() {
				entries.push(entry);
			}
			entries.reverse();

			// Populate KNN distance context (if present) before yielding
			// records. This makes distances available to
			// vector::distance::knn() during downstream projection evaluation.
			if let Some(ref knn_ctx) = knn_context {
				for entry in &entries {
					if let Value::Object(ref obj) = entry.value
						&& let Some(Value::RecordId(rid)) = obj.get("id")
					{
						knn_ctx.insert(rid.clone(), entry.distance).await;
					}
				}
			}

			let sorted: Vec<Value> = entries.into_iter().map(|e| e.value).collect();

			Ok(ValueBatch {
				values: sorted,
			})
		});

		// Filter out empty batches
		let filtered = result_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "KnnTopK", &self.metrics))
	}
}

/// Extract a numeric vector from a record value at the given idiom path.
///
/// Returns `None` if the field is missing, None/Null, not an array,
/// or contains non-numeric elements.
fn extract_vector(value: &Value, field: &Idiom) -> Option<Vec<Number>> {
	match value.pick(field) {
		Value::Array(arr) if !arr.is_empty() => {
			let mut nums = Vec::with_capacity(arr.len());
			for v in arr.iter() {
				match v {
					Value::Number(n) => nums.push(*n),
					_ => return None,
				}
			}
			Some(nums)
		}
		_ => None,
	}
}
