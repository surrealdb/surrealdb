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

use futures::StreamExt;
use surrealdb_types::ToSql;

use crate::catalog::Distance;
use crate::err::EngineError;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::Idiom;
use crate::val::{Number, Value};

/// The query vector of a brute-force KNN, as planned.
///
/// `Deferred` carries a non-literal right-hand side (bind parameter, function
/// call, array with computed elements) that the legacy planner would compute
/// once at plan time with no document context. The streaming equivalent
/// evaluates it once at stream open and coerces to `array<number>` with the
/// same coercion the legacy tree applies, so error messages match.
#[derive(Clone, Debug)]
pub(crate) enum KnnVectorSource {
	/// Plan-time literal vector.
	Literal(Vec<Number>),
	/// Expression evaluated once at stream open (statement scope, no row).
	Deferred(Arc<dyn PhysicalExpr>),
}

/// A heap entry storing an item with its computed distance.
///
/// Uses `Reverse` wrapping + reversed `Ord` so that `BinaryHeap` acts as a
/// min-heap of the **worst** (farthest) distances, matching the `SortTopK`
/// pattern. When the heap is full, the worst entry is evicted when a closer
/// item arrives.
struct DistanceEntry<T> {
	/// Computed distance from the query vector (sort key).
	distance: Number,
	/// The carried item (a full record value, a record id, ...).
	item: T,
	/// Insertion sequence number for stable tie-breaking.
	seq: u64,
}

impl<T> PartialEq for DistanceEntry<T> {
	fn eq(&self, other: &Self) -> bool {
		self.cmp(other) == Ordering::Equal
	}
}

impl<T> Eq for DistanceEntry<T> {}

impl<T> PartialOrd for DistanceEntry<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<T> Ord for DistanceEntry<T> {
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

/// Bounded top-K accumulator ordered by ascending distance, shared by the
/// brute-force [`KnnTopK`] operator and the KNN prefilter's graph-free exact
/// tier (#548).
///
/// Keeps the k nearest offered items; ties are broken by insertion order
/// (stability).
pub(crate) struct KnnTopKHeap<T> {
	k: usize,
	heap: BinaryHeap<std::cmp::Reverse<DistanceEntry<T>>>,
	seq: u64,
}

impl<T> KnnTopKHeap<T> {
	pub(crate) fn new(k: usize) -> Self {
		Self {
			k,
			heap: BinaryHeap::with_capacity(k + 1),
			seq: 0,
		}
	}

	/// Offer one candidate; it is kept only while it ranks among the k
	/// nearest seen so far.
	pub(crate) fn offer(&mut self, distance: Number, item: T) {
		let entry = DistanceEntry {
			distance,
			item,
			seq: self.seq,
		};
		self.seq += 1;
		if self.heap.len() >= self.k {
			// Heap is full -- only insert if closer than the farthest
			if let Some(worst) = self.heap.peek()
				&& entry.distance < worst.0.distance
			{
				self.heap.push(std::cmp::Reverse(entry));
				self.heap.pop();
			}
		} else {
			self.heap.push(std::cmp::Reverse(entry));
		}
	}

	/// Consume the heap, returning `(distance, item)` pairs nearest-first.
	pub(crate) fn into_sorted_nearest_first(mut self) -> Vec<(Number, T)> {
		// Pop yields farthest-first, so reverse after collecting.
		let mut entries: Vec<DistanceEntry<T>> = Vec::with_capacity(self.heap.len());
		while let Some(std::cmp::Reverse(entry)) = self.heap.pop() {
			entries.push(entry);
		}
		entries.reverse();
		entries.into_iter().map(|e| (e.distance, e.item)).collect()
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
	pub(crate) query_vector: KnnVectorSource,
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
		query_vector: KnnVectorSource,
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
impl ExecOperator for KnnTopK {
	fn name(&self) -> &'static str {
		"KnnTopK"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let dimension = match &self.query_vector {
			KnnVectorSource::Literal(v) => v.len().to_string(),
			KnnVectorSource::Deferred(_) => "deferred".to_string(),
		};
		vec![
			("field".to_string(), self.field.to_sql()),
			("k".to_string(), self.k.to_string()),
			("distance".to_string(), format!("{:?}", self.distance)),
			("dimension".to_string(), dimension),
		]
	}

	fn required_context(&self) -> ContextLevel {
		let ctx = self.input.required_context();
		match &self.query_vector {
			// A deferred vector expression carries its own requirement
			// (e.g. a function call needs Database level).
			KnnVectorSource::Deferred(expr) => ctx.max(expr.required_context()),
			KnnVectorSource::Literal(_) => ctx,
		}
	}

	fn access_mode(&self) -> AccessMode {
		let mode = self.input.access_mode();
		match &self.query_vector {
			// A deferred vector expression can itself require writes
			// (e.g. a mutating custom function).
			KnnVectorSource::Deferred(expr) => mode.combine(expr.access_mode()),
			KnnVectorSource::Literal(_) => mode,
		}
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
		let mut input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let field = self.field.clone();
		let query_vector = self.query_vector.clone();
		let k = self.k;
		let distance = self.distance.clone();
		let cancellation = ctx.cancellation().clone();
		let knn_context = self.knn_context.clone();
		let exec_ctx = ctx.clone();

		let result_stream = futures::stream::once(async move {
			// Resolve the query vector. Deferred sources evaluate once here,
			// with no row context, and coerce exactly like the legacy tree's
			// plan-time compute of the KNN right-hand side.
			let query_vector: Vec<Number> = match &query_vector {
				KnnVectorSource::Literal(v) => v.clone(),
				KnnVectorSource::Deferred(expr) => {
					let value = expr.evaluate(EvalContext::from_exec_ctx(&exec_ctx)).await?;
					value
						.coerce_to::<Vec<Number>>()
						.map_err(|e| crate::expr::ControlFlow::Err(anyhow::Error::new(e)))?
				}
			};

			let mut heap: KnnTopKHeap<Value> = KnnTopKHeap::new(k);

			while let Some(batch_result) = input_stream.next().await {
				if cancellation.is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						EngineError::QueryCancelled
					)));
				}
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Err(e),
				};

				for value in batch.into_values() {
					// Extract the vector field from the record
					let record_vec = match extract_vector(&value, &field) {
						Some(v) => v,
						None => continue, // Skip records without a valid vector field
					};

					// Compute the distance
					let dist = match crate::idx::trees::vector::distance_compute(
						&distance,
						&record_vec,
						&query_vector,
					) {
						Ok(d) => d,
						Err(_) => continue, // Skip on dimension mismatch etc.
					};

					heap.offer(dist, value);
				}
			}

			// Extract results ordered by distance (nearest first).
			let entries = heap.into_sorted_nearest_first();

			// Populate KNN distance context (if present) before yielding
			// records. This makes distances available to
			// vector::distance::knn() during downstream projection evaluation.
			if let Some(ref knn_ctx) = knn_context {
				for (distance, value) in &entries {
					if let Value::Object(obj) = value
						&& let Some(Value::RecordId(rid)) = obj.get("id")
					{
						knn_ctx.insert(rid.clone(), *distance).await;
					}
				}
			}

			let sorted: Vec<Value> = entries.into_iter().map(|(_, v)| v).collect();

			Ok(ValueBatch::new(sorted))
		});

		// Filter out empty batches
		let filtered = result_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "KnnTopK", &self.metrics))
	}
}

/// Extract a numeric vector from a record value at the given idiom path.
///
/// Returns `None` if the field is missing, None/Null, not an array,
/// or contains non-numeric elements. Shared with the KNN prefilter's exact
/// tier (#548), which scores allow-list records outside the graph.
pub(crate) fn extract_vector(value: &Value, field: &Idiom) -> Option<Vec<Number>> {
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
