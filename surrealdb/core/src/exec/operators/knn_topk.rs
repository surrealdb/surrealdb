//! The bounded heap and vector extraction of brute-force nearest-neighbor
//! search.
//!
//! When no HNSW index is available, a KNN query (`field <|k, EUCLIDEAN|> vec`)
//! scores every record and keeps the `k` nearest. [`KnnTopKHeap`] is the
//! bounded max-heap that holds the candidates, and [`extract_vector`] reads the
//! vector a record holds at the searched field.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::val::field_path::FieldPath;
use crate::val::{Number, Value};

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

/// Extract a numeric vector from a record value at the given field path.
///
/// Returns `None` if the field is missing, None/Null, not an array,
/// or contains non-numeric elements. Shared with the KNN prefilter's exact
/// tier, which scores allow-list records outside the graph.
pub(crate) fn extract_vector(value: &Value, field: &FieldPath) -> Option<Vec<Number>> {
	match field.extract(value).as_ref() {
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
