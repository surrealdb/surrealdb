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
/// Ordered by distance, then by insertion sequence, so the `BinaryHeap`'s
/// greatest entry is the farthest, and among equally far ones the latest:
/// the entry a closer item evicts when the heap is full.
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
		// `Number` orders totally (NaN included); ties are broken by
		// insertion order (stability).
		self.distance.cmp(&other.distance).then_with(|| self.seq.cmp(&other.seq))
	}
}

/// Bounded top-K accumulator ordered by ascending distance, shared by
/// brute-force KNN and the KNN prefilter's graph-free exact tier.
///
/// Keeps the k nearest offered items; ties are broken by insertion order
/// (stability).
pub(crate) struct KnnTopKHeap<T> {
	k: usize,
	heap: BinaryHeap<DistanceEntry<T>>,
	seq: u64,
}

/// The most entries a [`KnnTopKHeap`] reserves up front. `k` is the user's
/// (up to `u32::MAX`), so reserving all of it would allocate for neighbours
/// that may never be offered; past this the heap grows as entries arrive.
const PREALLOCATED_ENTRIES: usize = 1024;

impl<T> KnnTopKHeap<T> {
	pub(crate) fn new(k: usize) -> Self {
		Self {
			k,
			heap: BinaryHeap::with_capacity(k.min(PREALLOCATED_ENTRIES)),
			seq: 0,
		}
	}

	/// Whether [`Self::offer`] would keep an item at `distance`: the heap is
	/// not full, or `distance` is closer than its farthest entry.
	pub(crate) fn admits(&self, distance: &Number) -> bool {
		self.heap.len() < self.k || self.heap.peek().is_some_and(|worst| *distance < worst.distance)
	}

	/// Offer one candidate; it is kept only while it ranks among the k
	/// nearest seen so far.
	pub(crate) fn offer(&mut self, distance: Number, item: T) {
		// A candidate the heap would turn away takes no sequence number, so
		// the admitted ones keep their relative offer order.
		if !self.admits(&distance) {
			return;
		}
		let entry = DistanceEntry {
			distance,
			item,
			seq: self.seq,
		};
		self.seq += 1;
		if self.heap.len() < self.k {
			self.heap.push(entry);
		} else if let Some(mut worst) = self.heap.peek_mut() {
			// Full, and `admits` proved the entry closer than the farthest:
			// it replaces that one in place, with one sift down when the
			// guard drops.
			*worst = entry;
		}
	}

	/// Consume the heap, yielding `(distance, item)` pairs nearest-first.
	/// The heap is sorted in place; the pairs are not collected again, so a
	/// caller builds its own result from them, sized by the iterator's length.
	pub(crate) fn into_sorted_nearest_first(self) -> impl ExactSizeIterator<Item = (Number, T)> {
		self.heap.into_sorted_vec().into_iter().map(|e| (e.distance, e.item))
	}
}

/// Reads the numeric vector at `field` of a record into `out`, replacing its
/// contents, and returns whether there is one.
///
/// Nested arrays are flattened in order into one vector, as a vector index
/// flattens them when it stores the record, so brute force measures the same
/// vector the index search does. `false` if the field is missing, None/Null,
/// not an array, holds no number, holds anything other than numbers and
/// arrays, or holds more than `limit` numbers: reading stops there, so an
/// oversized value is refused without being read in full.
pub(crate) fn extract_vector(
	value: &Value,
	field: &FieldPath,
	out: &mut Vec<Number>,
	limit: usize,
) -> bool {
	fn flatten(values: &[Value], out: &mut Vec<Number>, limit: usize) -> bool {
		values.iter().all(|v| {
			if let Value::Number(n) = v {
				if out.len() < limit {
					out.push(n.clone());
					true
				} else {
					false
				}
			} else if let Value::Array(a) = v {
				flatten(&a.0, out, limit)
			} else {
				false
			}
		})
	}
	out.clear();
	if let Value::Array(arr) = field.extract(value).as_ref() {
		flatten(&arr.0, out, limit) && !out.is_empty()
	} else {
		false
	}
}

#[cfg(test)]
#[cfg(test)]
mod tests {
	use super::{KnnTopKHeap, extract_vector};
	use crate::val::field_path::FieldPath;
	use crate::val::{Array, Number, Object, Value};

	/// A record's vector is read flat or flattened from nested arrays, in
	/// order; a missing field, a non-array, an array holding anything but
	/// numbers and arrays, and one holding no number at all read as none.
	#[test]
	fn extract_vector_reads_flat_and_nested_arrays() {
		let n = |v: i64| Value::Number(Number::Int(v));
		let a = |v: Vec<Value>| Value::Array(Array(v));
		let record = |point: Option<Value>| {
			let mut o = Object::default();
			if let Some(p) = point {
				o.insert("point", p);
			}
			Value::Object(o)
		};
		let field = FieldPath::field("point");
		let read_at_most = |point: Option<Value>, limit: usize| {
			let mut out = vec![Number::Int(-1)];
			extract_vector(&record(point), &field, &mut out, limit)
				.then(|| out.into_iter().map(|n| n.to_int()).collect::<Vec<_>>())
		};
		let read = |point: Option<Value>| read_at_most(point, usize::MAX);
		assert_eq!(read(Some(a(vec![n(1), n(2), n(3)]))), Some(vec![1, 2, 3]));
		assert_eq!(read(Some(a(vec![a(vec![n(1), n(2)]), a(vec![n(3)])]))), Some(vec![1, 2, 3]));
		assert_eq!(
			read(Some(a(vec![n(1), a(vec![a(vec![n(2)]), n(3)]), n(4)]))),
			Some(vec![1, 2, 3, 4])
		);
		assert_eq!(read(None), None);
		assert_eq!(read(Some(Value::Null)), None);
		assert_eq!(read(Some(n(1))), None);
		assert_eq!(read(Some(a(vec![]))), None);
		assert_eq!(read(Some(a(vec![a(vec![]), a(vec![])]))), None);
		assert_eq!(read(Some(a(vec![n(1), Value::None]))), None);
		assert_eq!(read(Some(a(vec![a(vec![n(1)]), a(vec![Value::Null])]))), None);
		// Reading stops past the limit: a vector of exactly `limit` numbers is
		// read, one with more is refused.
		assert_eq!(read_at_most(Some(a(vec![n(1), a(vec![n(2), n(3)])])), 3), Some(vec![1, 2, 3]));
		assert_eq!(read_at_most(Some(a(vec![n(1), a(vec![n(2), n(3)]), n(4)])), 3), None);
	}

	/// The heap keeps the k nearest offers, nearest first, with equal
	/// distances in the order they were offered, however the farthest are
	/// replaced.
	#[test]
	fn keeps_the_k_nearest_in_offer_order() {
		let distances = [5.0, 1.0, 9.0, 3.0, 1.0, 7.0, 0.5, 3.0, 2.0, 8.0];
		let mut heap = KnnTopKHeap::new(4);
		for (i, d) in distances.iter().enumerate() {
			heap.offer(Number::Float(*d), i);
		}
		// Full, with 2.0 farthest: only a strictly closer distance gets in.
		assert!(heap.admits(&Number::Float(1.5)));
		assert!(!heap.admits(&Number::Float(2.0)));
		assert!(!heap.admits(&Number::Float(4.0)));
		// An offer at the boundary distance is turned away: the entry already
		// there (index 8) keeps its slot.
		heap.offer(Number::Float(2.0), 99);
		let got: Vec<(f64, usize)> =
			heap.into_sorted_nearest_first().map(|(d, i)| (d.to_float(), i)).collect();
		assert_eq!(got, [(0.5, 6), (1.0, 1), (1.0, 4), (2.0, 8)]);
	}

	/// A heap reserves at most `PREALLOCATED_ENTRIES` up front, however large
	/// the user's `k`, and grows past it as entries arrive; a heap of size 0
	/// admits nothing.
	#[test]
	fn reserves_a_bounded_number_of_entries() {
		let mut heap = KnnTopKHeap::new(u32::MAX as usize);
		assert!(heap.heap.capacity() <= super::PREALLOCATED_ENTRIES, "{}", heap.heap.capacity());
		let n = super::PREALLOCATED_ENTRIES * 2 + 1;
		for i in (0..n).rev() {
			heap.offer(Number::Int(i as i64), i);
		}
		let kept: Vec<usize> = heap.into_sorted_nearest_first().map(|(_, i)| i).collect();
		assert_eq!(kept, (0..n).collect::<Vec<_>>());
		let mut empty = KnnTopKHeap::new(0);
		assert!(!empty.admits(&Number::Int(0)));
		empty.offer(Number::Int(0), 0usize);
		assert_eq!(empty.into_sorted_nearest_first().len(), 0);
	}
}
