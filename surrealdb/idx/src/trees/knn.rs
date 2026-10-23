use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use ahash::{HashSet, HashSetExt};
// A doc-ID set is what the vector-to-documents mapping stores, so its shape and
// the variant-compaction rule that keeps it minimal are declared below this layer.
pub(crate) use surrealdb_datastore::values::ids::Ids64;

use crate::trees::dynamicset::DynamicSet;
use crate::trees::hnsw::{ElementId, VectorId};

/// A queue of `(distance, element)` pairs that pops from either end.
///
/// Ordered by distance under `f64::total_cmp`; pairs at an equal distance keep
/// their insertion order, so [`Self::pop_first`] takes the earliest-pushed of
/// the nearest and [`Self::pop_last`] the latest-pushed of the furthest.
///
/// Held as a min-max heap in one `Vec`: even levels hold the minimum of their
/// subtree, odd levels the maximum. A push and either pop are O(log n), both
/// peeks O(1), and nothing allocates beyond the buffer's amortised growth.
#[derive(Default, Debug, Clone)]
pub(super) struct DoublePriorityQueue {
	heap: Vec<QueueEntry>,
	/// The push sequence the next entry takes, which orders equal distances.
	next_seq: u64,
}

#[derive(Debug, Clone, Copy)]
struct QueueEntry {
	dist: f64,
	seq: u64,
	id: ElementId,
}

impl QueueEntry {
	/// The queue order: distance, then push sequence. Sequences are unique, so
	/// no two entries compare equal.
	fn key(&self) -> (FloatKey, u64) {
		(FloatKey(self.dist), self.seq)
	}
}

impl DoublePriorityQueue {
	pub(super) fn from(d: f64, e: ElementId) -> Self {
		let mut q = DoublePriorityQueue::default();
		q.push(d, e);
		q
	}

	pub(super) fn len(&self) -> usize {
		self.heap.len()
	}

	pub(super) fn is_empty(&self) -> bool {
		self.heap.is_empty()
	}

	pub(super) fn push(&mut self, dist: f64, id: ElementId) {
		let seq = self.next_seq;
		self.next_seq += 1;
		self.heap.push(QueueEntry {
			dist,
			seq,
			id,
		});
		self.bubble_up(self.heap.len() - 1);
	}

	pub(super) fn pop_first(&mut self) -> Option<(f64, ElementId)> {
		self.remove(0)
	}

	pub(super) fn pop_last(&mut self) -> Option<(f64, ElementId)> {
		self.remove(self.max_index()?)
	}

	pub(super) fn peek_first(&self) -> Option<(f64, ElementId)> {
		self.heap.first().map(|e| (e.dist, e.id))
	}

	pub(super) fn peek_last_dist(&self) -> Option<f64> {
		self.max_index().map(|i| self.heap[i].dist)
	}

	pub(super) fn to_vec(&self) -> Vec<(f64, ElementId)> {
		self.to_vec_limit(self.heap.len())
	}

	/// The `limit` nearest pairs, nearest first. Only those are sorted: the rest
	/// are partitioned off first.
	pub(super) fn to_vec_limit(&self, limit: usize) -> Vec<(f64, ElementId)> {
		if limit == 0 {
			return Vec::new();
		}
		let mut entries = self.heap.clone();
		if limit < entries.len() {
			entries.select_nth_unstable_by_key(limit - 1, QueueEntry::key);
			entries.truncate(limit);
		}
		entries.sort_unstable_by_key(QueueEntry::key);
		entries.into_iter().map(|e| (e.dist, e.id)).collect()
	}

	/// Every pair, nearest first, consuming the queue so its buffer is sorted in place.
	pub(super) fn into_sorted_vec(self) -> Vec<(f64, ElementId)> {
		let mut entries = self.heap;
		entries.sort_unstable_by_key(QueueEntry::key);
		entries.into_iter().map(|e| (e.dist, e.id)).collect()
	}

	pub(super) fn to_set(&self) -> HashSet<ElementId> {
		let mut s = HashSet::with_capacity(self.heap.len());
		for e in &self.heap {
			s.insert(e.id);
		}
		s
	}

	pub(super) fn to_dynamic_set<S: DynamicSet>(&self, set: &mut S) {
		for e in &self.heap {
			set.insert(e.id);
		}
	}

	/// Whether the entry at `a` orders before the entry at `b`.
	fn less(&self, a: usize, b: usize) -> bool {
		self.heap[a].key() < self.heap[b].key()
	}

	/// Whether position `i` is on a min level (an even depth).
	fn is_min_level(i: usize) -> bool {
		(usize::BITS - 1 - (i + 1).leading_zeros()).is_multiple_of(2)
	}

	/// The position of the furthest entry: the root, or the larger of its children.
	fn max_index(&self) -> Option<usize> {
		match self.heap.len() {
			0 => None,
			1 => Some(0),
			2 => Some(1),
			_ => Some(if self.less(1, 2) {
				2
			} else {
				1
			}),
		}
	}

	/// Removes the entry at `i`, restoring the heap from that position.
	fn remove(&mut self, i: usize) -> Option<(f64, ElementId)> {
		if i >= self.heap.len() {
			return None;
		}
		let e = self.heap.swap_remove(i);
		if i < self.heap.len() {
			self.trickle_down(i);
		}
		Some((e.dist, e.id))
	}

	fn bubble_up(&mut self, i: usize) {
		if i == 0 {
			return;
		}
		let parent = (i - 1) / 2;
		if Self::is_min_level(i) {
			if self.less(parent, i) {
				self.heap.swap(i, parent);
				self.bubble_up_by(parent, false);
			} else {
				self.bubble_up_by(i, true);
			}
		} else if self.less(i, parent) {
			self.heap.swap(i, parent);
			self.bubble_up_by(parent, true);
		} else {
			self.bubble_up_by(i, false);
		}
	}

	/// Moves the entry at `i` up through its grandparents while it orders
	/// before them (`min`) or after them (not `min`).
	fn bubble_up_by(&mut self, mut i: usize, min: bool) {
		while i > 2 {
			let grandparent = ((i - 1) / 2 - 1) / 2;
			let out_of_order = if min {
				self.less(i, grandparent)
			} else {
				self.less(grandparent, i)
			};
			if !out_of_order {
				break;
			}
			self.heap.swap(i, grandparent);
			i = grandparent;
		}
	}

	fn trickle_down(&mut self, mut i: usize) {
		let min = Self::is_min_level(i);
		// `first(a, b)`: whether `a` belongs above `b` on this level's side.
		let first = |q: &Self, a: usize, b: usize| {
			if min {
				q.less(a, b)
			} else {
				q.less(b, a)
			}
		};
		loop {
			let len = self.heap.len();
			// The most extreme of the children and grandchildren.
			let mut m = None;
			for d in [2 * i + 1, 2 * i + 2, 4 * i + 3, 4 * i + 4, 4 * i + 5, 4 * i + 6] {
				if d < len && m.is_none_or(|m| first(self, d, m)) {
					m = Some(d);
				}
			}
			let Some(m) = m else {
				return;
			};
			if !first(self, m, i) {
				return;
			}
			self.heap.swap(m, i);
			if m <= 2 * i + 2 {
				// A child: nothing below it can be out of order with `i`'s level.
				return;
			}
			// A grandchild: the entry moved down may now belong beyond its parent.
			let parent = (m - 1) / 2;
			if first(self, parent, m) {
				self.heap.swap(m, parent);
			}
			i = m;
		}
	}
}

/// Treats f64 as a sortable data type.
/// It provides an implementation so it can be used as a key in a BTreeMap or
/// BTreeSet.
#[derive(Debug, Clone, Copy)]
pub(super) struct FloatKey(f64);
impl From<FloatKey> for f64 {
	fn from(v: FloatKey) -> Self {
		v.0
	}
}

impl From<f64> for FloatKey {
	fn from(v: f64) -> Self {
		FloatKey(v)
	}
}

impl Eq for FloatKey {}

impl PartialEq<Self> for FloatKey {
	fn eq(&self, other: &Self) -> bool {
		self.0.total_cmp(&other.0) == Ordering::Equal
	}
}

impl PartialOrd<Self> for FloatKey {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for FloatKey {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.total_cmp(&other.0)
	}
}

pub(super) type KnnResult = BTreeSet<(FloatKey, VectorId)>;

/// Ceiling for a graph search's over-query retries, as a multiple of its
/// initial (already ef-boosted) candidate list. The list doubles per retry, so
/// the worst case is three extra graph searches — bounded work that converts
/// "the nearest candidates named fewer than k admissible records" (a selective
/// allow-list or filter, or records indexing several vectors) from a truncated
/// result into a converging one.
pub(super) const OVERQUERY_CEILING_FACTOR: usize = 8;

pub(super) struct KnnResultBuilder {
	/// The number of expected results
	knn: usize,
	/// The sorted results: each id at most once, at the nearest distance it
	/// was offered.
	priority_list: KnnResult,
	/// The distance each id holds its slot in `priority_list` at. An id that
	/// holds no slot is absent from the map, which is what makes `contains` a
	/// faithful answer for the callers that release per-candidate state on it.
	slots: BTreeMap<VectorId, FloatKey>,
}

impl KnnResultBuilder {
	pub(super) fn new(knn: usize) -> Self {
		Self {
			knn,
			priority_list: BTreeSet::new(),
			slots: BTreeMap::new(),
		}
	}

	/// Check if we accept a new entry with the provided distance.
	/// We accept only if the list is not full and the distance is closer
	/// than the farest element in the list
	pub(super) fn check_add(&self, submitted_dist: f64) -> bool {
		if self.priority_list.len() >= self.knn
			&& let Some((max_dist, _)) = self.priority_list.last()
			&& submitted_dist > max_dist.0
		{
			return false;
		}
		true
	}

	/// Add the result to the priority list.
	/// Returns any evicted ids, so any filter cache can be freed
	pub(super) fn add_graph_result(&mut self, dist: f64, added_docs: &Ids64) -> Vec<VectorId> {
		let mut evicted_ids = Vec::with_capacity(added_docs.len() as usize);
		for doc_id in added_docs.iter() {
			if let Some(evited_id) = self.add_vector_id_result(dist, VectorId::DocId(doc_id)) {
				evicted_ids.push(evited_id);
			}
		}
		evicted_ids
	}

	/// Add the result to the priority list.
	/// Returns any evicted id, so any filter cache can be freed
	///
	/// An id holds one slot, at the nearest distance it is offered: a record
	/// indexing several vectors, which the graph reaches through each of them,
	/// is one result, as the pending scan and the exact tier offer it. A
	/// nearer offer for an id already holding a slot moves that slot and
	/// evicts nothing; a farther or equal one changes nothing.
	pub(super) fn add_vector_id_result(&mut self, dist: f64, id: VectorId) -> Option<VectorId> {
		let dist = FloatKey(dist);
		match self.slots.entry(id) {
			Entry::Occupied(mut slot) => {
				if dist < *slot.get() {
					self.priority_list.remove(&(*slot.get(), slot.key().clone()));
					self.priority_list.insert((dist, slot.key().clone()));
					slot.insert(dist);
				}
				return None;
			}
			Entry::Vacant(slot) => {
				self.priority_list.insert((dist, slot.key().clone()));
				slot.insert(dist);
			}
		}
		// Is the priority list full?
		if self.priority_list.len() <= self.knn {
			return None;
		}
		// The farthest slot leaves the result, and its id with it.
		let (_, id) = self.priority_list.pop_last()?;
		self.slots.remove(&id);
		Some(id)
	}

	/// The number of distinct ids the result holds.
	pub(super) fn len(&self) -> usize {
		self.priority_list.len()
	}

	/// How many more ids the result can take before it is full.
	pub(super) fn remaining(&self) -> usize {
		self.knn.saturating_sub(self.priority_list.len())
	}

	/// Returns whether the id currently holds at least one slot in the result.
	///
	/// Lets a caller tell a candidate that is still in the running from one
	/// that was scored and rejected, so per-candidate state kept alongside the
	/// builder can be released for the latter.
	pub(super) fn contains(&self, id: &VectorId) -> bool {
		self.slots.contains_key(id)
	}

	pub(super) fn collect(self) -> KnnResult {
		self.priority_list
	}
}

#[cfg(test)]
pub(super) mod tests {
	use std::cmp::Reverse;
	use std::collections::{BTreeSet, BinaryHeap};
	use std::fs::File;
	use std::io::{BufRead, BufReader};

	use ahash::HashSet;
	use anyhow::Result;
	use flate2::read::GzDecoder;
	use rand::rngs::SmallRng;
	use rand::{Rng, SeedableRng};
	use rust_decimal::prelude::Zero;
	use surrealdb_expr::parser as syn;
	use test_log::test;
	use tracing::info;
	use web_time::SystemTime;

	use crate::catalog::{Distance, VectorType};
	use crate::docids::DocId;
	use crate::trees::hnsw::VectorId;
	use crate::trees::knn::{DoublePriorityQueue, FloatKey, Ids64, KnnResultBuilder};
	use crate::trees::vector::{SharedVector, Vector};
	use crate::val::convert_public::convert_public_value_to_internal;
	use crate::val::{Number, Value};

	pub(crate) fn get_seed_rnd() -> SmallRng {
		let seed: u64 = std::env::var("TEST_SEED")
			.unwrap_or_else(|_| rand::random::<u64>().to_string())
			.parse()
			.expect("Failed to parse seed");
		info!("Seed: {}", seed);
		// Create a seeded RNG
		SmallRng::seed_from_u64(seed)
	}

	#[derive(Debug)]
	pub(in crate::trees) enum TestCollection {
		Unique(Vec<(DocId, SharedVector)>),
		NonUnique(Vec<(DocId, SharedVector)>),
	}

	impl TestCollection {
		pub(in crate::trees) fn to_vec_ref(&self) -> &Vec<(DocId, SharedVector)> {
			match self {
				TestCollection::Unique(c) | TestCollection::NonUnique(c) => c,
			}
		}

		pub(in crate::trees) fn len(&self) -> usize {
			self.to_vec_ref().len()
		}
	}

	pub(in crate::trees) fn new_vectors_from_file<V: From<Vector>>(
		t: VectorType,
		path: &str,
		limit: Option<usize>,
	) -> Result<Vec<(DocId, V)>> {
		// Open the gzip file
		let file = File::open(path)?;

		// Create a GzDecoder to read the file
		let gz = GzDecoder::new(file);

		// Wrap the decoder in a BufReader
		let reader = BufReader::new(gz);

		let mut res = Vec::new();
		// Iterate over each line in the file
		for (i, line_result) in reader.lines().enumerate() {
			if let Some(l) = limit
				&& l == i
			{
				break;
			}
			let line = line_result?;
			let Value::Array(array) = convert_public_value_to_internal(syn::value(&line).unwrap())
			else {
				panic!("Expected a valid array value");
			};
			let vec = Vector::try_from_value(t, array.len(), Value::Array(array))?.into();
			res.push((i as DocId, vec));
		}
		Ok(res)
	}

	pub(in crate::trees) fn new_random_vec(
		rng: &mut SmallRng,
		t: VectorType,
		dim: usize,
		r#gen: &RandomItemGenerator,
	) -> SharedVector {
		let mut vec: Vec<Number> = Vec::with_capacity(dim);
		for _ in 0..dim {
			vec.push(r#gen.generate_for(rng, t));
		}
		let vec = Vector::try_from_vector(t, &vec).unwrap();
		if vec.is_null() {
			// Some similarities (cosine) is undefined for null vector.
			new_random_vec(rng, t, dim, r#gen)
		} else {
			vec.into()
		}
	}

	impl Vector {
		pub(super) fn is_null(&self) -> bool {
			match self {
				Self::F64(a) => !a.iter().any(|a| !a.is_zero()),
				Self::F16(a) => !a.iter().any(|a| !a.is_zero()),
				Self::F32(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I64(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I32(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I16(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I8(a) => !a.iter().any(|a| !a.is_zero()),
				Self::U8(a) => !a.iter().any(|a| !a.is_zero()),
			}
		}
	}

	impl TestCollection {
		pub(in crate::trees) fn new(
			unique: bool,
			collection_size: usize,
			vt: VectorType,
			dimension: usize,
			distance: &Distance,
		) -> Self {
			let mut rng = get_seed_rnd();
			let r#gen = RandomItemGenerator::new(distance, dimension);
			if unique {
				TestCollection::new_unique(collection_size, vt, dimension, &r#gen, &mut rng)
			} else {
				TestCollection::new_random(collection_size, vt, dimension, &r#gen, &mut rng)
			}
		}

		fn add(&mut self, doc: DocId, pt: SharedVector) {
			match self {
				TestCollection::Unique(vec) => vec,
				TestCollection::NonUnique(vec) => vec,
			}
			.push((doc, pt));
		}

		fn new_unique(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
			r#gen: &RandomItemGenerator,
			rng: &mut SmallRng,
		) -> Self {
			let mut vector_set = HashSet::default();
			let mut attempts = collection_size * 2;
			while vector_set.len() < collection_size {
				vector_set.insert(new_random_vec(rng, vector_type, dimension, r#gen));
				attempts -= 1;
				if attempts == 0 {
					panic!("Fail generating a unique random collection {vector_type} {dimension}");
				}
			}
			let mut coll = TestCollection::Unique(Vec::with_capacity(vector_set.len()));
			for (i, v) in vector_set.into_iter().enumerate() {
				coll.add(i as DocId, v);
			}
			coll
		}

		fn new_random(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
			r#gen: &RandomItemGenerator,
			rng: &mut SmallRng,
		) -> Self {
			let mut coll = TestCollection::NonUnique(Vec::with_capacity(collection_size));
			// Prepare data set
			for doc_id in 0..collection_size {
				coll.add(doc_id as DocId, new_random_vec(rng, vector_type, dimension, r#gen));
			}
			coll
		}

		pub(in crate::trees) fn is_unique(&self) -> bool {
			matches!(self, TestCollection::Unique(_))
		}
	}

	pub(in crate::trees) enum RandomItemGenerator {
		Int(i64, i64),
		Float(f64, f64),
	}

	impl RandomItemGenerator {
		pub(in crate::trees) fn new(dist: &Distance, dim: usize) -> Self {
			if let Distance::Jaccard = dist {
				Self::Int(0, (dim / 2) as i64)
			} else if let Distance::Hamming = dist {
				Self::Int(0, 2)
			} else {
				Self::Float(-20.0, 20.0)
			}
		}
		fn generate(&self, rng: &mut SmallRng) -> Number {
			match self {
				RandomItemGenerator::Int(from, to) => Number::Int(rng.random_range(*from..*to)),
				RandomItemGenerator::Float(from, to) => {
					Number::Float(rng.random_range(*from..=*to))
				}
			}
		}

		fn generate_for(&self, rng: &mut SmallRng, vector_type: VectorType) -> Number {
			if let VectorType::U8 = vector_type {
				match self {
					RandomItemGenerator::Int(from, to) => {
						let from = (*from).max(0);
						let to = (*to).max(from + 1).min(u8::MAX as i64 + 1);
						Number::Int(rng.random_range(from..to))
					}
					RandomItemGenerator::Float(_, _) => Number::Int(rng.random_range(0..20)),
				}
			} else if let VectorType::I8 = vector_type {
				match self {
					RandomItemGenerator::Int(from, to) => {
						let from = (*from).max(i8::MIN as i64);
						let to = (*to).max(from + 1).min(i8::MAX as i64 + 1);
						Number::Int(rng.random_range(from..to))
					}
					RandomItemGenerator::Float(_, _) => Number::Int(rng.random_range(-20..20)),
				}
			} else {
				self.generate(rng)
			}
		}
	}

	#[test]
	fn knn_result_builder_test() {
		let mut b = KnnResultBuilder::new(7);
		b.add_graph_result(0.0, &Ids64::One(5));
		b.add_graph_result(0.2, &Ids64::Vec3([0, 1, 2]));
		b.add_graph_result(0.2, &Ids64::One(3));
		b.add_graph_result(0.2, &Ids64::Vec2([6, 8]));
		let res = b.collect();
		assert_eq!(
			res,
			BTreeSet::from([
				(FloatKey(0.0), VectorId::DocId(5)),
				(FloatKey(0.2), VectorId::DocId(0)),
				(FloatKey(0.2), VectorId::DocId(1)),
				(FloatKey(0.2), VectorId::DocId(2)),
				(FloatKey(0.2), VectorId::DocId(3)),
				(FloatKey(0.2), VectorId::DocId(6)),
				(FloatKey(0.2), VectorId::DocId(8))
			])
		);
	}

	#[test]
	fn knn_result_builder_equidistant_duplicate_takes_one_slot() {
		// Two equidistant vectors of one record submit the same (distance, id)
		// pair twice. The second takes no slot, so the id holds exactly one and
		// is released the moment that slot is evicted.
		let mut b = KnnResultBuilder::new(1);
		assert_eq!(b.add_vector_id_result(0.5, VectorId::DocId(1)), None);
		assert_eq!(b.add_vector_id_result(0.5, VectorId::DocId(1)), None);
		assert!(b.contains(&VectorId::DocId(1)));

		// A closer result takes the only slot, evicting the duplicated id.
		assert_eq!(b.add_vector_id_result(0.1, VectorId::DocId(2)), Some(VectorId::DocId(1)));
		assert!(!b.contains(&VectorId::DocId(1)));
		assert!(b.contains(&VectorId::DocId(2)));

		assert_eq!(b.collect(), BTreeSet::from([(FloatKey(0.1), VectorId::DocId(2))]));
	}

	#[test]
	fn knn_result_builder_holds_one_slot_per_id_at_its_nearest() {
		// One id reached at several distances, as a record indexing several
		// vectors is, holds one slot at the nearest: a farther offer changes
		// nothing, and a nearer one moves the slot without evicting anyone.
		let mut b = KnnResultBuilder::new(2);
		assert_eq!(b.add_vector_id_result(0.2, VectorId::DocId(1)), None);
		assert_eq!(b.add_vector_id_result(0.3, VectorId::DocId(1)), None);
		assert_eq!(b.add_vector_id_result(0.4, VectorId::DocId(2)), None);
		assert_eq!(b.add_vector_id_result(0.1, VectorId::DocId(1)), None);
		assert!(b.contains(&VectorId::DocId(2)));

		// A third id evicts the farthest, a whole id with its single slot.
		assert_eq!(b.add_vector_id_result(0.05, VectorId::DocId(3)), Some(VectorId::DocId(2)));
		assert!(!b.contains(&VectorId::DocId(2)));
		assert_eq!(
			b.collect(),
			BTreeSet::from([
				(FloatKey(0.05), VectorId::DocId(3)),
				(FloatKey(0.1), VectorId::DocId(1))
			])
		);
	}

	#[test]
	fn knn_result_builder_bounds_ids_not_offers() {
		// A bound built on the builder counts ids: one id offered through any
		// number of elements leaves room for the others, so a filtered walk
		// that bounds its admissions this way keeps admitting other records.
		let mut b = KnnResultBuilder::new(2);
		for d in [0.1, 0.2, 0.3, 0.4] {
			b.add_vector_id_result(d, VectorId::DocId(1));
		}
		assert_eq!(b.len(), 1);
		assert!(b.check_add(5.0), "a second id still places");
		b.add_vector_id_result(5.0, VectorId::DocId(2));
		assert!(!b.check_add(6.0), "with k ids held, a farther offer is turned away");
		assert!(b.check_add(4.0), "a nearer offer still places");
	}

	#[test]
	fn knn_result_builder_equidistant_graph_results_release_shared_docs() {
		// A record whose indexed field holds several vectors belongs to several
		// elements' doc sets, and two of those elements can sit at the same
		// distance from the query point. The shared doc then reaches the builder
		// twice at one distance, and must still be reported as evicted when its
		// single slot goes.
		let mut b = KnnResultBuilder::new(2);
		assert!(b.add_graph_result(0.2, &Ids64::Vec2([1, 2])).is_empty());
		assert!(b.add_graph_result(0.2, &Ids64::One(1)).is_empty());

		assert_eq!(b.add_graph_result(0.1, &Ids64::One(3)), vec![VectorId::DocId(2)]);
		assert!(b.contains(&VectorId::DocId(1)));

		assert_eq!(b.add_graph_result(0.05, &Ids64::One(4)), vec![VectorId::DocId(1)]);
		assert!(!b.contains(&VectorId::DocId(1)));
	}

	#[test]
	fn test_priority_node() {
		let (n1, n2, n3) = ((FloatKey(1.0), 1), (FloatKey(2.0), 2), (FloatKey(3.0), 3));
		let mut q = BinaryHeap::from([n3, n1, n2]);

		assert_eq!(q.pop(), Some(n3));
		assert_eq!(q.pop(), Some(n2));
		assert_eq!(q.pop(), Some(n1));

		let (n1, n2, n3) = (Reverse(n1), Reverse(n2), Reverse(n3));
		let mut q = BinaryHeap::from([n3, n1, n2]);

		assert_eq!(q.pop(), Some(n1));
		assert_eq!(q.pop(), Some(n2));
		assert_eq!(q.pop(), Some(n3));
	}

	#[test]
	fn test_double_priority_queue() {
		let mut q = DoublePriorityQueue::from(2.0, 2);
		q.push(3.0, 4);
		q.push(3.0, 3);
		q.push(1.0, 1);

		assert_eq!(q.len(), 4);
		assert_eq!(q.peek_first(), Some((1.0, 1)));
		assert_eq!(q.peek_last_dist(), Some(3.0));

		assert_eq!(q.pop_first(), Some((1.0, 1)));
		assert_eq!(q.len(), 3);
		assert_eq!(q.peek_first(), Some((2.0, 2)));
		assert_eq!(q.peek_last_dist(), Some(3.0));

		assert_eq!(q.pop_first(), Some((2.0, 2)));
		assert_eq!(q.len(), 2);
		assert_eq!(q.peek_first(), Some((3.0, 4)));
		assert_eq!(q.peek_last_dist(), Some(3.0));

		assert_eq!(q.pop_first(), Some((3.0, 4)));
		assert_eq!(q.len(), 1);
		assert_eq!(q.peek_first(), Some((3.0, 3)));
		assert_eq!(q.peek_last_dist(), Some(3.0));

		assert_eq!(q.pop_first(), Some((3.0, 3)));
		assert_eq!(q.len(), 0);
		assert_eq!(q.peek_first(), None);
		assert_eq!(q.peek_last_dist(), None);

		let mut q = DoublePriorityQueue::from(2.0, 2);
		q.push(3.0, 4);
		q.push(3.0, 3);
		q.push(1.0, 1);

		assert_eq!(q.pop_last(), Some((3.0, 3)));
		assert_eq!(q.len(), 3);
		assert_eq!(q.peek_first(), Some((1.0, 1)));
		assert_eq!(q.peek_last_dist(), Some(3.0));

		assert_eq!(q.pop_last(), Some((3.0, 4)));
		assert_eq!(q.len(), 2);
		assert_eq!(q.peek_first(), Some((1.0, 1)));
		assert_eq!(q.peek_last_dist(), Some(2.0));

		assert_eq!(q.pop_last(), Some((2.0, 2)));
		assert_eq!(q.len(), 1);
		assert_eq!(q.peek_first(), Some((1.0, 1)));
		assert_eq!(q.peek_last_dist(), Some(1.0));

		assert_eq!(q.pop_last(), Some((1.0, 1)));
		assert_eq!(q.len(), 0);
		assert_eq!(q.peek_first(), None);
		assert_eq!(q.peek_last_dist(), None);
	}

	/// Random pushes and pops over few distinct distances, so ties are common,
	/// match a model ordered by `(distance, push order)`.
	#[test]
	fn test_double_priority_queue_matches_model() {
		use rand::rngs::SmallRng;
		use rand::{Rng, SeedableRng};

		for distinct in [8u32, 1_000_000] {
			let mut rng = SmallRng::seed_from_u64(7);
			let mut q = DoublePriorityQueue::default();
			let mut model: Vec<(FloatKey, u64, u64)> = Vec::new();
			for seq in 0..5_000u64 {
				// Pushes outnumber pops, so the queue grows deep enough to exercise every level.
				match rng.random_range(0..5) {
					0..=2 => {
						let d = f64::from(rng.random_range(0..distinct)) / 4.0;
						let id = rng.random_range(0..1_000u64);
						q.push(d, id);
						model.push((FloatKey(d), seq, id));
						model.sort();
					}
					3 => {
						let expected = (!model.is_empty()).then(|| model.remove(0));
						assert_eq!(q.pop_first(), expected.map(|(d, _, id)| (d.0, id)));
					}
					_ => {
						let expected = model.pop();
						assert_eq!(q.pop_last(), expected.map(|(d, _, id)| (d.0, id)));
					}
				}
				assert_eq!(q.len(), model.len());
				assert_eq!(q.peek_first(), model.first().map(|(d, _, id)| (d.0, *id)));
				assert_eq!(q.peek_last_dist(), model.last().map(|(d, _, _)| d.0));
			}
			let expected: Vec<_> = model.iter().map(|(d, _, id)| (d.0, *id)).collect();
			assert_eq!(q.to_vec(), expected);
			for limit in [0, 1, 10, expected.len(), expected.len() + 1] {
				let prefix: Vec<_> = expected.iter().copied().take(limit).collect();
				assert_eq!(q.to_vec_limit(limit), prefix);
			}
			assert_eq!(q.into_sorted_vec(), expected);
		}
	}

	#[test]
	#[ignore]
	// In HNSW we are maintaining a candidate list that requires both to know the
	// first element and the last element of a set.
	// There is two possible options.
	// 1. Using a BTreeSet that provide first() and last() methods.
	// 2. Maintaining two BinaryHeap. One providing the min, and the other the max.
	// This test checks that option 2 is faster than option 1.
	// Actually option 2 is about 4 times faster than option 1.
	fn confirm_binaryheaps_faster_than_btreeset() {
		// Build samples
		const TOTAL: usize = 500;
		let mut pns = Vec::with_capacity(TOTAL);
		for i in 0..TOTAL {
			pns.push((FloatKey(i as f64), i as u64));
		}

		// Test BTreeSet
		let duration_btree_set = {
			let first = Some(&pns[0]);
			let t = SystemTime::now();
			let mut bt = BTreeSet::new();
			for pn in &pns {
				bt.insert(*pn);
				assert_eq!(bt.first(), first);
				assert_eq!(bt.last(), Some(pn));
			}
			t.elapsed().unwrap()
		};

		// Test double BinaryHeap
		let duration_binary_heap = {
			let r_first = Reverse(pns[0]);
			let first = Some(&r_first);
			let t = SystemTime::now();
			let mut max = BinaryHeap::with_capacity(TOTAL);
			let mut min = BinaryHeap::with_capacity(TOTAL);
			for pn in &pns {
				max.push(*pn);
				min.push(Reverse(*pn));
				assert_eq!(min.peek(), first);
				assert_eq!(max.peek(), Some(pn));
			}
			t.elapsed().unwrap()
		};
		info!("{duration_btree_set:?} {duration_binary_heap:?}");
		assert!(duration_btree_set > duration_binary_heap);
	}
}
