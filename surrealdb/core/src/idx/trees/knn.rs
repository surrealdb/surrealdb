use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ahash::{HashSet, HashSetExt};
// A doc-ID set is what the vector-to-documents mapping stores, so its shape and
// the variant-compaction rule that keeps it minimal are declared below this layer.
pub(in crate::idx) use surrealdb_datastore::values::ids::Ids64;

use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::{ElementId, VectorId};

#[derive(Default, Debug, Clone)]
pub(super) struct DoublePriorityQueue(BTreeMap<FloatKey, VecDeque<ElementId>>, usize);

impl DoublePriorityQueue {
	pub(super) fn from(d: f64, e: ElementId) -> Self {
		let mut q = DoublePriorityQueue::default();
		q.push(d, e);
		q
	}

	pub(super) fn len(&self) -> usize {
		self.1
	}

	pub(super) fn push(&mut self, dist: f64, id: ElementId) {
		match self.0.entry(FloatKey(dist)) {
			Entry::Vacant(e) => {
				e.insert(VecDeque::from([id]));
			}
			Entry::Occupied(mut e) => {
				e.get_mut().push_back(id);
			}
		}
		self.1 += 1;
	}

	pub(super) fn pop_first(&mut self) -> Option<(f64, ElementId)> {
		if let Some(mut e) = self.0.first_entry() {
			let d = e.key().0;
			let q = e.get_mut();
			if let Some(v) = q.pop_front() {
				if q.is_empty() {
					e.remove();
				}
				self.1 -= 1;
				return Some((d, v));
			}
		}
		None
	}

	pub(super) fn pop_last(&mut self) -> Option<(f64, ElementId)> {
		if let Some(mut e) = self.0.last_entry() {
			let d = e.key().0;
			let q = e.get_mut();
			if let Some(v) = q.pop_back() {
				if q.is_empty() {
					e.remove();
				}
				self.1 -= 1;
				return Some((d, v));
			}
		}
		None
	}

	pub(super) fn peek_first(&self) -> Option<(f64, ElementId)> {
		self.0.first_key_value().map(|(k, q)| {
			let k = k.0;
			let v = *q.iter().next().expect("contains always has one element"); // By design the contains always contains one element
			(k, v)
		})
	}

	pub(super) fn peek_last_dist(&self) -> Option<f64> {
		self.0.last_key_value().map(|(k, _)| k.0)
	}

	pub(super) fn to_vec(&self) -> Vec<(f64, ElementId)> {
		let mut v = Vec::with_capacity(self.1);
		for (d, q) in &self.0 {
			for e in q {
				v.push((d.0, *e));
			}
		}
		v
	}

	pub(super) fn to_vec_limit(&self, mut limit: usize) -> Vec<(f64, ElementId)> {
		let mut v = Vec::with_capacity(self.1.min(limit));
		for (d, q) in &self.0 {
			for e in q {
				v.push((d.0, *e));
				limit -= 1;
				if limit == 0 {
					return v;
				}
			}
		}
		v
	}

	pub(super) fn to_set(&self) -> HashSet<ElementId> {
		let mut s = HashSet::with_capacity(self.1);
		for q in self.0.values() {
			for v in q {
				s.insert(*v);
			}
		}
		s
	}

	pub(super) fn to_dynamic_set<S: DynamicSet>(&self, set: &mut S) {
		for q in self.0.values() {
			for v in q {
				set.insert(*v);
			}
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

pub(super) struct KnnResultBuilder {
	/// The number of expected results
	knn: usize,
	/// The sorted results
	priority_list: KnnResult,
	/// Count the number of time a vector id is present in the result
	vector_id_count: BTreeMap<VectorId, usize>,
}

impl KnnResultBuilder {
	pub(super) fn new(knn: usize) -> Self {
		Self {
			knn,
			priority_list: BTreeSet::new(),
			vector_id_count: BTreeMap::new(),
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
	pub(super) fn add_vector_id_result(&mut self, dist: f64, id: VectorId) -> Option<VectorId> {
		// Insert the result in the list
		self.priority_list.insert((FloatKey(dist), id.clone()));
		// Update the vector count
		self.vector_id_count.entry(id).and_modify(|c| *c += 1).or_insert(1);
		// Is the priority list full?
		if self.priority_list.len() <= self.knn {
			return None;
		}
		// We remove the last element
		if let Some((_, id)) = self.priority_list.pop_last()
			&& let Entry::Occupied(mut e) = self.vector_id_count.entry(id)
		{
			let c = e.get_mut();
			if *c <= 1 {
				// This entry does not exist anymore in the result list, it can be evicted
				let (id, _) = e.remove_entry();
				return Some(id);
			}
			*c -= 1;
		}
		None
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
	use test_log::test;
	use web_time::SystemTime;

	use crate::catalog::{Distance, VectorType};
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::VectorId;
	use crate::idx::trees::knn::{DoublePriorityQueue, FloatKey, Ids64, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::syn;
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
	pub(in crate::idx::trees) enum TestCollection {
		Unique(Vec<(DocId, SharedVector)>),
		NonUnique(Vec<(DocId, SharedVector)>),
	}

	impl TestCollection {
		pub(in crate::idx::trees) fn to_vec_ref(&self) -> &Vec<(DocId, SharedVector)> {
			match self {
				TestCollection::Unique(c) | TestCollection::NonUnique(c) => c,
			}
		}

		pub(in crate::idx::trees) fn len(&self) -> usize {
			self.to_vec_ref().len()
		}
	}

	pub(in crate::idx::trees) fn new_vectors_from_file<V: From<Vector>>(
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

	pub(in crate::idx::trees) fn new_random_vec(
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
		pub(in crate::idx::trees) fn new(
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

		pub(in crate::idx::trees) fn is_unique(&self) -> bool {
			matches!(self, TestCollection::Unique(_))
		}
	}

	pub(in crate::idx::trees) enum RandomItemGenerator {
		Int(i64, i64),
		Float(f64, f64),
	}

	impl RandomItemGenerator {
		pub(in crate::idx::trees) fn new(dist: &Distance, dim: usize) -> Self {
			match dist {
				Distance::Jaccard => Self::Int(0, (dim / 2) as i64),
				Distance::Hamming => Self::Int(0, 2),
				_ => Self::Float(-20.0, 20.0),
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
			match vector_type {
				VectorType::U8 => match self {
					RandomItemGenerator::Int(from, to) => {
						let from = (*from).max(0);
						let to = (*to).max(from + 1).min(u8::MAX as i64 + 1);
						Number::Int(rng.random_range(from..to))
					}
					RandomItemGenerator::Float(_, _) => Number::Int(rng.random_range(0..20)),
				},
				VectorType::I8 => match self {
					RandomItemGenerator::Int(from, to) => {
						let from = (*from).max(i8::MIN as i64);
						let to = (*to).max(from + 1).min(i8::MAX as i64 + 1);
						Number::Int(rng.random_range(from..to))
					}
					RandomItemGenerator::Float(_, _) => Number::Int(rng.random_range(-20..20)),
				},
				_ => self.generate(rng),
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
