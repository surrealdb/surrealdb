use std::cmp::{Ordering, Reverse};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};

#[cfg(debug_assertions)]
use ahash::HashMap;
use ahash::{HashSet, HashSetExt};
use revision::revisioned;
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::idx::docids::DocId;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::ElementId;
use crate::idx::trees::store::NodeId;

#[derive(Debug, Clone, Copy, Ord, Eq, PartialEq, PartialOrd)]
pub(super) struct PriorityNode(Reverse<FloatKey>, NodeId);

impl PriorityNode {
	pub(super) fn new(d: f64, id: NodeId) -> Self {
		Self(Reverse(FloatKey::new(d)), id)
	}

	pub(super) fn id(&self) -> NodeId {
		self.1
	}
}

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
			let v = *q.iter().next().unwrap(); // By design the contains always contains one element
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

impl FloatKey {
	pub(super) fn new(f: f64) -> Self {
		FloatKey(f)
	}
}
impl From<FloatKey> for f64 {
	fn from(v: FloatKey) -> Self {
		v.0
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

/// Ids64 is a collection able to store u64 identifiers in an optimised way.
/// The enumerations are optimised in a way that, depending on the number of
/// identifiers, the most memory efficient variant is used.
/// When identifiers are added or removed, the method returned the most
/// appropriate variant (if required).
#[derive(Debug, Clone, PartialEq)]
#[revisioned(revision = 1)]
#[derive(Serialize, Deserialize)]
pub(in crate::idx) enum Ids64 {
	Empty,
	One(u64),
	Vec2([u64; 2]),
	Vec3([u64; 3]),
	Vec4([u64; 4]),
	Vec5([u64; 5]),
	Vec6([u64; 6]),
	Vec7([u64; 7]),
	Vec8([u64; 8]),
	Bits(RoaringTreemap),
}

impl Ids64 {
	fn len(&self) -> u64 {
		match self {
			Self::Empty => 0,
			Self::One(_) => 1,
			Self::Vec2(_) => 2,
			Self::Vec3(_) => 3,
			Self::Vec4(_) => 4,
			Self::Vec5(_) => 5,
			Self::Vec6(_) => 6,
			Self::Vec7(_) => 7,
			Self::Vec8(_) => 8,
			Self::Bits(b) => b.len(),
		}
	}

	pub(super) fn is_empty(&self) -> bool {
		matches!(self, Self::Empty)
	}

	fn append_to(&self, to: &mut RoaringTreemap) {
		match &self {
			Self::Empty => {}
			Self::One(d) => {
				to.insert(*d);
			}
			Self::Vec2(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec3(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec4(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec5(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec6(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec7(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Vec8(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Self::Bits(b) => {
				for d in b {
					to.insert(d);
				}
			}
		}
	}

	fn remove_to(&self, to: &mut RoaringTreemap) {
		match &self {
			Self::Empty => {}
			Self::One(d) => {
				to.remove(*d);
			}
			Self::Vec2(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec3(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec4(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec5(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec6(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec7(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Vec8(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Self::Bits(b) => {
				for d in b {
					to.remove(d);
				}
			}
		}
	}

	fn append_iter_ref<'a, I>(&mut self, docs: I) -> Option<Self>
	where
		I: Iterator<Item = &'a DocId>,
	{
		let mut new_doc: Option<Self> = None;
		for &doc in docs {
			if let Some(ref mut nd) = new_doc {
				let nd = nd.insert(doc);
				if nd.is_some() {
					new_doc = nd;
				};
			} else {
				new_doc = self.insert(doc);
			}
		}
		new_doc
	}

	fn append_iter<I>(&mut self, docs: I) -> Option<Self>
	where
		I: Iterator<Item = DocId>,
	{
		let mut new_doc: Option<Self> = None;
		for doc in docs {
			if let Some(mut nd) = new_doc {
				new_doc = nd.insert(doc);
			} else {
				new_doc = self.insert(doc);
			}
		}
		new_doc
	}
	fn append_from(&mut self, from: &Ids64) -> Option<Self> {
		match from {
			Self::Empty => None,
			Self::One(d) => self.insert(*d),
			Self::Vec2(a) => self.append_iter_ref(a.iter()),
			Self::Vec3(a) => self.append_iter_ref(a.iter()),
			Self::Vec4(a) => self.append_iter_ref(a.iter()),
			Self::Vec5(a) => self.append_iter_ref(a.iter()),
			Self::Vec6(a) => self.append_iter_ref(a.iter()),
			Self::Vec7(a) => self.append_iter_ref(a.iter()),
			Self::Vec8(a) => self.append_iter_ref(a.iter()),
			Self::Bits(a) => self.append_iter(a.iter()),
		}
	}

	pub(in crate::idx) fn iter(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
		match &self {
			Self::Empty => Box::new(EmptyIterator {}),
			Self::One(d) => Box::new(OneDocIterator(Some(*d))),
			Self::Vec2(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec3(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec4(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec5(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec6(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec7(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Vec8(a) => Box::new(SliceDocIterator(a.iter())),
			Self::Bits(a) => Box::new(a.iter()),
		}
	}

	fn contains(&self, d: DocId) -> bool {
		match self {
			Self::Empty => false,
			Self::One(o) => *o == d,
			Self::Vec2(a) => a.contains(&d),
			Self::Vec3(a) => a.contains(&d),
			Self::Vec4(a) => a.contains(&d),
			Self::Vec5(a) => a.contains(&d),
			Self::Vec6(a) => a.contains(&d),
			Self::Vec7(a) => a.contains(&d),
			Self::Vec8(a) => a.contains(&d),
			Self::Bits(b) => b.contains(d),
		}
	}

	pub(super) fn insert(&mut self, d: DocId) -> Option<Self> {
		if !self.contains(d) {
			match self {
				Self::Empty => Some(Self::One(d)),
				Self::One(o) => Some(Self::Vec2([*o, d])),
				Self::Vec2(a) => Some(Self::Vec3([a[0], a[1], d])),
				Self::Vec3(a) => Some(Self::Vec4([a[0], a[1], a[2], d])),
				Self::Vec4(a) => Some(Self::Vec5([a[0], a[1], a[2], a[3], d])),
				Self::Vec5(a) => Some(Self::Vec6([a[0], a[1], a[2], a[3], a[4], d])),
				Self::Vec6(a) => Some(Self::Vec7([a[0], a[1], a[2], a[3], a[4], a[5], d])),
				Self::Vec7(a) => Some(Self::Vec8([a[0], a[1], a[2], a[3], a[4], a[5], a[6], d])),
				Self::Vec8(a) => Some(Self::Bits(RoaringTreemap::from([
					a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], d,
				]))),
				Self::Bits(b) => {
					b.insert(d);
					None
				}
			}
		} else {
			None
		}
	}

	pub(super) fn remove(&mut self, d: DocId) -> Option<Self> {
		match self {
			Self::Empty => None,
			Self::One(i) => {
				if d == *i {
					Some(Self::Empty)
				} else {
					None
				}
			}
			Self::Vec2(a) => a.iter().find(|&&i| i != d).map(|&i| Self::One(i)),
			Self::Vec3(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 2 {
					Some(Self::Vec2([v[0], v[1]]))
				} else {
					None
				}
			}
			Self::Vec4(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 3 {
					Some(Self::Vec3([v[0], v[1], v[2]]))
				} else {
					None
				}
			}
			Self::Vec5(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 4 {
					Some(Self::Vec4([v[0], v[1], v[2], v[3]]))
				} else {
					None
				}
			}
			Self::Vec6(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 5 {
					Some(Self::Vec5([v[0], v[1], v[2], v[3], v[4]]))
				} else {
					None
				}
			}
			Self::Vec7(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 6 {
					Some(Self::Vec6([v[0], v[1], v[2], v[3], v[4], v[5]]))
				} else {
					None
				}
			}
			Self::Vec8(a) => {
				let v: Vec<DocId> = a.iter().filter(|&&i| i != d).copied().collect();
				if v.len() == 7 {
					Some(Self::Vec7([v[0], v[1], v[2], v[3], v[4], v[5], v[6]]))
				} else {
					None
				}
			}
			Self::Bits(b) => {
				if !b.remove(d) || b.len() != 8 {
					None
				} else {
					let v: Vec<DocId> = b.iter().collect();
					Some(Self::Vec8([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]]))
				}
			}
		}
	}
}

struct EmptyIterator;

impl Iterator for EmptyIterator {
	type Item = DocId;

	fn next(&mut self) -> Option<Self::Item> {
		None
	}
}

struct OneDocIterator(Option<DocId>);

impl Iterator for OneDocIterator {
	type Item = DocId;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.take()
	}
}

struct SliceDocIterator<'a, I>(I)
where
	I: Iterator<Item = &'a DocId>;

impl<'a, I> Iterator for SliceDocIterator<'a, I>
where
	I: Iterator<Item = &'a DocId>,
{
	type Item = DocId;

	fn next(&mut self) -> Option<Self::Item> {
		self.0.next().copied()
	}
}

pub(super) struct KnnResultBuilder {
	knn: u64,
	docs: RoaringTreemap,
	priority_list: BTreeMap<FloatKey, Ids64>,
}

impl KnnResultBuilder {
	pub(super) fn new(knn: usize) -> Self {
		Self {
			knn: knn as u64,
			docs: RoaringTreemap::default(),
			priority_list: BTreeMap::default(),
		}
	}
	pub(super) fn check_add(&self, dist: f64) -> bool {
		if self.docs.len() >= self.knn {
			if let Some(pr) = self.priority_list.keys().last() {
				if dist > pr.0 {
					return false;
				}
			}
		}
		true
	}

	pub(super) fn add(&mut self, dist: f64, docs: Ids64) -> Ids64 {
		let pr = FloatKey(dist);
		docs.append_to(&mut self.docs);
		match self.priority_list.entry(pr) {
			Entry::Vacant(e) => {
				e.insert(docs.clone());
			}
			Entry::Occupied(mut e) => {
				let d = e.get_mut();
				if let Some(n) = d.append_from(&docs) {
					e.insert(n);
				}
			}
		}

		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					if let Some((_, evicted_docs)) = self.priority_list.pop_last() {
						evicted_docs.remove_to(&mut self.docs);
						return evicted_docs;
					}
				}
			}
		}
		Ids64::Empty
	}

	pub(super) fn build(
		self,
		#[cfg(debug_assertions)] visited_nodes: HashMap<NodeId, usize>,
	) -> KnnResult {
		let mut sorted_docs = VecDeque::with_capacity(self.knn as usize);
		let mut left = self.knn;
		for (pr, docs) in self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left as usize) {
					sorted_docs.push_back((doc_id, pr.0));
				}
				break;
			}
			for doc_id in docs.iter() {
				sorted_docs.push_back((doc_id, pr.0));
			}
			left -= dl;
			// We don't expect anymore result, we can leave
			if left == 0 {
				break;
			}
		}
		trace!("sorted_docs: {:?}", sorted_docs);
		KnnResult {
			docs: sorted_docs,
			#[cfg(debug_assertions)]
			visited_nodes,
		}
	}
}

pub struct KnnResult {
	pub(in crate::idx::trees) docs: VecDeque<(DocId, f64)>,
	#[cfg(debug_assertions)]
	#[cfg_attr(all(debug_assertions, not(test)), expect(dead_code))]
	pub(in crate::idx::trees) visited_nodes: HashMap<NodeId, usize>,
}

#[cfg(test)]
pub(super) mod tests {
	use std::cmp::Reverse;
	use std::collections::{BTreeSet, BinaryHeap, VecDeque};
	use std::fs::File;
	use std::io::{BufRead, BufReader};
	use std::time::SystemTime;

	#[cfg(debug_assertions)]
	use ahash::HashMap;
	use ahash::HashSet;
	use anyhow::Result;
	use flate2::read::GzDecoder;
	use rand::prelude::SmallRng;
	use rand::{Rng, SeedableRng};
	use roaring::RoaringTreemap;
	use rust_decimal::prelude::Zero;
	use test_log::test;

	use crate::catalog::{Distance, VectorType};
	use crate::idx::docids::DocId;
	use crate::idx::trees::knn::{DoublePriorityQueue, FloatKey, Ids64, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::syn;
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
			if let Some(l) = limit {
				if l == i {
					break;
				}
			}
			let line = line_result?;
			let Ok(Value::Array(array)) = syn::value(&line) else {
				panic!()
			};
			let vec = Vector::try_from_value(t, array.len(), &Value::Array(array))?.into();
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
			vec.push(r#gen.generate(rng));
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
				Self::F32(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I64(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I32(a) => !a.iter().any(|a| !a.is_zero()),
				Self::I16(a) => !a.iter().any(|a| !a.is_zero()),
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
				RandomItemGenerator::Int(from, to) => Number::Int(rng.gen_range(*from..*to)),
				RandomItemGenerator::Float(from, to) => Number::Float(rng.gen_range(*from..=*to)),
			}
		}
	}

	#[test]
	fn knn_result_builder_test() {
		let mut b = KnnResultBuilder::new(7);
		b.add(0.0, Ids64::One(5));
		b.add(0.2, Ids64::Vec3([0, 1, 2]));
		b.add(0.2, Ids64::One(3));
		b.add(0.2, Ids64::Vec2([6, 8]));
		let res = b.build(
			#[cfg(debug_assertions)]
			HashMap::default(),
		);
		assert_eq!(
			res.docs,
			VecDeque::from([(5, 0.0), (0, 0.2), (1, 0.2), (2, 0.2), (3, 0.2), (6, 0.2), (8, 0.2)])
		);
	}

	#[test]
	fn test_ids() {
		let mut ids = Ids64::Empty;
		let mut ids = ids.insert(10).expect("Ids64::One");
		assert_eq!(ids, Ids64::One(10));
		let mut ids = ids.insert(20).expect("Ids64::Vec2");
		assert_eq!(ids, Ids64::Vec2([10, 20]));
		let mut ids = ids.insert(30).expect("Ids64::Vec3");
		assert_eq!(ids, Ids64::Vec3([10, 20, 30]));
		let mut ids = ids.insert(40).expect("Ids64::Vec4");
		assert_eq!(ids, Ids64::Vec4([10, 20, 30, 40]));
		let mut ids = ids.insert(50).expect("Ids64::Vec5");
		assert_eq!(ids, Ids64::Vec5([10, 20, 30, 40, 50]));
		let mut ids = ids.insert(60).expect("Ids64::Vec6");
		assert_eq!(ids, Ids64::Vec6([10, 20, 30, 40, 50, 60]));
		let mut ids = ids.insert(70).expect("Ids64::Vec7");
		assert_eq!(ids, Ids64::Vec7([10, 20, 30, 40, 50, 60, 70]));
		let mut ids = ids.insert(80).expect("Ids64::Vec8");
		assert_eq!(ids, Ids64::Vec8([10, 20, 30, 40, 50, 60, 70, 80]));
		let mut ids = ids.insert(90).expect("Ids64::Bits");
		assert_eq!(ids, Ids64::Bits(RoaringTreemap::from([10, 20, 30, 40, 50, 60, 70, 80, 90])));
		assert_eq!(ids.insert(100), None);
		assert_eq!(
			ids,
			Ids64::Bits(RoaringTreemap::from([10, 20, 30, 40, 50, 60, 70, 80, 90, 100]))
		);
		assert_eq!(ids.remove(10), None);
		assert_eq!(ids, Ids64::Bits(RoaringTreemap::from([20, 30, 40, 50, 60, 70, 80, 90, 100])));
		let mut ids = ids.remove(20).expect("Ids64::Vec8");
		assert_eq!(ids, Ids64::Vec8([30, 40, 50, 60, 70, 80, 90, 100]));
		let mut ids = ids.remove(30).expect("Ids64::Vec7");
		assert_eq!(ids, Ids64::Vec7([40, 50, 60, 70, 80, 90, 100]));
		let mut ids = ids.remove(40).expect("Ids64::Vec6");
		assert_eq!(ids, Ids64::Vec6([50, 60, 70, 80, 90, 100]));
		let mut ids = ids.remove(50).expect("Ids64::Vec5");
		assert_eq!(ids, Ids64::Vec5([60, 70, 80, 90, 100]));
		let mut ids = ids.remove(60).expect("Ids64::Vec4");
		assert_eq!(ids, Ids64::Vec4([70, 80, 90, 100]));
		let mut ids = ids.remove(70).expect("Ids64::Vec3");
		assert_eq!(ids, Ids64::Vec3([80, 90, 100]));
		let mut ids = ids.remove(80).expect("Ids64::Vec2");
		assert_eq!(ids, Ids64::Vec2([90, 100]));
		let mut ids = ids.remove(90).expect("Ids64::One");
		assert_eq!(ids, Ids64::One(100));
		let ids = ids.remove(100).expect("Ids64::Empty");
		assert_eq!(ids, Ids64::Empty);
	}

	#[test]
	fn test_priority_node() {
		let (n1, n2, n3) =
			((FloatKey::new(1.0), 1), (FloatKey::new(2.0), 2), (FloatKey::new(3.0), 3));
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

		let mut q = DoublePriorityQueue::from(2.0, 2).clone();
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
			pns.push((FloatKey::new(i as f64), i as u64));
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
