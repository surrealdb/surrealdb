use crate::idx::docids::DocId;
use crate::idx::trees::store::NodeId;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};
#[derive(Debug, Clone)]
pub(super) struct PriorityNode(pub(super) f64, pub(super) u64);

impl PartialEq<Self> for PriorityNode {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0 && self.1 == other.1
	}
}

impl Eq for PriorityNode {}

impl PartialOrd for PriorityNode {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match self.0.partial_cmp(&other.0) {
			None => {}
			Some(o) => {
				if !matches!(o, Ordering::Equal) {
					return Some(o);
				}
			}
		}
		self.1.partial_cmp(&other.1)
	}
}

impl Ord for PriorityNode {
	fn cmp(&self, other: &Self) -> Ordering {
		let o = self.0.total_cmp(&other.0);
		if !matches!(o, Ordering::Equal) {
			return o;
		}
		self.1.cmp(&other.1)
	}
}

#[derive(Debug)]
pub(super) struct PriorityResult(f64);

impl Eq for PriorityResult {}

impl PartialEq<Self> for PriorityResult {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}
impl PartialOrd for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.0.partial_cmp(&other.0)
	}
}

impl Ord for PriorityResult {
	fn cmp(&self, other: &Self) -> Ordering {
		self.0.total_cmp(&other.0)
	}
}

#[derive(Debug, Clone)]
pub(super) enum Docs {
	One(DocId),
	Vec2([DocId; 2]),
	Vec3([DocId; 3]),
	Vec4([DocId; 4]),
	Bits(RoaringTreemap),
}

impl Docs {
	fn len(&self) -> u64 {
		match self {
			Docs::One(_) => 1,
			Docs::Vec2(_) => 2,
			Docs::Vec3(_) => 3,
			Docs::Vec4(_) => 4,
			Docs::Bits(b) => b.len(),
		}
	}

	fn append_to(&self, to: &mut RoaringTreemap) {
		match &self {
			Docs::One(d) => {
				to.insert(*d);
			}
			Docs::Vec2(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Docs::Vec3(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Docs::Vec4(a) => {
				for d in a {
					to.insert(*d);
				}
			}
			Docs::Bits(b) => {
				for d in b {
					to.insert(d);
				}
			}
		}
	}

	fn remove_to(&self, to: &mut RoaringTreemap) {
		match &self {
			Docs::One(d) => {
				to.remove(*d);
			}
			Docs::Vec2(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Docs::Vec3(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Docs::Vec4(a) => {
				for &d in a {
					to.remove(d);
				}
			}
			Docs::Bits(b) => {
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
			if let Some(mut nd) = new_doc {
				new_doc = nd.insert(doc);
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
	fn append_from(&mut self, from: &Docs) -> Option<Self> {
		match from {
			Docs::One(d) => self.insert(*d),
			Docs::Vec2(a) => self.append_iter_ref(a.iter()),
			Docs::Vec3(a) => self.append_iter_ref(a.iter()),
			Docs::Vec4(a) => self.append_iter_ref(a.iter()),
			Docs::Bits(a) => self.append_iter(a.iter()),
		}
	}

	fn iter(&self) -> Box<dyn Iterator<Item = DocId> + '_> {
		match &self {
			Docs::One(d) => Box::new(OneDocIterator(Some(*d))),
			Docs::Vec2(a) => Box::new(SliceDocIterator(a.iter())),
			Docs::Vec3(a) => Box::new(SliceDocIterator(a.iter())),
			Docs::Vec4(a) => Box::new(SliceDocIterator(a.iter())),
			Docs::Bits(a) => Box::new(a.iter()),
		}
	}

	pub(super) fn insert(&mut self, d: DocId) -> Option<Self> {
		match self {
			Docs::One(o) => Some(Docs::Vec2([*o, d])),
			Docs::Vec2(a) => Some(Docs::Vec3([a[0], a[1], d])),
			Docs::Vec3(a) => Some(Docs::Vec4([a[0], a[1], a[2], d])),
			Docs::Vec4(a) => Some(Docs::Bits(RoaringTreemap::from([a[0], a[1], a[2], a[3], d]))),
			Docs::Bits(b) => {
				b.insert(d);
				None
			}
		}
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
		self.0.next().cloned()
	}
}

pub(super) struct KnnResultBuilder {
	knn: u64,
	docs: RoaringTreemap,
	priority_list: BTreeMap<PriorityResult, Docs>,
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
		if self.docs.len() < self.knn {
			true
		} else if let Some(pr) = self.priority_list.keys().last() {
			dist <= pr.0
		} else {
			true
		}
	}

	pub(super) fn add(&mut self, dist: f64, docs: &Docs) {
		let pr = PriorityResult(dist);
		docs.append_to(&mut self.docs);
		match self.priority_list.entry(pr) {
			Entry::Vacant(e) => {
				e.insert(docs.clone());
			}
			Entry::Occupied(mut e) => {
				let d = e.get_mut();
				d.append_from(docs);
			}
		}

		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					if let Some((_, evicted_docs)) = self.priority_list.pop_last() {
						evicted_docs.remove_to(&mut self.docs);
					}
				}
			}
		}
	}

	pub(super) fn build(
		self,
		#[cfg(debug_assertions)] visited_nodes: HashMap<NodeId, usize>,
	) -> KnnResult {
		let mut sorted_docs = VecDeque::with_capacity(self.knn as usize);
		#[cfg(debug_assertions)]
		debug!("self.priority_list: {:?} - self.docs: {:?}", self.priority_list, self.docs);
		let mut left = self.knn;
		for (_, docs) in self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left as usize) {
					sorted_docs.push_back(doc_id);
				}
				break;
			}
			for doc_id in docs.iter() {
				sorted_docs.push_back(doc_id);
			}
			left -= dl;
			// We don't expect anymore result, we can leave
			if left == 0 {
				break;
			}
		}
		debug!("sorted_docs: {:?}", sorted_docs);
		KnnResult {
			docs: sorted_docs,
			#[cfg(debug_assertions)]
			visited_nodes,
		}
	}
}

pub struct KnnResult {
	pub(in crate::idx::trees) docs: VecDeque<DocId>,
	#[cfg(debug_assertions)]
	#[allow(dead_code)]
	pub(in crate::idx::trees) visited_nodes: HashMap<NodeId, usize>,
}

#[cfg(test)]
pub(super) mod tests {
	use crate::idx::docids::DocId;
	use crate::idx::trees::vector::{SharedVector, TreeVector};
	use crate::sql::index::VectorType;
	use crate::sql::Number;
	use rand::prelude::StdRng;
	use rand::{Rng, SeedableRng};
	use std::sync::Arc;

	pub(super) fn get_seed_rnd() -> StdRng {
		let seed: u64 = std::env::var("TEST_SEED")
			.unwrap_or_else(|_| rand::random::<u64>().to_string())
			.parse()
			.expect("Failed to parse seed");
		debug!("Seed: {}", seed);
		// Create a seeded RNG
		StdRng::seed_from_u64(seed)
	}

	pub(in crate::idx::trees) enum TestCollection {
		Unique(Vec<(DocId, SharedVector)>),
		NonUnique(Vec<(DocId, SharedVector)>),
	}

	impl AsRef<Vec<(DocId, SharedVector)>> for TestCollection {
		fn as_ref(&self) -> &Vec<(DocId, SharedVector)> {
			match self {
				TestCollection::Unique(c) | TestCollection::NonUnique(c) => c,
			}
		}
	}

	pub(in crate::idx::trees) fn new_vec(mut n: i64, t: VectorType, dim: usize) -> SharedVector {
		let mut vec = TreeVector::new(t, dim);
		vec.add(Number::Int(n));
		for _ in 1..dim {
			n += 1;
			vec.add(Number::Int(n));
		}
		Arc::new(vec)
	}

	pub(in crate::idx::trees) fn new_random_vec(
		rng: &mut StdRng,
		t: VectorType,
		dim: usize,
	) -> SharedVector {
		let mut vec = TreeVector::new(t, dim);
		for _ in 0..dim {
			vec.add(Number::Float(rng.gen_range(-5.0..5.0)));
		}
		Arc::new(vec)
	}

	impl TestCollection {
		pub(in crate::idx::trees) fn new_unique(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
		) -> TestCollection {
			let mut collection = vec![];
			for doc_id in 0..collection_size as DocId {
				collection.push((doc_id, new_vec((doc_id + 1) as i64, vector_type, dimension)));
			}
			TestCollection::Unique(collection)
		}

		pub(in crate::idx::trees) fn new_random(
			collection_size: usize,
			vector_type: VectorType,
			dimension: usize,
		) -> TestCollection {
			let mut rng = get_seed_rnd();
			let mut collection = vec![];

			// Prepare data set
			for doc_id in 0..collection_size {
				collection
					.push((doc_id as DocId, new_random_vec(&mut rng, vector_type, dimension)));
			}
			TestCollection::NonUnique(collection)
		}

		pub(in crate::idx::trees) fn is_unique(&self) -> bool {
			matches!(self, TestCollection::Unique(_))
		}
	}
}
