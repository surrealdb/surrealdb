use crate::idx::docids::DocId;
use crate::idx::trees::store::NodeId;
use roaring::RoaringTreemap;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, VecDeque};

pub(super) struct PriorityNode(f64, NodeId);

impl PriorityNode {
	pub(super) fn new(dist: f64, node_id: NodeId) -> Self {
		Self(dist, node_id)
	}

	pub(super) fn dist(&self) -> f64 {
		self.0
	}

	pub(super) fn node_id(&self) -> NodeId {
		self.1
	}
}

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

pub(super) struct KnnResultBuilder {
	knn: u64,
	docs: RoaringTreemap,
	priority_list: BTreeMap<PriorityResult, RoaringTreemap>,
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

	pub(super) fn add(&mut self, dist: f64, docs: &RoaringTreemap) {
		let pr = PriorityResult(dist);
		match self.priority_list.entry(pr) {
			Entry::Vacant(e) => {
				for doc in docs {
					self.docs.insert(doc);
				}
				e.insert(docs.clone());
			}
			Entry::Occupied(mut e) => {
				let d = e.get_mut();
				for doc in docs {
					d.insert(doc);
					self.docs.insert(doc);
				}
			}
		}

		#[cfg(debug_assertions)]
		debug!("KnnResult add - dist: {} - docs: {:?} - total: {}", dist, docs, self.docs.len());
		debug!("{:?}", self.priority_list);

		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					if let Some((_, evicted_docs)) = self.priority_list.pop_last() {
						self.docs -= evicted_docs;
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
			for doc_id in docs {
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
	use crate::idx::trees::vector::{SharedVector, Vector};
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
		let mut vec = Vector::new(t, dim);
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
		let mut vec = Vector::new(t, dim);
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
