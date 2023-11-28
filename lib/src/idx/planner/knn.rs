use crate::sql::Id;
use std::cmp::Ordering;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet, VecDeque};

pub(super) struct KnnPriorityList {
	knn: usize,
	docs: HashSet<Id>,
	priority_list: BTreeMap<PriorityResult, HashSet<Id>>,
}

impl KnnPriorityList {
	pub(super) fn new(knn: usize) -> Self {
		Self {
			knn,
			docs: HashSet::new(),
			priority_list: BTreeMap::default(),
		}
	}
	fn check_add(&self, dist: f64) -> bool {
		if self.docs.len() < self.knn {
			true
		} else if let Some(pr) = self.priority_list.keys().last() {
			dist <= pr.0
		} else {
			true
		}
	}

	fn add(&mut self, dist: f64, id: &Id) {
		let pr = PriorityResult(dist);
		match self.priority_list.entry(pr) {
			Entry::Vacant(e) => {
				let mut h = HashSet::new();
				h.insert(id.clone());
				e.insert(h);
				self.docs.insert(id.clone());
			}
			Entry::Occupied(mut e) => {
				let h = e.get_mut();
				h.insert(id.clone());
			}
		}

		#[cfg(debug_assertions)]
		debug!(
			"KnnPriorityList add - dist: {} - id: {:?} - total: {}",
			dist,
			id,
			self.priority_list.len()
		);

		// Do possible eviction
		let docs_len = self.docs.len();
		if docs_len > self.knn {
			if let Some((_, d)) = self.priority_list.last_key_value() {
				if docs_len - d.len() >= self.knn {
					if let Some((_, evicted_docs)) = self.priority_list.pop_last() {
						for id in evicted_docs {
							self.docs.remove(&id);
						}
					}
				}
			}
		}
	}

	fn build(self) -> HashSet<Id> {
		let mut sorted_docs = VecDeque::with_capacity(self.knn);
		#[cfg(debug_assertions)]
		debug!("self.priority_list: {:?} - self.docs: {:?}", self.priority_list, self.docs);
		let mut left = self.knn;
		for (_, docs) in self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.into_iter().take(left) {
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
		let mut r = HashSet::with_capacity(sorted_docs.len());
		for id in sorted_docs {
			r.insert(id);
		}
		r
	}
}

#[derive(Debug)]
pub(in crate::idx) struct PriorityResult(pub(crate) f64);

impl Eq for PriorityResult {}

impl PartialEq<Self> for PriorityResult {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl PartialOrd<Self> for PriorityResult {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for PriorityResult {
	fn cmp(&self, other: &Self) -> Ordering {
		cmp_f64(&self.0, &other.0)
	}
}

pub(in crate::idx) fn cmp_f64(f1: &f64, f2: &f64) -> Ordering {
	if let Some(cmp) = f1.partial_cmp(f2) {
		return cmp;
	}
	if f1.is_nan() {
		if f2.is_nan() {
			Ordering::Equal
		} else {
			Ordering::Less
		}
	} else {
		Ordering::Greater
	}
}
