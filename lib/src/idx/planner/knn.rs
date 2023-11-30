use crate::sql::{Number, Thing};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::Mutex;

pub(super) struct KnnPriorityList(Arc<Mutex<Inner>>);

struct Inner {
	knn: usize,
	docs: HashSet<Arc<Thing>>,
	priority_list: BTreeMap<Number, HashSet<Arc<Thing>>>,
}
impl KnnPriorityList {
	pub(super) fn new(knn: usize) -> Self {
		Self(Arc::new(Mutex::new(Inner {
			knn,
			docs: HashSet::new(),
			priority_list: BTreeMap::default(),
		})))
	}

	pub(super) async fn add(&self, dist: Number, thing: &Thing) {
		let mut i = self.0.lock().await;
		if i.check_add(&dist) {
			i.add(dist, thing);
		}
	}

	pub(super) async fn build(&self) -> HashSet<Arc<Thing>> {
		self.0.lock().await.build()
	}
}

impl Inner {
	fn check_add(&self, dist: &Number) -> bool {
		if self.docs.len() < self.knn {
			true
		} else if let Some(d) = self.priority_list.keys().last() {
			d.gt(dist)
		} else {
			true
		}
	}

	pub(super) fn add(&mut self, dist: Number, thg: &Thing) {
		let thg = Arc::new(thg.clone());
		match self.priority_list.entry(dist) {
			Entry::Vacant(e) => {
				let mut h = HashSet::new();
				h.insert(thg.clone());
				e.insert(h);
				self.docs.insert(thg);
			}
			Entry::Occupied(mut e) => {
				let h = e.get_mut();
				h.insert(thg);
			}
		}

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

	fn build(&self) -> HashSet<Arc<Thing>> {
		let mut sorted_docs = VecDeque::with_capacity(self.knn);
		#[cfg(debug_assertions)]
		debug!("self.priority_list: {:?} - self.docs: {:?}", self.priority_list, self.docs);
		let mut left = self.knn;
		for docs in self.priority_list.values() {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left) {
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
			r.insert(id.clone());
		}
		r
	}
}
