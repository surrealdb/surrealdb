use crate::sql::{Expression, Number, Thing};
use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
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
			docs: HashSet::with_capacity(knn),
			priority_list: BTreeMap::default(),
		})))
	}

	pub(super) async fn add(&self, dist: Number, thing: &Thing) {
		let mut i = self.0.lock().await;
		if i.check_add(&dist) {
			i.add(dist, thing);
		}
		drop(i);
	}

	pub(super) async fn build(&self) -> HashMap<Arc<Thing>, Number> {
		let l = self.0.lock().await;
		let r = l.build();
		drop(l);
		r
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

	fn build(&self) -> HashMap<Arc<Thing>, Number> {
		let mut result = HashMap::with_capacity(self.knn);
		#[cfg(debug_assertions)]
		debug!("self.priority_list: {:?} - self.docs: {:?}", self.priority_list, self.docs);
		let mut left = self.knn;
		for (dist, docs) in &self.priority_list {
			let dl = docs.len();
			if dl > left {
				for doc_id in docs.iter().take(left) {
					result.insert(doc_id.clone(), dist.clone());
				}
				break;
			}
			for doc_id in docs {
				result.insert(doc_id.clone(), dist.clone());
			}
			left -= dl;
			// We don't expect anymore result, we can leave
			if left == 0 {
				break;
			}
		}
		result
	}
}

pub(crate) struct KnnBruteForceResult {
	exp: HashMap<Arc<Expression>, usize>,
	res: Vec<HashMap<Arc<Thing>, Number>>,
}

impl KnnBruteForceResult {
	pub(super) fn with_capacity(capacity: usize) -> Self {
		Self {
			exp: HashMap::with_capacity(capacity),
			res: Vec::with_capacity(capacity),
		}
	}

	pub(super) fn insert(&mut self, e: Arc<Expression>, m: HashMap<Arc<Thing>, Number>) {
		self.exp.insert(e.clone(), self.res.len());
		self.res.push(m);
	}
}

#[derive(Clone)]
pub(crate) struct KnnBruteForceResults(Arc<std::collections::HashMap<String, KnnBruteForceResult>>);

impl From<std::collections::HashMap<String, KnnBruteForceResult>> for KnnBruteForceResults {
	fn from(map: std::collections::HashMap<String, KnnBruteForceResult>) -> Self {
		Self(map.into())
	}
}
impl KnnBruteForceResults {
	pub(super) fn contains(&self, exp: &Expression, thg: &Thing) -> bool {
		if let Some(result) = self.0.get(thg.tb.as_str()) {
			if let Some(&pos) = result.exp.get(exp) {
				if let Some(things) = result.res.get(pos) {
					return things.contains_key(thg);
				}
			}
		}
		false
	}

	pub(crate) fn get_dist(&self, pos: usize, thg: &Thing) -> Option<Number> {
		if let Some(result) = self.0.get(thg.tb.as_str()) {
			if let Some(things) = result.res.get(pos) {
				return things.get(thg).cloned();
			}
		}
		None
	}
}
