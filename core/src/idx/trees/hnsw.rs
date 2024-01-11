use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, BinaryHeap, HashMap, HashSet};

struct HnswIndex<const M: usize, const M0: usize, const EFC: usize> {
	h: Hnsw<M, M0, EFC>,
	d: HashMap<SharedVector, Docs>,
}

impl<const M: usize, const M0: usize, const EFC: usize> HnswIndex<M, M0, EFC> {
	fn new(_distance: Distance) -> Self {
		let h = Hnsw::default();
		let d = HashMap::new();
		HnswIndex {
			h,
			d,
		}
	}

	fn insert(&mut self, o: SharedVector, d: DocId) {
		self.h.insert(o.clone());
		match self.d.entry(o) {
			Entry::Occupied(mut e) => {
				let docs = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					e.insert(new_docs);
				}
			}
			Entry::Vacant(e) => {
				e.insert(Docs::One(d));
			}
		}
	}

	fn search(&mut self, o: &SharedVector, n: usize, ef: usize) -> KnnResult {
		let neighbors = self.h.knn_search(o, n, ef);
		let mut builder = KnnResultBuilder::new(n);
		for (e, d) in neighbors {
			if builder.check_add(d) {
				if let Some(docs) = self.d.get(&e) {
					builder.add(d, docs);
				}
			}
		}

		builder.build(
			#[cfg(debug_assertions)]
			HashMap::new(),
		)
	}
}
#[derive(Debug, Clone)]
struct EnterPoint {
	id: ElementId,
	level: usize,
}

struct Hnsw<const M: usize, const M0: usize, const EFC: usize> {
	dist: Distance,
	ml: f64,
	layers: Vec<Layer>,
	enter_point: Option<EnterPoint>,
	elements: Vec<SharedVector>,
	rng: SmallRng,
}

struct Layer(HashMap<ElementId, Vec<ElementId>>);

impl Layer {
	fn new() -> Self {
		Self(HashMap::with_capacity(1))
	}
}

type ElementId = u64;

impl<const M: usize, const M0: usize, const EFC: usize> Default for Hnsw<M, M0, EFC> {
	fn default() -> Self {
		Self::new(None, None)
	}
}

impl<const M: usize, const M0: usize, const EFC: usize> Hnsw<M, M0, EFC> {
	fn new(ml: Option<f64>, dist: Option<Distance>) -> Self {
		println!("NEW - M0: {M0} - M: {M} - ml: {ml:?} - dist: {dist:?}");
		Self {
			ml: ml.unwrap_or(1.0 / (M as f64).ln()),
			dist: dist.unwrap_or(Distance::Euclidean),
			enter_point: None,
			layers: Vec::default(),
			elements: Vec::default(),
			rng: SmallRng::from_entropy(),
		}
	}

	fn insert(&mut self, q: SharedVector) -> ElementId {
		let id = self.elements.len() as ElementId;
		let level = self.get_random_level();

		for l in self.layers.len()..=level {
			println!("Create Layer {l}");
			self.layers.push(Layer::new());
		}

		if let Some(ep) = self.enter_point.clone() {
			self.insert_element(&q, ep, id, level);
		} else {
			self.insert_first_element(id, level);
		}

		self.elements.push(q);
		id
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	fn insert_first_element(&mut self, id: ElementId, level: usize) {
		println!("insert_first_element - id: {id} - level: {level}");
		for lc in 0..=level {
			self.layers[lc].0.insert(id, vec![]);
		}
		self.enter_point = Some(EnterPoint {
			id,
			level,
		})
	}

	fn insert_element(
		&mut self,
		q: &SharedVector,
		mut ep: EnterPoint,
		id: ElementId,
		level: usize,
	) {
		println!("insert_element q: {q:?} - id: {id} - level: {level} -  ep: {ep:?}");
		let graph_ep_level = ep.level;

		for lc in ((level + 1)..=ep.level).rev() {
			println!("1- LC: {lc}");
			let w = self.search_layer(q, &ep, 1, lc).into_sorted_vec();
			ep = EnterPoint {
				id: w[0].1,
				level: lc,
			}
		}

		for lc in (0..=ep.level.min(level)).rev() {
			println!("2- LC: {lc}");
			let w = self.search_layer(q, &ep, EFC, lc);
			println!("2- W: {w:?}");
			let neighbors = self.select_neighbors_simple(w, lc);
			println!("2- N: {neighbors:?}");
			let layer = &mut self.layers[lc].0;
			for e in neighbors {
				if let Some(elements) = layer.get_mut(&e) {
					elements.push(id);
				} else {
					unreachable!()
				}
				println!("e: : {e:?}");
			}

			todo!()
		}

		if level > graph_ep_level {
			self.enter_point = Some(EnterPoint {
				id,
				level,
			});
		}
	}

	/// query element q
	/// enter points ep
	/// number of nearest to q
	/// elements to return ef
	/// layer number lc
	/// Output: ef closest neighbors to q
	fn search_layer(
		&self,
		q: &SharedVector,
		ep: &EnterPoint,
		ef: usize,
		lc: usize,
	) -> BinaryHeap<PriorityNode> {
		let ep_dist = self.distance(&self.elements[ep.id as usize], q);
		let ep_pr = PriorityNode(ep_dist, ep.id);
		let mut candidates = BTreeSet::from([ep_pr.clone()]);
		let mut w = BinaryHeap::from([ep_pr]);
		let mut visited = HashSet::from([ep.id]);
		while let Some(c) = candidates.pop_first() {
			let f_dist = candidates.last().map(|f| f.0).unwrap_or(c.0);
			if c.0 > f_dist {
				break;
			}
			for (&e_id, e_neighbors) in &self.layers[lc].0 {
				if e_neighbors.contains(&c.1) {
					if visited.insert(e_id) {
						let e_dist = self.distance(&self.elements[e_id as usize], q);
						if e_dist < f_dist || w.len() < ef {
							candidates.insert(PriorityNode(e_dist, e_id));
							w.push(PriorityNode(e_dist, e_id));
							if w.len() > ef {
								w.pop();
							}
						}
					}
				}
			}
		}
		w
	}

	fn select_neighbors_simple(&self, w: BinaryHeap<PriorityNode>, lc: usize) -> Vec<ElementId> {
		let m = if lc == 0 {
			M0
		} else {
			M
		};
		let mut n = Vec::with_capacity(m);
		for pr in w {
			n.push(pr.1);
			if n.len() == m {
				break;
			}
		}
		n
	}

	fn distance(&self, v1: &SharedVector, v2: &SharedVector) -> f64 {
		v1.distance(&self.dist, v2).unwrap_or(f64::INFINITY)
	}

	fn knn_search(&self, _q: &SharedVector, _k: usize, _ef: usize) -> Vec<(SharedVector, f64)> {
		todo!()
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::HnswIndex;
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::vector::SharedVector;
	use crate::sql::index::{Distance, VectorType};
	use std::collections::HashMap;
	use test_log::test;

	fn insert_collection_one_by_one<const M: usize, const M0: usize, const EFC: usize>(
		h: &mut HnswIndex<M, M0, EFC>,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id);
			map.insert(*doc_id, obj.clone());
		}
		Ok(map)
	}

	fn find_collection<const M: usize, const M0: usize, const EFC: usize>(
		h: &mut HnswIndex<M, M0, EFC>,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn, 500);
				if collection.is_unique() {
					assert!(
						res.docs.contains(doc_id),
						"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
						obj,
						knn,
						doc_id,
						res.docs
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				assert_eq!(
					expected_len,
					res.docs.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {}",
					expected_len,
					res.docs.len(),
					collection.as_ref().len(),
				)
			}
		}
		Ok(())
	}

	fn test_hnsw_collection<const M: usize, const M0: usize, const EFC: usize>(
		distance: Distance,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let mut h: HnswIndex<M, M0, EFC> = HnswIndex::new(distance);
		insert_collection_one_by_one::<M, M0, EFC>(&mut h, collection)?;
		find_collection::<M, M0, EFC>(&mut h, &collection)?;
		Ok(())
	}

	fn test_hnsw_collection_distances<
		const D: usize,
		const M: usize,
		const M0: usize,
		const EFC: usize,
	>(
		vt: VectorType,
		collection: TestCollection,
	) -> Result<(), Error> {
		for distance in [
			Distance::Euclidean,
			Distance::Manhattan,
			Distance::Hamming,
			Distance::Minkowski(2.into()),
		] {
			debug!(
				"Distance: {:?} - Collection: {} - Vector type: {}",
				distance,
				collection.as_ref().len(),
				vt,
			);
			test_hnsw_collection::<M, M0, EFC>(distance, &collection)?;
		}
		Ok(())
	}

	#[test]
	fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			const DIM: usize = 10;
			test_hnsw_collection_distances::<DIM, 12, 24, 500>(
				vt,
				TestCollection::new_unique(DIM, vt, 2),
			)?;
		}
		Ok(())
	}
}
