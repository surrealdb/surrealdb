use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};

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
struct EntryPoint {
	pos: usize,
	layer: usize,
}

struct Hnsw<const M: usize, const M0: usize, const EFC: usize> {
	dist: Distance,
	/// `ml` is multi-layer factor to determine the number of neighbors in different layers.
	ml: f64,
	/// `top_ep` is the entry point ID for the top layer of HNSW graph.
	top_ep: EntryPoint,
	/// `zero` is a vector that stores the neighbors for the zero layer. M0 here denotes the
	/// maximum number of connections each node can have in the zero (base) layer.
	/// `layers` is a vector of nodes where each node represent a data point. The entire vector represents a layer in
	/// the graph. The index of each element in the vector is the ID of the data point.
	layers: Vec<Vec<Node<M, M0>>>,
	/// `elements` stores the actual data points (vectors). Each entry in this vector is a reference-counted SharedVector
	/// type, so we can share these vectors across different layers/nodes without duplication.
	elements: Vec<SharedVector>,
	/// `rng` is used to generate the random level (layer).
	/// a new node will be inserted into during the graph construction.
	rng: SmallRng,
}

type ElementId = u64;

pub struct Node<const M: usize, const M0: usize> {
	point: ElementId,
	neighbors: Neighbors<M, M0>,
}

pub enum Neighbors<const M: usize, const M0: usize> {
	Zero([ElementId; M0]),
	Upper([ElementId; M]),
}

impl<const M: usize, const M0: usize> Neighbors<M, M0> {
	fn contains(&self, e: &ElementId) -> bool {
		match self {
			Neighbors::Zero(a) => a.contains(e),
			Neighbors::Upper(a) => a.contains(e),
		}
	}
}

impl<const M: usize, const M0: usize, const EFC: usize> Default for Hnsw<M, M0, EFC> {
	fn default() -> Self {
		Self::new(None, None)
	}
}

impl<const M: usize, const M0: usize, const EFC: usize> Hnsw<M, M0, EFC> {
	fn new(ml: Option<f64>, dist: Option<Distance>) -> Self {
		println!("NEW - M0: {M0} - M: {M} - ml: {ml:?}");
		Self {
			ml: ml.unwrap_or(1.0 / (M as f64).ln()),
			dist: dist.unwrap_or(Distance::Euclidean),
			top_ep: EntryPoint {
				pos: 0,
				layer: 0,
			},
			layers: Vec::default(),
			elements: Vec::default(),
			rng: SmallRng::from_entropy(),
		}
	}

	fn insert(&mut self, q: SharedVector) -> usize {
		let id = self.elements.len();
		let level = self.get_random_level();
		println!("Insert q: {q:?} - id: {id} - level: {level}");
		self.elements.push(q.clone());
		if self.layers.is_empty() {
			self.insert_first_element(level)
		} else {
			self.insert_element(level, id, q)
		}
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		let layer = (-unif.ln() * self.ml).floor() as usize; // calculate the layer
		layer
	}

	fn insert_first_element(&mut self, level: usize) -> usize {
		while self.layers.len() <= level {
			// It's always index 0 with no neighbors since its the first element.
			let node = Node {
				point: 0,
				neighbors: Neighbors::Zero([!0; M0]),
			};
			self.layers.push(vec![node]);
			self.top_ep = EntryPoint {
				pos: 0,
				layer: 0,
			};
		}
		0
	}

	fn insert_element(&mut self, _id: usize, level: usize, q: SharedVector) -> usize {
		let ep = self.top_ep.clone();
		println!("layers: {} - ep: {ep:?}", self.layers.len());
		let ep_level = self.layers.len() - 1;

		for lc in (ep_level..level + 1).rev() {
			println!("LC: {lc}");
			let w = self.search_layer(&q, &ep, 1, lc);
		}

		todo!()
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
		ep: &EntryPoint,
		ef: usize,
		lc: usize,
	) -> BinaryHeap<PriorityNode> {
		let e = self.layers[ep.layer][ep.pos].point;
		let mut candidates = HashSet::from([e]);
		let mut visited = candidates.clone();
		let pr = PriorityNode::new(self.distance(&self.elements[e as usize], q), e);
		let mut w = BinaryHeap::from([pr]);
		while !candidates.is_empty() {
			let (c, c_dist) = self.get_nearest(q, &candidates);
			let (_, f_dist) = self.get_furthest(q, &candidates);
			if c_dist > f_dist {
				break;
			}
			for e in &self.layers[lc] {
				if e.neighbors.contains(&c) {
					if visited.insert(e.point) {
						let e_dist = self.distance(&self.elements[e.point as usize], q);
						if e_dist < f_dist || w.len() < ef {
							candidates.insert(e.point);
							w.push(PriorityNode::new(e_dist, e.point));
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

	fn get_nearest(&self, q: &SharedVector, candidates: &HashSet<ElementId>) -> (ElementId, f64) {
		let mut dist = f64::INFINITY;
		let mut n = 0;
		for &i in candidates {
			if let Some(e) = self.elements.get(i as usize) {
				let d = self.distance(e, q);
				if d < dist {
					n = i;
					dist = d;
				}
			}
		}
		(n, dist)
	}

	fn get_furthest(&self, q: &SharedVector, candidates: &HashSet<ElementId>) -> (ElementId, f64) {
		let mut dist = f64::INFINITY;
		let mut f = 0;
		for &i in candidates {
			if let Some(e) = self.elements.get(i as usize) {
				let d = self.distance(e, q);
				if d > dist {
					f = i;
					dist = d;
				}
			}
		}
		(f, dist)
	}

	fn distance(&self, v1: &SharedVector, v2: &SharedVector) -> f64 {
		self.dist.compute(v1, v2).unwrap_or(f64::INFINITY)
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
