use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::vector::SharedVector;
use crate::sql::index::Distance;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet};
use tokio::sync::RwLock;

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

	async fn insert(&mut self, o: SharedVector, d: DocId) {
		self.h.insert(o.clone()).await;
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

struct Hnsw<const M: usize, const M0: usize, const EFC: usize> {
	dist: Distance,
	ml: f64,
	layers: Vec<RwLock<Layer>>,
	enter_point: Option<ElementId>,
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
		debug!("NEW - M0: {M0} - M: {M} - ml: {ml:?} - dist: {dist:?}");
		Self {
			ml: ml.unwrap_or(1.0 / (M as f64).ln()),
			dist: dist.unwrap_or(Distance::Euclidean),
			enter_point: None,
			layers: Vec::default(),
			elements: Vec::default(),
			rng: SmallRng::from_entropy(),
		}
	}

	async fn insert(&mut self, q: SharedVector) -> ElementId {
		let id = self.elements.len() as ElementId;
		let level = self.get_random_level();
		let layers = self.layers.len();

		for l in layers..=level {
			debug!("Create Layer {l}");
			self.layers.push(RwLock::new(Layer::new()));
		}

		if let Some(ep) = self.enter_point.clone() {
			self.insert_element(&q, ep, id, level, layers - 1).await;
		} else {
			self.insert_first_element(id, level).await;
		}

		self.elements.push(q);
		id
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	async fn insert_first_element(&mut self, id: ElementId, level: usize) {
		debug!("insert_first_element - id: {id} - level: {level}");
		for lc in 0..=level {
			self.layers[lc].write().await.0.insert(id, vec![]);
		}
		self.enter_point = Some(id);
		debug!("E - EP: {id}");
	}

	async fn insert_element(
		&mut self,
		q: &SharedVector,
		mut ep: ElementId,
		id: ElementId,
		level: usize,
		top_layer_level: usize,
	) {
		debug!("insert_element q: {q:?} - id: {id} - level: {level} -  ep: {ep:?} - top-layer: {top_layer_level}");

		for lc in ((level + 1)..=top_layer_level).rev() {
			debug!("1 - LC: {lc}");
			let w = self.search_layer(q, ep, 1, lc).await;
			debug!("1 - W: {w:?}");
			if let Some(n) = w.first() {
				ep = n.1;
				debug!("1 - EP: {ep}");
			}
		}

		// TODO: One thread per level
		let mut m_max = M;
		for lc in (0..=top_layer_level.min(level)).rev() {
			if lc == 0 {
				m_max = M0;
			}
			debug!("2 - LC: {lc}");
			let w = self.search_layer(q, ep, EFC, lc).await;
			debug!("2 - W: {w:?}");
			let mut neighbors = Vec::with_capacity(m_max.min(w.len()));
			self.select_neighbors_simple(&w, m_max, &mut neighbors);
			debug!("2 - N: {neighbors:?}");
			// add bidirectional connections from neighbors to q at layer lc
			let mut layer = self.layers[lc].write().await;
			layer.0.insert(id, neighbors.clone());
			debug!("2 - Layer: {:?}", layer.0);
			for e_id in neighbors {
				if let Some(e_conn) = layer.0.get_mut(&e_id) {
					if e_conn.len() >= m_max {
						self.select_and_shrink_neighbors_simple(e_id, id, q, e_conn, m_max);
					} else {
						e_conn.push(id);
					}
				} else {
					unreachable!("Element: {}", e_id)
				}
			}
			if let Some(n) = w.first() {
				ep = n.1;
				debug!("2 - EP: {ep}");
			} else {
				unreachable!("W is empty")
			}
		}

		for lc in (top_layer_level + 1)..=level {
			let mut layer = self.layers[lc].write().await;
			if layer.0.insert(id, vec![]).is_some() {
				unreachable!("Already there {id}");
			}
		}

		if level > top_layer_level {
			self.enter_point = Some(id);
			debug!("E - EP: {id}");
		}
		self.debug_print_check().await;
	}

	async fn debug_print_check(&self) {
		debug!("EP: {:?}", self.enter_point);
		for (i, l) in self.layers.iter().enumerate() {
			let l = l.read().await;
			debug!("LAYER {i} {:?}", l.0);
			let m_max = if i == 0 {
				M0
			} else {
				M
			};
			for f in l.0.values() {
				assert!(f.len() <= m_max);
			}
		}
	}

	/// query element q
	/// enter points ep
	/// number of nearest to q
	/// elements to return ef
	/// layer number lc
	/// Output: ef closest neighbors to q
	async fn search_layer(
		&self,
		q: &SharedVector,
		ep_id: ElementId,
		ef: usize,
		lc: usize,
	) -> BTreeSet<PriorityNode> {
		let ep_dist = self.distance(&self.elements[ep_id as usize], q);
		let ep_pr = PriorityNode(ep_dist, ep_id);
		let mut candidates = BTreeSet::from([ep_pr.clone()]);
		let mut w = BTreeSet::from([ep_pr]);
		let mut visited = HashSet::from([ep_id]);
		while let Some(c) = candidates.pop_first() {
			let f_dist = candidates.last().map(|f| f.0).unwrap_or(c.0);
			if c.0 > f_dist {
				break;
			}
			for (&e_id, e_neighbors) in &self.layers[lc].read().await.0 {
				if e_neighbors.contains(&c.1) {
					if visited.insert(e_id) {
						let e_dist = self.distance(&self.elements[e_id as usize], q);
						if e_dist < f_dist || w.len() < ef {
							candidates.insert(PriorityNode(e_dist, e_id));
							w.insert(PriorityNode(e_dist, e_id));
							if w.len() > ef {
								w.pop_last();
							}
						}
					}
				}
			}
		}
		w
	}

	fn select_and_shrink_neighbors_simple(
		&self,
		e_id: ElementId,
		new_f_id: ElementId,
		new_f: &SharedVector,
		elements: &mut Vec<ElementId>,
		m_max: usize,
	) {
		let e = &self.elements[e_id as usize];
		let mut w = BTreeSet::default();
		w.insert(PriorityNode(self.distance(e, new_f), new_f_id));
		for f_id in elements.drain(..) {
			let f_dist = self.distance(&self.elements[f_id as usize], e);
			w.insert(PriorityNode(f_dist, f_id));
		}
		self.select_neighbors_simple(&w, m_max, elements);
	}

	fn select_neighbors_simple(
		&self,
		w: &BTreeSet<PriorityNode>,
		m_max: usize,
		neighbors: &mut Vec<ElementId>,
	) {
		for pr in w {
			neighbors.push(pr.1);
			if neighbors.len() == m_max {
				break;
			}
		}
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

	async fn insert_collection_one_by_one<const M: usize, const M0: usize, const EFC: usize>(
		h: &mut HnswIndex<M, M0, EFC>,
		collection: &TestCollection,
	) -> Result<HashMap<DocId, SharedVector>, Error> {
		let mut map = HashMap::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id).await;
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

	async fn test_hnsw_collection<const M: usize, const M0: usize, const EFC: usize>(
		distance: Distance,
		collection: &TestCollection,
	) -> Result<(), Error> {
		let mut h: HnswIndex<M, M0, EFC> = HnswIndex::new(distance);
		insert_collection_one_by_one::<M, M0, EFC>(&mut h, collection).await?;
		find_collection::<M, M0, EFC>(&mut h, &collection)?;
		Ok(())
	}

	async fn test_hnsw_collection_distances<
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
			test_hnsw_collection::<M, M0, EFC>(distance, &collection).await?;
		}
		Ok(())
	}

	#[test(tokio::test)]
	async fn test_hnsw_unique_xs() -> Result<(), Error> {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			const DIM: usize = 10;
			test_hnsw_collection_distances::<DIM, 12, 24, 500>(
				vt,
				TestCollection::new_unique(DIM, vt, 2),
			)
			.await?;
		}
		Ok(())
	}
}
