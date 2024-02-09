use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::vector::{SharedVector, TreeVector};
use crate::kvs::Key;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::{Array, Thing, Value};
use radix_trie::Trie;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct HnswIndex {
	dim: usize,
	vector_type: VectorType,
	hnsw: Hnsw,
	vec_docs: BTreeMap<SharedVector, (Ids64, ElementId)>,
	doc_ids: Trie<Key, DocId>,
	ids_doc: Vec<Thing>,
}

impl HnswIndex {
	pub(crate) fn new(p: &HnswParams) -> Self {
		let doc_ids = Trie::default();
		let ids_doc = Vec::default();
		let dim = p.dimension as usize;
		let vector_type = p.vector_type;
		let hnsw = Hnsw::new(p);
		let vec_docs = BTreeMap::new();
		HnswIndex {
			dim,
			vector_type,
			hnsw,
			vec_docs,
			doc_ids,
			ids_doc,
		}
	}

	pub(crate) async fn index_document(
		&mut self,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		// Resolve the doc_id
		let doc_key: Key = rid.into();
		let doc_id = if let Some(doc_id) = self.doc_ids.get(&doc_key) {
			*doc_id
		} else {
			let doc_id = self.ids_doc.len() as DocId;
			self.ids_doc.push(rid.clone());
			self.doc_ids.insert(doc_key, doc_id);
			doc_id
		};
		// Index the values
		for v in content {
			// Extract the vector
			let vector = TreeVector::try_from_value(self.vector_type, self.dim, v)?;
			vector.check_dimension(self.dim)?;
			self.insert(Arc::new(vector), doc_id).await;
		}
		Ok(())
	}

	async fn insert(&mut self, o: SharedVector, d: DocId) {
		match self.vec_docs.entry(o) {
			Entry::Occupied(mut e) => {
				let (docs, element_id) = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					let element_id = *element_id;
					e.insert((new_docs, element_id));
				}
			}
			Entry::Vacant(e) => {
				let o = e.key().clone();
				let element_id = self.hnsw.insert(o).await;
				e.insert((Ids64::One(d), element_id));
			}
		}
	}

	async fn remove(&mut self, o: SharedVector, d: DocId) -> bool {
		if let Entry::Occupied(mut e) = self.vec_docs.entry(o) {
			let (docs, e_id) = e.get_mut();
			if let Some(new_docs) = docs.remove(d) {
				let e_id = *e_id;
				if new_docs.is_empty() {
					e.remove();
					return self.hnsw.remove(e_id).await;
				} else {
					e.insert((new_docs, e_id));
					return true;
				}
			}
		}
		false
	}

	pub(crate) async fn remove_document(
		&mut self,
		rid: &Thing,
		content: Vec<Value>,
	) -> Result<(), Error> {
		let doc_key: Key = rid.into();
		if let Some(doc_id) = self.doc_ids.get(&doc_key).cloned() {
			for v in content {
				// Extract the vector
				let vector = TreeVector::try_from_value(self.vector_type, self.dim, v)?;
				vector.check_dimension(self.dim)?;
				// Remove the vector
				self.remove(Arc::new(vector), doc_id).await;
			}
		}
		Ok(())
	}

	pub(crate) async fn knn_search(
		&self,
		a: Array,
		n: usize,
		ef: usize,
	) -> Result<VecDeque<(Thing, f64)>, Error> {
		// Extract the vector
		let vector = Arc::new(TreeVector::try_from_array(self.vector_type, a)?);
		vector.check_dimension(self.dim)?;
		// Do the search
		let res = self.search(&vector, n, ef).await;
		Ok(self.result(res))
	}

	fn result(&self, res: KnnResult) -> VecDeque<(Thing, f64)> {
		res.docs
			.into_iter()
			.map(|(doc_id, dist)| (self.ids_doc[doc_id as usize].clone(), dist))
			.collect()
	}

	async fn search(&self, o: &SharedVector, n: usize, ef: usize) -> KnnResult {
		let neighbors = self.hnsw.knn_search(o, n, ef).await;

		let mut builder = KnnResultBuilder::new(n);
		for pn in neighbors {
			if builder.check_add(pn.0) {
				let v = &self.hnsw.elements[&pn.1];
				if let Some((docs, _)) = self.vec_docs.get(v) {
					builder.add(pn.0, docs);
				}
			}
		}

		builder.build(
			#[cfg(debug_assertions)]
			HashMap::new(),
		)
	}
}

struct Hnsw {
	m: usize,
	m0: usize,
	efc: usize,
	ml: f64,
	dist: Distance,
	layers: Vec<RwLock<Layer>>,
	enter_point: Option<ElementId>,
	elements: HashMap<ElementId, SharedVector>,
	next_element_id: ElementId,
	rng: SmallRng,
	neighbors: SelectNeighbors,
}

struct Layer(HashMap<ElementId, Vec<ElementId>>);

impl Layer {
	fn new() -> Self {
		// We set a capacity of 1, because we create a layer because we are adding one element
		Self(HashMap::with_capacity(1))
	}
}

type ElementId = u64;

impl Hnsw {
	fn new(p: &HnswParams) -> Self {
		Self {
			m: p.m as usize,
			m0: p.m0 as usize,
			efc: p.ef_construction as usize,
			ml: p.ml.to_float(),
			dist: p.distance.clone(),
			enter_point: None,
			layers: Vec::default(),
			elements: HashMap::default(),
			next_element_id: 0,
			rng: SmallRng::from_entropy(),
			neighbors: p.into(),
		}
	}

	async fn insert(&mut self, q_pt: SharedVector) -> ElementId {
		let q_level = self.get_random_level();
		self.insert_level(q_pt, q_level).await
	}

	async fn insert_level(&mut self, q_pt: SharedVector, q_level: usize) -> ElementId {
		let q_id = self.next_element_id;
		let layers = self.layers.len();

		for _l in layers..=q_level {
			#[cfg(debug_assertions)]
			debug!("Create Layer {_l}");
			self.layers.push(RwLock::new(Layer::new()));
		}

		self.elements.insert(q_id, q_pt.clone());

		if let Some(ep_id) = self.enter_point {
			self.insert_element(q_id, &q_pt, q_level, ep_id, layers - 1).await;
		} else {
			self.insert_first_element(q_id, q_level).await;
		}

		self.next_element_id += 1;
		q_id
	}

	async fn remove(&mut self, e_id: ElementId) -> bool {
		#[cfg(debug_assertions)]
		debug!("Remove {e_id}");

		let mut removed = false;

		let e_pt = self.elements.get(&e_id).cloned();
		if let Some(e_pt) = e_pt {
			let layers = self.layers.len();
			let mut new_enter_point = None;

			// Are we deleting the current enter point?
			if Some(e_id) == self.enter_point {
				let layer = self.layers[layers - 1].read().await;
				let mut w = BTreeSet::new();
				self.search_layer(&e_pt, e_id, 1, &layer, &mut w, Some(e_id)).await;
				new_enter_point = w.first().map(|pn| pn.1);
			}

			self.elements.remove(&e_id);

			let mut m_max = self.m;
			// TODO one thread per layer
			for lc in (0..layers).rev() {
				if lc == 0 {
					m_max = self.m0;
				}
				let mut layer = self.layers[lc].write().await;
				if let Some(f_ids) = layer.0.remove(&e_id) {
					for q_id in f_ids {
						if let Some(q_pt) = self.elements.get(&q_id) {
							let mut c = BTreeSet::new();
							self.search_layer(q_pt, q_id, self.efc, &layer, &mut c, Some(q_id))
								.await;
							let neighbors =
								self.neighbors.select(self, &layer, q_id, q_pt, c, m_max);
							assert!(
								!neighbors.contains(&q_id),
								"!neighbors.contains(&q_id) = layer: {lc} - q_id: {q_id} - f_ids: {neighbors:?}"
							);
							layer.0.insert(q_id, neighbors);
						}
					}
					removed = true;
				}
			}

			if removed && Some(e_id) == self.enter_point {
				self.enter_point = new_enter_point;
			}
		}
		removed
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	async fn insert_first_element(&mut self, id: ElementId, level: usize) {
		#[cfg(debug_assertions)]
		debug!("insert_first_element - id: {id} - level: {level}");
		for lc in 0..=level {
			self.layers[lc].write().await.0.insert(id, vec![]);
		}
		self.enter_point = Some(id);
		#[cfg(debug_assertions)]
		debug!("E - EP: {id}");
	}

	async fn insert_element(
		&mut self,
		q_id: ElementId,
		q_pt: &SharedVector,
		q_level: usize,
		mut ep_id: ElementId,
		top_layer_level: usize,
	) {
		#[cfg(debug_assertions)]
		debug!("insert_element q_pt: {q_pt:?} - q_id: {q_id} - level: {q_level} -  ep_id: {ep_id:?} - top-layer: {top_layer_level}");
		for lc in ((q_level + 1)..=top_layer_level).rev() {
			let l = self.layers[lc].read().await;
			let mut w = BTreeSet::new();
			self.search_layer(q_pt, ep_id, 1, &l, &mut w, None).await;
			if let Some(n) = w.first() {
				ep_id = n.1;
			}
		}

		// TODO: One thread per level
		let mut m_max = self.m;
		for lc in (0..=top_layer_level.min(q_level)).rev() {
			if lc == 0 {
				m_max = self.m0;
			}
			let mut w = BTreeSet::new();
			let mut layer = self.layers[lc].write().await;

			self.search_layer(q_pt, ep_id, self.efc, &layer, &mut w, None).await;

			// Extract ep for the next iteration (next layer)
			if let Some(n) = w.first() {
				ep_id = n.1;
			} else {
				unreachable!("W is empty")
			}

			let neighbors = self.neighbors.select(self, &layer, q_id, q_pt, w, m_max);
			layer.0.insert(q_id, neighbors.clone());
			for n_id in neighbors {
				let e_conn =
					layer.0.get_mut(&n_id).unwrap_or_else(|| unreachable!("Element: {}", n_id));
				e_conn.push(q_id);
				if e_conn.len() >= m_max {
					let n_pt = &self.elements[&n_id];
					let n_c = self.build_priority_list(n_id, e_conn);
					let conn_neighbors =
						self.neighbors.select(self, &layer, n_id, n_pt, n_c, m_max);
					layer.0.insert(n_id, conn_neighbors);
				}
			}
		}

		for lc in (top_layer_level + 1)..=q_level {
			let mut layer = self.layers[lc].write().await;
			if layer.0.insert(q_id, vec![]).is_some() {
				unreachable!("Already there {q_id}");
			}
		}

		if q_level > top_layer_level {
			self.enter_point = Some(q_id);
			#[cfg(debug_assertions)]
			debug!("E - ep_id: {q_id}");
		}
		#[cfg(debug_assertions)]
		self.debug_print_check().await;
	}

	fn build_priority_list(
		&self,
		e_id: ElementId,
		neighbors: &[ElementId],
	) -> BTreeSet<PriorityNode> {
		let e_pt = &self.elements[&e_id];
		let mut w = BTreeSet::default();
		for n_id in neighbors {
			if let Some(n_pt) = self.elements.get(n_id) {
				let dist = self.dist.calculate(e_pt, n_pt);
				w.insert(PriorityNode(dist, *n_id));
			} else {
				unreachable!() // Todo remove once deletion is implemented
			}
		}
		w
	}

	#[cfg(debug_assertions)]
	async fn debug_print_check(&self) {
		debug!("EP: {:?}", self.enter_point);
		for (i, l) in self.layers.iter().enumerate() {
			let l = l.read().await;
			debug!("LAYER {i} - len: {} - {:?}", l.0.len(), l.0);
			let m_max = if i == 0 {
				self.m0
			} else {
				self.m
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
		l: &Layer,
		w: &mut BTreeSet<PriorityNode>,
		ignore: Option<ElementId>,
	) {
		let ep_pt = &self.elements[&ep_id];
		let ep_dist = self.dist.calculate(ep_pt, q);
		let ep_pn = PriorityNode(ep_dist, ep_id);
		let mut visited = if let Some(i) = ignore {
			if i != ep_id {
				w.insert(ep_pn.clone());
			}
			HashSet::from([ep_id, i])
		} else {
			w.insert(ep_pn.clone());
			HashSet::from([ep_id])
		};
		let mut candidates = BTreeSet::from([ep_pn]);
		while let Some(c) = candidates.pop_first() {
			let mut f_dist = w.last().map(|pn| pn.0).unwrap_or(f64::MAX);
			if c.0 > f_dist {
				break;
			}
			if let Some(neighbourhood) = l.0.get(&c.1) {
				for e_id in neighbourhood {
					if visited.insert(*e_id) {
						if let Some(e_pt) = self.elements.get(e_id) {
							let e_dist = self.dist.calculate(e_pt, q);
							if e_dist <= f_dist || w.len() < ef {
								let pn = PriorityNode(e_dist, *e_id);
								candidates.insert(pn.clone());
								w.insert(pn);
								if w.len() > ef {
									w.pop_last();
								}
								f_dist = w.last().map(|pn| pn.0).unwrap_or(f64::MAX);
							}
						}
					}
				}
			}
		}
	}

	async fn knn_search(&self, q: &SharedVector, k: usize, ef: usize) -> Vec<PriorityNode> {
		#[cfg(debug_assertions)]
		let expected_w_len = self.elements.len().min(k);
		if let Some(mut ep) = self.enter_point {
			let mut w = BTreeSet::new();
			let l = self.layers.len();
			for lc in (1..l).rev() {
				let l = self.layers[lc].read().await;
				self.search_layer(q, ep, 1, &l, &mut w, None).await;
				if let Some(n) = w.first() {
					ep = n.1;
				} else {
					unreachable!()
				}
				w.clear();
			}
			{
				let l = self.layers[0].read().await;
				self.search_layer(q, ep, ef, &l, &mut w, None).await;
				#[cfg(debug_assertions)]
				if w.len() < expected_w_len {
					debug!(
						"0 search_layer - ep: {ep} - ef: {ef} - k: {k} - layer: {} - w: {}",
						l.0.len(),
						w.len()
					);
				}
				let w: Vec<PriorityNode> = w.into_iter().collect();
				w.into_iter().take(k).collect()
			}
		} else {
			vec![]
		}
	}
}

#[derive(Debug)]
enum SelectNeighbors {
	Heuristic,
	HeuristicExt,
	HeuristicKeep,
	HeuristicExtKeep,
}

impl From<&HnswParams> for SelectNeighbors {
	fn from(p: &HnswParams) -> Self {
		if p.keep_pruned_connections {
			if p.extend_candidates {
				Self::HeuristicExtKeep
			} else {
				Self::HeuristicKeep
			}
		} else if p.extend_candidates {
			Self::HeuristicExt
		} else {
			Self::Heuristic
		}
	}
}

impl SelectNeighbors {
	fn select(
		&self,
		h: &Hnsw,
		lc: &Layer,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: BTreeSet<PriorityNode>,
		m_max: usize,
	) -> Vec<ElementId> {
		match self {
			Self::Heuristic => Self::heuristic(c, m_max),
			Self::HeuristicExt => Self::heuristic_ext(h, lc, q_id, q_pt, c, m_max),
			Self::HeuristicKeep => Self::heuristic_keep(c, m_max),
			Self::HeuristicExtKeep => Self::heuristic_ext_keep(h, lc, q_id, q_pt, c, m_max),
		}
	}
	fn heuristic(mut c: BTreeSet<PriorityNode>, m_max: usize) -> Vec<ElementId> {
		let mut r = Vec::with_capacity(m_max.min(c.len()));
		let mut closest_neighbors_distance = f64::MAX;
		while let Some(e) = c.pop_first() {
			if e.0 < closest_neighbors_distance {
				r.push(e.1);
				closest_neighbors_distance = e.0;
				if r.len() >= m_max {
					break;
				}
			}
		}
		r
	}

	fn heuristic_keep(mut c: BTreeSet<PriorityNode>, m_max: usize) -> Vec<ElementId> {
		let mut r = Vec::with_capacity(m_max.min(c.len()));
		let mut closest_neighbors_distance = f64::INFINITY;
		let mut wd = Vec::new();
		while let Some(e) = c.pop_first() {
			if e.0 < closest_neighbors_distance {
				r.push(e.1);
				closest_neighbors_distance = e.0;
				if r.len() >= m_max {
					break;
				}
			} else {
				wd.push(e);
			}
		}
		let d = (m_max - r.len()).min(wd.len());
		if d > 0 {
			wd.drain(0..d).for_each(|e| r.push(e.1));
		}
		r
	}

	fn extand(
		h: &Hnsw,
		lc: &Layer,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: &mut BTreeSet<PriorityNode>,
		m_max: usize,
	) {
		let mut ex: HashSet<ElementId> = c.iter().map(|pn| pn.1).collect();
		let mut ext = Vec::with_capacity(m_max.min(c.len()));
		for e in c.iter() {
			for &e_adj in lc.0.get(&e.1).unwrap_or_else(|| unreachable!("Missing element {}", e.1))
			{
				if e_adj != q_id && ex.insert(e_adj) {
					if let Some(pt) = h.elements.get(&e_adj) {
						ext.push(PriorityNode(h.dist.calculate(q_pt, pt), e_adj));
					}
				}
			}
		}
		for pn in ext {
			c.insert(pn);
		}
	}

	fn heuristic_ext(
		h: &Hnsw,
		lc: &Layer,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: BTreeSet<PriorityNode>,
		m_max: usize,
	) -> Vec<ElementId> {
		Self::extand(h, lc, q_id, q_pt, &mut c, m_max);
		Self::heuristic(c, m_max)
	}

	fn heuristic_ext_keep(
		h: &Hnsw,
		lc: &Layer,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: BTreeSet<PriorityNode>,
		m_max: usize,
	) -> Vec<ElementId> {
		Self::extand(h, lc, q_id, q_pt, &mut c, m_max);
		Self::heuristic_keep(c, m_max)
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::{Hnsw, HnswIndex};
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, TreeVector};
	use crate::sql::index::{Distance, HnswParams, VectorType};
	use roaring::RoaringTreemap;
	use serial_test::serial;
	use std::collections::btree_map::Entry;
	use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
	use std::sync::Arc;

	async fn insert_collection_hnsw(
		h: &mut Hnsw,
		collection: &TestCollection,
	) -> BTreeSet<SharedVector> {
		let mut set = BTreeSet::new();
		for (_, obj) in collection.as_ref() {
			h.insert(obj.clone()).await;
			set.insert(obj.clone());
			check_hnsw_properties(h, set.len()).await;
		}
		set
	}
	async fn find_collection_hnsw(h: &mut Hnsw, collection: &TestCollection) {
		let max_knn = 20.min(collection.as_ref().len());
		for (_, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.knn_search(obj, knn, 80).await;
				if collection.is_unique() {
					let mut found = false;
					for pn in &res {
						if h.elements[&pn.1].eq(obj) {
							found = true;
							break;
						}
					}
					assert!(
						found,
						"Search: {:?} - Knn: {} - Vector not found - Got: {:?} - Dist: {} - Coll: {}",
						obj.len(),
						knn,
						res,
						h.dist,
						collection.as_ref().len(),
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				if expected_len != res.len() {
					info!("expected_len != res.len()")
				}
				assert_eq!(
					expected_len,
					res.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {} - Dist: {} - Res: {:?}",
					expected_len,
					res.len(),
					collection.as_ref().len(),
					h.dist,
					res,
				)
			}
		}
	}

	async fn test_hnsw_collection(p: &HnswParams, collection: &TestCollection) {
		let mut h = Hnsw::new(p);
		insert_collection_hnsw(&mut h, collection).await;
		find_collection_hnsw(&mut h, &collection).await;
	}

	fn new_params(
		dimension: usize,
		vector_type: VectorType,
		distance: Distance,
		m: usize,
		extend_candidates: bool,
		keep_pruned_connections: bool,
	) -> HnswParams {
		let m = m as u16;
		let m0 = m * 2;
		HnswParams {
			dimension: dimension as u16,
			distance,
			vector_type,
			m,
			m0,
			ef_construction: 500,
			ml: (1.0 / (m as f64).ln()).into(),
			extend_candidates,
			keep_pruned_connections,
		}
	}

	async fn test_hnsw(
		distance: Distance,
		vt: VectorType,
		collection_size: usize,
		dimension: usize,
		m: usize,
		extend_candidates: bool,
		keep_pruned_connections: bool,
	) {
		info!("test_hnsw - dist: {distance} - type: {vt} - coll size: {collection_size} - dim: {dimension} - m: {m} - ext: {extend_candidates} - keep: {keep_pruned_connections}");
		let collection = TestCollection::new(true, collection_size, vt, dimension, &distance);
		let params =
			new_params(dimension, vt, distance, m, extend_candidates, keep_pruned_connections);
		test_hnsw_collection(&params, &collection).await;
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_hnsw_xs() {
		for d in [
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::Euclidean,
			Distance::Hamming,
			Distance::Jaccard,
			Distance::Manhattan,
			Distance::Minkowski(2.into()),
			Distance::Pearson,
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for extend in [false, true] {
					for keep in [false, true] {
						test_hnsw(d.clone(), vt, 30, 3, 12, extend, keep).await;
					}
				}
			}
		}
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_hnsw_small_euclidean_check() {
		test_hnsw(Distance::Euclidean, VectorType::F64, 100, 2, 24, true, true).await
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_hnsw_small() {
		for d in [
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::Euclidean,
			Distance::Hamming,
			Distance::Jaccard,
			Distance::Manhattan,
			Distance::Minkowski(2.into()),
			Distance::Pearson,
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for extend in [false, true] {
					for keep in [false, true] {
						test_hnsw(d.clone(), vt, 200, 5, 12, extend, keep).await;
					}
				}
			}
		}
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_hnsw_large_euclidean() {
		test_hnsw(Distance::Euclidean, VectorType::F64, 200, 5, 12, false, false).await
	}

	async fn insert_collection_hnsw_index(
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> BTreeMap<SharedVector, HashSet<DocId>> {
		let mut map: BTreeMap<SharedVector, HashSet<DocId>> = BTreeMap::new();
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id).await;
			match map.entry(obj.clone()) {
				Entry::Occupied(mut e) => {
					e.get_mut().insert(*doc_id);
				}
				Entry::Vacant(e) => {
					e.insert(HashSet::from([*doc_id]));
				}
			}
			check_hnsw_properties(&h.hnsw, map.len()).await;
		}
		map
	}

	async fn find_collection_hnsw_index(h: &mut HnswIndex, collection: &TestCollection) {
		let max_knn = 20.min(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn, 500).await;
				if knn == 1 && res.docs.len() == 1 && res.docs[0].1 > 0.0 {
					let docs: Vec<DocId> = res.docs.iter().map(|(d, _)| *d).collect();
					if collection.is_unique() {
						assert!(
							docs.contains(doc_id),
							"Search: {:?} - Knn: {} - Wrong Doc - Expected: {} - Got: {:?}",
							obj,
							knn,
							doc_id,
							res.docs
						);
					}
				}
				let expected_len = collection.as_ref().len().min(knn);
				assert_eq!(
					expected_len,
					res.docs.len(),
					"Wrong knn count - Expected: {} - Got: {} - - Docs: {:?} - Collection: {}",
					expected_len,
					res.docs.len(),
					res.docs,
					collection.as_ref().len(),
				)
			}
		}
	}

	async fn delete_hnsw_index_collection(
		h: &mut HnswIndex,
		collection: &TestCollection,
		mut map: BTreeMap<SharedVector, HashSet<DocId>>,
	) {
		for (doc_id, obj) in collection.as_ref() {
			assert!(h.remove(obj.clone(), *doc_id).await, "Delete failed: {:?} {}", obj, doc_id);
			if let Entry::Occupied(mut e) = map.entry(obj.clone()) {
				let set = e.get_mut();
				set.remove(doc_id);
				if set.is_empty() {
					e.remove();
				}
			}
			check_hnsw_properties(&h.hnsw, map.len()).await;
		}
	}

	async fn test_hnsw_index(
		distance: Distance,
		vt: VectorType,
		collection_size: usize,
		dimension: usize,
		unique: bool,
		m: usize,
		extend_candidates: bool,
		keep_pruned_connections: bool,
	) {
		info!("test_hnsw_index - dist: {distance} - type: {vt} - coll size: {collection_size} - dim: {dimension} - unique: {unique} - m: {m} - ext: {extend_candidates} - keep: {keep_pruned_connections}");
		let collection = TestCollection::new(unique, collection_size, vt, dimension, &distance);
		let p = new_params(dimension, vt, distance, m, extend_candidates, keep_pruned_connections);
		let mut h = HnswIndex::new(&p);
		let map = insert_collection_hnsw_index(&mut h, &collection).await;
		find_collection_hnsw_index(&mut h, &collection).await;
		delete_hnsw_index_collection(&mut h, &collection, map).await;
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_hnsw_index_xs() {
		for d in [
			Distance::Chebyshev,
			Distance::Cosine,
			Distance::Euclidean,
			Distance::Hamming,
			Distance::Jaccard,
			Distance::Manhattan,
			Distance::Minkowski(2.into()),
			Distance::Pearson,
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for unique in [false, true] {
					test_hnsw_index(d.clone(), vt, 30, 2, unique, 12, true, true).await;
				}
			}
		}
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_building() {
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 2, true, true);
		let mut hnsw = Hnsw::new(&p);
		assert_eq!(hnsw.elements.len(), 0);
		assert_eq!(hnsw.enter_point, None);
		assert_eq!(hnsw.layers.len(), 0);

		let a_vec = new_i16_vec(1, 1);
		let a0 = hnsw.insert_level(a_vec.clone(), 0).await;
		assert_eq!(hnsw.elements.len(), 1);
		assert_eq!(hnsw.enter_point, Some(a0));
		assert_eq!(hnsw.layers.len(), 1);
		assert_eq!(hnsw.layers[0].read().await.0, HashMap::from([(a0, vec![])]));

		let b1 = hnsw.insert_level(new_i16_vec(2, 2), 0).await;
		assert_eq!(hnsw.elements.len(), 2);
		assert_eq!(hnsw.enter_point, Some(a0));
		assert_eq!(hnsw.layers.len(), 1);
		assert_eq!(hnsw.layers[0].read().await.0, HashMap::from([(a0, vec![b1]), (b1, vec![a0])]));

		let c2 = hnsw.insert_level(new_i16_vec(3, 3), 0).await;
		assert_eq!(hnsw.elements.len(), 3);
		assert_eq!(hnsw.enter_point, Some(a0));
		assert_eq!(hnsw.layers.len(), 1);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([(a0, vec![b1, c2]), (b1, vec![a0, c2]), (c2, vec![b1, a0])])
		);

		let d3 = hnsw.insert_level(new_i16_vec(4, 4), 1).await;
		assert_eq!(hnsw.elements.len(), 4);
		assert_eq!(hnsw.enter_point, Some(d3));
		assert_eq!(hnsw.layers.len(), 2);
		assert_eq!(hnsw.layers[1].read().await.0, HashMap::from([(d3, vec![])]));
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3]),
				(b1, vec![a0, c2, d3]),
				(c2, vec![b1, a0, d3]),
				(d3, vec![c2, b1, a0])
			])
		);

		let e4 = hnsw.insert_level(new_i16_vec(5, 5), 2).await;
		assert_eq!(hnsw.elements.len(), 5);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![])]));
		assert_eq!(hnsw.layers[1].read().await.0, HashMap::from([(d3, vec![e4]), (e4, vec![d3])]));
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, a0]),
				(e4, vec![d3, c2, b1, a0])
			])
		);

		let f5 = hnsw.insert_level(new_i16_vec(6, 6), 2).await;
		assert_eq!(hnsw.elements.len(), 6);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([(d3, vec![e4, f5]), (e4, vec![d3, f5]), (f5, vec![e4, d3])])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, b1]),
				(f5, vec![e4, d3, c2, b1]),
			])
		);

		let g6 = hnsw.insert_level(new_i16_vec(7, 7), 1).await;
		assert_eq!(hnsw.elements.len(), 7);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([
				(d3, vec![e4, f5]),
				(e4, vec![d3, f5]),
				(f5, vec![e4, g6]),
				(g6, vec![f5, e4])
			])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, g6]),
				(f5, vec![e4, g6, d3, c2]),
				(g6, vec![f5, e4, d3, c2]),
			])
		);

		let h7 = hnsw.insert_level(new_i16_vec(8, 8), 0).await;
		assert_eq!(hnsw.elements.len(), 8);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([
				(d3, vec![e4, f5]),
				(e4, vec![d3, f5]),
				(f5, vec![e4, g6]),
				(g6, vec![f5, e4])
			])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, g6]),
				(f5, vec![e4, g6, d3, h7]),
				(g6, vec![f5, h7, e4, d3]),
				(h7, vec![g6, f5, e4, d3]),
			])
		);

		let i8 = hnsw.insert_level(new_i16_vec(9, 9), 0).await;
		assert_eq!(hnsw.elements.len(), 9);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([
				(d3, vec![e4, f5]),
				(e4, vec![d3, f5]),
				(f5, vec![e4, g6]),
				(g6, vec![f5, e4])
			])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, g6]),
				(f5, vec![e4, g6, d3, h7]),
				(g6, vec![f5, h7, e4, i8]),
				(h7, vec![g6, i8, f5, e4]),
				(i8, vec![h7, g6, f5, e4]),
			])
		);

		let j9 = hnsw.insert_level(new_i16_vec(10, 10), 0).await;
		assert_eq!(hnsw.elements.len(), 10);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([
				(d3, vec![e4, f5]),
				(e4, vec![d3, f5]),
				(f5, vec![e4, g6]),
				(g6, vec![f5, e4])
			])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, g6]),
				(f5, vec![e4, g6, d3, h7]),
				(g6, vec![f5, h7, e4, i8]),
				(h7, vec![g6, i8, f5, j9]),
				(i8, vec![h7, j9, g6, f5]),
				(j9, vec![i8, h7, g6, f5]),
			])
		);

		let h10 = hnsw.insert_level(new_i16_vec(11, 11), 1).await;
		assert_eq!(hnsw.elements.len(), 11);
		assert_eq!(hnsw.enter_point, Some(e4));
		assert_eq!(hnsw.layers.len(), 3);
		assert_eq!(hnsw.layers[2].read().await.0, HashMap::from([(e4, vec![f5]), (f5, vec![e4])]));
		assert_eq!(
			hnsw.layers[1].read().await.0,
			HashMap::from([
				(d3, vec![e4, f5]),
				(e4, vec![d3, f5]),
				(f5, vec![e4, g6]),
				(g6, vec![f5, e4]),
				(h10, vec![g6, f5])
			])
		);
		assert_eq!(
			hnsw.layers[0].read().await.0,
			HashMap::from([
				(a0, vec![b1, c2, d3, e4]),
				(b1, vec![a0, c2, d3, e4]),
				(c2, vec![b1, d3, a0, e4]),
				(d3, vec![c2, e4, b1, f5]),
				(e4, vec![d3, f5, c2, g6]),
				(f5, vec![e4, g6, d3, h7]),
				(g6, vec![f5, h7, e4, i8]),
				(h7, vec![g6, i8, f5, j9]),
				(i8, vec![h7, j9, g6, h10]),
				(j9, vec![i8, h10, h7, g6]),
				(h10, vec![j9, i8, h7, g6]),
			])
		);
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_invalid_size() {
		let collection = TestCollection::Unique(vec![
			(0, new_i16_vec(-2, -3)),
			(1, new_i16_vec(-2, 1)),
			(2, new_i16_vec(-4, 3)),
			(3, new_i16_vec(-3, 1)),
			(4, new_i16_vec(-1, 1)),
			(5, new_i16_vec(-2, 3)),
			(6, new_i16_vec(3, 0)),
			(7, new_i16_vec(-1, -2)),
			(8, new_i16_vec(-2, 2)),
			(9, new_i16_vec(-4, -2)),
			(10, new_i16_vec(0, 3)),
		]);
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, true, true);
		let mut h = Hnsw::new(&p);
		insert_collection_hnsw(&mut h, &collection).await;
		let pt = new_i16_vec(-2, -3);
		let knn = 10;
		let efs = 501;
		let hnsw_res = h.knn_search(&pt, knn, efs).await;
		assert_eq!(hnsw_res.len(), knn);
		// let brute_force_res = collection.knn(&pt, Distance::Euclidean, knn);
		// let recall = brute_force_res.recall(&hnsw_res);
		// assert_eq!(1.0, recall);
	}

	#[test_log::test(tokio::test)]
	#[serial]
	async fn test_recall() {
		let (dim, vt, m, size) = (5, VectorType::F64, 24, 500);
		let collection = TestCollection::new(true, size, vt, dim, &Distance::Euclidean);
		let p = new_params(dim, vt, Distance::Euclidean, m, true, true);
		let mut h = HnswIndex::new(&p);
		insert_collection_hnsw_index(&mut h, &collection).await;

		let mut last_recall = 0.0;
		for efs in [10, 20, 40, 80] {
			let mut total_recall = 0.0;
			for (doc_id, pt) in collection.as_ref() {
				let knn = 10;
				let hnsw_res = h.search(pt, knn, efs).await;
				assert_eq!(
					hnsw_res.docs.len(),
					knn,
					"Different size - knn: {knn} - doc: {doc_id} - efs: {efs} - docs: {:?}",
					collection.as_ref().len()
				);
				let brute_force_res = collection.knn(pt, Distance::Euclidean, knn);
				total_recall += brute_force_res.recall(&hnsw_res);
			}
			let recall = total_recall / collection.as_ref().len() as f64;
			assert!(recall >= 0.9, "Recall: {} - Last: {}", recall, last_recall);
			assert!(recall >= last_recall, "Recall: {} - Last: {}", recall, last_recall);
			last_recall = recall;
		}
	}

	async fn check_hnsw_properties(h: &Hnsw, expected_count: usize) {
		// let mut deleted_foreign_elements = 0;
		// let mut foreign_elements = 0;
		let mut layer_size = h.elements.len();
		assert_eq!(layer_size, expected_count);
		for (lc, l) in h.layers.iter().enumerate() {
			let l = l.read().await;
			assert!(l.0.len() <= layer_size, "{} - {}", l.0.len(), layer_size);
			layer_size = l.0.len();
			let m_layer = if lc == 0 {
				h.m0
			} else {
				h.m
			};
			for (e_id, f_ids) in &l.0 {
				assert!(f_ids.len() <= m_layer, "Foreign list len");
				assert!(
					!f_ids.contains(e_id),
					"!f_ids.contains(e_id) = layer: {lc} - el: {e_id} - f_ids: {f_ids:?}"
				);
				assert!(
					h.elements.contains_key(e_id),
					"h.elements.contains_key(e_id) - layer: {lc} - el: {e_id} - f_ids: {f_ids:?}"
				);

				// for f_id in f_ids {
				// 	if !h.elements.contains_key(f_id) {
				// 		deleted_foreign_elements += 1;
				// 	}
				// }
				// foreign_elements += f_ids.len();
			}
		}
		// if deleted_foreign_elements > 0 && deleted_foreign_elements > 0 {
		// 	let miss_rate = deleted_foreign_elements as f64 / foreign_elements as f64;
		// 	assert!(miss_rate < 0.5, "Miss rate: {miss_rate}");
		// }
	}

	impl TestCollection {
		fn knn(&self, pt: &SharedVector, dist: Distance, n: usize) -> KnnResult {
			let mut b = KnnResultBuilder::new(n);
			for (doc_id, doc_pt) in self.as_ref() {
				let d = dist.calculate(doc_pt, pt);
				if b.check_add(d) {
					b.add(d, &Ids64::One(*doc_id));
				}
			}
			b.build(HashMap::new())
		}
	}

	impl KnnResult {
		fn recall(&self, res: &KnnResult) -> f64 {
			let mut bits = RoaringTreemap::new();
			for &(doc_id, _) in &self.docs {
				bits.insert(doc_id);
			}
			let mut found = 0;
			for &(doc_id, _) in &res.docs {
				if bits.contains(doc_id) {
					found += 1;
				}
			}
			found as f64 / bits.len() as f64
		}
	}

	fn new_i16_vec(x: isize, y: isize) -> SharedVector {
		let mut vec = TreeVector::new(VectorType::I16, 2);
		vec.add(x.into());
		vec.add(y.into());
		Arc::new(vec)
	}
}
