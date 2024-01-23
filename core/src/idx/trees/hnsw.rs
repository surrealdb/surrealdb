use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::knn::{Docs, KnnResult, KnnResultBuilder, PriorityNode};
use crate::idx::trees::vector::{SharedVector, TreeVector};
use crate::kvs::Key;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::{Array, Thing, Value};
use radix_trie::Trie;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) struct HnswIndex {
	dim: usize,
	vector_type: VectorType,
	hnsw: Hnsw,
	vec_docs: HashMap<SharedVector, Docs>,
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
		let vec_docs = HashMap::new();
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
				let docs = e.get_mut();
				if let Some(new_docs) = docs.insert(d) {
					e.insert(new_docs);
				}
			}
			Entry::Vacant(e) => {
				let o = e.key().clone();
				e.insert(Docs::One(d));
				self.hnsw.insert(o).await;
			}
		}
	}

	async fn remove(&mut self, o: SharedVector, d: DocId) -> bool {
		if let Entry::Occupied(mut e) = self.vec_docs.entry(o) {
			if let Some(d) = e.get_mut().remove(d) {
				if d.is_empty() {
					let o = e.key().clone();
					e.remove();
					return self.hnsw.remove(o).await;
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
				let v = &self.hnsw.elements[pn.1 as usize];
				if let Some(docs) = self.vec_docs.get(v) {
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
	elements: Vec<SharedVector>,
	rng: SmallRng,
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
			elements: Vec::default(),
			rng: SmallRng::from_entropy(),
		}
	}

	async fn insert(&mut self, q: SharedVector) -> ElementId {
		let id = self.elements.len() as ElementId;
		let level = self.get_random_level();
		let layers = self.layers.len();

		for l in layers..=level {
			#[cfg(debug_assertions)]
			debug!("Create Layer {l}");
			self.layers.push(RwLock::new(Layer::new()));
		}

		if let Some(ep) = self.enter_point {
			self.insert_element(&q, ep, id, level, layers - 1).await;
		} else {
			self.insert_first_element(id, level).await;
		}

		self.elements.push(q);
		id
	}

	async fn remove(&mut self, _q: SharedVector) -> bool {
		todo!()
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
		q: &SharedVector,
		mut ep: ElementId,
		id: ElementId,
		level: usize,
		top_layer_level: usize,
	) {
		#[cfg(debug_assertions)]
		debug!("insert_element q: {q:?} - id: {id} - level: {level} -  ep: {ep:?} - top-layer: {top_layer_level}");
		for lc in ((level + 1)..=top_layer_level).rev() {
			let w = self.search_layer(q, ep, 1, lc).await;
			if let Some(n) = w.first() {
				ep = n.1;
			}
		}

		// TODO: One thread per level
		let mut m_max = self.m;
		for lc in (0..=top_layer_level.min(level)).rev() {
			if lc == 0 {
				m_max = self.m0;
			}
			#[cfg(debug_assertions)]
			debug!("2 - LC: {lc}");
			let w = self.search_layer(q, ep, self.efc, lc).await;
			#[cfg(debug_assertions)]
			debug!("2 - W: {w:?}");
			let mut neighbors = Vec::with_capacity(m_max.min(w.len()));
			self.select_neighbors_simple(&w, m_max, &mut neighbors);
			#[cfg(debug_assertions)]
			debug!("2 - N: {neighbors:?}");
			// add bidirectional connections from neighbors to q at layer lc
			let mut layer = self.layers[lc].write().await;
			layer.0.insert(id, neighbors.clone());
			#[cfg(debug_assertions)]
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
				#[cfg(debug_assertions)]
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
			#[cfg(debug_assertions)]
			debug!("E - EP: {id}");
		}
		self.debug_print_check().await;
	}

	#[cfg(debug_assertions)]
	async fn debug_print_check(&self) {
		debug!("EP: {:?}", self.enter_point);
		for (i, l) in self.layers.iter().enumerate() {
			let l = l.read().await;
			debug!("LAYER {i} {:?}", l.0);
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
				if e_neighbors.contains(&c.1) && visited.insert(e_id) {
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
		self.dist.dist(v1, v2)
	}

	async fn knn_search(&self, q: &SharedVector, k: usize, ef: usize) -> Vec<PriorityNode> {
		if let Some(mut ep) = self.enter_point {
			let l = self.layers.len();
			for lc in (1..l).rev() {
				let w = self.search_layer(q, ep, 1, lc).await;
				if let Some(n) = w.first() {
					ep = n.1;
				} else {
					unreachable!()
				}
			}
			let w = self.search_layer(q, ep, ef, 0).await;
			let w: Vec<PriorityNode> = w.into_iter().collect();
			w.into_iter().take(k).collect()
		} else {
			vec![]
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::docids::DocId;
	use crate::idx::trees::hnsw::{Hnsw, HnswIndex};
	use crate::idx::trees::knn::tests::TestCollection;
	use crate::idx::trees::vector::SharedVector;
	use crate::sql::index::{Distance, HnswParams, VectorType};
	use radix_trie::TrieCommon;
	use std::collections::HashSet;
	use test_log::test;

	async fn insert_collection_hnsw(
		h: &mut Hnsw,
		collection: &TestCollection,
	) -> HashSet<SharedVector> {
		let mut set = HashSet::with_capacity(collection.as_ref().len());
		for (_, obj) in collection.as_ref() {
			h.insert(obj.clone()).await;
			set.insert(obj.clone());
		}
		set
	}
	async fn find_collection_hnsw(h: &mut Hnsw, collection: &TestCollection) {
		let max_knn = 20.max(collection.as_ref().len());
		for (_, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.knn_search(obj, knn, 500).await;
				if collection.is_unique() {
					let mut found = false;
					for pn in &res {
						if h.elements[pn.1 as usize].eq(obj) {
							found = true;
							break;
						}
					}
					assert!(
						found,
						"Search: {:?} - Knn: {} - Vector not found - Got: {:?} - Dist: {} - Coll: {:?}",
						obj,
						knn,
						res,
						h.dist,
						collection,
					);
				}
				let expected_len = collection.as_ref().len().min(knn);
				assert_eq!(
					expected_len,
					res.len(),
					"Wrong knn count - Expected: {} - Got: {:?} - Dist: {} - Collection: {}",
					expected_len,
					res,
					h.dist,
					collection.as_ref().len(),
				)
			}
		}
	}

	async fn delete_hnsw_collection(h: &mut Hnsw, collection: &TestCollection) {
		let mut count = collection.as_ref().len();
		for (_, obj) in collection.as_ref() {
			assert!(h.remove(obj.clone()).await, "Delete failed: {:?}", obj);
			count -= 1;
			assert_eq!(h.elements.len(), count);
		}
	}

	async fn test_hnsw_collection(p: &HnswParams, collection: &TestCollection) {
		let mut h = Hnsw::new(p);
		insert_collection_hnsw(&mut h, collection).await;
		find_collection_hnsw(&mut h, &collection).await;
		delete_hnsw_collection(&mut h, &collection).await;
	}

	fn new_params(dimension: usize, vector_type: VectorType, distance: Distance) -> HnswParams {
		HnswParams {
			dimension: dimension as u16,
			distance,
			vector_type,
			m: 12,
			m0: 24,
			ef_construction: 500,
			ml: (1.0 / 12.0_f64.ln()).into(),
			doc_ids_order: 0,
			doc_ids_cache: 0,
		}
	}

	#[test(tokio::test)]
	async fn test_hnsw_unique_col_10_dim_2() {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for distance in [
				Distance::Euclidean,
				Distance::Manhattan,
				Distance::Hamming,
				Distance::Minkowski(2.into()),
				Distance::Chebyshev,
			] {
				let for_jaccard = distance == Distance::Jaccard;
				let dimension = 2;
				let params = new_params(dimension, vt, distance);
				test_hnsw_collection(
					&params,
					&TestCollection::new_unique(10, vt, dimension, for_jaccard),
				)
				.await;
			}
		}
	}

	#[test(tokio::test)]
	async fn test_hnsw_random_col_10_dim_2() {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for distance in [
				Distance::Chebyshev,
				Distance::Cosine,
				Distance::Euclidean,
				Distance::Hamming,
				Distance::Jaccard,
				Distance::Manhattan,
				Distance::Minkowski(2.into()),
				Distance::Pearson,
			] {
				let dimension = 2;
				let collection =
					TestCollection::new_random(10, vt, dimension, distance == Distance::Jaccard);
				let params = new_params(dimension, vt, distance);
				test_hnsw_collection(&params, &collection).await;
			}
		}
	}

	#[test(tokio::test)]
	async fn test_hnsw_unique_coll_20_dim_1536() {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			let dimension = 1536;
			let collection = TestCollection::new_unique(20, vt, dimension, false);
			let params = new_params(dimension, vt, Distance::Hamming);
			test_hnsw_collection(&params, &collection).await;
		}
	}

	async fn insert_collection_hnsw_index(
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> HashSet<SharedVector> {
		let mut set = HashSet::with_capacity(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			h.insert(obj.clone(), *doc_id).await;
			set.insert(obj.clone());
		}
		set
	}

	async fn find_collection_hnsw_index(h: &mut HnswIndex, collection: &TestCollection) {
		let max_knn = 20.max(collection.as_ref().len());
		for (doc_id, obj) in collection.as_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn, 500).await;
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

	async fn delete_hnsw_index_collection(h: &mut HnswIndex, collection: &TestCollection) {
		let mut count = collection.as_ref().len();
		for (doc_id, obj) in collection.as_ref() {
			assert!(h.remove(obj.clone(), *doc_id).await, "Delete failed: {:?} {}", obj, doc_id);
			count -= 1;
			assert_eq!(h.doc_ids.len(), count);
		}
	}

	async fn test_hnsw_index_collection(p: &HnswParams, collection: &TestCollection) {
		let mut h = HnswIndex::new(p);
		insert_collection_hnsw_index(&mut h, collection).await;
		find_collection_hnsw_index(&mut h, &collection).await;
		delete_hnsw_index_collection(&mut h, &collection).await;
	}

	#[test(tokio::test)]
	async fn test_hnsw_index_random_col_10_dim_2() {
		for vt in
			[VectorType::F64, VectorType::F32, VectorType::I64, VectorType::I32, VectorType::I16]
		{
			for distance in [
				Distance::Chebyshev,
				Distance::Cosine,
				Distance::Euclidean,
				Distance::Hamming,
				Distance::Jaccard,
				Distance::Manhattan,
				Distance::Minkowski(2.into()),
				Distance::Pearson,
			] {
				let dimension = 2;
				let collection =
					TestCollection::new_random(10, vt, dimension, distance == Distance::Jaccard);
				let params = new_params(dimension, vt, distance);
				test_hnsw_index_collection(&params, &collection).await;
			}
		}
	}
}
