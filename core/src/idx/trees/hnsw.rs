use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::trees::dynamicset::{ArraySet, DynamicSet, HashBrownSet};
use crate::idx::trees::graph::UndirectedGraph;
use crate::idx::trees::knn::{DoublePriorityQueue, Ids64, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::kvs::Key;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::{Array, Thing, Value};
use hashbrown::hash_map::Entry;
use hashbrown::{HashMap, HashSet};
use radix_trie::Trie;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use roaring::RoaringTreemap;
use std::collections::VecDeque;

pub struct HnswIndex {
	dim: usize,
	vector_type: VectorType,
	hnsw: HnswFlavor,
	docs: HnswDocs,
	vec_docs: HashMap<SharedVector, (Ids64, ElementId)>,
}

impl HnswIndex {
	pub fn new(p: &HnswParams) -> Self {
		Self {
			dim: p.dimension as usize,
			vector_type: p.vector_type,
			hnsw: HnswFlavor::new(p),
			docs: HnswDocs::default(),
			vec_docs: HashMap::default(),
		}
	}

	pub fn index_document(&mut self, rid: &Thing, content: &Vec<Value>) -> Result<(), Error> {
		// Resolve the doc_id
		let doc_id = self.docs.resolve(rid);
		// Index the values
		for value in content {
			// Extract the vector
			let vector = Vector::try_from_value(self.vector_type, self.dim, value)?;
			vector.check_dimension(self.dim)?;
			self.insert(vector.into(), doc_id);
		}
		Ok(())
	}

	fn insert(&mut self, o: SharedVector, d: DocId) {
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
				let element_id = self.hnsw.insert(o);
				e.insert((Ids64::One(d), element_id));
			}
		}
	}

	fn remove(&mut self, o: SharedVector, d: DocId) {
		if let Entry::Occupied(mut e) = self.vec_docs.entry(o) {
			let (docs, e_id) = e.get_mut();
			if let Some(new_docs) = docs.remove(d) {
				let e_id = *e_id;
				if new_docs.is_empty() {
					e.remove();
					self.hnsw.remove(e_id);
				} else {
					e.insert((new_docs, e_id));
				}
			}
		}
	}

	pub(crate) fn remove_document(
		&mut self,
		rid: &Thing,
		content: &Vec<Value>,
	) -> Result<(), Error> {
		if let Some(doc_id) = self.docs.remove(rid) {
			for v in content {
				// Extract the vector
				let vector = Vector::try_from_value(self.vector_type, self.dim, v)?;
				vector.check_dimension(self.dim)?;
				// Remove the vector
				self.remove(vector.into(), doc_id);
			}
		}
		Ok(())
	}

	pub fn knn_search(
		&self,
		a: &Array,
		n: usize,
		ef: usize,
	) -> Result<VecDeque<(Thing, f64)>, Error> {
		// Extract the vector
		let vector = Vector::try_from_array(self.vector_type, a)?;
		vector.check_dimension(self.dim)?;
		// Do the search
		let res = self.search(&vector.into(), n, ef);
		Ok(self.result(res))
	}

	fn result(&self, res: KnnResult) -> VecDeque<(Thing, f64)> {
		res.docs
			.into_iter()
			.filter_map(|(doc_id, dist)| self.docs.get(doc_id).map(|t| (t.clone(), dist)))
			.collect()
	}

	fn search(&self, o: &SharedVector, n: usize, ef: usize) -> KnnResult {
		let neighbors = self.hnsw.knn_search(o, n, ef);

		let mut builder = KnnResultBuilder::new(n);
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist) {
				let v = self.hnsw.element(&e_id);
				if let Some((docs, _)) = self.vec_docs.get(v) {
					builder.add(e_dist, docs);
				}
			}
		}

		builder.build(
			#[cfg(debug_assertions)]
			HashMap::new(),
		)
	}
}

#[derive(Default)]
struct HnswDocs {
	doc_ids: Trie<Key, DocId>,
	ids_doc: Vec<Option<Thing>>,
	available: RoaringTreemap,
}

impl HnswDocs {
	fn resolve(&mut self, rid: &Thing) -> DocId {
		let doc_key: Key = rid.into();
		if let Some(doc_id) = self.doc_ids.get(&doc_key) {
			*doc_id
		} else {
			let doc_id = self.next_doc_id();
			self.ids_doc.push(Some(rid.clone()));
			self.doc_ids.insert(doc_key, doc_id);
			doc_id
		}
	}

	fn next_doc_id(&mut self) -> DocId {
		if let Some(doc_id) = self.available.iter().next() {
			self.available.remove(doc_id);
			doc_id
		} else {
			self.ids_doc.len() as DocId
		}
	}

	fn get(&self, doc_id: DocId) -> Option<Thing> {
		if let Some(t) = self.ids_doc.get(doc_id as usize) {
			t.clone()
		} else {
			None
		}
	}

	fn remove(&mut self, rid: &Thing) -> Option<DocId> {
		let doc_key: Key = rid.into();
		if let Some(doc_id) = self.doc_ids.remove(&doc_key) {
			let n = doc_id as usize;
			if n < self.ids_doc.len() {
				self.ids_doc[n] = None;
			}
			self.available.insert(doc_id);
			Some(doc_id)
		} else {
			None
		}
	}
}

enum HnswFlavor {
	Array4(Hnsw<ArraySet<ElementId, 9>, ArraySet<ElementId, 5>>),
	Array8(Hnsw<ArraySet<ElementId, 17>, ArraySet<ElementId, 9>>),
	Array12(Hnsw<ArraySet<ElementId, 25>, ArraySet<ElementId, 13>>),
	Array16(Hnsw<HashBrownSet<ElementId>, ArraySet<ElementId, 17>>),
	Array20(Hnsw<HashBrownSet<ElementId>, ArraySet<ElementId, 21>>),
	Array24(Hnsw<HashBrownSet<ElementId>, ArraySet<ElementId, 25>>),
	Array28(Hnsw<HashBrownSet<ElementId>, ArraySet<ElementId, 29>>),
	Hash(Hnsw<HashBrownSet<ElementId>, HashBrownSet<ElementId>>),
}

impl HnswFlavor {
	fn new(p: &HnswParams) -> Self {
		match p.m {
			1..=4 => Self::Array4(Hnsw::new(p)),
			_ => Self::Hash(Hnsw::new(p)),
		}
	}

	fn insert(&mut self, q_pt: SharedVector) -> ElementId {
		match self {
			HnswFlavor::Array4(h) => h.insert(q_pt),
			HnswFlavor::Array8(h) => h.insert(q_pt),
			HnswFlavor::Array12(h) => h.insert(q_pt),
			HnswFlavor::Array16(h) => h.insert(q_pt),
			HnswFlavor::Array20(h) => h.insert(q_pt),
			HnswFlavor::Array24(h) => h.insert(q_pt),
			HnswFlavor::Array28(h) => h.insert(q_pt),
			HnswFlavor::Hash(h) => h.insert(q_pt),
		}
	}

	fn remove(&mut self, e_id: ElementId) -> bool {
		match self {
			HnswFlavor::Array4(h) => h.remove(e_id),
			HnswFlavor::Array8(h) => h.remove(e_id),
			HnswFlavor::Array12(h) => h.remove(e_id),
			HnswFlavor::Array16(h) => h.remove(e_id),
			HnswFlavor::Array20(h) => h.remove(e_id),
			HnswFlavor::Array24(h) => h.remove(e_id),
			HnswFlavor::Array28(h) => h.remove(e_id),
			HnswFlavor::Hash(h) => h.remove(e_id),
		}
	}

	fn knn_search(&self, q: &SharedVector, k: usize, efs: usize) -> Vec<(f64, ElementId)> {
		match self {
			HnswFlavor::Array4(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array8(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array12(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array16(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array20(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array24(h) => h.knn_search(q, k, efs),
			HnswFlavor::Array28(h) => h.knn_search(q, k, efs),
			HnswFlavor::Hash(h) => h.knn_search(q, k, efs),
		}
	}

	fn element(&self, e_id: &ElementId) -> &SharedVector {
		match self {
			HnswFlavor::Array4(h) => h.element(e_id),
			HnswFlavor::Array8(h) => h.element(e_id),
			HnswFlavor::Array12(h) => h.element(e_id),
			HnswFlavor::Array16(h) => h.element(e_id),
			HnswFlavor::Array20(h) => h.element(e_id),
			HnswFlavor::Array24(h) => h.element(e_id),
			HnswFlavor::Array28(h) => h.element(e_id),
			HnswFlavor::Hash(h) => h.element(e_id),
		}
	}
}

struct Hnsw<L0, L>
where
	L0: DynamicSet<ElementId>,
	L: DynamicSet<ElementId>,
{
	m: usize,
	m0: usize,
	efc: usize,
	ml: f64,
	dist: Distance,
	layer0: UndirectedGraph<ElementId, L0>,
	layers: Vec<UndirectedGraph<ElementId, L>>,
	enter_point: Option<ElementId>,
	elements: HashMap<ElementId, SharedVector>,
	next_element_id: ElementId,
	rng: SmallRng,
	neighbors: SelectNeighbors,
}

pub(super) type ElementId = u64;

impl<L0, L> Hnsw<L0, L>
where
	L0: DynamicSet<ElementId>,
	L: DynamicSet<ElementId>,
{
	fn new(p: &HnswParams) -> Self {
		let m0 = p.m0 as usize;
		Self {
			m: p.m as usize,
			m0: p.m0 as usize,
			efc: p.ef_construction as usize,
			ml: p.ml.to_float(),
			dist: p.distance.clone(),
			enter_point: None,
			layer0: UndirectedGraph::new(m0),
			layers: Vec::default(),
			elements: HashMap::default(),
			next_element_id: 0,
			rng: SmallRng::from_entropy(),
			neighbors: p.into(),
		}
	}

	fn insert(&mut self, q_pt: SharedVector) -> ElementId {
		let q_level = self.get_random_level();
		self.insert_level(q_pt, q_level)
	}

	fn insert_level(&mut self, q_pt: SharedVector, q_level: usize) -> ElementId {
		let q_id = self.next_element_id;
		let layers = self.layers.len();

		// Be sure we have existing layers
		let mut m_max = self.m;
		for l in layers..=q_level {
			if l == 0 {
				m_max = self.m0
			}
			#[cfg(debug_assertions)]
			debug!("Create Layer {l} - m_max: {m_max}");
			self.layers.push(UndirectedGraph::new(m_max));
		}

		self.elements.insert(q_id, q_pt.clone());

		if let Some(ep_id) = self.enter_point {
			self.insert_element(q_id, &q_pt, q_level, ep_id, layers - 1);
		} else {
			self.insert_first_element(q_id, q_level);
		}

		self.next_element_id += 1;
		q_id
	}

	fn remove(&mut self, e_id: ElementId) -> bool {
		#[cfg(debug_assertions)]
		debug!("Remove {e_id}");

		let mut removed = false;

		let e_pt = self.elements.get(&e_id).cloned();
		if let Some(e_pt) = e_pt {
			let layers = self.layers.len();
			let mut new_enter_point = None;

			// Are we deleting the current enter point?
			if Some(e_id) == self.enter_point {
				let layer = &self.layers[layers - 1];
				new_enter_point = self.search_layer_single_ignore_ep(&e_pt, e_id, layer);
			}

			self.elements.remove(&e_id);

			let mut m_max = self.m;
			for lc in (0..layers).rev() {
				if lc == 0 {
					m_max = self.m0
				}
				if let Some(f_ids) = self.layers[lc].remove_node_and_bidirectional_edges(&e_id) {
					for &q_id in f_ids.iter() {
						if let Some(q_pt) = self.elements.get(&q_id) {
							let layer = &self.layers[lc];
							let c = self.search_layer_multi_ignore_ep(q_pt, q_id, self.efc, layer);
							let neighbors =
								self.neighbors.select(self, layer, q_id, q_pt, c, m_max);
							#[cfg(debug_assertions)]
							{
								assert!(
									!neighbors.contains(&q_id),
									"!neighbors.contains(&q_id) = layer: {lc} - q_id: {q_id} - f_ids: {neighbors:?}"
								);
								assert!(
									!neighbors.contains(&e_id),
									"!neighbors.contains(&e_id) = layer: {lc} - e_id: {e_id} - f_ids: {neighbors:?}"
								);
								assert!(neighbors.len() < m_max);
							}
							self.layers[lc].set_node(q_id, neighbors);
						}
					}
					removed = true;
				}
			}

			if removed && Some(e_id) == self.enter_point {
				self.enter_point = new_enter_point.map(|(_, e_id)| e_id);
			}
		}
		removed
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	fn insert_first_element(&mut self, id: ElementId, level: usize) {
		#[cfg(debug_assertions)]
		debug!("insert_first_element - id: {id} - level: {level}");
		for lc in 0..=level {
			self.layers[lc].add_empty_node(id);
		}
		self.enter_point = Some(id);
		#[cfg(debug_assertions)]
		debug!("E - EP: {id}");
	}

	fn insert_element(
		&mut self,
		q_id: ElementId,
		q_pt: &SharedVector,
		q_level: usize,
		mut ep_id: ElementId,
		top_layer_level: usize,
	) {
		#[cfg(debug_assertions)]
		debug!("insert_element q_pt: {q_pt:?} - q_id: {q_id} - level: {q_level} -  ep_id: {ep_id:?} - top-layer: {top_layer_level}");
		let mut ep_dist = self.get_element_distance(q_pt, &ep_id).unwrap_or_else(|| unreachable!());
		for lc in ((q_level + 1)..=top_layer_level).rev() {
			(ep_dist, ep_id) = self
				.search_layer_single(q_pt, ep_dist, ep_id, 1, &self.layers[lc])
				.peek_first()
				.unwrap_or_else(|| unreachable!())
		}

		let mut eps = DoublePriorityQueue::from(ep_dist, ep_id);
		for lc in (0..=top_layer_level.min(q_level)).rev() {
			let m_max = if lc == 0 {
				self.m0
			} else {
				self.m
			};

			let w;
			let neighbors = {
				let layer = &self.layers[lc];
				w = self.search_layer_multi(q_pt, eps, self.efc, layer);
				eps = w.clone();
				self.neighbors.select(self, layer, q_id, q_pt, w, m_max)
			};

			let neighbors = self.layers[lc].add_node_and_bidirectional_edges(q_id, neighbors);

			for e_id in neighbors {
				let e_conn = self.layers[lc]
					.get_edges(&e_id)
					.unwrap_or_else(|| unreachable!("Element: {}", e_id));
				if e_conn.len() > m_max {
					let e_pt = &self.elements[&e_id];
					let e_c = self.build_priority_list(e_id, e_conn);
					let e_new_conn =
						self.neighbors.select(self, &self.layers[lc], e_id, e_pt, e_c, m_max);
					assert!(!e_new_conn.contains(&e_id));
					self.layers[lc].set_node(e_id, e_new_conn);
				}
			}
		}

		for lc in (top_layer_level + 1)..=q_level {
			if !self.layers[lc].add_empty_node(q_id) {
				unreachable!("Already there {}", q_id);
			}
		}

		if q_level > top_layer_level {
			self.enter_point = Some(q_id);
			#[cfg(debug_assertions)]
			debug!("E - ep_id: {q_id}");
		}
	}

	fn build_priority_list<S: DynamicSet<ElementId>>(
		&self,
		e_id: ElementId,
		neighbors: &S,
	) -> DoublePriorityQueue {
		let e_pt = &self.elements[&e_id];
		let mut w = DoublePriorityQueue::default();
		for n_id in neighbors.iter() {
			if let Some(n_pt) = self.elements.get(n_id) {
				let dist = self.dist.calculate(e_pt, n_pt);
				w.push(dist, *n_id);
			}
		}
		w
	}

	fn get_element_distance(&self, q: &SharedVector, e_id: &ElementId) -> Option<f64> {
		self.elements.get(e_id).map(|e_pt| self.dist.calculate(e_pt, q))
	}

	fn get_element_vector(&self, e_id: &ElementId) -> Option<SharedVector> {
		self.elements.get(e_id).cloned()
	}

	fn search_layer_single<S: DynamicSet<ElementId>>(
		&self,
		q: &SharedVector,
		ep_dist: f64,
		ep_id: ElementId,
		ef: usize,
		l: &UndirectedGraph<ElementId, S>,
	) -> DoublePriorityQueue {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(ep_dist, ep_id);
		let w = candidates.clone();
		self.search_layer(q, candidates, visited, w, ef, l)
	}

	fn search_layer_multi<S: DynamicSet<ElementId>>(
		&self,
		q: &SharedVector,
		candidates: DoublePriorityQueue,
		ef: usize,
		l: &UndirectedGraph<ElementId, S>,
	) -> DoublePriorityQueue {
		let w = candidates.clone();
		let visited = w.to_set();
		self.search_layer(q, candidates, visited, w, ef, l)
	}

	fn search_layer_single_ignore_ep<S: DynamicSet<ElementId>>(
		&self,
		q: &SharedVector,
		ep_id: ElementId,
		l: &UndirectedGraph<ElementId, S>,
	) -> Option<(f64, ElementId)> {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(0.0, ep_id);
		let w = candidates.clone();
		let q = self.search_layer(q, candidates, visited, w, 1, l);
		q.peek_first()
	}

	fn search_layer_multi_ignore_ep<S: DynamicSet<ElementId>>(
		&self,
		q: &SharedVector,
		ep_id: ElementId,
		ef: usize,
		l: &UndirectedGraph<ElementId, S>,
	) -> DoublePriorityQueue {
		let visited = HashSet::from([ep_id]);
		let candidates = DoublePriorityQueue::from(0.0, ep_id);
		let w = DoublePriorityQueue::default();
		self.search_layer(q, candidates, visited, w, ef, l)
	}

	fn search_layer<S: DynamicSet<ElementId>>(
		&self,
		q: &SharedVector,
		mut candidates: DoublePriorityQueue,
		mut visited: HashSet<ElementId>,
		mut w: DoublePriorityQueue,
		ef: usize,
		l: &UndirectedGraph<ElementId, S>,
	) -> DoublePriorityQueue {
		let mut f_dist = if let Some(d) = w.peek_last_dist() {
			d
		} else {
			return w;
		};
		while let Some((dist, doc)) = candidates.pop_first() {
			if dist > f_dist {
				break;
			}
			if let Some(neighbourhood) = l.get_edges(&doc) {
				for &e_id in neighbourhood.iter() {
					if visited.insert(e_id) {
						if let Some(e_pt) = self.elements.get(&e_id) {
							let e_dist = self.dist.calculate(e_pt, q);
							if e_dist < f_dist || w.len() < ef {
								candidates.push(e_dist, e_id);
								w.push(e_dist, e_id);
								if w.len() > ef {
									w.pop_last();
								}
								f_dist = w.peek_last_dist().unwrap(); // w can't be empty
							}
						}
					}
				}
			}
		}
		w
	}

	fn knn_search(&self, q: &SharedVector, k: usize, efs: usize) -> Vec<(f64, ElementId)> {
		#[cfg(debug_assertions)]
		let expected_w_len = self.elements.len().min(k);
		if let Some(mut ep_id) = self.enter_point {
			let mut ep_dist =
				self.get_element_distance(q, &ep_id).unwrap_or_else(|| unreachable!());
			let l = self.layers.len();
			for lc in (1..l).rev() {
				(ep_dist, ep_id) = self
					.search_layer_single(q, ep_dist, ep_id, 1, &self.layers[lc])
					.peek_first()
					.unwrap_or_else(|| unreachable!());
			}
			{
				let w = self.search_layer_single(q, ep_dist, ep_id, efs, &self.layers[0]);
				#[cfg(debug_assertions)]
				if w.len() < expected_w_len {
					debug!(
						"0 search_layer - ep_id: {ep_id:?} - ef_search: {efs} - k: {k} - w.len: {} < {expected_w_len}",
						w.len()
					);
				}
				w.to_vec_limit(k)
			}
		} else {
			vec![]
		}
	}

	#[inline]
	fn element(&self, e_id: &ElementId) -> &SharedVector {
		&self.elements[e_id]
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
	fn select<L0, L, S>(
		&self,
		h: &Hnsw<L0, L>,
		lc: &UndirectedGraph<ElementId, S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: DoublePriorityQueue,
		m_max: usize,
	) -> S
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		match self {
			Self::Heuristic => Self::heuristic(c, h, m_max),
			Self::HeuristicExt => Self::heuristic_ext(h, lc, q_id, q_pt, c, m_max),
			Self::HeuristicKeep => Self::heuristic_keep(c, h, m_max),
			Self::HeuristicExtKeep => Self::heuristic_ext_keep(h, lc, q_id, q_pt, c, m_max),
		}
	}

	fn heuristic<L0, L, S>(mut c: DoublePriorityQueue, h: &Hnsw<L0, L>, m_max: usize) -> S
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		if c.len() <= m_max {
			return c.to_dynamic_set(m_max);
		}
		let mut r = S::with_capacity(m_max);
		while let Some((e_dist, e_id)) = c.pop_first() {
			if Self::is_closer(h, e_dist, e_id, &mut r) && r.len() == m_max {
				break;
			}
		}
		r
	}

	fn heuristic_keep<L0, L, S>(mut c: DoublePriorityQueue, h: &Hnsw<L0, L>, m_max: usize) -> S
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		if c.len() <= m_max {
			return c.to_dynamic_set(m_max);
		}
		let mut pruned = Vec::new();
		let mut r = S::with_capacity(m_max);
		while let Some((e_dist, e_id)) = c.pop_first() {
			if Self::is_closer(h, e_dist, e_id, &mut r) {
				if r.len() == m_max {
					break;
				}
			} else {
				pruned.push(e_id);
			}
		}
		let n = m_max - r.len();
		if n > 0 {
			for e_id in pruned.drain(0..n) {
				r.insert(e_id);
			}
		}
		r
	}

	fn extend<L0, L, S>(
		h: &Hnsw<L0, L>,
		lc: &UndirectedGraph<ElementId, S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		c: &mut DoublePriorityQueue,
		m_max: usize,
	) where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		let mut ex = c.to_set();
		let mut ext = Vec::with_capacity(m_max.min(c.len()));
		for (_, e_id) in c.to_vec().into_iter() {
			for &e_adj in lc.get_edges(&e_id).unwrap_or_else(|| unreachable!()).iter() {
				if e_adj != q_id && ex.insert(e_adj) {
					if let Some(d) = h.get_element_distance(q_pt, &e_adj) {
						ext.push((d, e_adj));
					}
				}
			}
		}
		for (e_dist, e_id) in ext {
			c.push(e_dist, e_id);
		}
	}

	fn heuristic_ext<L0, L, S>(
		h: &Hnsw<L0, L>,
		lc: &UndirectedGraph<ElementId, S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		m_max: usize,
	) -> S
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		Self::extend(h, lc, q_id, q_pt, &mut c, m_max);
		Self::heuristic(c, h, m_max)
	}

	fn heuristic_ext_keep<L0, L, S>(
		h: &Hnsw<L0, L>,
		lc: &UndirectedGraph<ElementId, S>,
		q_id: ElementId,
		q_pt: &SharedVector,
		mut c: DoublePriorityQueue,
		m_max: usize,
	) -> S
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		Self::extend(h, lc, q_id, q_pt, &mut c, m_max);
		Self::heuristic_keep(c, h, m_max)
	}

	fn is_closer<L0, L, S>(h: &Hnsw<L0, L>, e_dist: f64, e_id: ElementId, r: &mut S) -> bool
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
		S: DynamicSet<ElementId>,
	{
		if let Some(current_vec) = h.get_element_vector(&e_id) {
			for r_id in r.iter() {
				if let Some(r_dist) = h.get_element_distance(&current_vec, r_id) {
					if e_dist > r_dist {
						return false;
					}
				}
			}
			r.insert(e_id);
			true
		} else {
			false
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::trees::dynamicset::DynamicSet;
	use crate::idx::trees::hnsw::{ElementId, Hnsw, HnswFlavor, HnswIndex};
	use crate::idx::trees::knn::tests::{new_vectors_from_file, TestCollection};
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::sql::index::{Distance, HnswParams, VectorType};
	use hashbrown::{hash_map::Entry, HashMap, HashSet};
	use ndarray::Array1;
	use roaring::RoaringTreemap;
	use std::sync::Arc;
	use test_log::test;

	fn insert_collection_hnsw(
		h: &mut HnswFlavor,
		collection: &TestCollection,
	) -> HashSet<SharedVector> {
		let mut set = HashSet::new();
		for (_, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone().into();
			h.insert(obj.clone());
			set.insert(obj);
			h.check_hnsw_properties(set.len());
		}
		set
	}
	fn find_collection_hnsw(h: &HnswFlavor, collection: &TestCollection) {
		let max_knn = 20.min(collection.len());
		for (_, obj) in collection.to_vec_ref() {
			let obj = obj.clone().into();
			for knn in 1..max_knn {
				let res = h.knn_search(&obj, knn, 80);
				if collection.is_unique() {
					let mut found = false;
					for (_, e_id) in &res {
						if h.element(e_id).eq(&obj) {
							found = true;
							break;
						}
					}
					assert!(
						found,
						"Search: {:?} - Knn: {} - Vector not found - Got: {:?} - Coll: {}",
						obj,
						knn,
						res,
						collection.len(),
					);
				}
				let expected_len = collection.len().min(knn);
				if expected_len != res.len() {
					info!("expected_len != res.len()")
				}
				assert_eq!(
					expected_len,
					res.len(),
					"Wrong knn count - Expected: {} - Got: {} - Collection: {} - - Res: {:?}",
					expected_len,
					res.len(),
					collection.len(),
					res,
				)
			}
		}
	}

	fn test_hnsw_collection(p: &HnswParams, collection: &TestCollection) {
		let mut h = HnswFlavor::new(p);
		insert_collection_hnsw(&mut h, collection);
		find_collection_hnsw(&h, &collection);
	}

	fn new_params(
		dimension: usize,
		vector_type: VectorType,
		distance: Distance,
		m: usize,
		efc: usize,
		extend_candidates: bool,
		keep_pruned_connections: bool,
	) -> HnswParams {
		let m = m as u16;
		let m0 = m * 2;
		HnswParams::new(
			dimension as u16,
			distance,
			vector_type,
			m,
			m0,
			(1.0 / (m as f64).ln()).into(),
			efc as u16,
			extend_candidates,
			keep_pruned_connections,
		)
	}

	fn test_hnsw(collection_size: usize, p: HnswParams) {
		info!("Collection size: {collection_size} - Params: {p:?}");
		let collection = TestCollection::new(
			true,
			collection_size,
			p.vector_type,
			p.dimension as usize,
			&p.distance,
		);
		test_hnsw_collection(&p, &collection);
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn tests_hnsw() -> Result<(), Error> {
		let mut futures = Vec::new();
		for (dist, dim) in [
			(Distance::Chebyshev, 5),
			(Distance::Cosine, 5),
			(Distance::Euclidean, 5),
			(Distance::Hamming, 20),
			// (Distance::Jaccard, 100),
			(Distance::Manhattan, 5),
			(Distance::Minkowski(2.into()), 5),
			//(Distance::Pearson, 5),
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for (extend, keep) in [(false, false), (true, false), (false, true), (true, true)] {
					let p = new_params(dim, vt, dist.clone(), 24, 500, extend, keep);
					let f = tokio::spawn(async move {
						test_hnsw(30, p);
					});
					futures.push(f);
				}
			}
		}
		for f in futures {
			f.await.expect("Task error");
		}
		Ok(())
	}

	fn insert_collection_hnsw_index(
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> HashMap<SharedVector, HashSet<DocId>> {
		let mut map: HashMap<SharedVector, HashSet<DocId>> = HashMap::new();
		for (doc_id, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone().into();
			h.insert(obj.clone(), *doc_id);
			match map.entry(obj) {
				Entry::Occupied(mut e) => {
					e.get_mut().insert(*doc_id);
				}
				Entry::Vacant(e) => {
					e.insert(HashSet::from([*doc_id]));
				}
			}
			h.hnsw.check_hnsw_properties(map.len());
		}
		map
	}

	fn find_collection_hnsw_index(h: &mut HnswIndex, collection: &TestCollection) {
		let max_knn = 20.min(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let obj: SharedVector = obj.clone().into();
				let res = h.search(&obj, knn, 500);
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
				let expected_len = collection.len().min(knn);
				assert_eq!(
					expected_len,
					res.docs.len(),
					"Wrong knn count - Expected: {} - Got: {} - - Docs: {:?} - Collection: {}",
					expected_len,
					res.docs.len(),
					res.docs,
					collection.len(),
				)
			}
		}
	}

	fn delete_hnsw_index_collection(
		h: &mut HnswIndex,
		collection: &TestCollection,
		mut map: HashMap<SharedVector, HashSet<DocId>>,
	) {
		for (doc_id, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone().into();
			h.remove(obj.clone(), *doc_id);
			if let Entry::Occupied(mut e) = map.entry(obj.clone()) {
				let set = e.get_mut();
				set.remove(doc_id);
				if set.is_empty() {
					e.remove();
				}
			}
			h.hnsw.check_hnsw_properties(map.len());
		}
	}

	fn test_hnsw_index(collection_size: usize, unique: bool, p: HnswParams) {
		info!("test_hnsw_index - coll size: {collection_size} - params: {p:?}");
		let collection = TestCollection::new(
			unique,
			collection_size,
			p.vector_type,
			p.dimension as usize,
			&p.distance,
		);
		let mut h = HnswIndex::new(&p);
		let map = insert_collection_hnsw_index(&mut h, &collection);
		find_collection_hnsw_index(&mut h, &collection);
		delete_hnsw_index_collection(&mut h, &collection, map);
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn tests_hnsw_index() -> Result<(), Error> {
		let mut futures = Vec::new();
		for (dist, dim) in [
			(Distance::Chebyshev, 5),
			(Distance::Cosine, 5),
			(Distance::Euclidean, 5),
			(Distance::Hamming, 20),
			// (Distance::Jaccard, 100),
			(Distance::Manhattan, 5),
			(Distance::Minkowski(2.into()), 5),
			(Distance::Pearson, 5),
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for (extend, keep) in [(false, false), (true, false), (false, true), (true, true)] {
					for unique in [false, true] {
						let p = new_params(dim, vt, dist.clone(), 8, 150, extend, keep);
						let f = tokio::spawn(async move {
							test_hnsw_index(30, unique, p);
						});
						futures.push(f);
					}
				}
			}
		}
		for f in futures {
			f.await.expect("Task error");
		}
		Ok(())
	}

	#[test]
	fn test_invalid_size() {
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
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true);
		let mut h = HnswFlavor::new(&p);
		insert_collection_hnsw(&mut h, &collection);
		let pt = new_i16_vec(-2, -3);
		let knn = 10;
		let efs = 501;
		let hnsw_res = h.knn_search(&pt, knn, efs);
		assert_eq!(hnsw_res.len(), knn);
	}

	async fn test_recall(
		embeddings_file: &str,
		ingest_limit: usize,
		queries_file: &str,
		query_limit: usize,
		p: HnswParams,
		tests_ef_recall: &[(usize, f64)],
	) -> Result<(), Error> {
		info!("Build data collection");
		let collection: Arc<TestCollection> =
			Arc::new(TestCollection::NonUnique(new_vectors_from_file(
				p.vector_type,
				&format!("../tests/data/{embeddings_file}"),
				Some(ingest_limit),
			)?));

		let mut h = HnswIndex::new(&p);
		info!("Insert collection");
		for (doc_id, obj) in collection.to_vec_ref() {
			h.insert(obj.clone(), *doc_id);
		}

		let h = Arc::new(h);

		info!("Build query collection");
		let queries = Arc::new(TestCollection::NonUnique(new_vectors_from_file(
			p.vector_type,
			&format!("../tests/data/{queries_file}"),
			Some(query_limit),
		)?));

		info!("Check recall");
		let mut futures = Vec::with_capacity(tests_ef_recall.len());
		for &(efs, expected_recall) in tests_ef_recall {
			let queries = queries.clone();
			let collection = collection.clone();
			let h = h.clone();
			let f = tokio::spawn(async move {
				let mut total_recall = 0.0;
				for (_, pt) in queries.to_vec_ref() {
					let knn = 10;
					let hnsw_res = h.search(pt, knn, efs);
					assert_eq!(hnsw_res.docs.len(), knn, "Different size - knn: {knn}",);
					let brute_force_res = collection.knn(pt, Distance::Euclidean, knn);
					let rec = brute_force_res.recall(&hnsw_res);
					if rec == 1.0 {
						assert_eq!(brute_force_res.docs, hnsw_res.docs);
					}
					total_recall += rec;
				}
				let recall = total_recall / queries.to_vec_ref().len() as f64;
				info!("EFS: {efs} - Recall: {recall}");
				assert!(
					recall >= expected_recall,
					"EFS: {efs} - Recall: {recall} - Expected: {expected_recall}"
				);
			});
			futures.push(f);
		}
		for f in futures {
			f.await.expect("Task failure");
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_recall_euclidean() -> Result<(), Error> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, false);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			3000,
			"hnsw-random-5000-20-euclidean.gz",
			500,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_recall_euclidean_keep_pruned_connections() -> Result<(), Error> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, true);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			3000,
			"hnsw-random-5000-20-euclidean.gz",
			500,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_recall_euclidean_full() -> Result<(), Error> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, true, true);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			1000,
			"hnsw-random-5000-20-euclidean.gz",
			200,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	impl HnswFlavor {
		fn check_hnsw_properties(&self, expected_count: usize) {
			match self {
				HnswFlavor::Array4(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array8(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array12(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array16(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array20(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array24(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Array28(h) => check_hnsw_props(h, expected_count),
				HnswFlavor::Hash(h) => check_hnsw_props(h, expected_count),
			}
		}
	}

	fn check_hnsw_props<L0, L>(h: &Hnsw<L0, L>, expected_count: usize)
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
	{
		let mut layer_size = h.elements.len();
		assert_eq!(layer_size, expected_count);
		for (lc, l) in h.layers.iter().enumerate() {
			assert!(l.len() <= layer_size, "{} - {}", l.len(), layer_size);
			layer_size = l.len();
			let m_layer = if lc == 0 {
				h.m0
			} else {
				h.m
			};
			for (e_id, f_ids) in l.nodes() {
				assert!(
					f_ids.len() <= m_layer,
					"Foreign list e_id: {e_id} - len = len({}) <= m_layer({m_layer}) - lc: {lc}",
					f_ids.len(),
				);
				assert!(
					!f_ids.contains(e_id),
					"!f_ids.contains(e_id) = layer: {lc} - el: {e_id} - f_ids: {f_ids:?}"
				);
				assert!(
					h.elements.contains_key(e_id),
					"h.elements.contains_key(e_id) - layer: {lc} - el: {e_id} - f_ids: {f_ids:?}"
				);
			}
		}
		h.debug_print_check();
	}

	impl TestCollection {
		fn knn(&self, pt: &SharedVector, dist: Distance, n: usize) -> KnnResult {
			let mut b = KnnResultBuilder::new(n);
			for (doc_id, doc_pt) in self.to_vec_ref() {
				let d = dist.calculate(doc_pt, pt);
				if b.check_add(d) {
					b.add(d, &Ids64::One(*doc_id));
				}
			}
			b.build(
				#[cfg(debug_assertions)]
				HashMap::new(),
			)
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
		let vec = Vector::I16(Array1::from_vec(vec![x as i16, y as i16]));
		vec.into()
	}

	impl<L0, L> Hnsw<L0, L>
	where
		L0: DynamicSet<ElementId>,
		L: DynamicSet<ElementId>,
	{
		fn debug_print_check(&self) {
			debug!("EP: {:?}", self.enter_point);
			for (i, l) in self.layers.iter().enumerate() {
				debug!("LAYER {i} - len: {}", l.len());
				let m_max = if i == 0 {
					self.m0
				} else {
					self.m
				};
				for f in l.nodes().values() {
					assert!(f.len() <= m_max);
				}
			}
		}
	}
}
