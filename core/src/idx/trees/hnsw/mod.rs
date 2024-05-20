mod heuristic;
mod layer;

use crate::err::Error;
use crate::idx::docids::DocId;
use crate::idx::planner::checker::ConditionChecker;
use crate::idx::planner::iterators::KnnIteratorResult;
use crate::idx::trees::dynamicset::{ArraySet, DynamicSet, HashBrownSet};
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::layer::HnswLayer;
use crate::idx::trees::knn::{DoublePriorityQueue, Ids64, KnnResult, KnnResultBuilder};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::kvs::Key;
use crate::sql::index::{Distance, HnswParams, VectorType};
use crate::sql::{Number, Thing, Value};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use radix_trie::Trie;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use reblessive::tree::Stk;
use roaring::RoaringTreemap;
use std::collections::VecDeque;

pub struct HnswIndex {
	dim: usize,
	vector_type: VectorType,
	hnsw: HnswFlavor,
	docs: HnswDocs,
	vec_docs: VecDocs,
}

type VecDocs = HashMap<SharedVector, (Ids64, ElementId)>;

type ASet<const N: usize> = ArraySet<ElementId, N>;
type HSet = HashBrownSet<ElementId>;

enum HnswFlavor {
	H5_9(Hnsw<ASet<9>, ASet<5>>),
	H5_17(Hnsw<ASet<17>, ASet<5>>),
	H5_25(Hnsw<ASet<25>, ASet<5>>),
	H5set(Hnsw<HSet, ASet<5>>),
	H9_17(Hnsw<ASet<17>, ASet<9>>),
	H9_25(Hnsw<ASet<25>, ASet<9>>),
	H9set(Hnsw<HSet, ASet<9>>),
	H13_25(Hnsw<ASet<25>, ASet<13>>),
	H13set(Hnsw<HSet, ASet<13>>),
	H17set(Hnsw<HSet, ASet<17>>),
	H21set(Hnsw<HSet, ASet<21>>),
	H25set(Hnsw<HSet, ASet<25>>),
	H29set(Hnsw<HSet, ASet<29>>),
	Hset(Hnsw<HSet, HSet>),
}

impl HnswFlavor {
	fn new(p: &HnswParams) -> Self {
		match p.m {
			1..=4 => match p.m0 {
				1..=8 => Self::H5_9(Hnsw::<ASet<9>, ASet<5>>::new(p)),
				9..=16 => Self::H5_17(Hnsw::<ASet<17>, ASet<5>>::new(p)),
				17..=24 => Self::H5_25(Hnsw::<ASet<25>, ASet<5>>::new(p)),
				_ => Self::H5set(Hnsw::<HSet, ASet<5>>::new(p)),
			},
			5..=8 => match p.m0 {
				1..=16 => Self::H9_17(Hnsw::<ASet<17>, ASet<9>>::new(p)),
				17..=24 => Self::H9_25(Hnsw::<ASet<25>, ASet<9>>::new(p)),
				_ => Self::H9set(Hnsw::<HSet, ASet<9>>::new(p)),
			},
			9..=12 => match p.m0 {
				17..=24 => Self::H13_25(Hnsw::<ASet<25>, ASet<13>>::new(p)),
				_ => Self::H13set(Hnsw::<HSet, ASet<13>>::new(p)),
			},
			13..=16 => Self::H17set(Hnsw::<HSet, ASet<17>>::new(p)),
			17..=20 => Self::H21set(Hnsw::<HSet, ASet<21>>::new(p)),
			21..=24 => Self::H25set(Hnsw::<HSet, ASet<25>>::new(p)),
			25..=28 => Self::H29set(Hnsw::<HSet, ASet<29>>::new(p)),
			_ => Self::Hset(Hnsw::<HSet, HSet>::new(p)),
		}
	}

	fn insert(&mut self, q_pt: SharedVector) -> ElementId {
		match self {
			HnswFlavor::H5_9(h) => h.insert(q_pt),
			HnswFlavor::H5_17(h) => h.insert(q_pt),
			HnswFlavor::H5_25(h) => h.insert(q_pt),
			HnswFlavor::H5set(h) => h.insert(q_pt),
			HnswFlavor::H9_17(h) => h.insert(q_pt),
			HnswFlavor::H9_25(h) => h.insert(q_pt),
			HnswFlavor::H9set(h) => h.insert(q_pt),
			HnswFlavor::H13_25(h) => h.insert(q_pt),
			HnswFlavor::H13set(h) => h.insert(q_pt),
			HnswFlavor::H17set(h) => h.insert(q_pt),
			HnswFlavor::H21set(h) => h.insert(q_pt),
			HnswFlavor::H25set(h) => h.insert(q_pt),
			HnswFlavor::H29set(h) => h.insert(q_pt),
			HnswFlavor::Hset(h) => h.insert(q_pt),
		}
	}
	fn remove(&mut self, e_id: ElementId) -> bool {
		match self {
			HnswFlavor::H5_9(h) => h.remove(e_id),
			HnswFlavor::H5_17(h) => h.remove(e_id),
			HnswFlavor::H5_25(h) => h.remove(e_id),
			HnswFlavor::H5set(h) => h.remove(e_id),
			HnswFlavor::H9_17(h) => h.remove(e_id),
			HnswFlavor::H9_25(h) => h.remove(e_id),
			HnswFlavor::H9set(h) => h.remove(e_id),
			HnswFlavor::H13_25(h) => h.remove(e_id),
			HnswFlavor::H13set(h) => h.remove(e_id),
			HnswFlavor::H17set(h) => h.remove(e_id),
			HnswFlavor::H21set(h) => h.remove(e_id),
			HnswFlavor::H25set(h) => h.remove(e_id),
			HnswFlavor::H29set(h) => h.remove(e_id),
			HnswFlavor::Hset(h) => h.remove(e_id),
		}
	}
	fn knn_search(&self, q: &SharedVector, k: usize, efs: usize) -> Vec<(f64, ElementId)> {
		match self {
			HnswFlavor::H5_9(h) => h.knn_search(q, k, efs),
			HnswFlavor::H5_17(h) => h.knn_search(q, k, efs),
			HnswFlavor::H5_25(h) => h.knn_search(q, k, efs),
			HnswFlavor::H5set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H9_17(h) => h.knn_search(q, k, efs),
			HnswFlavor::H9_25(h) => h.knn_search(q, k, efs),
			HnswFlavor::H9set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H13_25(h) => h.knn_search(q, k, efs),
			HnswFlavor::H13set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H17set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H21set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H25set(h) => h.knn_search(q, k, efs),
			HnswFlavor::H29set(h) => h.knn_search(q, k, efs),
			HnswFlavor::Hset(h) => h.knn_search(q, k, efs),
		}
	}
	async fn knn_search_checked(
		&self,
		q: &SharedVector,
		k: usize,
		efs: usize,
		vec_docs: &VecDocs,
		stk: &mut Stk,
		condition_checker: &mut ConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>, Error> {
		match self {
			HnswFlavor::H5_9(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H5_17(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H5_25(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H5set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H9_17(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H9_25(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H9set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H13_25(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H13set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H17set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H21set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H25set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::H29set(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
			HnswFlavor::Hset(h) => {
				h.knn_search_checked(q, k, efs, vec_docs, stk, condition_checker).await
			}
		}
	}
	fn get_vector(&self, e_id: &ElementId) -> Option<&SharedVector> {
		match self {
			HnswFlavor::H5_9(h) => h.get_vector(e_id),
			HnswFlavor::H5_17(h) => h.get_vector(e_id),
			HnswFlavor::H5_25(h) => h.get_vector(e_id),
			HnswFlavor::H5set(h) => h.get_vector(e_id),
			HnswFlavor::H9_17(h) => h.get_vector(e_id),
			HnswFlavor::H9_25(h) => h.get_vector(e_id),
			HnswFlavor::H9set(h) => h.get_vector(e_id),
			HnswFlavor::H13_25(h) => h.get_vector(e_id),
			HnswFlavor::H13set(h) => h.get_vector(e_id),
			HnswFlavor::H17set(h) => h.get_vector(e_id),
			HnswFlavor::H21set(h) => h.get_vector(e_id),
			HnswFlavor::H25set(h) => h.get_vector(e_id),
			HnswFlavor::H29set(h) => h.get_vector(e_id),
			HnswFlavor::Hset(h) => h.get_vector(e_id),
		}
	}
	#[cfg(test)]
	fn check_hnsw_properties(&self, expected_count: usize) {
		match self {
			HnswFlavor::H5_9(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H5_17(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H5_25(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H5set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H9_17(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H9_25(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H9set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H13_25(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H13set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H17set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H21set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H25set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::H29set(h) => h.check_hnsw_properties(expected_count),
			HnswFlavor::Hset(h) => h.check_hnsw_properties(expected_count),
		}
	}
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

	pub(crate) fn get_thing(&self, doc_id: DocId) -> Option<&Thing> {
		if let Some(r) = self.docs.ids_doc.get(doc_id as usize) {
			r.as_ref()
		} else {
			None
		}
	}

	pub async fn knn_search(
		&self,
		o: &Vec<Number>,
		n: usize,
		ef: usize,
		stk: &mut Stk,
		mut chk: ConditionChecker<'_>,
	) -> Result<VecDeque<KnnIteratorResult>, Error> {
		// Extract the vector
		let vector: SharedVector = Vector::try_from_vector(self.vector_type, o)?.into();
		vector.check_dimension(self.dim)?;
		// Do the search
		let result = self.search(&vector, n, ef, stk, &mut chk).await?;
		let res = chk.convert_result(result.docs).await?;
		Ok(res)
	}

	async fn search(
		&self,
		o: &SharedVector,
		n: usize,
		ef: usize,
		stk: &mut Stk,
		chk: &mut ConditionChecker<'_>,
	) -> Result<KnnResult, Error> {
		let neighbors = match chk {
			ConditionChecker::Hnsw(_) => self.hnsw.knn_search(o, n, ef),
			ConditionChecker::HnswCondition(_) => {
				self.hnsw.knn_search_checked(o, n, ef, &self.vec_docs, stk, chk).await?
			}
			#[cfg(test)]
			ConditionChecker::None => self.hnsw.knn_search(o, n, ef),
			_ => unreachable!(),
		};
		let result = self.build_result(neighbors, n, chk);
		Ok(result)
	}

	fn build_result(
		&self,
		neighbors: Vec<(f64, ElementId)>,
		n: usize,
		chk: &mut ConditionChecker<'_>,
	) -> KnnResult {
		let mut builder = KnnResultBuilder::new(n);
		for (e_dist, e_id) in neighbors {
			if builder.check_add(e_dist) {
				if let Some(v) = self.hnsw.get_vector(&e_id) {
					if let Some((docs, _)) = self.vec_docs.get(v) {
						builder.add(e_dist, docs, chk);
					}
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

#[cfg(test)]
fn check_hnsw_props<L0, L>(h: &Hnsw<L0, L>, expected_count: usize)
where
	L0: DynamicSet<ElementId>,
	L: DynamicSet<ElementId>,
{
	assert_eq!(h.elements.elements.len(), expected_count);
	for layer in h.layers.iter() {
		layer.check_props(&h.elements);
	}
}

struct HnswElements {
	elements: HashMap<ElementId, SharedVector>,
	next_element_id: ElementId,
	dist: Distance,
}

impl HnswElements {
	fn new(dist: Distance) -> Self {
		Self {
			elements: Default::default(),
			next_element_id: 0,
			dist,
		}
	}

	fn get_vector(&self, e_id: &ElementId) -> Option<&SharedVector> {
		self.elements.get(e_id)
	}

	fn distance(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		self.dist.calculate(a, b)
	}
	fn get_distance(&self, q: &SharedVector, e_id: &ElementId) -> Option<f64> {
		self.elements.get(e_id).map(|e_pt| self.dist.calculate(e_pt, q))
	}

	fn remove(&mut self, e_id: &ElementId) {
		self.elements.remove(e_id);
	}
}

struct Hnsw<L0, L>
where
	L0: DynamicSet<ElementId>,
	L: DynamicSet<ElementId>,
{
	m: usize,
	efc: usize,
	ml: f64,
	layer0: HnswLayer<L0>,
	layers: Vec<HnswLayer<L>>,
	enter_point: Option<ElementId>,
	elements: HnswElements,
	rng: SmallRng,
	heuristic: Heuristic,
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
			efc: p.ef_construction as usize,
			ml: p.ml.to_float(),
			enter_point: None,
			layer0: HnswLayer::new(m0),
			layers: Vec::default(),
			elements: HnswElements::new(p.distance.clone()),
			rng: SmallRng::from_entropy(),
			heuristic: p.into(),
		}
	}

	fn insert_level(&mut self, q_pt: SharedVector, q_level: usize) -> ElementId {
		// Attribute an ID to the vector
		let q_id = self.elements.next_element_id;
		let top_up_layers = self.layers.len();

		// Be sure we have existing (up) layers if required
		for _ in top_up_layers..q_level {
			self.layers.push(HnswLayer::new(self.m));
		}

		// Store the vector
		self.elements.elements.insert(q_id, q_pt.clone());

		if let Some(ep_id) = self.enter_point {
			// We already have an enter_point, let's insert the element in the layers
			self.insert_element(q_id, &q_pt, q_level, ep_id, top_up_layers);
		} else {
			// Otherwise is the first element
			self.insert_first_element(q_id, q_level);
		}

		self.elements.next_element_id += 1;
		q_id
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	fn insert_first_element(&mut self, id: ElementId, level: usize) {
		if level > 0 {
			for layer in self.layers.iter_mut().take(level) {
				layer.add_empty_node(id);
			}
		}
		self.layer0.add_empty_node(id);
		self.enter_point = Some(id);
	}

	fn insert_element(
		&mut self,
		q_id: ElementId,
		q_pt: &SharedVector,
		q_level: usize,
		mut ep_id: ElementId,
		top_up_layers: usize,
	) {
		let mut ep_dist =
			self.elements.get_distance(q_pt, &ep_id).unwrap_or_else(|| unreachable!());

		if q_level < top_up_layers {
			for layer in self.layers[q_level..top_up_layers].iter_mut().rev() {
				(ep_dist, ep_id) = layer
					.search_single(&self.elements, q_pt, ep_dist, ep_id, 1)
					.peek_first()
					.unwrap_or_else(|| unreachable!())
			}
		}

		let mut eps = DoublePriorityQueue::from(ep_dist, ep_id);

		let insert_to_up_layers = q_level.min(top_up_layers);
		if insert_to_up_layers > 0 {
			for layer in self.layers.iter_mut().take(insert_to_up_layers).rev() {
				eps = layer.insert(&self.elements, &self.heuristic, self.efc, q_id, q_pt, eps);
			}
		}

		self.layer0.insert(&self.elements, &self.heuristic, self.efc, q_id, q_pt, eps);

		if top_up_layers < q_level {
			for layer in self.layers[top_up_layers..q_level].iter_mut() {
				if !layer.add_empty_node(q_id) {
					unreachable!("Already there {}", q_id);
				}
			}
		}

		if q_level > top_up_layers {
			self.enter_point = Some(q_id);
		}
	}

	fn insert(&mut self, q_pt: SharedVector) -> ElementId {
		let q_level = self.get_random_level();
		self.insert_level(q_pt, q_level)
	}

	fn remove(&mut self, e_id: ElementId) -> bool {
		let mut removed = false;

		let e_pt = self.elements.get_vector(&e_id).cloned();
		// Do we have the vector?
		if let Some(e_pt) = e_pt {
			let layers = self.layers.len();
			let mut new_enter_point = None;

			// Are we deleting the current enter point?
			if Some(e_id) == self.enter_point {
				// Let's find a new enter point
				new_enter_point = if layers == 0 {
					self.layer0.search_single_ignore_ep(&self.elements, &e_pt, e_id)
				} else {
					self.layers[layers - 1].search_single_ignore_ep(&self.elements, &e_pt, e_id)
				};
			}

			self.elements.remove(&e_id);

			// Remove from the up layers
			for layer in self.layers.iter_mut().rev() {
				if layer.remove(&self.elements, &self.heuristic, e_id, self.efc) {
					removed = true;
				}
			}

			// Remove from layer 0
			if self.layer0.remove(&self.elements, &self.heuristic, e_id, self.efc) {
				removed = true;
			}

			if removed && new_enter_point.is_some() {
				// Update the enter point
				self.enter_point = new_enter_point.map(|(_, e_id)| e_id);
			}
		}
		removed
	}

	fn knn_search(&self, q: &SharedVector, k: usize, efs: usize) -> Vec<(f64, ElementId)> {
		#[cfg(debug_assertions)]
		let expected_w_len = self.elements.elements.len().min(k);
		if let Some((ep_dist, ep_id)) = self.search_ep(q) {
			let w = self.layer0.search_single(&self.elements, q, ep_dist, ep_id, efs);
			#[cfg(debug_assertions)]
			if w.len() < expected_w_len {
				debug!(
						"0 search_layer - ep_id: {ep_id:?} - ef_search: {efs} - k: {k} - w.len: {} < {expected_w_len}",
						w.len()
					);
			}
			w.to_vec_limit(k)
		} else {
			vec![]
		}
	}

	async fn knn_search_checked(
		&self,
		q: &SharedVector,
		k: usize,
		efs: usize,
		vec_docs: &VecDocs,
		stk: &mut Stk,
		chk: &mut ConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>, Error> {
		#[cfg(debug_assertions)]
		let expected_w_len = self.elements.elements.len().min(k);
		if let Some((ep_dist, ep_id)) = self.search_ep(q) {
			let w = self
				.layer0
				.search_single_checked(&self.elements, q, ep_dist, ep_id, efs, vec_docs, stk, chk)
				.await?;
			#[cfg(debug_assertions)]
			if w.len() < expected_w_len {
				debug!(
						"0 search_layer - ep_id: {ep_id:?} - ef_search: {efs} - k: {k} - w.len: {} < {expected_w_len}",
						w.len()
					);
			}
			Ok(w.to_vec_limit(k))
		} else {
			Ok(vec![])
		}
	}

	fn search_ep(&self, q: &SharedVector) -> Option<(f64, ElementId)> {
		if let Some(mut ep_id) = self.enter_point {
			let mut ep_dist =
				self.elements.get_distance(q, &ep_id).unwrap_or_else(|| unreachable!());
			for layer in self.layers.iter().rev() {
				(ep_dist, ep_id) = layer
					.search_single(&self.elements, q, ep_dist, ep_id, 1)
					.peek_first()
					.unwrap_or_else(|| unreachable!());
			}
			Some((ep_dist, ep_id))
		} else {
			None
		}
	}

	fn get_vector(&self, e_id: &ElementId) -> Option<&SharedVector> {
		self.elements.get_vector(e_id)
	}
	#[cfg(test)]
	fn check_hnsw_properties(&self, expected_count: usize) {
		check_hnsw_props(self, expected_count);
	}
}

#[cfg(test)]
mod tests {
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::planner::checker::ConditionChecker;
	use crate::idx::trees::hnsw::{HnswFlavor, HnswIndex};
	use crate::idx::trees::knn::tests::{new_vectors_from_file, TestCollection};
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::sql::index::{Distance, HnswParams, VectorType};
	use hashbrown::{hash_map::Entry, HashMap, HashSet};
	use ndarray::Array1;
	use reblessive::tree::Stk;
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
						if let Some(v) = h.get_vector(e_id) {
							if obj.eq(v) {
								found = true;
								break;
							}
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
		let m = m as u8;
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

	async fn find_collection_hnsw_index(
		stk: &mut Stk,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) {
		let max_knn = 20.min(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let res = h.search(obj, knn, 500, stk, &mut ConditionChecker::None).await.unwrap();
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

	async fn test_hnsw_index(collection_size: usize, unique: bool, p: HnswParams) {
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
		let mut stack = reblessive::tree::TreeStack::new();
		stack
			.enter(|stk| async {
				find_collection_hnsw_index(stk, &mut h, &collection).await;
			})
			.finish()
			.await;
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
							test_hnsw_index(30, unique, p).await;
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
	fn test_simple_hnsw() {
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
		let res = h.knn_search(&pt, knn, efs);
		assert_eq!(res.len(), knn);
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
				let mut stack = reblessive::tree::TreeStack::new();
				stack
					.enter(|stk| async {
						let mut total_recall = 0.0;
						for (_, pt) in queries.to_vec_ref() {
							let knn = 10;
							let hnsw_res = h
								.search(pt, knn, efs, stk, &mut ConditionChecker::None)
								.await
								.unwrap();
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
					})
					.finish()
					.await;
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

	impl TestCollection {
		fn knn(&self, pt: &SharedVector, dist: Distance, n: usize) -> KnnResult {
			let mut b = KnnResultBuilder::new(n);
			for (doc_id, doc_pt) in self.to_vec_ref() {
				let mut chk = ConditionChecker::None;
				let d = dist.calculate(doc_pt, pt);
				if b.check_add(d) {
					b.add(d, &Ids64::One(*doc_id), &mut chk);
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
}
