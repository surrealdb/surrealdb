pub(crate) mod docs;
mod elements;
mod flavor;
mod heuristic;
pub mod index;
mod layer;

use anyhow::Result;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use reblessive::tree::Stk;
use revision::{Revisioned, revisioned};
use serde::{Deserialize, Serialize};

use crate::catalog::{DatabaseDefinition, HnswParams};
use crate::idx::IndexKeyBase;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::hnsw::layer::{HnswLayer, LayerState};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::{KVValue, Transaction};

struct HnswSearch {
	pt: SharedVector,
	k: usize,
	ef: usize,
}

impl HnswSearch {
	pub(super) fn new(pt: SharedVector, k: usize, ef: usize) -> Self {
		Self {
			pt,
			k,
			ef,
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct HnswState {
	enter_point: Option<ElementId>,
	next_element_id: ElementId,
	layer0: LayerState,
	layers: Vec<LayerState>,
}

impl KVValue for HnswState {
	#[inline]
	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let mut val = Vec::new();
		self.serialize_revisioned(&mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> anyhow::Result<Self> {
		Ok(Self::deserialize_revisioned(&mut val.as_slice())?)
	}
}

struct Hnsw<L0, L>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	ikb: IndexKeyBase,
	state: HnswState,
	m: usize,
	efc: usize,
	ml: f64,
	layer0: HnswLayer<L0>,
	layers: Vec<HnswLayer<L>>,
	elements: HnswElements,
	rng: SmallRng,
	heuristic: Heuristic,
}

pub(crate) type ElementId = u64;

impl<L0, L> Hnsw<L0, L>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	fn new(ikb: IndexKeyBase, p: &HnswParams) -> Result<Self> {
		let m0 = p.m0 as usize;
		Ok(Self {
			state: Default::default(),
			m: p.m as usize,
			efc: p.ef_construction as usize,
			ml: p.ml.to_float(),
			layer0: HnswLayer::new(ikb.clone(), 0, m0),
			layers: Vec::default(),
			elements: HnswElements::new(ikb.clone(), p.distance.clone()),
			rng: SmallRng::from_entropy(),
			heuristic: p.into(),
			ikb,
		})
	}

	async fn check_state(&mut self, tx: &Transaction) -> Result<()> {
		// Read the state
		let st: HnswState = tx.get(&self.ikb.new_hs_key(), None).await?.unwrap_or_default();
		// Compare versions
		if st.layer0.version != self.state.layer0.version {
			self.layer0.load(tx, &st.layer0).await?;
		}
		for ((new_stl, stl), layer) in
			st.layers.iter().zip(self.state.layers.iter_mut()).zip(self.layers.iter_mut())
		{
			if new_stl.version != stl.version {
				layer.load(tx, new_stl).await?;
			}
		}
		// Retrieve missing layers
		for i in self.layers.len()..st.layers.len() {
			let mut l = HnswLayer::new(self.ikb.clone(), i + 1, self.m);
			l.load(tx, &st.layers[i]).await?;
			self.layers.push(l);
		}
		// Remove non-existing layers
		for _ in self.layers.len()..st.layers.len() {
			self.layers.pop();
		}
		// Set the enter_point
		self.elements.set_next_element_id(st.next_element_id);
		self.state = st;
		Ok(())
	}

	async fn insert_level(
		&mut self,
		tx: &Transaction,
		q_pt: Vector,
		q_level: usize,
	) -> Result<ElementId> {
		// Attributes an ID to the vector
		let q_id = self.elements.next_element_id();
		let top_up_layers = self.layers.len();

		// Be sure we have existing (up) layers if required
		for i in top_up_layers..q_level {
			self.layers.push(HnswLayer::new(self.ikb.clone(), i + 1, self.m));
			self.state.layers.push(LayerState::default());
		}

		// Store the vector
		let pt_ser = SerializedVector::from(&q_pt);
		let q_pt = self.elements.insert(tx, q_id, q_pt, &pt_ser).await?;

		if let Some(ep_id) = self.state.enter_point {
			// We already have an enter_point, let's insert the element in the layers
			self.insert_element(tx, q_id, &q_pt, q_level, ep_id, top_up_layers).await?;
		} else {
			// Otherwise is the first element
			self.insert_first_element(tx, q_id, q_level).await?;
		}

		self.state.next_element_id = self.elements.inc_next_element_id();
		Ok(q_id)
	}

	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.r#gen(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	async fn insert_first_element(
		&mut self,
		tx: &Transaction,
		id: ElementId,
		level: usize,
	) -> Result<()> {
		if level > 0 {
			// Insert in up levels
			for (layer, state) in
				self.layers.iter_mut().zip(self.state.layers.iter_mut()).take(level)
			{
				layer.add_empty_node(tx, id, state).await?;
			}
		}
		// Insert in layer 0
		self.layer0.add_empty_node(tx, id, &mut self.state.layer0).await?;
		// Update the enter point
		self.state.enter_point = Some(id);
		//
		Ok(())
	}

	async fn insert_element(
		&mut self,
		tx: &Transaction,
		q_id: ElementId,
		q_pt: &SharedVector,
		q_level: usize,
		mut ep_id: ElementId,
		top_up_layers: usize,
	) -> Result<()> {
		if let Some(mut ep_dist) = self.elements.get_distance(tx, q_pt, &ep_id).await? {
			if q_level < top_up_layers {
				for layer in self.layers[q_level..top_up_layers].iter_mut().rev() {
					if let Some(ep_dist_id) = layer
						.search_single(tx, &self.elements, q_pt, ep_dist, ep_id, 1)
						.await?
						.peek_first()
					{
						(ep_dist, ep_id) = ep_dist_id;
					} else {
						#[cfg(debug_assertions)]
						unreachable!()
					}
				}
			}

			let mut eps = DoublePriorityQueue::from(ep_dist, ep_id);

			let insert_to_up_layers = q_level.min(top_up_layers);
			if insert_to_up_layers > 0 {
				for (layer, st) in self
					.layers
					.iter_mut()
					.zip(self.state.layers.iter_mut())
					.take(insert_to_up_layers)
					.rev()
				{
					eps = layer
						.insert(
							(tx, st),
							&self.elements,
							&self.heuristic,
							self.efc,
							(q_id, q_pt),
							eps,
						)
						.await?;
				}
			}

			self.layer0
				.insert(
					(tx, &mut self.state.layer0),
					&self.elements,
					&self.heuristic,
					self.efc,
					(q_id, q_pt),
					eps,
				)
				.await?;

			if top_up_layers < q_level {
				for (layer, st) in self.layers[top_up_layers..q_level]
					.iter_mut()
					.zip(self.state.layers[top_up_layers..q_level].iter_mut())
				{
					if !layer.add_empty_node(tx, q_id, st).await? {
						#[cfg(debug_assertions)]
						unreachable!("Already there {}", q_id);
					}
				}
			}

			if q_level > top_up_layers {
				self.state.enter_point = Some(q_id);
			}
		} else {
			#[cfg(debug_assertions)]
			unreachable!()
		}
		Ok(())
	}

	async fn save_state(&self, tx: &Transaction) -> Result<()> {
		let state_key = self.ikb.new_hs_key();
		tx.set(&state_key, &self.state, None).await?;
		Ok(())
	}

	async fn insert(&mut self, tx: &Transaction, q_pt: Vector) -> Result<ElementId> {
		let q_level = self.get_random_level();
		let res = self.insert_level(tx, q_pt, q_level).await?;
		self.save_state(tx).await?;
		Ok(res)
	}

	async fn remove(&mut self, tx: &Transaction, e_id: ElementId) -> Result<bool> {
		let mut removed = false;

		// Do we have the vector?
		if let Some(e_pt) = self.elements.get_vector(tx, &e_id).await? {
			// Check if we are deleted the current enter_point
			let mut new_enter_point = if Some(e_id) == self.state.enter_point {
				None
			} else {
				self.state.enter_point
			};

			// Remove from the up layers
			for (layer, st) in self.layers.iter_mut().zip(self.state.layers.iter_mut()).rev() {
				if new_enter_point.is_none() {
					new_enter_point = layer
						.search_single_with_ignore(tx, &self.elements, &e_pt, e_id, self.efc)
						.await?;
				}
				if layer.remove(tx, st, &self.elements, &self.heuristic, e_id, self.efc).await? {
					removed = true;
				}
			}

			// Check possible new enter_point at layer0
			if new_enter_point.is_none() {
				new_enter_point = self
					.layer0
					.search_single_with_ignore(tx, &self.elements, &e_pt, e_id, self.efc)
					.await?;
			}

			// Remove from layer 0
			if self
				.layer0
				.remove(tx, &mut self.state.layer0, &self.elements, &self.heuristic, e_id, self.efc)
				.await?
			{
				removed = true;
			}

			self.elements.remove(tx, e_id).await?;

			self.state.enter_point = new_enter_point;
		}

		self.save_state(tx).await?;
		Ok(removed)
	}

	async fn knn_search(
		&self,
		tx: &Transaction,
		search: &HnswSearch,
	) -> Result<Vec<(f64, ElementId)>> {
		if let Some((ep_dist, ep_id)) = self.search_ep(tx, &search.pt).await? {
			let w = self
				.layer0
				.search_single(tx, &self.elements, &search.pt, ep_dist, ep_id, search.ef)
				.await?;
			Ok(w.to_vec_limit(search.k))
		} else {
			Ok(vec![])
		}
	}

	#[expect(clippy::too_many_arguments)]
	async fn knn_search_checked(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		hnsw_docs: &HnswDocs,
		vec_docs: &VecDocs,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>> {
		if let Some((ep_dist, ep_id)) = self.search_ep(tx, &search.pt).await? {
			if let Some(ep_pt) = self.elements.get_vector(tx, &ep_id).await? {
				let search_ctx = HnswCheckedSearchContext::new(
					&self.elements,
					hnsw_docs,
					vec_docs,
					&search.pt,
					search.ef,
				);
				let w = self
					.layer0
					.search_single_checked(db, tx, stk, &search_ctx, &ep_pt, ep_dist, ep_id, chk)
					.await?;
				return Ok(w.to_vec_limit(search.k));
			}
		}
		Ok(vec![])
	}

	async fn search_ep(
		&self,
		tx: &Transaction,
		pt: &SharedVector,
	) -> Result<Option<(f64, ElementId)>> {
		if let Some(mut ep_id) = self.state.enter_point {
			if let Some(mut ep_dist) = self.elements.get_distance(tx, pt, &ep_id).await? {
				for layer in self.layers.iter().rev() {
					if let Some(ep_dist_id) = layer
						.search_single(tx, &self.elements, pt, ep_dist, ep_id, 1)
						.await?
						.peek_first()
					{
						(ep_dist, ep_id) = ep_dist_id;
					} else {
						#[cfg(debug_assertions)]
						unreachable!()
					}
				}
				return Ok(Some((ep_dist, ep_id)));
			} else {
				#[cfg(debug_assertions)]
				unreachable!()
			}
		}
		Ok(None)
	}

	async fn get_vector(&self, tx: &Transaction, e_id: &ElementId) -> Result<Option<SharedVector>> {
		self.elements.get_vector(tx, e_id).await
	}
	#[cfg(test)]
	fn check_hnsw_properties(&self, expected_count: usize) {
		check_hnsw_props(self, expected_count);
	}
}

#[cfg(test)]
fn check_hnsw_props<L0, L>(h: &Hnsw<L0, L>, expected_count: usize)
where
	L0: DynamicSet,
	L: DynamicSet,
{
	assert_eq!(h.elements.len(), expected_count);
	for layer in h.layers.iter() {
		layer.check_props(&h.elements);
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::Entry;
	use std::ops::Deref;
	use std::sync::Arc;

	use ahash::{HashMap, HashSet};
	use anyhow::Result;
	use ndarray::Array1;
	use reblessive::tree::Stk;
	use roaring::RoaringTreemap;
	use test_log::test;

	use crate::catalog::{
		DatabaseDefinition, DatabaseId, Distance, HnswParams, NamespaceId, VectorType,
	};
	use crate::ctx::{Context, MutableContext};
	use crate::idx::IndexKeyBase;
	use crate::idx::docids::DocId;
	use crate::idx::planner::checker::HnswConditionChecker;
	use crate::idx::trees::hnsw::flavor::HnswFlavor;
	use crate::idx::trees::hnsw::index::HnswIndex;
	use crate::idx::trees::hnsw::{ElementId, HnswSearch};
	use crate::idx::trees::knn::tests::{TestCollection, new_vectors_from_file};
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::{Datastore, Transaction, TransactionType};
	use crate::val::{RecordIdKey, Value};

	async fn insert_collection_hnsw(
		tx: &Transaction,
		h: &mut HnswFlavor,
		collection: &TestCollection,
	) -> HashMap<ElementId, SharedVector> {
		let mut map = HashMap::default();
		for (_, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone();
			let e_id = h.insert(tx, obj.clone_vector()).await.unwrap();
			map.insert(e_id, obj);
			h.check_hnsw_properties(map.len());
		}
		map
	}

	async fn find_collection_hnsw(tx: &Transaction, h: &HnswFlavor, collection: &TestCollection) {
		let max_knn = 20.min(collection.len());
		for (_, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let search = HnswSearch::new(obj.clone(), knn, 80);
				let res = h.knn_search(tx, &search).await.unwrap();
				if collection.is_unique() {
					let mut found = false;
					for (_, e_id) in &res {
						if let Some(v) = h.get_vector(tx, e_id).await.unwrap() {
							if v.eq(obj) {
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

	async fn delete_collection_hnsw(
		tx: &Transaction,
		h: &mut HnswFlavor,
		mut map: HashMap<ElementId, SharedVector>,
	) {
		let element_ids: Vec<ElementId> = map.keys().copied().collect();
		for e_id in element_ids {
			assert!(h.remove(tx, e_id).await.unwrap());
			map.remove(&e_id);
			h.check_hnsw_properties(map.len());
		}
	}

	async fn test_hnsw_collection(p: &HnswParams, collection: &TestCollection) {
		let ds = Datastore::new("memory").await.unwrap();
		let mut h =
			HnswFlavor::new(IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"), p)
				.unwrap();
		let map = {
			let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
			let map = insert_collection_hnsw(&tx, &mut h, collection).await;
			tx.commit().await.unwrap();
			map
		};
		{
			let tx = ds.transaction(TransactionType::Read, Optimistic).await.unwrap();
			find_collection_hnsw(&tx, &h, collection).await;
			tx.cancel().await.unwrap();
		}
		{
			let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
			delete_collection_hnsw(&tx, &mut h, map).await;
			tx.commit().await.unwrap();
		}
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
		HnswParams {
			dimension: dimension as u16,
			distance,
			vector_type,
			m,
			m0,
			ml: (1.0 / (m as f64).ln()).into(),
			ef_construction: efc as u16,
			extend_candidates,
			keep_pruned_connections,
		}
	}

	async fn test_hnsw(collection_size: usize, p: HnswParams) {
		info!("Collection size: {collection_size} - Params: {p:?}");
		let collection = TestCollection::new(
			true,
			collection_size,
			p.vector_type,
			p.dimension as usize,
			&p.distance,
		);
		test_hnsw_collection(&p, &collection).await;
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn tests_hnsw() -> Result<()> {
		let mut futures = Vec::new();
		for (dist, dim) in [
			(Distance::Chebyshev, 5),
			(Distance::Cosine, 5),
			(Distance::Euclidean, 5),
			(Distance::Hamming, 20),
			// (Distance::Jaccard, 100),
			(Distance::Manhattan, 5),
			(Distance::Minkowski(2.into()), 5),
			// (Distance::Pearson, 5),
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
						test_hnsw(30, p).await;
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

	async fn insert_collection_hnsw_index(
		tx: &Transaction,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> Result<HashMap<SharedVector, HashSet<DocId>>> {
		let mut map: HashMap<SharedVector, HashSet<DocId>> = HashMap::default();
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.index_document(tx, &RecordIdKey::Number(*doc_id as i64), &content).await.unwrap();
			match map.entry(obj.clone()) {
				Entry::Occupied(mut e) => {
					e.get_mut().insert(*doc_id);
				}
				Entry::Vacant(e) => {
					e.insert(HashSet::from_iter([*doc_id]));
				}
			}
			h.check_hnsw_properties(map.len());
		}
		Ok(map)
	}

	async fn find_collection_hnsw_index(
		tx: &Transaction,
		db: &DatabaseDefinition,
		stk: &mut Stk,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) {
		let max_knn = 20.min(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let mut chk = HnswConditionChecker::new();
				let search = HnswSearch::new(obj.clone(), knn, 500);
				let res = h.search(db, tx, stk, &search, &mut chk).await.unwrap();
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

	async fn delete_hnsw_index_collection(
		tx: &Transaction,
		h: &mut HnswIndex,
		collection: &TestCollection,
		mut map: HashMap<SharedVector, HashSet<DocId>>,
	) -> Result<()> {
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.remove_document(tx, RecordIdKey::Number(*doc_id as i64), &content).await?;
			if let Entry::Occupied(mut e) = map.entry(obj.clone()) {
				let set = e.get_mut();
				set.remove(doc_id);
				if set.is_empty() {
					e.remove();
				}
			}
			// Check properties
			h.check_hnsw_properties(map.len());
		}
		Ok(())
	}

	async fn new_ctx(ds: &Datastore, tt: TransactionType) -> Context {
		let tx = Arc::new(ds.transaction(tt, Optimistic).await.unwrap());
		let mut ctx = MutableContext::default();
		ctx.set_transaction(tx);
		ctx.freeze()
	}

	async fn test_hnsw_index(collection_size: usize, unique: bool, p: HnswParams) {
		info!("test_hnsw_index - coll size: {collection_size} - params: {p:?}");

		let ds = Datastore::new("memory").await.unwrap();

		let collection = TestCollection::new(
			unique,
			collection_size,
			p.vector_type,
			p.dimension as usize,
			&p.distance,
		);

		// Create index
		let (mut h, map) = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let mut h = HnswIndex::new(
				&tx,
				IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
				"test".to_string(),
				&p,
			)
			.await
			.unwrap();
			// Fill index
			let map = insert_collection_hnsw_index(&tx, &mut h, &collection).await.unwrap();
			tx.commit().await.unwrap();
			(h, map)
		};

		// Search index
		{
			let mut stack = reblessive::tree::TreeStack::new();
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();

			let db = tx.ensure_ns_db("myns", "mydb", false).await.unwrap();

			stack
				.enter(|stk| async {
					find_collection_hnsw_index(&tx, &db, stk, &mut h, &collection).await;
				})
				.finish()
				.await;
		}

		// Delete collection
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			delete_hnsw_index_collection(&tx, &mut h, &collection, map).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn tests_hnsw_index() -> Result<()> {
		let mut futures = Vec::new();
		for (dist, dim) in [
			(Distance::Chebyshev, 5),
			(Distance::Cosine, 5),
			(Distance::Euclidean, 5),
			(Distance::Hamming, 20),
			// (Distance::Jaccard, 100),
			(Distance::Manhattan, 5),
			(Distance::Minkowski(2.into()), 5),
			// (Distance::Pearson, 5),
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for (extend, keep) in [(false, false), (true, false), (false, true), (true, true)] {
					for unique in [true, false] {
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

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_simple_hnsw() {
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
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix");
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true);
		let mut h = HnswFlavor::new(ikb, &p).unwrap();
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		{
			let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
			insert_collection_hnsw(&tx, &mut h, &collection).await;
			tx.commit().await.unwrap();
		}
		{
			let tx = ds.transaction(TransactionType::Read, Optimistic).await.unwrap();
			let search = HnswSearch::new(new_i16_vec(-2, -3), 10, 501);
			let res = h.knn_search(&tx, &search).await.unwrap();
			assert_eq!(res.len(), 10);
		}
	}

	async fn test_recall(
		embeddings_file: &str,
		ingest_limit: usize,
		queries_file: &str,
		query_limit: usize,
		p: HnswParams,
		tests_ef_recall: &[(usize, f64)],
	) -> Result<()> {
		info!("Build data collection");

		let ds = Arc::new(Datastore::new("memory").await?);
		let tx = ds.transaction(TransactionType::Write, Optimistic).await.unwrap();
		let db = tx.ensure_ns_db("myns", "mydb", false).await.unwrap();
		tx.commit().await.unwrap();

		let collection: Arc<TestCollection> =
			Arc::new(TestCollection::NonUnique(new_vectors_from_file(
				p.vector_type,
				&format!("../../tests/data/{embeddings_file}"),
				Some(ingest_limit),
			)?));

		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let mut h = HnswIndex::new(
			&tx,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb", "ix"),
			"Index".to_string(),
			&p,
		)
		.await?;
		info!("Insert collection");
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.index_document(&tx, &RecordIdKey::Number(*doc_id as i64), &content).await?;
		}
		tx.commit().await?;

		let h = Arc::new(h);

		info!("Build query collection");
		let queries = Arc::new(TestCollection::NonUnique(new_vectors_from_file(
			p.vector_type,
			&format!("../../tests/data/{queries_file}"),
			Some(query_limit),
		)?));

		info!("Check recall");
		let mut futures = Vec::with_capacity(tests_ef_recall.len());
		for &(efs, expected_recall) in tests_ef_recall {
			let queries = queries.clone();
			let collection = collection.clone();
			let h = h.clone();
			let ds = ds.clone();
			let db = db.clone();
			let f = tokio::spawn(async move {
				let mut stack = reblessive::tree::TreeStack::new();
				stack
					.enter(|stk| async {
						let mut total_recall = 0.0;
						for (_, pt) in queries.to_vec_ref() {
							let knn = 10;
							let mut chk = HnswConditionChecker::new();
							let search = HnswSearch::new(pt.clone(), knn, efs);

							let ctx = new_ctx(&ds, TransactionType::Read).await;
							let tx = ctx.tx();
							let hnsw_res =
								h.search(&db, &tx, stk, &search, &mut chk).await.unwrap();
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
	async fn test_recall_euclidean() -> Result<()> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, false);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			1000,
			"hnsw-random-5000-20-euclidean.gz",
			300,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_recall_euclidean_keep_pruned_connections() -> Result<()> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, true);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			750,
			"hnsw-random-5000-20-euclidean.gz",
			200,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_recall_euclidean_full() -> Result<()> {
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, true, true);
		test_recall(
			"hnsw-random-9000-20-euclidean.gz",
			500,
			"hnsw-random-5000-20-euclidean.gz",
			100,
			p,
			&[(10, 0.98), (40, 1.0)],
		)
		.await
	}

	impl TestCollection {
		fn knn(&self, pt: &SharedVector, dist: Distance, n: usize) -> KnnResult {
			let mut b = KnnResultBuilder::new(n);
			for (doc_id, doc_pt) in self.to_vec_ref() {
				let d = dist.calculate(doc_pt, pt);
				if b.check_add(d) {
					b.add(d, Ids64::One(*doc_id));
				}
			}
			b.build(
				#[cfg(debug_assertions)]
				HashMap::default(),
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
