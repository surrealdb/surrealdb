pub(in crate::idx) mod docs;
mod elements;
mod flavor;
mod heuristic;
pub mod index;
mod layer;

use crate::err::Error;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::docs::VecDocs;
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;

use crate::idx::trees::hnsw::layer::HnswLayer;
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::SharedVector;
use crate::kvs::Transaction;
use crate::sql::index::HnswParams;
use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};
use reblessive::tree::Stk;

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

pub(crate) type ElementId = u64;

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
		let q_id = self.elements.next_element_id();
		let top_up_layers = self.layers.len();

		// Be sure we have existing (up) layers if required
		for _ in top_up_layers..q_level {
			self.layers.push(HnswLayer::new(self.m));
		}

		// Store the vector
		self.elements.insert(q_id, q_pt.clone());

		if let Some(ep_id) = self.enter_point {
			// We already have an enter_point, let's insert the element in the layers
			self.insert_element(q_id, &q_pt, q_level, ep_id, top_up_layers);
		} else {
			// Otherwise is the first element
			self.insert_first_element(q_id, q_level);
		}

		self.elements.inc_next_element_id();
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
		if let Some(mut ep_dist) = self.elements.get_distance(q_pt, &ep_id) {
			if q_level < top_up_layers {
				for layer in self.layers[q_level..top_up_layers].iter_mut().rev() {
					if let Some(ep_dist_id) =
						layer.search_single(&self.elements, q_pt, ep_dist, ep_id, 1).peek_first()
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
				for layer in self.layers.iter_mut().take(insert_to_up_layers).rev() {
					eps = layer.insert(&self.elements, &self.heuristic, self.efc, q_id, q_pt, eps);
				}
			}

			self.layer0.insert(&self.elements, &self.heuristic, self.efc, q_id, q_pt, eps);

			if top_up_layers < q_level {
				for layer in self.layers[top_up_layers..q_level].iter_mut() {
					if !layer.add_empty_node(q_id) {
						#[cfg(debug_assertions)]
						unreachable!("Already there {}", q_id);
					}
				}
			}

			if q_level > top_up_layers {
				self.enter_point = Some(q_id);
			}
		} else {
			#[cfg(debug_assertions)]
			unreachable!()
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

	fn knn_search(&self, search: &HnswSearch) -> Vec<(f64, ElementId)> {
		if let Some((ep_dist, ep_id)) = self.search_ep(&search.pt) {
			let w =
				self.layer0.search_single(&self.elements, &search.pt, ep_dist, ep_id, search.ef);
			w.to_vec_limit(search.k)
		} else {
			vec![]
		}
	}

	async fn knn_search_checked(
		&self,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		hnsw_docs: &HnswDocs,
		vec_docs: &VecDocs,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>, Error> {
		if let Some((ep_dist, ep_id)) = self.search_ep(&search.pt) {
			if let Some(ep_pt) = self.elements.get_vector(&ep_id) {
				let search_ctx = HnswCheckedSearchContext::new(
					&self.elements,
					hnsw_docs,
					vec_docs,
					&search.pt,
					search.ef,
				);
				let w = self
					.layer0
					.search_single_checked(tx, stk, &search_ctx, ep_pt, ep_dist, ep_id, chk)
					.await?;
				return Ok(w.to_vec_limit(search.k));
			}
		}
		Ok(vec![])
	}

	fn search_ep(&self, pt: &SharedVector) -> Option<(f64, ElementId)> {
		if let Some(mut ep_id) = self.enter_point {
			if let Some(mut ep_dist) = self.elements.get_distance(pt, &ep_id) {
				for layer in self.layers.iter().rev() {
					if let Some(ep_dist_id) =
						layer.search_single(&self.elements, pt, ep_dist, ep_id, 1).peek_first()
					{
						(ep_dist, ep_id) = ep_dist_id;
					} else {
						#[cfg(debug_assertions)]
						unreachable!()
					}
				}
				return Some((ep_dist, ep_id));
			} else {
				#[cfg(debug_assertions)]
				unreachable!()
			}
		}
		None
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
fn check_hnsw_props<L0, L>(h: &Hnsw<L0, L>, expected_count: usize)
where
	L0: DynamicSet<ElementId>,
	L: DynamicSet<ElementId>,
{
	assert_eq!(h.elements.len(), expected_count);
	for layer in h.layers.iter() {
		layer.check_props(&h.elements);
	}
}

#[cfg(test)]
mod tests {
	use crate::ctx::Context;
	use crate::err::Error;
	use crate::idx::docids::DocId;
	use crate::idx::planner::checker::HnswConditionChecker;
	use crate::idx::trees::hnsw::flavor::HnswFlavor;
	use crate::idx::trees::hnsw::index::HnswIndex;
	use crate::idx::trees::hnsw::HnswSearch;
	use crate::idx::trees::knn::tests::{new_vectors_from_file, TestCollection};
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SharedVector, Vector};
	use crate::idx::IndexKeyBase;
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::{Datastore, Transaction, TransactionType};
	use crate::sql::index::{Distance, HnswParams, VectorType};
	use ahash::{HashMap, HashSet};
	use ndarray::Array1;
	use reblessive::tree::Stk;
	use roaring::RoaringTreemap;
	use std::collections::hash_map::Entry;
	use std::sync::Arc;
	use test_log::test;

	fn insert_collection_hnsw(
		h: &mut HnswFlavor,
		collection: &TestCollection,
	) -> HashSet<SharedVector> {
		let mut set = HashSet::default();
		for (_, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone();
			h.insert(obj.clone());
			set.insert(obj);
			h.check_hnsw_properties(set.len());
		}
		set
	}
	fn find_collection_hnsw(h: &HnswFlavor, collection: &TestCollection) {
		let max_knn = 20.min(collection.len());
		for (_, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let search = HnswSearch::new(obj.clone(), knn, 80);
				let res = h.knn_search(&search);
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
		find_collection_hnsw(&h, collection);
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

	async fn insert_collection_hnsw_index(
		tx: &Transaction,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> Result<HashMap<SharedVector, HashSet<DocId>>, Error> {
		let mut map: HashMap<SharedVector, HashSet<DocId>> = HashMap::default();
		for (doc_id, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone();
			h.insert(tx, obj.clone(), *doc_id).await?;
			match map.entry(obj) {
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
		stk: &mut Stk,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) {
		let max_knn = 20.min(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let mut chk = HnswConditionChecker::new();
				let search = HnswSearch::new(obj.clone(), knn, 500);
				let res = h.search(tx, stk, &search, &mut chk).await.unwrap();
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
	) -> Result<(), Error> {
		for (doc_id, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone();
			h.remove(tx, obj.clone(), *doc_id).await?;
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

	async fn new_ctx(ds: &Datastore, tt: TransactionType) -> Context<'_> {
		let tx = Arc::new(ds.transaction(tt, Optimistic).await.unwrap());
		let mut ctx = Context::default();
		ctx.set_transaction(tx);
		ctx
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
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let mut h =
			HnswIndex::new(&tx, IndexKeyBase::default(), "test".to_string(), &p).await.unwrap();
		// Fill index
		let map = insert_collection_hnsw_index(&tx, &mut h, &collection).await.unwrap();
		tx.commit().await.unwrap();

		// Search index
		let mut stack = reblessive::tree::TreeStack::new();
		let ctx = new_ctx(&ds, TransactionType::Read).await;
		let tx = ctx.tx();
		stack
			.enter(|stk| async {
				find_collection_hnsw_index(&tx, stk, &mut h, &collection).await;
			})
			.finish()
			.await;

		// Delete collection
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		delete_hnsw_index_collection(&tx, &mut h, &collection, map).await.unwrap();
		tx.commit().await.unwrap();
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
		let search = HnswSearch::new(new_i16_vec(-2, -3), 10, 501);
		let res = h.knn_search(&search);
		assert_eq!(res.len(), 10);
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

		let ds = Arc::new(Datastore::new("memory").await?);

		let collection: Arc<TestCollection> =
			Arc::new(TestCollection::NonUnique(new_vectors_from_file(
				p.vector_type,
				&format!("../tests/data/{embeddings_file}"),
				Some(ingest_limit),
			)?));

		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let mut h = HnswIndex::new(&tx, IndexKeyBase::default(), "Index".to_string(), &p).await?;
		info!("Insert collection");
		for (doc_id, obj) in collection.to_vec_ref() {
			h.insert(&tx, obj.clone(), *doc_id).await?;
		}
		tx.commit().await?;

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
			let ds = ds.clone();
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
							let hnsw_res = h.search(&tx, stk, &search, &mut chk).await.unwrap();
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
			1000,
			"hnsw-random-5000-20-euclidean.gz",
			300,
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
			750,
			"hnsw-random-5000-20-euclidean.gz",
			200,
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
