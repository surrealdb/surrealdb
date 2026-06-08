pub(crate) mod cache;
pub(crate) mod docs;
mod elements;
mod filter;
mod flavor;
mod heuristic;
pub mod index;
mod layer;

use std::sync::Arc;

use anyhow::Result;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use reblessive::tree::Stk;
use revision::{DeserializeRevisioned, SerializeRevisioned, revisioned};
use roaring::RoaringTreemap;
use serde::{Deserialize, Serialize};

use crate::catalog::{HnswParams, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::seqdocids::DocId;
use crate::idx::trees::dynamicset::DynamicSet;
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::elements::HnswElements;
use crate::idx::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::idx::trees::hnsw::heuristic::Heuristic;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::hnsw::layer::{HnswLayer, LayerState};
use crate::idx::trees::knn::DoublePriorityQueue;
use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
use crate::kvs::{KVValue, Transaction, impl_kv_value_revisioned};
use crate::val::RecordIdKey;

/// Parameters for a k-nearest neighbor search on the HNSW graph.
struct HnswSearch {
	/// The query vector to search for.
	pt: SharedVector,
	/// The number of nearest neighbors to return.
	k: usize,
	/// The size of the dynamic candidate list during search (exploration factor).
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

/// Persisted state of the HNSW graph, stored in the key-value store.
///
/// Tracks the current entry point, element ID counter, and per-layer state.
/// This state is loaded at startup and saved after each mutation to ensure
/// consistency across concurrent transactions.
#[revisioned(revision = 1)]
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct HnswState {
	/// The entry point element for graph traversal, or `None` if the graph is empty.
	enter_point: Option<ElementId>,
	/// The next available element ID for new insertions.
	next_element_id: ElementId,
	/// State of layer 0 (the base layer containing all elements).
	layer0: LayerState,
	/// State of the upper layers (layers 1..N with progressively fewer elements).
	layers: Vec<LayerState>,
}

impl KVValue for HnswState {
	type KeyContext = ();

	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(self, &mut val)?;
		Ok(val)
	}

	#[inline]
	fn kv_decode_value(mut val: &[u8], _: ()) -> Result<Self> {
		Ok(DeserializeRevisioned::deserialize_revisioned(&mut val)?)
	}
}

/// Coalesced pending vector state for a single record.
///
/// This value is stored under the record-keyed `!hr` pending key. The key
/// identifies the record; `doc_id` records the current graph document mapping
/// when one already exists. `old_vectors` is the graph baseline to remove, and
/// `new_vectors` is the latest desired indexed state for that record.
#[revisioned(revision = 1)]
pub(crate) struct HnswRecordPendingUpdate {
	/// Existing internal document ID, if the record has already reached the graph.
	doc_id: Option<DocId>,
	/// Vectors currently represented in the graph for this pending record.
	old_vectors: Vec<SerializedVector>,
	/// Latest vectors that should represent the record after compaction.
	new_vectors: Vec<SerializedVector>,
}

/// A pending vector update queued for later application to the HNSW graph.
///
/// During concurrent writes, vector updates are not applied directly to the graph.
/// Instead, they are serialized to the key-value store as pending updates and later
/// applied in batch by a background task via [`HnswIndex::index_pendings`].
#[revisioned(revision = 1)]
pub(crate) struct VectorPendingUpdate {
	/// Identifies the document being updated (by doc ID if known, or record key if new).
	id: VectorId,
	/// The previous vectors to remove from the index (empty for new documents).
	old_vectors: Vec<SerializedVector>,
	/// The new vectors to insert into the index (empty for deletions).
	new_vectors: Vec<SerializedVector>,
}

/// Identifies a vector's owning document, either by its internal doc ID or its record key.
///
/// When a document is first indexed, its doc ID may not yet be assigned, so the
/// record key is used. Once the pending update is applied, the doc ID is resolved.
#[revisioned(revision = 1)]
#[derive(Debug, PartialOrd, Ord, Hash, PartialEq, Eq, Clone)]
pub(crate) enum VectorId {
	/// A previously resolved internal document ID.
	DocId(DocId),
	/// A record key for a document whose doc ID has not yet been resolved.
	RecordKey(Arc<RecordIdKey>),
}

impl_kv_value_revisioned!(HnswRecordPendingUpdate);
impl_kv_value_revisioned!(VectorPendingUpdate);

/// Core HNSW (Hierarchical Navigable Small World) graph implementation.
///
/// The graph is organized into multiple layers: a base layer (layer 0) that contains
/// all elements, and upper layers with progressively fewer elements for fast
/// long-range traversal. The type parameters `L0` and `L` control the neighbor
/// set implementation for layer 0 and upper layers respectively, allowing
/// compile-time optimization based on the `m` (max connections) parameter.
struct Hnsw<L0, L>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	/// Key base for generating index-related storage keys.
	ikb: IndexKeyBase,
	/// Persisted graph state (entry point, element counter, layer states).
	state: HnswState,
	/// Maximum number of connections per element in upper layers.
	m: usize,
	/// Size of the dynamic candidate list during construction.
	efc: usize,
	/// Level multiplier used in the random level generation formula.
	ml: f64,
	/// The base layer (layer 0) containing all elements.
	layer0: HnswLayer<L0>,
	/// Upper layers (1..N), each containing a subset of elements.
	layers: Vec<HnswLayer<L>>,
	/// Storage and cache for element vectors.
	elements: HnswElements,
	/// Random number generator for level assignment.
	rng: SmallRng,
	/// Heuristic strategy for neighbor selection.
	heuristic: Heuristic,
}

/// Unique identifier for an element (vector) in the HNSW graph.
pub(crate) type ElementId = u64;

impl<L0, L> Hnsw<L0, L>
where
	L0: DynamicSet,
	L: DynamicSet,
{
	/// Creates a new HNSW graph with the given parameters.
	fn new(
		table_id: TableId,
		ikb: IndexKeyBase,
		p: &HnswParams,
		vector_cache: VectorCache,
	) -> Result<Self> {
		let m0 = p.m0 as usize;
		Ok(Self {
			state: Default::default(),
			m: p.m as usize,
			efc: p.ef_construction as usize,
			ml: p.ml.to_float(),
			layer0: HnswLayer::new(ikb.clone(), 0, m0),
			layers: Vec::default(),
			elements: HnswElements::new(table_id, ikb.clone(), p.distance.clone(), vector_cache),
			rng: SmallRng::from_rng(&mut rand::rng()),
			heuristic: p.into(),
			ikb,
		})
	}

	/// Returns `true` if the persisted state has drifted from the in-memory state
	/// in a way that would cause [`check_state`](Self::check_state) to mutate.
	///
	/// Safe to call under a shared (read) lock — performs only a KV read of the
	/// `hs` key plus a few field comparisons. Used as the steady-state fast path
	/// so concurrent kNN searches do not serialise on the graph write lock.
	async fn needs_state_reload(&self, ctx: &FrozenContext) -> Result<bool> {
		let tx = ctx.tx();
		let st: HnswState = tx.get(&self.ikb.new_hs_key(), None).await?.unwrap_or_default();
		// Writable transactions may need to migrate legacy `Hl` layout even when
		// versions match. Mirrors `force_migration` in `check_state`.
		if tx.writeable() && st.layer0.chunks > 0 {
			return Ok(true);
		}
		if st.layer0.version != self.state.layer0.version {
			return Ok(true);
		}
		if st.layers.len() != self.state.layers.len() {
			return Ok(true);
		}
		if st.layers.len() != self.layers.len() {
			return Ok(true);
		}
		for (new_stl, stl) in st.layers.iter().zip(self.state.layers.iter()) {
			if new_stl.version != stl.version {
				return Ok(true);
			}
		}
		if st.next_element_id != self.elements.next_element_id() {
			return Ok(true);
		}
		Ok(false)
	}

	/// Loads and synchronizes the in-memory graph state from the key-value store.
	///
	/// Compares the stored layer versions with the current in-memory versions,
	/// reloading any layers that have changed. Also handles layer migration
	/// from the legacy `Hl` format to the current `Hn` format.
	async fn check_state(&mut self, ctx: &FrozenContext) -> Result<()> {
		let tx = ctx.tx();
		// Read the state
		let mut st: HnswState = tx.get(&self.ikb.new_hs_key(), None).await?.unwrap_or_default();
		// Possible migration
		let mut migrated = false;
		let force_migration = tx.writeable() && st.layer0.chunks > 0;
		// Compare versions
		if st.layer0.version != self.state.layer0.version || force_migration {
			migrated |= self.layer0.load(ctx, &tx, &mut st.layer0).await?;
		}
		for ((new_stl, stl), layer) in
			st.layers.iter_mut().zip(self.state.layers.iter_mut()).zip(self.layers.iter_mut())
		{
			if new_stl.version != stl.version || force_migration {
				migrated |= layer.load(ctx, &tx, new_stl).await?;
			}
		}
		// Retrieve missing layers
		for i in self.layers.len()..st.layers.len() {
			let mut l = HnswLayer::new(self.ikb.clone(), i + 1, self.m);
			migrated |= l.load(ctx, &tx, &mut st.layers[i]).await?;
			self.layers.push(l);
		}
		// Remove non-existing layers
		while self.layers.len() > st.layers.len() {
			self.layers.pop();
		}
		// Set the enter_point
		self.elements.set_next_element_id(st.next_element_id);
		self.state = st;
		// If any layer was migrated from Hl to Hn, persist the updated state
		// so that subsequent loads don't attempt to fetch the now-deleted Hl keys.
		if migrated {
			self.save_state(&tx).await?;
		}
		Ok(())
	}

	/// Inserts a vector into the graph at the specified level.
	///
	/// Assigns a new element ID, creates any missing upper layers, stores
	/// the vector, and connects it to its nearest neighbors at each layer.
	async fn insert_level(
		&mut self,
		ctx: &HnswContext<'_>,
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
		let q_pt = self.elements.insert(&ctx.tx, q_id, q_pt, &pt_ser).await?;

		if let Some(ep_id) = self.state.enter_point {
			// We already have an enter_point, let's insert the element in the layers
			self.insert_element(ctx, q_id, &q_pt, q_level, ep_id, top_up_layers).await?;
		} else {
			// Otherwise is the first element
			self.insert_first_element(&ctx.tx, q_id, q_level).await?;
		}

		self.state.next_element_id = self.elements.inc_next_element_id();
		Ok(q_id)
	}

	/// Generates a random level for a new element using the level multiplier `ml`.
	fn get_random_level(&mut self) -> usize {
		let unif: f64 = self.rng.random(); // generate a uniform random number between 0 and 1
		(-unif.ln() * self.ml).floor() as usize // calculate the layer
	}

	/// Inserts the very first element into an empty graph, setting it as the entry point.
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

	/// Inserts an element into the graph when an entry point already exists.
	///
	/// Traverses the upper layers to find the closest entry point, then inserts
	/// the element into each layer from `q_level` down to layer 0, connecting
	/// it to its nearest neighbors. Updates the entry point if the new element
	/// is assigned to a higher layer than the current entry point.
	async fn insert_element(
		&mut self,
		ctx: &HnswContext<'_>,
		q_id: ElementId,
		q_pt: &SharedVector,
		q_level: usize,
		mut ep_id: ElementId,
		top_up_layers: usize,
	) -> Result<()> {
		if let Some(mut ep_dist) = self.elements.get_distance(&ctx.tx, q_pt, &ep_id).await? {
			if q_level < top_up_layers {
				for layer in self.layers[q_level..top_up_layers].iter_mut().rev() {
					if let Some(ep_dist_id) = layer
						.search_single(ctx, &self.elements, q_pt, ep_dist, ep_id, 1, None)
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
							ctx,
							st,
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
					ctx,
					&mut self.state.layer0,
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
					if !layer.add_empty_node(&ctx.tx, q_id, st).await? {
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

	/// Persists the current graph state to the key-value store.
	async fn save_state(&self, tx: &Transaction) -> Result<()> {
		let state_key = self.ikb.new_hs_key();
		tx.set(&state_key, &self.state).await?;
		Ok(())
	}

	/// Inserts a vector into the graph at a randomly chosen level and persists the state.
	async fn insert(&mut self, ctx: &HnswContext<'_>, q_pt: Vector) -> Result<ElementId> {
		let q_level = self.get_random_level();
		let res = self.insert_level(ctx, q_pt, q_level).await?;
		self.save_state(&ctx.tx).await?;
		Ok(res)
	}

	/// Removes an element from the graph, reconnecting its neighbors and updating
	/// the entry point if necessary. Returns `true` if the element was found and removed.
	async fn remove(&mut self, ctx: &HnswContext<'_>, e_id: ElementId) -> Result<bool> {
		let mut removed = false;

		// Do we have the vector?
		if let Some(e_pt) = self.elements.get_vector(&ctx.tx, &e_id).await? {
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
						.search_single_with_ignore(ctx, &self.elements, &e_pt, e_id, self.efc)
						.await?;
				}
				if layer.remove(ctx, st, &self.elements, &self.heuristic, e_id, self.efc).await? {
					removed = true;
				}
			}

			// Check possible new enter_point at layer0
			if new_enter_point.is_none() {
				new_enter_point = self
					.layer0
					.search_single_with_ignore(ctx, &self.elements, &e_pt, e_id, self.efc)
					.await?;
			}

			// Remove from layer 0
			if self
				.layer0
				.remove(
					ctx,
					&mut self.state.layer0,
					&self.elements,
					&self.heuristic,
					e_id,
					self.efc,
				)
				.await?
			{
				removed = true;
			}

			self.elements.remove(&ctx.tx, e_id).await?;

			self.state.enter_point = new_enter_point;
		}

		self.save_state(&ctx.tx).await?;
		Ok(removed)
	}

	/// Performs a k-nearest neighbor search on the graph without filtering.
	///
	/// Optionally excludes documents present in `pending_docs` (those with
	/// pending updates that have already been searched separately).
	async fn knn_search(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		if let Some((ep_dist, ep_id)) = self.search_ep(ctx, &search.pt, pending_docs).await? {
			let w = self
				.layer0
				.search_single(
					ctx,
					&self.elements,
					&search.pt,
					ep_dist,
					ep_id,
					search.ef,
					pending_docs,
				)
				.await?;
			Ok(w.to_vec_limit(search.k))
		} else {
			Ok(vec![])
		}
	}

	/// Performs a k-nearest neighbor search with a conditional document filter.
	///
	/// Similar to [`knn_search`](Self::knn_search), but additionally applies a
	/// user-defined filter to exclude non-matching documents from the results.
	async fn knn_search_with_filter(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		stk: &mut Stk,
		filter: &mut HnswTruthyDocumentFilter<'_>,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		if let Some((ep_dist, ep_id)) = self.search_ep(ctx, &search.pt, pending_docs).await?
			&& self.elements.get_vector(&ctx.tx, &ep_id).await?.is_some()
		{
			let w = self
				.layer0
				.search_single_with_filter(
					ctx,
					stk,
					&self.elements,
					search,
					ep_dist,
					ep_id,
					filter,
					pending_docs,
				)
				.await?;
			return Ok(w.to_vec_limit(search.k));
		}
		Ok(vec![])
	}

	/// Finds the best entry point for a search by traversing the upper layers.
	///
	/// Starting from the graph's entry point, greedily descends through the upper
	/// layers to find the closest element to the query vector `pt`.
	async fn search_ep(
		&self,
		ctx: &HnswContext<'_>,
		pt: &SharedVector,
		pending_doc: Option<&RoaringTreemap>,
	) -> Result<Option<(f64, ElementId)>> {
		if let Some(mut ep_id) = self.state.enter_point {
			if let Some(mut ep_dist) = self.elements.get_distance(&ctx.tx, pt, &ep_id).await? {
				for layer in self.layers.iter().rev() {
					if let Some(ep_dist_id) = layer
						.search_single(ctx, &self.elements, pt, ep_dist, ep_id, 1, pending_doc)
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

	/// Retrieves the vector associated with the given element ID.
	async fn get_vector(&self, tx: &Transaction, e_id: &ElementId) -> Result<Option<SharedVector>> {
		self.elements.get_vector(tx, e_id).await
	}
	#[cfg(test)]
	async fn check_hnsw_properties(&self, expected_count: usize) {
		check_hnsw_props(self, expected_count).await;
	}
}

#[cfg(test)]
async fn check_hnsw_props<L0, L>(h: &Hnsw<L0, L>, expected_count: usize)
where
	L0: DynamicSet,
	L: DynamicSet,
{
	assert_eq!(h.elements.len().await, expected_count);
	for layer in h.layers.iter() {
		layer.check_props(&h.elements).await;
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::Entry;
	use std::ops::Deref;
	use std::sync::Arc;

	use ahash::{HashMap, HashSet, HashSetExt};
	use anyhow::Result;
	use ndarray::Array1;
	use rand::rngs::SmallRng;
	use reblessive::tree::Stk;
	use test_log::test;

	use crate::catalog::providers::{CatalogProvider, TableProvider};
	use crate::catalog::{
		DatabaseId, Distance, HnswParams, IndexId, NamespaceId, TableDefinition, TableId,
		VectorType,
	};
	use crate::ctx::{Context, FrozenContext};
	use crate::dbs::Session;
	use crate::idx::IndexKeyBase;
	use crate::idx::seqdocids::DocId;
	use crate::idx::trees::hnsw::docs::VecDocs;
	use crate::idx::trees::hnsw::flavor::HnswFlavor;
	use crate::idx::trees::hnsw::index::{HnswContext, HnswIndex};
	use crate::idx::trees::hnsw::{
		ElementId, HnswRecordPendingUpdate, HnswSearch, HnswState, VectorId,
	};
	use crate::idx::trees::knn::tests::{
		RandomItemGenerator, TestCollection, get_seed_rnd, new_random_vec, new_vectors_from_file,
	};
	use crate::idx::trees::knn::{Ids64, KnnResult, KnnResultBuilder};
	use crate::idx::trees::vector::{SerializedVector, SharedVector, Vector};
	use crate::kvs::LockType::Optimistic;
	use crate::kvs::{Datastore, TransactionType};
	use crate::val::{RecordIdKey, Value};

	async fn insert_collection_hnsw(
		ctx: &HnswContext<'_>,
		h: &mut HnswFlavor,
		collection: &TestCollection,
	) -> HashMap<ElementId, SharedVector> {
		let mut map = HashMap::default();
		for (_, obj) in collection.to_vec_ref() {
			let obj: SharedVector = obj.clone();
			let e_id = h.insert(ctx, obj.clone_vector()).await.unwrap();
			map.insert(e_id, obj);
			h.check_hnsw_properties(map.len()).await;
		}
		map
	}

	async fn find_collection_hnsw(
		ctx: &HnswContext<'_>,
		h: &HnswFlavor,
		collection: &TestCollection,
	) {
		let max_knn = 20.min(collection.len());
		for (_, obj) in collection.to_vec_ref() {
			for knn in 1..max_knn {
				let search = HnswSearch::new(obj.clone(), knn, 80);
				let res = h.knn_search(ctx, &search, None).await.unwrap();
				if collection.is_unique() {
					let mut found = false;
					for (_, e_id) in &res {
						if let Some(v) = h.get_vector(&ctx.tx, e_id).await.unwrap()
							&& v.eq(obj)
						{
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

	async fn delete_collection_hnsw(
		ctx: &HnswContext<'_>,
		h: &mut HnswFlavor,
		mut map: HashMap<ElementId, SharedVector>,
	) {
		let element_ids: Vec<ElementId> = map.keys().copied().collect();
		for e_id in element_ids {
			assert!(h.remove(ctx, e_id).await.unwrap());
			map.remove(&e_id);
			h.check_hnsw_properties(map.len()).await;
		}
	}

	async fn test_hnsw_collection(p: &HnswParams, collection: &TestCollection) {
		let ds = Datastore::new("memory").await.unwrap();
		let ns = NamespaceId(1);
		let db = DatabaseId(2);
		let tb = TableId(3);
		let tb = TableDefinition::new(ns, db, tb, "tb".into());
		let ikb = IndexKeyBase::new(ns, db, "tb".into(), IndexId(4));
		let vec_docs =
			VecDocs::new(ikb.clone(), tb.table_id, ds.index_store().vector_cache().clone(), false);
		let mut h = HnswFlavor::new(
			tb.table_id,
			IndexKeyBase::new(NamespaceId(1), DatabaseId(2), tb.name.clone(), IndexId(4)),
			p,
			ds.index_store().vector_cache().clone(),
		)
		.unwrap();
		let map = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let ctx = HnswContext::new(&ctx, ikb.clone(), &vec_docs);
			let map = insert_collection_hnsw(&ctx, &mut h, collection).await;
			ctx.tx.commit().await.unwrap();
			map
		};
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let ctx = HnswContext::new(&ctx, ikb.clone(), &vec_docs);
			find_collection_hnsw(&ctx, &h, collection).await;
			ctx.tx.cancel().await.unwrap();
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let ctx = HnswContext::new(&ctx, ikb.clone(), &vec_docs);
			delete_collection_hnsw(&ctx, &mut h, map).await;
			ctx.tx.commit().await.unwrap();
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn new_params(
		dimension: usize,
		vector_type: VectorType,
		distance: Distance,
		m: usize,
		efc: usize,
		extend_candidates: bool,
		keep_pruned_connections: bool,
		use_hashed_vector: bool,
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
			use_hashed_vector,
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
				for (extend, keep, use_hashed_vector) in [
					(false, false, false),
					(true, false, true),
					(false, true, false),
					(true, true, true),
				] {
					let p =
						new_params(dim, vt, dist.clone(), 24, 500, extend, keep, use_hashed_vector);
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

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_hnsw_u8_euclidean() -> Result<()> {
		let p = new_params(5, VectorType::U8, Distance::Euclidean, 24, 500, false, false, false);
		test_hnsw(30, p).await;
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn test_hnsw_inner_product_smoke() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		{
			let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
			tx.ensure_ns_db(None, "test", "test").await?;
			tx.commit().await?;
		}
		let session = Session::owner().with_ns("test").with_db("test");
		let sql = "
			DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST INNER_PRODUCT TYPE F32 EFC 100 M 12;
			CREATE pts:1 SET point = [1f, 0f];
			CREATE pts:2 SET point = [2f, 0f];
			CREATE pts:3 SET point = [0f, 1f];
		";
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}

		let mut response =
			ds.execute("SELECT id FROM pts WHERE point <|2,40|> [1f, 0f];", &session, None).await?;
		let result = response.remove(0).result?;
		let surrealdb_types::Value::Array(result) = result else {
			panic!("Expected array result");
		};
		assert_eq!(result.len(), 2);
		Ok(())
	}

	async fn insert_collection_hnsw_index(
		ctx: &FrozenContext,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) -> Result<HashMap<SharedVector, HashSet<DocId>>> {
		let mut map: HashMap<SharedVector, HashSet<DocId>> = HashMap::default();
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.index(ctx, &RecordIdKey::Number(*doc_id as i64), None, Some(content)).await?;
			match map.entry(obj.clone()) {
				Entry::Occupied(mut e) => {
					e.get_mut().insert(*doc_id);
				}
				Entry::Vacant(e) => {
					e.insert(HashSet::from_iter([*doc_id]));
				}
			}
			h.index_pendings(ctx).await?;
			h.check_hnsw_properties(map.len()).await;
		}
		Ok(map)
	}

	async fn find_collection_hnsw_index(
		ctx: &FrozenContext,
		stk: &mut Stk,
		h: &mut HnswIndex,
		collection: &TestCollection,
	) {
		let ctx = h.new_hnsw_context(ctx);
		let max_knn = 20.min(collection.len());
		for (doc_id, obj) in collection.to_vec_ref() {
			let doc_id = VectorId::DocId(*doc_id);
			for knn in 1..max_knn {
				let search = HnswSearch::new(obj.clone(), knn, 500);
				let mut builder = KnnResultBuilder::new(search.k);
				h.search_graph(&ctx, stk, &search, None, &mut None, &mut builder).await.unwrap();
				let res = builder.collect();
				let first_dist: f64 = res.first().unwrap().0.into();
				if knn == 1 && res.len() == 1 && first_dist > 0.0 {
					let docs: Vec<VectorId> = res.iter().map(|(_, id)| id.clone()).collect();
					if collection.is_unique() {
						assert!(
							docs.contains(&doc_id),
							"Search: {:?} - Knn: {} - Wrong Doc - Expected: {:?} - Got: {:?}",
							obj,
							knn,
							doc_id,
							res
						);
					}
				}
				let expected_len = collection.len().min(knn);
				assert_eq!(
					expected_len,
					res.len(),
					"Wrong knn count - Expected: {} - Got: {} - - Docs: {:?} - Collection: {}",
					expected_len,
					res.len(),
					res,
					collection.len(),
				)
			}
		}
	}

	async fn delete_hnsw_index_collection(
		ctx: &FrozenContext,
		h: &mut HnswIndex,
		collection: &TestCollection,
		mut map: HashMap<SharedVector, HashSet<DocId>>,
	) -> Result<()> {
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			let id = RecordIdKey::Number(*doc_id as i64);
			h.index(ctx, &id, Some(content), None).await?;
			if let Entry::Occupied(mut e) = map.entry(obj.clone()) {
				let set = e.get_mut();
				set.remove(doc_id);
				if set.is_empty() {
					e.remove();
				}
			}
			h.index_pendings(ctx).await?;
			// Check properties
			h.check_hnsw_properties(map.len()).await;
		}
		Ok(())
	}

	async fn new_ctx(ds: &Datastore, tt: TransactionType) -> FrozenContext {
		let tx = Arc::new(ds.transaction(tt, Optimistic).await.unwrap());
		let mut ctx = Context::new_test();
		ctx.set_transaction(tx);
		ctx.freeze()
	}

	fn vector_content(vector: &SharedVector) -> Vec<Value> {
		vec![Value::from(vector.deref())]
	}

	fn serialized(vector: &SharedVector) -> SerializedVector {
		SerializedVector::from(vector.deref())
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
			let ns = NamespaceId(1);
			let db = DatabaseId(2);
			let tb = TableId(3);
			let ix = IndexId(4);
			let tx = ctx.tx();
			let mut h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				IndexKeyBase::new(ns, db, "tb".into(), ix),
				tb,
				&p,
			)
			.await
			.unwrap();
			// Fill index
			let map = insert_collection_hnsw_index(&ctx, &mut h, &collection).await.unwrap();
			tx.commit().await.unwrap();
			(h, map)
		};

		// Search index
		{
			let mut stack = reblessive::tree::TreeStack::new();
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();

			stack
				.enter(|stk| async {
					find_collection_hnsw_index(&ctx, stk, &mut h, &collection).await;
				})
				.finish()
				.await;
			tx.cancel().await.unwrap();
		}

		// Delete collection
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			delete_hnsw_index_collection(&ctx, &mut h, &collection, map).await.unwrap();
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
			(Distance::Pearson, 5),
		] {
			for vt in [
				VectorType::F64,
				VectorType::F32,
				VectorType::I64,
				VectorType::I32,
				VectorType::I16,
			] {
				for (extend, keep, use_hashed_vector) in [
					(false, false, true),
					(true, false, false),
					(false, true, true),
					(true, true, false),
				] {
					for unique in [true, false] {
						let p = new_params(
							dim,
							vt,
							dist.clone(),
							8,
							150,
							extend,
							keep,
							use_hashed_vector,
						);
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
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let ds = Arc::new(Datastore::new("memory").await.unwrap());
		let vec_docs =
			VecDocs::new(ikb.clone(), TableId(3), ds.index_store().vector_cache().clone(), false);
		let mut h =
			HnswFlavor::new(TableId(3), ikb.clone(), &p, ds.index_store().vector_cache().clone())
				.unwrap();
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let ctx = HnswContext::new(&ctx, ikb.clone(), &vec_docs);
			insert_collection_hnsw(&ctx, &mut h, &collection).await;
			ctx.tx.commit().await.unwrap();
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let ctx = HnswContext::new(&ctx, ikb.clone(), &vec_docs);
			let search = HnswSearch::new(new_i16_vec(-2, -3), 10, 501);
			let res = h.knn_search(&ctx, &search, None).await.unwrap();
			ctx.tx.cancel().await.unwrap();
			assert_eq!(res.len(), 10);
		}
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_pending_coalesces_new_record_updates() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let h = HnswIndex::new(
			ctx.get_index_stores().vector_cache().clone(),
			&tx,
			ikb.clone(),
			TableId(3),
			&p,
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);
		let second = new_i16_vec(2, 2);

		h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
		h.index(&ctx, &id, Some(vector_content(&first)), Some(vector_content(&second))).await?;

		let pending: HnswRecordPendingUpdate = tx.get(&ikb.new_hr_key(&id), None).await?.unwrap();
		assert_eq!(pending.doc_id, None);
		assert!(pending.old_vectors.is_empty());
		assert_eq!(pending.new_vectors, vec![serialized(&second)]);
		tx.cancel().await?;
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_pending_preserves_existing_doc_baseline() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let h = HnswIndex::new(
			ctx.get_index_stores().vector_cache().clone(),
			&tx,
			ikb.clone(),
			TableId(3),
			&p,
		)
		.await?;
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);
		let second = new_i16_vec(2, 2);

		h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
		assert_eq!(h.index_pendings(&ctx).await?, 1);
		h.index(&ctx, &id, Some(vector_content(&first)), Some(vector_content(&second))).await?;
		h.index(&ctx, &id, Some(vector_content(&second)), None).await?;

		let pending: HnswRecordPendingUpdate = tx.get(&ikb.new_hr_key(&id), None).await?.unwrap();
		assert_eq!(pending.doc_id, Some(0));
		assert_eq!(pending.old_vectors, vec![serialized(&first)]);
		assert!(pending.new_vectors.is_empty());
		tx.cancel().await?;
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_compaction_deletes_pending_key_after_final_batch() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);
		let h = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				ikb.clone(),
				TableId(3),
				&p,
			)
			.await?;
			h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
			tx.commit().await?;
			h
		};

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());
		assert!(!plan.has_more());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(h.apply_compaction(&ctx, plan).await?);
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get::<_>(&ikb.new_hr_key(&id), None).await?.is_none());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_empty_compaction_plan_preserves_concurrent_pending_write() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let h = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				ikb.clone(),
				TableId(3),
				&p,
			)
			.await?;
			tx.commit().await?;
			h
		};
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(!plan.has_work());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
			ctx.tx().commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(!h.apply_compaction(&ctx, plan).await?);
			ctx.tx().cancel().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get::<_>(&ikb.new_hr_key(&id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_compaction_preserves_changed_pending_value() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);
		let second = new_i16_vec(2, 2);

		let h = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				ikb.clone(),
				TableId(3),
				&p,
			)
			.await?;
			h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
			tx.commit().await?;
			h
		};

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			h.index(&ctx, &id, Some(vector_content(&first)), Some(vector_content(&second))).await?;
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(!h.apply_compaction(&ctx, plan).await?);
			ctx.tx().cancel().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let pending: HnswRecordPendingUpdate =
				ctx.tx().get(&ikb.new_hr_key(&id), None).await?.unwrap();
			assert_eq!(pending.new_vectors, vec![serialized(&second)]);
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_compaction_preserves_post_snapshot_pending_key() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let first_id = RecordIdKey::Number(1);
		let second_id = RecordIdKey::Number(2);
		let first = new_i16_vec(1, 1);
		let second = new_i16_vec(2, 2);

		let h = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				ikb.clone(),
				TableId(3),
				&p,
			)
			.await?;
			h.index(&ctx, &first_id, None, Some(vector_content(&first))).await?;
			tx.commit().await?;
			h
		};

		let plan = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		assert!(plan.has_work());

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			h.index(&ctx, &second_id, None, Some(vector_content(&second))).await?;
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(h.apply_compaction(&ctx, plan).await?);
			ctx.tx().commit().await?;
		}

		{
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			assert!(ctx.tx().get::<_>(&ikb.new_hr_key(&first_id), None).await?.is_none());
			assert!(ctx.tx().get::<_>(&ikb.new_hr_key(&second_id), None).await?.is_some());
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_compaction_generation_allows_one_winner() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let p = new_params(2, VectorType::I16, Distance::Euclidean, 3, 500, true, true, true);
		let id = RecordIdKey::Number(1);
		let first = new_i16_vec(1, 1);

		let h = {
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			let tx = ctx.tx();
			let h = HnswIndex::new(
				ctx.get_index_stores().vector_cache().clone(),
				&tx,
				ikb.clone(),
				TableId(3),
				&p,
			)
			.await?;
			h.index(&ctx, &id, None, Some(vector_content(&first))).await?;
			tx.commit().await?;
			h
		};

		let plan_1 = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};
		let plan_2 = {
			let ctx = new_ctx(&ds, TransactionType::Read).await;
			let plan = HnswIndex::prepare_compaction(&ctx, &ikb).await?;
			ctx.tx().cancel().await?;
			plan
		};

		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(h.apply_compaction(&ctx, plan_1).await?);
			ctx.tx().commit().await?;
		}
		{
			let ctx = new_ctx(&ds, TransactionType::Write).await;
			assert!(!h.apply_compaction(&ctx, plan_2).await?);
			ctx.tx().cancel().await?;
		}
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_blocking_define_index_compacts_pending_vectors() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let db = {
			let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
			let db = tx.ensure_ns_db(None, "test", "test").await?;
			tx.commit().await?;
			db
		};
		let session = Session::owner().with_ns("test").with_db("test");
		let sql = "
			CREATE pts:1 SET point = [1f, 2f];
			CREATE pts:2 SET point = [2f, 3f];
			CREATE pts:3 SET point = [3f, 4f];
			DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;
		";
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}

		let tx = ds.transaction(TransactionType::Read, Optimistic).await?;
		let tb = "pts".into();
		let ix =
			tx.get_tb_index(db.namespace_id, db.database_id, &tb, "hnsw_pts", None).await?.unwrap();
		let ikb = IndexKeyBase::new(db.namespace_id, db.database_id, tb, ix.index_id);
		let pending_records = tx.getr(ikb.new_hr_range()?, None).await?;
		let pending_appends = tx.getr(ikb.new_hp_range()?, None).await?;
		let state: HnswState = tx.get(&ikb.new_hs_key(), None).await?.unwrap();
		tx.cancel().await?;

		assert!(pending_records.is_empty());
		assert!(pending_appends.is_empty());
		assert_eq!(state.next_element_id, 3);
		Ok(())
	}

	#[test(tokio::test(flavor = "multi_thread"))]
	async fn hnsw_query_reads_record_keyed_pending_vectors() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		{
			let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
			tx.ensure_ns_db(None, "test", "test").await?;
			tx.commit().await?;
		}
		let session = Session::owner().with_ns("test").with_db("test");
		let sql = "
			DEFINE INDEX hnsw_pts ON pts FIELDS point HNSW DIMENSION 2 DIST EUCLIDEAN TYPE F32 EFC 100 M 12;
			CREATE pts:1 SET point = [1f, 2f];
			CREATE pts:2 SET point = [2f, 3f];
			CREATE pts:3 SET point = [3f, 4f];
		";
		for response in ds.execute(sql, &session, None).await? {
			response.result?;
		}

		let mut response =
			ds.execute("SELECT id FROM pts WHERE point <|2,40|> [1f, 2f];", &session, None).await?;
		let result = response.remove(0).result?;
		let surrealdb_types::Value::Array(result) = result else {
			panic!("Expected array result");
		};
		assert_eq!(result.len(), 2);
		Ok(())
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
		let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
		let db = tx.ensure_ns_db(None, "myns", "mydb").await?;
		tx.commit().await?;

		let collection: Arc<TestCollection> =
			Arc::new(TestCollection::NonUnique(new_vectors_from_file(
				p.vector_type,
				&format!("../../tests/data/{embeddings_file}"),
				Some(ingest_limit),
			)?));

		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let tb = TableId(3);
		let ix = IndexId(4);
		let h = HnswIndex::new(
			ctx.get_index_stores().vector_cache().clone(),
			&tx,
			IndexKeyBase::new(db.namespace_id, db.database_id, "tb".into(), ix),
			tb,
			&p,
		)
		.await?;
		info!("Insert collection");
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.index(&ctx, &RecordIdKey::Number(*doc_id as i64), None, Some(content)).await?;
		}

		info!("Index pendings");
		assert_eq!(h.index_pendings(&ctx).await?, collection.len());
		assert_eq!(h.index_pendings(&ctx).await?, 0);

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
			let queries = Arc::clone(&queries);
			let collection = Arc::clone(&collection);
			let h = Arc::clone(&h);
			let ds = Arc::clone(&ds);
			let f = tokio::spawn(async move {
				let mut stack = reblessive::tree::TreeStack::new();
				stack
					.enter(|stk| async {
						let mut total_recall = 0.0;
						for (_, pt) in queries.to_vec_ref() {
							let knn = 10;
							let search = HnswSearch::new(pt.clone(), knn, efs);

							let ctx = new_ctx(&ds, TransactionType::Read).await;
							let ctx = h.new_hnsw_context(&ctx);
							let mut builder = KnnResultBuilder::new(knn);
							h.search_graph(&ctx, stk, &search, None, &mut None, &mut builder)
								.await
								.unwrap();
							ctx.tx.cancel().await.unwrap();
							let res = builder.collect();
							assert_eq!(res.len(), knn, "Different size - knn: {knn}",);
							let brute_force_res = collection.knn(pt, &Distance::Euclidean, knn);
							let rec = compute_recall(&brute_force_res, &res);
							if rec == 1.0 {
								assert_eq!(brute_force_res, res);
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
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, false, false);
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
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, false, true, false);
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
		let p = new_params(20, VectorType::F32, Distance::Euclidean, 8, 100, true, true, true);
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

	/// Diagnostic: measure HNSW Recall@10 against an exact (brute-force) ground
	/// truth, on a collection generated in-process at an arbitrary dimension and
	/// distance. Unlike `test_recall`, (a) the data is synthesized rather than
	/// loaded from a fixture, so we can probe high dimensions / cosine, and (b)
	/// the brute-force ground truth uses the SAME distance as the index (the
	/// fixture-based `test_recall` hardcodes Euclidean). Recall is averaged over
	/// the query set and asserted against a per-`ef` threshold (the graph RNG is
	/// entropy-seeded, so recall is not bit-reproducible — thresholds, like the
	/// other recall tests, not equality). Set `TEST_SEED` for reproducible data.
	async fn diag_recall_generated(
		collection_size: usize,
		query_count: usize,
		dimension: usize,
		p: HnswParams,
		tests_ef_recall: &[(usize, f64)],
	) -> Result<()> {
		let dist = p.distance.clone();
		let vt = p.vector_type;
		info!(
			"=== diag recall: dim={dimension} dist={dist:?} M={} M0={} EFC={} keep_pruned={} n={collection_size} q={query_count} ===",
			p.m, p.m0, p.ef_construction, p.keep_pruned_connections
		);

		let ds = Arc::new(Datastore::new("memory").await?);
		let tx = ds.transaction(TransactionType::Write, Optimistic).await?;
		let db = tx.ensure_ns_db(None, "myns", "mydb").await?;
		tx.commit().await?;

		// Draw the indexed set and the query set from ONE continuous seeded RNG
		// stream so the queries are disjoint from the indexed vectors. Calling
		// `TestCollection::new` twice re-seeds from TEST_SEED each time, which
		// makes the first `query_count` queries exact copies of the first indexed
		// vectors — every query then has itself (distance 0) as its true nearest
		// neighbour, turning Recall@10 into a self-query over-estimate. Sharing
		// one RNG keeps the run reproducible while the query draws continue past
		// the indexed draws, so the two sets do not overlap.
		let mut rng = get_seed_rnd();
		let item_gen = RandomItemGenerator::new(&dist, dimension);
		let gen_set = |rng: &mut SmallRng, n: usize| {
			let v: Vec<(DocId, SharedVector)> = (0..n)
				.map(|i| (i as DocId, new_random_vec(rng, vt, dimension, &item_gen)))
				.collect();
			TestCollection::NonUnique(v)
		};
		let collection = gen_set(&mut rng, collection_size);

		let ctx = new_ctx(&ds, TransactionType::Write).await;
		let tx = ctx.tx();
		let h = HnswIndex::new(
			ctx.get_index_stores().vector_cache().clone(),
			&tx,
			IndexKeyBase::new(db.namespace_id, db.database_id, "tb".into(), IndexId(4)),
			TableId(3),
			&p,
		)
		.await?;
		for (doc_id, obj) in collection.to_vec_ref() {
			let content = vec![Value::from(obj.deref())];
			h.index(&ctx, &RecordIdKey::Number(*doc_id as i64), None, Some(content)).await?;
		}
		// `index_pendings` applies queued inserts to the graph in batches
		// (capped per call), so drain it until no pendings remain.
		let mut indexed = 0;
		loop {
			let n = h.index_pendings(&ctx).await?;
			indexed += n;
			if n == 0 {
				break;
			}
		}
		assert_eq!(indexed, collection.len());
		tx.commit().await?;

		let queries = gen_set(&mut rng, query_count);
		let knn = 10;

		for &(efs, expected_recall) in tests_ef_recall {
			let mut stack = reblessive::tree::TreeStack::new();
			stack
				.enter(|stk| async {
					let mut total_recall = 0.0;
					for (_, pt) in queries.to_vec_ref() {
						let search = HnswSearch::new(pt.clone(), knn, efs);
						let qctx = new_ctx(&ds, TransactionType::Read).await;
						let qctx = h.new_hnsw_context(&qctx);
						let mut builder = KnnResultBuilder::new(knn);
						h.search_graph(&qctx, stk, &search, None, &mut None, &mut builder)
							.await
							.unwrap();
						qctx.tx.cancel().await.unwrap();
						let res = builder.collect();
						let brute_force_res = collection.knn(pt, &dist, knn);
						total_recall += compute_recall(&brute_force_res, &res);
					}
					let recall = total_recall / queries.to_vec_ref().len() as f64;
					info!(
						"dim={dimension} keep_pruned={} EF={efs} -> Recall@{knn} = {recall:.4}",
						p.keep_pruned_connections
					);
					assert!(
						recall >= expected_recall,
						"dim={dimension} EF={efs} Recall={recall:.4} < expected {expected_recall}"
					);
				})
				.finish()
				.await;
		}
		Ok(())
	}

	/// Diagnostic for the "low 768d recall" report: at the `DEFINE INDEX HNSW`
	/// defaults (M=12 => M0=24, EFC=150), show that Recall@10 on cosine data is
	/// lower at 768 dimensions than at 128 for the same parameters, and that it
	/// climbs back toward 1.0 purely by raising the search `ef`. This confirms
	/// the behaviour is a tuning/curse-of-dimensionality curve, not a recall cap.
	/// NOTE: synthetic uniform vectors are a stand-in; absolute numbers will
	/// differ from real embeddings, but the directional levers (ef, dimension)
	/// are what this asserts. Ignored by default (slow at 768d): run with
	/// `cargo test -p surrealdb-core --release diag_recall -- --ignored --nocapture`.
	#[test(tokio::test(flavor = "multi_thread"))]
	#[ignore = "diagnostic (slow, high-dim): cosine Recall@10 vs dimension"]
	async fn diag_recall_cosine_dim_curve() -> Result<()> {
		// Held-out queries (TEST_SEED=42), Recall@10:
		//   128-d: EF 10/40/100/200 -> ~0.50 / 0.88 / 0.99 / 1.00
		//   768-d: EF 40/100/200/400 -> ~0.68 / 0.91 / 0.99 / 1.00
		// 768-d needs a markedly higher EF than 128-d for equal recall, and both
		// converge to 1.0 — recall is EF-bound, not capped. (768-d @ EF=100 ~0.91
		// lands in the reported "0.915" regime.) Thresholds are loose floors at
		// low EF (low + noisier) and firm at high EF (the convergence guard),
		// shared across both dims so each must hold for the worse 768-d curve.
		let efs = &[(10, 0.20), (40, 0.55), (100, 0.82), (200, 0.95), (400, 0.98)];
		for dim in [128usize, 768] {
			let p =
				new_params(dim, VectorType::F32, Distance::Cosine, 12, 150, false, false, false);
			diag_recall_generated(1500, 100, dim, p, efs).await?;
		}
		Ok(())
	}

	/// Diagnostic: at 768d cosine with default build params, compare the default
	/// neighbour-selection heuristic against `KEEP_PRUNED_CONNECTIONS` across an
	/// `ef` sweep. Hypothesis was that keeping pruned connections (refilling
	/// neighbour lists up to M) would lift recall in high dimensions where the
	/// diversity heuristic prunes more. On this small uniform-random collection
	/// the effect is negligible (recall is identical to the default within
	/// noise) — the lever is expected to matter more at scale and on clustered
	/// data, so this stands as a comparison/regression guard, not proof of a
	/// win. Run with `--ignored --nocapture`.
	#[test(tokio::test(flavor = "multi_thread"))]
	#[ignore = "diagnostic (slow, high-dim): KEEP_PRUNED_CONNECTIONS effect at 768d cosine"]
	async fn diag_recall_cosine_768_keep_pruned() -> Result<()> {
		// Held-out queries (TEST_SEED=42): default vs KEEP_PRUNED at 768-d track
		// each other within noise (e.g. EF=100 ~0.91 either way), so the effect is
		// negligible on this data. Thresholds match the dim-curve 768-d floors.
		let efs = &[(40, 0.55), (100, 0.82), (200, 0.95), (400, 0.98)];
		info!("--- default heuristic (keep_pruned=false) ---");
		diag_recall_generated(
			1500,
			100,
			768,
			new_params(768, VectorType::F32, Distance::Cosine, 12, 150, false, false, false),
			efs,
		)
		.await?;
		info!("--- KEEP_PRUNED_CONNECTIONS (keep_pruned=true) ---");
		diag_recall_generated(
			1500,
			100,
			768,
			new_params(768, VectorType::F32, Distance::Cosine, 12, 150, false, true, false),
			efs,
		)
		.await?;
		Ok(())
	}

	impl TestCollection {
		fn knn(&self, pt: &SharedVector, dist: &Distance, n: usize) -> KnnResult {
			let mut b = KnnResultBuilder::new(n);
			for (doc_id, doc_pt) in self.to_vec_ref() {
				let d = dist.calculate(doc_pt, pt);
				if b.check_add(d) {
					b.add_graph_result(d, &Ids64::One(*doc_id));
				}
			}
			b.collect()
		}
	}

	fn compute_recall(res1: &KnnResult, res2: &KnnResult) -> f64 {
		let mut docs = HashSet::with_capacity(res1.len());
		for (_, doc_id) in res1.iter() {
			docs.insert(doc_id.clone());
		}
		let mut found = 0;
		for (_, doc_id) in res2.iter() {
			if docs.contains(doc_id) {
				found += 1;
			}
		}
		found as f64 / docs.len() as f64
	}

	fn new_i16_vec(x: isize, y: isize) -> SharedVector {
		let vec = Vector::I16(Array1::from_vec(vec![x as i16, y as i16]));
		vec.into()
	}
}
