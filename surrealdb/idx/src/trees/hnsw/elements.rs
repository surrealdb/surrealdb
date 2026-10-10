use std::sync::Arc;

use ahash::HashMap;
use anyhow::{Result, ensure};
use surrealdb_datastore::Transaction;

use crate::IndexKeyBase;
use crate::catalog::{Distance, IndexId, TableId};
use crate::trees::hnsw::ElementId;
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::vector::{DistanceExt as _, SerializedVector, SharedVector, Vector};

/// The vectors of the elements a staged batch is adding: a contiguous run of
/// element ids, none of them yet in the store's committed state or in any
/// process-wide cache.
pub(super) struct ElementOverlay {
	/// The first element id the batch allocated.
	first: ElementId,
	/// The batch's vectors, in element-id order from `first`.
	vectors: Vec<SharedVector>,
}

impl ElementOverlay {
	pub(super) fn new(first: ElementId, vectors: Vec<SharedVector>) -> Self {
		Self {
			first,
			vectors,
		}
	}

	/// The vector of a batch element, or `None` for any other element.
	pub(super) fn get(&self, id: ElementId) -> Option<&SharedVector> {
		let offset = id.checked_sub(self.first)?;
		self.vectors.get(usize::try_from(offset).ok()?)
	}
}

/// Manages storage and retrieval of element vectors in the HNSW graph.
///
/// Vectors are stored in the key-value store and cached in-memory via
/// [`VectorCache`] for fast distance computations during graph traversal.
///
/// A view made by [`Self::view`] with an overlay also answers for the elements a
/// staged batch is adding, straight from the batch, ahead of the cache and the
/// store. That keeps those vectors out of the process-wide cache until the
/// batch has committed.
pub(super) struct HnswElements {
	/// The table this index belongs to.
	table_id: TableId,
	/// The index identifier.
	index_id: IndexId,
	/// Key base for generating element storage keys.
	ikb: IndexKeyBase,
	/// In-memory LRU cache for element vectors.
	vector_cache: VectorCache,
	/// Stored index format used to scope cache entries.
	format_version: u16,
	/// The next element ID to assign.
	next_element_id: ElementId,
	/// Distance metric for similarity computations.
	dist: Distance,
	/// The vectors of a staged batch, consulted before the cache and the store.
	overlay: Option<Arc<ElementOverlay>>,
}

impl HnswElements {
	/// Creates a new `HnswElements` instance.
	pub(super) fn new(
		table_id: TableId,
		ikb: IndexKeyBase,
		dist: Distance,
		vector_cache: VectorCache,
		format_version: u16,
	) -> Self {
		Self {
			table_id,
			index_id: ikb.index(),
			ikb,
			vector_cache,
			format_version,
			next_element_id: 0,
			dist,
			overlay: None,
		}
	}

	/// A view of the same elements, for reads outside the graph. With an
	/// `overlay` it also answers for a staged batch's own elements, ahead of the
	/// cache and the store.
	pub(super) fn view(&self, overlay: Option<Arc<ElementOverlay>>) -> Self {
		Self {
			table_id: self.table_id,
			index_id: self.index_id,
			ikb: self.ikb.clone(),
			vector_cache: self.vector_cache.clone(),
			format_version: self.format_version,
			next_element_id: self.next_element_id,
			dist: self.dist.clone(),
			overlay,
		}
	}

	/// Enters committed element vectors into the process-wide cache.
	pub(super) async fn cache_committed(
		&self,
		vectors: impl Iterator<Item = (ElementId, SharedVector)>,
	) {
		for (id, vector) in vectors {
			self.vector_cache
				.insert(
					self.ikb.ns(),
					self.ikb.db(),
					self.table_id,
					self.index_id,
					self.format_version,
					id,
					vector,
				)
				.await;
		}
	}

	/// Drops every cache entry this index holds at its format version: vectors,
	/// document sets and document ids.
	pub(super) async fn evict_index(&self) {
		self.vector_cache
			.remove_index(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.index_id,
				self.format_version,
			)
			.await;
	}

	/// Drops the given elements' vectors from the process-wide cache.
	pub(super) async fn evict(&self, ids: impl Iterator<Item = ElementId>) {
		for id in ids {
			self.vector_cache
				.remove(
					self.ikb.ns(),
					self.ikb.db(),
					self.table_id,
					self.index_id,
					self.format_version,
					id,
				)
				.await;
		}
	}

	/// Sets the next element ID (used when loading state from the key-value store).
	pub(super) fn set_next_element_id(&mut self, next: ElementId) {
		self.next_element_id = next;
	}

	/// Returns the current next element ID without incrementing.
	pub(super) fn next_element_id(&self) -> ElementId {
		self.next_element_id
	}

	/// Increments and returns the next element ID.
	pub(super) fn inc_next_element_id(&mut self) -> ElementId {
		self.next_element_id += 1;
		self.next_element_id
	}

	#[cfg(test)]
	pub(super) async fn len(&self) -> usize {
		self.vector_cache
			.len(self.ikb.ns(), self.ikb.db(), self.table_id, self.index_id, self.format_version)
			.await as usize
	}

	#[cfg(test)]
	pub(super) async fn contains(&self, e_id: ElementId) -> bool {
		self.vector_cache
			.contains(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.index_id,
				self.format_version,
				e_id,
			)
			.await
	}

	/// Stores a vector in the key-value store and caches it. Returns the shared vector.
	pub(super) async fn insert(
		&mut self,
		tx: &Transaction,
		id: ElementId,
		vec: Vector,
		ser_vec: &SerializedVector,
	) -> Result<SharedVector> {
		let key = self.ikb.new_he_key(id);
		tx.set_key(&key, ser_vec).await?;
		let pt: SharedVector = vec.into();
		self.vector_cache
			.insert(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.index_id,
				self.format_version,
				id,
				pt.clone(),
			)
			.await;
		Ok(pt)
	}

	/// Retrieves a vector by element ID, checking a staged batch's own elements
	/// first, then the cache, then the key-value store.
	pub(super) async fn get_vector(
		&self,
		tx: &Transaction,
		e_id: &ElementId,
	) -> Result<Option<SharedVector>> {
		if let Some(overlay) = &self.overlay
			&& let Some(v) = overlay.get(*e_id)
		{
			return Ok(Some(v.clone()));
		}
		if let Some(v) = self
			.vector_cache
			.get(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.index_id,
				self.format_version,
				*e_id,
			)
			.await
		{
			return Ok(Some(v));
		}
		let key = self.ikb.new_he_key(*e_id);
		match tx.get_key(&key, None).await? {
			None => Ok(None),
			Some(vec) => {
				let vec = Vector::from(vec);
				let vec: SharedVector = vec.into();
				self.vector_cache
					.insert(
						self.ikb.ns(),
						self.ikb.db(),
						self.table_id,
						self.index_id,
						self.format_version,
						*e_id,
						vec.clone(),
					)
					.await;
				Ok(Some(vec))
			}
		}
	}

	/// [`Self::get_vector`] for several element IDs, in input order, reading
	/// every distinct cache miss from the store once, in one multi-get.
	pub(super) async fn get_vectors(
		&self,
		tx: &Transaction,
		e_ids: &[ElementId],
	) -> Result<Vec<Option<SharedVector>>> {
		let mut vectors = Vec::with_capacity(e_ids.len());
		// Each distinct missing element, at the first position asking for it.
		let mut misses: Vec<(ElementId, usize)> = Vec::new();
		// Each later position asking for an element already known missing,
		// with that element's index in `misses`: it skips the cache lookup.
		// Repeated cache hits are deliberately not tracked: the graph search,
		// the hottest caller, passes ids its visited set has already made
		// distinct, so recording every element to spare a repeated hit its one
		// cache lookup would add a map insert per element there and save
		// nothing. Only a repeated miss, which would cost a store read, is
		// worth remembering.
		let mut repeats: Vec<(usize, usize)> = Vec::new();
		let mut miss_slot: HashMap<ElementId, usize> = HashMap::default();
		for (pos, e_id) in e_ids.iter().enumerate() {
			// With no miss yet, as on a warm graph search, nothing is hashed.
			if !miss_slot.is_empty()
				&& let Some(&slot) = miss_slot.get(e_id)
			{
				repeats.push((slot, pos));
				vectors.push(None);
				continue;
			}
			let hit = match self.overlay.as_ref().and_then(|o| o.get(*e_id)) {
				Some(v) => Some(v.clone()),
				None => {
					self.vector_cache
						.get(
							self.ikb.ns(),
							self.ikb.db(),
							self.table_id,
							self.index_id,
							self.format_version,
							*e_id,
						)
						.await
				}
			};
			if hit.is_none() {
				miss_slot.insert(*e_id, misses.len());
				misses.push((*e_id, pos));
			}
			vectors.push(hit);
		}
		if misses.is_empty() {
			return Ok(vectors);
		}
		let keys = misses.iter().map(|(e_id, _)| self.ikb.new_he_key(*e_id)).collect();
		let fetched: Vec<Option<SerializedVector>> = tx.get_many_key(keys, None).await?;
		// A short answer would leave the trailing misses unread, silently.
		ensure!(
			fetched.len() == misses.len(),
			"{} vectors read for {} elements",
			fetched.len(),
			misses.len()
		);
		for ((e_id, pos), vec) in misses.iter().copied().zip(fetched) {
			if let Some(vec) = vec {
				let vec: SharedVector = Vector::from(vec).into();
				self.vector_cache
					.insert(
						self.ikb.ns(),
						self.ikb.db(),
						self.table_id,
						self.index_id,
						self.format_version,
						e_id,
						vec.clone(),
					)
					.await;
				vectors[pos] = Some(vec);
			}
		}
		for (slot, pos) in repeats {
			let fetched = vectors[misses[slot].1].clone();
			vectors[pos] = fetched;
		}
		Ok(vectors)
	}

	/// Computes the distance between two vectors using the configured distance metric.
	pub(super) fn distance(&self, a: &SharedVector, b: &SharedVector) -> f64 {
		self.dist.calculate_shared(a, b)
	}

	/// Computes the distance between a query vector and an element's stored vector.
	pub(super) async fn get_distance(
		&self,
		tx: &Transaction,
		q: &SharedVector,
		e_id: &ElementId,
	) -> Result<Option<f64>> {
		Ok(self.get_vector(tx, e_id).await?.map(|r| self.dist.calculate_shared(&r, q)))
	}

	/// Removes an element's vector from both the cache and the key-value store.
	pub(super) async fn remove(&mut self, tx: &Transaction, e_id: ElementId) -> Result<()> {
		self.vector_cache
			.remove(
				self.ikb.ns(),
				self.ikb.db(),
				self.table_id,
				self.index_id,
				self.format_version,
				e_id,
			)
			.await;
		let key = self.ikb.new_he_key(e_id);
		tx.del_key(&key).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use anyhow::Result;
	use ndarray::Array1;
	use surrealdb_kvs::TransactionType;

	use super::{ElementOverlay, HnswElements};
	use crate::IndexKeyBase;
	use crate::catalog::{DatabaseId, Distance, IndexId, NamespaceId, TableId};
	use crate::test_env::TestIndexStore;
	use crate::trees::vector::{SerializedVector, SharedVector, Vector};

	fn vector(c: f32) -> Vector {
		Vector::F32(Array1::from_vec(vec![c, c + 1.0]))
	}

	/// `get_vectors` answers each position as `get_vector` would, in input
	/// order: a cached element, an element only in the store (read once,
	/// however often it is asked for, and cached after), an element the index
	/// does not hold, and an element a staged batch's overlay supplies.
	#[tokio::test(flavor = "multi_thread")]
	async fn get_vectors_answers_every_position_in_order() -> Result<()> {
		let ds = TestIndexStore::new().await;
		let ikb = IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(4));
		let mut elements = HnswElements::new(
			TableId(3),
			ikb.clone(),
			Distance::Euclidean,
			ds.index_stores().vector_cache().clone(),
			crate::catalog::VECTOR_DOC_ELEMENTS_FORMAT_VERSION,
		);
		let (cached, stored) = (vector(1.0), vector(2.0));
		{
			let ctx = ds.env(TransactionType::Write).await;
			let tx = ctx.tx();
			// Element 1 through the cache; element 2 only into the store.
			elements.insert(&tx, 1, cached.clone(), &SerializedVector::from(&cached)).await?;
			tx.set_key(&ikb.new_he_key(2), &SerializedVector::from(&stored)).await?;
			tx.commit().await?;
		}
		let expect = |v: &Vector| Some(SharedVector::from(v.clone()));
		let ids = [2, 1, 2, 9, 1, 2];
		let want = [
			expect(&stored),
			expect(&cached),
			expect(&stored),
			None,
			expect(&cached),
			expect(&stored),
		];
		for round in ["store read", "cached"] {
			let ctx = ds.env(TransactionType::Read).await;
			let got = elements.get_vectors(&ctx.tx(), &ids).await?;
			assert_eq!(got, want, "{round}");
			for (id, v) in ids.iter().zip(&got) {
				assert_eq!(&elements.get_vector(&ctx.tx(), id).await?, v, "{round}: element {id}");
			}
			ctx.tx().cancel().await?;
		}
		// A staged batch's elements answer from its overlay, ahead of the store.
		let batch = vector(10.0);
		let view =
			elements.view(Some(Arc::new(ElementOverlay::new(10, vec![batch.clone().into()]))));
		let ctx = ds.env(TransactionType::Read).await;
		let got = view.get_vectors(&ctx.tx(), &[10, 2, 10]).await?;
		assert_eq!(got, [expect(&batch), expect(&stored), expect(&batch)]);
		ctx.tx().cancel().await?;
		Ok(())
	}
}
