//! SurrealDB KV implementation of DiskANN's provider traits.
//!
//! The upstream DiskANN graph owns search and mutation algorithms, but delegates persistence and
//! distance/computer access through provider traits. This module maps those trait calls onto
//! SurrealDB transactions, keeps the persisted `!de`/`!dn`/`!ds` keys authoritative, and batches
//! cache misses so RocksDB-backed lookups avoid long chains of individual point reads.
//!
//! # Cache coherency invariant
//!
//! The shared [`DiskAnnCache`] is consulted by both committed and in-flight transactions, so a
//! single rule keeps cache and KV from diverging:
//!
//! 1. **Read-only transactions** are the only path allowed to publish KV-sourced state into the
//!    cache. The `read_state` / `read_elements` / `read_nodes` paths gate their cache writes on
//!    `!context.tx.writeable()` for exactly this reason — a writable tx's `tx.get` returns its own
//!    buffered writes, so caching that view would leak uncommitted state to other transactions.
//! 2. **Writable transactions** may write through the cache *only* from within a
//!    `DiskAnnIndex::apply_compaction` frame, because that frame holds the graph write lock across
//!    the commit and clears the per-index cache via [`DiskAnnCache::remove_index`] on apply or
//!    commit failure. The mutating call sites are `set_element`, `delete`, `set_neighbors`,
//!    `append_vector`, and `write_state`.
//!
//! Adding a new cache write from a writable tx outside an `apply_compaction` frame, or relaxing
//! the read-only-tx guard, reintroduces the race described in
//! [issue #7318](https://github.com/surrealdb/surrealdb/issues/7318).

use std::marker::PhantomData;
use std::sync::Arc;

use ahash::{HashSet, HashSetExt};
use anyhow::Result;
use diskann::error::ErrorExt;
use diskann::graph::glue::{
	CopyIds, DefaultPostProcessor, ExpandBeam, InsertStrategy, PruneStrategy, SearchExt,
	SearchStrategy,
};
use diskann::graph::{AdjacencyList, workingset};
use diskann::provider::{
	Accessor, BuildDistanceComputer, BuildQueryComputer, DataProvider, DelegateNeighbor, Delete,
	ElementStatus, ExecutionContext, HasId, NeighborAccessor, NeighborAccessorMut, NoopGuard,
	SetElement,
};
use diskann::utils::VectorRepr;
use diskann::{ANNError, ANNResult, default_post_processor};
use diskann_utils::future::AssertSend;
use diskann_vector::Half;
use diskann_vector::distance::Metric;
use tracing::warn;

use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::idx::IndexKeyBase;
use crate::idx::trees::diskann::cache::DiskAnnCache;
use crate::idx::trees::diskann::{DiskAnnElement, DiskAnnNode, DiskAnnState, ElementId};
use crate::idx::trees::vector::SerializedVector;
#[cfg(test)]
use crate::idx::trees::vector::Vector;
use crate::kvs::{KVValue, Transaction};

/// Provider execution context passed through the upstream DiskANN trait calls.
#[derive(Clone)]
pub(super) struct DiskAnnProviderContext {
	/// Transaction used for all KV reads and writes in the current graph operation.
	pub(super) tx: Arc<Transaction>,
	/// Index key builder for the graph key families.
	pub(super) ikb: IndexKeyBase,
}

impl ExecutionContext for DiskAnnProviderContext {}

/// Vector element types that DiskANN can read from and write to persisted SurrealDB vectors.
pub(super) trait DiskAnnVectorElement:
	VectorRepr + Copy + Default + Send + Sync + 'static
{
	/// Converts a typed DiskANN slice into SurrealDB's revisioned vector representation.
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector;
	/// Copies a persisted vector into a typed scratch buffer for DiskANN distance computation.
	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()>;
}

fn vector_type_mismatch(expected: &str, vector: &SerializedVector) -> ANNError {
	ANNError::log_index_error(format!("DiskANN expected {expected} vector, got {vector:?}"))
}

fn vector_len_mismatch(current: usize, expected: usize) -> ANNError {
	ANNError::log_index_error(format!(
		"DiskANN vector dimension mismatch: got {current}, expected {expected}"
	))
}

impl DiskAnnVectorElement for f32 {
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector {
		SerializedVector::F32(slice.to_vec())
	}

	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()> {
		let SerializedVector::F32(values) = vector else {
			return Err(vector_type_mismatch("F32", vector));
		};
		if values.len() != buffer.len() {
			return Err(vector_len_mismatch(values.len(), buffer.len()));
		}
		buffer.copy_from_slice(values);
		Ok(())
	}
}

impl DiskAnnVectorElement for Half {
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector {
		SerializedVector::F16(slice.iter().map(|v| v.to_bits()).collect())
	}

	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()> {
		let SerializedVector::F16(values) = vector else {
			return Err(vector_type_mismatch("F16", vector));
		};
		if values.len() != buffer.len() {
			return Err(vector_len_mismatch(values.len(), buffer.len()));
		}
		for (dst, bits) in buffer.iter_mut().zip(values.iter()) {
			*dst = Half::from_bits(*bits);
		}
		Ok(())
	}
}

impl DiskAnnVectorElement for i8 {
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector {
		SerializedVector::I8(slice.to_vec())
	}

	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()> {
		let SerializedVector::I8(values) = vector else {
			return Err(vector_type_mismatch("I8", vector));
		};
		if values.len() != buffer.len() {
			return Err(vector_len_mismatch(values.len(), buffer.len()));
		}
		buffer.copy_from_slice(values);
		Ok(())
	}
}

impl DiskAnnVectorElement for u8 {
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector {
		SerializedVector::U8(slice.to_vec())
	}

	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()> {
		let SerializedVector::U8(values) = vector else {
			return Err(vector_type_mismatch("U8", vector));
		};
		if values.len() != buffer.len() {
			return Err(vector_len_mismatch(values.len(), buffer.len()));
		}
		buffer.copy_from_slice(values);
		Ok(())
	}
}

/// DiskANN data provider backed by SurrealDB's KV transaction API.
///
/// The upstream DiskANN graph code asks the provider for state, vectors, and adjacency lists during
/// graph search and mutation. This implementation keeps KV as the source of truth, while the shared
/// [`DiskAnnCache`] absorbs hot reads and batches cache misses with `getm`.
pub(super) struct DiskAnnProvider {
	pub(super) ikb: IndexKeyBase,
	table_id: TableId,
	cache: DiskAnnCache,
	dim: usize,
	metric: Metric,
}

impl DiskAnnProvider {
	/// Creates a provider for one DiskANN index and vector type/metric pair.
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		cache: DiskAnnCache,
		dim: usize,
		metric: Metric,
	) -> Self {
		Self {
			ikb,
			table_id,
			cache,
			dim,
			metric,
		}
	}

	/// Returns the cache scope tuple for this provider's index.
	fn cache_index(&self) -> (NamespaceId, DatabaseId, TableId, IndexId) {
		(self.ikb.ns(), self.ikb.db(), self.table_id, self.ikb.index())
	}

	/// Builds the provider context for a specific transaction.
	pub(super) fn context(&self, tx: Arc<Transaction>) -> DiskAnnProviderContext {
		DiskAnnProviderContext {
			tx,
			ikb: self.ikb.clone(),
		}
	}

	/// Reads persisted graph state, using the cache for the entry point and next element id.
	///
	/// The `writeable()` guard around `insert_state` enforces rule (1) of the module-level
	/// [cache coherency invariant](self): only read-only transactions publish KV state into the
	/// shared cache. Same rule the doc-id path enforces in `DiskAnnDocs::get_things_batch`
	/// (`docs.rs`).
	async fn read_state(&self, context: &DiskAnnProviderContext) -> Result<DiskAnnState> {
		let index = self.cache_index();
		if let Some(state) = self.cache.get_state(index) {
			return Ok(state);
		}
		let state = context.tx.get(&context.ikb.new_ds_key(), None).await?.unwrap_or_default();
		if !context.tx.writeable() {
			self.cache.insert_state(index, state.clone());
		}
		Ok(state)
	}

	/// Persists graph state and refreshes the cached copy atomically from the caller's view.
	async fn write_state(
		&self,
		context: &DiskAnnProviderContext,
		state: &DiskAnnState,
	) -> Result<()> {
		context.tx.set(&context.ikb.new_ds_key(), state).await?;
		self.cache.insert_state(self.cache_index(), state.clone());
		Ok(())
	}

	/// Reads element vector/status pairs in input order.
	///
	/// Cached entries are returned immediately; only misses are sent to KV through `getm`. Missing
	/// elements remain `None` so callers can preserve the previous single-read behavior.
	async fn read_elements(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Arc<DiskAnnElement>>>> {
		let index = self.cache_index();
		let mut elements = vec![None; element_ids.len()];
		let mut miss_positions = Vec::new();
		let mut miss_keys = Vec::new();
		for (pos, element_id) in element_ids.iter().copied().enumerate() {
			if let Some(element) = self.cache.get_element(index, element_id) {
				elements[pos] = Some(element);
			} else {
				miss_positions.push((pos, element_id));
				miss_keys.push(context.ikb.new_de_key(element_id));
			}
		}
		if !miss_keys.is_empty() {
			let fetched: Vec<Option<DiskAnnElement>> = context.tx.getm(miss_keys, None).await?;
			let cache_misses = !context.tx.writeable();
			for ((pos, element_id), element) in miss_positions.into_iter().zip(fetched) {
				if let Some(element) = element {
					elements[pos] = Some(if cache_misses {
						self.cache.insert_element(index, element_id, element)
					} else {
						Arc::new(element)
					});
				}
			}
		}
		Ok(elements)
	}

	/// Reads adjacency lists in input order.
	///
	/// Graph search fans out through adjacency lists, so batching cache misses here removes a
	/// large number of sequential point reads on KV backends that support efficient multi-get.
	async fn read_nodes(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Arc<DiskAnnNode>>>> {
		let index = self.cache_index();
		let mut nodes = vec![None; element_ids.len()];
		let mut miss_positions = Vec::new();
		let mut miss_keys = Vec::new();
		for (pos, element_id) in element_ids.iter().copied().enumerate() {
			if let Some(node) = self.cache.get_node(index, element_id) {
				nodes[pos] = Some(node);
			} else {
				miss_positions.push((pos, element_id));
				miss_keys.push(context.ikb.new_dn_key(element_id));
			}
		}
		if !miss_keys.is_empty() {
			let fetched: Vec<Option<DiskAnnNode>> = context.tx.getm(miss_keys, None).await?;
			let cache_misses = !context.tx.writeable();
			for ((pos, element_id), node) in miss_positions.into_iter().zip(fetched) {
				if let Some(node) = node {
					nodes[pos] = Some(if cache_misses {
						self.cache.insert_node(index, element_id, node)
					} else {
						Arc::new(node)
					});
				}
			}
		}
		Ok(nodes)
	}

	/// Allocates and persists the next graph element ID.
	pub(super) async fn allocate_element_id(
		&self,
		context: &DiskAnnProviderContext,
	) -> Result<ElementId> {
		let mut state = self.read_state(context).await?;
		let element_id = state.next_element_id;
		state.next_element_id = state.next_element_id.saturating_add(1);
		self.write_state(context, &state).await?;
		Ok(element_id)
	}

	/// Sets the entry point only when the graph has none.
	pub(super) async fn ensure_entry_point(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
	) -> Result<()> {
		let mut state = self.read_state(context).await?;
		if state.enter_point.is_none() {
			state.enter_point = Some(element_id);
			self.write_state(context, &state).await?;
		}
		Ok(())
	}

	/// Replaces the persisted graph entry point.
	pub(super) async fn set_entry_point(
		&self,
		context: &DiskAnnProviderContext,
		element_id: Option<ElementId>,
	) -> Result<()> {
		let mut state = self.read_state(context).await?;
		state.enter_point = element_id;
		self.write_state(context, &state).await
	}

	/// Returns usable graph starting points, repairing deleted entry points with a fallback scan.
	pub(super) async fn valid_starting_points(
		&self,
		context: &DiskAnnProviderContext,
	) -> ANNResult<Vec<ElementId>> {
		let state =
			self.read_state(context).await.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		if let Some(element_id) = state.enter_point
			&& self
				.status_by_internal_id(context, element_id)
				.await
				.is_ok_and(ElementStatus::is_valid)
		{
			return Ok(vec![element_id]);
		}
		// The stored entry point may have been deleted. Scan for a replacement and populate the
		// element cache with any entries touched during the fallback scan.
		let rng =
			context.ikb.new_de_range().map_err(|e| ANNError::log_index_error(e.to_string()))?;
		let mut cursor = context
			.tx
			.open_vals_cursor(rng, crate::idx::planner::ScanDirection::Forward, 0, None)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		let cache_misses = !context.tx.writeable();
		loop {
			let batch = cursor
				.next_batch(crate::kvs::ScanLimit::Count(crate::kvs::NORMAL_BATCH_SIZE))
				.await
				.map_err(|e| ANNError::log_index_error(e.to_string()))?;
			if batch.is_empty() {
				break;
			}
			for (key, value) in &batch {
				let key: crate::key::index::de::De<'_> = storekey::decode_borrow(key)
					.map_err(|e| ANNError::log_index_error(e.to_string()))?;
				let element = DiskAnnElement::kv_decode_value(value, ())
					.map_err(|e| ANNError::log_index_error(e.to_string()))?;
				let deleted = element.deleted;
				// Only publish non-tombstoned entries into the shared cache, and only from a
				// read-only tx (writable txs may have buffered uncommitted writes; see
				// `read_state`). Tombstones aren't useful for any subsequent reader.
				if !deleted && cache_misses {
					self.cache.insert_element(self.cache_index(), key.element_id, element);
				}
				if !deleted {
					// On a writable tx (i.e. compaction or a record-write path), persist
					// the discovered entry point so subsequent searches don't have to
					// re-scan `!de:*` from the dangling stored value. On a read-only tx
					// (KNN search) we can't write — the next compaction will fix it.
					//
					// Persistence is a pure hint: a transient failure here (e.g. a
					// concurrent `delc` on the state key) shouldn't fail the whole
					// search, since the discovered entry point is already correct in
					// memory and the next compaction will rediscover it.
					if context.tx.writeable()
						&& let Err(e) = self.set_entry_point(context, Some(key.element_id)).await
					{
						warn!(
							error = %e,
							element_id = key.element_id,
							"Failed to persist DiskANN entry-point hint; continuing with in-memory value",
						);
					}
					return Ok(vec![key.element_id]);
				}
			}
		}
		Ok(Vec::new())
	}

	/// Returns present, non-deleted vectors for the requested element ids.
	///
	/// Deleted and missing elements are reported as `None`, matching the pre-cache behavior used by
	/// post-search result construction.
	#[cfg(test)]
	async fn get_vectors(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Vector>>> {
		let elements = self.read_elements(context, element_ids).await?;
		Ok(elements
			.into_iter()
			.map(|element| {
				element.and_then(|element| {
					if element.deleted {
						None
					} else {
						Some(Vector::from(element.vector.clone()))
					}
				})
			})
			.collect())
	}
}

impl DataProvider for DiskAnnProvider {
	type Context = DiskAnnProviderContext;
	type InternalId = ElementId;
	type ExternalId = ElementId;
	type Error = ANNError;
	type Guard = NoopGuard<ElementId>;

	fn to_internal_id(
		&self,
		_: &Self::Context,
		gid: &Self::ExternalId,
	) -> Result<Self::InternalId, Self::Error> {
		Ok(*gid)
	}

	fn to_external_id(
		&self,
		_: &Self::Context,
		id: Self::InternalId,
	) -> Result<Self::ExternalId, Self::Error> {
		Ok(id)
	}
}

impl<T> SetElement<&[T]> for DiskAnnProvider
where
	T: DiskAnnVectorElement,
{
	type SetError = ANNError;

	async fn set_element(
		&self,
		context: &Self::Context,
		id: &Self::ExternalId,
		element: &[T],
	) -> Result<Self::Guard, Self::SetError> {
		if element.len() != self.dim {
			return Err(vector_len_mismatch(element.len(), self.dim));
		}
		let key = context.ikb.new_de_key(*id);
		let element = DiskAnnElement {
			vector: T::serialized_from_slice(element),
			deleted: false,
		};
		context
			.tx
			.set(&key, &element)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// Writable-tx cache write-through: only sound because the caller is inside an
		// `apply_compaction` frame that clears the per-index cache on commit/apply failure.
		// See the module-level `Cache coherency invariant` (rule 2).
		self.cache.insert_element(self.cache_index(), *id, element);
		Ok(NoopGuard::new(*id))
	}
}

impl Delete for DiskAnnProvider {
	async fn delete(
		&self,
		context: &Self::Context,
		gid: &Self::ExternalId,
	) -> Result<(), Self::Error> {
		let key = context.ikb.new_de_key(*gid);
		let Some(element) = self
			.read_elements(context, std::slice::from_ref(gid))
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?
			.pop()
			.flatten()
		else {
			return Ok(());
		};
		let mut element = (*element).clone();
		element.deleted = true;
		context
			.tx
			.set(&key, &element)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// Keep the deleted marker hot and visible to subsequent status checks.
		self.cache.insert_element(self.cache_index(), *gid, element);
		Ok(())
	}

	async fn release(
		&self,
		context: &Self::Context,
		id: Self::InternalId,
	) -> Result<(), Self::Error> {
		context
			.tx
			.del(&context.ikb.new_de_key(id))
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// Release removes the element record entirely, so remove any cached vector/status too.
		self.cache.remove_element(self.cache_index(), id);
		Ok(())
	}

	async fn status_by_internal_id(
		&self,
		context: &Self::Context,
		id: Self::InternalId,
	) -> Result<ElementStatus, Self::Error> {
		let Some(element) = self
			.read_elements(context, std::slice::from_ref(&id))
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?
			.pop()
			.flatten()
		else {
			return Err(ANNError::log_index_error(format!("DiskANN element {id} is missing")));
		};
		if element.deleted {
			Ok(ElementStatus::Deleted)
		} else {
			Ok(ElementStatus::Valid)
		}
	}

	fn status_by_external_id(
		&self,
		context: &Self::Context,
		gid: &Self::ExternalId,
	) -> impl std::future::Future<Output = Result<ElementStatus, Self::Error>> + Send {
		self.status_by_internal_id(context, *gid)
	}
}

/// Typed vector accessor used by DiskANN distance computation and pruning.
pub(super) struct DiskAnnAccessor<'a, T> {
	/// Provider that owns KV/cache access.
	provider: &'a DiskAnnProvider,
	/// Transaction/key context for the current DiskANN operation.
	context: &'a DiskAnnProviderContext,
	/// Reusable scratch buffer for converting persisted vectors into typed slices.
	buffer: Box<[T]>,
	/// Ties the accessor to the vector element type without storing values of that type.
	_marker: PhantomData<T>,
}

impl<'a, T> DiskAnnAccessor<'a, T>
where
	T: DiskAnnVectorElement,
{
	fn new(provider: &'a DiskAnnProvider, context: &'a DiskAnnProviderContext) -> Self {
		Self {
			provider,
			context,
			buffer: vec![T::default(); provider.dim].into_boxed_slice(),
			_marker: PhantomData,
		}
	}
}

impl<T> HasId for DiskAnnAccessor<'_, T> {
	type Id = ElementId;
}

impl<T> Accessor for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	type Element<'a>
		= &'a [T]
	where
		Self: 'a;
	type ElementRef<'a> = &'a [T];
	type GetError = ANNError;

	async fn get_element(&mut self, id: ElementId) -> Result<&[T], ANNError> {
		// Delegated through the batched path so the cache-population invariant only lives in one
		// place. `read_elements(&[id])` uses `getm` on a 1-element vec, which is a no-cost call.
		let mut elements = self
			.provider
			.read_elements(self.context, std::slice::from_ref(&id))
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		let Some(element) = elements.pop().flatten() else {
			return Err(ANNError::log_index_error(format!("DiskANN element {id} is missing")));
		};
		T::copy_from_serialized(&element.vector, &mut self.buffer)?;
		Ok(&self.buffer)
	}

	/// Batches element reads requested by DiskANN's distance-computation path.
	async fn on_elements_unordered<Itr, F>(&mut self, itr: Itr, mut f: F) -> Result<(), ANNError>
	where
		Self: Sync,
		Itr: Iterator<Item = Self::Id> + Send,
		F: Send + for<'a> FnMut(Self::ElementRef<'a>, Self::Id),
	{
		let ids: Vec<_> = itr.collect();
		let elements = self
			.provider
			.read_elements(self.context, &ids)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		for (id, element) in ids.into_iter().zip(elements) {
			let Some(element) = element else {
				return Err(ANNError::log_index_error(format!("DiskANN element {id} is missing")));
			};
			T::copy_from_serialized(&element.vector, &mut self.buffer)?;
			f(&self.buffer, id);
		}
		Ok(())
	}
}

impl<'a, T> DelegateNeighbor<'a> for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	type Delegate = DiskAnnNeighborAccessor<'a>;

	fn delegate_neighbor(&'a mut self) -> Self::Delegate {
		DiskAnnNeighborAccessor {
			provider: self.provider,
			context: self.context,
		}
	}
}

impl<T> BuildQueryComputer<&[T]> for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	type QueryComputerError = ANNError;
	type QueryComputer = <T as VectorRepr>::QueryDistance;

	fn build_query_computer(&self, from: &[T]) -> Result<Self::QueryComputer, ANNError> {
		Ok(T::query_distance(from, self.provider.metric))
	}
}

impl<T> BuildDistanceComputer for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	type DistanceComputerError = ANNError;
	type DistanceComputer = <T as VectorRepr>::Distance;

	fn build_distance_computer(&self) -> Result<Self::DistanceComputer, ANNError> {
		Ok(T::distance(self.provider.metric, Some(self.provider.dim)))
	}
}

impl<T> SearchExt for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	async fn starting_points(&self) -> ANNResult<Vec<ElementId>> {
		self.provider.valid_starting_points(self.context).await
	}
}

impl<T> ExpandBeam<&[T]> for DiskAnnAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	/// Expands one search beam using batched adjacency-list reads and batched candidate distances.
	///
	/// The default DiskANN implementation fetches neighbors one node at a time. This override keeps
	/// predicate semantics intact, deduplicates candidates before distance calculation, and lets
	/// `distances_unordered` batch the vector reads behind the candidate ids.
	async fn expand_beam<Itr, P, F>(
		&mut self,
		ids: Itr,
		computer: &Self::QueryComputer,
		mut pred: P,
		mut on_neighbors: F,
	) -> ANNResult<()>
	where
		Itr: Iterator<Item = Self::Id> + Send,
		P: diskann::graph::glue::HybridPredicate<Self::Id> + Send + Sync,
		F: FnMut(f32, Self::Id) + Send,
	{
		let ids: Vec<_> = ids.collect();
		let nodes = self
			.provider
			.read_nodes(self.context, &ids)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// `distances_unordered` does not depend on input order, so a single dedup pass via
		// `HashSet::insert` is sufficient — we no longer need a parallel `Vec` of candidates.
		let candidate_capacity = nodes.iter().flatten().map(|node| node.neighbors.len()).sum();
		let mut seen = HashSet::with_capacity(candidate_capacity);
		for node in nodes.iter().flatten() {
			for &id in &node.neighbors {
				if pred.eval(&id) {
					seen.insert(id);
				}
			}
		}
		self.distances_unordered(seen.into_iter(), computer, |distance, id| {
			if pred.eval_mut(&id) {
				on_neighbors(distance, id);
			}
		})
		.send()
		.await
		.allow_transient("allowing transient error in beam expansion")?;
		Ok(())
	}
}

/// Accessor for reading and mutating DiskANN adjacency lists.
#[derive(Clone, Copy)]
pub(super) struct DiskAnnNeighborAccessor<'a> {
	/// Provider that owns adjacency-list KV/cache access.
	provider: &'a DiskAnnProvider,
	/// Transaction/key context for the current DiskANN operation.
	context: &'a DiskAnnProviderContext,
}

impl HasId for DiskAnnNeighborAccessor<'_> {
	type Id = ElementId;
}

impl NeighborAccessor for DiskAnnNeighborAccessor<'_> {
	async fn get_neighbors(
		self,
		id: ElementId,
		neighbors: &mut AdjacencyList<ElementId>,
	) -> ANNResult<Self> {
		if let Some(node) = self
			.provider
			.read_nodes(self.context, &[id])
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?
			.into_iter()
			.next()
			.flatten()
		{
			neighbors.overwrite_trusted(&node.neighbors);
		} else {
			neighbors.clear();
		}
		Ok(self)
	}
}

impl NeighborAccessorMut for DiskAnnNeighborAccessor<'_> {
	async fn set_neighbors(self, id: ElementId, neighbors: &[ElementId]) -> ANNResult<Self> {
		let node = DiskAnnNode {
			neighbors: neighbors.to_vec(),
		};
		self.context
			.tx
			.set(&self.context.ikb.new_dn_key(id), &node)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// Keep cached adjacency coherent with the persisted graph mutation.
		self.provider.cache.insert_node(self.provider.cache_index(), id, node);
		Ok(self)
	}

	async fn append_vector(self, id: ElementId, neighbors: &[ElementId]) -> ANNResult<Self> {
		let key = self.context.ikb.new_dn_key(id);
		let mut node: DiskAnnNode = self
			.provider
			.read_nodes(self.context, &[id])
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?
			.into_iter()
			.next()
			.flatten()
			.map(|node| (*node).clone())
			.unwrap_or_default();
		for neighbor in neighbors {
			if !node.neighbors.contains(neighbor) {
				node.neighbors.push(*neighbor);
			}
		}
		self.context
			.tx
			.set(&key, &node)
			.await
			.map_err(|e| ANNError::log_index_error(e.to_string()))?;
		// Keep cached adjacency coherent with the persisted graph mutation.
		self.provider.cache.insert_node(self.provider.cache_index(), id, node);
		Ok(self)
	}
}

/// DiskANN search/insert/prune strategy using SurrealDB's provider accessors.
#[derive(Debug)]
pub(super) struct DiskAnnStrategy<T>(PhantomData<fn() -> T>);

impl<T> Default for DiskAnnStrategy<T> {
	fn default() -> Self {
		Self(PhantomData)
	}
}

impl<T> Clone for DiskAnnStrategy<T> {
	fn clone(&self) -> Self {
		Self::default()
	}
}

impl<T> SearchStrategy<DiskAnnProvider, &[T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type QueryComputer = <T as VectorRepr>::QueryDistance;
	type SearchAccessorError = ANNError;
	type SearchAccessor<'a> = DiskAnnAccessor<'a, T>;

	fn search_accessor<'a>(
		&'a self,
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
	) -> Result<DiskAnnAccessor<'a, T>, ANNError> {
		Ok(DiskAnnAccessor::new(provider, context))
	}
}

impl<T> DefaultPostProcessor<DiskAnnProvider, &[T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	default_post_processor!(CopyIds);
}

impl<T> PruneStrategy<DiskAnnProvider> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type WorkingSet = workingset::Map<ElementId, Box<[T]>, workingset::map::Ref<[T]>>;
	type DistanceComputer<'a> = <T as VectorRepr>::Distance;
	type PruneAccessor<'a> = DiskAnnAccessor<'a, T>;
	type PruneAccessorError = ANNError;

	fn create_working_set(&self, capacity: usize) -> Self::WorkingSet {
		workingset::map::Builder::new(workingset::map::Capacity::Default).build(capacity)
	}

	fn prune_accessor<'a>(
		&'a self,
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
	) -> Result<Self::PruneAccessor<'a>, ANNError> {
		Ok(DiskAnnAccessor::new(provider, context))
	}
}

impl<T> InsertStrategy<DiskAnnProvider, &[T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type PruneStrategy = Self;

	fn prune_strategy(&self) -> Self::PruneStrategy {
		self.clone()
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
	use crate::idx::trees::diskann::cache::DiskAnnCache;
	use crate::kvs::{Datastore, LockType, TransactionType};

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	async fn provider_and_context() -> Result<(DiskAnnProvider, DiskAnnProviderContext)> {
		let ds = Datastore::new("memory").await?;
		let tx = Arc::new(ds.transaction(TransactionType::Write, LockType::Optimistic).await?);
		let ikb = ikb();
		let provider =
			DiskAnnProvider::new(ikb, TableId(4), DiskAnnCache::new(1024 * 1024), 2, Metric::L2);
		let context = provider.context(tx);
		Ok((provider, context))
	}

	#[tokio::test]
	async fn diskann_accessor_batches_elements_and_reports_missing() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set(
				&context.ikb.new_de_key(1),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![1.0, 2.0]),
					deleted: false,
				},
			)
			.await?;
		context
			.tx
			.set(
				&context.ikb.new_de_key(2),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![3.0, 4.0]),
					deleted: false,
				},
			)
			.await?;

		let mut accessor = DiskAnnAccessor::<f32>::new(&provider, &context);
		let mut seen = Vec::new();
		accessor
			.on_elements_unordered(vec![1, 2].into_iter(), |values, id| {
				seen.push((id, values.to_vec()));
			})
			.await
			.unwrap();

		assert_eq!(seen, vec![(1, vec![1.0, 2.0]), (2, vec![3.0, 4.0])]);
		// Provider reads through a writable tx must not publish into the shared cache —
		// `tx.get` returns the writer's own buffered (possibly uncommitted) writes, and the
		// cache is process-wide, so leaking that view to other txs is the failure mode
		// #7318 surfaced.
		assert!(provider.cache.get_element(provider.cache_index(), 1).is_none());
		assert!(provider.cache.get_element(provider.cache_index(), 2).is_none());
		assert!(
			accessor.on_elements_unordered(vec![3].into_iter(), |_values, _id| {}).await.is_err()
		);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_provider_batches_nodes_and_updates_cache_on_mutation() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set(
				&context.ikb.new_dn_key(1),
				&DiskAnnNode {
					neighbors: vec![2, 3],
				},
			)
			.await?;
		context
			.tx
			.set(
				&context.ikb.new_dn_key(2),
				&DiskAnnNode {
					neighbors: vec![4],
				},
			)
			.await?;

		let nodes = provider.read_nodes(&context, &[1, 2]).await?;
		assert_eq!(nodes[0].as_ref().unwrap().neighbors, vec![2, 3]);
		assert_eq!(nodes[1].as_ref().unwrap().neighbors, vec![4]);

		let neighbor_accessor = DiskAnnNeighborAccessor {
			provider: &provider,
			context: &context,
		};
		neighbor_accessor.set_neighbors(1, &[8, 9]).await.unwrap();

		let cached = provider.cache.get_node(provider.cache_index(), 1).unwrap();
		assert_eq!(cached.neighbors, vec![8, 9]);
		Ok(())
	}

	#[tokio::test]
	async fn diskann_provider_batch_vectors_omits_deleted_elements() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set(
				&context.ikb.new_de_key(1),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![1.0, 2.0]),
					deleted: false,
				},
			)
			.await?;
		context
			.tx
			.set(
				&context.ikb.new_de_key(2),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![3.0, 4.0]),
					deleted: true,
				},
			)
			.await?;

		let vectors = provider.get_vectors(&context, &[1, 2, 3]).await?;
		assert!(vectors[0].is_some());
		assert!(vectors[1].is_none());
		assert!(vectors[2].is_none());
		Ok(())
	}
}
