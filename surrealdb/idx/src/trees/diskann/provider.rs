//! SurrealDB KV implementation of DiskANN's provider traits.
//!
//! The upstream DiskANN graph owns search and mutation algorithms, but delegates persistence and
//! distance/computer access through provider traits. This module maps those trait calls onto
//! SurrealDB transactions, keeps the persisted `!de`/`!dn`/`!ds` keys authoritative, and batches
//! cache misses so RocksDB-backed lookups avoid long chains of individual point reads.
//!
//! # Cache coherency invariant
//!
//! The shared [`DiskAnnCache`] serves committed and in-flight transactions alike, on this process
//! and alongside every other process's commits. Every graph entry is stamped with the wrapper that
//! published it and the `!dg` compaction generation its value holds at, and a transaction reads
//! through a [`CacheView`] of its own wrapper's entries ([`DiskAnnProviderContext::view`]). Every
//! commit that writes `!de`, `!dn` or `!ds`, or a vector's document mapping, also advances `!dg`
//! with a conditional put, so one generation identifies one committed state of all of them, and an
//! entry stamped at a reader's own generation is exactly what that reader would read from KV.
//!
//! An entry also holds past its own generation while the wrapper's [`Continuity`] span covers it.
//! Every generation in the span was committed by one of the wrapper's own frames, and each wrote
//! through or evicted every entry it changed. So an entry stamped inside the span that no commit
//! has replaced still holds at every later generation up to the span's head, and a commit costs
//! the cache only the entries it changed. A frame that finds `!dg` anywhere but the span's head —
//! another process committed, or the wrapper has not committed yet — reopens the span at its own
//! snapshot, so nothing from before a commit the wrapper did not make is served past it.
//!
//! The one write outside these rules is a search on a writable transaction persisting a repaired
//! entry point (see [`DiskAnnProvider::valid_starting_points`]), which does not advance `!dg`. A
//! cached `!ds` can therefore hold an older entry point than KV does at the same generation, which
//! is harmless: every reader checks that an entry point is live before starting from it.
//!
//! Who publishes which stamp:
//!
//! 1. **Read-only transactions** publish what they read from KV, stamped with their snapshot's
//!    generation — except a reader whose generation is inside the span but behind its head, which
//!    publishes nothing. What it read holds at its own generation, but a frame since may have
//!    changed the key, and once that frame's write-through is evicted the stale entry would be
//!    served across the span. Writable transactions publish nothing they read, since their reads
//!    include their own buffered writes.
//! 2. **A compaction frame** (`DiskAnnIndex::apply_compaction`) writes through every value it
//!    writes, stamped with the generation it commits at, and is served those entries itself. It
//!    holds the graph write lock across the commit, so no reader is served them early, and it
//!    extends the span only once the commit has succeeded. A frame that fails to commit leaves its
//!    generation free for another process to commit with a different graph, so it clears the
//!    index's entries with [`DiskAnnCache::remove_index`], and closes the span, before it releases
//!    the lock. A write outside a frame writes nothing through; it evicts.
//! 3. **A read that will be written back comes from the transaction.**
//!    [`DiskAnnProvider::write_state`] persists the whole `!ds` struct, so a read-modify-write of
//!    it uses [`DiskAnnProvider::read_state_for_update`], and the value written back is exactly the
//!    transaction's, entry point included. Deriving `next_element_id` from anything else hands out
//!    an id another commit already used and rewinds the allocator, so every later allocation
//!    collides too.
//!
//! A compaction frame with a read view adds one publisher: what the view reads is committed at the
//! view's own generation, so it is published as a read-only transaction's read is, under the
//! overlay rule its frame enforces (`DiskAnnBuildFrame::publish_clean_elements`): never for a key
//! the frame has written, and never after the frame is closed.
//!
//! Every view is taken under the wrapper's graph lock and used only while it is held, shared by
//! readers and exclusive for a frame. That orders a reader's fence check and its publish against a
//! frame's commit and its extension of the span. A wrapper the registry has dropped still runs the
//! searches holding it, beside its replacement: the two share cache keys but not a lock, so each is
//! served only the entries it published.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::convert::Infallible;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use anyhow::{Result, ensure};
use diskann::graph::glue::{
	CopyIds, DefaultPostProcessor, HybridPredicate, InsertStrategy, MultiInsertStrategy,
	PruneAccessor, PruneStrategy, SearchAccessor, SearchStrategy,
};
use diskann::graph::{AdjacencyList, workingset};
use diskann::provider::{
	DataProvider, Delete, ElementStatus, ExecutionContext, HasId, NeighborAccessor,
	NeighborAccessorMut, NoopGuard, SetElement,
};
use diskann::utils::VectorRepr;
use diskann::{ANNError, ANNResult, default_post_processor};
use diskann_utils::views::Matrix;
use diskann_vector::distance::Metric;
use diskann_vector::{Half, PreprocessedDistanceFunction};
use surrealdb_datastore::Transaction;
use tracing::warn;

use crate::IndexKeyBase;
use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
use crate::key::KVValue;
use crate::key::schema::DiskannElementKey;
use crate::trees::diskann::cache::{CacheView, Continuity, DiskAnnCache, Stamp};
use crate::trees::diskann::frame::{DiskAnnBuildFrame, FrameKv, TaskGuard};
use crate::trees::diskann::{DiskAnnElement, DiskAnnNode, DiskAnnState, ElementId};
use crate::trees::vector::SerializedVector;
#[cfg(test)]
use crate::trees::vector::Vector;

/// Whose choice an adjacency list written to the graph is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum NodeWrite {
	/// The list an insert chose for the element it inserted, or any other full write outside a
	/// batched insert's back-edge phase.
	Own,
	/// A back-edge a batched insert added to an existing list.
	BackEdge,
}

/// Provider execution context passed through the upstream DiskANN trait calls.
#[derive(Clone)]
pub(super) struct DiskAnnProviderContext {
	/// Transaction used for all KV reads and writes in the current graph operation.
	pub(super) tx: Arc<Transaction>,
	/// Index key builder for the graph key families.
	pub(super) ikb: IndexKeyBase,
	/// What `tx` is served from, and publishes into, the cache.
	pub(super) view: CacheView,
	/// The compaction frame this context runs in, if any. `None` for KNN search and the
	/// pending-write path.
	pub(super) frame: Option<Arc<DiskAnnBuildFrame>>,
}

impl DiskAnnProviderContext {
	/// The stamp this context publishes a value it read from KV under, or `None` when it must not
	/// publish (rule 1 of the module-level [cache coherency invariant](self)).
	fn publication(&self) -> Option<Stamp> {
		if self.tx.writeable() {
			None
		} else {
			self.view.publication()
		}
	}

	/// The same context, running in `frame`.
	pub(super) fn in_frame(self, frame: Arc<DiskAnnBuildFrame>) -> Self {
		Self {
			frame: Some(frame),
			..self
		}
	}

	/// The frame this context runs in, if any.
	pub(super) fn frame(&self) -> Option<&DiskAnnBuildFrame> {
		self.frame.as_deref()
	}

	/// How this context reaches KV: through `tx`, checked against the frame's cancellation before
	/// every operation.
	pub(super) fn kv(&self) -> FrameKv<'_> {
		FrameKv::new(&self.tx, self.frame())
	}

	/// Maps a provider failure into the error upstream takes, and records it, typed, as the frame's
	/// failure. Upstream stringifies the error, and in its batched paths drops it altogether, so
	/// the frame's record is what fails the frame, with the error's classification intact.
	fn ann_err(&self, e: anyhow::Error) -> ANNError {
		let message = e.to_string();
		if let Some(frame) = self.frame() {
			frame.record_error(e);
		}
		ANNError::message(message)
	}
}

impl ExecutionContext for DiskAnnProviderContext {
	/// Wraps every future upstream's batched insert spawns.
	///
	/// Builds the frame's [`TaskGuard`] before returning, and moves it inside the returned future
	/// so it is dropped there: upstream joins its tasks and discards the `JoinError`, so a panic
	/// is only observable from within the task. The guard is built here, synchronously, so the
	/// task's share of the frame's places is held from the moment this returns, before the future
	/// is first polled. The current tracing span is carried into the task for attribution in logs;
	/// it is not what routes the failure.
	fn wrap_spawn<F, T>(&self, f: F) -> impl std::future::Future<Output = T> + Send + 'static
	where
		F: std::future::Future<Output = T> + Send + 'static,
	{
		let guard = self.frame.as_ref().map(TaskGuard::new);
		let span = tracing::Span::current();
		tracing::Instrument::instrument(
			async move {
				// Dropped inside the future: during an unwind from `f.await`, and with the future
				// when it is dropped without ever being polled.
				let _guard = guard;
				f.await
			},
			span,
		)
	}
}

/// Vector element types that DiskANN can read from and write to persisted SurrealDB vectors.
pub(super) trait DiskAnnVectorElement:
	VectorRepr + Copy + Default + Send + Sync + std::fmt::Debug + 'static
{
	/// Converts a typed DiskANN slice into SurrealDB's revisioned vector representation.
	fn serialized_from_slice(slice: &[Self]) -> SerializedVector;
	/// Copies a persisted vector into a typed scratch buffer for DiskANN distance computation.
	fn copy_from_serialized(vector: &SerializedVector, buffer: &mut [Self]) -> ANNResult<()>;
}

fn vector_type_mismatch(expected: &str, vector: &SerializedVector) -> ANNError {
	ANNError::message(format!("DiskANN expected {expected} vector, got {vector:?}"))
}

fn vector_len_mismatch(current: usize, expected: usize) -> ANNError {
	ANNError::message(format!(
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
	format_version: u16,
	cache: DiskAnnCache,
	/// This wrapper's owner id for the graph entries it publishes.
	owner: u64,
	dim: usize,
	metric: Metric,
}

impl DiskAnnProvider {
	/// Creates a provider for one DiskANN index and vector type/metric pair.
	pub(super) fn new(
		ikb: IndexKeyBase,
		table_id: TableId,
		format_version: u16,
		cache: DiskAnnCache,
		dim: usize,
		metric: Metric,
	) -> Self {
		Self {
			ikb,
			table_id,
			format_version,
			owner: cache.new_owner(),
			cache,
			dim,
			metric,
		}
	}

	/// A provider for the same index that publishes under this provider's cache owner, so what a
	/// handle built over it writes through is attributed exactly as this provider's writes are.
	pub(super) fn same_owner(&self) -> Self {
		Self {
			ikb: self.ikb.clone(),
			table_id: self.table_id,
			format_version: self.format_version,
			cache: self.cache.clone(),
			owner: self.owner,
			dim: self.dim,
			metric: self.metric,
		}
	}

	/// The vector dimension of this provider's index.
	pub(super) fn dim(&self) -> usize {
		self.dim
	}

	/// Returns the cache scope tuple for this provider's index.
	fn cache_index(&self) -> (NamespaceId, DatabaseId, TableId, IndexId, u16) {
		(self.ikb.ns(), self.ikb.db(), self.table_id, self.ikb.index(), self.format_version)
	}

	/// Builds the provider context for a transaction whose snapshot holds `!dg` at `generation`,
	/// under a graph lock over which the wrapper's span is `continuity`.
	pub(super) fn context(
		&self,
		tx: Arc<Transaction>,
		generation: Option<u64>,
		continuity: Option<Continuity>,
	) -> DiskAnnProviderContext {
		DiskAnnProviderContext {
			tx,
			ikb: self.ikb.clone(),
			view: self.reader_view(generation, continuity),
			frame: None,
		}
	}

	/// The view a transaction that only reads the graph, and whose snapshot holds `!dg` at
	/// `generation`, reads this wrapper's entries through while the span is `continuity`.
	pub(super) fn reader_view(
		&self,
		generation: Option<u64>,
		continuity: Option<Continuity>,
	) -> CacheView {
		CacheView::reader(self.owner, generation, continuity)
	}

	/// Builds the provider context for a compaction frame advancing `!dg` from `generation`,
	/// under the graph write lock over which the wrapper's span is `continuity`.
	pub(super) fn frame_context(
		&self,
		tx: Arc<Transaction>,
		generation: Option<u64>,
		continuity: Option<Continuity>,
	) -> DiskAnnProviderContext {
		DiskAnnProviderContext {
			tx,
			ikb: self.ikb.clone(),
			view: CacheView::frame(self.owner, generation, continuity),
			frame: None,
		}
	}

	/// Reads persisted graph state, using the cache for the entry point and next element id.
	///
	/// **Read-only.** A caller that intends to write the state back must use
	/// [`Self::read_state_for_update`] instead (rule 3 of the module-level
	/// [cache coherency invariant](self)).
	async fn read_state(&self, context: &DiskAnnProviderContext) -> Result<DiskAnnState> {
		if let Some(state) = context.frame().and_then(DiskAnnBuildFrame::written_state) {
			return Ok(state);
		}
		let index = self.cache_index();
		if let Some(state) = self.cache.get_state(index, &context.view) {
			return Ok(state);
		}
		let state = self.load_state(context).await?;
		if let Some(stamp) = context.publication() {
			self.cache.insert_state(index, stamp, state.clone());
		}
		Ok(state)
	}

	/// Reads persisted graph state for a read-modify-write: the state a frame last wrote, or the
	/// transaction's — never the cache, and never a frame's read view.
	///
	/// `!ds` holds the element-id allocator alongside the entry point, and
	/// [`Self::write_state`] persists both, so the value written back must be exactly the
	/// transaction's. The transaction's snapshot includes every committed write, and on a
	/// writable transaction also this frame's own earlier allocations, which the frame's overlay
	/// holds too, so successive calls within one frame see each other. Two *concurrent* frames are
	/// fenced by the `!dg` conditional bump that lets only one commit.
	///
	/// Costs one point read per call outside a frame, or in a frame that has not written the state
	/// yet. Every caller is on a compaction path that performs orders
	/// of magnitude more KV work per vector, and the search path does not use this.
	async fn read_state_for_update(
		&self,
		context: &DiskAnnProviderContext,
	) -> Result<DiskAnnState> {
		if let Some(state) = context.frame().and_then(DiskAnnBuildFrame::written_state) {
			return Ok(state);
		}
		self.load_state(context).await
	}

	/// Reads `!ds` from the transaction, defaulting when the key is absent.
	async fn load_state(&self, context: &DiskAnnProviderContext) -> Result<DiskAnnState> {
		Ok(context.kv().get_key(&context.ikb.new_ds_key()).await?.unwrap_or_default())
	}

	/// Persists graph state, and writes it through to the cache as
	/// [`Self::write_element_through`] does an element.
	async fn write_state(
		&self,
		context: &DiskAnnProviderContext,
		state: &DiskAnnState,
	) -> Result<()> {
		context.kv().set_key(&context.ikb.new_ds_key(), state).await?;
		let through = || match context.view.write_through() {
			Some(stamp) => self.cache.insert_state(self.cache_index(), stamp, state.clone()),
			None => self.cache.remove_state(self.cache_index()),
		};
		match context.frame() {
			Some(frame) => frame.wrote_state(state.clone(), through),
			None => through(),
		}
		Ok(())
	}

	/// Writes one element through to the cache (rule 2 of the module-level
	/// [cache coherency invariant](self)): at the frame's commit generation inside a compaction
	/// frame, and outside one by evicting whatever the cache held for it.
	///
	/// Inside a frame the element is also recorded as written, in the same critical section, and a
	/// closed frame does neither.
	fn write_element_through(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
		element: DiskAnnElement,
	) {
		let element = Arc::new(element);
		let through = || match context.view.write_through() {
			Some(stamp) => {
				self.cache.insert_shared_element(
					self.cache_index(),
					element_id,
					stamp,
					Arc::clone(&element),
				);
			}
			None => self.cache.remove_element(self.cache_index(), element_id),
		};
		match context.frame() {
			Some(frame) => frame.wrote_element(element_id, Some(Arc::clone(&element)), through),
			None => through(),
		}
	}

	/// Writes one adjacency list through to the cache, as [`Self::write_element_through`] does
	/// an element.
	fn write_node_through(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
		node: DiskAnnNode,
		write: NodeWrite,
	) {
		let node = Arc::new(node);
		let through = || match context.view.write_through() {
			Some(stamp) => {
				self.cache.insert_shared_node(
					self.cache_index(),
					element_id,
					stamp,
					Arc::clone(&node),
				);
			}
			None => self.cache.remove_node(self.cache_index(), element_id),
		};
		match context.frame() {
			Some(frame) => {
				frame.wrote_node(element_id, Arc::clone(&node), write == NodeWrite::Own, through)
			}
			None => through(),
		}
	}

	/// Persists an adjacency list and writes it through, as `NeighborAccessorMut::set_neighbors`
	/// does. `write` says whose choice the list is.
	pub(super) async fn write_node(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
		node: DiskAnnNode,
		write: NodeWrite,
	) -> Result<()> {
		context.kv().set_key(&context.ikb.new_dn_key(element_id), &node).await?;
		self.write_node_through(context, element_id, node, write);
		Ok(())
	}

	/// Reads element vector/status pairs in input order.
	///
	/// Inside a frame, an element the frame wrote is answered from its overlay. Every other id is
	/// answered from the cache when the context's view is served the entry, and each distinct miss
	/// is read once, in one batch: through the frame's read view when it has one, which publishes
	/// what it read (see [`DiskAnnBuildFrame::publish_clean_elements`]), and through the
	/// transaction otherwise. Missing elements remain `None` so callers can preserve the previous
	/// single-read behavior.
	pub(super) async fn read_elements(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Arc<DiskAnnElement>>>> {
		let index = self.cache_index();
		let frame = context.frame();
		let mut elements = vec![None; element_ids.len()];
		let written = frame.map(|frame| frame.written_elements_of(element_ids));
		// Each distinct missing element, at the first position asking for it.
		let mut misses: Vec<(ElementId, usize)> = Vec::new();
		// Each later position asking for an element already known missing,
		// with that element's index in `misses`.
		let mut repeats: Vec<(usize, usize)> = Vec::new();
		let mut miss_slot: HashMap<ElementId, usize> = HashMap::default();
		for (pos, element_id) in element_ids.iter().copied().enumerate() {
			if let Some(own) = written.as_ref().and_then(|written| written[pos].clone()) {
				if let Some(frame) = frame {
					frame.stats.reads_overlay.fetch_add(1, Ordering::Relaxed);
				}
				elements[pos] = own;
			} else if let Some(element) = self.cache.get_element(index, element_id, &context.view) {
				elements[pos] = Some(element);
			} else {
				match miss_slot.entry(element_id) {
					Entry::Occupied(slot) => repeats.push((*slot.get(), pos)),
					Entry::Vacant(slot) => {
						slot.insert(misses.len());
						misses.push((element_id, pos));
					}
				}
			}
		}
		if !misses.is_empty() {
			let keys: Vec<_> = misses.iter().map(|(id, _)| context.ikb.new_de_key(*id)).collect();
			let view = match frame {
				Some(frame) => frame.view()?,
				None => None,
			};
			match (frame, view) {
				(Some(frame), Some(view)) => {
					let fetched: Vec<Option<DiskAnnElement>> =
						frame.read_through(&view, &keys).await?;
					let fetched: Vec<(ElementId, usize, Arc<DiskAnnElement>)> = misses
						.iter()
						.copied()
						.zip(fetched)
						.filter_map(|((id, pos), element)| Some((id, pos, Arc::new(element?))))
						.collect();
					if let Some(stamp) = view.publication() {
						frame.publish_clean_elements(
							fetched.iter().map(|(id, _, element)| (*id, element)),
							|id, element| {
								self.cache.insert_shared_element(
									index,
									id,
									stamp,
									Arc::clone(element),
								);
							},
						);
					}
					for (_, pos, element) in fetched {
						elements[pos] = Some(element);
					}
				}
				(frame, _) => {
					if let Some(frame) = frame {
						frame.stats.reads_tx.fetch_add(1, Ordering::Relaxed);
					}
					let fetched: Vec<Option<DiskAnnElement>> =
						context.kv().get_many_key(keys).await?;
					// A short answer would leave the trailing misses unread, silently.
					ensure!(
						fetched.len() == misses.len(),
						"{} elements read for {} requested",
						fetched.len(),
						misses.len()
					);
					let publish = context.publication();
					for ((element_id, pos), element) in misses.iter().copied().zip(fetched) {
						if let Some(element) = element {
							let element = match publish {
								Some(stamp) => {
									self.cache.insert_element(index, element_id, stamp, element)
								}
								None => Arc::new(element),
							};
							elements[pos] = Some(element);
						}
					}
				}
			}
			for (slot, pos) in repeats {
				let fetched = elements[misses[slot].1].clone();
				elements[pos] = fetched;
			}
		}
		Ok(elements)
	}

	/// Reads adjacency lists in input order, routed as [`Self::read_elements`] routes elements.
	///
	/// Graph search fans out through adjacency lists, so batching cache misses here removes a
	/// large number of sequential point reads on KV backends that support efficient multi-get.
	pub(super) async fn read_nodes(
		&self,
		context: &DiskAnnProviderContext,
		element_ids: &[ElementId],
	) -> Result<Vec<Option<Arc<DiskAnnNode>>>> {
		let index = self.cache_index();
		let frame = context.frame();
		let mut nodes = vec![None; element_ids.len()];
		let written = frame.map(|frame| frame.written_nodes_of(element_ids));
		let mut miss_positions = Vec::new();
		let mut miss_keys = Vec::new();
		for (pos, element_id) in element_ids.iter().copied().enumerate() {
			if let Some(own) = written.as_ref().and_then(|written| written[pos].clone()) {
				if let Some(frame) = frame {
					frame.stats.reads_overlay.fetch_add(1, Ordering::Relaxed);
				}
				nodes[pos] = Some(own);
			} else if let Some(node) = self.cache.get_node(index, element_id, &context.view) {
				nodes[pos] = Some(node);
			} else {
				miss_positions.push((pos, element_id));
				miss_keys.push(context.ikb.new_dn_key(element_id));
			}
		}
		if miss_keys.is_empty() {
			return Ok(nodes);
		}
		let view = match frame {
			Some(frame) => frame.view()?,
			None => None,
		};
		match (frame, view) {
			(Some(frame), Some(view)) => {
				let fetched: Vec<Option<DiskAnnNode>> =
					frame.read_through(&view, &miss_keys).await?;
				let fetched: Vec<(ElementId, usize, Arc<DiskAnnNode>)> = miss_positions
					.into_iter()
					.zip(fetched)
					.filter_map(|((pos, id), node)| Some((id, pos, Arc::new(node?))))
					.collect();
				if let Some(stamp) = view.publication() {
					frame.publish_clean_nodes(
						fetched.iter().map(|(id, _, node)| (*id, node)),
						|id, node| {
							self.cache.insert_shared_node(index, id, stamp, Arc::clone(node));
						},
					);
				}
				for (_, pos, node) in fetched {
					nodes[pos] = Some(node);
				}
			}
			(frame, _) => {
				if let Some(frame) = frame {
					frame.stats.reads_tx.fetch_add(1, Ordering::Relaxed);
				}
				let fetched: Vec<Option<DiskAnnNode>> =
					context.kv().get_many_key(miss_keys).await?;
				// A short answer would leave the trailing misses unread, silently.
				ensure!(
					fetched.len() == miss_positions.len(),
					"{} nodes read for {} requested",
					fetched.len(),
					miss_positions.len()
				);
				let publish = context.publication();
				for ((pos, element_id), node) in miss_positions.into_iter().zip(fetched) {
					if let Some(node) = node {
						nodes[pos] = Some(match publish {
							Some(stamp) => self.cache.insert_node(index, element_id, stamp, node),
							None => Arc::new(node),
						});
					}
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
		Ok(self.allocate_element_ids(context, 1).await?.start)
	}

	/// Reserves `count` consecutive element ids and persists the advanced counter once.
	///
	/// The range is the caller's alone for the life of the transaction. Ids are never reused, so a
	/// frame that does not commit leaves a gap.
	pub(super) async fn allocate_element_ids(
		&self,
		context: &DiskAnnProviderContext,
		count: u64,
	) -> Result<std::ops::Range<ElementId>> {
		let mut state = self.read_state_for_update(context).await?;
		let start = state.next_element_id;
		state.next_element_id = start.saturating_add(count);
		self.write_state(context, &state).await?;
		Ok(start..state.next_element_id)
	}

	/// Sets the entry point only when the graph has none.
	pub(super) async fn ensure_entry_point(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
	) -> Result<()> {
		let mut state = self.read_state_for_update(context).await?;
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
		let mut state = self.read_state_for_update(context).await?;
		state.enter_point = element_id;
		self.write_state(context, &state).await
	}

	/// Returns usable graph starting points for a **search**, repairing a deleted entry point
	/// with a fallback scan.
	///
	/// The stored entry point and its liveness come from the cache. That is sound *only* because
	/// the caller merely starts a traversal from the result: a stale entry point makes the greedy
	/// search begin somewhere worse, which costs recall in a structure that is approximate by
	/// construction.
	///
	/// A caller that **persists** the result must use [`Self::valid_starting_points_for_update`].
	/// Writing a value derived from the cache is the same defect as writing a cached state
	/// struct back — see rule 3 of the module-level
	/// [cache coherency invariant](self) — and it is easy to miss here, because the write happens
	/// one call further out rather than in this function.
	pub(super) async fn valid_starting_points(
		&self,
		context: &DiskAnnProviderContext,
	) -> ANNResult<Vec<ElementId>> {
		self.starting_points(context, false).await
	}

	/// Returns usable graph starting points for a caller that will persist the result.
	///
	/// `DiskAnnGraph::remove` hands its result straight to
	/// [`Self::set_entry_point`]. If the stored entry point came from the cache, and its
	/// liveness check came from the cache too, a process whose cache predates another process's
	/// commit will persist a tombstoned entry point over the live one that process committed —
	/// leaving every later search and insert starting from a deleted node. Reading both through
	/// the transaction removes that; the fallback scan below is already transactional.
	pub(super) async fn valid_starting_points_for_update(
		&self,
		context: &DiskAnnProviderContext,
	) -> ANNResult<Vec<ElementId>> {
		self.starting_points(context, true).await
	}

	/// Shared body. `for_update` selects transactional reads for the stored entry point and its
	/// liveness; the fallback scan reads the transaction either way.
	async fn starting_points(
		&self,
		context: &DiskAnnProviderContext,
		for_update: bool,
	) -> ANNResult<Vec<ElementId>> {
		let state = if for_update {
			self.read_state_for_update(context).await
		} else {
			self.read_state(context).await
		}
		.map_err(|e| context.ann_err(e))?;
		// The entry point first, then the other start points, each only while live. A failure to
		// read one is the search's failure, not a sign that it is gone: falling through to the
		// scan would hide it, and scan the whole range.
		let candidates: Vec<ElementId> = state
			.enter_point
			.into_iter()
			.chain(state.start_points.iter().copied().filter(|p| Some(*p) != state.enter_point))
			.collect();
		let live = self.live_of(context, &candidates, for_update).await?;
		if !live.is_empty() {
			return Ok(live);
		}
		// The stored entry point may have been deleted. Scan for a replacement and populate the
		// element cache with any entries touched during the fallback scan.
		let rng = context.ikb.new_de_range().map_err(|e| context.ann_err(e))?;
		let kv = context.kv();
		let mut cursor = kv.open_vals_cursor(rng).await.map_err(|e| context.ann_err(e))?;
		let publish = context.publication();
		loop {
			let batch = cursor
				.next_batch(surrealdb_kvs::consts::NORMAL_BATCH_SIZE)
				.await
				.map_err(|e| context.ann_err(e))?;
			if batch.is_empty() {
				break;
			}
			for (key, value) in &batch {
				let key: DiskannElementKey<'_> =
					storekey::decode_borrow(key).map_err(|e| context.ann_err(e.into()))?;
				let element =
					DiskAnnElement::kv_decode_value(value, ()).map_err(|e| context.ann_err(e))?;
				let deleted = element.deleted;
				// Only publish non-tombstoned entries into the shared cache, and only where the
				// context may publish at all (see `DiskAnnProviderContext::publication`).
				// Tombstones aren't useful for any subsequent reader.
				if !deleted && let Some(stamp) = publish {
					self.cache.insert_element(self.cache_index(), key.element_id, stamp, element);
				}
				if !deleted {
					// On a writable tx (i.e. compaction or a record-write path), persist
					// the discovered entry point so subsequent searches don't have to
					// re-scan `!de:*` from the dangling stored value. On a read-only tx
					// (KNN search) we can't write — the next compaction will fix it. A
					// `for_update` caller persists the result itself, so it is not
					// written here as well.
					//
					// Persistence is a pure hint: a transient failure here (e.g. a
					// concurrent `delc` on the state key) shouldn't fail the whole
					// search, since the discovered entry point is already correct in
					// memory and the next compaction will rediscover it.
					if !for_update
						&& context.tx.writeable()
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

	/// The elements of `candidates` that exist and are not tombstoned, in input order, read as
	/// [`Self::entry_point_is_live`] reads one.
	async fn live_of(
		&self,
		context: &DiskAnnProviderContext,
		candidates: &[ElementId],
		for_update: bool,
	) -> ANNResult<Vec<ElementId>> {
		if candidates.is_empty() {
			return Ok(Vec::new());
		}
		if !for_update {
			let elements =
				self.read_elements(context, candidates).await.map_err(|e| context.ann_err(e))?;
			return Ok(candidates
				.iter()
				.zip(elements)
				.filter(|(_, e)| e.as_ref().is_some_and(|e| !e.deleted))
				.map(|(id, _)| *id)
				.collect());
		}
		let mut live = Vec::with_capacity(candidates.len());
		for id in candidates {
			if self.entry_point_is_live(context, *id, true).await? {
				live.push(*id);
			}
		}
		Ok(live)
	}

	/// Replaces the graph's start points other than its entry point.
	pub(super) async fn set_start_points(
		&self,
		context: &DiskAnnProviderContext,
		start_points: Vec<ElementId>,
	) -> Result<()> {
		let mut state = self.read_state_for_update(context).await?;
		state.start_points = start_points;
		self.write_state(context, &state).await
	}

	/// Keeps the graph's start points usable once `deleted` has been tombstoned.
	///
	/// `deleted`, and any other start point no longer live, leaves the set. When the entry point
	/// is gone, the first live start point takes its place, and only a graph with none left falls
	/// back to scanning for a live element. When `deleted` was a start point, a live neighbour of
	/// it joins the set in its place, so deleting start points one at a time does not shrink the
	/// set while the graph holds elements near them. The deletion of any other element, while the
	/// entry point is live, leaves the state as it is and writes nothing.
	///
	/// Everything it reads to decide what it persists — the state, the liveness of each start
	/// point and of each candidate, and the deleted element's adjacency list — comes from the
	/// transaction (rule 3 of the module-level [cache coherency invariant](self)).
	pub(super) async fn retire_start_point(
		&self,
		context: &DiskAnnProviderContext,
		deleted: ElementId,
	) -> Result<()> {
		let mut state = self.read_state_for_update(context).await?;
		let was_start = state.enter_point == Some(deleted) || state.start_points.contains(&deleted);
		if !was_start
			&& let Some(entry) = state.enter_point
			&& self.entry_point_is_live(context, entry, true).await?
		{
			return Ok(());
		}
		let before = (state.enter_point, state.start_points.clone());
		let others: Vec<ElementId> =
			state.start_points.iter().copied().filter(|p| *p != deleted).collect();
		let mut kept = self.live_of(context, &others, true).await?;
		let entry_live = match state.enter_point {
			Some(entry) if entry != deleted => {
				self.entry_point_is_live(context, entry, true).await?
			}
			_ => false,
		};
		if !entry_live {
			state.enter_point = (!kept.is_empty()).then(|| kept.remove(0));
		}
		if was_start && let Some(entry) = state.enter_point {
			let node: Option<DiskAnnNode> =
				context.kv().get_key(&context.ikb.new_dn_key(deleted)).await?;
			let neighbours = node.map(|node| node.neighbors).unwrap_or_default();
			for candidate in neighbours {
				if candidate != entry
					&& candidate != deleted
					&& !kept.contains(&candidate)
					&& self.entry_point_is_live(context, candidate, true).await?
				{
					kept.push(candidate);
					break;
				}
			}
		}
		state.start_points = kept;
		if state.enter_point.is_some() {
			if (state.enter_point, &state.start_points) == (before.0, &before.1) {
				return Ok(());
			}
			return self.write_state(context, &state).await;
		}
		// No start point is left: scan for a live element to enter the graph from.
		self.write_state(context, &state).await?;
		let next = self.valid_starting_points_for_update(context).await?.into_iter().next();
		self.set_entry_point(context, next).await
	}

	/// Whether an element exists and is not tombstoned. A missing element answers `false`; a
	/// failure to read it is an error.
	///
	/// `for_update` reads `!de` through the transaction instead of the cache, for a caller whose
	/// answer will be persisted: a cached element can report a node live after another process
	/// has committed its deletion.
	async fn entry_point_is_live(
		&self,
		context: &DiskAnnProviderContext,
		element_id: ElementId,
		for_update: bool,
	) -> ANNResult<bool> {
		if !for_update {
			let element = self
				.read_elements(context, std::slice::from_ref(&element_id))
				.await
				.map_err(|e| context.ann_err(e))?
				.pop()
				.flatten();
			return Ok(element.is_some_and(|e| !e.deleted));
		}
		let element: Option<DiskAnnElement> = context
			.kv()
			.get_key(&context.ikb.new_de_key(element_id))
			.await
			.map_err(|e| context.ann_err(e))?;
		Ok(element.is_some_and(|e| !e.deleted))
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
		#[cfg(test)]
		if let Some(faults) = context.frame().and_then(|f| f.faults.as_ref()) {
			let fail = *faults.fail_set_element.lock().unwrap_or_else(|p| p.into_inner());
			if fail == Some(*id) {
				return Err(context.ann_err(anyhow::anyhow!("injected set_element failure")));
			}
			if let Some(park) = &faults.park_set_element {
				park.park().await;
			}
		}
		let key = context.ikb.new_de_key(*id);
		let element = DiskAnnElement {
			vector: T::serialized_from_slice(element),
			deleted: false,
		};
		context.kv().set_key(&key, &element).await.map_err(|e| context.ann_err(e))?;
		self.write_element_through(context, *id, element);
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
			.map_err(|e| context.ann_err(e))?
			.pop()
			.flatten()
		else {
			return Ok(());
		};
		let mut element = (*element).clone();
		element.deleted = true;
		context.kv().set_key(&key, &element).await.map_err(|e| context.ann_err(e))?;
		self.write_element_through(context, *gid, element);
		Ok(())
	}

	async fn release(
		&self,
		context: &Self::Context,
		id: Self::InternalId,
	) -> Result<(), Self::Error> {
		context.kv().del_key(&context.ikb.new_de_key(id)).await.map_err(|e| context.ann_err(e))?;
		// Release removes the element record entirely, so remove any cached vector/status too.
		let evict = || self.cache.remove_element(self.cache_index(), id);
		match context.frame() {
			Some(frame) => frame.wrote_element(id, None, evict),
			None => evict(),
		}
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
			.map_err(|e| context.ann_err(e))?
			.pop()
			.flatten()
		else {
			return Err(ANNError::message(format!("DiskANN element {id} is missing")));
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

/// Search accessor used by DiskANN greedy graph search.
///
/// Holds the query distance computer, preprocessed once from the query vector at
/// construction, plus a reusable scratch buffer for decoding persisted vectors into typed
/// slices during distance evaluation.
pub(super) struct DiskAnnSearchAccessor<'a, T: DiskAnnVectorElement> {
	/// Provider that owns KV/cache access.
	provider: &'a DiskAnnProvider,
	/// Transaction/key context for the current DiskANN operation.
	context: &'a DiskAnnProviderContext,
	/// Query distance computer, preprocessed from the search/insert query vector.
	query: <T as VectorRepr>::QueryDistance,
	/// Reusable scratch buffer for converting persisted vectors into typed slices.
	buffer: Box<[T]>,
	/// Whether a test injected a failure into the search for this query.
	#[cfg(test)]
	fail_search: bool,
}

impl<'a, T> DiskAnnSearchAccessor<'a, T>
where
	T: DiskAnnVectorElement,
{
	pub(super) fn new(
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
		query: &[T],
	) -> Result<Self, ANNError> {
		if query.len() != provider.dim {
			return Err(vector_len_mismatch(query.len(), provider.dim));
		}
		#[cfg(test)]
		let fail_search = context.frame().and_then(|f| f.faults.as_ref()).is_some_and(|f| {
			let target = f.fail_search_for.lock().unwrap_or_else(|p| p.into_inner());
			target.as_ref() == Some(&T::serialized_from_slice(query))
		});
		Ok(Self {
			provider,
			context,
			query: T::query_distance(query, provider.metric),
			buffer: vec![T::default(); provider.dim].into_boxed_slice(),
			#[cfg(test)]
			fail_search,
		})
	}

	/// Decodes `element` into the scratch buffer and returns its distance to the query.
	fn distance_to(&mut self, element: &DiskAnnElement) -> ANNResult<f32> {
		T::copy_from_serialized(&element.vector, &mut self.buffer)?;
		Ok(self.query.evaluate_similarity(&self.buffer))
	}
}

impl<T: DiskAnnVectorElement> HasId for DiskAnnSearchAccessor<'_, T> {
	type Id = ElementId;
}

impl<T> SearchAccessor for DiskAnnSearchAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	async fn starting_points(&self) -> ANNResult<Vec<ElementId>> {
		self.provider.valid_starting_points(self.context).await
	}

	async fn start_point_distances<F>(&mut self, mut f: F) -> ANNResult<()>
	where
		F: FnMut(ElementId, f32) + Send,
	{
		#[cfg(test)]
		if self.fail_search {
			return Err(ANNError::message("injected candidate-generation failure"));
		}
		let ids = self.provider.valid_starting_points(self.context).await?;
		let elements = self
			.provider
			.read_elements(self.context, &ids)
			.await
			.map_err(|e| self.context.ann_err(e))?;
		for (id, element) in ids.into_iter().zip(elements) {
			let Some(element) = element else {
				return Err(ANNError::message(format!("DiskANN start point {id} is missing")));
			};
			let distance = self.distance_to(&element)?;
			f(id, distance);
		}
		Ok(())
	}

	/// Expands one search beam using batched adjacency-list reads and batched candidate reads.
	///
	/// The default DiskANN implementation walks neighbors and fetches vectors one node at a
	/// time. This override reads every adjacency list for `ids` in a single `getm`, keeps the
	/// candidates the predicate admits (`eval_mut` both filters and deduplicates), then reads
	/// their vectors in a second `getm` before computing query distances.
	async fn expand_beam<Itr, P, F>(
		&mut self,
		ids: Itr,
		mut pred: P,
		mut on_neighbors: F,
	) -> ANNResult<()>
	where
		Itr: Iterator<Item = ElementId> + Send,
		P: HybridPredicate<ElementId> + Send + Sync,
		F: FnMut(ElementId, f32) + Send,
	{
		let ids: Vec<_> = ids.collect();
		let nodes = self
			.provider
			.read_nodes(self.context, &ids)
			.await
			.map_err(|e| self.context.ann_err(e))?;
		// A well-behaved predicate never re-admits an id it has already yielded, so filtering
		// the neighbors with `eval_mut` also deduplicates the surviving candidates.
		let mut candidates = Vec::new();
		for node in nodes.iter().flatten() {
			for &id in &node.neighbors {
				if pred.eval_mut(&id) {
					candidates.push(id);
				}
			}
		}
		let elements = self
			.provider
			.read_elements(self.context, &candidates)
			.await
			.map_err(|e| self.context.ann_err(e))?;
		for (id, element) in candidates.into_iter().zip(elements) {
			// A candidate whose element was compacted away mid-search is dropped: the
			// `expand_beam` contract tolerates skipping candidates in exceptional cases.
			if let Some(element) = element {
				let distance = self.distance_to(&element)?;
				on_neighbors(id, distance);
			}
		}
		Ok(())
	}
}

/// Working-set cache of decoded vectors, reused across a prune operation's rounds.
type DiskAnnWorkingSet<T> = workingset::Map<ElementId, Box<[T]>, workingset::map::Ref<[T]>>;
/// Read-only view over a [`DiskAnnWorkingSet`], returned to the prune algorithm by `fill`.
type DiskAnnWorkingSetView<'a, T> =
	workingset::map::View<'a, ElementId, Box<[T]>, workingset::map::Ref<[T]>>;

/// Prune accessor used by DiskANN index construction (insert and in-place delete).
///
/// Owns a bounded working-set cache that [`PruneAccessor::fill`] populates from batched KV
/// reads, and hands the prune algorithm a view over it together with an element-to-element
/// distance computer.
pub(super) struct DiskAnnPruneAccessor<'a, T: DiskAnnVectorElement> {
	/// Provider that owns KV/cache access.
	provider: &'a DiskAnnProvider,
	/// Transaction/key context for the current DiskANN operation.
	context: &'a DiskAnnProviderContext,
	/// Bounded cache of decoded vectors, reused across the operation's prune rounds.
	set: DiskAnnWorkingSet<T>,
	/// What the lists this accessor writes are: back-edges for the accessors upstream's batched
	/// insert builds from its seed, which its back-edge phase alone writes through.
	writes: NodeWrite,
}

impl<'a, T> DiskAnnPruneAccessor<'a, T>
where
	T: DiskAnnVectorElement,
{
	fn new(
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
		set: DiskAnnWorkingSet<T>,
		writes: NodeWrite,
	) -> Self {
		Self {
			provider,
			context,
			set,
			writes,
		}
	}
}

impl<T: DiskAnnVectorElement> HasId for DiskAnnPruneAccessor<'_, T> {
	type Id = ElementId;
}

impl<T> PruneAccessor for DiskAnnPruneAccessor<'_, T>
where
	T: DiskAnnVectorElement,
{
	type Neighbors<'b>
		= DiskAnnNeighborAccessor<'b>
	where
		Self: 'b;
	type ElementRef<'b> = &'b [T];
	type View<'b>
		= DiskAnnWorkingSetView<'b, T>
	where
		Self: 'b;
	type Distance<'b>
		= <T as VectorRepr>::Distance
	where
		Self: 'b;

	fn neighbors(&mut self) -> Self::Neighbors<'_> {
		DiskAnnNeighborAccessor {
			provider: self.provider,
			context: self.context,
			writes: self.writes,
		}
	}

	/// Makes the vectors for `itr` available in the returned view alongside a distance computer.
	///
	/// The upstream `Map::fill` closure is synchronous, but SurrealDB reads vectors from the
	/// transaction asynchronously. So this first batches the ids the working set does not
	/// already cache into a single `getm`, decodes them, then feeds the prefetched vectors to
	/// `Map::fill` through a synchronous closure. Deleted and missing elements are skipped,
	/// which the prune algorithm tolerates.
	async fn fill<Itr>(&mut self, itr: Itr) -> ANNResult<(Self::View<'_>, Self::Distance<'_>)>
	where
		Itr: ExactSizeIterator<Item = ElementId> + Clone + Send + Sync,
	{
		let missing: Vec<ElementId> = itr.clone().filter(|id| !self.set.contains_key(id)).collect();
		let fetched = self
			.provider
			.read_elements(self.context, &missing)
			.await
			.map_err(|e| self.context.ann_err(e))?;
		let mut decoded: HashMap<ElementId, Box<[T]>> = HashMap::with_capacity(missing.len());
		for (id, element) in missing.into_iter().zip(fetched) {
			if let Some(element) = element
				&& !element.deleted
			{
				let mut buffer = vec![T::default(); self.provider.dim].into_boxed_slice();
				T::copy_from_serialized(&element.vector, &mut buffer)?;
				decoded.insert(id, buffer);
			}
		}
		let distance = T::distance(self.provider.metric, Some(self.provider.dim));
		let view =
			self.set.fill(itr, |id| -> ANNResult<Option<Box<[T]>>> { Ok(decoded.remove(&id)) })?;
		Ok((view, distance))
	}
}

/// Accessor for reading and mutating DiskANN adjacency lists.
#[derive(Clone, Copy)]
pub(super) struct DiskAnnNeighborAccessor<'a> {
	/// Provider that owns adjacency-list KV/cache access.
	provider: &'a DiskAnnProvider,
	/// Transaction/key context for the current DiskANN operation.
	context: &'a DiskAnnProviderContext,
	/// What the lists this accessor writes are.
	writes: NodeWrite,
}

impl HasId for DiskAnnNeighborAccessor<'_> {
	type Id = ElementId;
}

impl NeighborAccessor for DiskAnnNeighborAccessor<'_> {
	async fn get_neighbors(
		&mut self,
		id: ElementId,
		neighbors: &mut AdjacencyList<ElementId>,
	) -> ANNResult<()> {
		if let Some(node) = self
			.provider
			.read_nodes(self.context, &[id])
			.await
			.map_err(|e| self.context.ann_err(e))?
			.into_iter()
			.next()
			.flatten()
		{
			neighbors.overwrite_trusted(&node.neighbors);
		} else {
			neighbors.clear();
		}
		Ok(())
	}
}

impl NeighborAccessorMut for DiskAnnNeighborAccessor<'_> {
	async fn set_neighbors(&mut self, id: ElementId, neighbors: &[ElementId]) -> ANNResult<()> {
		let node = DiskAnnNode {
			neighbors: neighbors.to_vec(),
		};
		self.provider
			.write_node(self.context, id, node, self.writes)
			.await
			.map_err(|e| self.context.ann_err(e))
	}

	async fn append_vector(&mut self, id: ElementId, neighbors: &[ElementId]) -> ANNResult<()> {
		#[cfg(test)]
		if let Some(faults) = self.context.frame().and_then(|f| f.faults.as_ref()) {
			if faults.panic_append.swap(false, Ordering::AcqRel) {
				panic!("injected panic in append_vector");
			}
			let fail = faults.fail_append.lock().unwrap_or_else(|p| p.into_inner()).take();
			if let Some(fail) = fail {
				return Err(self.context.ann_err(fail()));
			}
		}
		let mut node: DiskAnnNode = self
			.provider
			.read_nodes(self.context, &[id])
			.await
			.map_err(|e| self.context.ann_err(e))?
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
		self.provider
			.write_node(self.context, id, node, self.writes)
			.await
			.map_err(|e| self.context.ann_err(e))
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

impl<'a, T> SearchStrategy<'a, DiskAnnProvider, &'a [T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type SearchAccessorError = ANNError;
	type SearchAccessor = DiskAnnSearchAccessor<'a, T>;

	fn search_accessor(
		&'a self,
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
		query: &'a [T],
	) -> Result<DiskAnnSearchAccessor<'a, T>, ANNError> {
		DiskAnnSearchAccessor::new(provider, context, query)
	}
}

impl<'a, T> DefaultPostProcessor<'a, DiskAnnProvider, &'a [T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	default_post_processor!(CopyIds);
}

impl<T> PruneStrategy<DiskAnnProvider> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type PruneAccessor<'a> = DiskAnnPruneAccessor<'a, T>;
	type PruneAccessorError = ANNError;

	fn prune_accessor<'a>(
		&'a self,
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
		capacity: usize,
	) -> Result<Self::PruneAccessor<'a>, ANNError> {
		let set = workingset::map::Builder::new(workingset::map::Capacity::Default).build(capacity);
		Ok(DiskAnnPruneAccessor::new(provider, context, set, NodeWrite::Own))
	}
}

impl<'a, T> InsertStrategy<'a, DiskAnnProvider, &'a [T]> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	type PruneStrategy = Self;

	fn prune_strategy(&self) -> Self::PruneStrategy {
		self.clone()
	}
}

impl<T> MultiInsertStrategy<DiskAnnProvider, Matrix<T>> for DiskAnnStrategy<T>
where
	T: DiskAnnVectorElement,
{
	/// A working-set builder carrying the batch as a zero-copy overlay, so a prune candidate that
	/// is a member of the batch is read from memory, never from KV.
	type Seed = workingset::map::Builder<ElementId, workingset::map::Ref<[T]>>;
	type FinishError = Infallible;
	type PruneStrategy = Self;
	type InsertStrategy = Self;

	fn insert_strategy(&self) -> Self {
		self.clone()
	}

	fn finish<Itr>(
		&self,
		_: &DiskAnnProvider,
		_: &DiskAnnProviderContext,
		batch: &Arc<Matrix<T>>,
		ids: Itr,
	) -> impl std::future::Future<Output = Result<Self::Seed, Infallible>> + Send
	where
		Itr: ExactSizeIterator<Item = ElementId> + Send,
	{
		use workingset::map::{Builder, Capacity, Overlay};
		std::future::ready(Ok(Builder::new(Capacity::Default)
			.with_overlay(Overlay::from_batch(Arc::clone(batch), ids))))
	}

	fn seeded_prune_accessor<'a>(
		&'a self,
		provider: &'a DiskAnnProvider,
		context: &'a DiskAnnProviderContext,
		seed: &'a Self::Seed,
		capacity: usize,
	) -> ANNResult<DiskAnnPruneAccessor<'a, T>> {
		// Upstream builds a seeded accessor for candidate generation, which writes nothing, and
		// for the back-edge phase, which writes only back-edges.
		Ok(DiskAnnPruneAccessor::new(
			provider,
			context,
			seed.clone().build(capacity),
			NodeWrite::BackEdge,
		))
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionType;

	use super::*;
	use crate::catalog::{DatabaseId, IndexId, NamespaceId, TableId};
	use crate::test_env::TestIndexStore;
	use crate::trees::diskann::cache::DiskAnnCache;

	fn ikb() -> IndexKeyBase {
		IndexKeyBase::new(NamespaceId(1), DatabaseId(2), "tb".into(), IndexId(3))
	}

	async fn provider_and_context() -> Result<(DiskAnnProvider, DiskAnnProviderContext)> {
		let ds = TestIndexStore::new().await;
		let tx = Arc::new(ds.transaction(TransactionType::Write).await?);
		let ikb = ikb();
		let provider =
			DiskAnnProvider::new(ikb, TableId(4), 0, DiskAnnCache::new(1024 * 1024), 2, Metric::L2);
		let context = provider.context(tx, None, None);
		Ok((provider, context))
	}

	#[tokio::test]
	async fn diskann_provider_batches_element_reads_and_reports_missing() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set_key(
				&context.ikb.new_de_key(1),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![1.0, 2.0]),
					deleted: false,
				},
			)
			.await?;
		context
			.tx
			.set_key(
				&context.ikb.new_de_key(2),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![3.0, 4.0]),
					deleted: false,
				},
			)
			.await?;

		// A single `getm`-backed read returns present elements in input order and `None` for a
		// missing id.
		let elements = provider.read_elements(&context, &[1, 2, 3]).await?;
		assert_eq!(
			elements[0].as_ref().map(|e| e.vector.clone()),
			Some(SerializedVector::F32(vec![1.0, 2.0]))
		);
		assert_eq!(
			elements[1].as_ref().map(|e| e.vector.clone()),
			Some(SerializedVector::F32(vec![3.0, 4.0]))
		);
		assert!(elements[2].is_none());
		// Provider reads through a writable tx must not publish into the shared cache: `tx.get`
		// returns the writer's own buffered, possibly uncommitted, writes, and the cache is
		// process-wide.
		assert!(provider.cache.get_element(provider.cache_index(), 1, &context.view).is_none());
		assert!(provider.cache.get_element(provider.cache_index(), 2, &context.view).is_none());
		Ok(())
	}

	#[tokio::test]
	async fn diskann_provider_batches_nodes_and_updates_cache_on_mutation() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set_key(
				&context.ikb.new_dn_key(1),
				&DiskAnnNode {
					neighbors: vec![2, 3],
				},
			)
			.await?;
		context
			.tx
			.set_key(
				&context.ikb.new_dn_key(2),
				&DiskAnnNode {
					neighbors: vec![4],
				},
			)
			.await?;

		let nodes = provider.read_nodes(&context, &[1, 2]).await?;
		assert_eq!(nodes[0].as_ref().unwrap().neighbors, vec![2, 3]);
		assert_eq!(nodes[1].as_ref().unwrap().neighbors, vec![4]);

		// Outside a compaction frame a mutation writes nothing through: the transaction may never
		// commit, so what the cache held for the node is evicted instead.
		let mut neighbor_accessor = DiskAnnNeighborAccessor {
			provider: &provider,
			context: &context,
			writes: NodeWrite::Own,
		};
		neighbor_accessor.set_neighbors(1, &[8, 9]).await.unwrap();
		let reader_at =
			|generation| provider.context(Arc::clone(&context.tx), generation, None).view;
		assert!(provider.cache.get_node(provider.cache_index(), 1, &reader_at(None)).is_none());
		assert!(provider.cache.get_node(provider.cache_index(), 1, &reader_at(Some(1))).is_none());

		// Inside one it writes through, at the generation the frame commits at and no other.
		let frame = provider.frame_context(Arc::clone(&context.tx), None, None);
		let mut neighbor_accessor = DiskAnnNeighborAccessor {
			provider: &provider,
			context: &frame,
			writes: NodeWrite::Own,
		};
		neighbor_accessor.set_neighbors(1, &[8, 9]).await.unwrap();
		let cached =
			provider.cache.get_node(provider.cache_index(), 1, &reader_at(Some(1))).unwrap();
		assert_eq!(cached.neighbors, vec![8, 9]);
		assert!(provider.cache.get_node(provider.cache_index(), 1, &frame.view).is_some());
		assert!(provider.cache.get_node(provider.cache_index(), 1, &reader_at(None)).is_none());
		Ok(())
	}

	#[tokio::test]
	async fn diskann_provider_batch_vectors_omits_deleted_elements() -> Result<()> {
		let (provider, context) = provider_and_context().await?;
		context
			.tx
			.set_key(
				&context.ikb.new_de_key(1),
				&DiskAnnElement {
					vector: SerializedVector::F32(vec![1.0, 2.0]),
					deleted: false,
				},
			)
			.await?;
		context
			.tx
			.set_key(
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
