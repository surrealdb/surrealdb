use anyhow::Result;
use reblessive::tree::Stk;
use roaring::RoaringTreemap;

use crate::catalog::{HnswParams, TableId};
use crate::ctx::FrozenContext;
use crate::idx::IndexKeyBase;
use crate::idx::trees::dynamicset::{AHashSet, ArraySet};
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::idx::trees::hnsw::index::HnswContext;
use crate::idx::trees::hnsw::{ElementId, Hnsw, HnswSearch};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::kvs::Transaction;

/// Type-erased dispatch enum for [`Hnsw`] instances with different neighbor set sizes.
///
/// Each variant is a concrete `Hnsw<L0, L>` parameterized with fixed-size
/// `ArraySet` or dynamic `AHashSet` neighbor sets, chosen at construction time
/// based on the `m` and `m0` HNSW parameters. This avoids dynamic dispatch
/// overhead while supporting a range of neighbor set capacities.
pub(super) enum HnswFlavor {
	H5_9(Hnsw<ArraySet<9>, ArraySet<5>>),
	H5_17(Hnsw<ArraySet<17>, ArraySet<5>>),
	H5_25(Hnsw<ArraySet<25>, ArraySet<5>>),
	H5set(Hnsw<AHashSet, ArraySet<5>>),
	H9_17(Hnsw<ArraySet<17>, ArraySet<9>>),
	H9_25(Hnsw<ArraySet<25>, ArraySet<9>>),
	H9set(Hnsw<AHashSet, ArraySet<9>>),
	H13_25(Hnsw<ArraySet<25>, ArraySet<13>>),
	H13set(Hnsw<AHashSet, ArraySet<13>>),
	H17set(Hnsw<AHashSet, ArraySet<17>>),
	H21set(Hnsw<AHashSet, ArraySet<21>>),
	H25set(Hnsw<AHashSet, ArraySet<25>>),
	H29set(Hnsw<AHashSet, ArraySet<29>>),
	Hset(Hnsw<AHashSet, AHashSet>),
}

impl HnswFlavor {
	/// Creates a new HNSW graph variant selected by the `m` and `m0` parameters.
	///
	/// Chooses the most efficient fixed-size `ArraySet` that can accommodate
	/// the requested number of connections per layer, falling back to a
	/// dynamic `AHashSet` for larger values.
	pub(super) fn new(
		table_id: TableId,
		ibk: IndexKeyBase,
		p: &HnswParams,
		vector_cache: VectorCache,
	) -> Result<Self> {
		let res = match p.m {
			1..=4 => match p.m0 {
				1..=8 => Self::H5_9(Hnsw::<ArraySet<9>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				9..=16 => Self::H5_17(Hnsw::<ArraySet<17>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				17..=24 => Self::H5_25(Hnsw::<ArraySet<25>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				_ => {
					Self::H5set(Hnsw::<AHashSet, ArraySet<5>>::new(table_id, ibk, p, vector_cache)?)
				}
			},
			5..=8 => match p.m0 {
				1..=16 => Self::H9_17(Hnsw::<ArraySet<17>, ArraySet<9>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				17..=24 => Self::H9_25(Hnsw::<ArraySet<25>, ArraySet<9>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				_ => {
					Self::H9set(Hnsw::<AHashSet, ArraySet<9>>::new(table_id, ibk, p, vector_cache)?)
				}
			},
			9..=12 => match p.m0 {
				17..=24 => Self::H13_25(Hnsw::<ArraySet<25>, ArraySet<13>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
				_ => Self::H13set(Hnsw::<AHashSet, ArraySet<13>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
				)?),
			},
			13..=16 => {
				Self::H17set(Hnsw::<AHashSet, ArraySet<17>>::new(table_id, ibk, p, vector_cache)?)
			}
			17..=20 => {
				Self::H21set(Hnsw::<AHashSet, ArraySet<21>>::new(table_id, ibk, p, vector_cache)?)
			}
			21..=24 => {
				Self::H25set(Hnsw::<AHashSet, ArraySet<25>>::new(table_id, ibk, p, vector_cache)?)
			}
			25..=28 => {
				Self::H29set(Hnsw::<AHashSet, ArraySet<29>>::new(table_id, ibk, p, vector_cache)?)
			}
			_ => Self::Hset(Hnsw::<AHashSet, AHashSet>::new(table_id, ibk, p, vector_cache)?),
		};
		Ok(res)
	}

	/// Loads and synchronizes the in-memory graph state from the key-value store.
	pub(super) async fn check_state(&mut self, ctx: &FrozenContext) -> Result<()> {
		match self {
			HnswFlavor::H5_9(h) => h.check_state(ctx).await,
			HnswFlavor::H5_17(h) => h.check_state(ctx).await,
			HnswFlavor::H5_25(h) => h.check_state(ctx).await,
			HnswFlavor::H5set(h) => h.check_state(ctx).await,
			HnswFlavor::H9_17(h) => h.check_state(ctx).await,
			HnswFlavor::H9_25(h) => h.check_state(ctx).await,
			HnswFlavor::H9set(h) => h.check_state(ctx).await,
			HnswFlavor::H13_25(h) => h.check_state(ctx).await,
			HnswFlavor::H13set(h) => h.check_state(ctx).await,
			HnswFlavor::H17set(h) => h.check_state(ctx).await,
			HnswFlavor::H21set(h) => h.check_state(ctx).await,
			HnswFlavor::H25set(h) => h.check_state(ctx).await,
			HnswFlavor::H29set(h) => h.check_state(ctx).await,
			HnswFlavor::Hset(h) => h.check_state(ctx).await,
		}
	}

	/// Inserts a vector into the graph and returns its assigned element ID.
	pub(super) async fn insert(
		&mut self,
		ctx: &HnswContext<'_>,
		q_pt: Vector,
	) -> Result<ElementId> {
		match self {
			HnswFlavor::H5_9(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H5_17(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H5_25(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H5set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H9_17(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H9_25(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H9set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H13_25(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H13set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H17set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H21set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H25set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::H29set(h) => h.insert(ctx, q_pt).await,
			HnswFlavor::Hset(h) => h.insert(ctx, q_pt).await,
		}
	}
	/// Removes an element from the graph. Returns `true` if the element was found and removed.
	pub(super) async fn remove(&mut self, ctx: &HnswContext<'_>, e_id: ElementId) -> Result<bool> {
		match self {
			HnswFlavor::H5_9(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H5_17(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H5_25(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H5set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H9_17(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H9_25(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H9set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H13_25(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H13set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H17set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H21set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H25set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::H29set(h) => h.remove(ctx, e_id).await,
			HnswFlavor::Hset(h) => h.remove(ctx, e_id).await,
		}
	}
	/// Performs a k-nearest neighbor search on the graph.
	pub(super) async fn knn_search(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		match self {
			HnswFlavor::H5_9(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H5_17(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H5_25(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H5set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H9_17(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H9_25(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H9set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H13_25(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H13set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H17set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H21set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H25set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::H29set(h) => h.knn_search(ctx, search, pending_docs).await,
			HnswFlavor::Hset(h) => h.knn_search(ctx, search, pending_docs).await,
		}
	}
	/// Performs a k-nearest neighbor search with a conditional document filter.
	pub(super) async fn knn_search_with_filter(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		stk: &mut Stk,
		filter: &mut HnswTruthyDocumentFilter<'_>,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		match self {
			HnswFlavor::H5_9(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H5_17(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H5_25(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H5set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H9_17(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H9_25(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H9set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H13_25(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H13set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H17set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H21set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H25set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::H29set(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
			HnswFlavor::Hset(h) => {
				h.knn_search_with_filter(ctx, search, stk, filter, pending_docs).await
			}
		}
	}
	/// Retrieves the vector associated with the given element ID.
	pub(super) async fn get_vector(
		&self,
		tx: &Transaction,
		e_id: &ElementId,
	) -> Result<Option<SharedVector>> {
		match self {
			HnswFlavor::H5_9(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H5_17(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H5_25(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H5set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H9_17(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H9_25(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H9set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H13_25(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H13set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H17set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H21set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H25set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::H29set(h) => h.get_vector(tx, e_id).await,
			HnswFlavor::Hset(h) => h.get_vector(tx, e_id).await,
		}
	}
	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		match self {
			HnswFlavor::H5_9(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H5_17(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H5_25(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H5set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H9_17(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H9_25(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H9set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H13_25(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H13set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H17set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H21set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H25set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::H29set(h) => h.check_hnsw_properties(expected_count).await,
			HnswFlavor::Hset(h) => h.check_hnsw_properties(expected_count).await,
		}
	}
}
