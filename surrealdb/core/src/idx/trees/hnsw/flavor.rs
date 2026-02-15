use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{HnswParams, TableId};
use crate::ctx::Context;
use crate::idx::IndexKeyBase;
use crate::idx::trees::dynamicset::{AHashSet, ArraySet};
use crate::idx::trees::hnsw::cache::VectorCache;
use crate::idx::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::idx::trees::hnsw::index::HnswCheckedSearchContext;
use crate::idx::trees::hnsw::{ElementId, Hnsw, HnswSearch};
use crate::idx::trees::vector::{SharedVector, Vector};
use crate::kvs::Transaction;

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

	pub(super) async fn check_state(&mut self, ctx: &Context) -> Result<()> {
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

	pub(super) async fn insert(&mut self, tx: &Transaction, q_pt: Vector) -> Result<ElementId> {
		match self {
			HnswFlavor::H5_9(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H5_17(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H5_25(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H5set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H9_17(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H9_25(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H9set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H13_25(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H13set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H17set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H21set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H25set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::H29set(h) => h.insert(tx, q_pt).await,
			HnswFlavor::Hset(h) => h.insert(tx, q_pt).await,
		}
	}
	pub(super) async fn remove(&mut self, tx: &Transaction, e_id: ElementId) -> Result<bool> {
		match self {
			HnswFlavor::H5_9(h) => h.remove(tx, e_id).await,
			HnswFlavor::H5_17(h) => h.remove(tx, e_id).await,
			HnswFlavor::H5_25(h) => h.remove(tx, e_id).await,
			HnswFlavor::H5set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H9_17(h) => h.remove(tx, e_id).await,
			HnswFlavor::H9_25(h) => h.remove(tx, e_id).await,
			HnswFlavor::H9set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H13_25(h) => h.remove(tx, e_id).await,
			HnswFlavor::H13set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H17set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H21set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H25set(h) => h.remove(tx, e_id).await,
			HnswFlavor::H29set(h) => h.remove(tx, e_id).await,
			HnswFlavor::Hset(h) => h.remove(tx, e_id).await,
		}
	}
	pub(super) async fn knn_search(
		&self,
		tx: &Transaction,
		search: &HnswSearch,
	) -> Result<Vec<(f64, ElementId)>> {
		match self {
			HnswFlavor::H5_9(h) => h.knn_search(tx, search).await,
			HnswFlavor::H5_17(h) => h.knn_search(tx, search).await,
			HnswFlavor::H5_25(h) => h.knn_search(tx, search).await,
			HnswFlavor::H5set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H9_17(h) => h.knn_search(tx, search).await,
			HnswFlavor::H9_25(h) => h.knn_search(tx, search).await,
			HnswFlavor::H9set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H13_25(h) => h.knn_search(tx, search).await,
			HnswFlavor::H13set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H17set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H21set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H25set(h) => h.knn_search(tx, search).await,
			HnswFlavor::H29set(h) => h.knn_search(tx, search).await,
			HnswFlavor::Hset(h) => h.knn_search(tx, search).await,
		}
	}
	pub(super) async fn knn_search_checked(
		&self,
		search_ctx: &HnswCheckedSearchContext<'_>,
		stk: &mut Stk,
		filter: &mut HnswTruthyDocumentFilter,
	) -> Result<Vec<(f64, ElementId)>> {
		match self {
			HnswFlavor::H5_9(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H5_17(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H5_25(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H5set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H9_17(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H9_25(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H9set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H13_25(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H13set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H17set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H21set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H25set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::H29set(h) => h.knn_search_checked(search_ctx, stk, filter).await,
			HnswFlavor::Hset(h) => h.knn_search_checked(search_ctx, stk, filter).await,
		}
	}
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
