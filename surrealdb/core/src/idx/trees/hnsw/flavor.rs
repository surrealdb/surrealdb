use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{DatabaseDefinition, HnswParams};
use crate::idx::IndexKeyBase;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::{AHashSet, ArraySet};
use crate::idx::trees::hnsw::docs::{HnswDocs, VecDocs};
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
	pub(super) fn new(ibk: IndexKeyBase, p: &HnswParams) -> Result<Self> {
		let res = match p.m {
			1..=4 => match p.m0 {
				1..=8 => Self::H5_9(Hnsw::<ArraySet<9>, ArraySet<5>>::new(ibk, p)?),
				9..=16 => Self::H5_17(Hnsw::<ArraySet<17>, ArraySet<5>>::new(ibk, p)?),
				17..=24 => Self::H5_25(Hnsw::<ArraySet<25>, ArraySet<5>>::new(ibk, p)?),
				_ => Self::H5set(Hnsw::<AHashSet, ArraySet<5>>::new(ibk, p)?),
			},
			5..=8 => match p.m0 {
				1..=16 => Self::H9_17(Hnsw::<ArraySet<17>, ArraySet<9>>::new(ibk, p)?),
				17..=24 => Self::H9_25(Hnsw::<ArraySet<25>, ArraySet<9>>::new(ibk, p)?),
				_ => Self::H9set(Hnsw::<AHashSet, ArraySet<9>>::new(ibk, p)?),
			},
			9..=12 => match p.m0 {
				17..=24 => Self::H13_25(Hnsw::<ArraySet<25>, ArraySet<13>>::new(ibk, p)?),
				_ => Self::H13set(Hnsw::<AHashSet, ArraySet<13>>::new(ibk, p)?),
			},
			13..=16 => Self::H17set(Hnsw::<AHashSet, ArraySet<17>>::new(ibk, p)?),
			17..=20 => Self::H21set(Hnsw::<AHashSet, ArraySet<21>>::new(ibk, p)?),
			21..=24 => Self::H25set(Hnsw::<AHashSet, ArraySet<25>>::new(ibk, p)?),
			25..=28 => Self::H29set(Hnsw::<AHashSet, ArraySet<29>>::new(ibk, p)?),
			_ => Self::Hset(Hnsw::<AHashSet, AHashSet>::new(ibk, p)?),
		};
		Ok(res)
	}

	pub(super) async fn check_state(&mut self, tx: &Transaction) -> Result<()> {
		match self {
			HnswFlavor::H5_9(h) => h.check_state(tx).await,
			HnswFlavor::H5_17(h) => h.check_state(tx).await,
			HnswFlavor::H5_25(h) => h.check_state(tx).await,
			HnswFlavor::H5set(h) => h.check_state(tx).await,
			HnswFlavor::H9_17(h) => h.check_state(tx).await,
			HnswFlavor::H9_25(h) => h.check_state(tx).await,
			HnswFlavor::H9set(h) => h.check_state(tx).await,
			HnswFlavor::H13_25(h) => h.check_state(tx).await,
			HnswFlavor::H13set(h) => h.check_state(tx).await,
			HnswFlavor::H17set(h) => h.check_state(tx).await,
			HnswFlavor::H21set(h) => h.check_state(tx).await,
			HnswFlavor::H25set(h) => h.check_state(tx).await,
			HnswFlavor::H29set(h) => h.check_state(tx).await,
			HnswFlavor::Hset(h) => h.check_state(tx).await,
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
	#[expect(clippy::too_many_arguments)]
	pub(super) async fn knn_search_checked(
		&self,
		db: &DatabaseDefinition,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		hnsw_docs: &HnswDocs,
		vec_docs: &VecDocs,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>> {
		match self {
			HnswFlavor::H5_9(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5_17(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5_25(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9_17(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9_25(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H13_25(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H13set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H17set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H21set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H25set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H29set(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::Hset(h) => {
				h.knn_search_checked(db, tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
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
	pub(super) fn check_hnsw_properties(&self, expected_count: usize) {
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
