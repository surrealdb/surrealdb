use crate::err::Error;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::idx::trees::dynamicset::{AHashSet, ArraySet};
use crate::idx::trees::hnsw::docs::HnswDocs;
use crate::idx::trees::hnsw::docs::VecDocs;
use crate::idx::trees::hnsw::{ElementId, Hnsw, HnswSearch};
use crate::idx::trees::vector::SharedVector;
use crate::idx::IndexKeyBase;
use crate::kvs::Transaction;
use crate::sql::index::HnswParams;
use reblessive::tree::Stk;

pub(super) type ASet<const N: usize> = ArraySet<ElementId, N>;
pub(super) type HSet = AHashSet<ElementId>;

pub(super) enum HnswFlavor {
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
	pub(super) fn new(ibk: IndexKeyBase, p: &HnswParams) -> Self {
		match p.m {
			1..=4 => match p.m0 {
				1..=8 => Self::H5_9(Hnsw::<ASet<9>, ASet<5>>::new(ibk, p)),
				9..=16 => Self::H5_17(Hnsw::<ASet<17>, ASet<5>>::new(ibk, p)),
				17..=24 => Self::H5_25(Hnsw::<ASet<25>, ASet<5>>::new(ibk, p)),
				_ => Self::H5set(Hnsw::<HSet, ASet<5>>::new(ibk, p)),
			},
			5..=8 => match p.m0 {
				1..=16 => Self::H9_17(Hnsw::<ASet<17>, ASet<9>>::new(ibk, p)),
				17..=24 => Self::H9_25(Hnsw::<ASet<25>, ASet<9>>::new(ibk, p)),
				_ => Self::H9set(Hnsw::<HSet, ASet<9>>::new(ibk, p)),
			},
			9..=12 => match p.m0 {
				17..=24 => Self::H13_25(Hnsw::<ASet<25>, ASet<13>>::new(ibk, p)),
				_ => Self::H13set(Hnsw::<HSet, ASet<13>>::new(ibk, p)),
			},
			13..=16 => Self::H17set(Hnsw::<HSet, ASet<17>>::new(ibk, p)),
			17..=20 => Self::H21set(Hnsw::<HSet, ASet<21>>::new(ibk, p)),
			21..=24 => Self::H25set(Hnsw::<HSet, ASet<25>>::new(ibk, p)),
			25..=28 => Self::H29set(Hnsw::<HSet, ASet<29>>::new(ibk, p)),
			_ => Self::Hset(Hnsw::<HSet, HSet>::new(ibk, p)),
		}
	}

	pub(super) async fn check_state(&mut self, tx: &Transaction) -> Result<(), Error> {
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

	pub(super) fn insert(&mut self, q_pt: SharedVector) -> ElementId {
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
	pub(super) fn remove(&mut self, e_id: ElementId) -> bool {
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
	pub(super) fn knn_search(&self, search: &HnswSearch) -> Vec<(f64, ElementId)> {
		match self {
			HnswFlavor::H5_9(h) => h.knn_search(search),
			HnswFlavor::H5_17(h) => h.knn_search(search),
			HnswFlavor::H5_25(h) => h.knn_search(search),
			HnswFlavor::H5set(h) => h.knn_search(search),
			HnswFlavor::H9_17(h) => h.knn_search(search),
			HnswFlavor::H9_25(h) => h.knn_search(search),
			HnswFlavor::H9set(h) => h.knn_search(search),
			HnswFlavor::H13_25(h) => h.knn_search(search),
			HnswFlavor::H13set(h) => h.knn_search(search),
			HnswFlavor::H17set(h) => h.knn_search(search),
			HnswFlavor::H21set(h) => h.knn_search(search),
			HnswFlavor::H25set(h) => h.knn_search(search),
			HnswFlavor::H29set(h) => h.knn_search(search),
			HnswFlavor::Hset(h) => h.knn_search(search),
		}
	}
	pub(super) async fn knn_search_checked(
		&self,
		tx: &Transaction,
		stk: &mut Stk,
		search: &HnswSearch,
		hnsw_docs: &HnswDocs,
		vec_docs: &VecDocs,
		chk: &mut HnswConditionChecker<'_>,
	) -> Result<Vec<(f64, ElementId)>, Error> {
		match self {
			HnswFlavor::H5_9(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5_17(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5_25(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H5set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9_17(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9_25(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H9set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H13_25(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H13set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H17set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H21set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H25set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::H29set(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
			HnswFlavor::Hset(h) => {
				h.knn_search_checked(tx, stk, search, hnsw_docs, vec_docs, chk).await
			}
		}
	}
	pub(super) fn get_vector(&self, e_id: &ElementId) -> Option<&SharedVector> {
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
