use anyhow::Result;
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;

use crate::IndexKeyBase;
use crate::catalog::{HnswParams, TableId};
use crate::env::IndexEnv;
use crate::trees::dynamicset::{AHashSet, ArraySet};
use crate::trees::hnsw::cache::VectorCache;
use crate::trees::hnsw::filter::HnswTruthyDocumentFilter;
use crate::trees::hnsw::index::HnswContext;
use crate::trees::hnsw::{ElementId, Hnsw, HnswSearch};
use crate::trees::vector::{SharedVector, Vector};

/// Evaluates `$body` with `$h` bound to the concrete [`Hnsw`] inside a
/// flavor, whichever variant it is. `$flavor` is a reference (shared or
/// mutable), and `$h` takes the same kind.
macro_rules! with_hnsw {
	($flavor:expr, $h:ident => $body:expr) => {
		match $flavor {
			$crate::trees::hnsw::flavor::HnswFlavor::H5_9($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H5_17($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H5_25($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H5set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H9_17($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H9_25($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H9set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H13_25($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H13set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H17_33($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H21_41($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H25_49($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H29_57($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H17set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H21set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H25set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::H29set($h) => $body,
			$crate::trees::hnsw::flavor::HnswFlavor::Hset($h) => $body,
		}
	};
}
pub(super) use with_hnsw;

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
	H17_33(Hnsw<ArraySet<33>, ArraySet<17>>),
	H21_41(Hnsw<ArraySet<41>, ArraySet<21>>),
	H25_49(Hnsw<ArraySet<49>, ArraySet<25>>),
	H29_57(Hnsw<ArraySet<57>, ArraySet<29>>),
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
		format_version: u16,
	) -> Result<Self> {
		let res = match p.m {
			1..=4 => match p.m0 {
				1..=8 => Self::H5_9(Hnsw::<ArraySet<9>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				9..=16 => Self::H5_17(Hnsw::<ArraySet<17>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				17..=24 => Self::H5_25(Hnsw::<ArraySet<25>, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H5set(Hnsw::<AHashSet, ArraySet<5>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			5..=8 => match p.m0 {
				1..=16 => Self::H9_17(Hnsw::<ArraySet<17>, ArraySet<9>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				17..=24 => Self::H9_25(Hnsw::<ArraySet<25>, ArraySet<9>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H9set(Hnsw::<AHashSet, ArraySet<9>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			9..=12 => match p.m0 {
				17..=24 => Self::H13_25(Hnsw::<ArraySet<25>, ArraySet<13>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H13set(Hnsw::<AHashSet, ArraySet<13>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			13..=16 => match p.m0 {
				1..=32 => Self::H17_33(Hnsw::<ArraySet<33>, ArraySet<17>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H17set(Hnsw::<AHashSet, ArraySet<17>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			17..=20 => match p.m0 {
				1..=40 => Self::H21_41(Hnsw::<ArraySet<41>, ArraySet<21>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H21set(Hnsw::<AHashSet, ArraySet<21>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			21..=24 => match p.m0 {
				1..=48 => Self::H25_49(Hnsw::<ArraySet<49>, ArraySet<25>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H25set(Hnsw::<AHashSet, ArraySet<25>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			25..=28 => match p.m0 {
				1..=56 => Self::H29_57(Hnsw::<ArraySet<57>, ArraySet<29>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
				_ => Self::H29set(Hnsw::<AHashSet, ArraySet<29>>::new(
					table_id,
					ibk,
					p,
					vector_cache,
					format_version,
				)?),
			},
			_ => Self::Hset(Hnsw::<AHashSet, AHashSet>::new(
				table_id,
				ibk,
				p,
				vector_cache,
				format_version,
			)?),
		};
		Ok(res)
	}

	/// Returns `true` if [`check_state`](Self::check_state) would mutate the
	/// in-memory graph. Safe to call under a shared (read) lock.
	pub(super) async fn needs_state_reload(&self, env: &dyn IndexEnv) -> Result<bool> {
		with_hnsw!(self, h => h.needs_state_reload(env).await)
	}

	/// The variant's name, for tests asserting which flavour parameters select.
	#[cfg(test)]
	pub(super) fn variant(&self) -> &'static str {
		match self {
			HnswFlavor::H5_9(_) => "H5_9",
			HnswFlavor::H5_17(_) => "H5_17",
			HnswFlavor::H5_25(_) => "H5_25",
			HnswFlavor::H5set(_) => "H5set",
			HnswFlavor::H9_17(_) => "H9_17",
			HnswFlavor::H9_25(_) => "H9_25",
			HnswFlavor::H9set(_) => "H9set",
			HnswFlavor::H13_25(_) => "H13_25",
			HnswFlavor::H13set(_) => "H13set",
			HnswFlavor::H17_33(_) => "H17_33",
			HnswFlavor::H21_41(_) => "H21_41",
			HnswFlavor::H25_49(_) => "H25_49",
			HnswFlavor::H29_57(_) => "H29_57",
			HnswFlavor::H17set(_) => "H17set",
			HnswFlavor::H21set(_) => "H21set",
			HnswFlavor::H25set(_) => "H25set",
			HnswFlavor::H29set(_) => "H29set",
			HnswFlavor::Hset(_) => "Hset",
		}
	}

	/// Reloads every layer from the key-value store, whatever its version.
	pub(super) async fn reload(&mut self, env: &dyn IndexEnv) -> Result<()> {
		with_hnsw!(self, h => h.sync_state(env, true).await)
	}

	/// Draws the level a new element is inserted up to.
	pub(super) fn draw_level(&mut self) -> usize {
		with_hnsw!(self, h => h.get_random_level())
	}

	/// The base layer's node count and neighbour bound.
	pub(super) fn shape(&self) -> crate::trees::AnnGraphShape {
		with_hnsw!(self, h => h.shape())
	}

	/// The graph's committed element vectors, for reads outside the graph.
	pub(super) fn elements(&self) -> &crate::trees::hnsw::elements::HnswElements {
		with_hnsw!(self, h => &h.elements)
	}

	/// Loads and synchronizes the in-memory graph state from the key-value store.
	pub(super) async fn check_state(&mut self, env: &dyn IndexEnv) -> Result<()> {
		with_hnsw!(self, h => h.check_state(env).await)
	}

	/// Inserts a vector into the graph and returns its assigned element ID.
	pub(super) async fn insert(
		&mut self,
		ctx: &HnswContext<'_>,
		q_pt: Vector,
	) -> Result<ElementId> {
		with_hnsw!(self, h => h.insert(ctx, q_pt).await)
	}
	/// Removes an element from the graph. Returns `true` if the element was found and removed.
	pub(super) async fn remove(&mut self, ctx: &HnswContext<'_>, e_id: ElementId) -> Result<bool> {
		with_hnsw!(self, h => h.remove(ctx, e_id).await)
	}
	/// Performs a k-nearest neighbor search on the graph.
	pub(super) async fn knn_search(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		pending_docs: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		with_hnsw!(self, h => h.knn_search(ctx, search, pending_docs).await)
	}
	/// Performs a k-nearest neighbor search with a conditional document filter
	/// and/or an allow-list bitmap gating candidate admission (#548).
	pub(super) async fn knn_search_with_filter(
		&self,
		ctx: &HnswContext<'_>,
		search: &HnswSearch,
		filter: &mut Option<HnswTruthyDocumentFilter<'_>>,
		pending_docs: Option<&RoaringTreemap>,
		allow_list: Option<&RoaringTreemap>,
	) -> Result<Vec<(f64, ElementId)>> {
		with_hnsw!(self, h => h.knn_search_with_filter(ctx, search, filter, pending_docs, allow_list).await)
	}
	/// Retrieves the vectors of several element IDs, in input order.
	pub(super) async fn get_vectors(
		&self,
		tx: &Transaction,
		e_ids: &[ElementId],
	) -> Result<Vec<Option<SharedVector>>> {
		with_hnsw!(self, h => h.get_vectors(tx, e_ids).await)
	}

	/// Retrieves the vector associated with the given element ID.
	#[cfg(test)]
	pub(super) async fn get_vector(
		&self,
		tx: &Transaction,
		e_id: &ElementId,
	) -> Result<Option<SharedVector>> {
		with_hnsw!(self, h => h.get_vector(tx, e_id).await)
	}
	#[cfg(test)]
	pub(super) async fn check_hnsw_properties(&self, expected_count: usize) {
		with_hnsw!(self, h => h.check_hnsw_properties(expected_count).await)
	}
}
