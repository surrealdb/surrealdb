//! The HNSW index of a build without the `vector-index` feature.
//!
//! It keeps the API of the graph-backed index so the index stores, the index
//! operations and the executor compile unchanged, but no value of it can
//! exist: loading an HNSW index, or reading its pending updates for
//! compaction, fails with an error naming the missing feature. Stored HNSW
//! definitions and keys still decode.

use std::collections::VecDeque;
use std::convert::Infallible;

use anyhow::{Result, bail};
use roaring::RoaringTreemap;
use surrealdb_datastore::Transaction;

use crate::IndexKeyBase;
use crate::catalog::{HnswParams, TableId};
use crate::env::IndexEnv;
use crate::error::Error;
pub use crate::trees::hnsw::HNSW_COMPACTION_MAX_PENDING_KEYS;
use crate::trees::hnsw::cache::VectorCache;
pub use crate::trees::hnsw::outcome::HnswCompactionOutcome;
use crate::trees::{KnnCondFilter, KnnIteratorResult};
use crate::val::{Number, RecordIdKey, Value};

/// The error every attempt to use an HNSW index fails with.
fn disabled() -> anyhow::Error {
	anyhow::Error::new(Error::FeatureDisabled {
		feature: "vector-index",
		what: "HNSW indexes",
	})
}

/// A compaction plan that cannot be constructed.
pub struct HnswCompactionPlan(Infallible);

impl HnswCompactionPlan {
	/// Unreachable, as no value of this type exists.
	pub fn has_work(&self) -> bool {
		match self.0 {}
	}

	/// Unreachable, as no value of this type exists.
	pub fn has_more(&self) -> bool {
		match self.0 {}
	}
}

/// An HNSW index that cannot be constructed.
pub struct HnswIndex(Infallible);

impl HnswIndex {
	/// Unreachable, as no value of this type exists.
	pub(crate) fn format_version(&self) -> u16 {
		match self.0 {}
	}

	/// Always fails: this build has no HNSW support.
	pub(crate) async fn new(
		_vector_cache: VectorCache,
		_tx: &Transaction,
		_ikb: IndexKeyBase,
		_tb: TableId,
		_p: &HnswParams,
		_format_version: u16,
	) -> Result<Self> {
		bail!(disabled())
	}

	/// Unreachable, as no value of this type exists.
	pub(crate) async fn index(
		&self,
		_env: &dyn IndexEnv,
		_id: &RecordIdKey,
		_old_values: Option<Vec<Value>>,
		_new_values: Option<Vec<Value>>,
	) -> Result<()> {
		match self.0 {}
	}

	/// Always fails: this build cannot apply HNSW pending updates.
	pub(crate) async fn prepare_compaction(
		_env: &dyn IndexEnv,
		_ikb: &IndexKeyBase,
	) -> Result<HnswCompactionPlan> {
		bail!(disabled())
	}

	/// Unreachable, as no value of this type exists.
	pub(crate) async fn apply_compaction(
		&self,
		_env: &dyn IndexEnv,
		_plan: HnswCompactionPlan,
	) -> Result<HnswCompactionOutcome> {
		match self.0 {}
	}

	/// Unreachable, as no value of this type exists.
	pub async fn check_state(&self, _env: &dyn IndexEnv) -> Result<()> {
		match self.0 {}
	}

	/// Unreachable, as no value of this type exists.
	#[allow(clippy::too_many_arguments)]
	pub async fn knn_search(
		&self,
		_env: &dyn IndexEnv,
		_pt: &[Number],
		_k: usize,
		_ef: usize,
		_cond_filter: Option<KnnCondFilter<'_>>,
		_allow_list: Option<&RoaringTreemap>,
	) -> Result<VecDeque<KnnIteratorResult>> {
		match self.0 {}
	}

	/// Unreachable, as no value of this type exists.
	pub async fn graph_shape(&self, _env: &dyn IndexEnv) -> Result<crate::trees::AnnGraphShape> {
		match self.0 {}
	}

	/// Unreachable, as no value of this type exists.
	pub async fn exact_knn_search(
		&self,
		_env: &dyn IndexEnv,
		_pt: &[Number],
		_k: usize,
		_allow_list: &RoaringTreemap,
	) -> Result<Option<VecDeque<KnnIteratorResult>>> {
		match self.0 {}
	}
}
