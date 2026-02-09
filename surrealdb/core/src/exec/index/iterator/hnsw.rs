//! HNSW (Hierarchical Navigable Small World) KNN iterator.
//!
//! Provides iteration over k-nearest neighbors from a vector index.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;

use crate::catalog::{DatabaseId, IndexDefinition, NamespaceId, Record};
use crate::ctx::FrozenContext;
use crate::val::{Number, RecordId};

/// Result from KNN search: record ID, distance, and optional pre-fetched record.
pub type KnnResult = (Arc<RecordId>, f64, Option<Arc<Record>>);

/// Iterator for KNN search results.
///
/// Returns results ordered by distance (nearest first).
pub struct KnnIterator {
	/// Pre-computed results queue
	results: VecDeque<KnnResult>,
}

impl KnnIterator {
	/// Create a new KNN iterator.
	///
	/// Note: This is a placeholder implementation. The actual implementation
	/// needs to:
	/// 1. Get the HNSW index from IndexStores
	/// 2. Execute the KNN search
	/// 3. Return results ordered by distance
	pub async fn new(
		_ctx: &FrozenContext,
		_ns: NamespaceId,
		_db: DatabaseId,
		_ix: &IndexDefinition,
		_vector: &[Number],
		_k: u32,
		_ef: u32,
	) -> Result<Self> {
		// TODO: Implement HNSW KNN search integration
		// This requires:
		// - Getting the HNSW index from IndexStores
		// - Executing the approximate nearest neighbor search
		// - Collecting and sorting results by distance
		Ok(Self {
			results: VecDeque::new(),
		})
	}

	/// Fetch the next batch of KNN results.
	///
	/// Results are returned in order of increasing distance.
	pub async fn next_batch(&mut self, limit: u32) -> Result<Vec<KnnResult>> {
		let limit = limit as usize;
		let mut batch = Vec::with_capacity(limit.min(self.results.len()));

		while batch.len() < limit {
			if let Some(result) = self.results.pop_front() {
				batch.push(result);
			} else {
				break;
			}
		}

		Ok(batch)
	}

	/// Get the remaining count of results.
	pub fn remaining(&self) -> usize {
		self.results.len()
	}
}
