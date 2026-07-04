use std::sync::Arc;

use crate::dbs::Options;
use crate::exec::permission::CachedTableSelect;
use crate::expr::Cond;

#[cfg(diskann)]
pub(crate) mod diskann;
pub mod dynamicset;
mod graph;
pub mod hnsw;
pub(in crate::idx) mod knn;
pub mod store;
pub mod vector;

/// A pushed-down WHERE condition for an ANN KNN search, together with how the
/// truthy-document filter gates each candidate on the table's SELECT
/// permission before evaluating that condition.
pub(crate) struct KnnCondFilter<'a> {
	/// Query options used when evaluating the condition (and, on the legacy
	/// path, the permission).
	pub(crate) opt: &'a Options,
	/// The condition to evaluate against each candidate record.
	pub(crate) cond: Arc<Cond>,
	/// Pre-resolved SELECT-permission gate. `Some` on the streaming-executor
	/// path, carrying the scan operator's own physical permission; `None` on
	/// the legacy path, where the filter resolves the catalog permission
	/// lazily from the transaction and evaluates it via `Expr::compute`.
	pub(crate) select_gate: Option<CachedTableSelect>,
}
