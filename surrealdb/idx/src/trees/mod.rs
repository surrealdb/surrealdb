use std::sync::Arc;

use crate::catalog::Record;
use crate::trees::gate::{CachedTableSelect, CandidateCondition, CandidateFetchCounter};
use crate::val::RecordId;

#[cfg(diskann)]
pub mod diskann;
pub mod dynamicset;
pub mod gate;
mod graph;
pub mod hnsw;
pub(crate) mod knn;
pub mod store;
pub mod vector;

/// For earch KNN result we have the RecordID, the distance (f64), and an optional record.
/// Optimisation: The optional record is present if a filter has checked if the record is truthy has
/// been used
pub type KnnIteratorResult = (Arc<RecordId>, f64, Option<Arc<Record>>);

/// A pushed-down WHERE condition for an ANN KNN search, together with how the
/// truthy-document filter gates each candidate on the table's SELECT
/// permission before evaluating that condition.
///
/// Both halves are supplied by the executor that drives the search, which
/// holds the environment they are evaluated against.
pub struct KnnCondFilter<'a> {
	/// Resolved SELECT-permission gate, applied to a candidate before the
	/// condition sees it.
	pub select_gate: CachedTableSelect<'a>,
	/// The condition to evaluate against each admitted candidate record.
	///
	/// `None` runs the filter in permission-only mode: an admitted candidate is
	/// truthy by definition. Used when the search's whole WHERE is already
	/// satisfied by an allow-list bitmap, but a per-record SELECT permission
	/// still has to be enforced per candidate so hidden rows never consume
	/// top-K slots.
	pub cond: Option<Arc<dyn CandidateCondition + 'a>>,
	/// Counter for the candidates the filter fetches and evaluates
	/// in-traversal. `None` when the driving executor reports no metrics.
	pub metrics: Option<Arc<dyn CandidateFetchCounter>>,
}
