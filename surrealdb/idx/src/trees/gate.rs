//! Per-candidate seams for the ANN truthy-document filters.
//!
//! The HNSW and DiskANN filters evaluate a caller-supplied WHERE condition
//! against candidate records inside the search. Each candidate must first pass
//! the table's SELECT permission, otherwise the condition observes records the
//! caller cannot see and the result count, ordering and timing leak their
//! field values.
//!
//! Both halves need the execution environment — a context, the statement's
//! options, a cursor document — and that lives above this layer. So both are
//! declared here as traits and implemented above: [`TableSelectGate`] for the
//! permission, [`CandidateCondition`] for the condition. Each implementation
//! *holds* the environment; only the candidate crosses the boundary.
//!
//! They stay two traits, applied in that order by the filters, because the
//! order is a security boundary. A single trait doing both would move the
//! ordering into the implementations, where two of them could drift.
//!
//! Two executors drive the filters and resolve the permission differently: the
//! legacy path resolves the catalog permission from the transaction, while the
//! streaming executor has already resolved a physical permission for the
//! surrounding scan and passes that down. Neither is named here.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use reblessive::tree::Stk;

use crate::catalog::Record;
use crate::val::RecordId;

/// A boxed future returned by the per-candidate seams in this module.
///
/// Boxes at the trait boundary because both seams are held as trait objects.
/// Deliberately not `Send`: each takes the caller's `Stk`, and a future
/// holding one cannot be. Nothing needs it to be — the ANN search runs inside
/// a heap-allocated `TreeStack`, which erases auto-traits, so the `Send`
/// stream that drives the streaming executor is unaffected.
pub type BoxGateFut<'a> = Pin<Box<dyn Future<Output = anyhow::Result<bool>> + 'a>>;

/// A SELECT permission that has already been resolved against an execution
/// context, ready to be checked per candidate document.
///
/// Implemented by each executor, which owns both the resolved permission and
/// the context it evaluates against. Declared here so the ANN filters can
/// apply it without naming either.
///
/// `Send + Sync` so a filter carrying one can be held across an await by the
/// streaming executor's `Send` stream.
pub trait TableSelectGate: Send + Sync {
	/// The verdict for every candidate, when the resolved permission grants or
	/// denies the whole table outright. `None` when the permission is a
	/// predicate and each candidate has to go through [`Self::allows_doc`].
	///
	/// The ANN filters consult this first so the common unconditional case
	/// costs neither a boxed future nor a virtual expression evaluation per
	/// candidate.
	fn allows_every_doc(&self) -> Option<bool>;

	/// Returns `true` when the candidate `record`, stored under `rid`, is
	/// visible to the current session.
	fn allows_doc<'a>(
		&'a self,
		stk: &'a mut Stk,
		rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a>;
}

/// The WHERE condition pushed into an ANN search, evaluated per candidate.
///
/// Implemented by the layer that can evaluate an expression against a record,
/// holding the environment that evaluation needs. Declared here so the ANN
/// filters can apply the condition without naming the evaluator.
///
/// Applied only to candidates the [`TableSelectGate`] has already admitted.
pub trait CandidateCondition: Send + Sync {
	/// Returns `true` when the candidate `record`, stored under `rid`,
	/// satisfies the condition.
	fn matches<'a>(
		&'a self,
		stk: &'a mut Stk,
		rid: &'a Arc<RecordId>,
		record: &'a Arc<Record>,
	) -> BoxGateFut<'a>;
}

/// Counts the candidates an ANN truthy-document filter fetches and evaluates
/// inside the search, so the driving executor can report that cost.
///
/// Implemented by the layer that owns those metrics. Declared here, with the
/// other per-candidate seams, so the filters can count without naming it.
pub trait CandidateFetchCounter: Send + Sync {
	/// Record one candidate fetched and evaluated in-traversal.
	///
	/// Called once per filter-cache miss: a verdict served from the filter's
	/// query-local cache involves no new fetch and is not counted.
	fn record_fetch(&self);
}

/// How an ANN truthy-document filter gates each candidate on the table's
/// SELECT permission. Resolved once when the filter is built and reused for
/// every candidate; check each candidate via
/// [`check_cached_table_select_for_doc`].
pub enum CachedTableSelect<'a> {
	/// Permission checks are bypassed (auth disabled / privileged session).
	Skip,
	/// Every candidate is checked against the gate the executor supplied. The
	/// streaming executor passes the same permission that filters the fetched
	/// batch after the search, so the in-search and post-search checks cannot
	/// disagree.
	Gate(Arc<dyn TableSelectGate + 'a>),
}

/// Check a resolved [`CachedTableSelect`] against one candidate record.
pub(crate) async fn check_cached_table_select_for_doc(
	stk: &mut Stk,
	cached: &CachedTableSelect<'_>,
	rid: &Arc<RecordId>,
	record: &Arc<Record>,
) -> anyhow::Result<bool> {
	match cached {
		CachedTableSelect::Skip => Ok(true),
		// The candidate record carries its canonical `id` (spliced in from the
		// storage key on decode), so id-referencing permissions see the same
		// document here as in the post-search batch check.
		CachedTableSelect::Gate(gate) => match gate.allows_every_doc() {
			Some(allowed) => Ok(allowed),
			None => gate.allows_doc(stk, rid, record).await,
		},
	}
}
