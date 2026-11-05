//! The result of applying an HNSW compaction plan.

/// What applying a compaction plan did.
///
/// The two outcomes that apply nothing are kept apart for the reason given on
/// [`CompactionApply`](crate::CompactionApply).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HnswCompactionOutcome {
	/// Nothing was applied, because the plan captured nothing or the index's
	/// table is gone.
	Empty,
	/// Nothing was applied to the graph, because the plan was stale, and what
	/// the plan wrote must not commit: another compactor advanced the
	/// generation first, or a captured pending changed before the write.
	LostRace,
	/// The plan was applied into the transaction only, and the resident graph
	/// takes it when that transaction commits. A transaction that does not
	/// commit leaves the resident graph as it stood, so nothing needs
	/// discarding.
	Staged,
	/// The plan was applied to the resident graph in place, as it was written
	/// into the transaction. A transaction that does not commit invalidates the
	/// resident graph for every holder of the index; a caller still discards
	/// the process-local index, so the next access starts from a new one.
	InPlace,
}

impl HnswCompactionOutcome {
	/// Whether the plan was applied, so the transaction has a batch to commit.
	pub fn applied(self) -> bool {
		matches!(self, Self::Staged | Self::InPlace)
	}
}

/// An error returned while applying an HNSW compaction plan, paired with
/// whether the resident graph had already begun in-place mutation.
#[derive(Debug)]
pub struct HnswCompactionError {
	/// The underlying apply error.
	pub error: anyhow::Error,
	/// `true` once `apply_in_place` has registered `InvalidateResident` and
	/// started mutating the resident graph; `false` when the failure happened
	/// before the resident graph was touched (catalog lookup, compaction guards,
	/// `apply_staged`, or `reconcile`).
	pub in_place: bool,
}

impl HnswCompactionError {
	/// Wraps an error that occurred before the resident graph was mutated in place.
	pub(crate) fn untouched(error: anyhow::Error) -> Self {
		Self {
			error,
			in_place: false,
		}
	}

	/// Wraps an error that occurred after `apply_in_place` began mutating the resident graph.
	pub(crate) fn in_place(error: anyhow::Error) -> Self {
		Self {
			error,
			in_place: true,
		}
	}
}

impl std::fmt::Display for HnswCompactionError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		std::fmt::Display::fmt(&self.error, f)
	}
}

impl std::error::Error for HnswCompactionError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		self.error.source()
	}
}
