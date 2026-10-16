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
