//! The result of applying an HNSW compaction plan.

/// What applying a compaction plan did.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HnswCompactionOutcome {
	/// Nothing was applied: the plan was empty, another compactor advanced the
	/// generation first, or a captured pending changed before the write.
	Skipped,
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
		!matches!(self, Self::Skipped)
	}
}
