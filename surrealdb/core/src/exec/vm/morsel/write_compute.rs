//! The compute half of a morsel write: the hand-off types a compute prefix returns and the read
//! view its workers read through.
//!
//! Computing reads and evaluates but never writes: a morsel's results depend on its own rows
//! alone, so morsels run on any worker in any order. The apply half
//! ([`write_apply`](super::write_apply)) is what makes them visible.
//!
//! A fanned-out write loop numbers its positions in delivery order with [`Pos`]. For each
//! position the compute prefix hands the coordinator one [`Candidate`]: the record in flight, the
//! registers the residue reads, and a [`Stop`] saying how the coordinator continues the position.
//! A candidate is charged to the fan-out memory account for the bytes it holds, so the candidates
//! held at once stay bounded.
//!
//! [`capture_write_view`] takes the read view once, at loop entry. It does not decide whether
//! the loop fans out; the plan does. A loop that gets no view runs every position on the
//! coordinator, in order.
#![allow(dead_code, reason = "the morsel runtime is the first caller")]

use std::sync::Arc;

use smallvec::SmallVec;
use surrealdb_kvs::Snapshot;

use crate::err::EngineError;
use crate::exec::ExecutionContext;
use crate::exec::memory::size::value_bytes;
use crate::exec::operators::write::row::WriteRow;
use crate::expr::ControlFlow;
use crate::val::Value;

/// A position of a fanned-out write loop in delivery order. One level: a write loop is never
/// nested inside another fanned-out loop, so a position is one counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Pos(pub(crate) u64);

impl Pos {
	/// The position after this one.
	pub(crate) fn next(self) -> Pos {
		Pos(self.0.saturating_add(1))
	}
}

/// Where a position's compute prefix stopped, which says how the coordinator continues it.
pub(crate) enum Stop {
	/// The prefix reached the cut: the coordinator resumes the record at the cut label.
	Cut,
	/// A stage skipped the record: the coordinator resumes at the skip target so the record's
	/// end updates its counters.
	Skip,
	/// The record took an arm that is not the cut path: the prefix wrote nothing and the
	/// coordinator runs the whole position itself, in order.
	OffPath,
	/// The prefix raised: the coordinator raises it at this position, after every earlier
	/// position has applied.
	Failed(ControlFlow),
}

/// What a compute prefix hands the coordinator for one position.
pub(crate) struct Candidate {
	pub(crate) pos: Pos,
	/// The record in flight after the prefix; `None` for `OffPath` and `Failed`.
	pub(crate) row: Option<Box<WriteRow>>,
	pub(crate) stop: Stop,
	/// Registers the prefix wrote that the residue reads, as `(register, value)`.
	pub(crate) live: SmallVec<[(u16, Value); 8]>,
	/// Bytes this candidate is charged to the fan-out memory account for.
	pub(crate) bytes: usize,
}

const _: () = {
	const fn assert_send<T: Send>() {}
	assert_send::<Candidate>();
};

impl Candidate {
	/// A candidate whose `bytes` is computed from what it holds: the row's `held_bytes`, the
	/// live values' `value_bytes`, and the candidate's own size.
	///
	/// A row is present exactly when `stop` is `Cut` or `Skip`. An inconsistent pair is a caller
	/// bug, answered with an internal error rather than a panic.
	pub(crate) fn new(
		pos: Pos,
		row: Option<Box<WriteRow>>,
		stop: Stop,
		live: SmallVec<[(u16, Value); 8]>,
	) -> anyhow::Result<Candidate> {
		let resumes = matches!(stop, Stop::Cut | Stop::Skip);
		if row.is_some() != resumes {
			return Err(anyhow::Error::new(EngineError::Internal(if resumes {
				"a resumable write candidate carries no record".to_owned()
			} else {
				"a write candidate that is not resumable carries a record".to_owned()
			})));
		}
		let held = row.as_deref().map_or(0, WriteRow::held_bytes);
		let bytes = live
			.iter()
			.map(|(_, value)| value_bytes(value))
			.fold(held.saturating_add(std::mem::size_of::<Candidate>()), usize::saturating_add);
		Ok(Candidate {
			pos,
			row,
			stop,
			live,
			bytes,
		})
	}

	/// Whether the coordinator resumes this candidate's record (`Cut`, `Skip`) rather than
	/// re-running the position (`OffPath`) or raising (`Failed`).
	pub(crate) fn resumes(&self) -> bool {
		matches!(self.stop, Stop::Cut | Stop::Skip)
	}
}

/// The read view a writing loop's workers read through, taken once at loop entry, before its
/// source is opened and before anything is applied: the transaction's state at that moment,
/// its earlier buffered writes included. `None` when the backend cannot give one now; the loop
/// then runs every position on the coordinator, in order. The plan decides fan-out, not this.
pub(crate) fn capture_write_view(ctx: &ExecutionContext) -> Option<Arc<dyn Snapshot>> {
	ctx.txn().snapshot()
}

#[cfg(test)]
mod tests {
	use smallvec::smallvec;

	use super::*;
	use crate::val::TableName;

	fn row() -> Box<WriteRow> {
		Box::new(WriteRow::for_insert(TableName::from("t"), None))
	}

	fn kilobyte() -> Value {
		Value::String("x".repeat(1024).into())
	}

	#[test]
	fn positions_order_by_counter() {
		assert!(Pos(1) < Pos(2));
		assert_eq!(Pos(1).next(), Pos(2));
	}

	#[test]
	fn candidate_bytes_count_row_and_live() {
		let held = row().held_bytes();
		let live: SmallVec<[(u16, Value); 8]> = smallvec![(0, kilobyte())];
		let candidate = Candidate::new(Pos(0), Some(row()), Stop::Cut, live).expect("consistent");
		assert!(candidate.bytes >= held + 1024);
	}

	#[test]
	fn candidate_rejects_a_row_on_off_path_and_failed() {
		let off = Candidate::new(Pos(0), Some(row()), Stop::OffPath, SmallVec::new());
		assert!(off.is_err());
		let failed = Candidate::new(
			Pos(1),
			Some(row()),
			Stop::Failed(ControlFlow::Err(anyhow::anyhow!("raised"))),
			SmallVec::new(),
		);
		assert!(failed.is_err());
	}

	#[test]
	fn candidate_requires_a_row_to_resume() {
		assert!(Candidate::new(Pos(0), None, Stop::Cut, SmallVec::new()).is_err());
		assert!(Candidate::new(Pos(0), None, Stop::Skip, SmallVec::new()).is_err());
	}

	#[test]
	fn resumes_only_cut_and_skip() {
		let resumes = |row: Option<Box<WriteRow>>, stop: Stop| {
			Candidate::new(Pos(0), row, stop, SmallVec::new()).expect("consistent").resumes()
		};
		assert!(resumes(Some(row()), Stop::Cut));
		assert!(resumes(Some(row()), Stop::Skip));
		assert!(!resumes(None, Stop::OffPath));
		assert!(!resumes(None, Stop::Failed(ControlFlow::Err(anyhow::anyhow!("raised")))));
	}

	/// The in-memory backend gives no view, so a loop over it runs on the coordinator.
	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn capture_write_view_gives_none_on_mem() {
		use crate::exec::operators::test_util::TestDb;
		use crate::kvs::TransactionType;

		let db = TestDb::new("").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		assert!(capture_write_view(&ctx).is_none());
	}
}
