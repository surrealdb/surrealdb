//! The compute half of a morsel write: the hand-off types a compute prefix returns.
//!
//! Computing reads and evaluates but never writes: a morsel's results depend on its own rows
//! alone, so morsels run on any worker in any order. The statement's task applies them
//! ([`write_set`](super::write_set)), which is what makes them visible.
//!
//! A fanned-out write loop numbers its positions in delivery order with [`Pos`]. For each
//! position the compute prefix hands the coordinator one [`Candidate`]: the record in flight, its
//! write context, the registers the residue reads, and a [`Stop`] saying how the coordinator
//! continues the position.

use std::sync::Arc;

use smallvec::SmallVec;

use crate::err::EngineError;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::exec::operators::write::row::WriteRow;
use crate::exec::vm::ir_exec::PendingCandidate;
use crate::expr::ControlFlow;
use crate::val::Value;

/// A position of a fanned-out write loop in delivery order. One level: a write loop is never
/// nested inside another fanned-out loop, so a position is one counter.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct Pos(pub(crate) u64);

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
	/// The write context of the record's table, where the prefix resolved it before it stopped.
	pub(crate) ctx: Option<Arc<WriteCtx>>,
	pub(crate) stop: Stop,
	/// Registers the prefix wrote that the residue reads, as `(register, value)`.
	pub(crate) live: SmallVec<[(u16, Value); 8]>,
	/// For a `Failed` candidate, the record the prefix had taken up when it raised: the
	/// coordinator judges it before raising, as the serial run's gate judges it before the stages
	/// that raised. `None` for every other candidate, and for a failure raised before the prefix
	/// took a record up.
	pub(crate) failed_on: Option<crate::val::RecordId>,
}

const _: () = {
	const fn assert_send<T: Send>() {}
	assert_send::<Candidate>();
};

impl Candidate {
	/// A candidate for the position `pos`.
	///
	/// A row is present exactly when `stop` is `Cut` or `Skip`. An inconsistent pair is a caller
	/// bug, answered with an internal error rather than a panic.
	pub(crate) fn new(
		pos: Pos,
		row: Option<Box<WriteRow>>,
		ctx: Option<Arc<WriteCtx>>,
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
		Ok(Candidate {
			pos,
			row,
			ctx,
			stop,
			live,
			failed_on: None,
		})
	}

	/// The candidate for the position `pos` whose prefix raised `failure`, holding the record it
	/// had taken up when it raised, if any.
	pub(crate) fn failed(
		pos: Pos,
		failure: ControlFlow,
		failed_on: Option<crate::val::RecordId>,
	) -> Candidate {
		Candidate {
			pos,
			row: None,
			ctx: None,
			stop: Stop::Failed(failure),
			live: SmallVec::new(),
			failed_on,
		}
	}

	/// The candidate the coordinator holds for the next run of the write body, which resumes the
	/// record this one carries. An internal error for a candidate that carries none.
	pub(crate) fn into_pending(self) -> anyhow::Result<PendingCandidate> {
		let Some(row) = self.row else {
			return Err(anyhow::Error::new(EngineError::Internal(
				"a write candidate with no record was resumed".to_owned(),
			)));
		};
		Ok(PendingCandidate {
			row: *row,
			ctx: self.ctx,
			live: self.live,
			stop: self.stop,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::TableName;

	fn row() -> Box<WriteRow> {
		Box::new(WriteRow::for_insert(TableName::from("t"), None))
	}

	#[test]
	fn candidate_rejects_a_row_on_off_path_and_failed() {
		let off = Candidate::new(Pos(0), Some(row()), None, Stop::OffPath, SmallVec::new());
		assert!(off.is_err());
		let failed = Candidate::new(
			Pos(1),
			Some(row()),
			None,
			Stop::Failed(ControlFlow::Err(anyhow::anyhow!("raised"))),
			SmallVec::new(),
		);
		assert!(failed.is_err());
	}

	#[test]
	fn candidate_requires_a_row_to_resume() {
		assert!(Candidate::new(Pos(0), None, None, Stop::Cut, SmallVec::new()).is_err());
		assert!(Candidate::new(Pos(0), None, None, Stop::Skip, SmallVec::new()).is_err());
	}
}
