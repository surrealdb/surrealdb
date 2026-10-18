//! The per-loop verdicts a placement decision reads.
//!
//! A loop site is judged once, by analysis, and the verdict is a value of these types. The
//! physical layer only narrows a verdict: a loop whose mode is [`LoopMode::Serial`] never fans
//! out, and a loop whose mode is [`LoopMode::Independent`] may still be placed serially.

use crate::exec::logical::pipelines::ChainRefusal;

/// Whether the iterations of a loop may run in any order and at once.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopMode {
	/// The compute slices of the iterations may run in any order and at once, and the apply
	/// residue, if any, runs in order.
	Independent,
	/// One iteration at a time on the coordinator, for the first reason that held.
	Serial(LoopRefusal),
}

/// The first condition of independence that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LoopRefusal {
	/// The source cannot be split into ranges or ordinals.
	Unsplittable,
	/// An iteration carries state into the next.
	Carried,
	/// An iteration may leave the loop in order: `BREAK`, `RETURN`, a decisive element, or an
	/// exit to an enclosing loop.
	OrderedExit,
	/// The compute slice is not row-local.
	Effect(ChainRefusal),
	/// Something in the body makes the loop's effects unknowable or order-dependent.
	Barrier(BarrierCause),
	/// The sink has no combine.
	NoCombine,
	/// A write loop whose body writes a record of the table its source reads, at a key that is not
	/// the element's own, triggered writes included: a serial run's source may read what an
	/// earlier iteration rewrote.
	SourceWritten,
	/// A write loop over a stream that may repeat an identity, or whose write key is not the
	/// stream's key path: two iterations may write one key.
	MayRepeatWrite,
}

/// Why a loop body is a barrier: the loop's effects cannot be judged or ordered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BarrierCause {
	/// A catalog change point.
	CatalogChange,
	/// `USE`.
	Use,
	/// A session write.
	SessionWrite,
	/// `SLEEP`.
	Sleep,
	/// A body whose effects are opaque or unknown.
	Opaque,
	/// Async work queued by an event.
	AsyncEvent,
	/// A body peeled off as the first iteration.
	PeeledFirst,
	/// A write to a table undefined at the plan's catalog version, which defines it implicitly.
	ImplicitTable,
	/// A dynamic write target no provenance fact pins: its stage bundle is a unit loaded at run
	/// time.
	UnpinnedDynamicTarget,
}

/// Whether a loop's apply phase exists.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ApplyCut {
	/// The body holds no write: there is no apply residue.
	None,
	/// The body holds a write, triggered writes included: the residue the compute slice leaves
	/// behind runs in iteration order.
	Ordered,
}

/// Whether an iteration may leave its loop, and why.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Exit {
	None,
	Ordered(ExitCause),
}

/// What may end a loop before its source is exhausted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ExitCause {
	/// The loop's own `BREAK`.
	Break,
	/// A `RETURN` whose target lies outside the loop body.
	Return,
	/// The sink's decisive element: `any`, `all`, `find`.
	Decisive,
	/// A `BREAK` or `CONTINUE` targeting an enclosing loop.
	Outer,
}

/// How a loop's source reaches its body.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SourceDelivery {
	/// The source streams into the body.
	Stream,
	/// The source is read whole first, so every source error precedes every body error.
	ReadWhole,
}
