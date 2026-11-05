//! The error every IR entry point returns.

use thiserror::Error;

use super::core::verify::VerifyError;
use super::iface::ids::OpId;
use crate::exec::vm::payload::Refusal;

/// An invariant of the IR was broken, or the request writes a construct the language refuses.
/// Every variant except [`IrError::Rejected`] and [`IrError::Unsupported`] maps to an internal,
/// uncovered-statement outcome in the VM. None of them maps to a user-facing query error, because
/// a well-formed request cannot reach one. `Rejected` is the request's own fault and maps to the
/// refusal it carries. `Unsupported` maps to the planner's unsupported-statement outcome.
///
/// Bounded drivers (greedy rewriting, conversion, folding) report that they stopped through their
/// own report types, never through this error.
#[derive(Debug, Error)]
pub(crate) enum IrError {
	/// An entry point that is still a stub with no implementation. `what` names the
	/// `<module>::<fn>` that answered, so a request that reaches it is traceable.
	#[error("IR entry point not implemented: {what}")]
	NotYet {
		what: &'static str,
	},
	/// An id that no longer names a live entity. Accessors answer `None` for a dangling id; this
	/// error is for the mutations that cannot proceed past one.
	#[error("stale IR id: {0}")]
	Stale(&'static str),
	/// A mutation whose ids are live but whose precondition does not hold for them: an arity that
	/// does not match, a value defined inside the op it would replace, a region moved under
	/// itself. The mutation changed nothing, and a pass may decline its rewrite on it.
	#[error("IR mutation refused: {0}")]
	Refused(&'static str),
	/// An op was erased while its results were still used.
	#[error("operation {0:?} still has uses")]
	HasUses(OpId),
	/// A replacement changed the tag of the op it replaced, which would invalidate the dialect's
	/// rules for every user of that op.
	#[error("operation {0:?} changed tag")]
	TagChanged(OpId),
	/// A container's table of interned entities is full: an id cannot name another entry.
	#[error("IR table full: {0}")]
	Capacity(&'static str),
	/// The container failed verification.
	#[error("IR verification failed: {0}")]
	Verify(VerifyError),
	/// The request writes a construct that a region of the language does not admit. It carries the
	/// refusal the statement is answered with, so a caller reaches the user-facing error through
	/// [`Refusal::error`].
	#[error("rejected by the language: {}", .0.error())]
	Rejected(Box<Refusal>),
	/// A table or definition the conversion read is not in the per-planning memo, and its name is
	/// marked pending. The caller awaits the memo's load and converts again.
	#[error("IR catalog answer not loaded: {what}")]
	NotLoaded {
		what: &'static str,
	},
	/// The catalog changed before this point of the compile, so the memo's definitions do not
	/// describe the catalog the code runs against. The statement is planned again when it is
	/// reached, after the schema statements before it have run.
	#[error("IR plan depends on a catalog change: {what}")]
	LateBind {
		what: &'static str,
	},
	/// A construct the request writes that the planner declines to cover, with the reason it
	/// states. The statement fails with the planner's unsupported-statement error, as a statement
	/// no lowering covers does.
	#[error("not covered: {reason}")]
	Unsupported {
		reason: String,
	},
	/// An invariant of the planning protocol broke: a state its caller rules out.
	#[error("IR invariant broken: {what}")]
	Invariant {
		what: &'static str,
	},
	/// A catalog read the planner made failed. `cause` is the failure's text, so the statement's
	/// error names what the store answered.
	#[error("{what}: the catalog could not be read: {cause}")]
	CatalogUnreadable {
		what: &'static str,
		cause: String,
	},
}
