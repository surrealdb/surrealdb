//! Authority frames and snapshots: the two identity-bearing contexts an op is evaluated in.
//!
//! An op's frame and snapshot are part of its identity. Nothing is shared, merged or
//! deduplicated across frames, so a permission conjunct and a same-text `WHERE` conjunct are
//! distinct ops: their frames differ.

use super::ids::{FrameId, ValueId};
use crate::exec::vm::program::UnitFrame;
use crate::expr::logical::EvalFrame;

/// Who evaluates an op and under what authority.
///
/// The principal content is the compiled-unit frame. A unit never runs for a principal of another
/// class and fails closed. The IR defines no principal
/// type of its own. Whether permissions are enforced is derived from the evaluation mode and the
/// unit frame, and never stored a second time. The role level (viewer, editor, owner) is not part
/// of the frame; an op whose failure depends on it declares that dependency.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct AuthorityFrame {
	/// The evaluation mode and the `AUTH LIMIT` the expression runs under.
	pub(crate) eval: EvalFrame,
	/// The principal class, namespace, database and permission contract the code is lowered for.
	pub(crate) unit: UnitFrame,
}

/// The authority frames of one container, indexed by [`FrameId`].
///
/// Equal ids hold equal frames, so a frame comparison is an id comparison. A table only grows: an
/// entry is appended and never changed or removed.
#[derive(Clone, Debug)]
pub(crate) struct FrameTable {
	pub(crate) frames: Vec<AuthorityFrame>,
	/// The table this one was made as or cloned from, unique among the tables the process makes.
	lineage: u64,
}

impl Default for FrameTable {
	fn default() -> Self {
		Self::of(Vec::new())
	}
}

impl FrameTable {
	/// A table of a new lineage that holds `frames`.
	pub(crate) fn of(frames: Vec<AuthorityFrame>) -> Self {
		Self {
			frames,
			lineage: super::catalog::next_lineage(),
		}
	}

	/// What tells this table apart from another while a fact cache lives: its lineage and its
	/// length. A table made later never shares a lineage, wherever it is allocated. A clone shares
	/// its lineage and, since a table only grows, answers alike until one of them grows; two
	/// clones that each grow to the same length with different frames are not told apart, so a
	/// cache must not file the answers of two such clones.
	pub(crate) fn memo_identity(&self) -> (u64, usize) {
		(self.lineage, self.frames.len())
	}

	/// The frame `id` names, or `None` for an id outside the table.
	pub(crate) fn get(&self, id: FrameId) -> Option<&AuthorityFrame> {
		self.frames.get(usize::from(id.index()))
	}
}

/// The version of the data an op observes.
///
/// Set by the op that scopes a versioned read onto every op beneath it, and identity-bearing: a
/// read at a version is equal to no read of the current state. This type is distinct from the
/// storage layer's read-view `Snapshot` and is never imported unqualified beside it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Snapshot {
	Current,
	/// The value holding the version stamp, an operand-level fact of the scoping op.
	Version(ValueId),
}
