//! SSA values: one definition and one use-list each.

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use crate::exec::ir::iface::ids::{BlockId, OpId};
use crate::exec::ir::iface::types::TypeKnowledge;

/// An SSA value: one op result or one block argument.
#[derive(Clone, Debug)]
pub(crate) struct ValueData {
	pub(crate) def: ValueDef,
	/// Language-level knowledge only: what a literal, a parameter or the op's guaranteed result
	/// kind says. Knowledge sourced from storage is derived by analysis and never stored here.
	pub(crate) ty: TypeKnowledge,
	/// Maintained by the rewriter only.
	pub(crate) uses: SmallVec<[Use; 4]>,
	/// A name the printer may show; never identity.
	pub(crate) hint_name: Option<Strand>,
}

/// What defines a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ValueDef {
	/// The `index`th result of `op`.
	OpResult {
		op: OpId,
		index: u16,
	},
	/// The `index`th argument of `block`.
	BlockArg {
		block: BlockId,
		index: u16,
	},
}

/// One use of a value: the `operand`th operand of `user`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Use {
	pub(crate) user: OpId,
	pub(crate) operand: u16,
}
