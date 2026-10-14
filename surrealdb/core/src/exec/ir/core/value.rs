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

/// The first operand index that names an effect-token slot instead of a data operand.
///
/// The token operands of an op live in their own list ([`Ir::token_operands`](super::ir::Ir)), so
/// the positions of its data operands never move when the op takes a token. A use of a value as
/// the `n`th token operand is recorded with the operand index `TOKEN_SLOT_BASE + n`.
pub(crate) const TOKEN_SLOT_BASE: u16 = 0x8000;

/// One use of a value: the `operand`th operand of `user`, or the `(operand - TOKEN_SLOT_BASE)`th
/// token operand when the index is at least [`TOKEN_SLOT_BASE`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Use {
	pub(crate) user: OpId,
	pub(crate) operand: u16,
}

impl Use {
	/// The position among the token operands of `user` when the use is of a token slot.
	pub(crate) fn token_slot(&self) -> Option<usize> {
		self.operand.checked_sub(TOKEN_SLOT_BASE).map(usize::from)
	}
}
