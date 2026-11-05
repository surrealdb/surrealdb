//! Effect-token slots: the tokens an op takes and produces and a block receives.
//!
//! A token is an ordinary value of type [`TypeKnowledge::Token`], defined by an op or a block, and
//! read by the ops that order themselves against it. Its operands, results and block arguments are
//! held apart from the data lists ([`OpData::operands`](super::op::OpData),
//! [`OpData::results`](super::op::OpData) and [`BlockData::args`](super::block::BlockData)), so a
//! token never moves the position of a data operand, result or argument, and nothing that counts
//! or indexes the data lists sees one. The use-lists keep both exact: a use as the `n`th token
//! operand of an op records the operand index [`TOKEN_SLOT_BASE`] `+ n`.
//!
//! The tokens an op takes are part of the identity of the value it computes
//! ([`ValueKey`](crate::exec::ir::iface::identity::ValueKey)), but they do not move the op's
//! generation: they are derived from the container and derived again whenever a pass rewrites it.
//! A token is never lowered: the data lists are all the lowering reads.

use smallvec::SmallVec;

use super::ir::Ir;
use crate::exec::ir::iface::ids::{BlockId, OpId, ValueId};
use crate::exec::ir::iface::types::TokenClass;

/// The tokens one op takes and produces.
#[derive(Clone, Debug, Default)]
pub(crate) struct OpTokens {
	/// The tokens the op takes, one per class, in class order.
	pub(crate) operands: SmallVec<[ValueId; 2]>,
	/// The tokens the op produces, one per class, in class order.
	pub(crate) results: SmallVec<[ValueId; 2]>,
}

impl Ir {
	/// Whether effect tokens are threaded through the container: every op that orders itself
	/// against a class of world state takes the token of that class, and the verifier checks that
	/// it does ([`VerifyRule::TokenAgreement`](super::verify::VerifyRule)).
	pub(crate) fn is_threaded(&self) -> bool {
		self.threaded
	}

	/// Whether `op` was created in a threaded container after the threading, so that it holds no
	/// token whatever its effects call for. Its tokens are derived by the next threading.
	pub(crate) fn holds_no_tokens(&self, op: OpId) -> bool {
		self.untokened.contains_key(op)
	}

	/// Records that `op` was created, in a container whose threading it is not part of.
	pub(super) fn note_created(&mut self, op: OpId) {
		if self.threaded {
			self.untokened.insert(op, ());
		}
	}

	/// The tokens `op` takes, in class order. Empty for an op that takes none and for a dangling
	/// id.
	pub(crate) fn token_operands(&self, op: OpId) -> &[ValueId] {
		self.op_tokens.get(op).map_or(&[], |tokens| tokens.operands.as_slice())
	}

	/// The tokens `op` produces, in class order. Empty for an op that produces none and for a
	/// dangling id.
	pub(crate) fn token_results(&self, op: OpId) -> &[ValueId] {
		self.op_tokens.get(op).map_or(&[], |tokens| tokens.results.as_slice())
	}

	/// The tokens `block` receives, in class order.
	pub(crate) fn token_args(&self, block: BlockId) -> &[ValueId] {
		self.block(block).map_or(&[], |held| held.token_args.as_slice())
	}

	/// The token of `class` among `values`.
	pub(crate) fn token_of_class(&self, values: &[ValueId], class: TokenClass) -> Option<ValueId> {
		values.iter().copied().find(|value| self.token_class(*value) == Some(class))
	}

	/// The class of the token `value`, or `None` for a value that is not one.
	pub(crate) fn token_class(&self, value: ValueId) -> Option<TokenClass> {
		self.value(value).and_then(|data| data.ty.token_class())
	}
}
