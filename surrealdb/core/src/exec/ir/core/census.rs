//! The census of a container: which op tags and which shapes of op it holds.
//!
//! A pass or a rule that finds its candidates by an op's tag has nothing to find in a container
//! that holds none of those tags. The census answers that in one walk, so a driver can skip such
//! a pass or rule without running its candidate search. A gate built on the census is sound only
//! when the candidate search of the pass it gates matches on the very tags and flags the gate
//! asks for, and the pass touches neither the container nor the fact cache when it finds none.
//! Each gate states that proof where it is written.
//!
//! The tag set holds the tags of the ops outside stored-clause regions. A flag holds a fact a tag
//! does not state, and says in its own documentation which ops it covers: a flag a gated pass
//! reads inside stored code, such as [`Flag::FunctionBodyCall`], counts stored code too.
//!
//! To add a gate on a tag, ask [`Census::has`]. To add a flag, add a [`Flag`] variant, set it in
//! [`Census::observe_op`] or [`Census::observe_region`] from the op or region that states it, and
//! document there whether it covers stored code. The census of a container that changed is built
//! again with [`Census::of`]; it never updates in place.

use super::ir::Ir;
use super::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::ids::RegionId;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{LogOp, LogTag};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::PhysTag;

/// The number of tags of every dialect.
const TAGS: usize = ExprTag::ALL.len() + LogTag::ALL.len() + PhysTag::ALL.len();

/// The words of the tag set: one bit per tag.
const WORDS: usize = TAGS.div_ceil(64);

/// A fact about a container that no single tag states.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Flag {
	/// A stored-clause region is present. The tag set does not cover the ops inside it.
	StoredClause,
	/// A call op that holds a `FunctionBody` region, in stored code too: the inlining pass visits
	/// every such call and drops the body of one it does not inline.
	FunctionBodyCall,
	/// An op outside stored code owns a `PermissionFrame` region.
	PermissionFrame,
	/// A `logical.project` that is a layer, outside stored code.
	ProjectionLayer,
}

impl Flag {
	/// Every flag; the position of a flag is its bit in the flag word.
	const ALL: [Flag; 4] =
		[Self::StoredClause, Self::FunctionBodyCall, Self::PermissionFrame, Self::ProjectionLayer];

	const fn bit(self) -> u32 {
		1 << self as u32
	}
}

const _: () = assert!(Flag::ALL.len() <= u32::BITS as usize);

/// The position of `tag` in the tag set: its dialect's offset plus its declaration order.
const fn index(tag: OpTag) -> usize {
	match tag {
		OpTag::Expr(tag) => tag as usize,
		OpTag::Logical(tag) => ExprTag::ALL.len() + tag as usize,
		OpTag::Phys(tag) => ExprTag::ALL.len() + LogTag::ALL.len() + tag as usize,
	}
}

/// The tags and flags of a container; see the module documentation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Census {
	tags: [u64; WORDS],
	flags: u32,
}

impl Census {
	/// The census of `ir`, in one walk from its top region.
	pub(crate) fn of(ir: &Ir) -> Self {
		let mut census = Self {
			tags: [0; WORDS],
			flags: 0,
		};
		census.region(ir, ir.top(), false);
		census
	}

	/// Whether an op outside stored code has the tag `tag`.
	pub(crate) fn has(&self, tag: impl Into<OpTag>) -> bool {
		let at = index(tag.into());
		self.tags[at / 64] & (1 << (at % 64)) != 0
	}

	/// Whether an op outside stored code has any of `tags`.
	pub(crate) fn has_any(&self, tags: &[OpTag]) -> bool {
		tags.iter().any(|tag| self.has(*tag))
	}

	/// Whether `flag` holds.
	pub(crate) fn flag(&self, flag: Flag) -> bool {
		self.flags & flag.bit() != 0
	}

	/// Whether every tag and flag of `fresh` is also set in `self`: a census that is stale only by
	/// what a pass removed still covers the container.
	pub(crate) fn covers(&self, fresh: &Census) -> bool {
		self.tags.iter().zip(&fresh.tags).all(|(held, wanted)| wanted & !held == 0)
			&& fresh.flags & !self.flags == 0
	}

	fn set(&mut self, tag: OpTag) {
		let at = index(tag);
		self.tags[at / 64] |= 1 << (at % 64);
	}

	fn raise(&mut self, flag: Flag) {
		self.flags |= flag.bit();
	}

	fn region(&mut self, ir: &Ir, region: RegionId, stored: bool) {
		for op in ops_in(ir, region) {
			let Some(data) = ir.op(op) else {
				continue;
			};
			self.observe_op(&data.kind, stored);
			for nested in &data.regions {
				let Some(held) = ir.region(*nested) else {
					continue;
				};
				let inside = stored || matches!(held.kind, RegionKind::StoredClause(_));
				self.observe_region(&data.kind, held.kind, stored, inside);
				self.region(ir, *nested, inside);
			}
		}
	}

	/// Records an op: its tag when it stands outside stored code, and the flags its kind states.
	fn observe_op(&mut self, kind: &OpKind, stored: bool) {
		if stored {
			return;
		}
		self.set(kind.tag());
		if let OpKind::Logical(LogOp::Project(attrs)) = kind
			&& attrs.layer
		{
			self.raise(Flag::ProjectionLayer);
		}
	}

	/// Records a region `owner` holds: `stored` says the owner stands in stored code, `inside`
	/// that the region does.
	fn observe_region(&mut self, owner: &OpKind, region: RegionKind, stored: bool, inside: bool) {
		if inside && !stored {
			self.raise(Flag::StoredClause);
		}
		if region == RegionKind::FunctionBody && matches!(owner, OpKind::Expr(ExprOp::Call(_))) {
			self.raise(Flag::FunctionBodyCall);
		}
		if region == RegionKind::PermissionFrame && !stored {
			self.raise(Flag::PermissionFrame);
		}
	}
}

impl From<ExprTag> for OpTag {
	fn from(tag: ExprTag) -> Self {
		OpTag::Expr(tag)
	}
}

impl From<LogTag> for OpTag {
	fn from(tag: LogTag) -> Self {
		OpTag::Logical(tag)
	}
}

impl From<PhysTag> for OpTag {
	fn from(tag: PhysTag) -> Self {
		OpTag::Phys(tag)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn the_index_is_the_position_in_the_tag_list() {
		for (at, tag) in OpTag::ALL.iter().enumerate() {
			assert_eq!(index(*tag), at, "{}", tag.mnemonic());
		}
		assert_eq!(OpTag::ALL.len(), TAGS);
	}
}
