//! Value identity: when two ops compute one value.
//!
//! One contract serves every mechanism that shares a value: common-subexpression elimination,
//! hoisting, unswitching, prefetch and e-graph hash-consing. Two ops are one value only if their
//! [`ValueKey`]s are equal, their attributes are equal
//! ([`OpKind::attrs_eq`](crate::exec::ir::op_kind::OpKind::attrs_eq)), and the rules of the
//! analysis that asks admit them. A key holds a digest of the attributes, and two digests can
//! collide, so equal keys are a candidate only. Location, provenance, placement, a value's hint
//! name and `EXPLAIN` labels are never identity.

use smallvec::SmallVec;

use super::ids::{FrameId, OpId, SnapshotId, ValueId, WriteEpoch};
use crate::exec::ir::op_kind::OpTag;

/// The identity of the value an op computes.
///
/// Frame and snapshot are part of every key. Keys are compared only within one statement. An op
/// that is not deterministic, or whose body is an opaque leaf, carries `opaque: Some(op)` and is
/// equal to no other key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct ValueKey {
	pub(crate) tag: OpTag,
	/// The structural digest of the op's identity-bearing attributes only.
	pub(crate) attrs: AttrDigest,
	pub(crate) operands: SmallVec<[ValueId; 4]>,
	/// The effect tokens the op takes, which order it against the writes of its statement: two ops
	/// that take different tokens are two values.
	pub(crate) tokens: SmallVec<[ValueId; 2]>,
	pub(crate) frame: FrameId,
	pub(crate) snapshot: SnapshotId,
	/// The write epoch of an op that reads stored data.
	pub(crate) epoch: Option<WriteEpoch>,
	/// `Some(op)` makes the key equal to no key but itself.
	pub(crate) opaque: Option<OpId>,
}

/// The structural digest of an op's identity-bearing attributes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct AttrDigest(pub(crate) [u8; 16]);

impl AttrDigest {
	/// The digest of an op whose attributes are not digested: equal to no digest an implemented
	/// `attr_digest` answers.
	pub(crate) const OPAQUE: AttrDigest = AttrDigest([0xFF; 16]);
}
