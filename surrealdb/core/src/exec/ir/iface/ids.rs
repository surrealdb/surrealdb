//! Identifiers of the entities one IR container holds.
//!
//! Ids are scoped to their container, stable for the life of the entity they name, and never
//! part of a cache key's `Hash` or `Eq`: two containers holding the same program use different
//! ids for it.

slotmap::new_key_type! {
	/// An operation. Generational: a key outlives its op only as a dangling key, which every
	/// accessor answers with `None`; a key is never re-bound to another op.
	pub(crate) struct OpId;
	/// An SSA value: one op result or one block argument. One definition, one use-list.
	pub(crate) struct ValueId;
	/// A basic block: an ordered list of ops closed by a terminator.
	pub(crate) struct BlockId;
	/// A region: the blocks an op owns.
	pub(crate) struct RegionId;
}

/// An interned authority frame of one container. Equal ids hold equal frames.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct FrameId(u16);

impl FrameId {
	/// The id at position `index` of the container's frame table.
	pub(crate) const fn new(index: u16) -> Self {
		Self(index)
	}

	/// The position of this id in the container's frame table.
	pub(crate) const fn index(self) -> u16 {
		self.0
	}
}

/// An interned snapshot of one container. Equal ids hold equal snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct SnapshotId(u16);

impl SnapshotId {
	/// The id at position `index` of the container's snapshot table.
	pub(crate) const fn new(index: u16) -> Self {
		Self(index)
	}

	/// The position of this id in the container's snapshot table.
	pub(crate) const fn index(self) -> u16 {
		self.0
	}
}

/// The identity term of an op that reads stored data.
///
/// A read whose footprint no write of its statement can reach is `Stable`, and two such reads of
/// one key share a value. A read that a write can reach is `Unique` to its op and is equal to no
/// other read, so reads are never merged across a write that may change what they observe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum WriteEpoch {
	Stable(u32),
	Unique(OpId),
}
