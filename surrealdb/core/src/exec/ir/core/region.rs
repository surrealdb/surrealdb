//! Regions: the blocks an op owns.

use smallvec::SmallVec;

use crate::exec::ir::iface::ids::{BlockId, OpId};
use crate::exec::ir::iface::region_kind::RegionKind;

/// A region. Every region holds exactly one block; the vector leaves room for multi-block
/// regions without changing the type.
#[derive(Clone, Debug)]
pub(crate) struct RegionData {
	pub(crate) kind: RegionKind,
	pub(crate) blocks: SmallVec<[BlockId; 1]>,
	/// The op that owns the region; `None` only for a container's top region.
	pub(crate) parent: Option<OpId>,
}
