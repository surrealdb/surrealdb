//! Basic blocks: an ordered list of ops closed by a terminator.

use smallvec::SmallVec;

use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};

/// A block. Its last op is a terminator and no other op is.
///
/// Its arguments are the region's entry values: the current element and its position, a loop
/// accumulator, closure arguments, the statement timestamp of a statement region.
#[derive(Clone, Debug)]
pub(crate) struct BlockData {
	pub(crate) args: SmallVec<[ValueId; 2]>,
	/// The effect tokens the block receives, one per class, apart from `args` so that the
	/// positions of the data arguments never move. Empty in a container no token is threaded
	/// through.
	pub(crate) token_args: SmallVec<[ValueId; 2]>,
	pub(crate) ops: Vec<OpId>,
	pub(crate) parent: RegionId,
}
