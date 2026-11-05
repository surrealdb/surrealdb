//! Operations: one op's kind, operands, results, regions and identity-bearing context.

use smallvec::SmallVec;

use super::location::Location;
use super::rewriter::Provenance;
use crate::exec::ir::iface::bounds::OP_DATA_MAX_BYTES;
use crate::exec::ir::iface::ids::{BlockId, FrameId, RegionId, SnapshotId, ValueId};
#[allow(unused_imports)]
pub(crate) use crate::exec::ir::op_kind::{OpKind, OpTag};

/// One op of a container.
///
/// Its variant payload is its attributes. Operands are evaluated before the op, in order; a
/// sub-expression that may be skipped is a region, never an operand.
#[derive(Clone, Debug)]
pub(crate) struct OpData {
	pub(crate) kind: OpKind,
	pub(crate) operands: SmallVec<[ValueId; 4]>,
	pub(crate) results: SmallVec<[ValueId; 2]>,
	pub(crate) regions: SmallVec<[RegionId; 2]>,
	/// The authority the op runs under. Part of the op's identity.
	pub(crate) frame: FrameId,
	/// The version of the data the op reads. Part of the op's identity.
	pub(crate) snapshot: SnapshotId,
	/// Not part of the op's identity.
	pub(crate) loc: Location,
	/// Not part of the op's identity.
	pub(crate) provenance: Provenance,
	pub(crate) parent: BlockId,
}

const _: () = assert!(::core::mem::size_of::<OpData>() <= OP_DATA_MAX_BYTES);

#[cfg(test)]
mod tests {
	use super::{OP_DATA_MAX_BYTES, OpData};

	#[test]
	fn op_data_size() {
		let size = ::core::mem::size_of::<OpData>();
		assert!(size <= OP_DATA_MAX_BYTES, "OpData is {size} bytes");
	}
}
