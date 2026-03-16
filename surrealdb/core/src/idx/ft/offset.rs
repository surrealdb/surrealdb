use revision::revisioned;

use crate::idx::ft::Position;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Offset {
	pub(super) index: u32,
	// Start position of the original term
	pub(super) start: Position,
	// Start position of the generated term
	pub(super) gen_start: Position,
	// End position of the original term
	pub(super) end: Position,
}

impl Offset {
	pub(crate) fn new(index: u32, start: Position, gen_start: Position, end: Position) -> Self {
		Self {
			index,
			start,
			gen_start,
			end,
		}
	}
}
