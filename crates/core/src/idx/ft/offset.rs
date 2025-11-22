use revision::revisioned;

use crate::idx::ft::Position;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq)]
pub(super) struct Offset {
	pub(super) index: u32,
	// Start position of the original term
	pub(super) start: Position,
	// Start position of the generated term
	pub(super) gen_start: Position,
	// End position of the original term
	pub(super) end: Position,
}

impl Offset {
	pub(super) fn new(index: u32, start: Position, gen_start: Position, end: Position) -> Self {
		Self {
			index,
			start,
			gen_start,
			end,
		}
	}
}

#[cfg(test)]
#[revisioned(revision = 1)]
#[derive(Default, Clone, Debug, PartialEq)]
pub(crate) struct OffsetRecords(pub(super) Vec<Offset>);

#[cfg(test)]
crate::kvs::impl_kv_value_revisioned!(OffsetRecords);

#[cfg(test)]
mod tests {
	use crate::idx::ft::offset::{Offset, OffsetRecords};
	use crate::kvs::{KVValue, Val};

	#[test]
	fn test_offset_records() {
		let o = OffsetRecords(vec![
			Offset::new(0, 1, 2, 3),
			Offset::new(0, 11, 13, 22),
			Offset::new(1, 1, 3, 4),
		]);
		let v: Val = o.clone().kv_encode_value().unwrap();
		let o2 = OffsetRecords::kv_decode_value(v).unwrap();
		assert_eq!(o, o2)
	}
}
