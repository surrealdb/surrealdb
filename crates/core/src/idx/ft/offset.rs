use anyhow::Result;
use revision::revisioned;

use crate::err::Error;
use crate::idx::ft::Position;
use crate::kvs::KVValue;

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

#[derive(Default, Clone, Debug, PartialEq)]
pub(crate) struct OffsetRecords(pub(super) Vec<Offset>);

impl KVValue for OffsetRecords {
	#[inline]
	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let n_offsets = self.0.len();
		// We build a unique vector with every values (start and offset).
		let mut decompressed = Vec::with_capacity(1 + 4 * n_offsets);
		// The first push the size of the index,
		// so we can rebuild the OffsetsRecord on deserialization.
		decompressed.push(n_offsets as u32);
		// We want the value to be more or less sorted so the RLE compression
		// will be more effective
		// Indexes are likely to be very small
		for o in &self.0 {
			decompressed.push(o.index);
		}
		// `starts` and `offsets` are likely to be ascending
		for o in &self.0 {
			decompressed.push(o.start);
			decompressed.push(o.gen_start);
			decompressed.push(o.end);
		}
		Ok(bincode::serialize(&decompressed)?)
	}

	#[inline]
	fn kv_decode_value(val: Vec<u8>) -> Result<Self> {
		if val.is_empty() {
			return Ok(Self(Vec::new()));
		}
		let decompressed: Vec<u32> = bincode::deserialize(&val)?;
		let n_offsets = *decompressed
			.first()
			.ok_or(Error::CorruptedIndex("OffsetRecords::try_from(1)"))? as usize;
		// <= v1.4 the Offset contains only two field: start and end.
		// We check the number of integers. If there is only 3 per offset this is the
		// old format.
		let without_gen_start = n_offsets * 3 + 1 == decompressed.len();

		let mut indexes = decompressed.into_iter().skip(1);
		let mut tail = indexes.clone().skip(n_offsets);
		let mut res = Vec::with_capacity(n_offsets);
		for _ in 0..n_offsets {
			let index =
				indexes.next().ok_or(Error::CorruptedIndex("OffsetRecords::try_from(2)"))?;
			let start = tail.next().ok_or(Error::CorruptedIndex("OffsetRecords::try_from(3)"))?;
			let gen_start = if without_gen_start {
				start
			} else {
				tail.next().ok_or(Error::CorruptedIndex("OffsetRecords::try_from(4)"))?
			};
			let end = tail.next().ok_or(Error::CorruptedIndex("OffsetRecords::try_from(5)"))?;
			res.push(Offset::new(index, start, gen_start, end));
		}
		Ok(OffsetRecords(res))
	}
}

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

	#[test]
	fn test_migrate_v1_offset_records() {
		let decompressed = vec![3u32, 0, 0, 1, 1, 3, 11, 22, 1, 4];
		let v = bincode::serialize(&decompressed).unwrap();
		let o: OffsetRecords = OffsetRecords::kv_decode_value(v).unwrap();
		assert_eq!(
			o,
			OffsetRecords(vec![
				Offset::new(0, 1, 1, 3),
				Offset::new(0, 11, 11, 22),
				Offset::new(1, 1, 1, 4),
			])
		)
	}
}
