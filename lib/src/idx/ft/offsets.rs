use crate::err::Error;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::IndexKeyBase;
use crate::kvs::{Transaction, Val};

pub(super) type Position = u32;

pub(super) struct Offsets {
	index_key_base: IndexKeyBase,
}

impl Offsets {
	pub(super) fn new(index_key_base: IndexKeyBase) -> Self {
		Self {
			index_key_base,
		}
	}

	pub(super) async fn set_offsets(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_id: TermId,
		offsets: OffsetRecords,
	) -> Result<(), Error> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		let val: Val = offsets.try_into()?;
		tx.set(key, val).await?;
		Ok(())
	}

	pub(super) async fn get_offsets(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_id: TermId,
	) -> Result<Option<OffsetRecords>, Error> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		if let Some(val) = tx.get(key).await? {
			let offsets = val.try_into()?;
			Ok(Some(offsets))
		} else {
			Ok(None)
		}
	}

	pub(super) async fn remove_offsets(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_id: TermId,
	) -> Result<(), Error> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		tx.del(key).await
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Offset {
	pub(super) index: u32,
	pub(super) start: Position,
	pub(super) end: Position,
}

impl Offset {
	pub(super) fn new(index: u32, start: Position, end: Position) -> Self {
		Self {
			index,
			start,
			end,
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct OffsetRecords(pub(super) Vec<Offset>);

impl TryFrom<OffsetRecords> for Val {
	type Error = Error;

	fn try_from(offsets: OffsetRecords) -> Result<Self, Self::Error> {
		// We build a unique vector with every values (start and offset).
		let mut decompressed = Vec::new();
		// The first push the size of the index,
		// so we can rebuild the OffsetsRecord on deserialization.
		decompressed.push(offsets.0.len() as u32);
		// We want the value to be more or less sorted so the RLE compression
		// will be more effective
		// Indexes are likely to be very small
		for o in &offsets.0 {
			decompressed.push(o.index);
		}
		// `starts` and `offsets` are likely to be ascending
		for o in &offsets.0 {
			decompressed.push(o.start);
			decompressed.push(o.end);
		}
		Ok(bincode::serialize(&decompressed)?)
	}
}

impl TryFrom<Val> for OffsetRecords {
	type Error = Error;

	fn try_from(val: Val) -> Result<Self, Self::Error> {
		if val.is_empty() {
			return Ok(Self(vec![]));
		}
		let decompressed: Vec<u32> = bincode::deserialize(&val)?;
		let mut iter = decompressed.iter();
		let s = *iter.next().ok_or(Error::CorruptedIndex)?;
		let mut indexes = Vec::with_capacity(s as usize);
		for _ in 0..s {
			let index = *iter.next().ok_or(Error::CorruptedIndex)?;
			indexes.push(index);
		}
		let mut res = Vec::with_capacity(s as usize);
		for index in indexes {
			let start = *iter.next().ok_or(Error::CorruptedIndex)?;
			let end = *iter.next().ok_or(Error::CorruptedIndex)?;
			res.push(Offset::new(index, start, end));
		}
		Ok(OffsetRecords(res))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::offsets::{Offset, OffsetRecords};
	use crate::kvs::Val;

	#[test]
	fn test_offset_records() {
		let o =
			OffsetRecords(vec![Offset::new(0, 1, 2), Offset::new(0, 11, 22), Offset::new(1, 3, 4)]);
		let v: Val = o.clone().try_into().unwrap();
		let o2 = v.try_into().unwrap();
		assert_eq!(o, o2)
	}
}
