use crate::err::Error;
use crate::idx::docids::DocId;
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

#[derive(Clone, Debug, PartialEq)]
pub(super) struct OffsetRecords(pub(super) Vec<Offset>);

impl TryFrom<OffsetRecords> for Val {
	type Error = Error;

	fn try_from(offsets: OffsetRecords) -> Result<Self, Self::Error> {
		let n_offsets = offsets.0.len();
		// We build a unique vector with every values (start and offset).
		let mut decompressed = Vec::with_capacity(1 + 4 * n_offsets);
		// The first push the size of the index,
		// so we can rebuild the OffsetsRecord on deserialization.
		decompressed.push(n_offsets as u32);
		// We want the value to be more or less sorted so the RLE compression
		// will be more effective
		// Indexes are likely to be very small
		for o in &offsets.0 {
			decompressed.push(o.index);
		}
		// `starts` and `offsets` are likely to be ascending
		for o in &offsets.0 {
			decompressed.push(o.start);
			decompressed.push(o.gen_start);
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
		let n_offsets = *decompressed
			.first()
			.ok_or(Error::CorruptedIndex("OffsetRecords::try_from(1)"))? as usize;
		// <= v1.4 the Offset contains only two field: start and end.
		// We check the number of integers. If there is only 3 per offset this is the old format.
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
	use crate::idx::ft::offsets::{Offset, OffsetRecords};
	use crate::kvs::Val;

	#[test]
	fn test_offset_records() {
		let o = OffsetRecords(vec![
			Offset::new(0, 1, 2, 3),
			Offset::new(0, 11, 13, 22),
			Offset::new(1, 1, 3, 4),
		]);
		let v: Val = o.clone().try_into().unwrap();
		let o2 = v.try_into().unwrap();
		assert_eq!(o, o2)
	}

	#[test]
	fn test_migrate_v1_offset_records() {
		let decompressed = vec![3u32, 0, 0, 1, 1, 3, 11, 22, 1, 4];
		let v = bincode::serialize(&decompressed).unwrap();
		let o: OffsetRecords = v.try_into().unwrap();
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
