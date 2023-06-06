use crate::err::Error;
use crate::idx::ft::docids::DocId;
use crate::idx::ft::terms::TermId;
use crate::idx::IndexKeyBase;
use crate::kvs::{Transaction, Val};
use bitpacking::{BitPacker, BitPacker1x};

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
		offsets: OffsetsRecord,
	) -> Result<(), Error> {
		let key = self.index_key_base.new_bo_key(doc_id, term_id);
		tx.set(key, offsets).await?;
		Ok(())
	}

	pub(super) async fn get_offsets(
		&self,
		tx: &mut Transaction,
		doc_id: DocId,
		term_id: TermId,
	) -> Result<Option<OffsetsRecord>, Error> {
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
	start: u32,
	end: u32,
}

impl Offset {
	pub(super) fn new(start: u32, end: u32) -> Self {
		Self {
			start,
			end,
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct OffsetsRecord(Vec<Vec<Offset>>);

impl From<OffsetsRecord> for Val {
	fn from(offsets: OffsetsRecord) -> Self {
		// We build a unique vector with every values (start and offset).
		let mut vec = Vec::new();
		// The first push the size of the index,
		// so we can rebuild the OffsetsRecord on deserialization.
		vec.push(offsets.0.len() as u32);
		for i in &offsets.0 {
			vec.push(i.len() as u32);
			for o in i {
				vec.push(o.start);
				vec.push(o.end);
			}
		}
		let packer = BitPacker1x::new();
		let num_bits: u8 = packer.num_bits_sorted(0, &vec);
		let mut compressed = vec![0u8; 4 * BitPacker1x::BLOCK_LEN];
		// The offsets are increasing per nature, so we use a delta compression.
		packer.compress_sorted(0, &vec, &mut compressed[..], num_bits);
		compressed.insert(0, num_bits);
		compressed
	}
}

impl TryFrom<Val> for OffsetsRecord {
	type Error = Error;

	fn try_from(mut val: Val) -> Result<Self, Self::Error> {
		if val.is_empty() {
			return Ok(Self(vec![]));
		}
		let num_bits = val.remove(0);
		let mut decompressed = vec![0u32; BitPacker1x::BLOCK_LEN];
		BitPacker1x::new().decompress_sorted(0, &val, &mut decompressed[..], num_bits);
		let mut iter = decompressed.iter();
		let s = *iter.next().ok_or(Error::CorruptedIndex)?;
		let mut index = Vec::with_capacity(s as usize);
		for _ in 0..s {
			let l = *iter.next().ok_or(Error::CorruptedIndex)?;
			let mut offsets = Vec::with_capacity(l as usize);
			for _ in 0..l {
				let o = Offset {
					start: *iter.next().ok_or(Error::CorruptedIndex)?,
					end: *iter.next().ok_or(Error::CorruptedIndex)?,
				};
				offsets.push(o);
			}
			index.push(offsets);
		}
		Ok(Self(index))
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::ft::offsets::{Offset, OffsetsRecord};
	use crate::kvs::Val;

	#[test]
	fn test() {
		let o = OffsetsRecord(vec![
			vec![
				Offset {
					start: 1,
					end: 2,
				},
				Offset {
					start: 11,
					end: 22,
				},
			],
			vec![Offset {
				start: 3,
				end: 4,
			}],
		]);

		let v: Val = o.clone().into();
		let o2 = v.try_into().unwrap();
		assert_eq!(o, o2)
	}
}
