//! The value stored under a b-tree index entry.

use anyhow::Result;

use crate::idx::docids::DocId;
use crate::key::KVValue;
use crate::val::RecordId;

/// Value stored in b-tree index entries, both the non-unique and the unique
/// shape.
///
/// The base encoding is the revision-encoded [`RecordId`]. Indexes at
/// [`crate::catalog::BTREE_ENTRY_DOC_IDS_FORMAT_VERSION`] or later append the
/// record's table-level doc-ID as 8 big-endian bytes, so doc-ID-based plans
/// (roaring bitmap candidate fusion) can read the doc-ID straight from the entry
/// without probing the `!di` mapping.
///
/// The format is self-describing on decode: a revision-encoded `RecordId`
/// followed by either nothing (pre-doc-ID entry) or exactly 8 bytes. Readers that
/// only need the `RecordId` (including older binaries) can keep using
/// `revision::from_slice`, which ignores the trailing bytes.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct IndexEntryValue {
	pub rid: RecordId,
	pub doc_id: Option<DocId>,
}

impl KVValue for IndexEntryValue {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		let mut buf = Vec::new();
		revision::to_writer(&mut buf, &self.rid)
			.map_err(|e| anyhow::anyhow!("Failed to encode index entry value: {e}"))?;
		if let Some(doc_id) = self.doc_id {
			buf.extend_from_slice(&doc_id.to_be_bytes());
		}
		Ok(buf)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let mut reader = bytes;
		let rid: RecordId = revision::from_reader(&mut reader)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))?;
		let doc_id = match reader.len() {
			0 => None,
			8 => {
				let mut arr = [0u8; 8];
				arr.copy_from_slice(reader);
				Some(DocId::from_be_bytes(arr))
			}
			n => {
				return Err(anyhow::Error::new(crate::key::Error::Corrupted(
					"Index entry value has an invalid trailing doc-ID segment",
				))
				.context(format!("{n} trailing bytes after the record ID")));
			}
		};
		Ok(Self {
			rid,
			doc_id,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::RecordIdKey;

	/// An entry written without a doc-ID still decodes, and one written with it
	/// round-trips. Both shapes exist on disk, so the decode has to tell them
	/// apart from the byte length alone.
	#[test]
	fn both_entry_shapes_round_trip() {
		let rid = RecordId::new("person".into(), RecordIdKey::Number(1));

		let without = IndexEntryValue {
			rid: rid.clone(),
			doc_id: None,
		};
		let bytes = without.kv_encode_value().unwrap();
		assert_eq!(IndexEntryValue::kv_decode_value(&bytes, ()).unwrap(), without);

		let with = IndexEntryValue {
			rid,
			doc_id: Some(42),
		};
		let bytes = with.kv_encode_value().unwrap();
		assert_eq!(IndexEntryValue::kv_decode_value(&bytes, ()).unwrap(), with);
	}

	/// Anything other than nothing or exactly eight trailing bytes is corruption,
	/// not a newer writer.
	#[test]
	fn a_partial_doc_id_segment_is_rejected() {
		let value = IndexEntryValue {
			rid: RecordId::new("person".into(), RecordIdKey::Number(1)),
			doc_id: None,
		};
		let mut bytes = value.kv_encode_value().unwrap();
		bytes.extend_from_slice(&[0u8; 4]);
		assert!(IndexEntryValue::kv_decode_value(&bytes, ()).is_err());
	}
}
