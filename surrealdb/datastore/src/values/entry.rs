//! The value stored under a b-tree index entry.

use anyhow::Result;
use surrealdb_expr::val::RecordId;
use surrealdb_kvs::key::Error as KeyError;
use surrealdb_kvs::value::KVValue;

use super::ids::DocId;

/// Value stored in b-tree index entries, both the non-unique and the unique
/// shape.
///
/// The base encoding is the revision-encoded [`RecordId`]. Indexes at or after
/// the catalog's b-tree doc-IDs format version append the record's table-level
/// doc-ID as 8 big-endian bytes, so doc-ID-based plans
/// (roaring bitmap candidate fusion) can read the doc-ID straight from the entry
/// without probing the `!dj` mapping. Spatial point index entries additionally
/// append the point's `(x, y)` coordinates as two big-endian `f64` values (16
/// bytes) after the 8-byte doc-ID, for a 24-byte `(doc_id, x, y)` trailer.
///
/// The format is self-describing on decode: a revision-encoded `RecordId`
/// followed by nothing (pre-doc-ID entry), 8 bytes (`doc_id`), or on spatial
/// indexes 24 bytes (`doc_id` + `(x, y)`). Readers that only need the `RecordId`
/// (including older binaries) can keep using `revision::from_slice`, which
/// ignores the trailing bytes.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexEntryValue {
	pub rid: RecordId,
	pub doc_id: Option<DocId>,
	pub point: Option<(f64, f64)>,
}

/// Decoded trailing segment of a spatial [`IndexEntryValue`]: optional
/// table-level [`DocId`] and optional 2D coordinates `(x, y)`.
pub type EntryTrailer = (Option<DocId>, Option<(f64, f64)>);

fn parse_doc_id_trailer(trailer: &[u8]) -> Result<Option<DocId>> {
	match trailer.len() {
		0 => Ok(None),
		8 => {
			let mut arr = [0u8; 8];
			arr.copy_from_slice(trailer);
			Ok(Some(DocId::from_be_bytes(arr)))
		}
		n => Err(anyhow::Error::new(KeyError::Corrupted(
			"Index entry value has an invalid trailing doc-ID segment",
		))
		.context(format!("{n} trailing bytes after the record ID"))),
	}
}

fn parse_spatial_trailer(trailer: &[u8]) -> Result<EntryTrailer> {
	match trailer.len() {
		0 => Ok((None, None)),
		8 => {
			let mut arr = [0u8; 8];
			arr.copy_from_slice(trailer);
			Ok((Some(DocId::from_be_bytes(arr)), None))
		}
		24 => {
			let mut doc_arr = [0u8; 8];
			let mut x_arr = [0u8; 8];
			let mut y_arr = [0u8; 8];
			doc_arr.copy_from_slice(&trailer[..8]);
			x_arr.copy_from_slice(&trailer[8..16]);
			y_arr.copy_from_slice(&trailer[16..24]);
			Ok((
				Some(DocId::from_be_bytes(doc_arr)),
				Some((f64::from_be_bytes(x_arr), f64::from_be_bytes(y_arr))),
			))
		}
		n => Err(anyhow::Error::new(KeyError::Corrupted(
			"Index entry value has an invalid trailing doc-ID segment",
		))
		.context(format!("{n} trailing bytes after the record ID"))),
	}
}

impl IndexEntryValue {
	/// The record id an entry's stored bytes name, read without the doc-id
	/// that may follow it.
	pub fn decode_record_id(bytes: &[u8]) -> Result<RecordId> {
		revision::from_slice(bytes)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))
	}

	/// Decodes the trailing `doc_id` segment (0 or 8 bytes) without allocating a [`RecordId`].
	pub fn decode_trailer(bytes: &[u8]) -> Result<Option<DocId>> {
		let used = revision::skip_slice::<RecordId>(bytes)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))?;
		parse_doc_id_trailer(&bytes[used..])
	}

	/// Decodes the trailing `(doc_id, point)` segment (0, 8, or 24 bytes) of a
	/// spatial index entry without allocating a [`RecordId`].
	pub fn decode_spatial_trailer(bytes: &[u8]) -> Result<EntryTrailer> {
		let used = revision::skip_slice::<RecordId>(bytes)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))?;
		parse_spatial_trailer(&bytes[used..])
	}

	/// Decodes a full spatial [`IndexEntryValue`] (accepting 0-, 8-, or 24-byte
	/// trailers).
	pub fn decode_spatial(bytes: &[u8]) -> Result<Self> {
		let mut reader = bytes;
		let rid: RecordId = revision::from_reader(&mut reader)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))?;
		let (doc_id, point) = parse_spatial_trailer(reader)?;
		Ok(Self {
			rid,
			doc_id,
			point,
		})
	}
}

impl KVValue for IndexEntryValue {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		if self.point.is_some() && self.doc_id.is_none() {
			anyhow::bail!("Spatial point trailer requires a doc-ID");
		}
		let mut buf = Vec::new();
		revision::to_writer(&mut buf, &self.rid)
			.map_err(|e| anyhow::anyhow!("Failed to encode index entry value: {e}"))?;
		if let Some(doc_id) = self.doc_id {
			buf.extend_from_slice(&doc_id.to_be_bytes());
		}
		if let Some((x, y)) = self.point {
			buf.extend_from_slice(&x.to_be_bytes());
			buf.extend_from_slice(&y.to_be_bytes());
		}
		Ok(buf)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let mut reader = bytes;
		let rid: RecordId = revision::from_reader(&mut reader)
			.map_err(|e| anyhow::anyhow!("Failed to decode index entry value: {e}"))?;
		let doc_id = parse_doc_id_trailer(reader)?;
		Ok(Self {
			rid,
			doc_id,
			point: None,
		})
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_expr::val::RecordIdKey;

	use super::*;

	/// An entry written without a doc-ID still decodes, and one written with it
	/// (or with a spatial point trailer) round-trips.
	#[test]
	fn both_entry_shapes_round_trip() {
		let rid = RecordId::new("person".into(), RecordIdKey::Number(1));

		for (doc_id, point) in
			[(None, None), (Some(42), None), (Some(42), Some((-0.118092, 51.509865)))]
		{
			let entry = IndexEntryValue {
				rid: rid.clone(),
				doc_id,
				point,
			};
			let bytes = entry.kv_encode_value().unwrap();
			assert_eq!(IndexEntryValue::decode_spatial(&bytes).unwrap(), entry);
			assert_eq!(IndexEntryValue::decode_spatial_trailer(&bytes).unwrap(), (doc_id, point));
			if point.is_none() {
				assert_eq!(IndexEntryValue::kv_decode_value(&bytes, ()).unwrap(), entry);
				assert_eq!(IndexEntryValue::decode_trailer(&bytes).unwrap(), doc_id);
			} else {
				assert!(IndexEntryValue::kv_decode_value(&bytes, ()).is_err());
				assert!(IndexEntryValue::decode_trailer(&bytes).is_err());
			}
		}

		let invalid = IndexEntryValue {
			rid,
			doc_id: None,
			point: Some((1.0, 2.0)),
		};
		assert!(invalid.kv_encode_value().is_err());
	}

	/// Anything other than nothing or a valid trailer length is corruption,
	/// not a newer writer.
	#[test]
	fn a_partial_doc_id_segment_is_rejected() {
		let value = IndexEntryValue {
			rid: RecordId::new("person".into(), RecordIdKey::Number(1)),
			doc_id: None,
			point: None,
		};
		for bad_len in [4, 16, 24] {
			let mut bytes = value.kv_encode_value().unwrap();
			bytes.extend(std::iter::repeat_n(0u8, bad_len));
			assert!(IndexEntryValue::kv_decode_value(&bytes, ()).is_err());
			assert!(IndexEntryValue::decode_trailer(&bytes).is_err());
			if bad_len != 24 {
				assert!(IndexEntryValue::decode_spatial(&bytes).is_err());
				assert!(IndexEntryValue::decode_spatial_trailer(&bytes).is_err());
			}
		}
	}
}
