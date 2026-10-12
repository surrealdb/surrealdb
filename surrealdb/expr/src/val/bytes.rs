use std::ops::Deref;

use revision::{DeserializeRevisioned, Error, SkipCheckRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_collections::bounded;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::IndexFormat;

#[revisioned(revision = 1, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Encode, BorrowDecode)]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
#[repr(transparent)]
pub struct Bytes(pub ::bytes::Bytes);

/// Reads exactly what the derived serializer writes (revision 1: a varint length, then the bytes).
/// The bytes are read through [`bounded::read_bytes`], so a length that runs past the input costs
/// at most [`bounded::PREALLOC_BYTES`] beyond what the input holds.
impl DeserializeRevisioned for Bytes {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		bounded::read_revision(reader, "Bytes", 1)?;
		let len = bounded::read_len(reader)?;
		let bytes = bounded::read_bytes(reader, len)?;
		Ok(Self(::bytes::Bytes::from(bytes)))
	}
}

impl SkipCheckRevisioned for Bytes {
	fn skip_check_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), Error> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

impl Bytes {
	pub fn into_inner(self) -> bytes::Bytes {
		self.0
	}
}

impl From<Vec<u8>> for Bytes {
	fn from(v: Vec<u8>) -> Self {
		Self(bytes::Bytes::from(v))
	}
}

impl From<Bytes> for bytes::Bytes {
	fn from(bytes: Bytes) -> Self {
		bytes.0
	}
}

impl From<bytes::Bytes> for Bytes {
	fn from(bytes: bytes::Bytes) -> Self {
		Bytes(bytes)
	}
}

impl From<surrealdb_types::Bytes> for Bytes {
	fn from(v: surrealdb_types::Bytes) -> Self {
		Bytes(v.into_inner())
	}
}

impl From<Bytes> for surrealdb_types::Bytes {
	fn from(v: Bytes) -> Self {
		surrealdb_types::Bytes::from(v.into_inner())
	}
}

impl Deref for Bytes {
	type Target = bytes::Bytes;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl ToSql for Bytes {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "b\"{}\"", hex::encode_upper(&self.0))
	}
}

#[cfg(test)]
mod tests {
	use crate::val::{Bytes, Value};

	#[test]
	fn serialize() {
		let val = Value::Bytes(Bytes::from(vec![1, 2, 3, 5]));
		let serialized: Vec<u8> = revision::to_vec(&val).unwrap();
		println!("{serialized:?}");
		let deserialized: Value = revision::from_slice(&serialized).unwrap();
		assert_eq!(val, deserialized);
	}
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Bytes {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		Ok(Bytes(::bytes::Bytes::copy_from_slice(u.arbitrary()?)))
	}
}
