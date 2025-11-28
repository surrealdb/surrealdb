use std::fmt;
use std::ops::Deref;

use revision::revisioned;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::val::IndexFormat;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, Encode, BorrowDecode)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
#[repr(transparent)]
pub struct Bytes(pub(crate) ::bytes::Bytes);

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
		surrealdb_types::Bytes::new(v.into_inner())
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

impl Serialize for Bytes {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_bytes(&self.0)
	}
}

impl<'de> Deserialize<'de> for Bytes {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		struct RawBytesVisitor;

		impl<'de> Visitor<'de> for RawBytesVisitor {
			type Value = Bytes;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("bytes or sequence of bytes")
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Bytes(bytes::Bytes::from(v)))
			}

			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Bytes(bytes::Bytes::copy_from_slice(v)))
			}

			fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
			where
				A: SeqAccess<'de>,
			{
				let capacity = seq.size_hint().unwrap_or_default();
				let mut vec = Vec::with_capacity(capacity);
				while let Some(byte) = seq.next_element()? {
					vec.push(byte);
				}
				Ok(Bytes(bytes::Bytes::from(vec)))
			}
		}

		deserializer.deserialize_byte_buf(RawBytesVisitor)
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

	#[test]
	fn json_roundtrip() {
		let val = Bytes::from(vec![1, 2, 3, 5]);
		let json = serde_json::to_string(&val).unwrap();
		let deserialized = serde_json::from_str(&json).unwrap();
		assert_eq!(val, deserialized);
	}
}
