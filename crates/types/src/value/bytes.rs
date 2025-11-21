use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use hex;
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::write_sql;

/// Represents binary data in SurrealDB
///
/// Bytes stores raw binary data as a vector of unsigned 8-bit integers.

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Bytes(pub(crate) Vec<u8>);

impl Bytes {
	/// Create new bytes from Vec<u8>
	pub fn new(data: Vec<u8>) -> Self {
		Self(data)
	}

	/// Get the inner Vec<u8>
	pub fn inner(&self) -> &Vec<u8> {
		&self.0
	}

	/// Convert the bytes to a Vec<u8>
	pub fn into_inner(self) -> Vec<u8> {
		self.0
	}
}

impl From<Vec<u8>> for Bytes {
	fn from(v: Vec<u8>) -> Self {
		Self(v)
	}
}

impl From<Bytes> for Vec<u8> {
	fn from(val: Bytes) -> Self {
		val.0
	}
}

impl From<Bytes> for bytes::Bytes {
	fn from(bytes: Bytes) -> Self {
		bytes.0.into()
	}
}

impl From<bytes::Bytes> for Bytes {
	fn from(bytes: bytes::Bytes) -> Self {
		Bytes(bytes.into())
	}
}

impl Deref for Bytes {
	type Target = Vec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Bytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"{}\"", hex::encode_upper(&self.0))
	}
}

impl ToSql for crate::Bytes {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", self)
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

			fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
				formatter.write_str("bytes or sequence of bytes")
			}

			fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Bytes(v))
			}

			fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				Ok(Bytes(v.to_owned()))
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
				Ok(Bytes(vec))
			}
		}

		deserializer.deserialize_byte_buf(RawBytesVisitor)
	}
}
