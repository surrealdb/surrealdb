use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

use crate::val::IndexFormat;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[repr(transparent)]
pub struct Bytes(pub(crate) ::bytes::Bytes);

// Manual implementation of Revisioned traits
impl revision::Revisioned for Bytes {
	fn revision() -> u16 {
		1
	}
}

impl revision::SerializeRevisioned for Bytes {
	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		// Serialize the inner bytes as Vec<u8>
		let vec: Vec<u8> = self.0.as_ref().to_vec();
		vec.serialize_revisioned(writer)
	}
}

impl revision::DeserializeRevisioned for Bytes {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		// Deserialize as Vec<u8> and convert to bytes::Bytes
		let vec = Vec::<u8>::deserialize_revisioned(reader)?;
		Ok(Bytes(bytes::Bytes::from(vec)))
	}
}

// Manual implementation of Encode for Bytes
impl Encode<()> for Bytes {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		// Encode as a byte slice
		w.write_slice(self.0.as_ref())
	}
}

impl Encode<IndexFormat> for Bytes {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		// Encode as a byte slice
		w.write_slice(self.0.as_ref())
	}
}

// Manual implementation of BorrowDecode for Bytes
impl<'de> BorrowDecode<'de, ()> for Bytes {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		// Decode as a cow and convert to bytes::Bytes
		let cow = r.read_cow()?;
		Ok(Bytes(bytes::Bytes::copy_from_slice(cow.as_ref())))
	}
}

impl<'de> BorrowDecode<'de, IndexFormat> for Bytes {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		// Decode as a cow and convert to bytes::Bytes
		let cow = r.read_cow()?;
		Ok(Bytes(bytes::Bytes::copy_from_slice(cow.as_ref())))
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
		surrealdb_types::Bytes::new(v.into_inner())
	}
}

impl Deref for Bytes {
	type Target = bytes::Bytes;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Bytes {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "b\"{}\"", hex::encode_upper(&self.0))
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
