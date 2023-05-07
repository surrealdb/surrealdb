use base64_lib::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use serde::{
	de::{self, Visitor},
	Deserialize, Serialize,
};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct Bytes(pub(crate) Vec<u8>);

impl Bytes {
	pub fn into_inner(self) -> Vec<u8> {
		self.0
	}
}

impl From<Vec<u8>> for Bytes {
	fn from(v: Vec<u8>) -> Self {
		Self(v)
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
		write!(f, "encoding::base64::decode(\"{}\")", STANDARD_NO_PAD.encode(&self.0))
	}
}

impl Serialize for Bytes {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if serializer.is_human_readable() {
			serializer.serialize_str(&STANDARD_NO_PAD.encode(&self.0))
		} else {
			serializer.serialize_bytes(&self.0)
		}
	}
}

impl<'de> Deserialize<'de> for Bytes {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		if deserializer.is_human_readable() {
			struct Base64BytesVisitor;

			impl<'de> Visitor<'de> for Base64BytesVisitor {
				type Value = Bytes;

				fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
					formatter.write_str("a base64 str")
				}

				fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
				where
					E: de::Error,
				{
					STANDARD_NO_PAD
						.decode(&value)
						.map(Bytes)
						.map_err(|_| de::Error::custom("invalid base64"))
				}
			}

			deserializer.deserialize_str(Base64BytesVisitor)
		} else {
			struct RawBytesVisitor;

			impl<'de> Visitor<'de> for RawBytesVisitor {
				type Value = Bytes;

				fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
					formatter.write_str("bytes")
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
			}

			deserializer.deserialize_byte_buf(RawBytesVisitor)
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::sql::{Bytes, Value};

	#[test]
	fn serialize() {
		let val = Value::Bytes(Bytes(vec![1, 2, 3, 5]));
		let serialized: Vec<u8> = val.into();
		println!("{serialized:?}");
		let deserialized = Value::from(serialized);
		println!("{deserialized:?}");
	}
}
