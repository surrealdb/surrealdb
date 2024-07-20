use revision::revisioned;
use serde::{
	de::{self, Visitor},
	Deserialize, Serialize,
};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
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
		writeln!(f, "")?;
		let len = self.0.len();
		if len > 0 {
			for byte in &self.0[..len - 1] {
				writeln!(f, "    {},", byte)?;
			}
			writeln!(f, "    {}", self.0[len - 1])?;
		}
		Ok(())
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

#[cfg(test)]
mod tests {
	use crate::sql::{Bytes, Value};

	#[test]
	fn serialize() {
		let val = Value::Bytes(Bytes(vec![1, 2, 3, 5]));
		let serialized: Vec<u8> = val.clone().into();
		println!("{serialized:?}");
		let deserialized = Value::from(serialized);
		assert_eq!(val, deserialized);
	}
}
