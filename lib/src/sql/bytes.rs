use serde::Deserialize;
use serde::Serialize;
use std::ops::Deref;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Bytes(pub(super) Vec<u8>);

impl Deref for Bytes {
	type Target = Vec<u8>;

	fn deref(&self) -> &Self::Target {
		&self.0
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
