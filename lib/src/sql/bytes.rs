use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Bytes(pub(super) Vec<u8>);

impl Serialize for Bytes {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		serializer.serialize_bytes(&self.0)
	}
}
