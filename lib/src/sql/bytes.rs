use crate::sql::serde::is_internal_serialization;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Deserialize, Hash)]
pub struct Bytes(pub(super) Vec<u8>);

impl Serialize for Bytes {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			serializer.serialize_bytes(&self.0)
		} else {
			serializer.serialize_none()
		}
	}
}
