use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use uuid::Uuid;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Uuid;
	type Error = Error;

	type SerializeSeq = Impossible<Uuid, Error>;
	type SerializeTuple = Impossible<Uuid, Error>;
	type SerializeTupleStruct = Impossible<Uuid, Error>;
	type SerializeTupleVariant = Impossible<Uuid, Error>;
	type SerializeMap = Impossible<Uuid, Error>;
	type SerializeStruct = Impossible<Uuid, Error>;
	type SerializeStructVariant = Impossible<Uuid, Error>;

	const EXPECTED: &'static str = "a UUID";

	fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
		Uuid::from_slice(value).map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn nil() {
		let uuid = Uuid::nil();
		let serialized = uuid.serialize(Serializer.wrap()).unwrap();
		assert_eq!(uuid, serialized);
	}

	#[test]
	fn max() {
		let uuid = Uuid::max();
		let serialized = uuid.serialize(Serializer.wrap()).unwrap();
		assert_eq!(uuid, serialized);
	}
}
