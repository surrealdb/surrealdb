pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;
use uuid::Uuid;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Uuid;
	type Error = Error;

	type SerializeSeq = Impossible<Uuid, Error>;
	type SerializeTuple = SerializeCompactUuidTuple;
	type SerializeTupleStruct = Impossible<Uuid, Error>;
	type SerializeTupleVariant = Impossible<Uuid, Error>;
	type SerializeMap = Impossible<Uuid, Error>;
	type SerializeStruct = Impossible<Uuid, Error>;
	type SerializeStructVariant = Impossible<Uuid, Error>;

	const EXPECTED: &'static str = "a UUID";

	fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
		Uuid::from_slice(value).map_err(Error::custom)
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeCompactUuidTuple::default())
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}
}

#[derive(Default)]
pub(super) struct SerializeCompactUuidTuple {
	index: usize,
	bytes: [u8; 16],
}

impl serde::ser::SerializeTuple for SerializeCompactUuidTuple {
	type Ok = Uuid;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.bytes[self.index] = value.serialize(ser::primitive::u8::Serializer.wrap())?;
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Uuid::from_slice(&self.bytes).map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
