use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = u8;
	type Error = Error;

	type SerializeSeq = Impossible<u8, Error>;
	type SerializeTuple = Impossible<u8, Error>;
	type SerializeTupleStruct = Impossible<u8, Error>;
	type SerializeTupleVariant = Impossible<u8, Error>;
	type SerializeMap = Impossible<u8, Error>;
	type SerializeStruct = Impossible<u8, Error>;
	type SerializeStructVariant = Impossible<u8, Error>;

	const EXPECTED: &'static str = "a u8";

	#[inline]
	fn serialize_u8(self, value: u8) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
