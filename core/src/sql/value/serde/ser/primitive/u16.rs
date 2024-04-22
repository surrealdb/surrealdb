use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = u16;
	type Error = Error;

	type SerializeSeq = Impossible<u16, Error>;
	type SerializeTuple = Impossible<u16, Error>;
	type SerializeTupleStruct = Impossible<u16, Error>;
	type SerializeTupleVariant = Impossible<u16, Error>;
	type SerializeMap = Impossible<u16, Error>;
	type SerializeStruct = Impossible<u16, Error>;
	type SerializeStructVariant = Impossible<u16, Error>;

	const EXPECTED: &'static str = "a u16";

	#[inline]
	fn serialize_u16(self, value: u16) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
