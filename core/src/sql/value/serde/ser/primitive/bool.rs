use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = bool;
	type Error = Error;

	type SerializeSeq = Impossible<bool, Error>;
	type SerializeTuple = Impossible<bool, Error>;
	type SerializeTupleStruct = Impossible<bool, Error>;
	type SerializeTupleVariant = Impossible<bool, Error>;
	type SerializeMap = Impossible<bool, Error>;
	type SerializeStruct = Impossible<bool, Error>;
	type SerializeStructVariant = Impossible<bool, Error>;

	const EXPECTED: &'static str = "a boolean";

	#[inline]
	fn serialize_bool(self, value: bool) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
