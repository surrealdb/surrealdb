use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = i64;
	type Error = Error;

	type SerializeSeq = Impossible<i64, Error>;
	type SerializeTuple = Impossible<i64, Error>;
	type SerializeTupleStruct = Impossible<i64, Error>;
	type SerializeTupleVariant = Impossible<i64, Error>;
	type SerializeMap = Impossible<i64, Error>;
	type SerializeStruct = Impossible<i64, Error>;
	type SerializeStructVariant = Impossible<i64, Error>;

	const EXPECTED: &'static str = "an i64";

	#[inline]
	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
