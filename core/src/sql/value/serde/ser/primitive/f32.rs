use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = f32;
	type Error = Error;

	type SerializeSeq = Impossible<f32, Error>;
	type SerializeTuple = Impossible<f32, Error>;
	type SerializeTupleStruct = Impossible<f32, Error>;
	type SerializeTupleVariant = Impossible<f32, Error>;
	type SerializeMap = Impossible<f32, Error>;
	type SerializeStruct = Impossible<f32, Error>;
	type SerializeStructVariant = Impossible<f32, Error>;

	const EXPECTED: &'static str = "an f32";

	#[inline]
	fn serialize_f32(self, value: f32) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
