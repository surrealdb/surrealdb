use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = f64;
	type Error = Error;

	type SerializeSeq = Impossible<f64, Error>;
	type SerializeTuple = Impossible<f64, Error>;
	type SerializeTupleStruct = Impossible<f64, Error>;
	type SerializeTupleVariant = Impossible<f64, Error>;
	type SerializeMap = Impossible<f64, Error>;
	type SerializeStruct = Impossible<f64, Error>;
	type SerializeStructVariant = Impossible<f64, Error>;

	const EXPECTED: &'static str = "an f64";

	#[inline]
	fn serialize_f64(self, value: f64) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
