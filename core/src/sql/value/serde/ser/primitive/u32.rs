pub use super::opt::u32 as opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = u32;
	type Error = Error;

	type SerializeSeq = Impossible<u32, Error>;
	type SerializeTuple = Impossible<u32, Error>;
	type SerializeTupleStruct = Impossible<u32, Error>;
	type SerializeTupleVariant = Impossible<u32, Error>;
	type SerializeMap = Impossible<u32, Error>;
	type SerializeStruct = Impossible<u32, Error>;
	type SerializeStructVariant = Impossible<u32, Error>;

	const EXPECTED: &'static str = "a u32";

	#[inline]
	fn serialize_u32(self, value: u32) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
