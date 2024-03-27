pub use super::opt::u64 as opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = u64;
	type Error = Error;

	type SerializeSeq = Impossible<u64, Error>;
	type SerializeTuple = Impossible<u64, Error>;
	type SerializeTupleStruct = Impossible<u64, Error>;
	type SerializeTupleVariant = Impossible<u64, Error>;
	type SerializeMap = Impossible<u64, Error>;
	type SerializeStruct = Impossible<u64, Error>;
	type SerializeStructVariant = Impossible<u64, Error>;

	const EXPECTED: &'static str = "a u64";

	#[inline]
	fn serialize_u64(self, value: u64) -> Result<Self::Ok, Error> {
		Ok(value)
	}
}
