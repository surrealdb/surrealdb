use crate::err::Error;
use crate::sql::value::serde::ser;
use rust_decimal::Decimal;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Decimal;
	type Error = Error;

	type SerializeSeq = Impossible<Decimal, Error>;
	type SerializeTuple = Impossible<Decimal, Error>;
	type SerializeTupleStruct = Impossible<Decimal, Error>;
	type SerializeTupleVariant = Impossible<Decimal, Error>;
	type SerializeMap = Impossible<Decimal, Error>;
	type SerializeStruct = Impossible<Decimal, Error>;
	type SerializeStructVariant = Impossible<Decimal, Error>;

	const EXPECTED: &'static str = "a struct `Decimal`";

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
		value.parse::<Decimal>().map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn from_i32() {
		let decimal = Decimal::from(25);
		let serialized = Serialize::serialize(&decimal, Serializer.wrap()).unwrap();
		assert_eq!(decimal, serialized);
	}
}
