use crate::err::Error;
use crate::sql::value::serde::ser;
use bigdecimal::BigDecimal;
use serde::ser::Error as _;
use serde::ser::Impossible;
use std::fmt::Display;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = BigDecimal;
	type Error = Error;

	type SerializeSeq = Impossible<BigDecimal, Error>;
	type SerializeTuple = Impossible<BigDecimal, Error>;
	type SerializeTupleStruct = Impossible<BigDecimal, Error>;
	type SerializeTupleVariant = Impossible<BigDecimal, Error>;
	type SerializeMap = Impossible<BigDecimal, Error>;
	type SerializeStruct = Impossible<BigDecimal, Error>;
	type SerializeStructVariant = Impossible<BigDecimal, Error>;

	const EXPECTED: &'static str = "a struct `BigDecimal`";

	#[inline]
	fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: Display,
	{
		value.to_string().parse::<BigDecimal>().map_err(Error::custom)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn from_i32() {
		let decimal = BigDecimal::from(25);
		let serialized = serialize_internal(|| decimal.serialize(Serializer.wrap())).unwrap();
		assert_eq!(decimal, serialized);
	}
}
