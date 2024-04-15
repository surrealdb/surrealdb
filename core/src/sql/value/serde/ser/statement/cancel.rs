use crate::err::Error;
use crate::sql::statements::CancelStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = CancelStatement;
	type Error = Error;

	type SerializeSeq = Impossible<CancelStatement, Error>;
	type SerializeTuple = Impossible<CancelStatement, Error>;
	type SerializeTupleStruct = Impossible<CancelStatement, Error>;
	type SerializeTupleVariant = Impossible<CancelStatement, Error>;
	type SerializeMap = Impossible<CancelStatement, Error>;
	type SerializeStruct = Impossible<CancelStatement, Error>;
	type SerializeStructVariant = Impossible<CancelStatement, Error>;

	const EXPECTED: &'static str = "a unit struct `CancelStatement`";

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Error> {
		match name {
			"CancelStatement" => Ok(CancelStatement),
			name => Err(Error::custom(format!("unexpected unit struct `{name}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn default() {
		let stmt = CancelStatement;
		let value: CancelStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
