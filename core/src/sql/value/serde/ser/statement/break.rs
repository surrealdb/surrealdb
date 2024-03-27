use crate::err::Error;
use crate::sql::statements::BreakStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = BreakStatement;
	type Error = Error;

	type SerializeSeq = Impossible<BreakStatement, Error>;
	type SerializeTuple = Impossible<BreakStatement, Error>;
	type SerializeTupleStruct = Impossible<BreakStatement, Error>;
	type SerializeTupleVariant = Impossible<BreakStatement, Error>;
	type SerializeMap = Impossible<BreakStatement, Error>;
	type SerializeStruct = Impossible<BreakStatement, Error>;
	type SerializeStructVariant = Impossible<BreakStatement, Error>;

	const EXPECTED: &'static str = "a unit struct `BreakStatement`";

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Error> {
		match name {
			"BreakStatement" => Ok(BreakStatement),
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
		let stmt = BreakStatement;
		let value: BreakStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
