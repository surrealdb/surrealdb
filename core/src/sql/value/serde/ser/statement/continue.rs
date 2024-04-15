use crate::err::Error;
use crate::sql::statements::ContinueStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ContinueStatement;
	type Error = Error;

	type SerializeSeq = Impossible<ContinueStatement, Error>;
	type SerializeTuple = Impossible<ContinueStatement, Error>;
	type SerializeTupleStruct = Impossible<ContinueStatement, Error>;
	type SerializeTupleVariant = Impossible<ContinueStatement, Error>;
	type SerializeMap = Impossible<ContinueStatement, Error>;
	type SerializeStruct = Impossible<ContinueStatement, Error>;
	type SerializeStructVariant = Impossible<ContinueStatement, Error>;

	const EXPECTED: &'static str = "a unit struct `ContinueStatement`";

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Error> {
		match name {
			"ContinueStatement" => Ok(ContinueStatement),
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
		let stmt = ContinueStatement;
		let value: ContinueStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
