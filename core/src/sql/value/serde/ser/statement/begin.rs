use crate::err::Error;
use crate::sql::statements::BeginStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = BeginStatement;
	type Error = Error;

	type SerializeSeq = Impossible<BeginStatement, Error>;
	type SerializeTuple = Impossible<BeginStatement, Error>;
	type SerializeTupleStruct = Impossible<BeginStatement, Error>;
	type SerializeTupleVariant = Impossible<BeginStatement, Error>;
	type SerializeMap = Impossible<BeginStatement, Error>;
	type SerializeStruct = Impossible<BeginStatement, Error>;
	type SerializeStructVariant = Impossible<BeginStatement, Error>;

	const EXPECTED: &'static str = "a unit struct `BeginStatement`";

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Error> {
		match name {
			"BeginStatement" => Ok(BeginStatement),
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
		let stmt = BeginStatement;
		let value: BeginStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
