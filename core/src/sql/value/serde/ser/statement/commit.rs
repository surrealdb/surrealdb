use crate::err::Error;
use crate::sql::statements::CommitStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = CommitStatement;
	type Error = Error;

	type SerializeSeq = Impossible<CommitStatement, Error>;
	type SerializeTuple = Impossible<CommitStatement, Error>;
	type SerializeTupleStruct = Impossible<CommitStatement, Error>;
	type SerializeTupleVariant = Impossible<CommitStatement, Error>;
	type SerializeMap = Impossible<CommitStatement, Error>;
	type SerializeStruct = Impossible<CommitStatement, Error>;
	type SerializeStructVariant = Impossible<CommitStatement, Error>;

	const EXPECTED: &'static str = "a unit struct `CommitStatement`";

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Error> {
		match name {
			"CommitStatement" => Ok(CommitStatement),
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
		let stmt = CommitStatement;
		let value: CommitStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
