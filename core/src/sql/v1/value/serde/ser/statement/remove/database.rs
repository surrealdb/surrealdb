use crate::err::Error;
use crate::sql::statements::RemoveDatabaseStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveDatabaseStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveDatabaseStatement, Error>;
	type SerializeTuple = Impossible<RemoveDatabaseStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveDatabaseStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveDatabaseStatement, Error>;
	type SerializeMap = Impossible<RemoveDatabaseStatement, Error>;
	type SerializeStruct = SerializeRemoveDatabaseStatement;
	type SerializeStructVariant = Impossible<RemoveDatabaseStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveDatabaseStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveDatabaseStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeRemoveDatabaseStatement {
	name: Ident,
}

impl serde::ser::SerializeStruct for SerializeRemoveDatabaseStatement {
	type Ok = RemoveDatabaseStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveDatabaseStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveDatabaseStatement {
			name: self.name,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveDatabaseStatement::default();
		let value: RemoveDatabaseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
