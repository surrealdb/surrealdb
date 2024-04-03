use crate::err::Error;
use crate::sql::statements::RemoveTableStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveTableStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveTableStatement, Error>;
	type SerializeTuple = Impossible<RemoveTableStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveTableStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveTableStatement, Error>;
	type SerializeMap = Impossible<RemoveTableStatement, Error>;
	type SerializeStruct = SerializeRemoveTableStatement;
	type SerializeStructVariant = Impossible<RemoveTableStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveTableStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveTableStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveTableStatement {
	name: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveTableStatement {
	type Ok = RemoveTableStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveTableStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveTableStatement {
			name: self.name,
			if_exists: self.if_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveTableStatement::default();
		let value: RemoveTableStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
