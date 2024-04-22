use crate::err::Error;
use crate::sql::statements::RemoveFieldStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Idiom;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveFieldStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveFieldStatement, Error>;
	type SerializeTuple = Impossible<RemoveFieldStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveFieldStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveFieldStatement, Error>;
	type SerializeMap = Impossible<RemoveFieldStatement, Error>;
	type SerializeStruct = SerializeRemoveFieldStatement;
	type SerializeStructVariant = Impossible<RemoveFieldStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveFieldStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveFieldStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveFieldStatement {
	name: Idiom,
	what: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveFieldStatement {
	type Ok = RemoveFieldStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Idiom(value.serialize(ser::part::vec::Serializer.wrap())?);
			}
			"what" => {
				self.what = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveFieldStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveFieldStatement {
			name: self.name,
			what: self.what,
			if_exists: self.if_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveFieldStatement::default();
		let value: RemoveFieldStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
