use crate::err::Error;
use crate::sql::statements::RemoveFunctionStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveFunctionStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveFunctionStatement, Error>;
	type SerializeTuple = Impossible<RemoveFunctionStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveFunctionStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveFunctionStatement, Error>;
	type SerializeMap = Impossible<RemoveFunctionStatement, Error>;
	type SerializeStruct = SerializeRemoveFunctionStatement;
	type SerializeStructVariant = Impossible<RemoveFunctionStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveFunctionStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveFunctionStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeRemoveFunctionStatement {
	name: Ident,
}

impl serde::ser::SerializeStruct for SerializeRemoveFunctionStatement {
	type Ok = RemoveFunctionStatement;
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
					"unexpected field `RemoveFunctionStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveFunctionStatement {
			name: self.name,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveFunctionStatement::default();
		let value: RemoveFunctionStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
