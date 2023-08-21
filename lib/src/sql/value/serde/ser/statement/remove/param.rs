use crate::err::Error;
use crate::sql::statements::RemoveParamStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveParamStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveParamStatement, Error>;
	type SerializeTuple = Impossible<RemoveParamStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveParamStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveParamStatement, Error>;
	type SerializeMap = Impossible<RemoveParamStatement, Error>;
	type SerializeStruct = SerializeRemoveParamStatement;
	type SerializeStructVariant = Impossible<RemoveParamStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveParamStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveParamStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeRemoveParamStatement {
	name: Ident,
}

impl serde::ser::SerializeStruct for SerializeRemoveParamStatement {
	type Ok = RemoveParamStatement;
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
					"unexpected field `RemoveParamStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveParamStatement {
			name: self.name,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveParamStatement::default();
		let value: RemoveParamStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
