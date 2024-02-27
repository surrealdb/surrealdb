use crate::err::Error;
use crate::sql::statements::RemoveUserStatement;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveUserStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveUserStatement, Error>;
	type SerializeTuple = Impossible<RemoveUserStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveUserStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveUserStatement, Error>;
	type SerializeMap = Impossible<RemoveUserStatement, Error>;
	type SerializeStruct = SerializeRemoveUserStatement;
	type SerializeStructVariant = Impossible<RemoveUserStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveUserStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveUserStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeRemoveUserStatement {
	name: Ident,
	base: Base,
}

impl serde::ser::SerializeStruct for SerializeRemoveUserStatement {
	type Ok = RemoveUserStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"base" => {
				self.base = value.serialize(ser::base::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveUserStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveUserStatement {
			name: self.name,
			base: self.base,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveUserStatement::default();
		let value: RemoveUserStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
