use crate::err::Error;
use crate::sql::statements::RemoveTokenStatement;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveTokenStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveTokenStatement, Error>;
	type SerializeTuple = Impossible<RemoveTokenStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveTokenStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveTokenStatement, Error>;
	type SerializeMap = Impossible<RemoveTokenStatement, Error>;
	type SerializeStruct = SerializeRemoveTokenStatement;
	type SerializeStructVariant = Impossible<RemoveTokenStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveTokenStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveTokenStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveTokenStatement {
	name: Ident,
	base: Base,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveTokenStatement {
	type Ok = RemoveTokenStatement;
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
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveTokenStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveTokenStatement {
			name: self.name,
			base: self.base,
			if_exists: self.if_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveTokenStatement::default();
		let value: RemoveTokenStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
