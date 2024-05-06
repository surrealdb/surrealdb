use crate::err::Error;
use crate::sql::statements::RemoveAccessStatement;
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
	type Ok = RemoveAccessStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveAccessStatement, Error>;
	type SerializeTuple = Impossible<RemoveAccessStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveAccessStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveAccessStatement, Error>;
	type SerializeMap = Impossible<RemoveAccessStatement, Error>;
	type SerializeStruct = SerializeRemoveAccessStatement;
	type SerializeStructVariant = Impossible<RemoveAccessStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveAccessStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveAccessStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveAccessStatement {
	name: Ident,
	base: Base,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveAccessStatement {
	type Ok = RemoveAccessStatement;
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
					"unexpected field `RemoveAccessStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveAccessStatement {
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
		let stmt = RemoveAccessStatement::default();
		let value: RemoveAccessStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
