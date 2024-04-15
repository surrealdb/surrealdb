use crate::err::Error;
use crate::sql::statements::RemoveScopeStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveScopeStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveScopeStatement, Error>;
	type SerializeTuple = Impossible<RemoveScopeStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveScopeStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveScopeStatement, Error>;
	type SerializeMap = Impossible<RemoveScopeStatement, Error>;
	type SerializeStruct = SerializeRemoveScopeStatement;
	type SerializeStructVariant = Impossible<RemoveScopeStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveScopeStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveScopeStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveScopeStatement {
	name: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveScopeStatement {
	type Ok = RemoveScopeStatement;
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
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveScopeStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveScopeStatement {
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
		let stmt = RemoveScopeStatement::default();
		let value: RemoveScopeStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
