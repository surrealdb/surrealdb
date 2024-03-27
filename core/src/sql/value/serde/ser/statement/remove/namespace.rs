use crate::err::Error;
use crate::sql::statements::RemoveNamespaceStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveNamespaceStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveNamespaceStatement, Error>;
	type SerializeTuple = Impossible<RemoveNamespaceStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveNamespaceStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveNamespaceStatement, Error>;
	type SerializeMap = Impossible<RemoveNamespaceStatement, Error>;
	type SerializeStruct = SerializeRemoveNamespaceStatement;
	type SerializeStructVariant = Impossible<RemoveNamespaceStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveNamespaceStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveNamespaceStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveNamespaceStatement {
	name: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveNamespaceStatement {
	type Ok = RemoveNamespaceStatement;
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
					"unexpected field `RemoveNamespaceStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveNamespaceStatement {
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
		let stmt = RemoveNamespaceStatement::default();
		let value: RemoveNamespaceStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
