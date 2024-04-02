use crate::err::Error;
use crate::sql::statements::RemoveEventStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveEventStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveEventStatement, Error>;
	type SerializeTuple = Impossible<RemoveEventStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveEventStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveEventStatement, Error>;
	type SerializeMap = Impossible<RemoveEventStatement, Error>;
	type SerializeStruct = SerializeRemoveEventStatement;
	type SerializeStructVariant = Impossible<RemoveEventStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveEventStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveEventStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveEventStatement {
	name: Ident,
	what: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveEventStatement {
	type Ok = RemoveEventStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"what" => {
				self.what = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveEventStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveEventStatement {
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
		let stmt = RemoveEventStatement::default();
		let value: RemoveEventStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
