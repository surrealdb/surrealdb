use crate::err::Error;
use crate::sql::statements::RemoveIndexStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveIndexStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveIndexStatement, Error>;
	type SerializeTuple = Impossible<RemoveIndexStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveIndexStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveIndexStatement, Error>;
	type SerializeMap = Impossible<RemoveIndexStatement, Error>;
	type SerializeStruct = SerializeRemoveIndexStatement;
	type SerializeStructVariant = Impossible<RemoveIndexStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveIndexStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveIndexStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveIndexStatement {
	name: Ident,
	what: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveIndexStatement {
	type Ok = RemoveIndexStatement;
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
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveIndexStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveIndexStatement {
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
		let stmt = RemoveIndexStatement::default();
		let value: RemoveIndexStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
