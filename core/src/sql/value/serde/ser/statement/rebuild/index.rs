use crate::err::Error;
use crate::sql::statements::rebuild::RebuildIndexStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RebuildIndexStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RebuildIndexStatement, Error>;
	type SerializeTuple = Impossible<RebuildIndexStatement, Error>;
	type SerializeTupleStruct = Impossible<RebuildIndexStatement, Error>;
	type SerializeTupleVariant = Impossible<RebuildIndexStatement, Error>;
	type SerializeMap = Impossible<RebuildIndexStatement, Error>;
	type SerializeStruct = SerializeRebuildIndexStatement;
	type SerializeStructVariant = Impossible<RebuildIndexStatement, Error>;

	const EXPECTED: &'static str = "a struct `RebuildIndexStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRebuildIndexStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRebuildIndexStatement {
	name: Ident,
	what: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRebuildIndexStatement {
	type Ok = RebuildIndexStatement;
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
					"unexpected field `RebuildIndexStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RebuildIndexStatement {
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
		let stmt = RebuildIndexStatement::default();
		let value: RebuildIndexStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
