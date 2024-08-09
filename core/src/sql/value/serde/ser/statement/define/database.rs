use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::statements::DefineDatabaseStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineDatabaseStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineDatabaseStatement, Error>;
	type SerializeTuple = Impossible<DefineDatabaseStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineDatabaseStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineDatabaseStatement, Error>;
	type SerializeMap = Impossible<DefineDatabaseStatement, Error>;
	type SerializeStruct = SerializeDefineDatabaseStatement;
	type SerializeStructVariant = Impossible<DefineDatabaseStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineDatabaseStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineDatabaseStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineDatabaseStatement {
	name: Ident,
	changefeed: Option<ChangeFeed>,
	id: Option<u32>,
	comment: Option<Strand>,
	if_not_exists: bool,
	overwrite: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineDatabaseStatement {
	type Ok = DefineDatabaseStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"changefeed" => {
				self.changefeed = value.serialize(ser::changefeed::opt::Serializer.wrap())?;
			}
			"id" => {
				self.id = value.serialize(ser::primitive::u32::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			"overwrite" => {
				self.overwrite = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineDatabaseStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineDatabaseStatement {
			name: self.name,
			changefeed: self.changefeed,
			id: self.id,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
			overwrite: self.overwrite,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineDatabaseStatement::default();
		let value: DefineDatabaseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
