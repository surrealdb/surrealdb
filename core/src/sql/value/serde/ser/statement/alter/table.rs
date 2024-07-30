use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::statements::AlterTableStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Permissions;
use crate::sql::Strand;
use crate::sql::TableType;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = AlterTableStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AlterTableStatement, Error>;
	type SerializeTuple = Impossible<AlterTableStatement, Error>;
	type SerializeTupleStruct = Impossible<AlterTableStatement, Error>;
	type SerializeTupleVariant = Impossible<AlterTableStatement, Error>;
	type SerializeMap = Impossible<AlterTableStatement, Error>;
	type SerializeStruct = SerializeAlterTableStatement;
	type SerializeStructVariant = Impossible<AlterTableStatement, Error>;

	const EXPECTED: &'static str = "a struct `AlterTableStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeAlterTableStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeAlterTableStatement {
	name: Ident,
	drop: Option<bool>,
	full: Option<bool>,
	permissions: Option<Permissions>,
	changefeed: Option<Option<ChangeFeed>>,
	comment: Option<Option<Strand>>,
	if_exists: bool,
	kind: Option<TableType>,
}

impl serde::ser::SerializeStruct for SerializeAlterTableStatement {
	type Ok = AlterTableStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"drop" => {
				self.drop = value.serialize(ser::primitive::opt::bool::Serializer.wrap())?;
			}
			"full" => {
				self.full = value.serialize(ser::primitive::opt::bool::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permissions::opt::Serializer.wrap())?;
			}
			"changefeed" => {
				self.changefeed = value.serialize(ser::changefeed::opt::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::opt::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::table_type::opt::Serializer.wrap())?;
			}
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `AlterTableStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(AlterTableStatement {
			name: self.name,
			drop: self.drop,
			full: self.full,
			permissions: self.permissions,
			changefeed: self.changefeed,
			comment: self.comment,
			kind: self.kind,
			if_exists: self.if_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = AlterTableStatement::default();
		let value: AlterTableStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
