use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::statements::DefineTableStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Permissions;
use crate::sql::Strand;
use crate::sql::TableType;
use crate::sql::View;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineTableStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineTableStatement, Error>;
	type SerializeTuple = Impossible<DefineTableStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineTableStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineTableStatement, Error>;
	type SerializeMap = Impossible<DefineTableStatement, Error>;
	type SerializeStruct = SerializeDefineTableStatement;
	type SerializeStructVariant = Impossible<DefineTableStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineTableStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineTableStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineTableStatement {
	name: Ident,
	drop: bool,
	full: bool,
	id: Option<u32>,
	view: Option<View>,
	permissions: Permissions,
	changefeed: Option<ChangeFeed>,
	comment: Option<Strand>,
	if_not_exists: bool,
	overwrite: bool,
	kind: TableType,
}

impl serde::ser::SerializeStruct for SerializeDefineTableStatement {
	type Ok = DefineTableStatement;
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
				self.drop = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"full" => {
				self.full = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"id" => {
				self.id = value.serialize(ser::primitive::u32::opt::Serializer.wrap())?;
			}
			"view" => {
				self.view = value.serialize(ser::view::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permissions::Serializer.wrap())?;
			}
			"changefeed" => {
				self.changefeed = value.serialize(ser::changefeed::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::table_type::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			"overwrite" => {
				self.overwrite = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineTableStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineTableStatement {
			name: self.name,
			drop: self.drop,
			full: self.full,
			id: self.id,
			view: self.view,
			permissions: self.permissions,
			changefeed: self.changefeed,
			comment: self.comment,
			kind: self.kind,
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
		let stmt = DefineTableStatement::default();
		let value: DefineTableStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
