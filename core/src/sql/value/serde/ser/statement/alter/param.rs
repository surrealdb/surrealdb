use crate::err::Error;
use crate::sql::statements::AlterParamStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Permission;
use crate::sql::Strand;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = AlterParamStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AlterParamStatement, Error>;
	type SerializeTuple = Impossible<AlterParamStatement, Error>;
	type SerializeTupleStruct = Impossible<AlterParamStatement, Error>;
	type SerializeTupleVariant = Impossible<AlterParamStatement, Error>;
	type SerializeMap = Impossible<AlterParamStatement, Error>;
	type SerializeStruct = SerializeAlterParamStatement;
	type SerializeStructVariant = Impossible<AlterParamStatement, Error>;

	const EXPECTED: &'static str = "a struct `AlterParamStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeAlterParamStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeAlterParamStatement {
	name: Ident,
	if_exists: bool,
	value: Option<Value>,
	permissions: Option<Permission>,
	comment: Option<Option<Strand>>,
}

impl serde::ser::SerializeStruct for SerializeAlterParamStatement {
	type Ok = AlterParamStatement;
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
			"value" => {
				self.value = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permission::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected Param `AlterParamStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(AlterParamStatement {
			name: self.name,
			if_exists: self.if_exists,
			value: self.value,
			permissions: self.permissions,
			comment: self.comment,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = AlterParamStatement::default();
		let value: AlterParamStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
