use crate::err::Error;
use crate::sql::statements::DefineParamStatement;
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
	type Ok = DefineParamStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineParamStatement, Error>;
	type SerializeTuple = Impossible<DefineParamStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineParamStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineParamStatement, Error>;
	type SerializeMap = Impossible<DefineParamStatement, Error>;
	type SerializeStruct = SerializeDefineParamStatement;
	type SerializeStructVariant = Impossible<DefineParamStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineParamStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineParamStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineParamStatement {
	name: Ident,
	value: Value,
	comment: Option<Strand>,
	permissions: Permission,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineParamStatement {
	type Ok = DefineParamStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"value" => {
				self.value = value.serialize(ser::value::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permission::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineParamStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineParamStatement {
			name: self.name,
			value: self.value,
			comment: self.comment,
			permissions: self.permissions,
			if_not_exists: self.if_not_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineParamStatement::default();
		let value: DefineParamStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
