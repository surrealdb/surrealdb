use crate::err::Error;
use crate::sql::statements::DefineFieldStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Idiom;
use crate::sql::Kind;
use crate::sql::Permissions;
use crate::sql::Strand;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineFieldStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineFieldStatement, Error>;
	type SerializeTuple = Impossible<DefineFieldStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineFieldStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineFieldStatement, Error>;
	type SerializeMap = Impossible<DefineFieldStatement, Error>;
	type SerializeStruct = SerializeDefineFieldStatement;
	type SerializeStructVariant = Impossible<DefineFieldStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineFieldStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineFieldStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineFieldStatement {
	name: Idiom,
	what: Ident,
	flex: bool,
	kind: Option<Kind>,
	readonly: bool,
	value: Option<Value>,
	assert: Option<Value>,
	default: Option<Value>,
	permissions: Permissions,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineFieldStatement {
	type Ok = DefineFieldStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Idiom(value.serialize(ser::part::vec::Serializer.wrap())?);
			}
			"what" => {
				self.what = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"flex" => {
				self.flex = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::kind::opt::Serializer.wrap())?;
			}
			"readonly" => {
				self.readonly = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"value" => {
				self.value = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"assert" => {
				self.assert = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"default" => {
				self.default = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permissions::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineFieldStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineFieldStatement {
			name: self.name,
			what: self.what,
			flex: self.flex,
			kind: self.kind,
			readonly: self.readonly,
			value: self.value,
			assert: self.assert,
			default: self.default,
			permissions: self.permissions,
			comment: self.comment,
			if_not_exists: self.if_not_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineFieldStatement::default();
		let value: DefineFieldStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
