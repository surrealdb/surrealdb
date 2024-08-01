use crate::err::Error;
use crate::sql::statements::AlterFieldStatement;
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
	type Ok = AlterFieldStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AlterFieldStatement, Error>;
	type SerializeTuple = Impossible<AlterFieldStatement, Error>;
	type SerializeTupleStruct = Impossible<AlterFieldStatement, Error>;
	type SerializeTupleVariant = Impossible<AlterFieldStatement, Error>;
	type SerializeMap = Impossible<AlterFieldStatement, Error>;
	type SerializeStruct = SerializeAlterFieldStatement;
	type SerializeStructVariant = Impossible<AlterFieldStatement, Error>;

	const EXPECTED: &'static str = "a struct `AlterFieldStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeAlterFieldStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeAlterFieldStatement {
	name: Idiom,
	what: Ident,
	if_exists: bool,
	flex: Option<bool>,
	kind: Option<Option<Kind>>,
	readonly: Option<bool>,
	value: Option<Option<Value>>,
	assert: Option<Option<Value>>,
	default: Option<Option<Value>>,
	permissions: Option<Permissions>,
	comment: Option<Option<Strand>>,
}

impl serde::ser::SerializeStruct for SerializeAlterFieldStatement {
	type Ok = AlterFieldStatement;
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
			"if_exists" => {
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			"flex" => {
				self.flex = value.serialize(ser::primitive::opt::bool::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::kind::opt::opt::Serializer.wrap())?;
			}
			"readonly" => {
				self.readonly = value.serialize(ser::primitive::opt::bool::Serializer.wrap())?;
			}
			"value" => {
				self.value = value.serialize(ser::value::opt::opt::Serializer.wrap())?;
			}
			"assert" => {
				self.assert = value.serialize(ser::value::opt::opt::Serializer.wrap())?;
			}
			"default" => {
				self.default = value.serialize(ser::value::opt::opt::Serializer.wrap())?;
			}
			"permissions" => {
				self.permissions = value.serialize(ser::permissions::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `AlterFieldStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(AlterFieldStatement {
			name: self.name,
			what: self.what,
			if_exists: self.if_exists,
			flex: self.flex,
			kind: self.kind,
			readonly: self.readonly,
			value: self.value,
			assert: self.assert,
			default: self.default,
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
		let stmt = AlterFieldStatement::default();
		let value: AlterFieldStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
