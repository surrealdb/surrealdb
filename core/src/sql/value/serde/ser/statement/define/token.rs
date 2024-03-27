use crate::err::Error;
use crate::sql::statements::DefineTokenStatement;
use crate::sql::value::serde::ser;
use crate::sql::Algorithm;
use crate::sql::Base;
use crate::sql::Ident;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineTokenStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineTokenStatement, Error>;
	type SerializeTuple = Impossible<DefineTokenStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineTokenStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineTokenStatement, Error>;
	type SerializeMap = Impossible<DefineTokenStatement, Error>;
	type SerializeStruct = SerializeDefineTokenStatement;
	type SerializeStructVariant = Impossible<DefineTokenStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineTokenStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineTokenStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineTokenStatement {
	name: Ident,
	base: Base,
	kind: Algorithm,
	code: String,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineTokenStatement {
	type Ok = DefineTokenStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"base" => {
				self.base = value.serialize(ser::base::Serializer.wrap())?;
			}
			"kind" => {
				self.kind = value.serialize(ser::algorithm::Serializer.wrap())?;
			}
			"code" => {
				self.code = value.serialize(ser::string::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineTokenStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineTokenStatement {
			name: self.name,
			base: self.base,
			kind: self.kind,
			code: self.code,
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
		let stmt = DefineTokenStatement::default();
		let value: DefineTokenStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
