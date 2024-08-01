use crate::err::Error;
use crate::sql::statements::DefineEventStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Strand;
use crate::sql::Value;
use crate::sql::Values;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineEventStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineEventStatement, Error>;
	type SerializeTuple = Impossible<DefineEventStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineEventStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineEventStatement, Error>;
	type SerializeMap = Impossible<DefineEventStatement, Error>;
	type SerializeStruct = SerializeDefineEventStatement;
	type SerializeStructVariant = Impossible<DefineEventStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineEventStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineEventStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDefineEventStatement {
	name: Ident,
	what: Ident,
	when: Value,
	then: Values,
	comment: Option<Strand>,
	if_not_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeDefineEventStatement {
	type Ok = DefineEventStatement;
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
			"when" => {
				self.when = value.serialize(ser::value::Serializer.wrap())?;
			}
			"then" => {
				self.then = value.serialize(ser::values::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			"if_not_exists" => {
				self.if_not_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineEventStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineEventStatement {
			name: self.name,
			what: self.what,
			when: self.when,
			then: self.then,
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
		let stmt = DefineEventStatement::default();
		let value: DefineEventStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
