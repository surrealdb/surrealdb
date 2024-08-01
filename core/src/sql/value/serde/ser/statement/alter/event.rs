use crate::err::Error;
use crate::sql::statements::AlterEventStatement;
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
	type Ok = AlterEventStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AlterEventStatement, Error>;
	type SerializeTuple = Impossible<AlterEventStatement, Error>;
	type SerializeTupleStruct = Impossible<AlterEventStatement, Error>;
	type SerializeTupleVariant = Impossible<AlterEventStatement, Error>;
	type SerializeMap = Impossible<AlterEventStatement, Error>;
	type SerializeStruct = SerializeAlterEventStatement;
	type SerializeStructVariant = Impossible<AlterEventStatement, Error>;

	const EXPECTED: &'static str = "a struct `AlterEventStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeAlterEventStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeAlterEventStatement {
	name: Ident,
	what: Ident,
	if_exists: bool,
	when: Option<Value>,
	then: Option<Values>,
	comment: Option<Option<Strand>>,
}

impl serde::ser::SerializeStruct for SerializeAlterEventStatement {
	type Ok = AlterEventStatement;
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
			"when" => {
				self.when = value.serialize(ser::value::opt::Serializer.wrap())?;
			}
			"then" => {
				self.then = value.serialize(ser::values::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected Event `AlterEventStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(AlterEventStatement {
			name: self.name,
			what: self.what,
			if_exists: self.if_exists,
			when: self.when,
			then: self.then,
			comment: self.comment,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = AlterEventStatement::default();
		let value: AlterEventStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
