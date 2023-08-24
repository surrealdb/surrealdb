use crate::err::Error;
use crate::sql::statements::DefineNamespaceStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Strand;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineNamespaceStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineNamespaceStatement, Error>;
	type SerializeTuple = Impossible<DefineNamespaceStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineNamespaceStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineNamespaceStatement, Error>;
	type SerializeMap = Impossible<DefineNamespaceStatement, Error>;
	type SerializeStruct = SerializeDefineNamespaceStatement;
	type SerializeStructVariant = Impossible<DefineNamespaceStatement, Error>;

	const EXPECTED: &'static str = "a struct `DefineNamespaceStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDefineNamespaceStatement::default())
	}
}

#[derive(Default)]
pub struct SerializeDefineNamespaceStatement {
	name: Ident,
	id: Option<u32>,
	comment: Option<Strand>,
}

impl serde::ser::SerializeStruct for SerializeDefineNamespaceStatement {
	type Ok = DefineNamespaceStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Ident(value.serialize(ser::string::Serializer.wrap())?);
			}
			"id" => {
				self.id = value.serialize(ser::primitive::u32::opt::Serializer.wrap())?;
			}
			"comment" => {
				self.comment = value.serialize(ser::strand::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `DefineNamespaceStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(DefineNamespaceStatement {
			name: self.name,
			id: self.id,
			comment: self.comment,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DefineNamespaceStatement::default();
		let value: DefineNamespaceStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
