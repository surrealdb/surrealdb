use crate::err::Error;
use crate::sql::statements::remove::RemoveAnalyzerStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveAnalyzerStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveAnalyzerStatement, Error>;
	type SerializeTuple = Impossible<RemoveAnalyzerStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveAnalyzerStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveAnalyzerStatement, Error>;
	type SerializeMap = Impossible<RemoveAnalyzerStatement, Error>;
	type SerializeStruct = SerializeRemoveAnalyzerStatement;
	type SerializeStructVariant = Impossible<RemoveAnalyzerStatement, Error>;

	const EXPECTED: &'static str = "a struct `RemoveAnalyzerStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRemoveAnalyzerStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRemoveAnalyzerStatement {
	name: Ident,
	if_exists: bool,
}

impl serde::ser::SerializeStruct for SerializeRemoveAnalyzerStatement {
	type Ok = RemoveAnalyzerStatement;
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
				self.if_exists = value.serialize(ser::primitive::bool::Serializer.wrap())?
			}
			key => {
				return Err(Error::custom(format!(
					"unexpected field `RemoveAnalyzerStatement::{key}`"
				)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(RemoveAnalyzerStatement {
			name: self.name,
			if_exists: self.if_exists,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RemoveAnalyzerStatement::default();
		let value: RemoveAnalyzerStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
