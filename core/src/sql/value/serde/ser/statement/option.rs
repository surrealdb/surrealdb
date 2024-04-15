use crate::err::Error;
use crate::sql::statements::OptionStatement;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = OptionStatement;
	type Error = Error;

	type SerializeSeq = Impossible<OptionStatement, Error>;
	type SerializeTuple = Impossible<OptionStatement, Error>;
	type SerializeTupleStruct = Impossible<OptionStatement, Error>;
	type SerializeTupleVariant = Impossible<OptionStatement, Error>;
	type SerializeMap = Impossible<OptionStatement, Error>;
	type SerializeStruct = SerializeOptionStatement;
	type SerializeStructVariant = Impossible<OptionStatement, Error>;

	const EXPECTED: &'static str = "a struct `OptionStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeOptionStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeOptionStatement {
	name: Ident,
	what: bool,
}

impl serde::ser::SerializeStruct for SerializeOptionStatement {
	type Ok = OptionStatement;
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
				self.what = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `OptionStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(OptionStatement {
			name: self.name,
			what: self.what,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = OptionStatement::default();
		let value: OptionStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
