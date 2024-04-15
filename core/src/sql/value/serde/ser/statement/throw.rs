use crate::err::Error;
use crate::sql::statements::ThrowStatement;
use crate::sql::value::serde::ser;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ThrowStatement;
	type Error = Error;

	type SerializeSeq = Impossible<ThrowStatement, Error>;
	type SerializeTuple = Impossible<ThrowStatement, Error>;
	type SerializeTupleStruct = Impossible<ThrowStatement, Error>;
	type SerializeTupleVariant = Impossible<ThrowStatement, Error>;
	type SerializeMap = Impossible<ThrowStatement, Error>;
	type SerializeStruct = SerializeThrowStatement;
	type SerializeStructVariant = Impossible<ThrowStatement, Error>;

	const EXPECTED: &'static str = "a struct `ThrowStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeThrowStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeThrowStatement {
	error: Value,
}

impl serde::ser::SerializeStruct for SerializeThrowStatement {
	type Ok = ThrowStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"error" => {
				self.error = value.serialize(ser::value::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `ThrowStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(ThrowStatement {
			error: self.error,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = ThrowStatement::default();
		let value: ThrowStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
