use crate::err::Error;
use crate::sql::statements::KillStatement;
use crate::sql::value::serde::ser;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = KillStatement;
	type Error = Error;

	type SerializeSeq = Impossible<KillStatement, Error>;
	type SerializeTuple = Impossible<KillStatement, Error>;
	type SerializeTupleStruct = Impossible<KillStatement, Error>;
	type SerializeTupleVariant = Impossible<KillStatement, Error>;
	type SerializeMap = Impossible<KillStatement, Error>;
	type SerializeStruct = SerializeKillStatement;
	type SerializeStructVariant = Impossible<KillStatement, Error>;

	const EXPECTED: &'static str = "a struct `KillStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeKillStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeKillStatement {
	id: Option<Value>,
}

impl serde::ser::SerializeStruct for SerializeKillStatement {
	type Ok = KillStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"id" => {
				self.id = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `KillStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self.id {
			Some(id) => Ok(KillStatement {
				id,
			}),
			None => Err(Error::custom("`KillStatement` missing required field")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = KillStatement::default();
		let value: KillStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
