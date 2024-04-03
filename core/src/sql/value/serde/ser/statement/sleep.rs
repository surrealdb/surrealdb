use crate::err::Error;
use crate::sql::statements::sleep::SleepStatement;
use crate::sql::value::serde::ser;
use crate::sql::Duration;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = SleepStatement;
	type Error = Error;

	type SerializeSeq = Impossible<SleepStatement, Error>;
	type SerializeTuple = Impossible<SleepStatement, Error>;
	type SerializeTupleStruct = Impossible<SleepStatement, Error>;
	type SerializeTupleVariant = Impossible<SleepStatement, Error>;
	type SerializeMap = Impossible<SleepStatement, Error>;
	type SerializeStruct = SerializeSleepStatement;
	type SerializeStructVariant = Impossible<SleepStatement, Error>;

	const EXPECTED: &'static str = "a struct `SleepStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeSleepStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeSleepStatement {
	duration: Duration,
}

impl serde::ser::SerializeStruct for SerializeSleepStatement {
	type Ok = SleepStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"duration" => {
				self.duration = Duration(value.serialize(ser::duration::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `SleepStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(SleepStatement {
			duration: self.duration,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = SleepStatement::default();
		let value: SleepStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
