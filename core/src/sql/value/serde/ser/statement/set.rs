use crate::err::Error;
use crate::sql::statements::SetStatement;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = SetStatement;
	type Error = Error;

	type SerializeSeq = Impossible<SetStatement, Error>;
	type SerializeTuple = Impossible<SetStatement, Error>;
	type SerializeTupleStruct = Impossible<SetStatement, Error>;
	type SerializeTupleVariant = Impossible<SetStatement, Error>;
	type SerializeMap = Impossible<SetStatement, Error>;
	type SerializeStruct = SerializeSetStatement;
	type SerializeStructVariant = Impossible<SetStatement, Error>;

	const EXPECTED: &'static str = "a struct `SetStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeSetStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeSetStatement {
	name: Option<String>,
	what: Option<Value>,
	kind: Option<Kind>,
}

impl serde::ser::SerializeStruct for SerializeSetStatement {
	type Ok = SetStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"name" => {
				self.name = Some(value.serialize(ser::string::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"kind" => self.kind = value.serialize(ser::kind::opt::Serializer.wrap())?,
			key => {
				return Err(Error::custom(format!("unexpected field `SetStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.name, self.what) {
			(Some(name), Some(what)) => Ok(SetStatement {
				name,
				what,
				kind: self.kind,
			}),
			_ => Err(Error::custom("`SetStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = SetStatement::default();
		let value: SetStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
