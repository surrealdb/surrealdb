use crate::err::Error;
use crate::sql::statements::OutputStatement;
use crate::sql::value::serde::ser;
use crate::sql::Fetchs;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = OutputStatement;
	type Error = Error;

	type SerializeSeq = Impossible<OutputStatement, Error>;
	type SerializeTuple = Impossible<OutputStatement, Error>;
	type SerializeTupleStruct = Impossible<OutputStatement, Error>;
	type SerializeTupleVariant = Impossible<OutputStatement, Error>;
	type SerializeMap = Impossible<OutputStatement, Error>;
	type SerializeStruct = SerializeOutputStatement;
	type SerializeStructVariant = Impossible<OutputStatement, Error>;

	const EXPECTED: &'static str = "a struct `OutputStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeOutputStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeOutputStatement {
	what: Option<Value>,
	fetch: Option<Fetchs>,
}

impl serde::ser::SerializeStruct for SerializeOutputStatement {
	type Ok = OutputStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"what" => {
				self.what = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"fetch" => {
				self.fetch = value.serialize(ser::fetch::vec::opt::Serializer.wrap())?.map(Fetchs);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `OutputStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self.what {
			Some(what) => Ok(OutputStatement {
				what,
				fetch: self.fetch,
			}),
			None => Err(Error::custom("`OutputStatement` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = OutputStatement::default();
		let value: OutputStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_fetch() {
		let stmt = OutputStatement {
			fetch: Some(Default::default()),
			..Default::default()
		};
		let value: OutputStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
