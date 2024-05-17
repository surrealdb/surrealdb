use crate::err::Error;
use crate::sql::statements::InsertStatement;
use crate::sql::value::serde::ser;
use crate::sql::Data;
use crate::sql::Output;
use crate::sql::Timeout;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = InsertStatement;
	type Error = Error;

	type SerializeSeq = Impossible<InsertStatement, Error>;
	type SerializeTuple = Impossible<InsertStatement, Error>;
	type SerializeTupleStruct = Impossible<InsertStatement, Error>;
	type SerializeTupleVariant = Impossible<InsertStatement, Error>;
	type SerializeMap = Impossible<InsertStatement, Error>;
	type SerializeStruct = SerializeInsertStatement;
	type SerializeStructVariant = Impossible<InsertStatement, Error>;

	const EXPECTED: &'static str = "a struct `InsertStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeInsertStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeInsertStatement {
	into: Option<Value>,
	data: Option<Data>,
	ignore: Option<bool>,
	update: Option<Data>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
	relation: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeInsertStatement {
	type Ok = InsertStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"into" => {
				self.into = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"data" => {
				self.data = Some(value.serialize(ser::data::Serializer.wrap())?);
			}
			"ignore" => {
				self.ignore = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"update" => {
				self.update = value.serialize(ser::data::opt::Serializer.wrap())?;
			}
			"output" => {
				self.output = value.serialize(ser::output::opt::Serializer.wrap())?;
			}
			"timeout" => {
				self.timeout = value.serialize(ser::timeout::opt::Serializer.wrap())?;
			}
			"parallel" => {
				self.parallel = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"relation" => {
				self.relation = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `InsertStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.into, self.data, self.ignore, self.parallel, self.relation) {
			(Some(into), Some(data), Some(ignore), Some(parallel), Some(relation)) => {
				Ok(InsertStatement {
					into,
					data,
					ignore,
					parallel,
					update: self.update,
					output: self.output,
					timeout: self.timeout,
					relation,
				})
			}
			_ => Err(Error::custom("`InsertStatement` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = InsertStatement::default();
		let value: InsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_update() {
		let stmt = InsertStatement {
			update: Some(Default::default()),
			..Default::default()
		};
		let value: InsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = InsertStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: InsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = InsertStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: InsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
