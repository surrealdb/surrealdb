use crate::err::Error;
use crate::sql::statements::CreateStatement;
use crate::sql::value::serde::ser;
use crate::sql::Data;
use crate::sql::Duration;
use crate::sql::Output;
use crate::sql::Timeout;
use crate::sql::Values;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = CreateStatement;
	type Error = Error;

	type SerializeSeq = Impossible<CreateStatement, Error>;
	type SerializeTuple = Impossible<CreateStatement, Error>;
	type SerializeTupleStruct = Impossible<CreateStatement, Error>;
	type SerializeTupleVariant = Impossible<CreateStatement, Error>;
	type SerializeMap = Impossible<CreateStatement, Error>;
	type SerializeStruct = SerializeCreateStatement;
	type SerializeStructVariant = Impossible<CreateStatement, Error>;

	const EXPECTED: &'static str = "a struct `CreateStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeCreateStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeCreateStatement {
	only: Option<bool>,
	what: Option<Values>,
	data: Option<Data>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeCreateStatement {
	type Ok = CreateStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"only" => {
				self.only = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"what" => {
				self.what = Some(value.serialize(ser::values::Serializer.wrap())?);
			}
			"data" => {
				self.data = value.serialize(ser::data::opt::Serializer.wrap())?;
			}
			"output" => {
				self.output = value.serialize(ser::output::opt::Serializer.wrap())?;
			}
			"timeout" => {
				if let Some(duration) = value.serialize(ser::duration::opt::Serializer.wrap())? {
					self.timeout = Some(Timeout(Duration(duration)));
				}
			}
			"parallel" => {
				self.parallel = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `CreateStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.what, self.parallel) {
			(Some(what), Some(parallel)) => Ok(CreateStatement {
				only: self.only.is_some_and(|v| v),
				what,
				parallel,
				data: self.data,
				output: self.output,
				timeout: self.timeout,
			}),
			_ => Err(Error::custom("`CreateStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = CreateStatement::default();
		let value: CreateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_data() {
		let stmt = CreateStatement {
			data: Some(Default::default()),
			..Default::default()
		};
		let value: CreateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = CreateStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: CreateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = CreateStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: CreateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
