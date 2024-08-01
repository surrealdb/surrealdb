use crate::err::Error;
use crate::sql::statements::DeleteStatement;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
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
	type Ok = DeleteStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DeleteStatement, Error>;
	type SerializeTuple = Impossible<DeleteStatement, Error>;
	type SerializeTupleStruct = Impossible<DeleteStatement, Error>;
	type SerializeTupleVariant = Impossible<DeleteStatement, Error>;
	type SerializeMap = Impossible<DeleteStatement, Error>;
	type SerializeStruct = SerializeDeleteStatement;
	type SerializeStructVariant = Impossible<DeleteStatement, Error>;

	const EXPECTED: &'static str = "a struct `DeleteStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDeleteStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeDeleteStatement {
	only: Option<bool>,
	what: Option<Values>,
	cond: Option<Cond>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeDeleteStatement {
	type Ok = DeleteStatement;
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
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
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
			key => {
				return Err(Error::custom(format!("unexpected field `DeleteStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.what, self.parallel) {
			(Some(what), Some(parallel)) => Ok(DeleteStatement {
				only: self.only.is_some_and(|v| v),
				what,
				parallel,
				cond: self.cond,
				output: self.output,
				timeout: self.timeout,
			}),
			_ => Err(Error::custom("`DeleteStatement` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = DeleteStatement::default();
		let value: DeleteStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_cond() {
		let stmt = DeleteStatement {
			cond: Some(Default::default()),
			..Default::default()
		};
		let value: DeleteStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = DeleteStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: DeleteStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = DeleteStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: DeleteStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
