use crate::err::Error;
use crate::sql::statements::UpdateStatement;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
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
	type Ok = UpdateStatement;
	type Error = Error;

	type SerializeSeq = Impossible<UpdateStatement, Error>;
	type SerializeTuple = Impossible<UpdateStatement, Error>;
	type SerializeTupleStruct = Impossible<UpdateStatement, Error>;
	type SerializeTupleVariant = Impossible<UpdateStatement, Error>;
	type SerializeMap = Impossible<UpdateStatement, Error>;
	type SerializeStruct = SerializeUpdateStatement;
	type SerializeStructVariant = Impossible<UpdateStatement, Error>;

	const EXPECTED: &'static str = "a struct `UpdateStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeUpdateStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeUpdateStatement {
	only: Option<bool>,
	what: Option<Values>,
	data: Option<Data>,
	cond: Option<Cond>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeUpdateStatement {
	type Ok = UpdateStatement;
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
			"cond" => {
				self.cond = value.serialize(ser::cond::opt::Serializer.wrap())?;
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
				return Err(Error::custom(format!("unexpected field `UpdateStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.what, self.parallel) {
			(Some(what), Some(parallel)) => Ok(UpdateStatement {
				only: self.only.is_some_and(|v| v),
				what,
				parallel,
				data: self.data,
				cond: self.cond,
				output: self.output,
				timeout: self.timeout,
			}),
			_ => Err(Error::custom("`UpdateStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = UpdateStatement::default();
		let value: UpdateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_data() {
		let stmt = UpdateStatement {
			data: Some(Default::default()),
			..Default::default()
		};
		let value: UpdateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_cond() {
		let stmt = UpdateStatement {
			cond: Some(Default::default()),
			..Default::default()
		};
		let value: UpdateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = UpdateStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: UpdateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = UpdateStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: UpdateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
