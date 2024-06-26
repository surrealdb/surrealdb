use crate::err::Error;
use crate::sql::statements::UpsertStatement;
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
	type Ok = UpsertStatement;
	type Error = Error;

	type SerializeSeq = Impossible<UpsertStatement, Error>;
	type SerializeTuple = Impossible<UpsertStatement, Error>;
	type SerializeTupleStruct = Impossible<UpsertStatement, Error>;
	type SerializeTupleVariant = Impossible<UpsertStatement, Error>;
	type SerializeMap = Impossible<UpsertStatement, Error>;
	type SerializeStruct = SerializeUpsertStatement;
	type SerializeStructVariant = Impossible<UpsertStatement, Error>;

	const EXPECTED: &'static str = "a struct `UpsertStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeUpsertStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeUpsertStatement {
	only: Option<bool>,
	what: Option<Values>,
	data: Option<Data>,
	cond: Option<Cond>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeUpsertStatement {
	type Ok = UpsertStatement;
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
				self.what = Some(Values(value.serialize(ser::value::vec::Serializer.wrap())?));
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
				return Err(Error::custom(format!("unexpected field `UpsertStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.what, self.parallel) {
			(Some(what), Some(parallel)) => Ok(UpsertStatement {
				only: self.only.is_some_and(|v| v),
				what,
				parallel,
				data: self.data,
				cond: self.cond,
				output: self.output,
				timeout: self.timeout,
			}),
			_ => Err(Error::custom("`UpsertStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = UpsertStatement::default();
		let value: UpsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_data() {
		let stmt = UpsertStatement {
			data: Some(Default::default()),
			..Default::default()
		};
		let value: UpsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_cond() {
		let stmt = UpsertStatement {
			cond: Some(Default::default()),
			..Default::default()
		};
		let value: UpsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = UpsertStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: UpsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = UpsertStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: UpsertStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
