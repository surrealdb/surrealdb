use crate::err::Error;
use crate::sql::statements::RelateStatement;
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
	type Ok = RelateStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RelateStatement, Error>;
	type SerializeTuple = Impossible<RelateStatement, Error>;
	type SerializeTupleStruct = Impossible<RelateStatement, Error>;
	type SerializeTupleVariant = Impossible<RelateStatement, Error>;
	type SerializeMap = Impossible<RelateStatement, Error>;
	type SerializeStruct = SerializeRelateStatement;
	type SerializeStructVariant = Impossible<RelateStatement, Error>;

	const EXPECTED: &'static str = "a struct `RelateStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeRelateStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeRelateStatement {
	only: Option<bool>,
	kind: Option<Value>,
	from: Option<Value>,
	with: Option<Value>,
	uniq: Option<bool>,
	data: Option<Data>,
	output: Option<Output>,
	timeout: Option<Timeout>,
	parallel: Option<bool>,
}

impl serde::ser::SerializeStruct for SerializeRelateStatement {
	type Ok = RelateStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"only" => {
				self.only = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"kind" => {
				self.kind = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"from" => {
				self.from = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"with" => {
				self.with = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"uniq" => {
				self.uniq = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			"data" => {
				self.data = value.serialize(ser::data::opt::Serializer.wrap())?;
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
				return Err(Error::custom(format!("unexpected field `RelateStatement::{key}`",)));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.kind, self.from, self.with, self.uniq, self.parallel) {
			(Some(kind), Some(from), Some(with), Some(uniq), Some(parallel)) => {
				Ok(RelateStatement {
					only: self.only.is_some_and(|v| v),
					kind,
					from,
					with,
					uniq,
					parallel,
					data: self.data,
					output: self.output,
					timeout: self.timeout,
				})
			}
			_ => Err(Error::custom("`RelateStatement` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = RelateStatement::default();
		let value: RelateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_data() {
		let stmt = RelateStatement {
			data: Some(Default::default()),
			..Default::default()
		};
		let value: RelateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_output() {
		let stmt = RelateStatement {
			output: Some(Default::default()),
			..Default::default()
		};
		let value: RelateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_timeout() {
		let stmt = RelateStatement {
			timeout: Some(Default::default()),
			..Default::default()
		};
		let value: RelateStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
