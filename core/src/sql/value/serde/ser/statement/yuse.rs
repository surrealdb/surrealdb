use crate::err::Error;
use crate::sql::statements::UseStatement;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = UseStatement;
	type Error = Error;

	type SerializeSeq = Impossible<UseStatement, Error>;
	type SerializeTuple = Impossible<UseStatement, Error>;
	type SerializeTupleStruct = Impossible<UseStatement, Error>;
	type SerializeTupleVariant = Impossible<UseStatement, Error>;
	type SerializeMap = Impossible<UseStatement, Error>;
	type SerializeStruct = SerializeUseStatement;
	type SerializeStructVariant = Impossible<UseStatement, Error>;

	const EXPECTED: &'static str = "a struct `UseStatement`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeUseStatement::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeUseStatement {
	ns: Option<String>,
	db: Option<String>,
}

impl serde::ser::SerializeStruct for SerializeUseStatement {
	type Ok = UseStatement;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"ns" => {
				self.ns = value.serialize(ser::string::opt::Serializer.wrap())?;
			}
			"db" => {
				self.db = value.serialize(ser::string::opt::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `UseStatement::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(UseStatement {
			ns: self.ns,
			db: self.db,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = UseStatement::default();
		let value: UseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_ns() {
		let stmt = UseStatement {
			ns: Some(Default::default()),
			..Default::default()
		};
		let value: UseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_db() {
		let stmt = UseStatement {
			db: Some(Default::default()),
			..Default::default()
		};
		let value: UseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}

	#[test]
	fn with_both() {
		let stmt = UseStatement {
			ns: Some("ns".to_owned()),
			db: Some("db".to_owned()),
		};
		let value: UseStatement = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
