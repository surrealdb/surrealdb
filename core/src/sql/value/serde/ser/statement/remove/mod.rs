mod access;
mod analyzer;
mod database;
mod event;
mod field;
mod function;
mod index;
mod namespace;
mod param;
mod table;
mod user;

use crate::err::Error;
use crate::sql::statements::RemoveStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RemoveStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RemoveStatement, Error>;
	type SerializeTuple = Impossible<RemoveStatement, Error>;
	type SerializeTupleStruct = Impossible<RemoveStatement, Error>;
	type SerializeTupleVariant = Impossible<RemoveStatement, Error>;
	type SerializeMap = Impossible<RemoveStatement, Error>;
	type SerializeStruct = Impossible<RemoveStatement, Error>;
	type SerializeStructVariant = Impossible<RemoveStatement, Error>;

	const EXPECTED: &'static str = "an enum `RemoveStatement`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Namespace" => {
				Ok(RemoveStatement::Namespace(value.serialize(namespace::Serializer.wrap())?))
			}
			"Database" => {
				Ok(RemoveStatement::Database(value.serialize(database::Serializer.wrap())?))
			}
			"Function" => {
				Ok(RemoveStatement::Function(value.serialize(function::Serializer.wrap())?))
			}
			"Analyzer" => {
				Ok(RemoveStatement::Analyzer(value.serialize(analyzer::Serializer.wrap())?))
			}
			"Access" => Ok(RemoveStatement::Access(value.serialize(access::Serializer.wrap())?)),
			"Param" => Ok(RemoveStatement::Param(value.serialize(param::Serializer.wrap())?)),
			"Table" => Ok(RemoveStatement::Table(value.serialize(table::Serializer.wrap())?)),
			"Event" => Ok(RemoveStatement::Event(value.serialize(event::Serializer.wrap())?)),
			"Field" => Ok(RemoveStatement::Field(value.serialize(field::Serializer.wrap())?)),
			"Index" => Ok(RemoveStatement::Index(value.serialize(index::Serializer.wrap())?)),
			"User" => Ok(RemoveStatement::User(value.serialize(user::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn namespace() {
		let stmt = RemoveStatement::Namespace(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn database() {
		let stmt = RemoveStatement::Database(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn function() {
		let stmt = RemoveStatement::Function(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn analyzer() {
		let stmt = RemoveStatement::Analyzer(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn access() {
		let stmt = RemoveStatement::Access(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn param() {
		let stmt = RemoveStatement::Param(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn table() {
		let stmt = RemoveStatement::Table(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn event() {
		let stmt = RemoveStatement::Event(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn field() {
		let stmt = RemoveStatement::Field(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn index() {
		let stmt = RemoveStatement::Index(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn user() {
		let stmt = RemoveStatement::User(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
