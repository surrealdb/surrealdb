mod access;
mod analyzer;
mod database;
mod event;
mod field;
pub mod function;
mod index;
mod namespace;
mod param;
mod table;
mod user;

use crate::err::Error;
use crate::sql::statements::DefineStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DefineStatement;
	type Error = Error;

	type SerializeSeq = Impossible<DefineStatement, Error>;
	type SerializeTuple = Impossible<DefineStatement, Error>;
	type SerializeTupleStruct = Impossible<DefineStatement, Error>;
	type SerializeTupleVariant = Impossible<DefineStatement, Error>;
	type SerializeMap = Impossible<DefineStatement, Error>;
	type SerializeStruct = Impossible<DefineStatement, Error>;
	type SerializeStructVariant = Impossible<DefineStatement, Error>;

	const EXPECTED: &'static str = "an enum `DefineStatement`";

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
				Ok(DefineStatement::Namespace(value.serialize(namespace::Serializer.wrap())?))
			}
			"Database" => {
				Ok(DefineStatement::Database(value.serialize(database::Serializer.wrap())?))
			}
			"Function" => {
				Ok(DefineStatement::Function(value.serialize(function::Serializer.wrap())?))
			}
			"Analyzer" => {
				Ok(DefineStatement::Analyzer(value.serialize(analyzer::Serializer.wrap())?))
			}
			"Access" => Ok(DefineStatement::Access(value.serialize(access::Serializer.wrap())?)),
			"Param" => Ok(DefineStatement::Param(value.serialize(param::Serializer.wrap())?)),
			"Table" => Ok(DefineStatement::Table(value.serialize(table::Serializer.wrap())?)),
			"Event" => Ok(DefineStatement::Event(value.serialize(event::Serializer.wrap())?)),
			"Field" => Ok(DefineStatement::Field(value.serialize(field::Serializer.wrap())?)),
			"Index" => Ok(DefineStatement::Index(value.serialize(index::Serializer.wrap())?)),
			"User" => Ok(DefineStatement::User(value.serialize(user::Serializer.wrap())?)),
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
		let stmt = DefineStatement::Namespace(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn database() {
		let stmt = DefineStatement::Database(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn function() {
		let stmt = DefineStatement::Function(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn analyzer() {
		let stmt = DefineStatement::Analyzer(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn access() {
		let stmt = DefineStatement::Access(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn param() {
		let stmt = DefineStatement::Param(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn table() {
		let stmt = DefineStatement::Table(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn event() {
		let stmt = DefineStatement::Event(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn field() {
		let stmt = DefineStatement::Field(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn index() {
		let stmt = DefineStatement::Index(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn user() {
		let stmt = DefineStatement::User(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
