mod field;
mod table;

use crate::err::Error;
use crate::sql::statements::AlterStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = AlterStatement;
	type Error = Error;

	type SerializeSeq = Impossible<AlterStatement, Error>;
	type SerializeTuple = Impossible<AlterStatement, Error>;
	type SerializeTupleStruct = Impossible<AlterStatement, Error>;
	type SerializeTupleVariant = Impossible<AlterStatement, Error>;
	type SerializeMap = Impossible<AlterStatement, Error>;
	type SerializeStruct = Impossible<AlterStatement, Error>;
	type SerializeStructVariant = Impossible<AlterStatement, Error>;

	const EXPECTED: &'static str = "an enum `AlterStatement`";

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
			"Field" => Ok(AlterStatement::Field(value.serialize(field::Serializer.wrap())?)),
			"Table" => Ok(AlterStatement::Table(value.serialize(table::Serializer.wrap())?)),
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
	fn field() {
		let stmt = AlterStatement::Field(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn table() {
		let stmt = AlterStatement::Table(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
