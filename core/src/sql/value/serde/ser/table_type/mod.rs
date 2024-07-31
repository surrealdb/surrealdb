use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::TableType;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub mod opt;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = TableType;
	type Error = Error;

	type SerializeSeq = Impossible<TableType, Error>;
	type SerializeTuple = Impossible<TableType, Error>;
	type SerializeTupleStruct = Impossible<TableType, Error>;
	type SerializeTupleVariant = Impossible<TableType, Error>;
	type SerializeMap = Impossible<TableType, Error>;
	type SerializeStruct = Impossible<TableType, Error>;
	type SerializeStructVariant = Impossible<TableType, Error>;

	const EXPECTED: &'static str = "a `TableType`";

	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Relation" => {
				Ok(TableType::Relation(value.serialize(ser::relation::Serializer.wrap())?))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Normal" => Ok(TableType::Normal),
			"Any" => Ok(TableType::Any),
			variant => Err(Error::custom(format!("unknown variant `{name}::{variant}`"))),
		}
	}
}
