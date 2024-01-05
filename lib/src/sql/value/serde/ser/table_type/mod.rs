use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::TableType;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

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

	const EXPECTED: &'static str = "an `TableType`";

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
			"Normal" => Ok(TableType::Normal),
			"Any" => Ok(TableType::Any),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

// impl ser::Serializer for Serializer {
// 	type Ok = ChangeFeed;
// 	type Error = Error;

// 	type SerializeSeq = Impossible<ChangeFeed, Error>;
// 	type SerializeTuple = Impossible<ChangeFeed, Error>;
// 	type SerializeTupleStruct = Impossible<ChangeFeed, Error>;
// 	type SerializeTupleVariant = Impossible<ChangeFeed, Error>;
// 	type SerializeMap = Impossible<ChangeFeed, Error>;
// 	type SerializeStruct = SerializeChangeFeed;
// 	type SerializeStructVariant = Impossible<ChangeFeed, Error>;

// 	const EXPECTED: &'static str = "a struct `ChangeFeed`";

// 	#[inline]
// 	fn serialize_struct(
// 		self,
// 		_name: &'static str,
// 		_len: usize,
// 	) -> Result<Self::SerializeStruct, Error> {
// 		Ok(SerializeChangeFeed::default())
// 	}
// }

// #[derive(Default)]
// pub struct SerializeChangeFeed {
// 	expiry: Duration,
// }

// impl serde::ser::SerializeStruct for SerializeChangeFeed {
// 	type Ok = ChangeFeed;
// 	type Error = Error;

// 	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
// 	where
// 		T: ?Sized + Serialize,
// 	{
// 		match key {
// 			"expiry" => {
// 				self.expiry = value.serialize(ser::duration::Serializer.wrap())?;
// 			}
// 			key => {
// 				return Err(Error::custom(format!("unexpected field `ChangeFeed::{key}`")));
// 			}
// 		}
// 		Ok(())
// 	}

// 	fn end(self) -> Result<Self::Ok, Error> {
// 		Ok(ChangeFeed {
// 			expiry: self.expiry,
// 		})
// 	}
// }
