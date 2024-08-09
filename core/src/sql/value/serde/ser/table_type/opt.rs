use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::TableType;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<TableType>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<TableType>, Error>;
	type SerializeTuple = Impossible<Option<TableType>, Error>;
	type SerializeTupleStruct = Impossible<Option<TableType>, Error>;
	type SerializeTupleVariant = Impossible<Option<TableType>, Error>;
	type SerializeMap = Impossible<Option<TableType>, Error>;
	type SerializeStruct = Impossible<Option<TableType>, Error>;
	type SerializeStructVariant = Impossible<Option<TableType>, Error>;

	const EXPECTED: &'static str = "an `Option<TableType>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(super::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<TableType> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(TableType::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
