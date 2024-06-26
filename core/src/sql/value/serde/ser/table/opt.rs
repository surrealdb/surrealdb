use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Table;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Table>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Table>, Error>;
	type SerializeTuple = Impossible<Option<Table>, Error>;
	type SerializeTupleStruct = Impossible<Option<Table>, Error>;
	type SerializeTupleVariant = Impossible<Option<Table>, Error>;
	type SerializeMap = Impossible<Option<Table>, Error>;
	type SerializeStruct = Impossible<Option<Table>, Error>;
	type SerializeStructVariant = Impossible<Option<Table>, Error>;

	const EXPECTED: &'static str = "an `Option<Table>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Table(value.serialize(ser::string::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Table> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Table::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
