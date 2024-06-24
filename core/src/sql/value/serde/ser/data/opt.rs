use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Data;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Data>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Data>, Error>;
	type SerializeTuple = Impossible<Option<Data>, Error>;
	type SerializeTupleStruct = Impossible<Option<Data>, Error>;
	type SerializeTupleVariant = Impossible<Option<Data>, Error>;
	type SerializeMap = Impossible<Option<Data>, Error>;
	type SerializeStruct = Impossible<Option<Data>, Error>;
	type SerializeStructVariant = Impossible<Option<Data>, Error>;

	const EXPECTED: &'static str = "an `Option<Data>`";

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
		let option: Option<Data> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Data::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
