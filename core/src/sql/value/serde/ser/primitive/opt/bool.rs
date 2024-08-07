use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<bool>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<bool>, Error>;
	type SerializeTuple = Impossible<Option<bool>, Error>;
	type SerializeTupleStruct = Impossible<Option<bool>, Error>;
	type SerializeTupleVariant = Impossible<Option<bool>, Error>;
	type SerializeMap = Impossible<Option<bool>, Error>;
	type SerializeStruct = Impossible<Option<bool>, Error>;
	type SerializeStructVariant = Impossible<Option<bool>, Error>;

	const EXPECTED: &'static str = "an `Option<bool>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::primitive::bool::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<bool> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(bool::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
