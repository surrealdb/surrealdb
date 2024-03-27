use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Start;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Start>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Start>, Error>;
	type SerializeTuple = Impossible<Option<Start>, Error>;
	type SerializeTupleStruct = Impossible<Option<Start>, Error>;
	type SerializeTupleVariant = Impossible<Option<Start>, Error>;
	type SerializeMap = Impossible<Option<Start>, Error>;
	type SerializeStruct = Impossible<Option<Start>, Error>;
	type SerializeStructVariant = Impossible<Option<Start>, Error>;

	const EXPECTED: &'static str = "an `Option<Start>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Start(value.serialize(ser::value::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Start> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Start::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
