use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::value::Values;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Values>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Values>, Error>;
	type SerializeTuple = Impossible<Option<Values>, Error>;
	type SerializeTupleStruct = Impossible<Option<Values>, Error>;
	type SerializeTupleVariant = Impossible<Option<Values>, Error>;
	type SerializeMap = Impossible<Option<Values>, Error>;
	type SerializeStruct = Impossible<Option<Values>, Error>;
	type SerializeStructVariant = Impossible<Option<Values>, Error>;

	const EXPECTED: &'static str = "an `Option<Values>`";

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
		let option: Option<Values> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Values::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
