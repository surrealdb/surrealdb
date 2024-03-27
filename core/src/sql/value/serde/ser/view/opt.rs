use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::View;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<View>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<View>, Error>;
	type SerializeTuple = Impossible<Option<View>, Error>;
	type SerializeTupleStruct = Impossible<Option<View>, Error>;
	type SerializeTupleVariant = Impossible<Option<View>, Error>;
	type SerializeMap = Impossible<Option<View>, Error>;
	type SerializeStruct = Impossible<Option<View>, Error>;
	type SerializeStructVariant = Impossible<Option<View>, Error>;

	const EXPECTED: &'static str = "an `Option<View>`";

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
		let option: Option<View> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(View::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
