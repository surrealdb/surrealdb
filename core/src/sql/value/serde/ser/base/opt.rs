use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Base>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Base>, Error>;
	type SerializeTuple = Impossible<Option<Base>, Error>;
	type SerializeTupleStruct = Impossible<Option<Base>, Error>;
	type SerializeTupleVariant = Impossible<Option<Base>, Error>;
	type SerializeMap = Impossible<Option<Base>, Error>;
	type SerializeStruct = Impossible<Option<Base>, Error>;
	type SerializeStructVariant = Impossible<Option<Base>, Error>;

	const EXPECTED: &'static str = "an `Option<Base>`";

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
		let option: Option<Base> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Base::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
