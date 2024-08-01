use crate::err::Error;
use crate::sql::kind::Kind;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Option<Kind>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Option<Kind>>, Error>;
	type SerializeTuple = Impossible<Option<Option<Kind>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Option<Kind>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Option<Kind>>, Error>;
	type SerializeMap = Impossible<Option<Option<Kind>>, Error>;
	type SerializeStruct = Impossible<Option<Option<Kind>>, Error>;
	type SerializeStructVariant = Impossible<Option<Option<Kind>>, Error>;

	const EXPECTED: &'static str = "an `Option<Kind>`";

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
		let option: Option<Option<Kind>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Some(Kind::default()));
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
