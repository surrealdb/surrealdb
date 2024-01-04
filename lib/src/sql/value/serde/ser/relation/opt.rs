use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<(Option<Kind>, Option<Kind>)>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeTuple = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeTupleStruct = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeTupleVariant = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeMap = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeStruct = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;
	type SerializeStructVariant = Impossible<Option<(Option<Kind>, Option<Kind>)>, Error>;

	const EXPECTED: &'static str = "an `Option<(Option<Kind>, Option<Kind>)>`";

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
