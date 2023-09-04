use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Datetime;
use crate::sql::Version;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Version>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Version>, Error>;
	type SerializeTuple = Impossible<Option<Version>, Error>;
	type SerializeTupleStruct = Impossible<Option<Version>, Error>;
	type SerializeTupleVariant = Impossible<Option<Version>, Error>;
	type SerializeMap = Impossible<Option<Version>, Error>;
	type SerializeStruct = Impossible<Option<Version>, Error>;
	type SerializeStructVariant = Impossible<Option<Version>, Error>;

	const EXPECTED: &'static str = "an `Option<Version>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Version(Datetime(value.serialize(ser::datetime::Serializer.wrap())?))))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Version> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Version::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
