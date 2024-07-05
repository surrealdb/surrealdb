use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Access;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Access>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Access>, Error>;
	type SerializeTuple = Impossible<Option<Access>, Error>;
	type SerializeTupleStruct = Impossible<Option<Access>, Error>;
	type SerializeTupleVariant = Impossible<Option<Access>, Error>;
	type SerializeMap = Impossible<Option<Access>, Error>;
	type SerializeStruct = Impossible<Option<Access>, Error>;
	type SerializeStructVariant = Impossible<Option<Access>, Error>;

	const EXPECTED: &'static str = "an `Option<Access>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Access(value.serialize(ser::string::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Access> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Access::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
