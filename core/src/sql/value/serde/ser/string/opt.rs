use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<String>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<String>, Error>;
	type SerializeTuple = Impossible<Option<String>, Error>;
	type SerializeTupleStruct = Impossible<Option<String>, Error>;
	type SerializeTupleVariant = Impossible<Option<String>, Error>;
	type SerializeMap = Impossible<Option<String>, Error>;
	type SerializeStruct = Impossible<Option<String>, Error>;
	type SerializeStructVariant = Impossible<Option<String>, Error>;

	const EXPECTED: &'static str = "an `Option<String>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::string::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<String> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(String::new());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
