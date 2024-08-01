use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::value::Value;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Option<Value>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Option<Value>>, Error>;
	type SerializeTuple = Impossible<Option<Option<Value>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Option<Value>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Option<Value>>, Error>;
	type SerializeMap = Impossible<Option<Option<Value>>, Error>;
	type SerializeStruct = Impossible<Option<Option<Value>>, Error>;
	type SerializeStructVariant = Impossible<Option<Option<Value>>, Error>;

	const EXPECTED: &'static str = "an `Option<Value>`";

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
		let option: Option<Option<Value>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Some(Value::default()));
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
