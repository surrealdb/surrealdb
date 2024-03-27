use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::time::Duration;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Duration>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Duration>, Error>;
	type SerializeTuple = Impossible<Option<Duration>, Error>;
	type SerializeTupleStruct = Impossible<Option<Duration>, Error>;
	type SerializeTupleVariant = Impossible<Option<Duration>, Error>;
	type SerializeMap = Impossible<Option<Duration>, Error>;
	type SerializeStruct = Impossible<Option<Duration>, Error>;
	type SerializeStructVariant = Impossible<Option<Duration>, Error>;

	const EXPECTED: &'static str = "an `Option<Duration>`";

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
		let option: Option<Duration> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Duration::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
