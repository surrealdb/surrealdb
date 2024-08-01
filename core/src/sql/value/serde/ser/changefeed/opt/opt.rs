use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Option<ChangeFeed>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeTuple = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeMap = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeStruct = Impossible<Option<Option<ChangeFeed>>, Error>;
	type SerializeStructVariant = Impossible<Option<Option<ChangeFeed>>, Error>;

	const EXPECTED: &'static str = "an `Option<Option<ChangeFeed>>`";

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
		let option: Option<Option<ChangeFeed>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Some(ChangeFeed::default()));
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
