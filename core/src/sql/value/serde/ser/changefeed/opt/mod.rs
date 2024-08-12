use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[allow(clippy::module_inception)]
pub mod opt;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<ChangeFeed>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<ChangeFeed>, Error>;
	type SerializeTuple = Impossible<Option<ChangeFeed>, Error>;
	type SerializeTupleStruct = Impossible<Option<ChangeFeed>, Error>;
	type SerializeTupleVariant = Impossible<Option<ChangeFeed>, Error>;
	type SerializeMap = Impossible<Option<ChangeFeed>, Error>;
	type SerializeStruct = Impossible<Option<ChangeFeed>, Error>;
	type SerializeStructVariant = Impossible<Option<ChangeFeed>, Error>;

	const EXPECTED: &'static str = "an `Option<ChangeFeed>`";

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
		let option: Option<ChangeFeed> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(ChangeFeed::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
