use crate::err::Error;
use crate::sql::filter::Filter;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Vec<Filter>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeTuple = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeMap = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeStruct = Impossible<Option<Vec<Filter>>, Error>;
	type SerializeStructVariant = Impossible<Option<Vec<Filter>>, Error>;

	const EXPECTED: &'static str = "an `Option<Vec<Filter>>`";

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
		let option: Option<Vec<Filter>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(vec![Filter::Ascii]);
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
