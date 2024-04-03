use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Part;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Vec<Part>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Vec<Part>>, Error>;
	type SerializeTuple = Impossible<Option<Vec<Part>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Vec<Part>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Vec<Part>>, Error>;
	type SerializeMap = Impossible<Option<Vec<Part>>, Error>;
	type SerializeStruct = Impossible<Option<Vec<Part>>, Error>;
	type SerializeStructVariant = Impossible<Option<Vec<Part>>, Error>;

	const EXPECTED: &'static str = "an `Option<Vec<Part>>`";

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
		let option: Option<Vec<Part>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(vec![Part::All]);
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
