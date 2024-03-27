use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Split;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Vec<Split>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Vec<Split>>, Error>;
	type SerializeTuple = Impossible<Option<Vec<Split>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Vec<Split>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Vec<Split>>, Error>;
	type SerializeMap = Impossible<Option<Vec<Split>>, Error>;
	type SerializeStruct = Impossible<Option<Vec<Split>>, Error>;
	type SerializeStructVariant = Impossible<Option<Vec<Split>>, Error>;

	const EXPECTED: &'static str = "an `Option<Vec<Split>>`";

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
		let option: Option<Vec<Split>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(vec![Split::default()]);
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
