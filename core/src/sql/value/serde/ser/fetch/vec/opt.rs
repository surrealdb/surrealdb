use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Fetch;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Vec<Fetch>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeTuple = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeMap = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeStruct = Impossible<Option<Vec<Fetch>>, Error>;
	type SerializeStructVariant = Impossible<Option<Vec<Fetch>>, Error>;

	const EXPECTED: &'static str = "an `Option<Vec<Fetch>>`";

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
		let option: Option<Vec<Fetch>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(vec![Fetch::default()]);
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
