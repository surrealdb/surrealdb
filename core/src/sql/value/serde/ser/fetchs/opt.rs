use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Fetchs;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Fetchs>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Fetchs>, Error>;
	type SerializeTuple = Impossible<Option<Fetchs>, Error>;
	type SerializeTupleStruct = Impossible<Option<Fetchs>, Error>;
	type SerializeTupleVariant = Impossible<Option<Fetchs>, Error>;
	type SerializeMap = Impossible<Option<Fetchs>, Error>;
	type SerializeStruct = Impossible<Option<Fetchs>, Error>;
	type SerializeStructVariant = Impossible<Option<Fetchs>, Error>;

	const EXPECTED: &'static str = "an `Option<Fetchs>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Fetchs(value.serialize(ser::fetch::vec::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Fetchs> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Fetchs::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
