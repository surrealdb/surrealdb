use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[allow(clippy::module_inception)]
pub mod opt;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Kind>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Kind>, Error>;
	type SerializeTuple = Impossible<Option<Kind>, Error>;
	type SerializeTupleStruct = Impossible<Option<Kind>, Error>;
	type SerializeTupleVariant = Impossible<Option<Kind>, Error>;
	type SerializeMap = Impossible<Option<Kind>, Error>;
	type SerializeStruct = Impossible<Option<Kind>, Error>;
	type SerializeStructVariant = Impossible<Option<Kind>, Error>;

	const EXPECTED: &'static str = "an `Option<Kind>`";

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
		let option: Option<Kind> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Kind::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
