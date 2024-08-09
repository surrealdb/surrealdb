use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Permissions;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Permissions>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Permissions>, Error>;
	type SerializeTuple = Impossible<Option<Permissions>, Error>;
	type SerializeTupleStruct = Impossible<Option<Permissions>, Error>;
	type SerializeTupleVariant = Impossible<Option<Permissions>, Error>;
	type SerializeMap = Impossible<Option<Permissions>, Error>;
	type SerializeStruct = Impossible<Option<Permissions>, Error>;
	type SerializeStructVariant = Impossible<Option<Permissions>, Error>;

	const EXPECTED: &'static str = "an `Option<Permissions>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::permissions::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Permissions> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Permissions::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
