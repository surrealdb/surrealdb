use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::with::With;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<With>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<With>, Error>;
	type SerializeTuple = Impossible<Option<With>, Error>;
	type SerializeTupleStruct = Impossible<Option<With>, Error>;
	type SerializeTupleVariant = Impossible<Option<With>, Error>;
	type SerializeMap = Impossible<Option<With>, Error>;
	type SerializeStruct = Impossible<Option<With>, Error>;
	type SerializeStructVariant = Impossible<Option<With>, Error>;

	const EXPECTED: &'static str = "an `Option<With>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::with::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<With> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(With::NoIndex);
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
