use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<u64>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<u64>, Error>;
	type SerializeTuple = Impossible<Option<u64>, Error>;
	type SerializeTupleStruct = Impossible<Option<u64>, Error>;
	type SerializeTupleVariant = Impossible<Option<u64>, Error>;
	type SerializeMap = Impossible<Option<u64>, Error>;
	type SerializeStruct = Impossible<Option<u64>, Error>;
	type SerializeStructVariant = Impossible<Option<u64>, Error>;

	const EXPECTED: &'static str = "an `Option<u64>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::primitive::u64::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<u64> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(u64::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
