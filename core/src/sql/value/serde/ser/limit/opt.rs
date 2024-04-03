use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Limit;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Limit>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Limit>, Error>;
	type SerializeTuple = Impossible<Option<Limit>, Error>;
	type SerializeTupleStruct = Impossible<Option<Limit>, Error>;
	type SerializeTupleVariant = Impossible<Option<Limit>, Error>;
	type SerializeMap = Impossible<Option<Limit>, Error>;
	type SerializeStruct = Impossible<Option<Limit>, Error>;
	type SerializeStructVariant = Impossible<Option<Limit>, Error>;

	const EXPECTED: &'static str = "an `Option<Limit>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Limit(value.serialize(ser::value::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Limit> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Limit::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
