use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Cond;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Cond>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Cond>, Error>;
	type SerializeTuple = Impossible<Option<Cond>, Error>;
	type SerializeTupleStruct = Impossible<Option<Cond>, Error>;
	type SerializeTupleVariant = Impossible<Option<Cond>, Error>;
	type SerializeMap = Impossible<Option<Cond>, Error>;
	type SerializeStruct = Impossible<Option<Cond>, Error>;
	type SerializeStructVariant = Impossible<Option<Cond>, Error>;

	const EXPECTED: &'static str = "an `Option<Cond>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Cond(value.serialize(ser::value::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Cond> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Cond::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
