use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Explain;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Explain>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Explain>, Error>;
	type SerializeTuple = Impossible<Option<Explain>, Error>;
	type SerializeTupleStruct = Impossible<Option<Explain>, Error>;
	type SerializeTupleVariant = Impossible<Option<Explain>, Error>;
	type SerializeMap = Impossible<Option<Explain>, Error>;
	type SerializeStruct = Impossible<Option<Explain>, Error>;
	type SerializeStructVariant = Impossible<Option<Explain>, Error>;

	const EXPECTED: &'static str = "an `Option<Explain>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Explain(value.serialize(ser::primitive::bool::Serializer.wrap())?)))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Explain> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some_default() {
		let option = Some(Explain::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some_full() {
		let option = Some(Explain(true));
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
