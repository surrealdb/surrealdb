use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Output;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Output>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Output>, Error>;
	type SerializeTuple = Impossible<Option<Output>, Error>;
	type SerializeTupleStruct = Impossible<Option<Output>, Error>;
	type SerializeTupleVariant = Impossible<Option<Output>, Error>;
	type SerializeMap = Impossible<Option<Output>, Error>;
	type SerializeStruct = Impossible<Option<Output>, Error>;
	type SerializeStructVariant = Impossible<Option<Output>, Error>;

	const EXPECTED: &'static str = "an `Option<Output>`";

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
		let option: Option<Output> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Output::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
