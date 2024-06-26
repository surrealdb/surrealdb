use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Duration;
use crate::sql::Timeout;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Timeout>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Timeout>, Error>;
	type SerializeTuple = Impossible<Option<Timeout>, Error>;
	type SerializeTupleStruct = Impossible<Option<Timeout>, Error>;
	type SerializeTupleVariant = Impossible<Option<Timeout>, Error>;
	type SerializeMap = Impossible<Option<Timeout>, Error>;
	type SerializeStruct = Impossible<Option<Timeout>, Error>;
	type SerializeStructVariant = Impossible<Option<Timeout>, Error>;

	const EXPECTED: &'static str = "an `Option<Timeout>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(Timeout(Duration(value.serialize(ser::duration::Serializer.wrap())?))))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Timeout> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Timeout::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
