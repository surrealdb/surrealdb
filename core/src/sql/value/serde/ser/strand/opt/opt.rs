use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Strand;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Option<Strand>>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Option<Strand>>, Error>;
	type SerializeTuple = Impossible<Option<Option<Strand>>, Error>;
	type SerializeTupleStruct = Impossible<Option<Option<Strand>>, Error>;
	type SerializeTupleVariant = Impossible<Option<Option<Strand>>, Error>;
	type SerializeMap = Impossible<Option<Option<Strand>>, Error>;
	type SerializeStruct = Impossible<Option<Option<Strand>>, Error>;
	type SerializeStructVariant = Impossible<Option<Option<Strand>>, Error>;

	const EXPECTED: &'static str = "an `Option<Strand>`";

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
		let option: Option<Option<Strand>> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Some(Strand::default()));
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
