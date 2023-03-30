use crate::err::Error;
use crate::sql::value::serde::ser;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = String;
	type Error = Error;

	type SerializeSeq = Impossible<String, Error>;
	type SerializeTuple = Impossible<String, Error>;
	type SerializeTupleStruct = Impossible<String, Error>;
	type SerializeTupleVariant = Impossible<String, Error>;
	type SerializeMap = Impossible<String, Error>;
	type SerializeStruct = Impossible<String, Error>;
	type SerializeStructVariant = Impossible<String, Error>;

	const EXPECTED: &'static str = "a string";

	fn serialize_str(self, value: &str) -> Result<Self::Ok, Error> {
		Ok(value.to_owned())
	}

	#[inline]
	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		self.serialize_str(variant)
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
		value.serialize(self.wrap())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use ser::Serializer as _;
	use serde::Serialize;
	use std::borrow::Cow;

	#[test]
	fn string() {
		let duration = "foo".to_owned();
		let serialized = serialize_internal(|| duration.serialize(Serializer.wrap())).unwrap();
		assert_eq!(duration, serialized);
	}

	#[test]
	fn str() {
		let duration = "bar";
		let serialized = serialize_internal(|| duration.serialize(Serializer.wrap())).unwrap();
		assert_eq!(duration, serialized);
	}

	#[test]
	fn cow() {
		let duration = Cow::from("bar");
		let serialized = serialize_internal(|| duration.serialize(Serializer.wrap())).unwrap();
		assert_eq!(duration, serialized);
	}
}
