use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Array;
use crate::sql::Id;
use crate::sql::Object;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Id;
	type Error = Error;

	type SerializeSeq = Impossible<Id, Error>;
	type SerializeTuple = Impossible<Id, Error>;
	type SerializeTupleStruct = Impossible<Id, Error>;
	type SerializeTupleVariant = Impossible<Id, Error>;
	type SerializeMap = Impossible<Id, Error>;
	type SerializeStruct = Impossible<Id, Error>;
	type SerializeStructVariant = Impossible<Id, Error>;

	const EXPECTED: &'static str = "an enum `Id`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Number" => Ok(Id::Number(value.serialize(ser::primitive::i64::Serializer.wrap())?)),
			"String" => Ok(Id::String(value.serialize(ser::string::Serializer.wrap())?)),
			"Array" => Ok(Id::Array(Array(value.serialize(ser::value::vec::Serializer.wrap())?))),
			"Object" => {
				Ok(Id::Object(Object(value.serialize(ser::value::map::Serializer.wrap())?)))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn number() {
		let id = Id::Number(Default::default());
		let serialized = id.serialize(Serializer.wrap()).unwrap();
		assert_eq!(id, serialized);
	}

	#[test]
	fn string() {
		let id = Id::String(Default::default());
		let serialized = id.serialize(Serializer.wrap()).unwrap();
		assert_eq!(id, serialized);
	}

	#[test]
	fn array() {
		let id = Id::Array(Default::default());
		let serialized = id.serialize(Serializer.wrap()).unwrap();
		assert_eq!(id, serialized);
	}

	#[test]
	fn object() {
		let id = Id::Object(Default::default());
		let serialized = id.serialize(Serializer.wrap()).unwrap();
		assert_eq!(id, serialized);
	}
}
