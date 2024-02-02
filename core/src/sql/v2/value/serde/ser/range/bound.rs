use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Id;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::ops::Bound;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Bound<Id>;
	type Error = Error;

	type SerializeSeq = Impossible<Bound<Id>, Error>;
	type SerializeTuple = Impossible<Bound<Id>, Error>;
	type SerializeTupleStruct = Impossible<Bound<Id>, Error>;
	type SerializeTupleVariant = Impossible<Bound<Id>, Error>;
	type SerializeMap = Impossible<Bound<Id>, Error>;
	type SerializeStruct = Impossible<Bound<Id>, Error>;
	type SerializeStructVariant = Impossible<Bound<Id>, Error>;

	const EXPECTED: &'static str = "an enum `Bound<Id>`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Unbounded" => Ok(Bound::Unbounded),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

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
			"Included" => Ok(Bound::Included(value.serialize(ser::id::Serializer.wrap())?)),
			"Excluded" => Ok(Bound::Excluded(value.serialize(ser::id::Serializer.wrap())?)),
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

	#[test]
	fn unbounded() {
		let bound = Bound::Unbounded;
		let serialized = bound.serialize(Serializer.wrap()).unwrap();
		assert_eq!(bound, serialized);
	}

	#[test]
	fn included() {
		let bound = Bound::Included(Id::rand());
		let serialized = bound.serialize(Serializer.wrap()).unwrap();
		assert_eq!(bound, serialized);
	}

	#[test]
	fn excluded() {
		let bound = Bound::Excluded(Id::rand());
		let serialized = bound.serialize(Serializer.wrap()).unwrap();
		assert_eq!(bound, serialized);
	}
}
