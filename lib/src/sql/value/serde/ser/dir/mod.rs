use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Dir;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Dir;
	type Error = Error;

	type SerializeSeq = Impossible<Dir, Error>;
	type SerializeTuple = Impossible<Dir, Error>;
	type SerializeTupleStruct = Impossible<Dir, Error>;
	type SerializeTupleVariant = Impossible<Dir, Error>;
	type SerializeMap = Impossible<Dir, Error>;
	type SerializeStruct = Impossible<Dir, Error>;
	type SerializeStructVariant = Impossible<Dir, Error>;

	const EXPECTED: &'static str = "an enum `Dir`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"In" => Ok(Dir::In),
			"Out" => Ok(Dir::Out),
			"Both" => Ok(Dir::Both),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn r#in() {
		let dir = Dir::In;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn out() {
		let dir = Dir::Out;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}

	#[test]
	fn both() {
		let dir = Dir::Both;
		let serialized = dir.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dir, serialized);
	}
}
