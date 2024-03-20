pub(super) mod vec;

use crate::err::Error;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Tokenizer;
	type Error = Error;

	type SerializeSeq = Impossible<Tokenizer, Error>;
	type SerializeTuple = Impossible<Tokenizer, Error>;
	type SerializeTupleStruct = Impossible<Tokenizer, Error>;
	type SerializeTupleVariant = Impossible<Tokenizer, Error>;
	type SerializeMap = Impossible<Tokenizer, Error>;
	type SerializeStruct = Impossible<Tokenizer, Error>;
	type SerializeStructVariant = Impossible<Tokenizer, Error>;

	const EXPECTED: &'static str = "an enum `Tokenizer`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Blank" => Ok(Tokenizer::Blank),
			"Camel" => Ok(Tokenizer::Camel),
			"Class" => Ok(Tokenizer::Class),
			"Punct" => Ok(Tokenizer::Punct),
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
	fn blank() {
		let tokenizer = Tokenizer::Blank;
		let serialized = tokenizer.serialize(Serializer.wrap()).unwrap();
		assert_eq!(tokenizer, serialized);
	}

	#[test]
	fn camel() {
		let tokenizer = Tokenizer::Camel;
		let serialized = tokenizer.serialize(Serializer.wrap()).unwrap();
		assert_eq!(tokenizer, serialized);
	}

	#[test]
	fn class() {
		let tokenizer = Tokenizer::Class;
		let serialized = tokenizer.serialize(Serializer.wrap()).unwrap();
		assert_eq!(tokenizer, serialized);
	}

	#[test]
	fn punct() {
		let tokenizer = Tokenizer::Punct;
		let serialized = tokenizer.serialize(Serializer.wrap()).unwrap();
		assert_eq!(tokenizer, serialized);
	}
}
