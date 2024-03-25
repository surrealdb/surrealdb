use crate::err::Error;
use crate::sql::language::Language;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Language;
	type Error = Error;

	type SerializeSeq = Impossible<Language, Error>;
	type SerializeTuple = Impossible<Language, Error>;
	type SerializeTupleStruct = Impossible<Language, Error>;
	type SerializeTupleVariant = Impossible<Language, Error>;
	type SerializeMap = Impossible<Language, Error>;
	type SerializeStruct = Impossible<Language, Error>;
	type SerializeStructVariant = Impossible<Language, Error>;

	const EXPECTED: &'static str = "an enum `Language`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Arabic" => Ok(Language::Arabic),
			"Danish" => Ok(Language::Danish),
			"Dutch" => Ok(Language::Dutch),
			"English" => Ok(Language::English),
			"French" => Ok(Language::French),
			"German" => Ok(Language::German),
			"Greek" => Ok(Language::Greek),
			"Hungarian" => Ok(Language::Hungarian),
			"Italian" => Ok(Language::Italian),
			"Norwegian" => Ok(Language::Norwegian),
			"Portuguese" => Ok(Language::Portuguese),
			"Romanian" => Ok(Language::Romanian),
			"Russian" => Ok(Language::Russian),
			"Spanish" => Ok(Language::Spanish),
			"Swedish" => Ok(Language::Swedish),
			"Tamil" => Ok(Language::Tamil),
			"Turkish" => Ok(Language::Turkish),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::ser::Serialize;

	#[test]
	fn english() {
		let perm = Language::English;
		let serialized = perm.serialize(Serializer.wrap()).unwrap();
		assert_eq!(perm, serialized);
	}
}
