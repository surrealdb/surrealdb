pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::with::With;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = With;
	type Error = Error;

	type SerializeSeq = Impossible<With, Error>;
	type SerializeTuple = Impossible<With, Error>;
	type SerializeTupleStruct = Impossible<With, Error>;
	type SerializeTupleVariant = Impossible<With, Error>;
	type SerializeMap = Impossible<With, Error>;
	type SerializeStruct = Impossible<With, Error>;
	type SerializeStructVariant = Impossible<With, Error>;

	const EXPECTED: &'static str = "an enum `With`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"NoIndex" => Ok(With::NoIndex),
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
			"Index" => Ok(With::Index(value.serialize(ser::string::vec::Serializer.wrap())?)),
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
	fn with_noindex() {
		let with = With::NoIndex;
		let serialized = with.serialize(Serializer.wrap()).unwrap();
		assert_eq!(with, serialized);
	}

	#[test]
	fn with_index() {
		let with = With::Index(vec!["idx".to_string(), "uniq".to_string()]);
		let serialized = with.serialize(Serializer.wrap()).unwrap();
		assert_eq!(with, serialized);
	}
}
