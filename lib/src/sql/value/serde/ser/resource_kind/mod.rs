use crate::err::Error;
use crate::iam::{Level, ResourceKind};
use crate::sql::value::serde::ser;
use crate::sql::Number;
use rust_decimal::Decimal;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ResourceKind;
	type Error = Error;

	type SerializeSeq = Impossible<ResourceKind, Error>;
	type SerializeTuple = Impossible<ResourceKind, Error>;
	type SerializeTupleStruct = Impossible<ResourceKind, Error>;
	type SerializeTupleVariant = Impossible<ResourceKind, Error>;
	type SerializeMap = Impossible<ResourceKind, Error>;
	type SerializeStruct = Impossible<ResourceKind, Error>;
	type SerializeStructVariant = Impossible<ResourceKind, Error>;

	const EXPECTED: &'static str = "an enum `ResourceKind`";

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
			"No" => Ok(Level::No),
			"Root" => Ok(Level::Root),
			"Namespace" => Ok(Level::Namespace(value.serialize(ser::string::Serializer.wrap())?)),
			// TODO not sure how to parse 2-strings and 3-strings from a single value without vec or changing enum
			// "Database" => Ok(Number::Float(value.serialize(ser::primitive::f64::Serializer.wrap())?)),
			// "Scope" => Ok(Number::Float(value.serialize(ser::primitive::f64::Serializer.wrap())?)),
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
	fn int() {
		let number = Number::Int(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}

	#[test]
	fn float() {
		let number = Number::Float(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}

	#[test]
	fn decimal() {
		let number = Number::Decimal(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}
}
