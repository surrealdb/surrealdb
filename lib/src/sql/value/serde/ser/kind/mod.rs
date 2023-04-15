use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Kind;
	type Error = Error;

	type SerializeSeq = Impossible<Kind, Error>;
	type SerializeTuple = Impossible<Kind, Error>;
	type SerializeTupleStruct = Impossible<Kind, Error>;
	type SerializeTupleVariant = Impossible<Kind, Error>;
	type SerializeMap = Impossible<Kind, Error>;
	type SerializeStruct = Impossible<Kind, Error>;
	type SerializeStructVariant = Impossible<Kind, Error>;

	const EXPECTED: &'static str = "an enum `Kind`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Any" => Ok(Kind::Any),
			"Bool" => Ok(Kind::Bool),
			"Bytes" => Ok(Kind::Bytes),
			"Datetime" => Ok(Kind::Datetime),
			"Decimal" => Ok(Kind::Decimal),
			"Duration" => Ok(Kind::Duration),
			"Float" => Ok(Kind::Float),
			"Int" => Ok(Kind::Int),
			"Number" => Ok(Kind::Number),
			"Object" => Ok(Kind::Object),
			"Point" => Ok(Kind::Point),
			"String" => Ok(Kind::String),
			"Uuid" => Ok(Kind::Uuid),
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
			"Record" => Ok(Kind::Record(value.serialize(ser::table::vec::Serializer.wrap())?)),
			"Geometry" => Ok(Kind::Geometry(value.serialize(ser::string::vec::Serializer.wrap())?)),
			"Option" => todo!(),
			"Either" => todo!(),
			"Set" => todo!(),
			"Array" => todo!(),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use ser::Serializer as _;

	#[test]
	fn any() {
		let kind = Kind::Any;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn bool() {
		let kind = Kind::Bool;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn bytes() {
		let kind = Kind::Bytes;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn datetime() {
		let kind = Kind::Datetime;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn decimal() {
		let kind = Kind::Decimal;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn duration() {
		let kind = Kind::Duration;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn float() {
		let kind = Kind::Float;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn int() {
		let kind = Kind::Int;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn number() {
		let kind = Kind::Number;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn object() {
		let kind = Kind::Object;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn point() {
		let kind = Kind::Point;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn string() {
		let kind = Kind::String;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn uuid() {
		let kind = Kind::Uuid;
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn record() {
		let kind = Kind::Record(Default::default());
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Record(vec![Default::default()]);
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn geometry() {
		let kind = Kind::Geometry(Default::default());
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Geometry(vec![Default::default()]);
		let serialized = serialize_internal(|| kind.serialize(Serializer.wrap())).unwrap();
		assert_eq!(kind, serialized);
	}
}
