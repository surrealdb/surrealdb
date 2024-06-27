use super::Value;
use serde::{
	de::{EnumAccess, Unexpected, VariantAccess, Visitor},
	Deserialize, Deserializer,
};
use uuid::Variant;

enum Variant {
	None,
	Bool,
	Number,
	String,
	Duration,
	Datetime,
	Uuid,
	Array,
	Object,
	Bytes,
	RecordId,
}

const VARIANTS: &'static [&'static str] = &[
	"None", "Null", "Bool", "Number", "Strand", "Duration", "Datetime", "Uuid", "Array", "Object",
	"Bytes", "Thing",
];

struct VariantVisitor;

impl Visitor for VariantVisitor {
	type Value = Variant;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "variant identifier")
	}

	fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			0 | 1 => Variant::None,
			2 => Variant::Bool,
			3 => Variant::Number,
			4 => Variant::String,
			5 => Variant::Duration,
			6 => Variant::Datetime,
			7 => Variant::Uuid,
			8 => Variant::Array,
			9 => Variant::Object,
			10 => todo!("geometry"),
			11 => Variant::Bytes,
			12 => Variant::RecordId,
			x @ 13..=28 => {
				return Err(E::invalid_value(
					Unexpected::Unsigned(x),
					&"primitive value variant 0 <= i < 13",
				))
			}
			x => {
				return Err(E::invalid_value(Unexpected::Unsigned(x), &"variant index 0 <= i < 13"))
			}
		};
		Ok(v)
	}

	fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			"None" | "Null" => Variant::None,
			"Bool" => Variant::Bool,
			"Number" => Variant::Number,
			"Strand" => Variant::String,
			"Duration" => Variant::Duration,
			"Datetime" => Variant::Datetime,
			"Uuid" => Variant::Uuid,
			"Array" => Variant::Array,
			"Object" => Variant::Array,
			//"Geometry" => Variant::Geometry,
			"Bytes" => Variant::Bytes,
			"Thing" => Variant::RecordId,
			x => Err(E::unknown_variant(x, VARIANTS)),
		};
		Ok(v)
	}

	fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			b"None" | b"Null" => Variant::None,
			b"Bool" => Variant::Bool,
			b"Number" => Variant::Number,
			b"Strand" => Variant::String,
			b"Duration" => Variant::Duration,
			b"Datetime" => Variant::Datetime,
			b"Uuid" => Variant::Uuid,
			b"Array" => Variant::Array,
			b"Object" => Variant::Array,
			//"Geometry" => Variant::Geometry,
			b"Bytes" => Variant::Bytes,
			b"Thing" => Variant::RecordId,
			x => {
				let x = String::from_utf8_lossy(v);
				Err(E::unknown_variant(&x, VARIANTS))
			}
		};
		Ok(v)
	}
}

impl<'de> Deserialize<'de> for Variant {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		Deserializer::deserialize_identifier(deserializer, VariantVisitor)
	}
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
	type Value;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		writeln!(formatter, "enum Value")
	}

	fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
	where
		A: serde::de::EnumAccess<'de>,
	{
		match EnumAccess::variant(data) {
			(Variant::None, var) => {
				VariantAccess::unit_variant(var)?;
				Ok(Value::None)
			}
			(Variant::Bool, var) => VariantAccess::newtype_variant(var).map(Value::Bool),
			(Variant::Number, var) => VariantAccess::newtype_variant(var).map(Value::Number),
			(Variant::String, var) => VariantAccess::newtype_variant(var).map(Value::String),
			(Variant::Duration, var) => VariantAccess::newtype_variant(var).map(Value::Duration),
			(Variant::Datetime, var) => VariantAccess::newtype_variant(var).map(Value::Datetime),
			(Variant::Uuid, var) => VariantAccess::newtype_variant(var).map(Value::Uuid),
			(Variant::Array, var) => VariantAccess::newtype_variant(var).map(Value::Array),
			(Variant::Object, var) => VariantAccess::newtype_variant(var).map(Value::Object),
			(Variant::Bytes, var) => VariantAccess::newtype_variant(var).map(Value::Bytes),
			(Variant::RecordId, var) => VariantAccess::newtype_variant(var).map(Value::RecordId),
		}
	}
}

impl<'de> Deserialize<'de> for Value {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		Deserializer::deserialize_enum(
			deserializer,
			"$surrealdb::private::sql::Value",
			VARIANTS,
			ValueVisitor,
		)
	}
}
