use super::{Number, Value};
use serde::{
	de::{EnumAccess, Unexpected, VariantAccess, Visitor},
	Deserialize, Deserializer,
};
use uuid::Variant;

enum NumberVariant {
	Integer,
	Float,
	Decimal,
}

const NUMBER_VARIANTS: &[&str] = &["Int", "Float", "Decimal"];

struct NumberVariantVisitor;

impl Visitor for NumberVariantVisitor {
	type Value = ValueVariant;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "variant identifier")
	}

	fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			0 => NumberVariant::Integer,
			1 => NumberVariant::Float,
			2 => NumberVariant::Decimal,

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
			"Int" => NumberVariant::Integer,
			"Float" => NumberVariant::Float,
			"Decimal" => NumberVariant::Decimal,
			x => Err(E::unknown_variant(x, NUMBER_VARIANTS)),
		};
		Ok(v)
	}

	fn visit_bytes<E>(self, v: &str) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			b"Int" => NumberVariant::Integer,
			b"Float" => NumberVariant::Float,
			b"Decimal" => NumberVariant::Decimal,
			x => Err(E::unknown_variant(x, NUMBER_VARIANTS)),
		};
		Ok(v)
	}
}

impl Deserialize for NumberVariant {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		deserializer.deserialize_identifier(NumberVariantVisitor)
	}
}

struct NumberVisitor;

impl<'de> Visitor<'de> for NumberVisitor {
	type Value = Number;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		writeln!(formatter, "enum Number")
	}

	fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
	where
		A: serde::de::EnumAccess<'de>,
	{
		match EnumAccess::variant(data) {
			(NumberVariant::Integer, var) => {
				VariantAccess::newtype_variant(var).map(Number::Integer)
			}
			(NumberVariant::Float, var) => VariantAccess::newtype_variant(var).map(Number::Float),
			(NumberVariant::Decimal, var) => {
				VariantAccess::newtype_variant(var).map(Number::Decimal)
			}
		}
	}
}

impl<'de> Deserialize<'de> for Number {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		deserializer.deserialize_enum(
			"$surrealdb::private::sql::Number",
			NUMBER_VARIANTS,
			NumberVisitor,
		)
	}
}

enum ValueVariant {
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

const VALUE_VARIANTS: &[&str] = &[
	"None", "Null", "Bool", "Number", "Strand", "Duration", "Datetime", "Uuid", "Array", "Object",
	"Bytes", "Thing",
];

struct ValueVariantVisitor;

impl Visitor for ValueVariantVisitor {
	type Value = ValueVariant;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "variant identifier")
	}

	fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			0 | 1 => ValueVariant::None,
			2 => ValueVariant::Bool,
			3 => ValueVariant::Number,
			4 => ValueVariant::String,
			5 => ValueVariant::Duration,
			6 => ValueVariant::Datetime,
			7 => ValueVariant::Uuid,
			8 => ValueVariant::Array,
			9 => ValueVariant::Object,
			10 => todo!("geometry"),
			11 => ValueVariant::Bytes,
			12 => ValueVariant::RecordId,
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
			"None" | "Null" => ValueVariant::None,
			"Bool" => ValueVariant::Bool,
			"Number" => ValueVariant::Number,
			"Strand" => ValueVariant::String,
			"Duration" => ValueVariant::Duration,
			"Datetime" => ValueVariant::Datetime,
			"Uuid" => ValueVariant::Uuid,
			"Array" => ValueVariant::Array,
			"Object" => ValueVariant::Array,
			//"Geometry" => Variant::Geometry,
			"Bytes" => ValueVariant::Bytes,
			"Thing" => ValueVariant::RecordId,
			x => Err(E::unknown_variant(x, VALUE_VARIANTS)),
		};
		Ok(v)
	}

	fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
	where
		E: serde::de::Error,
	{
		let v = match v {
			b"None" | b"Null" => ValueVariant::None,
			b"Bool" => ValueVariant::Bool,
			b"Number" => ValueVariant::Number,
			b"Strand" => ValueVariant::String,
			b"Duration" => ValueVariant::Duration,
			b"Datetime" => ValueVariant::Datetime,
			b"Uuid" => ValueVariant::Uuid,
			b"Array" => ValueVariant::Array,
			b"Object" => ValueVariant::Array,
			//"Geometry" => Variant::Geometry,
			b"Bytes" => ValueVariant::Bytes,
			b"Thing" => ValueVariant::RecordId,
			x => {
				let x = String::from_utf8_lossy(v);
				Err(E::unknown_variant(&x, VALUE_VARIANTS))
			}
		};
		Ok(v)
	}
}

impl<'de> Deserialize<'de> for ValueVariant {
	fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
	where
		D: serde::Deserializer<'de>,
	{
		Deserializer::deserialize_identifier(deserializer, ValueVariantVisitor)
	}
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
	type Value = Value;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		writeln!(formatter, "enum Value")
	}

	fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
	where
		A: serde::de::EnumAccess<'de>,
	{
		match EnumAccess::variant(data) {
			(ValueVariant::None, var) => {
				VariantAccess::unit_variant(var)?;
				Ok(Value::None)
			}
			(ValueVariant::Bool, var) => VariantAccess::newtype_variant(var).map(Value::Bool),
			(ValueVariant::Number, var) => VariantAccess::newtype_variant(var).map(Value::Number),
			(ValueVariant::String, var) => VariantAccess::newtype_variant(var).map(Value::String),
			(ValueVariant::Duration, var) => {
				VariantAccess::newtype_variant(var).map(Value::Duration)
			}
			(ValueVariant::Datetime, var) => {
				VariantAccess::newtype_variant(var).map(Value::Datetime)
			}
			(ValueVariant::Uuid, var) => VariantAccess::newtype_variant(var).map(Value::Uuid),
			(ValueVariant::Array, var) => VariantAccess::newtype_variant(var).map(Value::Array),
			(ValueVariant::Object, var) => VariantAccess::newtype_variant(var).map(Value::Object),
			(ValueVariant::Bytes, var) => VariantAccess::newtype_variant(var).map(Value::Bytes),
			(ValueVariant::RecordId, var) => {
				VariantAccess::newtype_variant(var).map(Value::RecordId)
			}
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
			VALUE_VARIANTS,
			ValueVisitor,
		)
	}
}
