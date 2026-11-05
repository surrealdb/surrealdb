use std::borrow::Cow;
use std::fmt::Display;

use rust_decimal::prelude::ToPrimitive;
use serde::de::{
	self, Deserialize, DeserializeSeed, Deserializer as _, EnumAccess, Expected, IntoDeserializer,
	MapAccess, SeqAccess, Unexpected, VariantAccess, Visitor,
};
use serde::{Serialize, forward_to_deserialize_any};

use super::ser::Serializer;
use crate::error::Error;
use crate::number::Number;
use crate::value::Value;
use crate::{Array, Object};

impl serde::de::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: Display,
	{
		Self::serialization(msg.to_string(), Some(crate::SerializationError::Deserialization))
	}
}

/// How a fractional Decimal is offered to a visitor.
#[derive(Clone, Copy)]
enum Fraction {
	/// As `f64`, for a target that asked for a float or an integer: a float
	/// target accepts the nearest `f64`, and an integer target refuses it.
	Float,
	/// As `f64` when that reproduces the Decimal exactly, and otherwise as its
	/// decimal text, for a self-describing target such as `rust_decimal::Decimal`
	/// or `serde_json::Value`, so the value is never rounded on the way in.
	Exact,
}

fn deserialize_number<'de, V>(n: &Number, visitor: V, fraction: Fraction) -> Result<V::Value, Error>
where
	V: Visitor<'de>,
{
	match n {
		Number::Int(v) => visitor.visit_i64(*v),
		Number::I8(v) => visitor.visit_i8(*v),
		Number::U8(v) => visitor.visit_u8(*v),
		Number::I16(v) => visitor.visit_i16(*v),
		Number::U16(v) => visitor.visit_u16(*v),
		Number::I32(v) => visitor.visit_i32(*v),
		Number::U32(v) => visitor.visit_u32(*v),
		Number::U64(v) => visitor.visit_u64(*v),
		Number::F16(v) => visitor.visit_f32(f32::from(*v)),
		Number::F32(v) => visitor.visit_f32(*v),
		Number::Float(v) => visitor.visit_f64(*v),
		// A bigdecimal that fits a decimal is offered as that decimal. Any other value is offered
		// as its text, since no visitor method takes an arbitrary-precision number.
		Number::BigDecimal(v) => match rust_decimal::Decimal::try_from(v.as_ref()) {
			Ok(decimal) => deserialize_number(&Number::Decimal(decimal), visitor, fraction),
			Err(_) => visitor.visit_string(v.to_string()),
		},
		Number::Decimal(decimal) => {
			// A whole Decimal is offered as the narrowest of `u64`, `i64`, `u128` and
			// `i128` that holds it, so integer visitors of any width accept it. A
			// fractional Decimal is offered as described by `Fraction`, so it is never
			// truncated to an integer.
			if decimal.is_integer() {
				if let Some(v) = decimal.to_u64() {
					return visitor.visit_u64(v);
				}
				if let Some(v) = decimal.to_i64() {
					return visitor.visit_i64(v);
				}
				if let Some(v) = decimal.to_u128() {
					return visitor.visit_u128(v);
				}
				if let Some(v) = decimal.to_i128() {
					return visitor.visit_i128(v);
				}
				return Err(serde::de::Error::custom("decimal value not in range of an i128"));
			}
			let float = decimal.to_f64();
			match fraction {
				Fraction::Float => match float {
					Some(v) => visitor.visit_f64(v),
					None => {
						Err(serde::de::Error::custom("decimal value not representable as an f64"))
					}
				},
				Fraction::Exact => match float {
					// `f64` keeps a numeric target numeric when nothing is lost: the
					// shortest text of the float parses back to the same Decimal
					Some(v)
						if v.to_string().parse::<rust_decimal::Decimal>().ok()
							== Some(*decimal) =>
					{
						visitor.visit_f64(v)
					}
					_ => visitor.visit_string(decimal.to_string()),
				},
			}
		}
	}
}

macro_rules! deserialize_number {
	($method:ident) => {
		fn $method<V>(self, visitor: V) -> Result<V::Value, Error>
		where
			V: Visitor<'de>,
		{
			if let Value::Number(n) = self {
				deserialize_number(&n, visitor, Fraction::Float)
			} else {
				Err(self.invalid_type(&visitor))
			}
		}
	};
}

fn visit_array<'de, V>(array: Array, visitor: V) -> Result<V::Value, Error>
where
	V: Visitor<'de>,
{
	let len = array.len();
	let mut deserializer = SeqDeserializer::new(array);
	let seq = visitor.visit_seq(&mut deserializer)?;
	let remaining = deserializer.iter.len();
	if remaining == 0 {
		Ok(seq)
	} else {
		Err(serde::de::Error::invalid_length(len, &"fewer elements in array"))
	}
}

impl<'de> serde::Deserializer<'de> for Object {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		let len = self.len();
		let mut deserializer = MapDeserializer::new(self);
		let map = visitor.visit_map(&mut deserializer)?;
		let remaining = deserializer.iter.len();
		if remaining == 0 {
			Ok(map)
		} else {
			Err(serde::de::Error::invalid_length(len, &"fewer elements in map"))
		}
	}

	fn deserialize_enum<V>(
		self,
		_name: &'static str,
		_variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		let mut iter = self.into_iter();
		let (variant, value) = match iter.next() {
			Some(v) => v,
			None => {
				return Err(serde::de::Error::invalid_value(
					Unexpected::Map,
					&"map with a single key",
				));
			}
		};
		// enums are encoded in json as maps with a single key:value pair
		if iter.next().is_some() {
			return Err(serde::de::Error::invalid_value(Unexpected::Map, &"map with a single key"));
		}

		visitor.visit_enum(EnumDeserializer {
			variant,
			value: Some(value),
		})
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		drop(self);
		visitor.visit_unit()
	}

	forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
		bytes byte_buf option unit unit_struct newtype_struct seq tuple
		tuple_struct map struct identifier
	}
}

impl<'de> serde::Deserializer<'de> for Value {
	type Error = Error;

	#[inline]
	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Value::Null => visitor.visit_unit(),
			Value::Bool(v) => visitor.visit_bool(v),
			Value::Number(n) => deserialize_number(&n, visitor, Fraction::Exact),
			Value::String(v) => visitor.visit_string(v),
			Value::Array(v) => visit_array(v, visitor),
			Value::Object(v) => v.deserialize_any(visitor),
			Value::None => visitor.visit_none(),
			Value::Bytes(bytes) => visitor.visit_bytes(&bytes),
			Value::Date(v) => visitor.visit_string(v.to_string()),
			Value::Time(v) => visitor.visit_string(v.to_string()),
			Value::Timestamp(v) => visitor.visit_string(v.to_string()),
			Value::Interval(v) => visitor.visit_string(v.to_string()),
			Value::Datetime(datetime) => {
				let value = datetime
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				value.deserialize_any(visitor)
			}
			Value::Uuid(uuid) => {
				let value = uuid
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				value.deserialize_any(visitor)
			}
			Value::Duration(duration) => {
				let value = duration
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				value.deserialize_any(visitor)
			}
			Value::RecordId(record_id) => {
				let mut object = Object::new();
				object.insert("table".to_owned(), Value::String(record_id.table.to_string()));
				object.insert(
					"key".to_owned(),
					record_id
						.key
						.serialize(Serializer)
						.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?,
				);
				Value::Object(object).deserialize_any(visitor)
			}
			Value::Geometry(_)
			| Value::Table(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::Set(_) => Err(self.invalid_type(&visitor)),
		}
	}

	deserialize_number!(deserialize_i8);
	deserialize_number!(deserialize_i16);
	deserialize_number!(deserialize_i32);
	deserialize_number!(deserialize_i64);
	deserialize_number!(deserialize_i128);
	deserialize_number!(deserialize_u8);
	deserialize_number!(deserialize_u16);
	deserialize_number!(deserialize_u32);
	deserialize_number!(deserialize_u64);
	deserialize_number!(deserialize_u128);
	deserialize_number!(deserialize_f32);
	deserialize_number!(deserialize_f64);

	#[inline]
	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::None = self {
			visitor.visit_none()
		} else {
			visitor.visit_some(self)
		}
	}

	#[inline]
	fn deserialize_enum<V>(
		self,
		name: &'static str,
		variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Value::Object(value) => value.deserialize_enum(name, variants, visitor),
			Value::String(variant) => visitor.visit_enum(EnumDeserializer {
				variant,
				value: None,
			}),
			other @ (Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Bytes(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::Array(_)
			| Value::Set(_)) => Err(serde::de::Error::invalid_type(other.unexpected(), &"string or map")),
		}
	}

	#[inline]
	fn deserialize_newtype_struct<V>(
		self,
		name: &'static str,
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match (name, self) {
			("Datetime", Value::Datetime(datetime)) => {
				let value = datetime
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				visitor.visit_newtype_struct(value)
			}
			("Uuid", Value::Uuid(uuid)) => {
				let value = uuid
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				visitor.visit_newtype_struct(value)
			}
			("Duration", Value::Duration(duration)) => {
				let value = duration
					.into_inner()
					.serialize(Serializer)
					.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?;
				visitor.visit_newtype_struct(value)
			}
			(_, value) => visitor.visit_newtype_struct(value),
		}
	}

	fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::Bool(v) = self {
			visitor.visit_bool(v)
		} else {
			Err(self.invalid_type(&visitor))
		}
	}

	fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_string(visitor)
	}

	fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_string(visitor)
	}

	fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Value::String(v) => visitor.visit_string(v),
			Value::Number(Number::Decimal(d)) => visitor.visit_string(d.to_string()),
			Value::Number(Number::BigDecimal(d)) => visitor.visit_string(d.to_string()),
			other @ Value::None
			| other @ Value::Null
			| other @ Value::Bool(_)
			| other @ Value::Number(_)
			| other @ Value::Bytes(_)
			| other @ Value::Duration(_)
			| other @ Value::Datetime(_)
			| other @ Value::Date(_)
			| other @ Value::Time(_)
			| other @ Value::Timestamp(_)
			| other @ Value::Interval(_)
			| other @ Value::Uuid(_)
			| other @ Value::Geometry(_)
			| other @ Value::Table(_)
			| other @ Value::RecordId(_)
			| other @ Value::File(_)
			| other @ Value::Range(_)
			| other @ Value::Regex(_)
			| other @ Value::Array(_)
			| other @ Value::Object(_)
			| other @ Value::Set(_) => Err(other.invalid_type(&visitor)),
		}
	}

	fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_byte_buf(visitor)
	}

	fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::Bytes(v) = self {
			visitor.visit_bytes(&v)
		} else {
			Err(self.invalid_type(&visitor))
		}
	}

	fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::Null = self {
			visitor.visit_unit()
		} else {
			Err(self.invalid_type(&visitor))
		}
	}

	fn deserialize_unit_struct<V>(self, _name: &'static str, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_unit(visitor)
	}

	fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::Array(v) = self {
			visit_array(v, visitor)
		} else {
			Err(self.invalid_type(&visitor))
		}
	}

	fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_seq(visitor)
	}

	fn deserialize_tuple_struct<V>(
		self,
		_name: &'static str,
		_len: usize,
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_seq(visitor)
	}

	fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		if let Value::Object(v) = self {
			v.deserialize_any(visitor)
		} else {
			Err(self.invalid_type(&visitor))
		}
	}

	fn deserialize_struct<V>(
		self,
		name: &'static str,
		fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self {
			Value::Array(v) => visit_array(v, visitor),
			Value::Object(v) => v.deserialize_any(visitor),
			Value::RecordId(record_id) if name == "RecordId" => {
				let mut object = Object::new();
				object.insert("table".to_owned(), Value::String(record_id.table.to_string()));
				object.insert(
					"key".to_owned(),
					record_id
						.key
						.serialize(Serializer)
						.map_err(|err| <Error as serde::de::Error>::custom(err.to_string()))?,
				);
				let value = Value::Object(object);
				value.deserialize_struct(name, fields, visitor)
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::String(_)
			| Value::Bytes(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)
			| Value::Set(_) => Err(self.invalid_type(&visitor)),
		}
	}

	fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		self.deserialize_string(visitor)
	}

	fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		drop(self);
		visitor.visit_unit()
	}
}

struct EnumDeserializer {
	variant: String,
	value: Option<Value>,
}

impl<'de> EnumAccess<'de> for EnumDeserializer {
	type Error = Error;
	type Variant = VariantDeserializer;

	fn variant_seed<V>(self, seed: V) -> Result<(V::Value, VariantDeserializer), Error>
	where
		V: DeserializeSeed<'de>,
	{
		let variant = self.variant.into_deserializer();
		let visitor = VariantDeserializer {
			value: self.value,
		};
		seed.deserialize(variant).map(|v| (v, visitor))
	}
}

impl<'de> IntoDeserializer<'de, Error> for Value {
	type Deserializer = Self;

	fn into_deserializer(self) -> Self::Deserializer {
		self
	}
}

struct VariantDeserializer {
	value: Option<Value>,
}

impl<'de> VariantAccess<'de> for VariantDeserializer {
	type Error = Error;

	fn unit_variant(self) -> Result<(), Error> {
		match self.value {
			Some(value) => Deserialize::deserialize(value),
			None => Ok(()),
		}
	}

	fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, Error>
	where
		T: DeserializeSeed<'de>,
	{
		match self.value {
			Some(value) => seed.deserialize(value),
			None => {
				Err(serde::de::Error::invalid_type(Unexpected::UnitVariant, &"newtype variant"))
			}
		}
	}

	fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self.value {
			Some(Value::Array(v)) => {
				if v.is_empty() {
					visitor.visit_unit()
				} else {
					visit_array(v, visitor)
				}
			}
			Some(other) => {
				Err(serde::de::Error::invalid_type(other.unexpected(), &"tuple variant"))
			}
			None => Err(serde::de::Error::invalid_type(Unexpected::UnitVariant, &"tuple variant")),
		}
	}

	fn struct_variant<V>(
		self,
		_fields: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		match self.value {
			Some(Value::Object(v)) => v.deserialize_any(visitor),
			Some(other) => {
				Err(serde::de::Error::invalid_type(other.unexpected(), &"struct variant"))
			}
			None => Err(serde::de::Error::invalid_type(Unexpected::UnitVariant, &"struct variant")),
		}
	}
}

struct SeqDeserializer {
	iter: <Array as IntoIterator>::IntoIter,
}

impl SeqDeserializer {
	fn new(vec: Array) -> Self {
		SeqDeserializer {
			iter: vec.into_iter(),
		}
	}
}

impl<'de> SeqAccess<'de> for SeqDeserializer {
	type Error = Error;

	fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
	where
		T: DeserializeSeed<'de>,
	{
		match self.iter.next() {
			Some(value) => seed.deserialize(value).map(Some),
			None => Ok(None),
		}
	}

	fn size_hint(&self) -> Option<usize> {
		match self.iter.size_hint() {
			(lower, Some(upper)) if lower == upper => Some(upper),
			_ => None,
		}
	}
}

struct MapDeserializer {
	iter: <Object as IntoIterator>::IntoIter,
	value: Option<Value>,
}

impl MapDeserializer {
	fn new(map: Object) -> Self {
		MapDeserializer {
			iter: map.into_iter(),
			value: None,
		}
	}
}

impl<'de> MapAccess<'de> for MapDeserializer {
	type Error = Error;

	fn next_key_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Error>
	where
		T: DeserializeSeed<'de>,
	{
		match self.iter.next() {
			Some((key, value)) => {
				self.value = Some(value);
				let key_de = MapKeyDeserializer {
					key: Cow::Owned(key),
				};
				seed.deserialize(key_de).map(Some)
			}
			None => Ok(None),
		}
	}

	fn next_value_seed<T>(&mut self, seed: T) -> Result<T::Value, Error>
	where
		T: DeserializeSeed<'de>,
	{
		match self.value.take() {
			Some(value) => seed.deserialize(value),
			None => Err(serde::de::Error::custom("value is missing")),
		}
	}

	fn size_hint(&self) -> Option<usize> {
		match self.iter.size_hint() {
			(lower, Some(upper)) if lower == upper => Some(upper),
			_ => None,
		}
	}
}

struct MapKeyDeserializer<'de> {
	key: Cow<'de, str>,
}

macro_rules! unexpected {
	($name: ident, $unex: expr) => {
		fn $name<V>(self, visitor: V) -> Result<V::Value, Error>
		where
			V: Visitor<'de>,
		{
			Err(serde::de::Error::invalid_type($unex, &visitor))
		}
	};
}

impl<'de> serde::Deserializer<'de> for MapKeyDeserializer<'de> {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		BorrowedCowStrDeserializer::new(self.key).deserialize_any(visitor)
	}

	#[inline]
	fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: Visitor<'de>,
	{
		// Map keys cannot be null.
		visitor.visit_some(self)
	}

	forward_to_deserialize_any! {
		char str string bytes byte_buf unit unit_struct seq tuple tuple_struct
		map struct identifier ignored_any
	}

	unexpected!(deserialize_bool, Unexpected::Other("bool"));
	unexpected!(deserialize_i8, Unexpected::Other("number"));
	unexpected!(deserialize_i16, Unexpected::Other("number"));
	unexpected!(deserialize_i32, Unexpected::Other("number"));
	unexpected!(deserialize_i64, Unexpected::Other("number"));
	unexpected!(deserialize_u8, Unexpected::Other("number"));
	unexpected!(deserialize_u16, Unexpected::Other("number"));
	unexpected!(deserialize_u32, Unexpected::Other("number"));
	unexpected!(deserialize_u64, Unexpected::Other("number"));
	unexpected!(deserialize_f32, Unexpected::Other("number"));
	unexpected!(deserialize_f64, Unexpected::Other("number"));

	fn deserialize_newtype_struct<V>(
		self,
		_name: &'static str,
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		Err(serde::de::Error::invalid_type(Unexpected::NewtypeStruct, &visitor))
	}

	fn deserialize_enum<V>(
		self,
		_name: &'static str,
		_variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Self::Error>
	where
		V: Visitor<'de>,
	{
		Err(serde::de::Error::invalid_type(Unexpected::Enum, &visitor))
	}
}

impl Value {
	#[cold]
	fn invalid_type<E>(&self, exp: &dyn Expected) -> E
	where
		E: serde::de::Error,
	{
		serde::de::Error::invalid_type(self.unexpected(), exp)
	}

	#[cold]
	fn unexpected(&self) -> Unexpected<'_> {
		match self {
			Value::Null => Unexpected::Unit,
			Value::Bool(b) => Unexpected::Bool(*b),
			Value::Number(_) => Unexpected::Other("number"),
			Value::String(s) => Unexpected::Str(s),
			Value::Array(_) => Unexpected::Seq,
			Value::Object(_) => Unexpected::Map,
			Value::None => Unexpected::Option,
			Value::Bytes(_) => Unexpected::Seq,
			Value::Duration(_) => Unexpected::Other("duration"),
			Value::Datetime(_) => Unexpected::Other("datetime"),
			Value::Date(_) => Unexpected::Other("date"),
			Value::Time(_) => Unexpected::Other("time"),
			Value::Timestamp(_) => Unexpected::Other("timestamp"),
			Value::Interval(_) => Unexpected::Other("interval"),
			Value::Uuid(_) => Unexpected::Other("uuid"),
			Value::Geometry(_) => Unexpected::Other("geometry"),
			Value::Table(_) => Unexpected::Other("table"),
			Value::RecordId(_) => Unexpected::Other("record id"),
			Value::File(_) => Unexpected::Other("file"),
			Value::Range(_) => Unexpected::Other("range"),
			Value::Regex(_) => Unexpected::Other("regex"),
			Value::Set(_) => Unexpected::Other("set"),
		}
	}
}

struct BorrowedCowStrDeserializer<'de> {
	value: Cow<'de, str>,
}

impl<'de> BorrowedCowStrDeserializer<'de> {
	fn new(value: Cow<'de, str>) -> Self {
		BorrowedCowStrDeserializer {
			value,
		}
	}
}

impl<'de> de::Deserializer<'de> for BorrowedCowStrDeserializer<'de> {
	type Error = Error;

	fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Error>
	where
		V: de::Visitor<'de>,
	{
		match self.value {
			Cow::Borrowed(string) => visitor.visit_borrowed_str(string),
			Cow::Owned(string) => visitor.visit_string(string),
		}
	}

	fn deserialize_enum<V>(
		self,
		_name: &str,
		_variants: &'static [&'static str],
		visitor: V,
	) -> Result<V::Value, Error>
	where
		V: de::Visitor<'de>,
	{
		visitor.visit_enum(self)
	}

	forward_to_deserialize_any! {
		bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
		bytes byte_buf option unit unit_struct newtype_struct seq tuple
		tuple_struct map struct identifier ignored_any
	}
}

impl<'de> de::EnumAccess<'de> for BorrowedCowStrDeserializer<'de> {
	type Error = Error;
	type Variant = UnitOnly;

	fn variant_seed<T>(self, seed: T) -> Result<(T::Value, Self::Variant), Error>
	where
		T: de::DeserializeSeed<'de>,
	{
		let value = seed.deserialize(self)?;
		Ok((value, UnitOnly))
	}
}

struct UnitOnly;

impl<'de> de::VariantAccess<'de> for UnitOnly {
	type Error = Error;

	fn unit_variant(self) -> Result<(), Error> {
		Ok(())
	}

	fn newtype_variant_seed<T>(self, _seed: T) -> Result<T::Value, Error>
	where
		T: de::DeserializeSeed<'de>,
	{
		Err(de::Error::invalid_type(Unexpected::UnitVariant, &"newtype variant"))
	}

	fn tuple_variant<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Error>
	where
		V: de::Visitor<'de>,
	{
		Err(de::Error::invalid_type(Unexpected::UnitVariant, &"tuple variant"))
	}

	fn struct_variant<V>(
		self,
		_fields: &'static [&'static str],
		_visitor: V,
	) -> Result<V::Value, Error>
	where
		V: de::Visitor<'de>,
	{
		Err(de::Error::invalid_type(Unexpected::UnitVariant, &"struct variant"))
	}
}
