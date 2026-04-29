use core::fmt::Display;

use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::Deserialize;
use serde::ser::{Impossible, Serialize};

use crate::error::Error;
use crate::value::Value;
use crate::{Array, Bytes, Datetime, Duration, Number, Object, RecordId, SerializationError, Uuid};

pub struct Serializer;

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: Display,
	{
		Self::serialization(msg.to_string(), SerializationError::Serialization)
	}
}

type Result<T> = core::result::Result<T, Error>;

impl serde::Serializer for Serializer {
	type Ok = Value;
	type Error = Error;

	type SerializeSeq = SerializeVec;
	type SerializeTuple = SerializeVec;
	type SerializeTupleStruct = SerializeVec;
	type SerializeTupleVariant = SerializeTupleVariant;
	type SerializeMap = SerializeMap;
	type SerializeStruct = SerializeMap;
	type SerializeStructVariant = SerializeStructVariant;

	#[inline]
	fn serialize_bool(self, value: bool) -> Result<Value> {
		Ok(Value::Bool(value))
	}

	#[inline]
	fn serialize_i8(self, value: i8) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	#[inline]
	fn serialize_i16(self, value: i16) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	#[inline]
	fn serialize_i32(self, value: i32) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	fn serialize_i64(self, value: i64) -> Result<Value> {
		Ok(Value::Number(value.into()))
	}

	fn serialize_i128(self, value: i128) -> Result<Value> {
		if let Ok(value) = i64::try_from(value) {
			Ok(Value::Number(Number::Int(value)))
		} else if let Some(decimal) = Decimal::from_i128(value) {
			Ok(Value::Number(Number::Decimal(decimal)))
		} else {
			Err(Error::serialization("number out of range".into(), None))
		}
	}

	#[inline]
	fn serialize_u8(self, value: u8) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	#[inline]
	fn serialize_u16(self, value: u16) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	#[inline]
	fn serialize_u32(self, value: u32) -> Result<Value> {
		self.serialize_i64(value as i64)
	}

	#[inline]
	fn serialize_u64(self, value: u64) -> Result<Value> {
		match i64::try_from(value) {
			Ok(value) => self.serialize_i64(value),
			_ => Ok(Value::Number(Number::Decimal(
				Decimal::from_u64(value).expect("from_u64 will ALWAYS return some"),
			))),
		}
	}

	fn serialize_u128(self, value: u128) -> Result<Value> {
		if let Ok(value) = i64::try_from(value) {
			Ok(Value::Number(Number::Int(value)))
		} else if let Some(decimal) = Decimal::from_u128(value) {
			Ok(Value::Number(Number::Decimal(decimal)))
		} else {
			Err(Error::serialization("number out of range".into(), None))
		}
	}

	#[inline]
	fn serialize_f32(self, float: f32) -> Result<Value> {
		Ok(Value::Number(Number::from_float(float as f64)))
	}

	#[inline]
	fn serialize_f64(self, float: f64) -> Result<Value> {
		Ok(Value::Number(Number::from_float(float)))
	}

	#[inline]
	fn serialize_char(self, value: char) -> Result<Value> {
		Ok(Value::String(value.into()))
	}

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Value> {
		Ok(Value::String(value.to_owned()))
	}

	fn serialize_bytes(self, value: &[u8]) -> Result<Value> {
		Ok(Value::Bytes(Bytes(::bytes::Bytes::copy_from_slice(value))))
	}

	#[inline]
	fn serialize_unit(self) -> Result<Value> {
		Ok(Value::Null)
	}

	#[inline]
	fn serialize_unit_struct(self, _name: &'static str) -> Result<Value> {
		self.serialize_unit()
	}

	#[inline]
	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Value> {
		self.serialize_str(variant)
	}

	#[inline]
	fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<Value>
	where
		T: ?Sized + Serialize,
	{
		let serialized = value.serialize(self)?;
		match name {
			"Datetime" => {
				let datetime =
					chrono::DateTime::<chrono::Utc>::deserialize(serialized).map_err(|err| {
						Error::serialization(err.to_string(), SerializationError::Deserialization)
					})?;
				Ok(Value::Datetime(Datetime::from(datetime)))
			}
			"Uuid" => {
				let uuid = uuid::Uuid::deserialize(serialized).map_err(|err| {
					Error::serialization(err.to_string(), SerializationError::Deserialization)
				})?;
				Ok(Value::Uuid(Uuid::from(uuid)))
			}
			"Duration" => {
				let duration = std::time::Duration::deserialize(serialized).map_err(|err| {
					Error::serialization(err.to_string(), SerializationError::Deserialization)
				})?;
				Ok(Value::Duration(Duration::from(duration)))
			}
			_ => Ok(serialized),
		}
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Value>
	where
		T: ?Sized + Serialize,
	{
		let mut values = Object::new();
		values.insert(String::from(variant), value.serialize(Self)?);
		Ok(Value::Object(values))
	}

	#[inline]
	fn serialize_none(self) -> Result<Value> {
		Ok(Value::None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Value>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self)
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
		Ok(SerializeVec {
			vec: Array::with_capacity(len.unwrap_or(0)),
		})
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant> {
		Ok(SerializeTupleVariant {
			name: String::from(variant),
			vec: Array::with_capacity(len),
		})
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
		Ok(SerializeMap {
			map: Object::new(),
			next_key: None,
			record_id_struct: false,
		})
	}

	fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
		let mut map = self.serialize_map(Some(len))?;
		if name == "RecordId" {
			map.record_id_struct = true;
		}
		Ok(map)
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant> {
		Ok(SerializeStructVariant {
			name: String::from(variant),
			map: Object::new(),
		})
	}

	fn collect_str<T>(self, value: &T) -> Result<Value>
	where
		T: ?Sized + Display,
	{
		Ok(Value::String(value.to_string()))
	}
}

pub struct SerializeVec {
	vec: Array,
}

pub struct SerializeTupleVariant {
	name: String,
	vec: Array,
}

pub struct SerializeMap {
	map: Object,
	next_key: Option<String>,
	record_id_struct: bool,
}

pub struct SerializeStructVariant {
	name: String,
	map: Object,
}

impl serde::ser::SerializeSeq for SerializeVec {
	type Ok = Value;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Value> {
		Ok(Value::Array(self.vec))
	}
}

impl serde::ser::SerializeTuple for SerializeVec {
	type Ok = Value;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		serde::ser::SerializeSeq::serialize_element(self, value)
	}

	fn end(self) -> Result<Value> {
		serde::ser::SerializeSeq::end(self)
	}
}

impl serde::ser::SerializeTupleStruct for SerializeVec {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		serde::ser::SerializeSeq::serialize_element(self, value)
	}

	fn end(self) -> Result<Value> {
		serde::ser::SerializeSeq::end(self)
	}
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Value> {
		let mut object = Object::new();

		object.insert(self.name, Value::Array(self.vec));

		Ok(Value::Object(object))
	}
}

impl serde::ser::SerializeMap for SerializeMap {
	type Ok = Value;
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.next_key = Some(key.serialize(MapKeySerializer)?);
		Ok(())
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		let key = self.next_key.take();
		// Panic because this indicates a bug in the program rather than an
		// expected failure.
		let key = key.expect("serialize_value called before serialize_key");
		self.map.insert(key, value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Value> {
		if self.record_id_struct {
			let record_id = RecordId::deserialize(Value::Object(self.map)).map_err(|err| {
				Error::serialization(err.to_string(), SerializationError::Deserialization)
			})?;
			return Ok(Value::RecordId(record_id));
		}
		Ok(Value::Object(self.map))
	}
}

struct MapKeySerializer;

fn key_must_be_a_string() -> Error {
	Error::serialization("key must be a string".into(), SerializationError::Serialization)
}

fn float_key_must_be_finite() -> Error {
	Error::serialization("float key must be finite".into(), SerializationError::Serialization)
}

impl serde::Serializer for MapKeySerializer {
	type Ok = String;
	type Error = Error;

	type SerializeSeq = Impossible<String, Error>;
	type SerializeTuple = Impossible<String, Error>;
	type SerializeTupleStruct = Impossible<String, Error>;
	type SerializeTupleVariant = Impossible<String, Error>;
	type SerializeMap = Impossible<String, Error>;
	type SerializeStruct = Impossible<String, Error>;
	type SerializeStructVariant = Impossible<String, Error>;

	#[inline]
	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<String> {
		Ok(variant.to_owned())
	}

	#[inline]
	fn serialize_newtype_struct<T>(self, _name: &'static str, value: &T) -> Result<String>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self)
	}

	fn serialize_bool(self, value: bool) -> Result<String> {
		Ok(if value {
			"true"
		} else {
			"false"
		}
		.to_owned())
	}

	fn serialize_i8(self, value: i8) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_i16(self, value: i16) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_i32(self, value: i32) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_i64(self, value: i64) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_i128(self, value: i128) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_u8(self, value: u8) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_u16(self, value: u16) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_u32(self, value: u32) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_u64(self, value: u64) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_u128(self, value: u128) -> Result<String> {
		Ok(value.to_string())
	}

	fn serialize_f32(self, value: f32) -> Result<String> {
		if value.is_finite() {
			Ok(value.to_string())
		} else {
			Err(float_key_must_be_finite())
		}
	}

	fn serialize_f64(self, value: f64) -> Result<String> {
		if value.is_finite() {
			Ok(value.to_string())
		} else {
			Err(float_key_must_be_finite())
		}
	}

	#[inline]
	fn serialize_char(self, value: char) -> Result<String> {
		Ok({
			let mut s = String::new();
			s.push(value);
			s
		})
	}

	#[inline]
	fn serialize_str(self, value: &str) -> Result<String> {
		Ok(value.to_owned())
	}

	fn serialize_bytes(self, _value: &[u8]) -> Result<String> {
		Err(key_must_be_a_string())
	}

	fn serialize_unit(self) -> Result<String> {
		Err(key_must_be_a_string())
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<String> {
		Err(key_must_be_a_string())
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_value: &T,
	) -> Result<String>
	where
		T: ?Sized + Serialize,
	{
		Err(key_must_be_a_string())
	}

	fn serialize_none(self) -> Result<String> {
		Err(key_must_be_a_string())
	}

	fn serialize_some<T>(self, _value: &T) -> Result<String>
	where
		T: ?Sized + Serialize,
	{
		Err(key_must_be_a_string())
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
		Err(key_must_be_a_string())
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
		Err(key_must_be_a_string())
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct> {
		Err(key_must_be_a_string())
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant> {
		Err(key_must_be_a_string())
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
		Err(key_must_be_a_string())
	}

	fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
		Err(key_must_be_a_string())
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant> {
		Err(key_must_be_a_string())
	}

	fn collect_str<T>(self, value: &T) -> Result<String>
	where
		T: ?Sized + Display,
	{
		Ok(value.to_string())
	}
}

impl serde::ser::SerializeStruct for SerializeMap {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		serde::ser::SerializeMap::serialize_entry(self, key, value)
	}

	fn end(self) -> Result<Value> {
		serde::ser::SerializeMap::end(self)
	}
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<()>
	where
		T: ?Sized + Serialize,
	{
		self.map.insert(String::from(key), value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Value> {
		let mut object = Object::new();

		object.insert(self.name, Value::Object(self.map));

		Ok(Value::Object(object))
	}
}
