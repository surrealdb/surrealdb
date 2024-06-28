use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde::{
	ser::{Impossible, SerializeSeq, SerializeTuple, SerializeTupleStruct},
	Serialize, Serializer,
};

use crate::{api::Error, Number, Object, Value};

pub struct Serializer;

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

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Bool(v))
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v as _)
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v as _)
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		self.serialize_i64(v as _)
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Number(Number::Integer(v)))
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v as _)
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v as _)
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		self.serialize_u64(v as _)
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		if v < i64::MAX as u64 {
			Ok(Value::Number(Number::Integer(v as i64)))
		}

		Ok(Value::Number(Number::Decimal(Decimal::from(v))))
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		self.serialize_f64(v as _)
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Number(Number::Float(v)))
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		Ok(Value::String(v.into()))
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		Ok(Value::String(v.to_owned()))
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Bytes(v.to_vec()))
	}

	fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		todo!()
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::None)
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		self.serialize_unit()
	}

	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
		self.serialize_unit()
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		self.serialize_str(variant)
	}

	fn serialize_newtype_struct<T: ?Sized>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T: ?Sized>(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: serde::Serialize,
	{
		let v = value.serialize(v);
		let mut values = Object::new();
		values.insert(variant.to_owned(), v);
		Ok(Value::Object(values))
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Ok(SerializeVec {
			vec: Vec::with_capacity(len.unwrap_or(0)),
		})
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		todo!()
	}

	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		todo!()
	}

	fn serialize_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		self.serialize_map(Some(len))
	}

	fn serialize_struct_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		todo!()
	}

	fn collect_str<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: std::fmt::Display,
	{
		Ok(Value::String(value.to_string()))
	}
}

pub struct SerializeVec {
	vec: Vec<Value>,
}

impl SerializeSeq for SerializeVec {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Array(self.vec))
	}
}

impl SerializeTuple for SerializeVec {
	type Ok = Value;

	type Error = Error;

	fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Array(self.vec))
	}
}

impl SerializeTupleStruct for SerializeVec {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Array(self.vec))
	}
}

pub struct SerializeTupleVariant {
	name: String,
	vec: Vec<Value>,
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		self.vec.push(value.serialize(Serializer)?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let mut object = Object::new();
		object.insert(self.name, Value::Array(self.vec));
		Ok(Value::Object(object))
	}
}

pub struct SerializeMap {
	map: Object,
	next_key: Option<String>,
}

impl serde::ser::SerializeMap for SerializeMap {
	type Ok = Value;

	type Error = Error;

	fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		self.next_key = Some(key.serialize(MapKeySerializer)?);
		Ok(())
	}

	fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let v = value.serialize(Serializer)?;
		self.map
			.insert(self.next_key.take().expect("serialize_value called before serialize_key"), v);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Object(self.map))
	}
}

impl serde::ser::SerializeStruct for SerializeMap {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let v = value.serialize(Serializer)?;
		self.map.insert(key.to_string(), v);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Value::Object(self.map))
	}
}

pub struct SerializeStructVariant {
	name: String,
	map: Object,
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
	type Ok = Value;

	type Error = Error;

	fn serialize_field<T: ?Sized>(
		&mut self,
		key: &'static str,
		value: &T,
	) -> Result<(), Self::Error>
	where
		T: serde::Serialize,
	{
		let v = value.serialize(Serializer)?;
		self.map.insert(key.to_string(), v);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		let mut object = Object::new();
		object.insert(self.name, self.map.into());
		Ok(Value::Object(object))
	}
}

struct MapKeySerializer;

fn key_must_be_a_string() -> Error {
	Error::Serializer("Object key must be a string".to_string())
}

fn float_key_must_be_finite() -> Error {
	Error::Serializer("Object key numbers must be finite".to_string())
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
		Ok(value.to_string())
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
		T: ?Sized + std::fmt::Display,
	{
		Ok(value.to_string())
	}
}
