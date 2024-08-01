mod access_type;
mod algorithm;
mod base;
mod block;
mod cast;
mod changefeed;
mod cond;
mod constant;
mod data;
mod datetime;
mod decimal;
mod dir;
mod distance;
mod duration;
mod edges;
mod explain;
mod expression;
mod fetch;
mod fetchs;
mod field;
mod fields;
mod filter;
mod function;
mod geometry;
mod graph;
mod group;
mod id;
mod ident;
mod idiom;
mod index;
mod kind;
mod language;
mod limit;
mod mock;
mod number;
mod operator;
mod order;
mod output;
mod part;
mod permission;
mod permissions;
mod primitive;
mod range;
mod relation;
mod scoring;
mod split;
mod start;
mod statement;
mod strand;
mod string;
mod subquery;
mod table;
mod table_type;
mod thing;
mod timeout;
mod tokenizer;
mod uuid;
mod value;
mod values;
mod vectortype;
mod version;
mod view;
mod with;

use serde::ser::Error;
use serde::ser::Serialize;
use serde::ser::SerializeMap;
use serde::ser::SerializeSeq;
use serde::ser::SerializeStruct;
use serde::ser::SerializeStructVariant;
use serde::ser::SerializeTuple;
use serde::ser::SerializeTupleStruct;
use serde::ser::SerializeTupleVariant;
use std::fmt::Display;

pub use value::to_value;

trait Serializer: Sized {
	type Ok;
	type Error: Error;

	type SerializeSeq: SerializeSeq<Ok = Self::Ok, Error = Self::Error>;
	type SerializeTuple: SerializeTuple<Ok = Self::Ok, Error = Self::Error>;
	type SerializeTupleStruct: SerializeTupleStruct<Ok = Self::Ok, Error = Self::Error>;
	type SerializeTupleVariant: SerializeTupleVariant<Ok = Self::Ok, Error = Self::Error>;
	type SerializeMap: SerializeMap<Ok = Self::Ok, Error = Self::Error>;
	type SerializeStruct: SerializeStruct<Ok = Self::Ok, Error = Self::Error>;
	type SerializeStructVariant: SerializeStructVariant<Ok = Self::Ok, Error = Self::Error>;

	const EXPECTED: &'static str;

	fn unexpected(typ: &str, value: Option<impl Display>) -> Self::Error {
		let message = match value {
			Some(value) => format!("unexpected {typ} `{value}`, expected {}", Self::EXPECTED),
			None => format!("unexpected {typ}, expected {}", Self::EXPECTED),
		};
		Self::Error::custom(message)
	}

	#[inline]
	fn wrap(self) -> Wrapper<Self> {
		Wrapper(self)
	}

	fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("bool", Some(value)))
	}

	fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("i8", Some(value)))
	}

	fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("i16", Some(value)))
	}

	fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("i32", Some(value)))
	}

	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("i64", Some(value)))
	}

	fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("i128", Some(value)))
	}

	fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("u8", Some(value)))
	}

	fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("u16", Some(value)))
	}

	fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("u32", Some(value)))
	}

	fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("u64", Some(value)))
	}

	fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("u128", Some(value)))
	}

	fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("f32", Some(value)))
	}

	fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("f64", Some(value)))
	}

	fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("char", Some(value)))
	}

	fn serialize_str(self, _value: &str) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("str", None::<String>))
	}

	fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("bytes", None::<String>))
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("unit", None::<String>))
	}

	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("unit variant", Some(format!("{name}::{variant}"))))
	}

	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("unit struct", Some(name)))
	}

	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Err(Self::unexpected("newtype variant", Some(name)))
	}

	fn serialize_newtype_struct<T>(
		self,
		name: &'static str,
		_value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Err(Self::unexpected("newtype struct", Some(name)))
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Err(Self::unexpected("none", None::<String>))
	}

	fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Err(Self::unexpected("some", None::<String>))
	}

	fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		Err(Self::unexpected("sequence", None::<String>))
	}

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Err(Self::unexpected("tuple", None::<String>))
	}

	fn serialize_tuple_struct(
		self,
		name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		Err(Self::unexpected("tuple struct", Some(name)))
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		Err(Self::unexpected("tuple variant", Some(name)))
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		Err(Self::unexpected("map", None::<String>))
	}

	fn serialize_struct(
		self,
		name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		Err(Self::unexpected("struct", Some(name)))
	}

	fn serialize_struct_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		_variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		Err(Self::unexpected("struct variant", Some(name)))
	}

	fn collect_str<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: Display + ?Sized,
	{
		self.serialize_str(&value.to_string())
	}

	#[inline]
	fn is_human_readable(&self) -> bool {
		false
	}
}

struct Wrapper<S: Serializer>(S);

impl<S> serde::ser::Serializer for Wrapper<S>
where
	S: Serializer,
{
	type Ok = S::Ok;
	type Error = S::Error;

	type SerializeSeq = S::SerializeSeq;
	type SerializeTuple = S::SerializeTuple;
	type SerializeTupleStruct = S::SerializeTupleStruct;
	type SerializeTupleVariant = S::SerializeTupleVariant;
	type SerializeMap = S::SerializeMap;
	type SerializeStruct = S::SerializeStruct;
	type SerializeStructVariant = S::SerializeStructVariant;

	#[inline]
	fn serialize_bool(self, value: bool) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_bool(value)
	}

	#[inline]
	fn serialize_i8(self, value: i8) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_i8(value)
	}

	#[inline]
	fn serialize_i16(self, value: i16) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_i16(value)
	}

	#[inline]
	fn serialize_i32(self, value: i32) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_i32(value)
	}

	#[inline]
	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_i64(value)
	}

	#[inline]
	fn serialize_i128(self, value: i128) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_i128(value)
	}

	#[inline]
	fn serialize_u8(self, value: u8) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_u8(value)
	}

	#[inline]
	fn serialize_u16(self, value: u16) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_u16(value)
	}

	#[inline]
	fn serialize_u32(self, value: u32) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_u32(value)
	}

	#[inline]
	fn serialize_u64(self, value: u64) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_u64(value)
	}

	#[inline]
	fn serialize_u128(self, value: u128) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_u128(value)
	}

	#[inline]
	fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_f32(value)
	}

	#[inline]
	fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_f64(value)
	}

	#[inline]
	fn serialize_char(self, value: char) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_char(value)
	}

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_str(value)
	}

	#[inline]
	fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_bytes(value)
	}

	#[inline]
	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_unit()
	}

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_unit_variant(name, variant_index, variant)
	}

	#[inline]
	fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_unit_struct(name)
	}

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_newtype_variant(name, variant_index, variant, value)
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_newtype_struct(name, value)
	}

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		self.0.serialize_none()
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_some(value)
	}

	#[inline]
	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		self.0.serialize_seq(len)
	}

	#[inline]
	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		self.0.serialize_tuple(len)
	}

	#[inline]
	fn serialize_tuple_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		self.0.serialize_tuple_struct(name, len)
	}

	#[inline]
	fn serialize_tuple_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		self.0.serialize_tuple_variant(name, variant_index, variant, len)
	}

	#[inline]
	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		self.0.serialize_map(len)
	}

	#[inline]
	fn serialize_struct(
		self,
		name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		self.0.serialize_struct(name, len)
	}

	#[inline]
	fn serialize_struct_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		self.0.serialize_struct_variant(name, variant_index, variant, len)
	}

	#[inline]
	fn collect_str<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: Display + ?Sized,
	{
		self.0.collect_str(value)
	}

	#[inline]
	fn is_human_readable(&self) -> bool {
		self.0.is_human_readable()
	}
}
