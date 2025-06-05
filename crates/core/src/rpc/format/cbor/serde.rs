use crate::sql::SqlValue;

use super::Encode;
use super::err::Error;
use super::writer::Writer;
use serde::ser::{self, Serialize, Serializer};

impl Serializer for &mut Writer {
	type Ok = ();
	type Error = Error;

	type SerializeSeq = Self;
	type SerializeTuple = Self;
	type SerializeTupleStruct = Self;
	type SerializeTupleVariant = Self;
	type SerializeMap = Self;
	type SerializeStruct = Self;
	type SerializeStructVariant = Self;

	fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
		(v as i64).encode(self)
	}

	fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
		(v as i64).encode(self)
	}

	fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
		(v as i64).encode(self)
	}

	fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
		(v as u64).encode(self)
	}

	fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
		(v as u64).encode(self)
	}

	fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
		(v as u64).encode(self)
	}

	fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
		v.to_string().encode(self)
	}

	fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
		v.encode(self)
	}

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		SqlValue::None.encode(self)
	}

	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self)
	}

	fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
		SqlValue::Null.encode(self)
	}

	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
		SqlValue::Null.encode(self)
	}

	fn serialize_unit_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Self::Error> {
		variant.encode(self)
	}

	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self)
	}

	fn serialize_newtype_variant<T>(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		self.write_major(5, 1); // map with one entry
		variant.encode(self)?;
		value.serialize(self)
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
		self.write_major(4, len.unwrap_or(0) as u64);
		Ok(self)
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		self.write_major(4, len as u64);
		Ok(self)
	}

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleStruct, Self::Error> {
		self.write_major(4, len as u64);
		Ok(self)
	}

	fn serialize_tuple_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		self.write_major(5, 1); // map with one entry
		variant.encode(self)?;
		self.write_major(4, len as u64);
		Ok(self)
	}

	fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		self.write_major(5, len.unwrap_or(0) as u64);
		Ok(self)
	}

	fn serialize_struct(
		self,
		_name: &'static str,
		len: usize,
	) -> Result<Self::SerializeStruct, Self::Error> {
		self.write_major(5, len as u64);
		Ok(self)
	}

	fn serialize_struct_variant(
		self,
		_name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		self.write_major(5, 1); // map with one entry
		variant.encode(self)?;
		self.write_major(5, len as u64);
		Ok(self)
	}
}

impl ser::SerializeSeq for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeTuple for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeTupleStruct for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeTupleVariant for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeMap for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		key.serialize(&mut **self)
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeStruct for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		key.encode(self)?;
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}

impl ser::SerializeStructVariant for &mut Writer {
	type Ok = ();
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: ?Sized + Serialize,
	{
		key.encode(self)?;
		value.serialize(&mut **self)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(())
	}
}
