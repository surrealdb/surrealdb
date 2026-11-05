//! `StoredField` for the types a schema is built from.
//!
//! Every scalar is read through the strict accessors of the stored decoder, which apply exactly the
//! generic decoder's payload checks: a malformed item such as a small integer with a payload is
//! refused rather than read as the number its tag names. Scalars that the generic grammar holds
//! (`Value`, record ids, objects) are read by the generic decoder, with its refusals.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use surrealdb_kvs::release::Release;

use super::{Floor, SchemaError, StoredField, TypeSchema, rt, vm};
use crate::expr::arrival::{object_arrival, record_id_arrival, value_arrival};
use crate::val::stored::decode::strict;
use crate::val::stored::{Encoder, Error, Item, decode};
use crate::val::{
	BoundedDuration, Bytes, Date, Datetime, Duration, Interval, Number, RecordId, Strand,
	TableName, Time, Timestamp, Uuid, Value,
};

/// The tag and payload of an item.
fn parts(item: Item<'_>) -> Result<(u8, &[u8]), SchemaError> {
	item.bytes().split_first().map(|(&tag, payload)| (tag, payload)).ok_or(Error::Truncated.into())
}

/// A scalar read by a strict accessor, or [`SchemaError::wrong_kind`] when the item is another
/// kind.
fn scalar<'a, T>(
	item: Item<'a>,
	read: fn(u8, &'a [u8]) -> Result<Option<T>, Error>,
) -> Result<T, SchemaError> {
	let (tag, payload) = parts(item)?;
	read(tag, payload)?.ok_or(SchemaError::wrong_kind())
}

macro_rules! small_int {
	($t:ty, $name:literal) => {
		impl StoredField for $t {
			fn encode_item(
				&self,
				enc: &mut Encoder,
				_prior: Option<Item<'_>>,
			) -> Result<(), SchemaError> {
				enc.int(i64::from(*self));
				Ok(())
			}

			fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
				<$t>::try_from(scalar(item, strict::int)?).map_err(|_| SchemaError::out_of_range())
			}

			fn into_vm_value(self) -> Result<Value, SchemaError> {
				Ok(Value::Number(Number::Int(i64::from(self))))
			}

			fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
				<$t>::try_from(vm::integer(&value)?)
					.map_err(|_| SchemaError::vm($name, "an integer out of range"))
			}

			fn write_type_name(out: &mut String) {
				out.push_str($name);
			}
		}
	};
}

small_int!(i8, "int:i8");
small_int!(i16, "int:i16");
small_int!(u16, "int:u16");
small_int!(i32, "int:i32");
small_int!(u32, "int:u32");

impl StoredField for bool {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.bool(*self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::bool)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Bool(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::Bool(b) = value {
			Ok(b)
		} else {
			Err(SchemaError::vm("bool", "expected a bool"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("bool");
	}
}

/// A `u8` is an integer; a list of them is BYTES.
impl StoredField for u8 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.int(i64::from(*self));
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		u8::try_from(scalar(item, strict::int)?).map_err(|_| SchemaError::out_of_range())
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(Number::Int(i64::from(self))))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		u8::try_from(vm::integer(&value)?)
			.map_err(|_| SchemaError::vm("int:u8", "an integer out of range"))
	}

	fn write_type_name(out: &mut String) {
		out.push_str("int:u8");
	}

	fn write_list_type_name(out: &mut String) {
		out.push_str("bytes");
	}

	fn encode_list(
		items: &[Self],
		enc: &mut Encoder,
		_prior: Option<Item<'_>>,
	) -> Result<(), SchemaError> {
		enc.bytes(items);
		Ok(())
	}

	fn decode_list(item: Item<'_>, _depth: u32) -> Result<Vec<Self>, SchemaError> {
		let bytes = item.value()?.as_bytes().ok_or(SchemaError::wrong_kind())?;
		Ok(bytes.to_vec())
	}

	fn into_vm_list(items: Vec<Self>) -> Result<Value, SchemaError> {
		Ok(Value::Bytes(Bytes(items.into())))
	}

	fn from_vm_list(value: Value) -> Result<Vec<Self>, SchemaError> {
		if let Value::Bytes(bytes) = value {
			Ok(bytes.0.to_vec())
		} else {
			Err(SchemaError::vm("bytes", "expected bytes"))
		}
	}
}

impl StoredField for i64 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.int(*self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::int)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(Number::Int(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		i64::try_from(vm::integer(&value)?)
			.map_err(|_| SchemaError::vm("int:i64", "an integer out of range"))
	}

	fn write_type_name(out: &mut String) {
		out.push_str("int:i64");
	}
}

/// An INT up to `i64::MAX`, else an integral DECIMAL of scale 0.
impl StoredField for u64 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		match i64::try_from(*self) {
			Ok(v) => enc.int(v),
			Err(_) => enc.decimal(&Decimal::from(*self)),
		}
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		let (tag, payload) = parts(item)?;
		if let Some(v) = strict::int(tag, payload)? {
			return u64::try_from(v).map_err(|_| SchemaError::out_of_range());
		}
		match strict::decimal(tag, payload)? {
			Some(d) if d.fract().is_zero() => d.to_u64().ok_or(SchemaError::out_of_range()),
			Some(_) => Err(SchemaError::out_of_range()),
			None => Err(SchemaError::wrong_kind()),
		}
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		vm::int(i128::from(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		u64::try_from(vm::integer(&value)?)
			.map_err(|_| SchemaError::vm("int:u64", "an integer out of range"))
	}

	fn write_type_name(out: &mut String) {
		out.push_str("int:u64");
	}
}

/// INT where the value fits `i64`, and DECIMAL above that: the decimal's
/// 96-bit mantissa holds every value a 10-byte versionstamp takes, and a
/// value past it is refused rather than truncated.
impl StoredField for u128 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		match i64::try_from(*self) {
			Ok(v) => enc.int(v),
			Err(_) => {
				let v = i128::try_from(*self).map_err(|_| SchemaError::out_of_range())?;
				let d = Decimal::try_from_i128_with_scale(v, 0)
					.map_err(|_| SchemaError::out_of_range())?;
				enc.decimal(&d);
			}
		}
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		let (tag, payload) = parts(item)?;
		if let Some(v) = strict::int(tag, payload)? {
			return u128::try_from(v).map_err(|_| SchemaError::out_of_range());
		}
		match strict::decimal(tag, payload)? {
			Some(d) if d.fract().is_zero() => {
				let d = d.normalize();
				if d.scale() != 0 {
					return Err(SchemaError::out_of_range());
				}
				u128::try_from(d.mantissa()).map_err(|_| SchemaError::out_of_range())
			}
			Some(_) => Err(SchemaError::out_of_range()),
			None => Err(SchemaError::wrong_kind()),
		}
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		vm::int(i128::try_from(self).map_err(|_| SchemaError::out_of_range())?)
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		u128::try_from(vm::integer(&value)?)
			.map_err(|_| SchemaError::vm("int:u128", "an integer out of range"))
	}

	fn write_type_name(out: &mut String) {
		out.push_str("int:u128");
	}
}

/// FLOAT in its 4-byte form holding the `f32`'s own bits.
impl StoredField for f32 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.f32_bits(*self);
		Ok(())
	}

	/// Reads either width of FLOAT; the 8-byte form must hold a value an `f32` holds exactly.
	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		let wide = scalar(item, strict::float)?;
		let narrow = wide as f32;
		if wide.is_nan() || f64::from(narrow) == wide {
			Ok(narrow)
		} else {
			Err(SchemaError::out_of_range())
		}
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(Number::Float(f64::from(self))))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		Ok(vm::float(&value)? as f32)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("float:f32");
	}
}

impl StoredField for f64 {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.float(*self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::float)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(Number::Float(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::float(&value)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("float:f64");
	}
}

impl StoredField for String {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.string(self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::string).map(str::to_owned)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::String(Strand::from(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::string_of(value, "string").map(Strand::into_string)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("string");
	}
}

impl StoredField for Strand {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.string(self.as_str());
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::string).map(Strand::from)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::String(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::string_of(value, "string")
	}

	fn write_type_name(out: &mut String) {
		out.push_str("string");
	}
}

impl StoredField for TableName {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.string(self.as_str());
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::string).map(TableName::new)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::String(Strand::from(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::string_of(value, "string").map(TableName::from)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("string");
	}
}

impl StoredField for uuid::Uuid {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.uuid(self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::uuid)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Uuid(Uuid(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::Uuid(u) = value {
			Ok(u.0)
		} else {
			Err(SchemaError::vm("uuid", "expected a uuid"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("uuid");
	}
}

impl StoredField for Uuid {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		self.0.encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		uuid::Uuid::decode_item(item, depth).map(Uuid)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Uuid(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		uuid::Uuid::from_vm_value(value).map(Uuid)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("uuid");
	}
}

impl StoredField for DateTime<Utc> {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.datetime(self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::datetime)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Datetime(Datetime(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::Datetime(d) = value {
			Ok(d.0)
		} else {
			Err(SchemaError::vm("datetime", "expected a datetime"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("datetime");
	}
}

impl StoredField for Datetime {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		self.0.encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		DateTime::<Utc>::decode_item(item, depth).map(Datetime)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Datetime(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		DateTime::<Utc>::from_vm_value(value).map(Datetime)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("datetime");
	}
}

macro_rules! stored_temporal {
	($ty:ty, $variant:ident, $encode:ident, $strict:ident, $name:literal, $expected:literal) => {
		impl StoredField for $ty {
			fn encode_item(
				&self,
				enc: &mut Encoder,
				_prior: Option<Item<'_>>,
			) -> Result<(), SchemaError> {
				enc.$encode(self);
				Ok(())
			}

			fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
				scalar(item, strict::$strict)
			}

			fn into_vm_value(self) -> Result<Value, SchemaError> {
				Ok(Value::$variant(self))
			}

			fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
				if let Value::$variant(v) = value {
					Ok(v)
				} else {
					Err(SchemaError::vm($name, $expected))
				}
			}

			fn write_type_name(out: &mut String) {
				out.push_str($name);
			}
		}
	};
}

stored_temporal!(Date, Date, date, date, "date", "expected a date");
stored_temporal!(Time, Time, time, time, "time", "expected a time");
stored_temporal!(Timestamp, Timestamp, timestamp, timestamp, "timestamp", "expected a timestamp");
stored_temporal!(Interval, Interval, interval, interval, "interval", "expected an interval");

impl StoredField for std::time::Duration {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.duration(*self);
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		scalar(item, strict::duration)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Duration(Duration(self)))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::Duration(d) = value {
			Ok(d.0)
		} else {
			Err(SchemaError::vm("duration", "expected a duration"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("duration");
	}
}

impl StoredField for Duration {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		self.0.encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		std::time::Duration::decode_item(item, depth).map(Duration)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Duration(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		std::time::Duration::from_vm_value(value).map(Duration)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("duration");
	}
}

impl StoredField for BoundedDuration {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		self.0.encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		std::time::Duration::decode_item(item, depth).map(BoundedDuration)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		self.0.into_vm_value()
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		std::time::Duration::from_vm_value(value).map(BoundedDuration)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("duration");
	}
}

/// Any number representation: INT, FLOAT, DECIMAL, BIGDECIMAL, or a sized number.
impl StoredField for Number {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.number(self)?;
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		let (tag, payload) = parts(item)?;
		if let Some(v) = strict::int(tag, payload)? {
			return Ok(Number::Int(v));
		}
		if let Some(v) = strict::float(tag, payload)? {
			return Ok(Number::Float(v));
		}
		if let Some(v) = strict::decimal(tag, payload)? {
			return Ok(Number::Decimal(v));
		}
		if let Some(v) = strict::bigdecimal(tag, payload)? {
			return Ok(Number::BigDecimal(Box::new(v)));
		}
		if let Some(v) = strict::i8(tag, payload)? {
			return Ok(Number::I8(v));
		}
		if let Some(v) = strict::u8(tag, payload)? {
			return Ok(Number::U8(v));
		}
		if let Some(v) = strict::i16(tag, payload)? {
			return Ok(Number::I16(v));
		}
		if let Some(v) = strict::u16(tag, payload)? {
			return Ok(Number::U16(v));
		}
		if let Some(v) = strict::i32(tag, payload)? {
			return Ok(Number::I32(v));
		}
		if let Some(v) = strict::u32(tag, payload)? {
			return Ok(Number::U32(v));
		}
		if let Some(v) = strict::u64(tag, payload)? {
			return Ok(Number::U64(v));
		}
		if let Some(v) = strict::f16(tag, payload)? {
			return Ok(Number::F16(v));
		}
		if let Some(v) = strict::f32(tag, payload)? {
			return Ok(Number::F32(v));
		}
		Err(SchemaError::wrong_kind())
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::Number(n) = value {
			Ok(n)
		} else {
			Err(SchemaError::vm("number", "expected a number"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("number");
	}
}

/// Any value of the generic grammar. A list of values is the generic canonical array.
impl StoredField for Value {
	const NULLABLE: bool = true;

	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		Ok(enc.value(self)?)
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		Ok(decode(item.bytes())?)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		acc.raise(value_arrival(self));
		Ok(())
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(self)
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		Ok(value)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("value");
	}

	fn encode_list(
		items: &[Self],
		enc: &mut Encoder,
		_prior: Option<Item<'_>>,
	) -> Result<(), SchemaError> {
		Ok(enc.values(items)?)
	}

	fn decode_list(item: Item<'_>, _depth: u32) -> Result<Vec<Self>, SchemaError> {
		if let Value::Array(array) = decode(item.bytes())? {
			Ok(array.0)
		} else {
			Err(SchemaError::wrong_kind())
		}
	}
}

/// A release as the INT of `Release::to_u32`; never below the first release with a stored form.
impl StoredField for Release {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		enc.int(i64::from(self.to_u32()));
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		let v =
			u32::try_from(scalar(item, strict::int)?).map_err(|_| SchemaError::out_of_range())?;
		let release = Release::from_u32(v);
		if release < Release::FIRST_STORED {
			return Err(SchemaError::out_of_range());
		}
		Ok(release)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::Number(Number::Int(i64::from(self.to_u32()))))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		let v = u32::try_from(vm::integer(&value)?)
			.map_err(|_| SchemaError::vm("release", "an integer out of range"))?;
		let release = Release::from_u32(v);
		if release < Release::FIRST_STORED {
			return Err(SchemaError::vm("release", "a release below the first stored one"));
		}
		Ok(release)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("release");
	}
}

impl StoredField for RecordId {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		Ok(enc.record_id(self)?)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		acc.raise(record_id_arrival(self));
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		if let Value::RecordId(r) = decode(item.bytes())? {
			Ok(r)
		} else {
			Err(SchemaError::wrong_kind())
		}
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(Value::RecordId(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		if let Value::RecordId(r) = value {
			Ok(r)
		} else {
			Err(SchemaError::vm("record", "expected a record id"))
		}
	}

	fn write_type_name(out: &mut String) {
		out.push_str("record");
	}
}

/// A generic OBJECT: keys unique and ascending bytewise, which a `BTreeMap` of strings iterates.
impl StoredField for BTreeMap<String, Value> {
	fn encode_item(&self, enc: &mut Encoder, _prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		Ok(enc.object_entries(self.iter().rev().map(|(k, v)| (k.as_str(), v)), self.len())?)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		acc.raise(object_arrival(self.values()));
		Ok(())
	}

	fn decode_item(item: Item<'_>, _depth: u32) -> Result<Self, SchemaError> {
		if let Value::Object(object) = decode(item.bytes())? {
			Ok(object.0.into_iter().map(|(k, v)| (k.into_string(), v)).collect())
		} else {
			Err(SchemaError::wrong_kind())
		}
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		Ok(vm::pairs_into(self))
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::pairs_from(value)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("object<value>");
	}
}

impl<T: StoredField> StoredField for Box<T> {
	const NULLABLE: bool = T::NULLABLE;

	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		(**self).encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		T::decode_item(item, depth).map(Box::new)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		(**self).floor(acc)
	}

	fn unrevisioned_floor(&self) -> Option<Release> {
		(**self).unrevisioned_floor()
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		(*self).into_vm_value()
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		T::from_vm_value(value).map(Box::new)
	}

	fn write_type_name(out: &mut String) {
		T::write_type_name(out);
	}

	fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
		T::collect_schemas(out);
	}
}

/// In item position (a list element, a tuple element, a variant payload) an `Option` is a map
/// holding entry 2 for `Some` and no entry for `None`. A field of a derived type that is spelled
/// `Option` is absent for `None` instead and never reaches this form.
impl<T: StoredField> StoredField for Option<T> {
	const NULLABLE: bool = true;

	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		let mut w = rt::MapWriter::new(enc, prior, "option", |n| n == 2)?;
		match self {
			Some(inner) => w.put(inner, 2)?,
			None => w.skip(2)?,
		}
		w.finish()
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		let map = rt::map(item, depth, "option")?;
		let mut prev = 0;
		let mut some = None;
		for i in 0..map.len() {
			let (n, entry) = rt::entry(&map, i, &mut prev, "option")?;
			match n {
				2 => some = Some(rt::field::<T>(entry, depth, "option", 2)?),
				_ => rt::unknown(n, "option", &[])?,
			}
		}
		Ok(some)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		match self {
			Some(inner) => inner.floor(acc),
			None => Ok(()),
		}
	}

	fn unrevisioned_floor(&self) -> Option<Release> {
		self.as_ref().and_then(T::unrevisioned_floor)
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		vm::option_into(self)
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		vm::option_from(value)
	}

	fn write_type_name(out: &mut String) {
		out.push_str("option<");
		T::write_type_name(out);
		out.push('>');
	}

	fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
		T::collect_schemas(out);
	}
}

impl<T: StoredField> StoredField for Vec<T> {
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		T::encode_list(self, enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		T::decode_list(item, depth)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		self.iter().try_for_each(|element| element.floor(acc))
	}

	fn unrevisioned_floor(&self) -> Option<Release> {
		self.iter().fold(None, |acc, e| rt::max_release(acc, e.unrevisioned_floor()))
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		T::into_vm_list(self)
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		T::from_vm_list(value)
	}

	fn write_type_name(out: &mut String) {
		T::write_list_type_name(out);
	}

	fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
		T::collect_schemas(out);
	}
}

/// A tuple is a positional map: element `i` is entry `2 * (i + 1)`. The elements are listed in
/// order, then again highest first, the order the encoder writes them in.
macro_rules! tuple {
	(
		$len:literal;
		$(($t:ident $v:ident $i:tt $n:literal)),+;
		put $(($pi:tt $pn:literal)),+
	) => {
		impl<$($t: StoredField),+> StoredField for ($($t,)+) {
			fn encode_item(
				&self,
				enc: &mut Encoder,
				prior: Option<Item<'_>>,
			) -> Result<(), SchemaError> {
				let mut w =
					rt::MapWriter::new(enc, prior, "tuple", |n| matches!(n, $($n)|+))?;
				$(w.put(&self.$pi, $pn)?;)+
				w.finish()
			}

			fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
				let map = rt::map(item, depth, "tuple")?;
				let mut prev = 0;
				$(let mut $v: Option<$t> = None;)+
				for i in 0..map.len() {
					let (n, entry) = rt::entry(&map, i, &mut prev, "tuple")?;
					match n {
						$($n => $v = Some(rt::field::<$t>(entry, depth, "tuple", $n)?),)+
						_ => rt::unknown(n, "tuple", &[])?,
					}
				}
				Ok(($($v.ok_or(rt::missing("tuple", $n))?,)+))
			}

			fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
				$(self.$i.floor(acc)?;)+
				Ok(())
			}

			fn unrevisioned_floor(&self) -> Option<Release> {
				let mut r = None;
				$(r = rt::max_release(r, self.$i.unrevisioned_floor());)+
				r
			}

			fn into_vm_value(self) -> Result<Value, SchemaError> {
				Ok(vm::tuple([$(self.$i.into_vm_value()?),+]))
			}

			fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
				let mut items = vm::tuple_of(value, $len, "tuple")?;
				Ok(($($t::from_vm_value(vm::next(&mut items, "tuple")?)?,)+))
			}

			fn write_type_name(out: &mut String) {
				out.push_str("tuple<");
				let mut first = true;
				$(
					if !std::mem::take(&mut first) {
						out.push(',');
					}
					$t::write_type_name(out);
				)+
				out.push('>');
			}

			fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
				$($t::collect_schemas(out);)+
			}
		}
	};
}

tuple!(2; (A a 0 2), (B b 1 4); put (1 4), (0 2));
tuple!(3; (A a 0 2), (B b 1 4), (C c 2 6); put (2 6), (1 4), (0 2));

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::stored::encode::with_encoder;

	fn round_trip(value: u128) -> Result<u128, SchemaError> {
		let bytes = with_encoder(|enc| -> Result<Vec<u8>, SchemaError> {
			value.encode_item(enc, None)?;
			Ok(enc.as_bytes().to_vec())
		})?;
		u128::decode_item(Item::new(&bytes), 0)
	}

	/// A 128-bit unsigned field is INT where it fits `i64` and DECIMAL above
	/// that, up to the decimal's 96-bit mantissa: a versionstamp's 80 bits
	/// fit, and a value past the mantissa is refused rather than truncated.
	#[test]
	fn a_u128_round_trips_up_to_the_decimal_mantissa() {
		for value in [
			0,
			1,
			i64::MAX as u128,
			i64::MAX as u128 + 1,
			u64::MAX as u128,
			1u128 << 79,
			(1u128 << 96) - 1,
		] {
			assert_eq!(round_trip(value).unwrap(), value);
		}
		assert!(round_trip(1u128 << 96).is_err());
		assert!(round_trip(u128::MAX).is_err());
	}
}
