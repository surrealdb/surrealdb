use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Number;
use rust_decimal::Decimal;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Number;
	type Error = Error;

	type SerializeSeq = Impossible<Number, Error>;
	type SerializeTuple = Impossible<Number, Error>;
	type SerializeTupleStruct = Impossible<Number, Error>;
	type SerializeTupleVariant = Impossible<Number, Error>;
	type SerializeMap = Impossible<Number, Error>;
	type SerializeStruct = Impossible<Number, Error>;
	type SerializeStructVariant = Impossible<Number, Error>;

	const EXPECTED: &'static str = "an enum `Number`";

	#[inline]
	fn serialize_i8(self, value: i8) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i16(self, value: i16) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i32(self, value: i32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	fn serialize_i128(self, value: i128) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u8(self, value: u8) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u16(self, value: u16) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u32(self, value: u32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u64(self, value: u64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	fn serialize_u128(self, value: u128) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_f32(self, value: f32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_f64(self, value: f64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Error> {
		let decimal = value.parse::<Decimal>().map_err(Error::custom)?;
		Ok(decimal.into())
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
			"Int" => Ok(Number::Int(value.serialize(ser::primitive::i64::Serializer.wrap())?)),
			"Float" => Ok(Number::Float(value.serialize(ser::primitive::f64::Serializer.wrap())?)),
			"Decimal" => Ok(Number::Decimal(value.serialize(ser::decimal::Serializer.wrap())?)),
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
