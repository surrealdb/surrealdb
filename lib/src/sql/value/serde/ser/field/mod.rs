pub(super) mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Field;
use crate::sql::Idiom;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Field;
	type Error = Error;

	type SerializeSeq = Impossible<Field, Error>;
	type SerializeTuple = Impossible<Field, Error>;
	type SerializeTupleStruct = Impossible<Field, Error>;
	type SerializeTupleVariant = SerializeValueIdiomTuple;
	type SerializeMap = Impossible<Field, Error>;
	type SerializeStruct = Impossible<Field, Error>;
	type SerializeStructVariant = Impossible<Field, Error>;

	const EXPECTED: &'static str = "an enum `Field`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"All" => Ok(Field::All),
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
			"Alone" => Ok(Field::Alone(value.serialize(ser::value::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		match variant {
			"Alias" => Ok(SerializeValueIdiomTuple::default()),
			variant => Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`"))),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeValueIdiomTuple {
	index: usize,
	value: Option<Value>,
	idiom: Option<Idiom>,
}

impl serde::ser::SerializeTupleVariant for SerializeValueIdiomTuple {
	type Ok = Field;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			1 => {
				self.idiom = Some(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
			}
			index => {
				return Err(Error::custom(format!("unexpected `Field::Alias` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.value, self.idiom) {
			(Some(value), Some(idiom)) => Ok(Field::Alias(value, idiom)),
			_ => Err(Error::custom("`Field::Alias` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn all() {
		let field = Field::All;
		let serialized = serialize_internal(|| field.serialize(Serializer.wrap())).unwrap();
		assert_eq!(field, serialized);
	}

	#[test]
	fn alone() {
		let field = Field::Alone(Default::default());
		let serialized = serialize_internal(|| field.serialize(Serializer.wrap())).unwrap();
		assert_eq!(field, serialized);
	}

	#[test]
	fn alias() {
		let field = Field::Alias(Default::default(), Default::default());
		let serialized = serialize_internal(|| field.serialize(Serializer.wrap())).unwrap();
		assert_eq!(field, serialized);
	}
}
