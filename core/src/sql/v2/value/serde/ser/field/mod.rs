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
	type SerializeTupleVariant = Impossible<Field, Error>;
	type SerializeMap = Impossible<Field, Error>;
	type SerializeStruct = Impossible<Field, Error>;
	type SerializeStructVariant = SerializeValueIdiomTuple;

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

	fn serialize_struct_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		match variant {
			"Single" => Ok(SerializeValueIdiomTuple::default()),
			variant => Err(Error::custom(format!("unexpected struct variant `{name}::{variant}`"))),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeValueIdiomTuple {
	value: Option<Value>,
	idiom: Option<Option<Idiom>>,
}

impl serde::ser::SerializeStructVariant for SerializeValueIdiomTuple {
	type Ok = Field;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match key {
			"expr" => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			"alias" => {
				self.idiom = Some(value.serialize(SerializeOptionIdiom.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected `Field::Single` field `{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.value, self.idiom) {
			(Some(expr), Some(alias)) => Ok(Field::Single {
				expr,
				alias,
			}),
			_ => Err(Error::custom("`Field::Single` missing required value(s)")),
		}
	}
}

#[derive(Default)]
struct SerializeOptionIdiom;

impl ser::Serializer for SerializeOptionIdiom {
	type Ok = Option<Idiom>;
	type Error = Error;

	type SerializeSeq = Impossible<Self::Ok, Error>;
	type SerializeTuple = Impossible<Self::Ok, Error>;
	type SerializeTupleStruct = Impossible<Self::Ok, Error>;
	type SerializeTupleVariant = Impossible<Self::Ok, Error>;
	type SerializeMap = Impossible<Self::Ok, Error>;
	type SerializeStruct = Impossible<Self::Ok, Error>;
	type SerializeStructVariant = Impossible<Self::Ok, Error>;

	const EXPECTED: &'static str = "an enum `Option<Idiom>`";

	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		let idiom = Idiom(value.serialize(ser::part::vec::Serializer.wrap())?);
		Ok(Some(idiom))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use serde::Serialize;

	#[test]
	fn all() {
		let field = Field::All;
		let serialized = field.serialize(Serializer.wrap()).unwrap();
		assert_eq!(field, serialized);
	}

	#[test]
	fn alone() {
		let field = Field::Single {
			expr: Default::default(),
			alias: None,
		};
		let serialized = field.serialize(Serializer.wrap()).unwrap();
		assert_eq!(field, serialized);
	}

	#[test]
	fn alias() {
		let field = Field::Single {
			expr: Default::default(),
			alias: Some(Default::default()),
		};
		let serialized = field.serialize(Serializer.wrap()).unwrap();
		assert_eq!(field, serialized);
	}
}
