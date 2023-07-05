use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Field;
use crate::sql::Fields;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Fields;
	type Error = Error;

	type SerializeSeq = Impossible<Fields, Error>;
	type SerializeTuple = Impossible<Fields, Error>;
	type SerializeTupleStruct = SerializeFields;
	type SerializeTupleVariant = Impossible<Fields, Error>;
	type SerializeMap = Impossible<Fields, Error>;
	type SerializeStruct = Impossible<Fields, Error>;
	type SerializeStructVariant = Impossible<Fields, Error>;

	const EXPECTED: &'static str = "a struct `Fields`";

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Error> {
		Ok(SerializeFields::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeFields {
	index: usize,
	fields: Option<Vec<Field>>,
	boolean: Option<bool>,
}

impl serde::ser::SerializeTupleStruct for SerializeFields {
	type Ok = Fields;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.fields = Some(value.serialize(ser::field::vec::Serializer.wrap())?);
			}
			1 => {
				self.boolean = Some(value.serialize(ser::primitive::bool::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!("unexpected `Fields` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.fields, self.boolean) {
			(Some(fields), Some(boolean)) => Ok(Fields(fields, boolean)),
			_ => Err(Error::custom("`Fields` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let fields = Fields::default();
		let serialized = fields.serialize(Serializer.wrap()).unwrap();
		assert_eq!(fields, serialized);
	}

	#[test]
	fn all() {
		let fields = Fields(vec![Field::All], true);
		let serialized = fields.serialize(Serializer.wrap()).unwrap();
		assert_eq!(fields, serialized);
	}
}
