use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Field;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Field>;
	type Error = Error;

	type SerializeSeq = SerializeFieldVec;
	type SerializeTuple = Impossible<Vec<Field>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Field>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Field>, Error>;
	type SerializeMap = Impossible<Vec<Field>, Error>;
	type SerializeStruct = Impossible<Vec<Field>, Error>;
	type SerializeStructVariant = Impossible<Vec<Field>, Error>;

	const EXPECTED: &'static str = "a `Vec<Field>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeFieldVec(Vec::with_capacity(len.unwrap_or_default())))
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}
}

#[non_exhaustive]
pub struct SerializeFieldVec(Vec<Field>);

impl serde::ser::SerializeSeq for SerializeFieldVec {
	type Ok = Vec<Field>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::field::Serializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty() {
		let vec: Vec<Field> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Field::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
