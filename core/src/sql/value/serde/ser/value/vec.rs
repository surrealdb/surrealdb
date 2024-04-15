use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Value>;
	type Error = Error;

	type SerializeSeq = SerializeValueVec;
	type SerializeTuple = Impossible<Vec<Value>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Value>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Value>, Error>;
	type SerializeMap = Impossible<Vec<Value>, Error>;
	type SerializeStruct = Impossible<Vec<Value>, Error>;
	type SerializeStructVariant = Impossible<Vec<Value>, Error>;

	const EXPECTED: &'static str = "a `Vec<Value>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeValueVec(Vec::with_capacity(len.unwrap_or_default())))
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

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeValueVec(pub Vec<Value>);

impl serde::ser::SerializeSeq for SerializeValueVec {
	type Ok = Vec<Value>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::value::Serializer.wrap())?);
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
		let vec: Vec<Value> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Value::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
