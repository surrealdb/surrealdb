use crate::err::Error;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<String>;
	type Error = Error;

	type SerializeSeq = SerializeStringVec;
	type SerializeTuple = Impossible<Vec<String>, Error>;
	type SerializeTupleStruct = Impossible<Vec<String>, Error>;
	type SerializeTupleVariant = Impossible<Vec<String>, Error>;
	type SerializeMap = Impossible<Vec<String>, Error>;
	type SerializeStruct = Impossible<Vec<String>, Error>;
	type SerializeStructVariant = Impossible<Vec<String>, Error>;

	const EXPECTED: &'static str = "a `Vec<String>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeStringVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeStringVec(Vec<String>);

impl serde::ser::SerializeSeq for SerializeStringVec {
	type Ok = Vec<String>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::string::Serializer.wrap())?);
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
		let vec: Vec<String> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![String::new()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
