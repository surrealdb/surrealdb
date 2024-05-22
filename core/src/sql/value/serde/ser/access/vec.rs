use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Access;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Access>;
	type Error = Error;

	type SerializeSeq = SerializeAccessVec;
	type SerializeTuple = Impossible<Vec<Access>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Access>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Access>, Error>;
	type SerializeMap = Impossible<Vec<Access>, Error>;
	type SerializeStruct = Impossible<Vec<Access>, Error>;
	type SerializeStructVariant = Impossible<Vec<Access>, Error>;

	const EXPECTED: &'static str = "a `Access` sequence";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeAccessVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeAccessVec(Vec<Access>);

impl serde::ser::SerializeSeq for SerializeAccessVec {
	type Ok = Vec<Access>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Access(value.serialize(ser::string::Serializer.wrap())?));
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
		let vec: Vec<Access> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Access("foo".to_owned())];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
