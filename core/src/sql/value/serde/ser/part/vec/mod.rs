pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Part;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Part>;
	type Error = Error;

	type SerializeSeq = SerializePartVec;
	type SerializeTuple = Impossible<Vec<Part>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Part>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Part>, Error>;
	type SerializeMap = Impossible<Vec<Part>, Error>;
	type SerializeStruct = Impossible<Vec<Part>, Error>;
	type SerializeStructVariant = Impossible<Vec<Part>, Error>;

	const EXPECTED: &'static str = "a `Vec<Part>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializePartVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializePartVec(Vec<Part>);

impl serde::ser::SerializeSeq for SerializePartVec {
	type Ok = Vec<Part>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::part::Serializer.wrap())?);
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
		let vec: Vec<Part> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Part::All];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
