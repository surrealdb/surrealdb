use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub mod opt;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Kind>;
	type Error = Error;

	type SerializeSeq = SerializeKindVec;
	type SerializeTuple = Impossible<Vec<Kind>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Kind>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Kind>, Error>;
	type SerializeMap = Impossible<Vec<Kind>, Error>;
	type SerializeStruct = Impossible<Vec<Kind>, Error>;
	type SerializeStructVariant = Impossible<Vec<Kind>, Error>;

	const EXPECTED: &'static str = "a `Vec<Kind>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeKindVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeKindVec(Vec<Kind>);

impl serde::ser::SerializeSeq for SerializeKindVec {
	type Ok = Vec<Kind>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::kind::Serializer.wrap())?);
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
		let vec: Vec<Kind> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Kind::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
