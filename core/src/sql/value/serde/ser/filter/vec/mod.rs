pub mod opt;

use crate::err::Error;
use crate::sql::filter::Filter;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Filter>;
	type Error = Error;

	type SerializeSeq = SerializeFilterVec;
	type SerializeTuple = Impossible<Vec<Filter>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Filter>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Filter>, Error>;
	type SerializeMap = Impossible<Vec<Filter>, Error>;
	type SerializeStruct = Impossible<Vec<Filter>, Error>;
	type SerializeStructVariant = Impossible<Vec<Filter>, Error>;

	const EXPECTED: &'static str = "a `Vec<Filter>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeFilterVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeFilterVec(Vec<Filter>);

impl serde::ser::SerializeSeq for SerializeFilterVec {
	type Ok = Vec<Filter>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(super::Serializer.wrap())?);
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
		let vec: Vec<Filter> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Filter::Ascii];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
