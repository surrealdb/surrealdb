pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::value::serde::ser::fetch::FetchSerializer;
use crate::sql::Fetch;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Fetch>;
	type Error = Error;

	type SerializeSeq = SerializeFetchVec;
	type SerializeTuple = Impossible<Vec<Fetch>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Fetch>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Fetch>, Error>;
	type SerializeMap = Impossible<Vec<Fetch>, Error>;
	type SerializeStruct = Impossible<Vec<Fetch>, Error>;
	type SerializeStructVariant = Impossible<Vec<Fetch>, Error>;

	const EXPECTED: &'static str = "a `Vec<Fetch>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeFetchVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeFetchVec(Vec<Fetch>);

impl serde::ser::SerializeSeq for SerializeFetchVec {
	type Ok = Vec<Fetch>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(FetchSerializer.wrap())?);
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
		let vec: Vec<Fetch> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Fetch::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
