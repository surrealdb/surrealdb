use crate::err::Error;
use crate::sql::block::Entry;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Entry>;
	type Error = Error;

	type SerializeSeq = SerializeEntryVec;
	type SerializeTuple = Impossible<Vec<Entry>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Entry>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Entry>, Error>;
	type SerializeMap = Impossible<Vec<Entry>, Error>;
	type SerializeStruct = Impossible<Vec<Entry>, Error>;
	type SerializeStructVariant = Impossible<Vec<Entry>, Error>;

	const EXPECTED: &'static str = "a `Vec<Entry>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeEntryVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeEntryVec(Vec<Entry>);

impl serde::ser::SerializeSeq for SerializeEntryVec {
	type Ok = Vec<Entry>;
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
		let vec: Vec<Entry> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Entry::Value(Default::default())];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
