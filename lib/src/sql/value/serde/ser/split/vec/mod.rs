pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Idiom;
use crate::sql::Split;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Split>;
	type Error = Error;

	type SerializeSeq = SerializeSplitVec;
	type SerializeTuple = Impossible<Vec<Split>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Split>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Split>, Error>;
	type SerializeMap = Impossible<Vec<Split>, Error>;
	type SerializeStruct = Impossible<Vec<Split>, Error>;
	type SerializeStructVariant = Impossible<Vec<Split>, Error>;

	const EXPECTED: &'static str = "a `Vec<Split>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeSplitVec(Vec::with_capacity(len.unwrap_or_default())))
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

pub struct SerializeSplitVec(Vec<Split>);

impl serde::ser::SerializeSeq for SerializeSplitVec {
	type Ok = Vec<Split>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Split(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?)));
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;

	#[test]
	fn empty() {
		let vec: Vec<Split> = Vec::new();
		let serialized = serialize_internal(|| vec.serialize(Serializer.wrap())).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Split::default()];
		let serialized = serialize_internal(|| vec.serialize(Serializer.wrap())).unwrap();
		assert_eq!(vec, serialized);
	}
}
