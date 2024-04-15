pub mod opt;

use crate::err::Error;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Tokenizer>;
	type Error = Error;

	type SerializeSeq = SerializeTokenizerVec;
	type SerializeTuple = Impossible<Vec<Tokenizer>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Tokenizer>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Tokenizer>, Error>;
	type SerializeMap = Impossible<Vec<Tokenizer>, Error>;
	type SerializeStruct = Impossible<Vec<Tokenizer>, Error>;
	type SerializeStructVariant = Impossible<Vec<Tokenizer>, Error>;

	const EXPECTED: &'static str = "a `Vec<Tokenizer>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeTokenizerVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeTokenizerVec(Vec<Tokenizer>);

impl serde::ser::SerializeSeq for SerializeTokenizerVec {
	type Ok = Vec<Tokenizer>;
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
		let vec: Vec<Tokenizer> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Tokenizer::Blank];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
