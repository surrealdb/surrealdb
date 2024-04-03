pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Idiom;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Idiom>;
	type Error = Error;

	type SerializeSeq = SerializeIdiomVec;
	type SerializeTuple = Impossible<Vec<Idiom>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Idiom>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Idiom>, Error>;
	type SerializeMap = Impossible<Vec<Idiom>, Error>;
	type SerializeStruct = Impossible<Vec<Idiom>, Error>;
	type SerializeStructVariant = Impossible<Vec<Idiom>, Error>;

	const EXPECTED: &'static str = "a `Vec<Idiom>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdiomVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeIdiomVec(Vec<Idiom>);

impl serde::ser::SerializeSeq for SerializeIdiomVec {
	type Ok = Vec<Idiom>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
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
		let vec: Vec<Idiom> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Idiom::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
