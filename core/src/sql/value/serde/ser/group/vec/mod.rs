pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Group;
use crate::sql::Idiom;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Group>;
	type Error = Error;

	type SerializeSeq = SerializeGroupVec;
	type SerializeTuple = Impossible<Vec<Group>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Group>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Group>, Error>;
	type SerializeMap = Impossible<Vec<Group>, Error>;
	type SerializeStruct = Impossible<Vec<Group>, Error>;
	type SerializeStructVariant = Impossible<Vec<Group>, Error>;

	const EXPECTED: &'static str = "a `Vec<Group>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeGroupVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeGroupVec(Vec<Group>);

impl serde::ser::SerializeSeq for SerializeGroupVec {
	type Ok = Vec<Group>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Group(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?)));
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
		let vec: Vec<Group> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Group::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
