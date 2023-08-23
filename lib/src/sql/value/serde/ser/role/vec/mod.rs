use crate::err::Error;
use crate::iam::Role;
use crate::sql::value::serde::ser;
use crate::sql::Fetch;
use crate::sql::Idiom;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Role>;
	type Error = Error;

	type SerializeSeq = SerializeRoleVec;
	type SerializeTuple = Impossible<Vec<Role>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Role>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Role>, Error>;
	type SerializeMap = Impossible<Vec<Role>, Error>;
	type SerializeStruct = Impossible<Vec<Role>, Error>;
	type SerializeStructVariant = Impossible<Vec<Role>, Error>;

	const EXPECTED: &'static str = "a `Vec<Role>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeRoleVec(Vec::with_capacity(len.unwrap_or_default())))
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

pub struct SerializeRoleVec(Vec<Role>);

impl serde::ser::SerializeSeq for SerializeRoleVec {
	type Ok = Vec<Role>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::role::Serializer.wrap())?);
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
