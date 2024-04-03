use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Table;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Table>;
	type Error = Error;

	type SerializeSeq = SerializeTableVec;
	type SerializeTuple = Impossible<Vec<Table>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Table>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Table>, Error>;
	type SerializeMap = Impossible<Vec<Table>, Error>;
	type SerializeStruct = Impossible<Vec<Table>, Error>;
	type SerializeStructVariant = Impossible<Vec<Table>, Error>;

	const EXPECTED: &'static str = "a `Table` sequence";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeTableVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeTableVec(Vec<Table>);

impl serde::ser::SerializeSeq for SerializeTableVec {
	type Ok = Vec<Table>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Table(value.serialize(ser::string::Serializer.wrap())?));
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
		let vec: Vec<Table> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Table("foo".to_owned())];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
