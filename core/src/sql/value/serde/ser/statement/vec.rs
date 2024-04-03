use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Statement;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Statement>;
	type Error = Error;

	type SerializeSeq = SerializeStatementVec;
	type SerializeTuple = Impossible<Vec<Statement>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Statement>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Statement>, Error>;
	type SerializeMap = Impossible<Vec<Statement>, Error>;
	type SerializeStruct = Impossible<Vec<Statement>, Error>;
	type SerializeStructVariant = Impossible<Vec<Statement>, Error>;

	const EXPECTED: &'static str = "a `Vec<Statement>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeStatementVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeStatementVec(Vec<Statement>);

impl serde::ser::SerializeSeq for SerializeStatementVec {
	type Ok = Vec<Statement>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::statement::Serializer.wrap())?);
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::statements::BeginStatement;

	#[test]
	fn empty() {
		let vec: Vec<Statement> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Statement::Begin(BeginStatement)];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
