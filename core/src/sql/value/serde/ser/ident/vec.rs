use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Ident>;
	type Error = Error;

	type SerializeSeq = SerializeIdentVec;
	type SerializeTuple = Impossible<Vec<Ident>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Ident>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Ident>, Error>;
	type SerializeMap = Impossible<Vec<Ident>, Error>;
	type SerializeStruct = Impossible<Vec<Ident>, Error>;
	type SerializeStructVariant = Impossible<Vec<Ident>, Error>;

	const EXPECTED: &'static str = "a `Vec<Ident>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeIdentVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

#[non_exhaustive]
pub struct SerializeIdentVec(Vec<Ident>);

impl serde::ser::SerializeSeq for SerializeIdentVec {
	type Ok = Vec<Ident>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Ident(value.serialize(ser::string::Serializer.wrap())?));
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
		let vec: Vec<Ident> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Ident::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
