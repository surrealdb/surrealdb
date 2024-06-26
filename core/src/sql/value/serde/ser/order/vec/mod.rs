pub mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Order;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Order>;
	type Error = Error;

	type SerializeSeq = SerializeOrderVec;
	type SerializeTuple = Impossible<Vec<Order>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Order>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Order>, Error>;
	type SerializeMap = Impossible<Vec<Order>, Error>;
	type SerializeStruct = Impossible<Vec<Order>, Error>;
	type SerializeStructVariant = Impossible<Vec<Order>, Error>;

	const EXPECTED: &'static str = "a `Vec<Order>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeOrderVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializeOrderVec(Vec<Order>);

impl serde::ser::SerializeSeq for SerializeOrderVec {
	type Ok = Vec<Order>;
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
		let vec: Vec<Order> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Order::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
