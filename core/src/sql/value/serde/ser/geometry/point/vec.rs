use crate::err::Error;
use crate::sql::value::serde::ser;
use geo::Point;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Point<f64>>;
	type Error = Error;

	type SerializeSeq = SerializePointVec;
	type SerializeTuple = Impossible<Vec<Point<f64>>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Point<f64>>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Point<f64>>, Error>;
	type SerializeMap = Impossible<Vec<Point<f64>>, Error>;
	type SerializeStruct = Impossible<Vec<Point<f64>>, Error>;
	type SerializeStructVariant = Impossible<Vec<Point<f64>>, Error>;

	const EXPECTED: &'static str = "a `Vec<Point<f64>>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializePointVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializePointVec(Vec<Point<f64>>);

impl serde::ser::SerializeSeq for SerializePointVec {
	type Ok = Vec<Point<f64>>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(Point(value.serialize(ser::geometry::coord::Serializer.wrap())?));
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
		let vec: Vec<Point<f64>> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Point::default()];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
