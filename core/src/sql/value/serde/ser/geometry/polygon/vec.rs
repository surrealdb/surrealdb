use crate::err::Error;
use crate::sql::value::serde::ser;
use geo::Polygon;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Polygon<f64>>;
	type Error = Error;

	type SerializeSeq = SerializePolygonVec;
	type SerializeTuple = Impossible<Vec<Polygon<f64>>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Polygon<f64>>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Polygon<f64>>, Error>;
	type SerializeMap = Impossible<Vec<Polygon<f64>>, Error>;
	type SerializeStruct = Impossible<Vec<Polygon<f64>>, Error>;
	type SerializeStructVariant = Impossible<Vec<Polygon<f64>>, Error>;

	const EXPECTED: &'static str = "a `Vec<Polygon<f64>>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializePolygonVec(Vec::with_capacity(len.unwrap_or_default())))
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
pub struct SerializePolygonVec(Vec<Polygon<f64>>);

impl serde::ser::SerializeSeq for SerializePolygonVec {
	type Ok = Vec<Polygon<f64>>;
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
	use geo::LineString;

	#[test]
	fn empty() {
		let vec: Vec<Polygon<f64>> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Polygon::new(LineString(Vec::new()), Vec::new())];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
