use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Geometry;
use ser::Serializer as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Vec<Geometry>;
	type Error = Error;

	type SerializeSeq = SerializeGeometryVec;
	type SerializeTuple = Impossible<Vec<Geometry>, Error>;
	type SerializeTupleStruct = Impossible<Vec<Geometry>, Error>;
	type SerializeTupleVariant = Impossible<Vec<Geometry>, Error>;
	type SerializeMap = Impossible<Vec<Geometry>, Error>;
	type SerializeStruct = Impossible<Vec<Geometry>, Error>;
	type SerializeStructVariant = Impossible<Vec<Geometry>, Error>;

	const EXPECTED: &'static str = "a `Vec<Geometry>`";

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		Ok(SerializeGeometryVec(Vec::with_capacity(len.unwrap_or_default())))
	}
}

#[non_exhaustive]
pub struct SerializeGeometryVec(pub(super) Vec<Geometry>);

impl serde::ser::SerializeSeq for SerializeGeometryVec {
	type Ok = Vec<Geometry>;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.push(value.serialize(ser::geometry::Serializer.wrap())?);
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
		let vec: Vec<Geometry> = Vec::new();
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}

	#[test]
	fn vec() {
		let vec = vec![Geometry::Point(Default::default())];
		let serialized = vec.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vec, serialized);
	}
}
