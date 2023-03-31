pub(super) mod coord;
pub(super) mod line_string;
pub(super) mod point;
pub(super) mod polygon;
pub(super) mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Geometry;
use geo::LineString;
use geo::MultiLineString;
use geo::MultiPoint;
use geo::MultiPolygon;
use geo::Point;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Geometry;
	type Error = Error;

	type SerializeSeq = SerializeGeometryVec;
	type SerializeTuple = Impossible<Geometry, Error>;
	type SerializeTupleStruct = Impossible<Geometry, Error>;
	type SerializeTupleVariant = Impossible<Geometry, Error>;
	type SerializeMap = Impossible<Geometry, Error>;
	type SerializeStruct = Impossible<Geometry, Error>;
	type SerializeStructVariant = Impossible<Geometry, Error>;

	const EXPECTED: &'static str = "an enum `Geometry`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Point" => Ok(Geometry::Point(Point(
				value.serialize(ser::geometry::coord::Serializer.wrap())?,
			))),
			"Line" => Ok(Geometry::Line(LineString(
				value.serialize(ser::geometry::coord::vec::Serializer.wrap())?,
			))),
			"Polygon" => {
				Ok(Geometry::Polygon(value.serialize(ser::geometry::polygon::Serializer.wrap())?))
			}
			"MultiPoint" => Ok(Geometry::MultiPoint(MultiPoint(
				value.serialize(ser::geometry::point::vec::Serializer.wrap())?,
			))),
			"MultiLine" => Ok(Geometry::MultiLine(MultiLineString(
				value.serialize(ser::geometry::line_string::vec::Serializer.wrap())?,
			))),
			"MultiPolygon" => Ok(Geometry::MultiPolygon(MultiPolygon(
				value.serialize(ser::geometry::polygon::vec::Serializer.wrap())?,
			))),
			"Collection" => {
				Ok(Geometry::Collection(value.serialize(ser::geometry::vec::Serializer.wrap())?))
			}
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		let serialize_seq = vec::SerializeGeometryVec(Vec::with_capacity(len.unwrap_or_default()));
		Ok(SerializeGeometryVec(serialize_seq))
	}
}

pub(super) struct SerializeGeometryVec(vec::SerializeGeometryVec);

impl serde::ser::SerializeSeq for SerializeGeometryVec {
	type Ok = Geometry;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.0.serialize_element(value)
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Geometry::Collection(self.0.end()?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use geo::Coord;
	use geo::Polygon;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn point() {
		let geometry = Geometry::Point(Default::default());
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn line() {
		let geometry = Geometry::Line(LineString(vec![Coord::default()]));
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn polygon() {
		let polygon = Polygon::new(LineString(Vec::new()), Vec::new());
		let geometry = Geometry::Polygon(polygon);
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn multi_point() {
		let geometry = Geometry::MultiPoint(vec![(0., 0.), (1., 2.)].into());
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn multi_line() {
		let geometry = Geometry::MultiLine(MultiLineString::new(Vec::new()));
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn multi_polygon() {
		let geometry = Geometry::MultiPolygon(MultiPolygon::new(Vec::new()));
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}

	#[test]
	fn collection() {
		let geometry = Geometry::Collection(vec![Geometry::Point(Default::default())]);
		let serialized = serialize_internal(|| geometry.serialize(Serializer.wrap())).unwrap();
		assert_eq!(geometry, serialized);
	}
}
