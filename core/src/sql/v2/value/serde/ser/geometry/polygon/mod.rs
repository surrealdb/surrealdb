pub mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use geo::LineString;
use geo::Polygon;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Polygon<f64>;
	type Error = Error;

	type SerializeSeq = Impossible<Polygon<f64>, Error>;
	type SerializeTuple = Impossible<Polygon<f64>, Error>;
	type SerializeTupleStruct = Impossible<Polygon<f64>, Error>;
	type SerializeTupleVariant = Impossible<Polygon<f64>, Error>;
	type SerializeMap = Impossible<Polygon<f64>, Error>;
	type SerializeStruct = SerializePolygon;
	type SerializeStructVariant = Impossible<Polygon<f64>, Error>;

	const EXPECTED: &'static str = "a struct `Polygon<f64>`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializePolygon::default())
	}
}

#[derive(Default)]
pub(super) struct SerializePolygon {
	exterior: Option<LineString<f64>>,
	interiors: Option<Vec<LineString<f64>>>,
}

impl serde::ser::SerializeStruct for SerializePolygon {
	type Ok = Polygon<f64>;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"exterior" => {
				self.exterior = Some(LineString(
					value.serialize(ser::geometry::coord::vec::Serializer.wrap())?,
				));
			}
			"interiors" => {
				self.interiors =
					Some(value.serialize(ser::geometry::line_string::vec::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Polygon::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.exterior, self.interiors) {
			(Some(exterior), Some(interiors)) => Ok(Polygon::new(exterior, interiors)),
			_ => Err(Error::custom("`Polygon` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let polygon = Polygon::new(LineString(Vec::new()), Vec::new());
		let serialized = polygon.serialize(Serializer.wrap()).unwrap();
		assert_eq!(polygon, serialized);
	}
}
