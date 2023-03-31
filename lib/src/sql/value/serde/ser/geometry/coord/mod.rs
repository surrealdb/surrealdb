pub mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use geo::Coord;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Coord<f64>;
	type Error = Error;

	type SerializeSeq = Impossible<Coord<f64>, Error>;
	type SerializeTuple = Impossible<Coord<f64>, Error>;
	type SerializeTupleStruct = Impossible<Coord<f64>, Error>;
	type SerializeTupleVariant = Impossible<Coord<f64>, Error>;
	type SerializeMap = Impossible<Coord<f64>, Error>;
	type SerializeStruct = SerializeCoord;
	type SerializeStructVariant = Impossible<Coord<f64>, Error>;

	const EXPECTED: &'static str = "a struct `Coord<f64>`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeCoord::default())
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

#[derive(Default)]
pub(super) struct SerializeCoord {
	x: Option<f64>,
	y: Option<f64>,
}

impl serde::ser::SerializeStruct for SerializeCoord {
	type Ok = Coord;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"x" => {
				self.x = Some(value.serialize(ser::primitive::f64::Serializer.wrap())?);
			}
			"y" => {
				self.y = Some(value.serialize(ser::primitive::f64::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Coord::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.x, self.y) {
			(Some(x), Some(y)) => Ok(Coord {
				x,
				y,
			}),
			_ => Err(Error::custom("`Coord` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;

	#[test]
	fn default() {
		let coord = Coord::default();
		let serialized = serialize_internal(|| coord.serialize(Serializer.wrap())).unwrap();
		assert_eq!(coord, serialized);
	}
}
