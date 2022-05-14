use crate::ctx::Context;
use crate::err::Error;
use crate::sql::geometry::Geometry;
use crate::sql::value::Value;
use geo::algorithm::area::Area;
use geo::algorithm::bearing::Bearing;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::haversine_distance::HaversineDistance;

pub fn area(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Geometry(v) => match v {
			Geometry::Point(v) => Ok(v.signed_area().into()),
			Geometry::Line(v) => Ok(v.signed_area().into()),
			Geometry::Polygon(v) => Ok(v.signed_area().into()),
			Geometry::MultiPoint(v) => Ok(v.signed_area().into()),
			Geometry::MultiLine(v) => Ok(v.signed_area().into()),
			Geometry::MultiPolygon(v) => Ok(v.signed_area().into()),
			Geometry::Collection(v) => {
				Ok(v.into_iter().collect::<geo::Geometry<f64>>().signed_area().into())
			}
		},
		_ => Ok(Value::None),
	}
}

pub fn bearing(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Geometry(Geometry::Point(v)) => match args.remove(0) {
			Value::Geometry(Geometry::Point(w)) => Ok(v.bearing(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn centroid(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Geometry(v) => match v {
			Geometry::Point(v) => Ok(v.centroid().into()),
			Geometry::Line(v) => match v.centroid() {
				Some(x) => Ok(x.into()),
				None => Ok(Value::None),
			},
			Geometry::Polygon(v) => match v.centroid() {
				Some(x) => Ok(x.into()),
				None => Ok(Value::None),
			},
			Geometry::MultiPoint(v) => match v.centroid() {
				Some(x) => Ok(x.into()),
				None => Ok(Value::None),
			},
			Geometry::MultiLine(v) => match v.centroid() {
				Some(x) => Ok(x.into()),
				None => Ok(Value::None),
			},
			Geometry::MultiPolygon(v) => match v.centroid() {
				Some(x) => Ok(x.into()),
				None => Ok(Value::None),
			},
			Geometry::Collection(v) => {
				match v.into_iter().collect::<geo::Geometry<f64>>().centroid() {
					Some(x) => Ok(x.into()),
					None => Ok(Value::None),
				}
			}
		},
		_ => Ok(Value::None),
	}
}

pub fn distance(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Geometry(Geometry::Point(v)) => match args.remove(0) {
			Value::Geometry(Geometry::Point(w)) => Ok(v.haversine_distance(&w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub mod hash {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::fnc::util::geo;
	use crate::sql::geometry::Geometry;
	use crate::sql::value::Value;

	pub fn encode(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		match args.len() {
			2 => match args.remove(0) {
				Value::Geometry(Geometry::Point(v)) => match args.remove(0).as_int() {
					l if l > 0 && l <= 12 => Ok(geo::encode(v, l as usize).into()),
					_ => Err(Error::InvalidArguments {
						name: String::from("geo::encode"),
						message: String::from("The second argument must be an integer greater than 0 and less than or equal to 12."),
					}),
				},
				_ => Ok(Value::None),
			},
			1 => match args.remove(0) {
				Value::Geometry(Geometry::Point(v)) => Ok(geo::encode(v, 12).into()),
				_ => Ok(Value::None),
			},
			_ => unreachable!(),
		}
	}

	pub fn decode(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		match args.remove(0) {
			Value::Strand(v) => Ok(geo::decode(v).into()),
			_ => Ok(Value::None),
		}
	}
}
