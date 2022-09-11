use crate::err::Error;
use crate::sql::geometry::Geometry;
use crate::sql::value::Value;
use geo::algorithm::area::Area;
use geo::algorithm::bearing::Bearing;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::haversine_distance::HaversineDistance;

pub fn area((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
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

pub fn bearing((a, b): (Value, Value)) -> Result<Value, Error> {
	match a {
		Value::Geometry(Geometry::Point(v)) => match b {
			Value::Geometry(Geometry::Point(w)) => Ok(v.bearing(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn centroid((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
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

pub fn distance((from, to): (Value, Value)) -> Result<Value, Error> {
	match from {
		Value::Geometry(Geometry::Point(v)) => match to {
			Value::Geometry(Geometry::Point(w)) => Ok(v.haversine_distance(&w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub mod hash {

	use crate::err::Error;
	use crate::fnc::util::geo;
	use crate::sql::geometry::Geometry;
	use crate::sql::value::Value;

	pub fn encode((point, len): (Value, Option<usize>)) -> Result<Value, Error> {
		let len = match len {
			Some(len) if len > 0 && len < 12 => len,
			None => 12,
			_ => return Err(Error::InvalidArguments {
				name: String::from("geo::encode"),
				message: String::from("The second argument must be an integer greater than 0 and less than or equal to 12."),
			})
		};

		Ok(match point {
			Value::Geometry(Geometry::Point(v)) => geo::encode(v, len).into(),
			_ => Value::None,
		})
	}

	pub fn decode((arg,): (Value,)) -> Result<Value, Error> {
		match arg {
			Value::Strand(v) => Ok(geo::decode(v).into()),
			_ => Ok(Value::None),
		}
	}
}
