use anyhow::Result;
use geo::algorithm::bearing::HaversineBearing;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::chamberlain_duquette_area::ChamberlainDuquetteArea;
use geo::algorithm::haversine_distance::HaversineDistance;

use crate::val::{Geometry, Value};

pub fn area((arg,): (Geometry,)) -> Result<Value> {
	match arg {
		Geometry::Point(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::Line(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::Polygon(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::MultiPoint(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::MultiLine(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::MultiPolygon(v) => Ok(v.chamberlain_duquette_unsigned_area().into()),
		Geometry::Collection(v) => Ok(v
			.into_iter()
			.collect::<geo::Geometry<f64>>()
			.chamberlain_duquette_unsigned_area()
			.into()),
	}
}

pub fn bearing((v, w): (Geometry, Geometry)) -> Result<Value> {
	Ok(match (v, w) {
		(Geometry::Point(v), Geometry::Point(w)) => v.haversine_bearing(w).into(),
		_ => Value::None,
	})
}

pub fn centroid((arg,): (Geometry,)) -> Result<Value> {
	let centroid = match arg {
		Geometry::Point(v) => Some(v.centroid()),
		Geometry::Line(v) => v.centroid(),
		Geometry::Polygon(v) => v.centroid(),
		Geometry::MultiPoint(v) => v.centroid(),
		Geometry::MultiLine(v) => v.centroid(),
		Geometry::MultiPolygon(v) => v.centroid(),
		Geometry::Collection(v) => v.into_iter().collect::<geo::Geometry<f64>>().centroid(),
	};
	Ok(centroid.map(Into::into).unwrap_or(Value::None))
}

pub fn distance((v, w): (Geometry, Geometry)) -> Result<Value> {
	Ok(match (v, w) {
		(Geometry::Point(v), Geometry::Point(w)) => v.haversine_distance(&w).into(),
		_ => Value::None,
	})
}

pub mod hash {

	use anyhow::{Result, bail};

	use crate::err::Error;
	use crate::fnc::args::Optional;
	use crate::fnc::util::geo;
	use crate::val::{Geometry, Value};

	pub fn encode((arg, Optional(len)): (Geometry, Optional<i64>)) -> Result<Value> {
		let len = match len {
			Some(len) if (1..=12).contains(&len) => len as usize,
			None => 12usize,
			_ => bail!(Error::InvalidArguments {
				name: String::from("geo::encode"),
				message: String::from(
					"The second argument must be an integer greater than 0 and less than or equal to 12."
				),
			}),
		};

		Ok(match arg {
			Geometry::Point(v) => geo::encode(v, len).into(),
			_ => Value::None,
		})
	}

	pub fn decode((arg,): (Value,)) -> Result<Value> {
		match arg {
			Value::Strand(v) => Ok(geo::decode(v).into()),
			_ => Ok(Value::None),
		}
	}
}

pub mod is {
	use anyhow::Result;

	use crate::val::{Geometry, Value};

	pub fn valid((arg,): (Geometry,)) -> Result<Value> {
		Ok(arg.is_valid().into())
	}
}
