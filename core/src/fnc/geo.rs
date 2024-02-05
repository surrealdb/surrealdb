use crate::err::Error;
use crate::sql::geometry::Geometry;
use crate::sql::value::Value;
use geo::algorithm::bearing::HaversineBearing;
use geo::algorithm::centroid::Centroid;
use geo::algorithm::chamberlain_duquette_area::ChamberlainDuquetteArea;
use geo::algorithm::haversine_distance::HaversineDistance;

pub fn area((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Geometry(v) => match v {
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
		},
		_ => Ok(Value::None),
	}
}

pub fn bearing(points: (Value, Value)) -> Result<Value, Error> {
	Ok(match points {
		(Value::Geometry(Geometry::Point(v)), Value::Geometry(Geometry::Point(w))) => {
			v.haversine_bearing(w).into()
		}
		_ => Value::None,
	})
}

pub fn centroid((arg,): (Value,)) -> Result<Value, Error> {
	let centroid = match arg {
		Value::Geometry(v) => match v {
			Geometry::Point(v) => Some(v.centroid()),
			Geometry::Line(v) => v.centroid(),
			Geometry::Polygon(v) => v.centroid(),
			Geometry::MultiPoint(v) => v.centroid(),
			Geometry::MultiLine(v) => v.centroid(),
			Geometry::MultiPolygon(v) => v.centroid(),
			Geometry::Collection(v) => v.into_iter().collect::<geo::Geometry<f64>>().centroid(),
		},
		_ => None,
	};
	Ok(centroid.map(Into::into).unwrap_or(Value::None))
}

pub fn distance(points: (Value, Value)) -> Result<Value, Error> {
	Ok(match points {
		(Value::Geometry(Geometry::Point(v)), Value::Geometry(Geometry::Point(w))) => {
			v.haversine_distance(&w).into()
		}
		_ => Value::None,
	})
}

pub mod hash {

	use crate::err::Error;
	use crate::fnc::util::geo;
	use crate::sql::geometry::Geometry;
	use crate::sql::value::Value;

	pub fn encode((point, len): (Value, Option<usize>)) -> Result<Value, Error> {
		let len = match len {
			Some(len) if (1..=12).contains(&len) => len,
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
