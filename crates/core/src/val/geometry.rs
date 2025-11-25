#![allow(clippy::derived_hash_with_manual_eq)]
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash;
use std::iter::once;

use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::{Coord, LineString, LinesIter, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::Object;
use crate::fmt::Fmt;
use crate::val::{Array, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "$surrealdb::private::Geometry")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) enum Geometry {
	Point(Point<f64>),
	Line(LineString<f64>),
	Polygon(Polygon<f64>),
	MultiPoint(MultiPoint<f64>),
	MultiLine(MultiLineString<f64>),
	MultiPolygon(MultiPolygon<f64>),
	Collection(Vec<Geometry>),
	// Add new variants here
}

impl Geometry {
	/// Check if this is not a Collection
	pub fn is_geometry(&self) -> bool {
		!matches!(self, Self::Collection(_))
	}
	/// Check if this is a Collection
	pub fn is_collection(&self) -> bool {
		matches!(self, Self::Collection(_))
	}

	/// Check if this has valid latitude and longitude points:
	/// * -90 <= lat <= 90
	/// * -180 <= lng <= 180
	pub fn is_valid(&self) -> bool {
		match self {
			Geometry::Point(p) => {
				(-90.0..=90.0).contains(&p.0.y) && (-180.0..=180.0).contains(&p.0.x)
			}
			Geometry::MultiPoint(v) => v
				.iter()
				.all(|p| (-90.0..=90.0).contains(&p.0.y) && (-180.0..=180.0).contains(&p.0.x)),
			Geometry::Line(v) => v.lines_iter().all(|l| {
				(-90.0..=90.0).contains(&l.start.y)
					&& (-180.0..=180.0).contains(&l.start.x)
					&& (-90.0..=90.0).contains(&l.end.y)
					&& (-180.0..=180.0).contains(&l.end.x)
			}),
			Geometry::Polygon(v) => v.lines_iter().all(|l| {
				(-90.0..=90.0).contains(&l.start.y)
					&& (-180.0..=180.0).contains(&l.start.x)
					&& (-90.0..=90.0).contains(&l.end.y)
					&& (-180.0..=180.0).contains(&l.end.x)
			}),
			Geometry::MultiLine(v) => v.iter().all(|l| {
				l.lines_iter().all(|l| {
					(-90.0..=90.0).contains(&l.start.y)
						&& (-180.0..=180.0).contains(&l.start.x)
						&& (-90.0..=90.0).contains(&l.end.y)
						&& (-180.0..=180.0).contains(&l.end.x)
				})
			}),
			Geometry::MultiPolygon(v) => v.iter().all(|p| {
				p.lines_iter().all(|l| {
					(-90.0..=90.0).contains(&l.start.y)
						&& (-180.0..=180.0).contains(&l.start.x)
						&& (-90.0..=90.0).contains(&l.end.y)
						&& (-180.0..=180.0).contains(&l.end.x)
				})
			}),
			Geometry::Collection(v) => v.iter().all(Geometry::is_valid),
		}
	}
	/// Get the type of this Geometry as text
	pub fn as_type(&self) -> &'static str {
		match self {
			Self::Point(_) => "Point",
			Self::Line(_) => "LineString",
			Self::Polygon(_) => "Polygon",
			Self::MultiPoint(_) => "MultiPoint",
			Self::MultiLine(_) => "MultiLineString",
			Self::MultiPolygon(_) => "MultiPolygon",
			Self::Collection(_) => "GeometryCollection",
		}
	}
	/// Get the raw coordinates of this Geometry as an Array
	pub fn as_coordinates(&self) -> Value {
		fn point(v: &Point) -> Value {
			Array::from(vec![v.x(), v.y()]).into()
		}

		fn line(v: &LineString) -> Value {
			v.points().map(|v| point(&v)).collect::<Vec<Value>>().into()
		}

		fn polygon(v: &Polygon) -> Value {
			once(v.exterior()).chain(v.interiors()).map(line).collect::<Vec<Value>>().into()
		}

		fn multipoint(v: &MultiPoint) -> Value {
			v.iter().map(point).collect::<Vec<Value>>().into()
		}

		fn multiline(v: &MultiLineString) -> Value {
			v.iter().map(line).collect::<Vec<Value>>().into()
		}

		fn multipolygon(v: &MultiPolygon) -> Value {
			v.iter().map(polygon).collect::<Vec<Value>>().into()
		}

		fn collection(v: &[Geometry]) -> Value {
			v.iter().map(Geometry::as_coordinates).collect::<Vec<Value>>().into()
		}

		match self {
			Self::Point(v) => point(v),
			Self::Line(v) => line(v),
			Self::Polygon(v) => polygon(v),
			Self::MultiPoint(v) => multipoint(v),
			Self::MultiLine(v) => multiline(v),
			Self::MultiPolygon(v) => multipolygon(v),
			Self::Collection(v) => collection(v),
		}
	}

	/// Get the GeoJSON object representation for this geometry
	pub fn as_object(&self) -> Object {
		let mut obj = BTreeMap::<String, Value>::new();
		obj.insert("type".into(), self.as_type().into());
		obj.insert(
			match self {
				Self::Collection(_) => "geometries",
				_ => "coordinates",
			}
			.into(),
			self.as_coordinates(),
		);

		obj.into()
	}

	#[cfg_attr(not(feature = "scripting"), expect(dead_code))]
	pub(crate) fn try_from_object(object: &Object) -> Option<Geometry> {
		if object.len() != 2 {
			return None;
		}

		let Some(Value::String(key)) = object.get("type") else {
			return None;
		};

		match key.as_str() {
			"Point" => {
				object.get("coordinates").and_then(Geometry::array_to_point).map(Geometry::Point)
			}
			"LineString" => {
				object.get("coordinates").and_then(Geometry::array_to_line).map(Geometry::Line)
			}
			"Polygon" => object
				.get("coordinates")
				.and_then(Geometry::array_to_polygon)
				.map(Geometry::Polygon),
			"MultiPoint" => object
				.get("coordinates")
				.and_then(Geometry::array_to_multipoint)
				.map(Geometry::MultiPoint),
			"MultiLineString" => object
				.get("coordinates")
				.and_then(Geometry::array_to_multiline)
				.map(Geometry::MultiLine),
			"MultiPolygon" => object
				.get("coordinates")
				.and_then(Geometry::array_to_multipolygon)
				.map(Geometry::MultiPolygon),
			"GeometryCollection" => {
				let Some(Value::Array(x)) = object.get("geometries") else {
					return None;
				};

				let mut res = Vec::with_capacity(x.len());

				for x in x.iter() {
					let Value::Geometry(x) = x else {
						return None;
					};
					res.push(x.clone());
				}

				Some(Geometry::Collection(res))
			}

			_ => None,
		}
	}

	/// Converts a surreal value to a MultiPolygon if the array matches to a
	/// MultiPolygon.
	pub(crate) fn array_to_multipolygon(v: &Value) -> Option<MultiPolygon<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::array_to_polygon(x)?);
		}
		Some(MultiPolygon::new(res))
	}

	/// Converts a surreal value to a MultiLine if the array matches to a
	/// MultiLine.
	pub(crate) fn array_to_multiline(v: &Value) -> Option<MultiLineString<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::array_to_line(x)?);
		}
		Some(MultiLineString::new(res))
	}

	/// Converts a surreal value to a MultiPoint if the array matches to a
	/// MultiPoint.
	pub(crate) fn array_to_multipoint(v: &Value) -> Option<MultiPoint<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::array_to_point(x)?);
		}
		Some(MultiPoint::new(res))
	}

	/// Converts a surreal value to a Polygon if the array matches to a Polygon.
	pub(crate) fn array_to_polygon(v: &Value) -> Option<Polygon<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		if v.is_empty() {
			return None;
		}
		let first = Self::array_to_line(&v[0])?;
		for x in &v[1..] {
			res.push(Self::array_to_line(x)?);
		}
		Some(Polygon::new(first, res))
	}

	/// Converts a surreal value to a LineString if the array matches to a
	/// LineString.
	pub(crate) fn array_to_line(v: &Value) -> Option<LineString<f64>> {
		let mut res = Vec::new();
		let Value::Array(v) = v else {
			return None;
		};
		for x in v.iter() {
			res.push(Self::array_to_point(x)?);
		}
		Some(LineString::from(res))
	}

	/// Converts a surreal value to a Point if the array matches to a point.
	pub(crate) fn array_to_point(v: &Value) -> Option<Point<f64>> {
		let Value::Array(v) = v else {
			return None;
		};
		if v.len() != 2 {
			return None;
		}
		// FIXME: This truncates decimals and large integers into a f64.
		let Value::Number(ref a) = v.0[0] else {
			return None;
		};
		let Value::Number(ref b) = v.0[1] else {
			return None;
		};
		Some(Point::from(((*a).try_into().ok()?, (*b).try_into().ok()?)))
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	pub fn contains(&self, other: &Self) -> bool {
		match self {
			Self::Point(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::MultiPoint(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Self::Line(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::MultiLine(w) => w.iter().all(|x| w.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Self::Polygon(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::Polygon(w) => v.contains(w),
				Self::MultiPolygon(w) => w.iter().all(|x| w.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Self::MultiPoint(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::MultiPoint(w) => w.iter().all(|x| w.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Self::MultiLine(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::MultiLine(w) => w.iter().all(|x| w.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Self::MultiPolygon(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::Polygon(w) => v.contains(w),
				Self::MultiPoint(w) => v.contains(w),
				Self::MultiLine(w) => v.contains(w),
				Self::MultiPolygon(w) => v.contains(w),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
			},
			Self::Collection(v) => v.iter().all(|x| x.contains(other)),
		}
	}

	pub fn intersects(&self, other: &Self) -> bool {
		match self {
			Self::Point(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::Line(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::Polygon(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => v.intersects(w),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::MultiPoint(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::MultiLine(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::MultiPolygon(v) => match other {
				Self::Point(w) => v.intersects(w),
				Self::Line(w) => v.intersects(w),
				Self::Polygon(w) => v.intersects(w),
				Self::MultiPoint(w) => v.intersects(w),
				Self::MultiLine(w) => v.intersects(w),
				Self::MultiPolygon(w) => v.intersects(w),
				Self::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Self::Collection(v) => v.iter().all(|x| x.intersects(other)),
		}
	}
}

impl PartialOrd for Geometry {
	#[rustfmt::skip]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		fn coord(v: &Coord) -> (f64, f64) {
			v.x_y()
		}

		fn point(v: &Point) -> (f64, f64) {
			coord(&v.0)
		}

		fn line(v: &LineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.into_iter().map(coord)
		}

		fn polygon(v: &Polygon) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.interiors().iter().chain(once(v.exterior())).flat_map(line)
		}

		fn multipoint(v: &MultiPoint) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.iter().map(point)
		}

		fn multiline(v: &MultiLineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.iter().flat_map(line)
		}

		fn multipolygon(v: &MultiPolygon) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.iter().flat_map(polygon)
		}

		match (self, other) {
			//
			(Self::Point(_), Self::Line(_)) => Some(Ordering::Less),
			(Self::Point(_), Self::Polygon(_)) => Some(Ordering::Less),
			(Self::Point(_), Self::MultiPoint(_)) => Some(Ordering::Less),
			(Self::Point(_), Self::MultiLine(_)) => Some(Ordering::Less),
			(Self::Point(_), Self::MultiPolygon(_)) => Some(Ordering::Less),
			(Self::Point(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::Line(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::Line(_), Self::Polygon(_)) => Some(Ordering::Less),
			(Self::Line(_), Self::MultiPoint(_)) => Some(Ordering::Less),
			(Self::Line(_), Self::MultiLine(_)) => Some(Ordering::Less),
			(Self::Line(_), Self::MultiPolygon(_)) => Some(Ordering::Less),
			(Self::Line(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::Polygon(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::Polygon(_), Self::Line(_)) => Some(Ordering::Greater),
			(Self::Polygon(_), Self::MultiPoint(_)) => Some(Ordering::Less),
			(Self::Polygon(_), Self::MultiLine(_)) => Some(Ordering::Less),
			(Self::Polygon(_), Self::MultiPolygon(_)) => Some(Ordering::Less),
			(Self::Polygon(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::MultiPoint(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::MultiPoint(_), Self::Line(_)) => Some(Ordering::Greater),
			(Self::MultiPoint(_), Self::Polygon(_)) => Some(Ordering::Greater),
			(Self::MultiPoint(_), Self::MultiLine(_)) => Some(Ordering::Less),
			(Self::MultiPoint(_), Self::MultiPolygon(_)) => Some(Ordering::Less),
			(Self::MultiPoint(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::MultiLine(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::MultiLine(_), Self::Line(_)) => Some(Ordering::Greater),
			(Self::MultiLine(_), Self::Polygon(_)) => Some(Ordering::Greater),
			(Self::MultiLine(_), Self::MultiPoint(_)) => Some(Ordering::Greater),
			(Self::MultiLine(_), Self::MultiPolygon(_)) => Some(Ordering::Less),
			(Self::MultiLine(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::MultiPolygon(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::MultiPolygon(_), Self::Line(_)) => Some(Ordering::Greater),
			(Self::MultiPolygon(_), Self::Polygon(_)) => Some(Ordering::Greater),
			(Self::MultiPolygon(_), Self::MultiPoint(_)) => Some(Ordering::Greater),
			(Self::MultiPolygon(_), Self::MultiLine(_)) => Some(Ordering::Greater),
			(Self::MultiPolygon(_), Self::Collection(_)) => Some(Ordering::Less),
			//
			(Self::Collection(_), Self::Point(_)) => Some(Ordering::Greater),
			(Self::Collection(_), Self::Line(_)) => Some(Ordering::Greater),
			(Self::Collection(_), Self::Polygon(_)) => Some(Ordering::Greater),
			(Self::Collection(_), Self::MultiPoint(_)) => Some(Ordering::Greater),
			(Self::Collection(_), Self::MultiLine(_)) => Some(Ordering::Greater),
			(Self::Collection(_), Self::MultiPolygon(_)) => Some(Ordering::Greater),
			//
			(Self::Point(a), Self::Point(b)) => point(a).partial_cmp(&point(b)),
			(Self::Line(a), Self::Line(b)) => line(a).partial_cmp(line(b)),
			(Self::Polygon(a), Self::Polygon(b)) => polygon(a).partial_cmp(polygon(b)),
			(Self::MultiPoint(a), Self::MultiPoint(b)) => multipoint(a).partial_cmp(multipoint(b)),
			(Self::MultiLine(a), Self::MultiLine(b)) => multiline(a).partial_cmp(multiline(b)),
			(Self::MultiPolygon(a), Self::MultiPolygon(b)) => multipolygon(a).partial_cmp(multipolygon(b)),
			(Self::Collection(a), Self::Collection(b)) => a.partial_cmp(b),
		}
	}
}

impl From<(f64, f64)> for Geometry {
	fn from(v: (f64, f64)) -> Self {
		Self::Point(v.into())
	}
}

impl From<[f64; 2]> for Geometry {
	fn from(v: [f64; 2]) -> Self {
		Self::Point(v.into())
	}
}

impl From<Point<f64>> for Geometry {
	fn from(v: Point<f64>) -> Self {
		Self::Point(v)
	}
}

impl From<LineString<f64>> for Geometry {
	fn from(v: LineString<f64>) -> Self {
		Self::Line(v)
	}
}

impl From<Polygon<f64>> for Geometry {
	fn from(v: Polygon<f64>) -> Self {
		Self::Polygon(v)
	}
}

impl From<MultiPoint<f64>> for Geometry {
	fn from(v: MultiPoint<f64>) -> Self {
		Self::MultiPoint(v)
	}
}

impl From<MultiLineString<f64>> for Geometry {
	fn from(v: MultiLineString<f64>) -> Self {
		Self::MultiLine(v)
	}
}

impl From<MultiPolygon<f64>> for Geometry {
	fn from(v: MultiPolygon<f64>) -> Self {
		Self::MultiPolygon(v)
	}
}

impl From<Vec<Geometry>> for Geometry {
	fn from(v: Vec<Geometry>) -> Self {
		Self::Collection(v)
	}
}

impl From<Vec<Point<f64>>> for Geometry {
	fn from(v: Vec<Point<f64>>) -> Self {
		Self::MultiPoint(MultiPoint(v))
	}
}

impl From<Vec<LineString<f64>>> for Geometry {
	fn from(v: Vec<LineString<f64>>) -> Self {
		Self::MultiLine(MultiLineString(v))
	}
}

impl From<Vec<Polygon<f64>>> for Geometry {
	fn from(v: Vec<Polygon<f64>>) -> Self {
		Self::MultiPolygon(MultiPolygon(v))
	}
}

impl From<Geometry> for geo::Geometry<f64> {
	fn from(v: Geometry) -> Self {
		match v {
			Geometry::Point(v) => v.into(),
			Geometry::Line(v) => v.into(),
			Geometry::Polygon(v) => v.into(),
			Geometry::MultiPoint(v) => v.into(),
			Geometry::MultiLine(v) => v.into(),
			Geometry::MultiPolygon(v) => v.into(),
			Geometry::Collection(v) => v.into_iter().collect::<geo::Geometry<f64>>(),
		}
	}
}

impl FromIterator<Geometry> for geo::Geometry<f64> {
	fn from_iter<I: IntoIterator<Item = Geometry>>(iter: I) -> Self {
		let mut c: Vec<geo::Geometry<f64>> = vec![];
		for i in iter {
			c.push(i.into())
		}
		geo::Geometry::GeometryCollection(geo::GeometryCollection(c))
	}
}

impl From<Geometry> for surrealdb_types::Geometry {
	fn from(v: Geometry) -> Self {
		match v {
			Geometry::Point(v) => surrealdb_types::Geometry::Point(v),
			Geometry::Line(v) => surrealdb_types::Geometry::Line(v),
			Geometry::Polygon(v) => surrealdb_types::Geometry::Polygon(v),
			Geometry::MultiPoint(v) => surrealdb_types::Geometry::MultiPoint(v),
			Geometry::MultiLine(v) => surrealdb_types::Geometry::MultiLine(v),
			Geometry::MultiPolygon(v) => surrealdb_types::Geometry::MultiPolygon(v),
			Geometry::Collection(v) => {
				surrealdb_types::Geometry::Collection(v.into_iter().map(Into::into).collect())
			}
		}
	}
}

impl From<surrealdb_types::Geometry> for Geometry {
	fn from(v: surrealdb_types::Geometry) -> Self {
		match v {
			surrealdb_types::Geometry::Point(v) => Geometry::Point(v),
			surrealdb_types::Geometry::Line(v) => Geometry::Line(v),
			surrealdb_types::Geometry::Polygon(v) => Geometry::Polygon(v),
			surrealdb_types::Geometry::MultiPoint(v) => Geometry::MultiPoint(v),
			surrealdb_types::Geometry::MultiLine(v) => Geometry::MultiLine(v),
			surrealdb_types::Geometry::MultiPolygon(v) => Geometry::MultiPolygon(v),
			surrealdb_types::Geometry::Collection(v) => {
				Geometry::Collection(v.into_iter().map(Into::into).collect())
			}
		}
	}
}

impl ToSql for Geometry {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		match self {
			Self::Point(v) => {
				write_sql!(f, sql_fmt, "({}, {})", v.x(), v.y())
			}
			Self::Line(v) => write_sql!(
				f,
				sql_fmt,
				"{{ type: 'LineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
					f,
					fmt,
					"[{}, {}]",
					v.x(),
					v.y()
				))))
			),
			Self::Polygon(v) => write_sql!(
				f,
				sql_fmt,
				"{{ type: 'Polygon', coordinates: [{}] }}",
				Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(|v| Fmt::new(
					v,
					|v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(
							v,
							|v, f, fmt| write_sql!(f, fmt, "[{}, {}]", v.x(), v.y())
						)))
					)
				)))
			),
			Self::MultiPoint(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiPoint', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}, {}]",
						v.x(),
						v.y()
					))))
				)
			}
			Self::MultiLine(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiLineString', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| write_sql!(
						f,
						fmt,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(
							v,
							|v, f, fmt| write_sql!(f, fmt, "[{}, {}]", v.x(), v.y())
						)))
					))))
				)
			}
			Self::MultiPolygon(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'MultiPolygon', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f, fmt| {
						write_sql!(
							f,
							fmt,
							"[{}]",
							Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(
								|v| Fmt::new(v, |v, f, fmt| write_sql!(
									f,
									fmt,
									"[{}]",
									Fmt::comma_separated(v.points().map(|v| Fmt::new(
										v,
										|v, f, fmt| write_sql!(f, fmt, "[{}, {}]", v.x(), v.y())
									)))
								))
							))
						)
					}))),
				)
			}
			Self::Collection(v) => {
				write_sql!(
					f,
					sql_fmt,
					"{{ type: 'GeometryCollection', geometries: [{}] }}",
					Fmt::comma_separated(v)
				)
			}
		}
	}
}

impl hash::Hash for Geometry {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			Geometry::Point(p) => {
				"Point".hash(state);
				p.x().to_bits().hash(state);
				p.y().to_bits().hash(state);
			}
			Geometry::Line(l) => {
				"Line".hash(state);
				l.points().for_each(|v| {
					v.x().to_bits().hash(state);
					v.y().to_bits().hash(state);
				});
			}
			Geometry::Polygon(p) => {
				"Polygon".hash(state);
				p.exterior().points().for_each(|ext| {
					ext.x().to_bits().hash(state);
					ext.y().to_bits().hash(state);
				});
				p.interiors().iter().for_each(|int| {
					int.points().for_each(|v| {
						v.x().to_bits().hash(state);
						v.y().to_bits().hash(state);
					});
				});
			}
			Geometry::MultiPoint(v) => {
				"MultiPoint".hash(state);
				v.0.iter().for_each(|v| {
					v.x().to_bits().hash(state);
					v.y().to_bits().hash(state);
				});
			}
			Geometry::MultiLine(ml) => {
				"MultiLine".hash(state);
				ml.0.iter().for_each(|ls| {
					ls.points().for_each(|p| {
						p.x().to_bits().hash(state);
						p.y().to_bits().hash(state);
					});
				});
			}
			Geometry::MultiPolygon(mp) => {
				"MultiPolygon".hash(state);
				mp.0.iter().for_each(|p| {
					p.exterior().points().for_each(|ext| {
						ext.x().to_bits().hash(state);
						ext.y().to_bits().hash(state);
					});
					p.interiors().iter().for_each(|int| {
						int.points().for_each(|v| {
							v.x().to_bits().hash(state);
							v.y().to_bits().hash(state);
						});
					});
				});
			}
			Geometry::Collection(v) => {
				"GeometryCollection".hash(state);
				v.iter().for_each(|v| v.hash(state));
			}
		}
	}
}

impl<F> Encode<F> for Geometry {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> Result<(), storekey::EncodeError> {
		match self {
			Geometry::Point(point) => {
				w.write_u8(2)?;
				Encode::<F>::encode(&point.x(), w)?;
				Encode::<F>::encode(&point.y(), w)?;
			}
			Geometry::Line(line_string) => {
				w.write_u8(3)?;
				for p in line_string.points() {
					w.mark_terminator();
					Encode::<F>::encode(&p.x(), w)?;
					Encode::<F>::encode(&p.y(), w)?;
				}
				w.write_terminator()?;
			}
			Geometry::Polygon(polygon) => {
				w.write_u8(4)?;
				for p in polygon.exterior().points() {
					w.mark_terminator();
					Encode::<F>::encode(&p.x(), w)?;
					Encode::<F>::encode(&p.y(), w)?;
				}
				w.write_terminator()?;
				for l in polygon.interiors() {
					for p in l.points() {
						w.mark_terminator();
						Encode::<F>::encode(&p.x(), w)?;
						Encode::<F>::encode(&p.y(), w)?;
					}
					w.write_terminator()?;
				}
				w.write_terminator()?;
			}
			Geometry::MultiPoint(multi_point) => {
				w.write_u8(5)?;
				for p in multi_point.iter() {
					w.mark_terminator();
					Encode::<F>::encode(&p.x(), w)?;
					Encode::<F>::encode(&p.y(), w)?;
				}
				w.write_terminator()?;
			}
			Geometry::MultiLine(multi_line_string) => {
				w.write_u8(6)?;
				for l in multi_line_string.iter() {
					for p in l.points() {
						w.mark_terminator();
						Encode::<F>::encode(&p.x(), w)?;
						Encode::<F>::encode(&p.y(), w)?;
					}
					w.write_terminator()?;
				}
				w.write_terminator()?;
			}
			Geometry::MultiPolygon(multi_polygon) => {
				w.write_u8(7)?;
				for p in multi_polygon.iter() {
					for p in p.exterior().points() {
						w.mark_terminator();
						Encode::<F>::encode(&p.x(), w)?;
						Encode::<F>::encode(&p.y(), w)?;
					}
					w.write_terminator()?;
					for l in p.interiors() {
						for p in l.points() {
							w.mark_terminator();
							Encode::<F>::encode(&p.x(), w)?;
							Encode::<F>::encode(&p.y(), w)?;
						}
						w.write_terminator()?;
					}
					w.write_terminator()?;
				}
				w.write_terminator()?;
			}
			Geometry::Collection(items) => {
				w.write_u8(8)?;
				for g in items.iter() {
					w.mark_terminator();
					Encode::<F>::encode(g, w)?;
				}
				w.write_terminator()?;
			}
		}
		Ok(())
	}
}

impl<'de, F> BorrowDecode<'de, F> for Geometry {
	fn borrow_decode(r: &mut storekey::BorrowReader<'de>) -> Result<Self, storekey::DecodeError> {
		match r.read_u8()? {
			2 => {
				let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
				Ok(Geometry::Point(Point::from(point)))
			}
			3 => {
				let mut res = Vec::new();
				while !r.read_terminal()? {
					let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
					res.push(Coord::from(point))
				}
				Ok(Geometry::Line(LineString::new(res)))
			}
			4 => {
				let mut ext = Vec::new();
				while !r.read_terminal()? {
					let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
					ext.push(Coord::from(point))
				}
				let ext = LineString::new(ext);
				let mut int = Vec::new();
				while !r.read_terminal()? {
					let mut line = Vec::new();
					while !r.read_terminal()? {
						let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
						line.push(Coord::from(point))
					}
					int.push(LineString::new(line))
				}
				Ok(Geometry::Polygon(Polygon::new(ext, int)))
			}
			5 => {
				let mut res = Vec::new();
				while !r.read_terminal()? {
					let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
					res.push(Point::from(point))
				}
				Ok(Geometry::MultiPoint(MultiPoint::new(res)))
			}
			6 => {
				let mut lines = Vec::new();
				while !r.read_terminal()? {
					let mut res = Vec::new();
					while !r.read_terminal()? {
						let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
						res.push(Coord::from(point))
					}
					lines.push(LineString::new(res));
				}
				Ok(Geometry::MultiLine(MultiLineString::new(lines)))
			}
			7 => {
				let mut polygons = Vec::new();
				while !r.read_terminal()? {
					let mut ext = Vec::new();
					while !r.read_terminal()? {
						let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
						ext.push(Coord::from(point))
					}
					let ext = LineString::new(ext);
					let mut int = Vec::new();
					while !r.read_terminal()? {
						let mut line = Vec::new();
						while !r.read_terminal()? {
							let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
							line.push(Coord::from(point))
						}
						int.push(LineString::new(line))
					}
					polygons.push(Polygon::new(ext, int));
				}
				Ok(Geometry::MultiPolygon(MultiPolygon::new(polygons)))
			}
			8 => {
				let mut geoms = Vec::new();
				while !r.read_terminal()? {
					geoms.push(BorrowDecode::<'de, F>::borrow_decode(r)?)
				}
				Ok(Geometry::Collection(geoms))
			}
			_ => Err(storekey::DecodeError::InvalidFormat),
		}
	}
}

#[cfg(test)]
mod test {
	use core::f64;

	use geo::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

	use super::Geometry;

	fn round_trip(geom: Geometry) {
		let enc = storekey::encode_vec(&geom).unwrap();
		let dec = storekey::decode_borrow(&enc).unwrap();
		assert_eq!(geom, dec)
	}

	#[test]
	fn encode_decode() {
		round_trip(Geometry::Point(Point::new(0.0, f64::INFINITY)));
		round_trip(Geometry::Line(LineString::new(vec![
			Coord::from((0.0, f64::INFINITY)),
			Coord::from((f64::NEG_INFINITY, 1.0)),
		])));
		round_trip(Geometry::Polygon(Polygon::new(
			LineString::new(vec![
				Coord::from((0.0, f64::INFINITY)),
				Coord::from((f64::NEG_INFINITY, 1.0)),
			]),
			vec![
				LineString::new(vec![Coord::from((1.0, 2.0)), Coord::from((3.0, 4.0))]),
				LineString::new(vec![Coord::from((5.0, 6.0)), Coord::from((7.0, 8.0))]),
			],
		)));
		round_trip(Geometry::MultiPoint(MultiPoint::new(vec![
			Point::from((0.0, f64::INFINITY)),
			Point::from((f64::NEG_INFINITY, 1.0)),
		])));
		round_trip(Geometry::MultiLine(MultiLineString::new(vec![
			LineString::new(vec![
				Coord::from((0.0, f64::INFINITY)),
				Coord::from((f64::NEG_INFINITY, 1.0)),
			]),
			LineString::new(vec![Coord::from((1.0, 2.0)), Coord::from((3.0, 4.0))]),
		])));
		round_trip(Geometry::MultiPolygon(MultiPolygon::new(vec![
			Polygon::new(
				LineString::new(vec![
					Coord::from((0.0, f64::INFINITY)),
					Coord::from((f64::NEG_INFINITY, 1.0)),
				]),
				vec![
					LineString::new(vec![Coord::from((1.0, 2.0)), Coord::from((3.0, 4.0))]),
					LineString::new(vec![Coord::from((5.0, 6.0)), Coord::from((7.0, 8.0))]),
				],
			),
			Polygon::new(
				LineString::new(vec![Coord::from((9.0, 10.0)), Coord::from((11.0, 12.0))]),
				vec![
					LineString::new(vec![Coord::from((13.0, 14.0)), Coord::from((15.0, 16.0))]),
					LineString::new(vec![Coord::from((17.0, 18.0)), Coord::from((19.0, 20.0))]),
				],
			),
		])));
	}
}
