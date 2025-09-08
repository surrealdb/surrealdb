use std::cmp::Ordering;
use std::hash;
use std::iter::once;

use geo::{
	Coord, LineString, LinesIter, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use serde::{Deserialize, Serialize};

use crate::{GeometryKind, Object, SurrealValue, Value, array, object};

/// Represents geometric shapes in SurrealDB
///
/// Geometry types support various geometric shapes including points, lines, polygons,
/// and their multi-variants. This is useful for spatial data and geographic applications.
///
/// The types used internally originate from the `geo` crate.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Geometry {
	/// A single point in 2D space
	Point(Point<f64>),
	/// A line consisting of multiple connected points
	Line(LineString<f64>),
	/// A polygon with an exterior boundary and optional interior holes
	Polygon(Polygon<f64>),
	/// Multiple points
	MultiPoint(MultiPoint<f64>),
	/// Multiple lines
	MultiLine(MultiLineString<f64>),
	/// Multiple polygons
	MultiPolygon(MultiPolygon<f64>),
	/// A collection of different geometry types
	Collection(Vec<Geometry>),
}

macro_rules! impl_geometry {
	($($variant:ident($type:ty) => ($is:ident, $into:ident, $from:ident),)+) => {
		impl Geometry {
			/// Get the kind of geometry
			pub fn kind(&self) -> GeometryKind {
				match self {
					$(
						Self::$variant(_) => GeometryKind::$variant,
					)+
				}
			}

			$(
				/// Check if this is a of the given type
				pub fn $is(&self) -> bool {
					matches!(self, Self::$variant(_))
				}

				/// Convert this geometry into the given type
				pub fn $into(self) -> anyhow::Result<$type> {
					if let Self::$variant(v) = self {
						Ok(v)
					} else {
						Err(anyhow::anyhow!("Expected a geometry<{}> but got a geometry<{}>", GeometryKind::$variant, self.kind()))
					}
				}

				/// Create a new geometry from the given type
				pub fn $from(v: $type) -> Self {
					Self::$variant(v)
				}
			)+
		}
	}
}

impl_geometry! (
	Point(Point<f64>) => (is_point, into_point, from_point),
	Line(LineString<f64>) => (is_line, into_line, from_line),
	Polygon(Polygon<f64>) => (is_polygon, into_polygon, from_polygon),
	MultiPoint(MultiPoint<f64>) => (is_multipoint, into_multipoint, from_multipoint),
	MultiLine(MultiLineString<f64>) => (is_multiline, into_multiline, from_multiline),
	MultiPolygon(MultiPolygon<f64>) => (is_multipolygon, into_multipolygon, from_multipolygon),
	Collection(Vec<Geometry>) => (is_collection, into_collection, from_collection),
);

impl Geometry {
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
			array![v.x(), v.y()].into_value()
		}

		fn line(v: &LineString) -> Value {
			v.points().map(|v| point(&v)).collect::<Vec<Value>>().into_value()
		}

		fn polygon(v: &Polygon) -> Value {
			once(v.exterior()).chain(v.interiors()).map(line).collect::<Vec<Value>>().into_value()
		}

		fn multipoint(v: &MultiPoint) -> Value {
			v.iter().map(point).collect::<Vec<Value>>().into_value()
		}

		fn multiline(v: &MultiLineString) -> Value {
			v.iter().map(line).collect::<Vec<Value>>().into_value()
		}

		fn multipolygon(v: &MultiPolygon) -> Value {
			v.iter().map(polygon).collect::<Vec<Value>>().into_value()
		}

		fn collection(v: &[Geometry]) -> Value {
			v.iter().map(Geometry::as_coordinates).collect::<Vec<Value>>().into_value()
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

	/// Convert this geometry into an object
	pub fn as_object(&self) -> Object {
		object! {
			type: self.as_type().to_string(),
			coordinates: self.as_coordinates(),
		}
	}

	/// Try to convert an object into a geometry
	pub fn try_from_object(object: &Object) -> Option<Geometry> {
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

	/// Converts a surreal value to a MultiPolygon if the array matches to a MultiPolygon.
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

	/// Converts a surreal value to a MultiLine if the array matches to a MultiLine.
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

	/// Converts a surreal value to a MultiPoint if the array matches to a MultiPoint.
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

	/// Converts a surreal value to a LineString if the array matches to a LineString.
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
		let a = v.0[0].clone().into_float().ok()?;
		let b = v.0[1].clone().into_float().ok()?;
		Some(Point::from((a, b)))
	}
}

impl PartialOrd for Geometry {
	#[rustfmt::skip]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		#[inline]
		fn coord(v: &Coord) -> (f64, f64) {
			v.x_y()
		}

		#[inline]
		fn point(v: &Point) -> (f64, f64) {
			coord(&v.0)
		}

		#[inline]
		fn line(v: &LineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.into_iter().map(coord)
		}

		#[inline]
		fn polygon(v: &Polygon) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.interiors().iter().chain(once(v.exterior())).flat_map(line)
		}

		#[inline]
		fn multipoint(v: &MultiPoint) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.iter().map(point)
		}

		#[inline]
		fn multiline(v: &MultiLineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.iter().flat_map(line)
		}

		#[inline]
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
