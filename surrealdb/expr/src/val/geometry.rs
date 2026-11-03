#![allow(clippy::derived_hash_with_manual_eq)]
#![deny(clippy::wildcard_enum_match_arm)]
use std::cmp::Ordering;
use std::hash;
use std::iter::once;

use common::fmt::Fmt;
#[cfg(feature = "geo")]
use geo::algorithm::contains::Contains;
#[cfg(feature = "geo")]
use geo::algorithm::intersects::Intersects;
use geo_types::{
	Coord, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use revision::{DeserializeRevisioned, Error, SkipCheckRevisioned, revisioned};
use storekey::{BorrowDecode, Encode};
use surrealdb_collections::bounded;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::Object;
use crate::expr::kind::GeometryKind;
use crate::val::number::cmp_f64;
use crate::val::{Array, Value};

#[revisioned(revision = 1, deserialize = false, skip = true, walk = true)]
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Geometry {
	Point(Point<f64>),
	Line(LineString<f64>),
	Polygon(Polygon<f64>),
	MultiPoint(MultiPoint<f64>),
	MultiLine(MultiLineString<f64>),
	MultiPolygon(MultiPolygon<f64>),
	Collection(Vec<Geometry>),
	// Add new variants here
}

/// Reads exactly what the derived serializer writes (revision 1: a `u32` variant, then its
/// geometry). Every length is read through [`bounded`], so a claimed count costs at most
/// [`bounded::PREALLOC_BYTES`] beyond the coordinates the input holds, and every polygon ring goes
/// through [`ring_before_closing`] before [`Polygon::new`] closes it.
impl DeserializeRevisioned for Geometry {
	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, Error> {
		bounded::read_revision(reader, "Geometry", 1)?;
		match u32::deserialize_revisioned(reader)? {
			0 => Ok(Self::Point(Point(read_coord(reader)?))),
			1 => Ok(Self::Line(read_line(reader)?)),
			2 => Ok(Self::Polygon(read_polygon(reader)?)),
			3 => {
				let len = bounded::read_len(reader)?;
				let points = bounded::read_vec_with(reader, len, |r| Ok(Point(read_coord(r)?)))?;
				Ok(Self::MultiPoint(MultiPoint(points)))
			}
			4 => {
				let len = bounded::read_len(reader)?;
				let lines = bounded::read_vec_with(reader, len, read_line)?;
				Ok(Self::MultiLine(MultiLineString(lines)))
			}
			5 => {
				let len = bounded::read_len(reader)?;
				let polygons = bounded::read_vec_with(reader, len, read_polygon)?;
				Ok(Self::MultiPolygon(MultiPolygon(polygons)))
			}
			6 => Ok(Self::Collection(bounded::read_vec::<Geometry, _>(reader)?)),
			x => Err(Error::Deserialize(format!(
				"Invalid discriminant `{x}` for enum `Geometry` revision `1`"
			))),
		}
	}
}

impl SkipCheckRevisioned for Geometry {
	fn skip_check_revisioned<R: std::io::Read>(reader: &mut R) -> Result<(), Error> {
		Self::deserialize_revisioned(reader).map(drop)
	}
}

fn read_coord<R: std::io::Read>(reader: &mut R) -> Result<Coord<f64>, Error> {
	let x = f64::deserialize_revisioned(reader)?;
	let y = f64::deserialize_revisioned(reader)?;
	Ok(Coord {
		x,
		y,
	})
}

fn read_line<R: std::io::Read>(reader: &mut R) -> Result<LineString<f64>, Error> {
	let len = bounded::read_len(reader)?;
	Ok(LineString(bounded::read_vec_with(reader, len, read_coord)?))
}

fn read_polygon<R: std::io::Read>(reader: &mut R) -> Result<Polygon<f64>, Error> {
	let exterior = ring_before_closing(read_line(reader)?);
	let len = bounded::read_len(reader)?;
	let interiors = bounded::read_vec_with(reader, len, |r| read_line(r).map(ring_before_closing))?;
	Ok(Polygon::new(exterior, interiors))
}

impl Geometry {
	/// The heap bytes this geometry holds: its coordinates, and a vector header
	/// for each line, ring and collection that holds them. An estimate for a
	/// memory bound, iterative so a deeply nested collection cannot overflow the
	/// stack.
	pub fn heap_bytes(&self) -> usize {
		const COORD: usize = std::mem::size_of::<Coord<f64>>();
		const VEC: usize = std::mem::size_of::<Vec<Coord<f64>>>();
		let line = |line: &LineString<f64>| VEC + line.0.len() * COORD;
		let polygon = |polygon: &Polygon<f64>| {
			line(polygon.exterior()) + VEC + polygon.interiors().iter().map(line).sum::<usize>()
		};
		let mut total = 0;
		let mut stack = vec![self];
		while let Some(geometry) = stack.pop() {
			total += match geometry {
				Self::Point(_) => 0,
				Self::Line(l) => line(l),
				Self::Polygon(p) => polygon(p),
				Self::MultiPoint(points) => VEC + points.0.len() * COORD,
				Self::MultiLine(lines) => VEC + lines.0.iter().map(line).sum::<usize>(),
				Self::MultiPolygon(polygons) => VEC + polygons.0.iter().map(polygon).sum::<usize>(),
				Self::Collection(geometries) => {
					stack.extend(geometries.iter());
					VEC + geometries.len() * std::mem::size_of::<Geometry>()
				}
			};
		}
		total
	}

	/// Check if this is not a Collection
	pub fn is_geometry(&self) -> bool {
		!matches!(self, Self::Collection(_))
	}
	/// Check if this is a Collection
	pub fn is_collection(&self) -> bool {
		matches!(self, Self::Collection(_))
	}

	/// Check if this Geometry matches one of the specified [`GeometryKind`]s.
	pub fn is_geometry_type(&self, types: &[GeometryKind]) -> bool {
		types.contains(&self.geometry_kind())
	}

	/// Extract a `Point<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_point(self) -> Result<Point<f64>, Self> {
		if let Self::Point(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `LineString<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_line(self) -> Result<LineString<f64>, Self> {
		if let Self::Line(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `Polygon<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_polygon(self) -> Result<Polygon<f64>, Self> {
		if let Self::Polygon(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `MultiPoint<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_multipoint(self) -> Result<MultiPoint<f64>, Self> {
		if let Self::MultiPoint(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `MultiLineString<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_multiline(self) -> Result<MultiLineString<f64>, Self> {
		if let Self::MultiLine(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `MultiPolygon<f64>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_multipolygon(self) -> Result<MultiPolygon<f64>, Self> {
		if let Self::MultiPolygon(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
	}

	/// Extract a `Vec<Geometry>`, returning `Err(self)` if this is a different variant.
	pub fn try_into_collection(self) -> Result<Vec<Geometry>, Self> {
		if let Self::Collection(v) = self {
			Ok(v)
		} else {
			Err(self)
		}
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
			Geometry::Line(v) => v.lines().all(|l| {
				(-90.0..=90.0).contains(&l.start.y)
					&& (-180.0..=180.0).contains(&l.start.x)
					&& (-90.0..=90.0).contains(&l.end.y)
					&& (-180.0..=180.0).contains(&l.end.x)
			}),
			Geometry::Polygon(v) => polygon_lines(v).all(|l| {
				(-90.0..=90.0).contains(&l.start.y)
					&& (-180.0..=180.0).contains(&l.start.x)
					&& (-90.0..=90.0).contains(&l.end.y)
					&& (-180.0..=180.0).contains(&l.end.x)
			}),
			Geometry::MultiLine(v) => v.iter().all(|l| {
				l.lines().all(|l| {
					(-90.0..=90.0).contains(&l.start.y)
						&& (-180.0..=180.0).contains(&l.start.x)
						&& (-90.0..=90.0).contains(&l.end.y)
						&& (-180.0..=180.0).contains(&l.end.x)
				})
			}),
			Geometry::MultiPolygon(v) => v.iter().all(|p| {
				polygon_lines(p).all(|l| {
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
	/// The kind a `geometry<…>` type names this geometry by.
	pub fn geometry_kind(&self) -> GeometryKind {
		match self {
			Self::Point(_) => GeometryKind::Point,
			Self::Line(_) => GeometryKind::Line,
			Self::Polygon(_) => GeometryKind::Polygon,
			Self::MultiPoint(_) => GeometryKind::MultiPoint,
			Self::MultiLine(_) => GeometryKind::MultiLine,
			Self::MultiPolygon(_) => GeometryKind::MultiPolygon,
			Self::Collection(_) => GeometryKind::Collection,
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
		let mut obj = Object::default();
		obj.insert("type", self.as_type().into());
		if let Self::Collection(members) = self {
			// A collection's members are geometry values, not their raw
			// coordinates, so reading one back yields a geometry.
			obj.insert(
				"geometries",
				members.iter().cloned().map(Value::Geometry).collect::<Vec<Value>>().into(),
			);
		} else {
			obj.insert("coordinates", self.as_coordinates());
		}
		obj
	}

	/// The geometry a GeoJSON object describes: exactly a `type` and its
	/// `coordinates` (or `geometries`, for a collection, whose members are
	/// geometries or GeoJSON objects in turn). `None` for any other object.
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
					let geom = if let Value::Geometry(x) = x {
						x.clone()
					} else if let Value::Object(x) = x {
						Geometry::try_from_object(x)?
					} else {
						return None;
					};
					res.push(geom);
				}

				Some(Geometry::Collection(res))
			}

			_ => None,
		}
	}

	/// Converts a surreal value to a MultiPolygon if the array matches to a
	/// MultiPolygon.
	pub fn array_to_multipolygon(v: &Value) -> Option<MultiPolygon<f64>> {
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
	pub fn array_to_multiline(v: &Value) -> Option<MultiLineString<f64>> {
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
	pub fn array_to_multipoint(v: &Value) -> Option<MultiPoint<f64>> {
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
	pub fn array_to_polygon(v: &Value) -> Option<Polygon<f64>> {
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
	pub fn array_to_line(v: &Value) -> Option<LineString<f64>> {
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
	pub fn array_to_point(v: &Value) -> Option<Point<f64>> {
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
		Some(Point::from((a.clone().try_into().ok()?, b.clone().try_into().ok()?)))
	}

	// -----------------------------------
	// Value operations
	// -----------------------------------

	/// Whether this geometry contains `other`. Needs the `geo` feature.
	#[cfg(feature = "geo")]
	pub fn contains(&self, other: &Self) -> bool {
		match self {
			Self::Point(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::MultiPoint(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				Self::Line(_) | Self::Polygon(_) | Self::MultiLine(_) | Self::MultiPolygon(_) => {
					false
				}
			},
			Self::Line(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::MultiLine(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				Self::Polygon(_) | Self::MultiPoint(_) | Self::MultiPolygon(_) => false,
			},
			Self::Polygon(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::Polygon(w) => v.contains(w),
				Self::MultiPolygon(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				Self::MultiPoint(_) | Self::MultiLine(_) => false,
			},
			Self::MultiPoint(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::MultiPoint(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				Self::Line(_) | Self::Polygon(_) | Self::MultiLine(_) | Self::MultiPolygon(_) => {
					false
				}
			},
			Self::MultiLine(v) => match other {
				Self::Point(w) => v.contains(w),
				Self::Line(w) => v.contains(w),
				Self::MultiLine(w) => w.iter().all(|x| v.contains(x)),
				Self::Collection(w) => w.iter().all(|x| self.contains(x)),
				Self::Polygon(_) | Self::MultiPoint(_) | Self::MultiPolygon(_) => false,
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

	/// Whether this geometry intersects `other`. Needs the `geo` feature.
	#[cfg(feature = "geo")]
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

/// Prepares a stored polygon ring for [`Polygon::new`].
///
/// `Polygon::new` closes a ring whose ends are not `==`, and a coordinate that
/// holds a NaN is never `==` to anything, itself included. A ring whose first
/// coordinate holds a NaN is therefore stored with its closing coordinate, a
/// bit-identical copy of the first, and would gain another on every read. A
/// ring of at least two coordinates whose first holds a NaN and whose last is
/// bit-identical to it loses that last coordinate, so `Polygon::new` closes it
/// again with an identical copy and the ring reads back exactly as it was
/// written. Every other ring is returned unchanged.
pub fn ring_before_closing(mut ring: LineString<f64>) -> LineString<f64> {
	let closed_on_nan = match (ring.0.first(), ring.0.last()) {
		(Some(first), Some(last)) if ring.0.len() >= 2 => {
			(first.x.is_nan() || first.y.is_nan())
				&& first.x.to_bits() == last.x.to_bits()
				&& first.y.to_bits() == last.y.to_bits()
		}
		_ => false,
	};
	if closed_on_nan {
		ring.0.pop();
	}
	ring
}

/// Compares two coordinate streams lexicographically, `x` before `y`, with [`cmp_f64`]. A stream
/// that is a proper prefix of the other is ordered first.
fn cmp_coords(
	mut a: impl Iterator<Item = (f64, f64)>,
	mut b: impl Iterator<Item = (f64, f64)>,
) -> Ordering {
	loop {
		match (a.next(), b.next()) {
			(None, None) => return Ordering::Equal,
			(None, Some(_)) => return Ordering::Less,
			(Some(_), None) => return Ordering::Greater,
			(Some((ax, ay)), Some((bx, by))) => {
				match cmp_f64(ax, bx).then_with(|| cmp_f64(ay, by)) {
					Ordering::Equal => {}
					ordering @ (Ordering::Less | Ordering::Greater) => return ordering,
				}
			}
		}
	}
}

impl Geometry {
	/// The position of a geometry's kind in the order: points first, collections last.
	fn kind_rank(&self) -> u8 {
		match self {
			Self::Point(_) => 0,
			Self::Line(_) => 1,
			Self::Polygon(_) => 2,
			Self::MultiPoint(_) => 3,
			Self::MultiLine(_) => 4,
			Self::MultiPolygon(_) => 5,
			Self::Collection(_) => 6,
		}
	}

	/// A total order over geometries: the order [`PartialOrd`] reports.
	///
	/// Geometries compare by kind (point, line, polygon, multipoint, multiline, multipolygon,
	/// collection), then by their coordinates read in storage order (a polygon's interior rings
	/// before its exterior) with [`cmp_f64`] on `x` and then `y`, a shorter prefix first, and
	/// finally by shape: the ring lengths of a polygon, the line lengths of a multiline, the
	/// polygon count and shapes of a multipolygon, and the members, then the member count, of a
	/// collection. Two geometries compare equal exactly when they have the same kind, shape and
	/// coordinates, where `-0.0` equals `0.0` and a NaN equals a NaN of the same bits.
	///
	/// `surrealdb_types::Geometry` implements the same order; the two must agree.
	pub fn total_cmp(&self, other: &Self) -> Ordering {
		fn coord(v: &Coord) -> (f64, f64) {
			v.x_y()
		}

		fn point(v: &Point) -> (f64, f64) {
			coord(&v.0)
		}

		fn line(v: &LineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			v.0.iter().map(coord)
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

		/// The ring count, then every ring length, with the exterior last.
		fn polygon_shape(v: &Polygon) -> impl Iterator<Item = usize> + '_ {
			once(v.interiors().len())
				.chain(v.interiors().iter().map(|ring| ring.0.len()))
				.chain(once(v.exterior().0.len()))
		}

		let ranks = self.kind_rank().cmp(&other.kind_rank());
		if ranks != Ordering::Equal {
			return ranks;
		}
		// Both sides are of one kind from here. The match is exhaustive over `self`, so a new
		// variant does not compile until it has its own comparison; the `let ... else` arms can
		// only be taken for a kind mismatch, which the rank comparison above has already ruled out.
		match self {
			Self::Point(a) => {
				let Self::Point(b) = other else {
					return ranks;
				};
				cmp_coords(once(point(a)), once(point(b)))
			}
			Self::Line(a) => {
				let Self::Line(b) = other else {
					return ranks;
				};
				cmp_coords(line(a), line(b))
			}
			Self::Polygon(a) => {
				let Self::Polygon(b) = other else {
					return ranks;
				};
				cmp_coords(polygon(a), polygon(b))
					.then_with(|| polygon_shape(a).cmp(polygon_shape(b)))
			}
			Self::MultiPoint(a) => {
				let Self::MultiPoint(b) = other else {
					return ranks;
				};
				cmp_coords(multipoint(a), multipoint(b))
			}
			Self::MultiLine(a) => {
				let Self::MultiLine(b) = other else {
					return ranks;
				};
				cmp_coords(multiline(a), multiline(b))
					.then_with(|| a.iter().map(|l| l.0.len()).cmp(b.iter().map(|l| l.0.len())))
			}
			Self::MultiPolygon(a) => {
				let Self::MultiPolygon(b) = other else {
					return ranks;
				};
				cmp_coords(multipolygon(a), multipolygon(b)).then_with(|| {
					once(a.0.len())
						.chain(a.iter().flat_map(polygon_shape))
						.cmp(once(b.0.len()).chain(b.iter().flat_map(polygon_shape)))
				})
			}
			Self::Collection(a) => {
				let Self::Collection(b) = other else {
					return ranks;
				};
				a.iter()
					.zip(b.iter())
					.map(|(x, y)| x.total_cmp(y))
					.find(|ordering| *ordering != Ordering::Equal)
					.unwrap_or_else(|| a.len().cmp(&b.len()))
			}
		}
	}
}

impl PartialOrd for Geometry {
	/// Always `Some`: geometries are totally ordered by [`Geometry::total_cmp`], NaN
	/// coordinates included. Set membership follows this order. The order merges `-0.0` and
	/// `0.0`, as the derived `PartialEq` does, and equates a NaN with a NaN of the same bits,
	/// which the derived `PartialEq` does not, so `==` and the order disagree only for
	/// geometries holding a NaN.
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.total_cmp(other))
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

impl From<Geometry> for geo_types::Geometry<f64> {
	fn from(v: Geometry) -> Self {
		match v {
			Geometry::Point(v) => v.into(),
			Geometry::Line(v) => v.into(),
			Geometry::Polygon(v) => v.into(),
			Geometry::MultiPoint(v) => v.into(),
			Geometry::MultiLine(v) => v.into(),
			Geometry::MultiPolygon(v) => v.into(),
			Geometry::Collection(v) => v.into_iter().collect::<geo_types::Geometry<f64>>(),
		}
	}
}

impl From<geo_types::Geometry<f64>> for Geometry {
	/// Convert from the upstream geometry enum.
	///
	/// `geo`'s enum is a superset of SurrealQL's geometry variants: `Rect` and
	/// `Triangle` convert to their equivalent polygons, and a bare `Line`
	/// (segment) becomes a two-point line string.
	fn from(v: geo_types::Geometry<f64>) -> Self {
		match v {
			geo_types::Geometry::Point(v) => Geometry::Point(v),
			geo_types::Geometry::Line(v) => Geometry::Line(LineString::from(vec![v.start, v.end])),
			geo_types::Geometry::LineString(v) => Geometry::Line(v),
			geo_types::Geometry::Polygon(v) => Geometry::Polygon(v),
			geo_types::Geometry::MultiPoint(v) => Geometry::MultiPoint(v),
			geo_types::Geometry::MultiLineString(v) => Geometry::MultiLine(v),
			geo_types::Geometry::MultiPolygon(v) => Geometry::MultiPolygon(v),
			geo_types::Geometry::GeometryCollection(v) => {
				Geometry::Collection(v.0.into_iter().map(Geometry::from).collect())
			}
			geo_types::Geometry::Rect(v) => Geometry::Polygon(v.to_polygon()),
			geo_types::Geometry::Triangle(v) => Geometry::Polygon(v.to_polygon()),
		}
	}
}

impl FromIterator<Geometry> for geo_types::Geometry<f64> {
	fn from_iter<I: IntoIterator<Item = Geometry>>(iter: I) -> Self {
		let mut c: Vec<geo_types::Geometry<f64>> = vec![];
		for i in iter {
			c.push(i.into())
		}
		geo_types::Geometry::GeometryCollection(geo_types::GeometryCollection(c))
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

#[inline]
fn norm_zero(v: f64) -> f64 {
	if v == 0.0 {
		0.0
	} else {
		v
	}
}

impl hash::Hash for Geometry {
	fn hash<H: hash::Hasher>(&self, state: &mut H) {
		match self {
			Geometry::Point(p) => {
				"Point".hash(state);
				norm_zero(p.x()).to_bits().hash(state);
				norm_zero(p.y()).to_bits().hash(state);
			}
			Geometry::Line(l) => {
				"Line".hash(state);
				l.points().for_each(|v| {
					norm_zero(v.x()).to_bits().hash(state);
					norm_zero(v.y()).to_bits().hash(state);
				});
			}
			Geometry::Polygon(p) => {
				"Polygon".hash(state);
				p.exterior().points().for_each(|ext| {
					norm_zero(ext.x()).to_bits().hash(state);
					norm_zero(ext.y()).to_bits().hash(state);
				});
				p.interiors().iter().for_each(|int| {
					int.points().for_each(|v| {
						norm_zero(v.x()).to_bits().hash(state);
						norm_zero(v.y()).to_bits().hash(state);
					});
				});
			}
			Geometry::MultiPoint(v) => {
				"MultiPoint".hash(state);
				v.0.iter().for_each(|v| {
					norm_zero(v.x()).to_bits().hash(state);
					norm_zero(v.y()).to_bits().hash(state);
				});
			}
			Geometry::MultiLine(ml) => {
				"MultiLine".hash(state);
				ml.0.iter().for_each(|ls| {
					ls.points().for_each(|p| {
						norm_zero(p.x()).to_bits().hash(state);
						norm_zero(p.y()).to_bits().hash(state);
					});
				});
			}
			Geometry::MultiPolygon(mp) => {
				"MultiPolygon".hash(state);
				mp.0.iter().for_each(|p| {
					p.exterior().points().for_each(|ext| {
						norm_zero(ext.x()).to_bits().hash(state);
						norm_zero(ext.y()).to_bits().hash(state);
					});
					p.interiors().iter().for_each(|int| {
						int.points().for_each(|v| {
							norm_zero(v.x()).to_bits().hash(state);
							norm_zero(v.y()).to_bits().hash(state);
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
				Encode::<F>::encode(&norm_zero(point.x()), w)?;
				Encode::<F>::encode(&norm_zero(point.y()), w)?;
			}
			Geometry::Line(line_string) => {
				w.write_u8(3)?;
				for p in line_string.points() {
					w.mark_terminator();
					Encode::<F>::encode(&norm_zero(p.x()), w)?;
					Encode::<F>::encode(&norm_zero(p.y()), w)?;
				}
				w.write_terminator()?;
			}
			Geometry::Polygon(polygon) => {
				w.write_u8(4)?;
				for p in polygon.exterior().points() {
					w.mark_terminator();
					Encode::<F>::encode(&norm_zero(p.x()), w)?;
					Encode::<F>::encode(&norm_zero(p.y()), w)?;
				}
				w.write_terminator()?;
				for l in polygon.interiors() {
					for p in l.points() {
						w.mark_terminator();
						Encode::<F>::encode(&norm_zero(p.x()), w)?;
						Encode::<F>::encode(&norm_zero(p.y()), w)?;
					}
					w.write_terminator()?;
				}
				w.write_terminator()?;
			}
			Geometry::MultiPoint(multi_point) => {
				w.write_u8(5)?;
				for p in multi_point.iter() {
					w.mark_terminator();
					Encode::<F>::encode(&norm_zero(p.x()), w)?;
					Encode::<F>::encode(&norm_zero(p.y()), w)?;
				}
				w.write_terminator()?;
			}
			Geometry::MultiLine(multi_line_string) => {
				w.write_u8(6)?;
				for l in multi_line_string.iter() {
					for p in l.points() {
						w.mark_terminator();
						Encode::<F>::encode(&norm_zero(p.x()), w)?;
						Encode::<F>::encode(&norm_zero(p.y()), w)?;
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
						Encode::<F>::encode(&norm_zero(p.x()), w)?;
						Encode::<F>::encode(&norm_zero(p.y()), w)?;
					}
					w.write_terminator()?;
					for l in p.interiors() {
						for p in l.points() {
							w.mark_terminator();
							Encode::<F>::encode(&norm_zero(p.x()), w)?;
							Encode::<F>::encode(&norm_zero(p.y()), w)?;
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

/// Every polygon ring goes through [`ring_before_closing`] before [`Polygon::new`] closes it, so a
/// ring whose first coordinate holds a NaN decodes to the ring that was encoded.
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
				let ext = ring_before_closing(LineString::new(ext));
				let mut int = Vec::new();
				while !r.read_terminal()? {
					let mut line = Vec::new();
					while !r.read_terminal()? {
						let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
						line.push(Coord::from(point))
					}
					int.push(ring_before_closing(LineString::new(line)))
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
					let ext = ring_before_closing(LineString::new(ext));
					let mut int = Vec::new();
					while !r.read_terminal()? {
						let mut line = Vec::new();
						while !r.read_terminal()? {
							let point = <(f64, f64) as BorrowDecode<'de, F>>::borrow_decode(r)?;
							line.push(Coord::from(point))
						}
						int.push(ring_before_closing(LineString::new(line)))
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

/// The segments of a polygon's exterior ring and then of each interior ring.
fn polygon_lines(polygon: &Polygon) -> impl Iterator<Item = Line> + '_ {
	polygon.exterior().lines().chain(polygon.interiors().iter().flat_map(LineString::lines))
}

#[cfg(test)]
mod test {
	use core::f64;

	use geo_types::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

	use super::Geometry;

	fn to_public(geometry: &Geometry) -> surrealdb_types::Geometry {
		use surrealdb_types::Geometry as Public;
		match geometry.clone() {
			Geometry::Point(v) => Public::Point(v),
			Geometry::Line(v) => Public::Line(v),
			Geometry::Polygon(v) => Public::Polygon(v),
			Geometry::MultiPoint(v) => Public::MultiPoint(v),
			Geometry::MultiLine(v) => Public::MultiLine(v),
			Geometry::MultiPolygon(v) => Public::MultiPolygon(v),
			Geometry::Collection(v) => Public::Collection(v.iter().map(to_public).collect()),
		}
	}

	/// The geometry order is kept in `surrealdb_types` and here, and the two must agree on
	/// every pair, NaN payloads, signed zeros and infinities included: a value's order does not
	/// depend on which representation compares it.
	#[test]
	fn the_order_agrees_with_the_public_geometry_order() {
		let nan_b = f64::from_bits(f64::NAN.to_bits() ^ 1);
		let floats = [0.0, -0.0, 1.0, -1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN, nan_b];
		let mut cases = Vec::new();
		for &x in &floats {
			for &y in &[0.0, f64::NAN, 2.5] {
				cases.push(Geometry::Point(Point::new(x, y)));
			}
		}
		let ring = |x: f64| {
			LineString::new(vec![
				Coord::from((x, 0.0)),
				Coord::from((1.0, 0.0)),
				Coord::from((1.0, 1.0)),
				Coord::from((x, 0.0)),
			])
		};
		for &x in &floats {
			cases.push(Geometry::Line(LineString::new(vec![Coord::from((x, 1.0))])));
			cases.push(Geometry::Line(ring(x)));
			cases.push(Geometry::Polygon(Polygon::new(ring(x), vec![])));
			cases.push(Geometry::Polygon(Polygon::new(ring(0.0), vec![ring(x)])));
			cases.push(Geometry::MultiPoint(MultiPoint(vec![Point::new(x, 0.0)])));
			cases.push(Geometry::MultiLine(MultiLineString(vec![ring(x), ring(0.0)])));
			cases.push(Geometry::MultiPolygon(MultiPolygon(vec![Polygon::new(ring(x), vec![])])));
		}
		cases.push(Geometry::Line(LineString::new(vec![])));
		cases.push(Geometry::MultiPoint(MultiPoint(vec![])));
		cases.push(Geometry::Collection(vec![]));
		let leaves = cases.clone();
		for pair in leaves.chunks(5) {
			cases.push(Geometry::Collection(pair.to_vec()));
		}
		cases.push(Geometry::Collection(vec![Geometry::Collection(leaves[..3].to_vec())]));
		let public: Vec<_> = cases.iter().map(to_public).collect();
		for (i, a) in cases.iter().enumerate() {
			for (j, b) in cases.iter().enumerate() {
				assert_eq!(
					a.partial_cmp(b),
					public[i].partial_cmp(&public[j]),
					"{a:?} against {b:?}"
				);
			}
		}
	}

	fn round_trip(geom: &Geometry) {
		let enc = storekey::encode_vec(geom).unwrap();
		let dec = storekey::decode_borrow(&enc).unwrap();
		assert_eq!(geom, &dec)
	}

	#[test]
	fn encode_decode() {
		round_trip(&Geometry::Point(Point::new(0.0, f64::INFINITY)));
		round_trip(&Geometry::Line(LineString::new(vec![
			Coord::from((0.0, f64::INFINITY)),
			Coord::from((f64::NEG_INFINITY, 1.0)),
		])));
		round_trip(&Geometry::Polygon(Polygon::new(
			LineString::new(vec![
				Coord::from((0.0, f64::INFINITY)),
				Coord::from((f64::NEG_INFINITY, 1.0)),
			]),
			vec![
				LineString::new(vec![Coord::from((1.0, 2.0)), Coord::from((3.0, 4.0))]),
				LineString::new(vec![Coord::from((5.0, 6.0)), Coord::from((7.0, 8.0))]),
			],
		)));
		round_trip(&Geometry::MultiPoint(MultiPoint::new(vec![
			Point::from((0.0, f64::INFINITY)),
			Point::from((f64::NEG_INFINITY, 1.0)),
		])));
		round_trip(&Geometry::MultiLine(MultiLineString::new(vec![
			LineString::new(vec![
				Coord::from((0.0, f64::INFINITY)),
				Coord::from((f64::NEG_INFINITY, 1.0)),
			]),
			LineString::new(vec![Coord::from((1.0, 2.0)), Coord::from((3.0, 4.0))]),
		])));
		round_trip(&Geometry::MultiPolygon(MultiPolygon::new(vec![
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

	#[cfg(feature = "geo")]
	#[test]
	fn contains_checks_receiver_not_argument_tautology() {
		let line_a = Geometry::Line(LineString::from(vec![(0.0, 0.0), (10.0, 0.0)]));
		let ml_inside = Geometry::MultiLine(MultiLineString::new(vec![LineString::from(vec![
			(2.0, 0.0),
			(5.0, 0.0),
		])]));
		let ml_outside = Geometry::MultiLine(MultiLineString::new(vec![LineString::from(vec![
			(20.0, 20.0),
			(30.0, 20.0),
		])]));
		assert!(line_a.contains(&ml_inside));
		assert!(!line_a.contains(&ml_outside));
		assert!(ml_inside.contains(&ml_inside));
		assert!(!ml_inside.contains(&ml_outside));

		let poly_a = Geometry::Polygon(Polygon::new(
			LineString::from(vec![(0.0, 0.0), (10.0, 0.0), (10.0, 10.0), (0.0, 10.0), (0.0, 0.0)]),
			vec![],
		));
		let mp_inside = Geometry::MultiPolygon(MultiPolygon::new(vec![Polygon::new(
			LineString::from(vec![(2.0, 2.0), (5.0, 2.0), (5.0, 5.0), (2.0, 5.0), (2.0, 2.0)]),
			vec![],
		)]));
		let mp_outside = Geometry::MultiPolygon(MultiPolygon::new(vec![Polygon::new(
			LineString::from(vec![
				(20.0, 20.0),
				(30.0, 20.0),
				(30.0, 30.0),
				(20.0, 30.0),
				(20.0, 20.0),
			]),
			vec![],
		)]));
		assert!(poly_a.contains(&mp_inside));
		assert!(!poly_a.contains(&mp_outside));

		let mpt_a =
			Geometry::MultiPoint(MultiPoint::new(vec![Point::new(1.0, 1.0), Point::new(2.0, 2.0)]));
		let mpt_subset = Geometry::MultiPoint(MultiPoint::new(vec![Point::new(1.0, 1.0)]));
		let mpt_disjoint = Geometry::MultiPoint(MultiPoint::new(vec![Point::new(9.0, 9.0)]));
		assert!(mpt_a.contains(&mpt_subset));
		assert!(!mpt_a.contains(&mpt_disjoint));
	}

	#[test]
	fn negative_zero_coordinate_matches_positive_zero_in_hash_and_storekey() {
		use std::collections::hash_map::DefaultHasher;
		use std::hash::{Hash, Hasher};

		let neg = Geometry::Point(Point::new(-0.0, 1.0));
		let pos = Geometry::Point(Point::new(0.0, 1.0));
		assert_eq!(neg, pos);

		let hash_of = |g: &Geometry| {
			let mut h = DefaultHasher::new();
			g.hash(&mut h);
			h.finish()
		};
		assert_eq!(hash_of(&neg), hash_of(&pos));
		assert_eq!(storekey::encode_vec(&neg).unwrap(), storekey::encode_vec(&pos).unwrap());
	}
}
