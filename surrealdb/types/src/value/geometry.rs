use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash;
use std::iter::once;

use geo_types::{
	Coord, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon,
};
use serde::{Deserialize, Serialize};

use crate::sql::{SqlFormat, ToSql};
use crate::value::number::cmp_f64;
use crate::{GeometryKind, Object, SurrealValue, Value, array, object};

/// Represents geometric shapes in SurrealDB
///
/// Geometry types support various geometric shapes including points, lines, polygons,
/// and their multi-variants. This is useful for spatial data and geographic applications.
///
/// The types used internally originate from the `geo` crate.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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

impl Display for Geometry {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		use crate::value::format::F;

		match self {
			Self::Point(v) => {
				write!(f, "({}, {})", F(v.x()), F(v.y()))
			}
			Self::Line(v) => {
				write!(f, "{{ type: 'LineString', coordinates: [")?;
				for (i, point) in v.points().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					write!(f, "[{}, {}]", F(point.x()), F(point.y()))?;
				}
				write!(f, "] }}")
			}
			Self::Polygon(v) => {
				write!(f, "{{ type: 'Polygon', coordinates: [")?;
				for (ring_idx, ring) in once(v.exterior()).chain(v.interiors()).enumerate() {
					if ring_idx > 0 {
						write!(f, ", ")?;
					}
					write!(f, "[")?;
					for (i, point) in ring.points().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "[{}, {}]", F(point.x()), F(point.y()))?;
					}
					write!(f, "]")?;
				}
				write!(f, "] }}")
			}
			Self::MultiPoint(v) => {
				write!(f, "{{ type: 'MultiPoint', coordinates: [")?;
				for (i, point) in v.iter().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					write!(f, "[{}, {}]", F(point.x()), F(point.y()))?;
				}
				write!(f, "] }}")
			}
			Self::MultiLine(v) => {
				write!(f, "{{ type: 'MultiLineString', coordinates: [")?;
				for (line_idx, line) in v.iter().enumerate() {
					if line_idx > 0 {
						write!(f, ", ")?;
					}
					write!(f, "[")?;
					for (i, point) in line.points().enumerate() {
						if i > 0 {
							write!(f, ", ")?;
						}
						write!(f, "[{}, {}]", F(point.x()), F(point.y()))?;
					}
					write!(f, "]")?;
				}
				write!(f, "] }}")
			}
			Self::MultiPolygon(v) => {
				write!(f, "{{ type: 'MultiPolygon', coordinates: [")?;
				for (poly_idx, polygon) in v.iter().enumerate() {
					if poly_idx > 0 {
						write!(f, ", ")?;
					}
					write!(f, "[")?;
					for (ring_idx, ring) in
						once(polygon.exterior()).chain(polygon.interiors()).enumerate()
					{
						if ring_idx > 0 {
							write!(f, ", ")?;
						}
						write!(f, "[")?;
						for (i, point) in ring.points().enumerate() {
							if i > 0 {
								write!(f, ", ")?;
							}
							write!(f, "[{}, {}]", F(point.x()), F(point.y()))?;
						}
						write!(f, "]")?;
					}
					write!(f, "]")?;
				}
				write!(f, "] }}")
			}
			Self::Collection(v) => {
				write!(f, "{{ type: 'GeometryCollection', geometries: [")?;
				for (i, geom) in v.iter().enumerate() {
					if i > 0 {
						write!(f, ", ")?;
					}
					write!(f, "{}", geom)?;
				}
				write!(f, "] }}")
			}
		}
	}
}

impl ToSql for Geometry {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str(&self.to_string());
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

	/// Get the GeoJSON object representation for this geometry: its `type` and
	/// its `coordinates`, or for a collection its `geometries`, whose members
	/// are geometry values. [`Geometry::try_from_object`] reads it back.
	pub fn as_object(&self) -> Object {
		if let Self::Collection(members) = self {
			object! {
				type: Value::String(self.as_type().to_string()),
				geometries: members.iter().cloned().map(Value::Geometry).collect::<Vec<Value>>(),
			}
		} else {
			object! {
				type: Value::String(self.as_type().to_string()),
				coordinates: self.as_coordinates(),
			}
		}
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
	/// The server's own geometry type implements the same order; the two must agree.
	pub(crate) fn total_cmp(&self, other: &Self) -> Ordering {
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
	/// Always `Some`: geometries are totally ordered, NaN coordinates included. Set membership
	/// follows this order. The order merges `-0.0` and `0.0`, as the derived `PartialEq` does, and
	/// equates a NaN with a NaN of the same bits, which the derived `PartialEq` does not, so `==`
	/// and the order disagree only for geometries holding a NaN.
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.total_cmp(other))
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

impl From<Geometry> for geo_types::Geometry<f64> {
	fn from(v: Geometry) -> Self {
		match v {
			Geometry::Point(v) => v.into(),
			Geometry::Line(v) => v.into(),
			Geometry::Polygon(v) => v.into(),
			Geometry::MultiPoint(v) => v.into(),
			Geometry::MultiLine(v) => v.into(),
			Geometry::MultiPolygon(v) => v.into(),
			Geometry::Collection(v) => geo_types::Geometry::GeometryCollection(
				v.into_iter()
					.map(geo_types::Geometry::from)
					.collect::<geo_types::GeometryCollection<f64>>(),
			),
		}
	}
}

/// The segments of a polygon's exterior ring and then of each interior ring.
fn polygon_lines(polygon: &Polygon) -> impl Iterator<Item = Line> + '_ {
	polygon.exterior().lines().chain(polygon.interiors().iter().flat_map(LineString::lines))
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A geometry weighs its coordinates, however deep a collection holds them.
	#[test]
	fn a_geometry_weighs_its_coordinates() {
		let points: Vec<(f64, f64)> = (0..100_000).map(|i| (i as f64, i as f64)).collect();
		let coords = points.len() * std::mem::size_of::<Coord<f64>>();
		let line = Geometry::Line(LineString::from(points));
		assert!(line.heap_bytes() >= coords);
		let nested = Geometry::Collection(vec![Geometry::Collection(vec![line.clone()])]);
		assert!(nested.heap_bytes() > line.heap_bytes());
		assert_eq!(Geometry::Point(Point::new(1.0, 2.0)).heap_bytes(), 0);
	}

	fn collection() -> Geometry {
		Geometry::Collection(vec![
			Geometry::Point(Point::new(1.0, 2.0)),
			Geometry::Line(LineString::from(vec![(0.0, 0.0), (1.0, 1.0)])),
			Geometry::Collection(vec![Geometry::Point(Point::new(5.0, 6.0))]),
		])
	}

	#[test]
	fn collection_object_holds_member_geometries() {
		let object = collection().as_object();
		assert_eq!(object.get("type"), Some(&Value::String("GeometryCollection".to_string())));
		assert_eq!(object.get("coordinates"), None);
		let Some(Value::Array(members)) = object.get("geometries") else {
			panic!("a collection's object holds its members under `geometries`");
		};
		assert_eq!(members.len(), 3);
		assert_eq!(members[0], Value::Geometry(Geometry::Point(Point::new(1.0, 2.0))));
	}

	#[test]
	fn object_round_trips_every_kind() {
		let geometries = [
			Geometry::Point(Point::new(1.0, 2.0)),
			Geometry::Line(LineString::from(vec![(0.0, 0.0), (1.0, 1.0)])),
			Geometry::MultiPoint(MultiPoint::new(vec![Point::new(1.0, 2.0)])),
			collection(),
		];
		for geometry in geometries {
			assert_eq!(Geometry::try_from_object(&geometry.as_object()), Some(geometry));
		}
	}

	#[test]
	fn collection_members_may_be_geojson_objects() {
		let object = object! {
			type: "GeometryCollection",
			geometries: vec![
				Value::Object(object! { type: "Point", coordinates: vec![1.0, 2.0] }),
				Value::Geometry(Geometry::Point(Point::new(3.0, 4.0))),
			],
		};
		assert_eq!(
			Geometry::try_from_object(&object),
			Some(Geometry::Collection(vec![
				Geometry::Point(Point::new(1.0, 2.0)),
				Geometry::Point(Point::new(3.0, 4.0)),
			]))
		);
		let bad = object! {
			type: "GeometryCollection",
			geometries: vec![Value::String("x".to_string())],
		};
		assert_eq!(Geometry::try_from_object(&bad), None);
	}
}
