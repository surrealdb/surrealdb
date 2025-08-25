use std::cmp::Ordering;
use std::fmt::Display;
use std::hash;
use std::iter::once;

use geo::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use serde::{Deserialize, Serialize};

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

impl Display for Geometry {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Point(v) => {
				write!(f, "({}, {})", v.x(), v.y())
			}
			Self::Line(v) => {
				write!(f, "{{ type: 'LineString', coordinates: [")?;
				fmt_points(f, v.points())?;
				write!(f, "] }}")
			}
			Self::Polygon(v) => {
				write!(f, "{{ type: 'Polygon', coordinates: [")?;
				fmt_rings(f, once(v.exterior()).chain(v.interiors()))?;
				write!(f, "] }}")
			}
			Self::MultiPoint(v) => {
				write!(f, "{{ type: 'MultiPoint', coordinates: [")?;
				fmt_points(f, v.iter().copied())?;
				write!(f, "] }}")
			}
			Self::MultiLine(v) => {
				write!(f, "{{ type: 'MultiLineString', coordinates: [")?;
				fmt_lines(f, v.iter())?;
				write!(f, "] }}")
			}
			Self::MultiPolygon(v) => {
				write!(f, "{{ type: 'MultiPolygon', coordinates: [")?;
				fmt_polygons(f, v.iter())?;
				write!(f, "] }}")
			}
			Self::Collection(v) => {
				write!(f, "{{ type: 'GeometryCollection', geometries: [")?;
				fmt_geometries(f, v.iter())?;
				write!(f, "] }}")
			}
		}
	}
}

fn fmt_points<I>(f: &mut std::fmt::Formatter<'_>, mut points: I) -> std::fmt::Result
where
	I: Iterator<Item = Point<f64>>,
{
	if let Some(first) = points.next() {
		write!(f, "[{}, {}]", first.x(), first.y())?;
		for point in points {
			write!(f, ", [{}, {}]", point.x(), point.y())?;
		}
	}
	Ok(())
}

fn fmt_rings<'a, I>(f: &mut std::fmt::Formatter<'_>, mut rings: I) -> std::fmt::Result
where
	I: Iterator<Item = &'a LineString<f64>>,
{
	if let Some(first) = rings.next() {
		write!(f, "[")?;
		fmt_points(f, first.points())?;
		write!(f, "]")?;
		for ring in rings {
			write!(f, ", [")?;
			fmt_points(f, ring.points())?;
			write!(f, "]")?;
		}
	}
	Ok(())
}

fn fmt_lines<'a, I>(f: &mut std::fmt::Formatter<'_>, mut lines: I) -> std::fmt::Result
where
	I: Iterator<Item = &'a LineString<f64>>,
{
	if let Some(first) = lines.next() {
		write!(f, "[")?;
		fmt_points(f, first.points())?;
		write!(f, "]")?;
		for line in lines {
			write!(f, ", [")?;
			fmt_points(f, line.points())?;
			write!(f, "]")?;
		}
	}
	Ok(())
}

fn fmt_polygons<'a, I>(f: &mut std::fmt::Formatter<'_>, mut polygons: I) -> std::fmt::Result
where
	I: Iterator<Item = &'a Polygon<f64>>,
{
	if let Some(first) = polygons.next() {
		write!(f, "[")?;
		fmt_rings(f, once(first.exterior()).chain(first.interiors()))?;
		write!(f, "]")?;
		for polygon in polygons {
			write!(f, ", [")?;
			fmt_rings(f, once(polygon.exterior()).chain(polygon.interiors()))?;
			write!(f, "]")?;
		}
	}
	Ok(())
}

fn fmt_geometries<'a, I>(f: &mut std::fmt::Formatter<'_>, mut geometries: I) -> std::fmt::Result
where
	I: Iterator<Item = &'a Geometry>,
{
	if let Some(first) = geometries.next() {
		write!(f, "{}", first)?;
		for geometry in geometries {
			write!(f, ", {}", geometry)?;
		}
	}
	Ok(())
}
