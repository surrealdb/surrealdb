use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::serde::is_internal_serialization;
use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::{LineString, Point, Polygon};
use geo::{MultiLineString, MultiPoint, MultiPolygon};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::multi::separated_list0;
use nom::multi::separated_list1;
use nom::number::complete::double;
use nom::sequence::delimited;
use nom::sequence::preceded;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::iter::FromIterator;

const SINGLE: char = '\'';
const DOUBLE: char = '\"';

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub enum Geometry {
	Point(Point<f64>),
	Line(LineString<f64>),
	Polygon(Polygon<f64>),
	MultiPoint(MultiPoint<f64>),
	MultiLine(MultiLineString<f64>),
	MultiPolygon(MultiPolygon<f64>),
	Collection(Vec<Geometry>),
}

impl PartialOrd for Geometry {
	#[inline]
	fn partial_cmp(&self, _: &Self) -> Option<Ordering> {
		None
	}
}

impl From<(f64, f64)> for Geometry {
	fn from(v: (f64, f64)) -> Self {
		Geometry::Point(v.into())
	}
}

impl From<[f64; 2]> for Geometry {
	fn from(v: [f64; 2]) -> Self {
		Geometry::Point(v.into())
	}
}

impl From<Point<f64>> for Geometry {
	fn from(v: Point<f64>) -> Self {
		Geometry::Point(v)
	}
}

impl From<LineString<f64>> for Geometry {
	fn from(v: LineString<f64>) -> Self {
		Geometry::Line(v)
	}
}

impl From<Polygon<f64>> for Geometry {
	fn from(v: Polygon<f64>) -> Self {
		Geometry::Polygon(v)
	}
}

impl From<MultiPoint<f64>> for Geometry {
	fn from(v: MultiPoint<f64>) -> Self {
		Geometry::MultiPoint(v)
	}
}

impl From<MultiLineString<f64>> for Geometry {
	fn from(v: MultiLineString<f64>) -> Self {
		Geometry::MultiLine(v)
	}
}

impl From<MultiPolygon<f64>> for Geometry {
	fn from(v: MultiPolygon<f64>) -> Self {
		Geometry::MultiPolygon(v)
	}
}

impl From<Vec<Geometry>> for Geometry {
	fn from(v: Vec<Geometry>) -> Self {
		Geometry::Collection(v)
	}
}

impl From<Vec<Point<f64>>> for Geometry {
	fn from(v: Vec<Point<f64>>) -> Self {
		Geometry::MultiPoint(MultiPoint(v))
	}
}

impl From<Vec<LineString<f64>>> for Geometry {
	fn from(v: Vec<LineString<f64>>) -> Self {
		Geometry::MultiLine(MultiLineString(v))
	}
}

impl From<Vec<Polygon<f64>>> for Geometry {
	fn from(v: Vec<Polygon<f64>>) -> Self {
		Geometry::MultiPolygon(MultiPolygon(v))
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

impl Geometry {
	// -----------------------------------
	// Value operations
	// -----------------------------------

	pub fn contains(&self, other: &Geometry) -> bool {
		match self {
			Geometry::Point(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::MultiPoint(w) => w.iter().all(|x| v.contains(x)),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Geometry::Line(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::Line(w) => v.contains(w),
				Geometry::MultiLine(w) => w.iter().all(|x| w.contains(x)),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Geometry::Polygon(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::Line(w) => v.contains(w),
				Geometry::Polygon(w) => v.contains(w),
				Geometry::MultiPolygon(w) => w.iter().all(|x| w.contains(x)),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Geometry::MultiPoint(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::MultiPoint(w) => w.iter().all(|x| w.contains(x)),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Geometry::MultiLine(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::Line(w) => v.contains(w),
				Geometry::MultiLine(w) => w.iter().all(|x| w.contains(x)),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
				_ => false,
			},
			Geometry::MultiPolygon(v) => match other {
				Geometry::Point(w) => v.contains(w),
				Geometry::Line(w) => v.contains(w),
				Geometry::Polygon(w) => v.contains(w),
				Geometry::MultiPoint(w) => v.contains(w),
				Geometry::MultiLine(w) => v.contains(w),
				Geometry::MultiPolygon(w) => v.contains(w),
				Geometry::Collection(w) => w.iter().all(|x| self.contains(x)),
			},
			Geometry::Collection(v) => v.iter().all(|x| x.contains(other)),
		}
	}

	pub fn intersects(&self, other: &Geometry) -> bool {
		match self {
			Geometry::Point(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::Line(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::Polygon(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => v.intersects(w),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::MultiPoint(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::MultiLine(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => w.iter().any(|x| v.intersects(x)),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::MultiPolygon(v) => match other {
				Geometry::Point(w) => v.intersects(w),
				Geometry::Line(w) => v.intersects(w),
				Geometry::Polygon(w) => v.intersects(w),
				Geometry::MultiPoint(w) => v.intersects(w),
				Geometry::MultiLine(w) => v.intersects(w),
				Geometry::MultiPolygon(w) => v.intersects(w),
				Geometry::Collection(w) => w.iter().all(|x| self.intersects(x)),
			},
			Geometry::Collection(v) => v.iter().all(|x| x.intersects(other)),
		}
	}
}

impl fmt::Display for Geometry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Geometry::Point(v) => {
				write!(f, "({}, {})", v.x(), v.y())
			}
			Geometry::Line(v) => write!(
				f,
				"{{ type: 'LineString', coordinates: [{}] }}",
				v.points()
					.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Geometry::Polygon(v) => write!(
				f,
				"{{ type: 'Polygon', coordinates: [[{}]{}] }}",
				v.exterior()
					.points()
					.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
					.collect::<Vec<_>>()
					.join(", "),
				match v.interiors().len() {
					0 => String::new(),
					_ => format!(
						", [{}]",
						v.interiors()
							.iter()
							.map(|i| {
								format!(
									"[{}]",
									i.points()
										.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
										.collect::<Vec<_>>()
										.join(", ")
								)
							})
							.collect::<Vec<_>>()
							.join(", "),
					),
				}
			),
			Geometry::MultiPoint(v) => {
				write!(
					f,
					"{{ type: 'MultiPoint', coordinates: [{}] }}",
					v.iter()
						.map(|v| format!("[{}, {}]", v.x(), v.y()))
						.collect::<Vec<_>>()
						.join(", ")
				)
			}
			Geometry::MultiLine(v) => write!(
				f,
				"{{ type: 'MultiLineString', coordinates: [{}] }}",
				v.iter()
					.map(|v| format!(
						"[{}]",
						v.points()
							.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
							.collect::<Vec<_>>()
							.join(", ")
					))
					.collect::<Vec<_>>()
					.join(", ")
			),
			Geometry::MultiPolygon(v) => write!(
				f,
				"{{ type: 'MultiPolygon', coordinates: [{}] }}",
				v.iter()
					.map(|v| format!(
						"[[{}]{}]",
						v.exterior()
							.points()
							.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
							.collect::<Vec<_>>()
							.join(", "),
						match v.interiors().len() {
							0 => String::new(),
							_ => format!(
								", [{}]",
								v.interiors()
									.iter()
									.map(|i| {
										format!(
											"[{}]",
											i.points()
												.map(|ref v| format!("[{}, {}]", v.x(), v.y()))
												.collect::<Vec<_>>()
												.join(", ")
										)
									})
									.collect::<Vec<_>>()
									.join(", "),
							),
						}
					))
					.collect::<Vec<_>>()
					.join(", "),
			),
			Geometry::Collection(v) => {
				write!(
					f,
					"{{ type: 'GeometryCollection', geometries: [{}] }}",
					v.iter().map(|v| format!("{}", v)).collect::<Vec<_>>().join(", ")
				)
			}
		}
	}
}

impl Serialize for Geometry {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		if is_internal_serialization() {
			match self {
				Geometry::Point(v) => s.serialize_newtype_variant("Geometry", 0, "Point", v),
				Geometry::Line(v) => s.serialize_newtype_variant("Geometry", 1, "Line", v),
				Geometry::Polygon(v) => s.serialize_newtype_variant("Geometry", 2, "Polygon", v),
				Geometry::MultiPoint(v) => s.serialize_newtype_variant("Geometry", 3, "Points", v),
				Geometry::MultiLine(v) => s.serialize_newtype_variant("Geometry", 4, "Lines", v),
				Geometry::MultiPolygon(v) => {
					s.serialize_newtype_variant("Geometry", 5, "Polygons", v)
				}
				Geometry::Collection(v) => {
					s.serialize_newtype_variant("Geometry", 6, "Collection", v)
				}
			}
		} else {
			match self {
				Geometry::Point(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("Point")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(vec![v.x(), v.y()].as_slice())?;
					map.end()
				}
				Geometry::Line(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("LineString")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(
						v.points()
							.map(|p| vec![p.x(), p.y()])
							.collect::<Vec<Vec<f64>>>()
							.as_slice(),
					)?;
					map.end()
				}
				Geometry::Polygon(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("Polygon")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(
						vec![v
							.exterior()
							.points()
							.map(|p| vec![p.x(), p.y()])
							.collect::<Vec<Vec<f64>>>()]
						.into_iter()
						.chain(
							v.interiors()
								.iter()
								.map(|i| {
									i.points()
										.map(|p| vec![p.x(), p.y()])
										.collect::<Vec<Vec<f64>>>()
								})
								.collect::<Vec<Vec<Vec<f64>>>>(),
						)
						.collect::<Vec<Vec<Vec<f64>>>>()
						.as_slice(),
					)?;
					map.end()
				}
				Geometry::MultiPoint(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiPoint")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(v.0.as_slice())?;
					map.end()
				}
				Geometry::MultiLine(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiLineString")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(v.0.as_slice())?;
					map.end()
				}
				Geometry::MultiPolygon(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiPolygon")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(v.0.as_slice())?;
					map.end()
				}
				Geometry::Collection(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("GeometryCollection")?;
					map.serialize_key("geometries")?;
					map.serialize_value(v)?;
					map.end()
				}
			}
		}
	}
}

pub fn geometry(i: &str) -> IResult<&str, Geometry> {
	alt((simple, point, line, polygon, multipoint, multiline, multipolygon, collection))(i)
}

fn simple(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, x) = double(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(',')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, y) = double(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(')')(i)?;
	Ok((i, Geometry::Point((x, y).into())))
}

fn point(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, point_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, point_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn line(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, line_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, line_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn polygon(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, polygon_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, polygon_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn multipoint(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multipoint_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, multipoint_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn multiline(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multiline_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, multiline_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn multipolygon(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, multipolygon_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, multipolygon_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

fn collection(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('{')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = alt((
		|i| {
			let (i, _) = preceded(key_type, collection_type)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			let (i, _) = delimited(mightbespace, char(','), mightbespace)(i)?;
			let (i, _) = preceded(key_type, collection_type)(i)?;
			Ok((i, v))
		},
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char('}')(i)?;
	Ok((i, v.into()))
}

//
//
//

fn point_vals(i: &str) -> IResult<&str, Point<f64>> {
	let (i, v) = coordinate(i)?;
	Ok((i, v.into()))
}

fn line_vals(i: &str) -> IResult<&str, LineString<f64>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, coordinate)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v.into()))
}

fn polygon_vals(i: &str) -> IResult<&str, Polygon<f64>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, e) = line_vals(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	let (i, v) = separated_list0(commas, |i| {
		let (i, _) = char('[')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = line_vals(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(']')(i)?;
		Ok((i, v))
	})(i)?;
	Ok((i, Polygon::new(e, v)))
}

fn multipoint_vals(i: &str) -> IResult<&str, Vec<Point<f64>>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, point_vals)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn multiline_vals(i: &str) -> IResult<&str, Vec<LineString<f64>>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, line_vals)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn multipolygon_vals(i: &str) -> IResult<&str, Vec<Polygon<f64>>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, polygon_vals)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn collection_vals(i: &str) -> IResult<&str, Vec<Geometry>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, geometry)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

//
//
//

fn coordinate(i: &str) -> IResult<&str, (f64, f64)> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, x) = double(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(',')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, y) = double(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, (x, y)))
}

//
//
//

fn point_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("Point"), char(SINGLE)),
		delimited(char(DOUBLE), tag("Point"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn line_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("LineString"), char(SINGLE)),
		delimited(char(DOUBLE), tag("LineString"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn polygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("Polygon"), char(SINGLE)),
		delimited(char(DOUBLE), tag("Polygon"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multipoint_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiPoint"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiPoint"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multiline_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiLineString"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiLineString"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn multipolygon_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("MultiPolygon"), char(SINGLE)),
		delimited(char(DOUBLE), tag("MultiPolygon"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

fn collection_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		delimited(char(SINGLE), tag("GeometryCollection"), char(SINGLE)),
		delimited(char(DOUBLE), tag("GeometryCollection"), char(DOUBLE)),
	))(i)?;
	Ok((i, v))
}

//
//
//

fn key_type(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("type"),
		delimited(char(SINGLE), tag("type"), char(SINGLE)),
		delimited(char(DOUBLE), tag("type"), char(DOUBLE)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_vals(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("coordinates"),
		delimited(char(SINGLE), tag("coordinates"), char(SINGLE)),
		delimited(char(DOUBLE), tag("coordinates"), char(DOUBLE)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

fn key_geom(i: &str) -> IResult<&str, &str> {
	let (i, v) = alt((
		tag("geometries"),
		delimited(char(SINGLE), tag("geometries"), char(SINGLE)),
		delimited(char(DOUBLE), tag("geometries"), char(DOUBLE)),
	))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(':')(i)?;
	let (i, _) = mightbespace(i)?;
	Ok((i, v))
}

#[cfg(test)]
mod tests {

	use super::*;

	#[test]
	fn simple() {
		let sql = "(51.509865, -0.118092)";
		let res = geometry(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("(51.509865, -0.118092)", format!("{}", out));
	}
}
