#![allow(clippy::derived_hash_with_manual_eq)]

use crate::sql::comment::mightbespace;
use crate::sql::common::commas;
use crate::sql::error::IResult;
use crate::sql::fmt::Fmt;
use crate::sql::serde::is_internal_serialization;
use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::{Coord, LineString, Point, Polygon};
use geo::{MultiLineString, MultiPoint, MultiPolygon};
use nom::branch::alt;
use nom::bytes::complete::tag;
use nom::character::complete::char;
use nom::combinator::opt;
use nom::multi::separated_list0;
use nom::multi::separated_list1;
use nom::number::complete::double;
use nom::sequence::delimited;
use nom::sequence::preceded;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::iter::{once, FromIterator};
use std::{fmt, hash};

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
	#[rustfmt::skip]
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		fn coord(coord: &Coord) -> (f64, f64) {
			coord.x_y()
		}

		fn point(point: &Point) -> (f64, f64) {
			coord(&point.0)
		}

		fn line(line: &LineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			line.into_iter().map(coord)
		}

		fn polygon(polygon: &Polygon) -> impl Iterator<Item = (f64, f64)> + '_ {
			polygon.interiors().iter().chain(once(polygon.exterior())).flat_map(line)
		}

		fn multi_point(multi_point: &MultiPoint) -> impl Iterator<Item = (f64, f64)> + '_ {
			multi_point.iter().map(point)
		}

		fn multi_line(multi_line: &MultiLineString) -> impl Iterator<Item = (f64, f64)> + '_ {
			multi_line.iter().flat_map(line)
		}

		fn multi_polygon(multi_polygon: &MultiPolygon) -> impl Iterator<Item = (f64, f64)> + '_ {
			multi_polygon.iter().flat_map(polygon)
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
			(Self::MultiPoint(a), Self::MultiPoint(b)) => multi_point(a).partial_cmp(multi_point(b)),
			(Self::MultiLine(a), Self::MultiLine(b)) => multi_line(a).partial_cmp(multi_line(b)),
			(Self::MultiPolygon(a), Self::MultiPolygon(b)) => multi_polygon(a).partial_cmp(multi_polygon(b)),
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

impl Geometry {
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

impl fmt::Display for Geometry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Point(v) => {
				write!(f, "({}, {})", v.x(), v.y())
			}
			Self::Line(v) => write!(
				f,
				"{{ type: 'LineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"[{}, {}]",
					v.x(),
					v.y()
				))))
			),
			Self::Polygon(v) => write!(
				f,
				"{{ type: 'Polygon', coordinates: [[{}]{}] }}",
				Fmt::comma_separated(v.exterior().points().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"[{}, {}]",
					v.x(),
					v.y()
				)))),
				Fmt::new(v.interiors(), |interiors, f| {
					match interiors.len() {
						0 => Ok(()),
						_ => write!(
							f,
							", [{}]",
							Fmt::comma_separated(interiors.iter().map(|i| Fmt::new(i, |i, f| {
								write!(
									f,
									"[{}]",
									Fmt::comma_separated(i.points().map(|v| Fmt::new(
										v,
										|v, f| write!(f, "[{}, {}]", v.x(), v.y())
									)))
								)
							})))
						),
					}
				})
			),
			Self::MultiPoint(v) => {
				write!(
					f,
					"{{ type: 'MultiPoint', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
						f,
						"[{}, {}]",
						v.x(),
						v.y()
					))))
				)
			}
			Self::MultiLine(v) => write!(
				f,
				"{{ type: 'MultiLineString', coordinates: [{}] }}",
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| write!(
					f,
					"[{}]",
					Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
						f,
						"[{}, {}]",
						v.x(),
						v.y()
					))))
				))))
			),
			Self::MultiPolygon(v) => write!(
				f,
				"{{ type: 'MultiPolygon', coordinates: [{}] }}",
				Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| {
					write!(
						f,
						"[[{}]{}]",
						Fmt::comma_separated(
							v.exterior().points().map(|v| Fmt::new(v, |v, f| write!(
								f,
								"[{}, {}]",
								v.x(),
								v.y()
							)))
						),
						Fmt::new(v.interiors(), |interiors, f| {
							match interiors.len() {
								0 => Ok(()),
								_ => write!(
									f,
									", [{}]",
									Fmt::comma_separated(interiors.iter().map(|i| Fmt::new(
										i,
										|i, f| {
											write!(
												f,
												"[{}]",
												Fmt::comma_separated(i.points().map(|v| Fmt::new(
													v,
													|v, f| write!(f, "[{}, {}]", v.x(), v.y())
												)))
											)
										}
									)))
								),
							}
						})
					)
				}))),
			),
			Self::Collection(v) => {
				write!(
					f,
					"{{ type: 'GeometryCollection', geometries: [{}] }}",
					Fmt::comma_separated(v)
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
				Self::Point(v) => s.serialize_newtype_variant("Geometry", 0, "Point", v),
				Self::Line(v) => s.serialize_newtype_variant("Geometry", 1, "Line", v),
				Self::Polygon(v) => s.serialize_newtype_variant("Geometry", 2, "Polygon", v),
				Self::MultiPoint(v) => s.serialize_newtype_variant("Geometry", 3, "MultiPoint", v),
				Self::MultiLine(v) => s.serialize_newtype_variant("Geometry", 4, "MultiLine", v),
				Self::MultiPolygon(v) => {
					s.serialize_newtype_variant("Geometry", 5, "MultiPolygon", v)
				}
				Self::Collection(v) => s.serialize_newtype_variant("Geometry", 6, "Collection", v),
			}
		} else {
			match self {
				Self::Point(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("Point")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(vec![v.x(), v.y()].as_slice())?;
					map.end()
				}
				Self::Line(v) => {
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
				Self::Polygon(v) => {
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
				Self::MultiPoint(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiPoint")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(
						v.0.iter()
							.map(|v| vec![v.x(), v.y()])
							.collect::<Vec<Vec<f64>>>()
							.as_slice(),
					)?;
					map.end()
				}
				Self::MultiLine(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiLineString")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(
						v.0.iter()
							.map(|v| {
								v.points().map(|v| vec![v.x(), v.y()]).collect::<Vec<Vec<f64>>>()
							})
							.collect::<Vec<Vec<Vec<f64>>>>()
							.as_slice(),
					)?;
					map.end()
				}
				Self::MultiPolygon(v) => {
					let mut map = s.serialize_map(Some(2))?;
					map.serialize_key("type")?;
					map.serialize_value("MultiPolygon")?;
					map.serialize_key("coordinates")?;
					map.serialize_value(
						v.0.iter()
							.map(|v| {
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
							})
							.collect::<Vec<Vec<Vec<Vec<f64>>>>>()
							.as_slice(),
					)?;
					map.end()
				}
				Self::Collection(v) => {
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

pub fn geometry(i: &str) -> IResult<&str, Geometry> {
	alt((simple, point, line, polygon, multipoint, multiline, multipolygon, collection))(i)
}

fn simple(i: &str) -> IResult<&str, Geometry> {
	let (i, _) = char('(')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, x) = double(i)?;
	let (i, _) = commas(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, point_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, point_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, line_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, line_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, polygon_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, polygon_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipoint_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multipoint_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multiline_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multiline_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_vals, multipolygon_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, multipolygon_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
			let (i, _) = commas(i)?;
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
			Ok((i, v))
		},
		|i| {
			let (i, v) = preceded(key_geom, collection_vals)(i)?;
			let (i, _) = commas(i)?;
			let (i, _) = preceded(key_type, collection_type)(i)?;
			let (i, _) = mightbespace(i)?;
			let (i, _) = opt(char(','))(i)?;
			let (i, _) = mightbespace(i)?;
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
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v.into()))
}

fn polygon_vals(i: &str) -> IResult<&str, Polygon<f64>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, e) = line_vals(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	let (i, v) = separated_list0(commas, |i| {
		let (i, _) = char('[')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, v) = line_vals(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = opt(char(','))(i)?;
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
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn multiline_vals(i: &str) -> IResult<&str, Vec<LineString<f64>>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, line_vals)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn multipolygon_vals(i: &str) -> IResult<&str, Vec<Polygon<f64>>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, polygon_vals)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = char(']')(i)?;
	Ok((i, v))
}

fn collection_vals(i: &str) -> IResult<&str, Vec<Geometry>> {
	let (i, _) = char('[')(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, v) = separated_list1(commas, geometry)(i)?;
	let (i, _) = mightbespace(i)?;
	let (i, _) = opt(char(','))(i)?;
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
