#![allow(clippy::derived_hash_with_manual_eq)]

use crate::sql::fmt::Fmt;
use geo::algorithm::contains::Contains;
use geo::algorithm::intersects::Intersects;
use geo::{Coord, LineString, LinesIter, Point, Polygon};
use geo_types::{MultiLineString, MultiPoint, MultiPolygon};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::iter::once;
use std::{fmt, hash};

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

impl From<Geometry> for crate::val::Geometry {
	fn from(v: Geometry) -> Self {
		match v {
			Geometry::Point(v) => Self::Point(v),
			Geometry::Line(v) => Self::Line(v),
			Geometry::Polygon(v) => Self::Polygon(v),
			Geometry::MultiPoint(v) => Self::MultiPoint(v),
			Geometry::MultiLine(v) => Self::MultiLine(v),
			Geometry::MultiPolygon(v) => Self::MultiPolygon(v),
			Geometry::Collection(v) => Self::Collection(v.into_iter().map(Into::into).collect()),
		}
	}
}

impl From<crate::val::Geometry> for Geometry {
	fn from(v: crate::val::Geometry) -> Self {
		match v {
			crate::val::Geometry::Point(v) => Self::Point(v),
			crate::val::Geometry::Line(v) => Self::Line(v),
			crate::val::Geometry::Polygon(v) => Self::Polygon(v),
			crate::val::Geometry::MultiPoint(v) => Self::MultiPoint(v),
			crate::val::Geometry::MultiLine(v) => Self::MultiLine(v),
			crate::val::Geometry::MultiPolygon(v) => Self::MultiPolygon(v),
			crate::val::Geometry::Collection(v) => {
				Self::Collection(v.into_iter().map(Into::into).collect())
			}
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
				"{{ type: 'Polygon', coordinates: [{}] }}",
				Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(|v| Fmt::new(
					v,
					|v, f| write!(
						f,
						"[{}]",
						Fmt::comma_separated(v.points().map(|v| Fmt::new(v, |v, f| write!(
							f,
							"[{}, {}]",
							v.x(),
							v.y()
						))))
					)
				)))
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
			Self::MultiPolygon(v) => {
				write!(
					f,
					"{{ type: 'MultiPolygon', coordinates: [{}] }}",
					Fmt::comma_separated(v.iter().map(|v| Fmt::new(v, |v, f| {
						write!(
							f,
							"[{}]",
							Fmt::comma_separated(once(v.exterior()).chain(v.interiors()).map(
								|v| Fmt::new(v, |v, f| write!(
									f,
									"[{}]",
									Fmt::comma_separated(v.points().map(|v| Fmt::new(
										v,
										|v, f| write!(f, "[{}, {}]", v.x(), v.y())
									)))
								))
							))
						)
					}))),
				)
			}
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
