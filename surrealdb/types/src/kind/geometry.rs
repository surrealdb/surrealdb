use std::fmt::Display;

use serde::{Deserialize, Serialize};

use crate::{SqlFormat, ToSql};

/// Represents different types of geometric shapes in SurrealDB's type system
///
/// This enum defines the various geometry types that can be used in type definitions
/// and schema validation.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub enum GeometryKind {
	/// A single point in 2D space
	Point,
	/// A line consisting of multiple connected points
	Line,
	/// A polygon with an exterior boundary and optional interior holes
	Polygon,
	/// Multiple points
	MultiPoint,
	/// Multiple lines
	MultiLine,
	/// Multiple polygons
	MultiPolygon,
	/// A collection of different geometry types
	Collection,
}

impl ToSql for GeometryKind {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		match self {
			GeometryKind::Point => f.push_str("point"),
			GeometryKind::Line => f.push_str("line"),
			GeometryKind::Polygon => f.push_str("polygon"),
			GeometryKind::MultiPoint => f.push_str("multipoint"),
			GeometryKind::MultiLine => f.push_str("multiline"),
			GeometryKind::MultiPolygon => f.push_str("multipolygon"),
			GeometryKind::Collection => f.push_str("collection"),
		}
	}
}

impl Display for GeometryKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str(&self.to_sql())
	}
}
