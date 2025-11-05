use std::fmt::Display;

use serde::{Deserialize, Serialize};

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

impl Display for GeometryKind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			GeometryKind::Point => write!(f, "point"),
			GeometryKind::Line => write!(f, "line"),
			GeometryKind::Polygon => write!(f, "polygon"),
			GeometryKind::MultiPoint => write!(f, "multipoint"),
			GeometryKind::MultiLine => write!(f, "multiline"),
			GeometryKind::MultiPolygon => write!(f, "multipolygon"),
			GeometryKind::Collection => write!(f, "collection"),
		}
	}
}
