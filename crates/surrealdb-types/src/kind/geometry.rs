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
