#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum KindGeometry {
	Point,
	Line,
	Polygon,
	MultiPoint,
	MultiLine,
	MultiPolygon,
	Collection,
}