#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum KindGeometry {
    Feature,
	Point,
	Line,
	Polygon,
	MultiPoint,
	MultiLine,
	MultiPolygon,
	Collection,
}