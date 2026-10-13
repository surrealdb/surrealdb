use anyhow::anyhow;
use surrealdb_protocol::proto::v1 as proto;
use surrealdb_protocol::proto::v1::geometry::Geometry as GeometryInner;

use crate::Geometry;

impl From<Geometry> for proto::Geometry {
	fn from(value: Geometry) -> Self {
		match value {
			Geometry::Point(p) => proto::Geometry::point(p.into()),
			Geometry::Line(l) => proto::Geometry::line(l.into()),
			Geometry::Polygon(p) => proto::Geometry::polygon(p.into()),
			Geometry::MultiPoint(m) => proto::Geometry::multi_point(m.into()),
			Geometry::MultiLine(m) => proto::Geometry::multi_line(m.into()),
			Geometry::MultiPolygon(m) => proto::Geometry::multi_polygon(m.into()),
			Geometry::Collection(geometries) => {
				proto::Geometry::collection(proto::GeometryCollection {
					geometries: geometries.into_iter().map(Into::into).collect(),
				})
			}
		}
	}
}

impl TryFrom<proto::Geometry> for Geometry {
	type Error = anyhow::Error;

	fn try_from(value: proto::Geometry) -> Result<Self, Self::Error> {
		let inner = value.geometry.ok_or_else(|| anyhow!("Missing geometry variant"))?;
		match inner {
			GeometryInner::Point(p) => Ok(Geometry::Point(p.into())),
			GeometryInner::Line(l) => Ok(Geometry::Line(l.into())),
			GeometryInner::Polygon(p) => Ok(Geometry::Polygon(p.try_into()?)),
			GeometryInner::MultiPoint(m) => Ok(Geometry::MultiPoint(m.into())),
			GeometryInner::MultiLine(m) => Ok(Geometry::MultiLine(m.into())),
			GeometryInner::MultiPolygon(m) => Ok(Geometry::MultiPolygon(m.try_into()?)),
			GeometryInner::Collection(collection) => {
				let geometries = collection
					.geometries
					.into_iter()
					.map(Geometry::try_from)
					.collect::<anyhow::Result<Vec<_>>>()?;
				Ok(Geometry::Collection(geometries))
			}
		}
	}
}
