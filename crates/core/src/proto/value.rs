
use std::collections::BTreeMap;

use crate::proto::surrealdb::value::{
    Value as ValueProto,
    Array as ArrayProto,
    Object as ObjectProto,
    Geometry as GeometryProto,
    File as FileProto,
    RecordId as RecordIdProto,
    Id as IdProto,
    Point as PointProto,
    LineString as LineStringProto,
    Polygon as PolygonProto,
    MultiPoint as MultiPointProto,
    MultiLineString as MultiLineStringProto,
    MultiPolygon as MultiPolygonProto,
    GeometryCollection as GeometryCollectionProto,
};




impl TryFrom<ValueProto> for crate::expr::Value {
	type Error = anyhow::Error;

	fn try_from(proto: ValueProto) -> Result<Self, Self::Error> {
        use crate::proto::surrealdb::value::value;

		let Some(inner) = proto.inner else {
			return Ok(crate::expr::Value::None);
		};

		let value = match inner {
			value::Inner::Null(_) => crate::expr::Value::Null,
			value::Inner::Bool(v) => crate::expr::Value::Bool(v),
            value::Inner::Int(v) => crate::expr::Value::Number(v.into()),
            value::Inner::Float(v) => crate::expr::Value::Number(v.into()),
			value::Inner::Decimal(v) => crate::expr::Value::Number(
                crate::expr::Number::Decimal(crate::sql::DecimalExt::from_str_normalized(&v)?)
            ),
			value::Inner::Strand(v) => crate::expr::Value::Strand(v.into()),
			value::Inner::Duration(v) => crate::expr::Value::Duration(v.into()),
			value::Inner::Datetime(v) => crate::expr::Value::Datetime(v.try_into()?),
			value::Inner::Uuid(v) => crate::expr::Value::Uuid(v.try_into()?),
			value::Inner::Array(v) => {
			    crate::expr::Value::Array(v.try_into()?)
			}
			value::Inner::Object(v) => {
			    crate::expr::Value::Object(v.try_into()?)
			}
			value::Inner::Geometry(v) => crate::expr::Value::Geometry(v.try_into()?),
			value::Inner::Bytes(v) => crate::expr::Value::Bytes(v.into()),
			value::Inner::RecordId(v) => crate::expr::Value::Thing(v.try_into()?),
			value::Inner::File(v) => crate::expr::Value::File(v.try_into()?),
		};

		Ok(value)
	}
}

impl From<super::google::protobuf::Duration> for crate::expr::Duration {
	fn from(proto: super::google::protobuf::Duration) -> Self {
		crate::expr::Duration(std::time::Duration::from_nanos(
			proto.seconds as u64 * 1_000_000_000 + proto.nanos as u64,
		))
	}
}

impl TryFrom<super::google::protobuf::Timestamp> for crate::expr::Datetime {
	type Error = anyhow::Error;
	fn try_from(proto: super::google::protobuf::Timestamp) -> Result<Self, Self::Error> {
		Ok(crate::expr::Datetime(proto.try_into()?))
	}
}

impl TryFrom<ArrayProto> for crate::expr::Array {
    type Error = anyhow::Error;

    fn try_from(proto: ArrayProto) -> Result<Self, Self::Error> {
        let mut items = Vec::with_capacity(proto.values.len());
        for item in proto.values {
            items.push(crate::expr::Value::try_from(item)?);
        }
        Ok(crate::expr::Array(items))
    }
}

impl TryFrom<ObjectProto> for crate::expr::Object {
    type Error = anyhow::Error;

    fn try_from(proto: ObjectProto) -> Result<Self, Self::Error> {
        let mut object = BTreeMap::new();
        for (key, value) in proto.values {
            object.insert(key, crate::expr::Value::try_from(value)?);
        }
        Ok(crate::expr::Object(object))
    }
}

impl TryFrom<GeometryProto> for crate::expr::Geometry {
    type Error = anyhow::Error;

    fn try_from(proto: GeometryProto) -> Result<Self, Self::Error> {
        use crate::proto::surrealdb::value::geometry;

        let Some(inner) = proto.inner else {
            return Err(anyhow::anyhow!("Invalid Geometry: missing value"));
        };

        let geometry = match inner {
            geometry::Inner::Point(v) => crate::expr::Geometry::Point(v.into()),
            geometry::Inner::Line(v) => crate::expr::Geometry::Line(v.into()),
            geometry::Inner::Polygon(v) => crate::expr::Geometry::Polygon(v.try_into()?),
            geometry::Inner::MultiPoint(v) => crate::expr::Geometry::MultiPoint(v.into()),
            geometry::Inner::MultiLine(v) => crate::expr::Geometry::MultiLine(v.into()),
            geometry::Inner::MultiPolygon(v) => crate::expr::Geometry::MultiPolygon(v.try_into()?),
            geometry::Inner::Collection(v) => crate::expr::Geometry::Collection(v.try_into()?),
        };

        Ok(geometry)
    }
}

impl From<PointProto> for geo::Coord<f64> {
    fn from(proto: PointProto) -> Self {
        Self {
            x: proto.x,
            y: proto.y,
        }
    }
}

impl From<PointProto> for geo::Point<f64> {
    fn from(proto: PointProto) -> Self {
        Self::new(proto.x, proto.y)
    }
}

impl From<LineStringProto> for geo::LineString<f64> {
    fn from(proto: LineStringProto) -> Self {
        Self(
            proto.points.into_iter().map(Into::into).collect()
        )
    }
}

impl TryFrom<PolygonProto> for geo::Polygon<f64> {
    type Error = anyhow::Error;

    fn try_from(proto: PolygonProto) -> Result<Self, Self::Error> {
        let Some(exterior) = proto.exterior else {
            return Err(anyhow::anyhow!("Invalid Polygon: missing exterior"));
        };
        let interiors = proto.interiors.into_iter().map(Into::into).collect();
        Ok(Self::new(exterior.into(), interiors))
    }
}

impl From<MultiPointProto> for geo::MultiPoint<f64> {
    fn from(proto: MultiPointProto) -> Self {
        Self(
            proto.points.into_iter().map(Into::into).collect()
        )
    }
}

impl From<MultiLineStringProto> for geo::MultiLineString<f64> {
    fn from(proto: MultiLineStringProto) -> Self {
        Self(
            proto.lines.into_iter().map(Into::into).collect()
        )
    }
}

impl TryFrom<MultiPolygonProto> for geo::MultiPolygon<f64> {
    type Error = anyhow::Error;

    fn try_from(proto: MultiPolygonProto) -> Result<Self, Self::Error> {
        Ok(Self(
            proto.polygons.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>, _>>()?
        ))
    }
}
impl TryFrom<GeometryCollectionProto> for Vec<crate::expr::Geometry> {
    type Error = anyhow::Error;

    fn try_from(proto: GeometryCollectionProto) -> Result<Self, Self::Error> {
        let mut geometries = Vec::with_capacity(proto.geometries.len());
        for geometry in proto.geometries {
            geometries.push(crate::expr::Geometry::try_from(geometry)?);
        }
        Ok(geometries)
    }
}

impl TryFrom<RecordIdProto> for crate::expr::Thing {
    type Error = anyhow::Error;

    fn try_from(proto: RecordIdProto) -> Result<Self, Self::Error> {
        let Some(id) = proto.id else {
            return Err(anyhow::anyhow!("Invalid RecordId: missing id"));
        };
        Ok(Self {
            tb: proto.table,
            id: id.try_into()?,
        })
    }
}

impl From<FileProto> for crate::expr::File {
    fn from(proto: FileProto) -> Self {
        Self {
            bucket: proto.bucket,
            key: proto.key,
        }
    }
}


impl TryFrom<IdProto> for crate::expr::Id {
    type Error = anyhow::Error;

    fn try_from(proto: IdProto) -> Result<Self, Self::Error> {
        use crate::proto::surrealdb::value::id;
        let Some(inner) = proto.inner else {
            return Err(anyhow::anyhow!("Invalid Id: missing value"));
        };

        Ok(match inner {
            id::Inner::Number(v) => crate::expr::Id::Number(v),
            id::Inner::String(v) => crate::expr::Id::String(v),
            id::Inner::Uuid(v) => crate::expr::Id::Uuid(v.try_into()?),
            id::Inner::Array(v) => crate::expr::Id::Array(v.try_into()?),
        })
    }
}