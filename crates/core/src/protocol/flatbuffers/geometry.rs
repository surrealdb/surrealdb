use anyhow::Context;
use surrealdb_protocol::fb::v1 as proto_fb;

use crate::protocol::{FromFlatbuffers, ToFlatbuffers};
use crate::val::Geometry;

impl ToFlatbuffers for Geometry {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Geometry<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		match self {
			Geometry::Point(point) => {
				let geometry = point.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Point,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::Line(line_string) => {
				let geometry = line_string.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::LineString,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::Polygon(polygon) => {
				let geometry = polygon.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Polygon,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::MultiPoint(multi_point) => {
				let geometry = multi_point.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiPoint,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::MultiLine(multi_line_string) => {
				let geometry = multi_line_string.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiLineString,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::MultiPolygon(multi_polygon) => {
				let geometry = multi_polygon.to_fb(builder)?;
				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::MultiPolygon,
						geometry: Some(geometry.as_union_value()),
					},
				))
			}
			Geometry::Collection(geometries) => {
				let mut geometries_vec = Vec::with_capacity(geometries.len());
				for geometry in geometries {
					geometries_vec.push(geometry.to_fb(builder)?);
				}
				let geometries_vector = builder.create_vector(&geometries_vec);

				let collection = proto_fb::GeometryCollection::create(
					builder,
					&proto_fb::GeometryCollectionArgs {
						geometries: Some(geometries_vector),
					},
				);

				Ok(proto_fb::Geometry::create(
					builder,
					&proto_fb::GeometryArgs {
						geometry_type: proto_fb::GeometryType::Collection,
						geometry: Some(collection.as_union_value()),
					},
				))
			}
		}
	}
}

impl FromFlatbuffers for Geometry {
	type Input<'a> = proto_fb::Geometry<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		match input.geometry_type() {
			proto_fb::GeometryType::Point => {
				let point = input
					.geometry_as_point()
					.ok_or_else(|| anyhow::anyhow!("Expected Point geometry"))?;
				Ok(Geometry::Point(geo::Point::from_fb(point)?))
			}
			proto_fb::GeometryType::LineString => {
				let line_string = input
					.geometry_as_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected LineString geometry"))?;
				Ok(Geometry::Line(geo::LineString::from_fb(line_string)?))
			}
			proto_fb::GeometryType::Polygon => {
				let polygon = input
					.geometry_as_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected Polygon geometry"))?;
				Ok(Geometry::Polygon(geo::Polygon::from_fb(polygon)?))
			}
			proto_fb::GeometryType::MultiPoint => {
				let multi_point = input
					.geometry_as_multi_point()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPoint geometry"))?;
				Ok(Geometry::MultiPoint(geo::MultiPoint::from_fb(multi_point)?))
			}
			proto_fb::GeometryType::MultiLineString => {
				let multi_line_string = input
					.geometry_as_multi_line_string()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiLineString geometry"))?;
				Ok(Geometry::MultiLine(geo::MultiLineString::from_fb(multi_line_string)?))
			}
			proto_fb::GeometryType::MultiPolygon => {
				let multi_polygon = input
					.geometry_as_multi_polygon()
					.ok_or_else(|| anyhow::anyhow!("Expected MultiPolygon geometry"))?;
				Ok(Geometry::MultiPolygon(geo::MultiPolygon::from_fb(multi_polygon)?))
			}
			proto_fb::GeometryType::Collection => {
				let collection = input
					.geometry_as_collection()
					.ok_or_else(|| anyhow::anyhow!("Expected GeometryCollection"))?;
				let geometries_reader = collection.geometries().context("Geometries is not set")?;
				let mut geometries = Vec::with_capacity(geometries_reader.len());
				for geometry in geometries_reader {
					geometries.push(Geometry::from_fb(geometry)?);
				}
				Ok(Geometry::Collection(geometries))
			}
			_ => Err(anyhow::anyhow!(
				"Unsupported geometry type for FlatBuffers deserialization: {:?}",
				input.geometry_type()
			)),
		}
	}
}

impl ToFlatbuffers for geo::Point {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Point::create(
			builder,
			&proto_fb::PointArgs {
				x: self.x(),
				y: self.y(),
			},
		))
	}
}

impl FromFlatbuffers for geo::Point {
	type Input<'a> = proto_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Point::new(input.x(), input.y()))
	}
}

impl ToFlatbuffers for geo::Coord {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Point<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		Ok(proto_fb::Point::create(
			builder,
			&proto_fb::PointArgs {
				x: self.x,
				y: self.y,
			},
		))
	}
}

impl FromFlatbuffers for geo::Coord {
	type Input<'a> = proto_fb::Point<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		Ok(geo::Coord {
			x: input.x(),
			y: input.y(),
		})
	}
}

impl ToFlatbuffers for geo::LineString {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::LineString<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut points = Vec::with_capacity(self.0.len());
		for point in &self.0 {
			points.push(point.to_fb(builder)?);
		}
		let points_vector = builder.create_vector(&points);
		Ok(proto_fb::LineString::create(
			builder,
			&proto_fb::LineStringArgs {
				points: Some(points_vector),
			},
		))
	}
}

impl FromFlatbuffers for geo::LineString {
	type Input<'a> = proto_fb::LineString<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut points = Vec::new();
		for point in input.points().context("Points is not set")? {
			points.push(geo::Coord::from_fb(point)?);
		}
		Ok(Self(points))
	}
}

impl ToFlatbuffers for geo::Polygon {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::Polygon<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let exterior = self.exterior().to_fb(builder)?;
		let mut interiors = Vec::with_capacity(self.interiors().len());
		for interior in self.interiors() {
			interiors.push(interior.to_fb(builder)?);
		}
		let interiors_vector = builder.create_vector(&interiors);
		Ok(proto_fb::Polygon::create(
			builder,
			&proto_fb::PolygonArgs {
				exterior: Some(exterior),
				interiors: Some(interiors_vector),
			},
		))
	}
}

impl FromFlatbuffers for geo::Polygon {
	type Input<'a> = proto_fb::Polygon<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let exterior =
			input.exterior().ok_or_else(|| anyhow::anyhow!("Missing exterior in Polygon"))?;
		let exterior = geo::LineString::from_fb(exterior)?;

		let mut interiors = Vec::new();
		if let Some(interiors_reader) = input.interiors() {
			for interior in interiors_reader {
				interiors.push(geo::LineString::from_fb(interior)?);
			}
		}

		Ok(Self::new(exterior, interiors))
	}
}

impl ToFlatbuffers for geo::MultiPoint {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiPoint<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut points = Vec::with_capacity(self.0.len());
		for point in &self.0 {
			points.push(point.to_fb(builder)?);
		}
		let points_vector = builder.create_vector(&points);
		Ok(proto_fb::MultiPoint::create(
			builder,
			&proto_fb::MultiPointArgs {
				points: Some(points_vector),
			},
		))
	}
}

impl FromFlatbuffers for geo::MultiPoint {
	type Input<'a> = proto_fb::MultiPoint<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut points = Vec::new();
		for point in input.points().context("Points is not set")? {
			points.push(geo::Point::from_fb(point)?);
		}
		Ok(Self(points))
	}
}

impl ToFlatbuffers for geo::MultiLineString {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiLineString<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut lines = Vec::with_capacity(self.0.len());
		for line in &self.0 {
			lines.push(line.to_fb(builder)?);
		}
		let lines_vector = builder.create_vector(&lines);
		Ok(proto_fb::MultiLineString::create(
			builder,
			&proto_fb::MultiLineStringArgs {
				lines: Some(lines_vector),
			},
		))
	}
}

impl FromFlatbuffers for geo::MultiLineString {
	type Input<'a> = proto_fb::MultiLineString<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut lines = Vec::new();
		for line in input.lines().context("Lines is not set")? {
			lines.push(geo::LineString::from_fb(line)?);
		}
		Ok(Self(lines))
	}
}

impl ToFlatbuffers for geo::MultiPolygon {
	type Output<'bldr> = flatbuffers::WIPOffset<proto_fb::MultiPolygon<'bldr>>;

	#[inline]
	fn to_fb<'bldr>(
		&self,
		builder: &mut flatbuffers::FlatBufferBuilder<'bldr>,
	) -> anyhow::Result<Self::Output<'bldr>> {
		let mut polygons = Vec::with_capacity(self.0.len());
		for polygon in &self.0 {
			polygons.push(polygon.to_fb(builder)?);
		}
		let polygons_vector = builder.create_vector(&polygons);
		Ok(proto_fb::MultiPolygon::create(
			builder,
			&proto_fb::MultiPolygonArgs {
				polygons: Some(polygons_vector),
			},
		))
	}
}

impl FromFlatbuffers for geo::MultiPolygon {
	type Input<'a> = proto_fb::MultiPolygon<'a>;

	#[inline]
	fn from_fb(input: Self::Input<'_>) -> anyhow::Result<Self> {
		let mut polygons = Vec::new();
		for polygon in input.polygons().context("Polygons is not set")? {
			polygons.push(geo::Polygon::from_fb(polygon)?);
		}
		Ok(Self(polygons))
	}
}
