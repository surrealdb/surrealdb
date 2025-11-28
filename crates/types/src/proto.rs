//! Conversions between surrealdb-types and protobuf types
//!
//! This module provides From trait implementations for converting between
//! surrealdb-types Value, Variables, and other types and their protobuf
//! equivalents from the surrealdb-protocol crate.

use std::collections::BTreeMap;
use std::ops::Bound;

use anyhow::{Context, Result};
use surrealdb_protocol::proto::v1 as proto;

use crate::{
	Array, Bytes, Datetime, Duration, File, Geometry, Number, Object, Range, RecordId, RecordIdKey,
	RecordIdKeyRange, Set, Table, Uuid, Value, Variables,
};

// =============================================================================
// Variables conversions
// =============================================================================

impl From<proto::Variables> for Variables {
	fn from(proto_vars: proto::Variables) -> Self {
		let map: BTreeMap<String, Value> = proto_vars
			.variables
			.into_iter()
			.map(|(k, v)| (k, Value::try_from(v).unwrap_or(Value::None)))
			.collect();
		Variables::from(map)
	}
}

impl From<Variables> for proto::Variables {
	fn from(vars: Variables) -> Self {
		let variables: BTreeMap<String, proto::Value> =
			vars.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect();
		proto::Variables {
			variables,
		}
	}
}

// =============================================================================
// Value conversions
// =============================================================================

impl TryFrom<proto::Value> for Value {
	type Error = anyhow::Error;

	fn try_from(proto_val: proto::Value) -> Result<Self, Self::Error> {
		let Some(value) = proto_val.value else {
			return Ok(Value::None);
		};

		match value {
			proto::value::Value::Null(_) => Ok(Value::Null),
			proto::value::Value::Bool(b) => Ok(Value::Bool(b)),
			proto::value::Value::Int64(i) => Ok(Value::Number(Number::Int(i))),
			proto::value::Value::Float64(f) => Ok(Value::Number(Number::Float(f))),
			proto::value::Value::Decimal(d) => {
				// Parse decimal string
				Ok(d.value.parse().map(Number::Decimal).map(Value::Number).unwrap_or(Value::None))
			}
			proto::value::Value::String(s) => Ok(Value::String(s)),
			proto::value::Value::StringRecordId(s) => {
				// StringRecordId is represented as a string in protobuf
				// It should be parsed by the application layer if needed
				Ok(Value::String(s))
			}
			proto::value::Value::Bytes(b) => Ok(Value::Bytes(Bytes::from(b.to_vec()))),
			proto::value::Value::Duration(d) => {
				// Convert prost Duration to std Duration
				let secs = d.seconds as u64;
				let nanos = d.nanos as u32;
				Ok(Value::Duration(Duration(std::time::Duration::new(secs, nanos))))
			}
			proto::value::Value::Datetime(ts) => {
				// Convert prost Timestamp to chrono DateTime
				Ok(chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
					.map(|dt| Value::Datetime(Datetime(dt)))
					.unwrap_or(Value::None))
			}
			proto::value::Value::Uuid(u) => {
				// Parse UUID string
				Ok(u.value.parse().map(Uuid).map(Value::Uuid).unwrap_or(Value::None))
			}
			proto::value::Value::Geometry(g) => {
				Ok(Geometry::try_from(g).map(Value::Geometry).unwrap_or(Value::None))
			}
			proto::value::Value::Table(t) => Ok(Value::Table(Table::from(t))),
			proto::value::Value::RecordId(r) => {
				Ok(RecordId::try_from(r).map(Value::RecordId).unwrap_or(Value::None))
			}
			proto::value::Value::File(f) => Ok(Value::File(File {
				bucket: f.bucket,
				key: f.key,
			})),
			proto::value::Value::Range(r) => Ok(Value::Range(Box::new((*r).into()))),
			proto::value::Value::Object(o) => {
				let map: BTreeMap<String, Value> = o
					.items
					.into_iter()
					.map(|(k, v)| (k, Value::try_from(v).unwrap_or(Value::None)))
					.collect();
				Ok(Value::Object(Object(map)))
			}
			proto::value::Value::Array(a) => {
				let values: Vec<Value> = a
					.values
					.into_iter()
					.map(|v| Value::try_from(v).unwrap_or(Value::None))
					.collect();
				Ok(Value::Array(Array(values)))
			}
			proto::value::Value::Set(s) => {
				#[allow(clippy::mutable_key_type)]
				let values: std::collections::BTreeSet<Value> = s
					.values
					.into_iter()
					.map(|v| Value::try_from(v).unwrap_or(Value::None))
					.collect();
				Ok(Value::Set(Set(values)))
			}
			proto::value::Value::Regex(r) => {
				// Parse regex pattern
				Ok(r.parse().map(crate::Regex).map(Value::Regex).unwrap_or(Value::None))
			}
		}
	}
}

impl From<Value> for proto::Value {
	fn from(val: Value) -> Self {
		let value = match val {
			Value::None => None,
			Value::Null => Some(proto::value::Value::Null(proto::NullValue {})),
			Value::Bool(b) => Some(proto::value::Value::Bool(b)),
			Value::Number(Number::Int(i)) => Some(proto::value::Value::Int64(i)),
			Value::Number(Number::Float(f)) => Some(proto::value::Value::Float64(f)),
			Value::Number(Number::Decimal(d)) => {
				Some(proto::value::Value::Decimal(proto::Decimal {
					value: d.to_string(),
				}))
			}
			Value::String(s) => Some(proto::value::Value::String(s)),
			Value::Bytes(b) => Some(proto::value::Value::Bytes(bytes::Bytes::from(b.0))),
			Value::Duration(d) => {
				// Convert std Duration to prost Duration
				let seconds = d.0.as_secs() as i64;
				let nanos = d.0.subsec_nanos() as i32;
				Some(proto::value::Value::Duration(
					surrealdb_protocol::proto::prost_types::Duration {
						seconds,
						nanos,
					},
				))
			}
			Value::Datetime(dt) => {
				// Convert chrono DateTime to prost Timestamp
				let ts = dt.0.timestamp();
				let nanos = dt.0.timestamp_subsec_nanos() as i32;
				Some(proto::value::Value::Datetime(
					surrealdb_protocol::proto::prost_types::Timestamp {
						seconds: ts,
						nanos,
					},
				))
			}
			Value::Uuid(u) => Some(proto::value::Value::Uuid(proto::Uuid {
				value: u.0.to_string(),
			})),
			Value::Geometry(g) => {
				proto::Geometry::try_from(g).ok().map(proto::value::Value::Geometry)
			}
			Value::Table(t) => {
				// Tables are represented as strings in protobuf
				Some(proto::value::Value::String(t.to_string()))
			}
			Value::RecordId(r) => {
				proto::RecordId::try_from(r).ok().map(proto::value::Value::RecordId)
			}
			Value::File(f) => Some(proto::value::Value::File(proto::File {
				bucket: f.bucket,
				key: f.key,
			})),
			Value::Range(r) => Some(proto::value::Value::Range(Box::new((*r).into()))),
			Value::Regex(r) => {
				// Regex is not directly supported in protobuf, represent as string
				Some(proto::value::Value::String(r.0.to_string()))
			}
			Value::Array(a) => {
				let values: Vec<proto::Value> = a.0.into_iter().map(proto::Value::from).collect();
				Some(proto::value::Value::Array(proto::Array {
					values,
				}))
			}
			Value::Object(o) => {
				let items: BTreeMap<String, proto::Value> =
					o.0.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect();
				Some(proto::value::Value::Object(proto::Object {
					items,
				}))
			}
			Value::Set(s) => {
				let values: Vec<proto::Value> = s.0.into_iter().map(proto::Value::from).collect();
				Some(proto::value::Value::Set(proto::Set {
					values,
				}))
			}
		};

		proto::Value {
			value,
		}
	}
}

// =============================================================================
// Geometry conversions
// =============================================================================

impl TryFrom<proto::Geometry> for Geometry {
	type Error = anyhow::Error;

	fn try_from(g: proto::Geometry) -> Result<Self, Self::Error> {
		use geo::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

		let geometry = match g.geometry.context("Geometry field is missing")? {
			proto::geometry::Geometry::Point(p) => Geometry::Point(Point::new(p.x, p.y)),
			proto::geometry::Geometry::Line(l) => {
				let coords: Vec<Coord> = l
					.points
					.into_iter()
					.map(|p| Coord {
						x: p.x,
						y: p.y,
					})
					.collect();
				Geometry::Line(LineString::new(coords))
			}
			proto::geometry::Geometry::Polygon(p) => {
				let exterior = p.exterior.context("Polygon exterior is missing")?;
				let ext_coords: Vec<Coord> = exterior
					.points
					.into_iter()
					.map(|p| Coord {
						x: p.x,
						y: p.y,
					})
					.collect();
				let ext_line = LineString::new(ext_coords);

				let interiors: Vec<LineString> = p
					.interiors
					.into_iter()
					.map(|int| {
						let coords: Vec<Coord> = int
							.points
							.into_iter()
							.map(|p| Coord {
								x: p.x,
								y: p.y,
							})
							.collect();
						LineString::new(coords)
					})
					.collect();

				Geometry::Polygon(Polygon::new(ext_line, interiors))
			}
			proto::geometry::Geometry::MultiPoint(mp) => {
				let points: Vec<Point> =
					mp.points.into_iter().map(|p| Point::new(p.x, p.y)).collect();
				Geometry::MultiPoint(MultiPoint::new(points))
			}
			proto::geometry::Geometry::MultiLine(ml) => {
				let lines: Vec<LineString> = ml
					.lines
					.into_iter()
					.map(|l| {
						let coords: Vec<Coord> = l
							.points
							.into_iter()
							.map(|p| Coord {
								x: p.x,
								y: p.y,
							})
							.collect();
						LineString::new(coords)
					})
					.collect();
				Geometry::MultiLine(MultiLineString::new(lines))
			}
			proto::geometry::Geometry::MultiPolygon(mp) => {
				let polygons: Result<Vec<Polygon>> = mp
					.polygons
					.into_iter()
					.map(|p| {
						let exterior = p.exterior.context("MultiPolygon exterior is missing")?;
						let ext_coords: Vec<Coord> = exterior
							.points
							.into_iter()
							.map(|p| Coord {
								x: p.x,
								y: p.y,
							})
							.collect();
						let ext_line = LineString::new(ext_coords);

						let interiors: Vec<LineString> = p
							.interiors
							.into_iter()
							.map(|int| {
								let coords: Vec<Coord> = int
									.points
									.into_iter()
									.map(|p| Coord {
										x: p.x,
										y: p.y,
									})
									.collect();
								LineString::new(coords)
							})
							.collect();

						Ok(Polygon::new(ext_line, interiors))
					})
					.collect();
				Geometry::MultiPolygon(MultiPolygon::new(polygons?))
			}
			proto::geometry::Geometry::Collection(gc) => {
				let geoms: Result<Vec<Geometry>> =
					gc.geometries.into_iter().map(Geometry::try_from).collect();
				Geometry::Collection(geoms.context("Failed to convert GeometryCollection")?)
			}
		};

		Ok(geometry)
	}
}

impl TryFrom<Geometry> for proto::Geometry {
	type Error = anyhow::Error;

	fn try_from(g: Geometry) -> Result<Self, Self::Error> {
		let geometry = match g {
			Geometry::Point(p) => proto::geometry::Geometry::Point(proto::Point {
				x: p.x(),
				y: p.y(),
			}),
			Geometry::Line(l) => {
				let points: Vec<proto::Point> = l
					.into_iter()
					.map(|c| proto::Point {
						x: c.x,
						y: c.y,
					})
					.collect();
				proto::geometry::Geometry::Line(proto::Line {
					points,
				})
			}
			Geometry::Polygon(p) => {
				let exterior = {
					let coords: Vec<proto::Point> = p
						.exterior()
						.points()
						.map(|pt| proto::Point {
							x: pt.x(),
							y: pt.y(),
						})
						.collect();
					proto::Line {
						points: coords,
					}
				};

				let interiors: Vec<proto::Line> = p
					.interiors()
					.iter()
					.map(|int| {
						let coords: Vec<proto::Point> = int
							.points()
							.map(|pt| proto::Point {
								x: pt.x(),
								y: pt.y(),
							})
							.collect();
						proto::Line {
							points: coords,
						}
					})
					.collect();

				proto::geometry::Geometry::Polygon(proto::Polygon {
					exterior: Some(exterior),
					interiors,
				})
			}
			Geometry::MultiPoint(mp) => {
				let points: Vec<proto::Point> = mp
					.iter()
					.map(|p| proto::Point {
						x: p.x(),
						y: p.y(),
					})
					.collect();
				proto::geometry::Geometry::MultiPoint(proto::MultiPoint {
					points,
				})
			}
			Geometry::MultiLine(ml) => {
				let lines: Vec<proto::Line> = ml
					.iter()
					.map(|l| {
						let points: Vec<proto::Point> = l
							.points()
							.map(|pt| proto::Point {
								x: pt.x(),
								y: pt.y(),
							})
							.collect();
						proto::Line {
							points,
						}
					})
					.collect();
				proto::geometry::Geometry::MultiLine(proto::MultiLine {
					lines,
				})
			}
			Geometry::MultiPolygon(mp) => {
				let polygons: Vec<proto::Polygon> = mp
					.iter()
					.map(|p| {
						let exterior = {
							let coords: Vec<proto::Point> = p
								.exterior()
								.points()
								.map(|pt| proto::Point {
									x: pt.x(),
									y: pt.y(),
								})
								.collect();
							proto::Line {
								points: coords,
							}
						};

						let interiors: Vec<proto::Line> = p
							.interiors()
							.iter()
							.map(|int| {
								let coords: Vec<proto::Point> = int
									.points()
									.map(|pt| proto::Point {
										x: pt.x(),
										y: pt.y(),
									})
									.collect();
								proto::Line {
									points: coords,
								}
							})
							.collect();

						proto::Polygon {
							exterior: Some(exterior),
							interiors,
						}
					})
					.collect();
				proto::geometry::Geometry::MultiPolygon(proto::MultiPolygon {
					polygons,
				})
			}
			Geometry::Collection(gc) => {
				let geometries: Result<Vec<proto::Geometry>> =
					gc.into_iter().map(proto::Geometry::try_from).collect();
				proto::geometry::Geometry::Collection(proto::GeometryCollection {
					geometries: geometries.context("Failed to convert GeometryCollection")?,
				})
			}
		};

		Ok(proto::Geometry {
			geometry: Some(geometry),
		})
	}
}

// =============================================================================
// RecordId conversions
// =============================================================================

impl TryFrom<proto::RecordId> for RecordId {
	type Error = anyhow::Error;

	fn try_from(r: proto::RecordId) -> Result<Self, Self::Error> {
		let table = Table::new(r.table);
		let key = RecordIdKey::try_from(r.id.context("RecordId id field is missing")?)?;
		Ok(RecordId {
			table,
			key,
		})
	}
}

impl TryFrom<RecordId> for proto::RecordId {
	type Error = anyhow::Error;

	fn try_from(r: RecordId) -> Result<Self, Self::Error> {
		let table = r.table.to_string();
		let id = Some(proto::RecordIdKey::try_from(r.key)?);
		Ok(proto::RecordId {
			table,
			id,
		})
	}
}

impl TryFrom<proto::RecordIdKey> for RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(k: proto::RecordIdKey) -> Result<Self, Self::Error> {
		match k.id.context("RecordIdKey id field is missing")? {
			proto::record_id_key::Id::Int64(i) => Ok(RecordIdKey::Number(i)),
			proto::record_id_key::Id::String(s) => Ok(RecordIdKey::String(s)),
			proto::record_id_key::Id::Uuid(u) => {
				let uuid = u.value.parse().context("Invalid UUID format")?;
				Ok(RecordIdKey::Uuid(Uuid(uuid)))
			}
			proto::record_id_key::Id::Array(a) => Ok(RecordIdKey::Array(a.into())),
			proto::record_id_key::Id::Object(o) => Ok(RecordIdKey::Object(o.into())),
			proto::record_id_key::Id::Range(r) => {
				Ok(RecordIdKey::Range(Box::new((*r).try_into()?)))
			}
		}
	}
}

impl TryFrom<RecordIdKey> for proto::RecordIdKey {
	type Error = anyhow::Error;

	fn try_from(k: RecordIdKey) -> Result<Self, Self::Error> {
		let id = match k {
			RecordIdKey::Number(i) => Some(proto::record_id_key::Id::Int64(i)),
			RecordIdKey::String(s) => Some(proto::record_id_key::Id::String(s)),
			RecordIdKey::Uuid(u) => Some(proto::record_id_key::Id::Uuid(proto::Uuid {
				value: u.0.to_string(),
			})),
			RecordIdKey::Array(a) => Some(proto::record_id_key::Id::Array(a.into())),
			RecordIdKey::Object(o) => Some(proto::record_id_key::Id::Object(o.into())),
			RecordIdKey::Range(r) => {
				Some(proto::record_id_key::Id::Range(Box::new((*r).try_into()?)))
			}
		};

		Ok(proto::RecordIdKey {
			id,
		})
	}
}

// =============================================================================
// Range conversions
// =============================================================================

impl From<proto::Range> for Range {
	fn from(r: proto::Range) -> Self {
		let start = match r.start {
			None => Bound::Unbounded,
			Some(b) => match b.bound {
				Some(proto::value_bound::Bound::Inclusive(v)) => {
					Bound::Included(Value::try_from(*v).unwrap_or(Value::None))
				}
				Some(proto::value_bound::Bound::Exclusive(v)) => {
					Bound::Excluded(Value::try_from(*v).unwrap_or(Value::None))
				}
				Some(proto::value_bound::Bound::Unbounded(_)) | None => Bound::Unbounded,
			},
		};

		let end = match r.end {
			None => Bound::Unbounded,
			Some(b) => match b.bound {
				Some(proto::value_bound::Bound::Inclusive(v)) => {
					Bound::Included(Value::try_from(*v).unwrap_or(Value::None))
				}
				Some(proto::value_bound::Bound::Exclusive(v)) => {
					Bound::Excluded(Value::try_from(*v).unwrap_or(Value::None))
				}
				Some(proto::value_bound::Bound::Unbounded(_)) | None => Bound::Unbounded,
			},
		};

		Range {
			start,
			end,
		}
	}
}

impl From<Range> for proto::Range {
	fn from(r: Range) -> Self {
		let start = match r.start {
			Bound::Unbounded => None,
			Bound::Included(v) => Some(Box::new(proto::ValueBound {
				bound: Some(proto::value_bound::Bound::Inclusive(Box::new(proto::Value::from(v)))),
			})),
			Bound::Excluded(v) => Some(Box::new(proto::ValueBound {
				bound: Some(proto::value_bound::Bound::Exclusive(Box::new(proto::Value::from(v)))),
			})),
		};

		let end = match r.end {
			Bound::Unbounded => None,
			Bound::Included(v) => Some(Box::new(proto::ValueBound {
				bound: Some(proto::value_bound::Bound::Inclusive(Box::new(proto::Value::from(v)))),
			})),
			Bound::Excluded(v) => Some(Box::new(proto::ValueBound {
				bound: Some(proto::value_bound::Bound::Exclusive(Box::new(proto::Value::from(v)))),
			})),
		};

		proto::Range {
			start,
			end,
		}
	}
}

// =============================================================================
// Collection conversions
// =============================================================================

impl From<proto::Array> for Array {
	fn from(a: proto::Array) -> Self {
		let values: Vec<Value> =
			a.values.into_iter().map(|v| Value::try_from(v).unwrap_or(Value::None)).collect();
		Array(values)
	}
}

impl From<Array> for proto::Array {
	fn from(a: Array) -> Self {
		let values: Vec<proto::Value> = a.0.into_iter().map(proto::Value::from).collect();
		proto::Array {
			values,
		}
	}
}

impl From<proto::Object> for Object {
	fn from(o: proto::Object) -> Self {
		let map: BTreeMap<String, Value> = o
			.items
			.into_iter()
			.map(|(k, v)| (k, Value::try_from(v).unwrap_or(Value::None)))
			.collect();
		Object(map)
	}
}

impl From<Object> for proto::Object {
	fn from(o: Object) -> Self {
		let items: BTreeMap<String, proto::Value> =
			o.0.into_iter().map(|(k, v)| (k, proto::Value::from(v))).collect();
		proto::Object {
			items,
		}
	}
}

impl From<proto::Set> for Set {
	fn from(s: proto::Set) -> Self {
		#[allow(clippy::mutable_key_type)]
		let values: std::collections::BTreeSet<Value> =
			s.values.into_iter().map(|v| Value::try_from(v).unwrap_or(Value::None)).collect();
		Set(values)
	}
}

impl From<Set> for proto::Set {
	fn from(s: Set) -> Self {
		let values: Vec<proto::Value> = s.0.into_iter().map(proto::Value::from).collect();
		proto::Set {
			values,
		}
	}
}

// =============================================================================
// RecordIdKeyRange conversions
// =============================================================================

impl TryFrom<proto::RecordIdKeyRange> for RecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(r: proto::RecordIdKeyRange) -> Result<Self, Self::Error> {
		let start = match r.start {
			None => Bound::Unbounded,
			Some(b) => match b.bound.context("RecordIdKeyRange start bound is missing")? {
				proto::record_id_key_bound::Bound::Inclusive(k) => {
					Bound::Included(RecordIdKey::try_from(*k)?)
				}
				proto::record_id_key_bound::Bound::Exclusive(k) => {
					Bound::Excluded(RecordIdKey::try_from(*k)?)
				}
				proto::record_id_key_bound::Bound::Unbounded(_) => Bound::Unbounded,
			},
		};

		let end = match r.end {
			None => Bound::Unbounded,
			Some(b) => match b.bound.context("RecordIdKeyRange end bound is missing")? {
				proto::record_id_key_bound::Bound::Inclusive(k) => {
					Bound::Included(RecordIdKey::try_from(*k)?)
				}
				proto::record_id_key_bound::Bound::Exclusive(k) => {
					Bound::Excluded(RecordIdKey::try_from(*k)?)
				}
				proto::record_id_key_bound::Bound::Unbounded(_) => Bound::Unbounded,
			},
		};

		Ok(RecordIdKeyRange {
			start,
			end,
		})
	}
}

impl TryFrom<RecordIdKeyRange> for proto::RecordIdKeyRange {
	type Error = anyhow::Error;

	fn try_from(r: RecordIdKeyRange) -> Result<Self, Self::Error> {
		let start = match r.start {
			Bound::Unbounded => None,
			Bound::Included(k) => Some(Box::new(proto::RecordIdKeyBound {
				bound: Some(proto::record_id_key_bound::Bound::Inclusive(Box::new(
					proto::RecordIdKey::try_from(k)?,
				))),
			})),
			Bound::Excluded(k) => Some(Box::new(proto::RecordIdKeyBound {
				bound: Some(proto::record_id_key_bound::Bound::Exclusive(Box::new(
					proto::RecordIdKey::try_from(k)?,
				))),
			})),
		};

		let end = match r.end {
			Bound::Unbounded => None,
			Bound::Included(k) => Some(Box::new(proto::RecordIdKeyBound {
				bound: Some(proto::record_id_key_bound::Bound::Inclusive(Box::new(
					proto::RecordIdKey::try_from(k)?,
				))),
			})),
			Bound::Excluded(k) => Some(Box::new(proto::RecordIdKeyBound {
				bound: Some(proto::record_id_key_bound::Bound::Exclusive(Box::new(
					proto::RecordIdKey::try_from(k)?,
				))),
			})),
		};

		Ok(proto::RecordIdKeyRange {
			start,
			end,
		})
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::ops::Bound;

	use rstest::rstest;
	use surrealdb_protocol::proto::prost_types::{
		Duration as ProstDuration, Timestamp as ProstTimestamp,
	};

	use super::*;

	// =============================================================================
	// Value to proto conversion tests
	// =============================================================================

	#[rstest]
	#[case::none(Value::None, proto::Value { value: None })]
	#[case::null(Value::Null, proto::Value { value: Some(proto::value::Value::Null(proto::NullValue {})) })]
	#[case::bool_true(Value::Bool(true), proto::Value { value: Some(proto::value::Value::Bool(true)) })]
	#[case::bool_false(Value::Bool(false), proto::Value { value: Some(proto::value::Value::Bool(false)) })]
	#[case::int(Value::Number(Number::Int(42)), proto::Value { value: Some(proto::value::Value::Int64(42)) })]
	#[case::int_negative(Value::Number(Number::Int(-42)), proto::Value { value: Some(proto::value::Value::Int64(-42)) })]
	#[case::float(Value::Number(Number::Float(1.23)), proto::Value { value: Some(proto::value::Value::Float64(1.23)) })]
	#[case::decimal(
		Value::Number(Number::Decimal(rust_decimal::Decimal::new(123, 2))),
		proto::Value { value: Some(proto::value::Value::Decimal(proto::Decimal { value: "1.23".to_string() })) }
	)]
	#[case::duration(
		Value::Duration(Duration(std::time::Duration::new(10, 500))),
		proto::Value { value: Some(proto::value::Value::Duration(ProstDuration { seconds: 10, nanos: 500 })) }
	)]
	#[case::datetime(
		Value::Datetime(Datetime(chrono::DateTime::from_timestamp(1234567890, 123456789).unwrap())),
		proto::Value { value: Some(proto::value::Value::Datetime(ProstTimestamp { seconds: 1234567890, nanos: 123456789 })) }
	)]
	#[case::uuid(
		Value::Uuid(Uuid(uuid::Uuid::nil())),
		proto::Value { value: Some(proto::value::Value::Uuid(proto::Uuid { value: "00000000-0000-0000-0000-000000000000".to_string() })) }
	)]
	#[case::string(Value::String("test".to_string()), proto::Value { value: Some(proto::value::Value::String("test".to_string())) })]
	#[case::string_empty(Value::String("".to_string()), proto::Value { value: Some(proto::value::Value::String("".to_string())) })]
	#[case::bytes(
		Value::Bytes(Bytes::from(vec![1, 2, 3, 4, 5])),
		proto::Value { value: Some(proto::value::Value::Bytes(bytes::Bytes::from(vec![1, 2, 3, 4, 5]))) }
	)]
	#[case::table(
		Value::Table(Table::new("users".to_string())),
		proto::Value { value: Some(proto::value::Value::String("users".to_string())) }
	)]
	#[case::file(
		Value::File(File { bucket: "my-bucket".to_string(), key: "my-key".to_string() }),
		proto::Value { value: Some(proto::value::Value::File(proto::File { bucket: "my-bucket".to_string(), key: "my-key".to_string() })) }
	)]
	#[case::regex(
		Value::Regex(crate::Regex("test.*".parse().unwrap())),
		proto::Value { value: Some(proto::value::Value::String("test.*".to_string())) }
	)]
	#[case::array_empty(
		Value::Array(Array(vec![])),
		proto::Value { value: Some(proto::value::Value::Array(proto::Array { values: vec![] })) }
	)]
	#[case::array(
		Value::Array(Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Float(2.0))])),
		proto::Value { value: Some(proto::value::Value::Array(proto::Array {
			values: vec![
				proto::Value { value: Some(proto::value::Value::Int64(1)) },
				proto::Value { value: Some(proto::value::Value::Float64(2.0)) }
			]
		})) }
	)]
	#[case::object_empty(
		Value::Object(Object(BTreeMap::new())),
		proto::Value { value: Some(proto::value::Value::Object(proto::Object { items: BTreeMap::new() })) }
	)]
	#[case::object(
		Value::Object(Object({
			let mut map = BTreeMap::new();
			map.insert("key".to_string(), Value::String("value".to_string()));
			map
		})),
		proto::Value { value: Some(proto::value::Value::Object(proto::Object {
			items: {
				let mut map = BTreeMap::new();
				map.insert("key".to_string(), proto::Value { value: Some(proto::value::Value::String("value".to_string())) });
				map
			}
		})) }
	)]
	#[case::set_empty(
		Value::Set(Set({
			std::collections::BTreeSet::new()
		})),
		proto::Value { value: Some(proto::value::Value::Set(proto::Set { values: vec![] })) }
	)]
	fn test_value_to_proto(#[case] input: Value, #[case] expected: proto::Value) {
		let proto_value = proto::Value::from(input);
		assert_eq!(proto_value, expected);
	}

	// =============================================================================
	// Proto to Value conversion tests
	// =============================================================================

	#[test]
	fn test_proto_to_value_none() {
		let proto = proto::Value {
			value: None,
		};
		let value = Value::try_from(proto).unwrap();
		assert_eq!(value, Value::None);
	}

	#[test]
	fn test_proto_to_value_null() {
		let proto = proto::Value {
			value: Some(proto::value::Value::Null(proto::NullValue {})),
		};
		let value = Value::try_from(proto).unwrap();
		assert_eq!(value, Value::Null);
	}

	// =============================================================================
	// Geometry conversion tests
	// =============================================================================

	#[test]
	fn test_geometry_point_roundtrip() {
		let geom = Geometry::Point(geo::Point::new(1.5, 2.5));
		let proto = proto::Geometry::try_from(geom.clone()).unwrap();
		let back = Geometry::try_from(proto).unwrap();
		assert_eq!(geom, back);
	}

	#[test]
	fn test_geometry_line_roundtrip() {
		let geom = Geometry::Line(geo::LineString::new(vec![
			geo::Coord {
				x: 0.0,
				y: 0.0,
			},
			geo::Coord {
				x: 1.0,
				y: 1.0,
			},
		]));
		let proto = proto::Geometry::try_from(geom.clone()).unwrap();
		let back = Geometry::try_from(proto).unwrap();
		assert_eq!(geom, back);
	}

	#[test]
	fn test_geometry_polygon_roundtrip() {
		let exterior = geo::LineString::new(vec![
			geo::Coord {
				x: 0.0,
				y: 0.0,
			},
			geo::Coord {
				x: 1.0,
				y: 0.0,
			},
			geo::Coord {
				x: 1.0,
				y: 1.0,
			},
			geo::Coord {
				x: 0.0,
				y: 0.0,
			},
		]);
		let geom = Geometry::Polygon(geo::Polygon::new(exterior, vec![]));
		let proto = proto::Geometry::try_from(geom.clone()).unwrap();
		let back = Geometry::try_from(proto).unwrap();
		assert_eq!(geom, back);
	}

	#[test]
	fn test_geometry_multipoint_roundtrip() {
		let geom = Geometry::MultiPoint(geo::MultiPoint::new(vec![
			geo::Point::new(0.0, 0.0),
			geo::Point::new(1.0, 1.0),
		]));
		let proto = proto::Geometry::try_from(geom.clone()).unwrap();
		let back = Geometry::try_from(proto).unwrap();
		assert_eq!(geom, back);
	}

	// =============================================================================
	// RecordId conversion tests
	// =============================================================================

	#[test]
	fn test_record_id_int_roundtrip() {
		let record = RecordId::new("users", 123);
		let proto = proto::RecordId::try_from(record.clone()).unwrap();
		let back = RecordId::try_from(proto).unwrap();
		assert_eq!(record, back);
	}

	#[test]
	fn test_record_id_string_roundtrip() {
		let record = RecordId::new("users", "john");
		let proto = proto::RecordId::try_from(record.clone()).unwrap();
		let back = RecordId::try_from(proto).unwrap();
		assert_eq!(record, back);
	}

	#[test]
	fn test_record_id_uuid_roundtrip() {
		let record = RecordId::new("users", Uuid(uuid::Uuid::nil()));
		let proto = proto::RecordId::try_from(record.clone()).unwrap();
		let back = RecordId::try_from(proto).unwrap();
		assert_eq!(record, back);
	}

	#[test]
	fn test_record_id_array_roundtrip() {
		let record = RecordId::new(
			"users",
			RecordIdKey::Array(Array(vec![
				Value::String("a".to_string()),
				Value::Number(Number::Int(1)),
			])),
		);
		let proto = proto::RecordId::try_from(record.clone()).unwrap();
		let back = RecordId::try_from(proto).unwrap();
		assert_eq!(record, back);
	}

	// =============================================================================
	// Range conversion tests
	// =============================================================================

	#[test]
	fn test_range_unbounded_roundtrip() {
		let range = Range {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		};
		let proto: proto::Range = range.clone().into();
		let back: Range = proto.into();
		assert_eq!(range, back);
	}

	#[test]
	fn test_range_included_roundtrip() {
		let range = Range {
			start: Bound::Included(Value::Number(Number::Int(1))),
			end: Bound::Included(Value::Number(Number::Int(10))),
		};
		let proto: proto::Range = range.clone().into();
		let back: Range = proto.into();
		assert_eq!(range, back);
	}

	#[test]
	fn test_range_excluded_roundtrip() {
		let range = Range {
			start: Bound::Excluded(Value::Number(Number::Int(1))),
			end: Bound::Excluded(Value::Number(Number::Int(10))),
		};
		let proto: proto::Range = range.clone().into();
		let back: Range = proto.into();
		assert_eq!(range, back);
	}

	#[test]
	fn test_range_mixed_roundtrip() {
		let range = Range {
			start: Bound::Included(Value::String("a".to_string())),
			end: Bound::Unbounded,
		};
		let proto: proto::Range = range.clone().into();
		let back: Range = proto.into();
		assert_eq!(range, back);
	}

	// =============================================================================
	// Variables conversion tests
	// =============================================================================

	#[test]
	fn test_variables_roundtrip() {
		let mut vars = Variables::new();
		vars.insert("name".to_string(), Value::String("John".to_string()));
		vars.insert("age".to_string(), Value::Number(Number::Int(30)));

		let proto = proto::Variables::from(vars.clone());
		let back = Variables::from(proto);

		assert_eq!(vars, back);
	}

	// =============================================================================
	// Complex roundtrip tests
	// =============================================================================

	#[test]
	fn test_nested_array_roundtrip() {
		let value = Value::Array(Array(vec![
			Value::Array(Array(vec![Value::Number(Number::Int(1)), Value::Number(Number::Int(2))])),
			Value::Array(Array(vec![
				Value::String("a".to_string()),
				Value::String("b".to_string()),
			])),
		]));
		let proto = proto::Value::from(value.clone());
		let back = Value::try_from(proto).unwrap();
		assert_eq!(value, back);
	}

	#[test]
	fn test_nested_object_roundtrip() {
		let mut inner = BTreeMap::new();
		inner.insert("inner_key".to_string(), Value::Number(Number::Int(42)));

		let mut outer = BTreeMap::new();
		outer.insert("outer_key".to_string(), Value::Object(Object(inner)));

		let value = Value::Object(Object(outer));
		let proto = proto::Value::from(value.clone());
		let back = Value::try_from(proto).unwrap();
		assert_eq!(value, back);
	}
}
