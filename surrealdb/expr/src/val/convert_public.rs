//! Conversions from public [`surrealdb_types`] values into internal values.
//!
//! These sit at the value layer: they consume the public type surface and
//! produce internal `crate::val` values, with no `sql` involvement.

use std::ops::Bound;

use surrealdb_types::RecordId as PublicRecordId;

use crate::val::table_name_public::IntoTableName;

fn convert_public_geometry_to_internal(geom: surrealdb_types::Geometry) -> crate::val::Geometry {
	match geom {
		surrealdb_types::Geometry::Point(p) => crate::val::Geometry::Point(p),
		surrealdb_types::Geometry::Line(l) => crate::val::Geometry::Line(l),
		surrealdb_types::Geometry::Polygon(p) => crate::val::Geometry::Polygon(p),
		surrealdb_types::Geometry::MultiPoint(mp) => crate::val::Geometry::MultiPoint(mp),
		surrealdb_types::Geometry::MultiLine(ml) => crate::val::Geometry::MultiLine(ml),
		surrealdb_types::Geometry::MultiPolygon(mp) => crate::val::Geometry::MultiPolygon(mp),
		surrealdb_types::Geometry::Collection(c) => crate::val::Geometry::Collection(
			c.into_iter().map(convert_public_geometry_to_internal).collect(),
		),
	}
}

pub fn convert_public_value_to_internal(value: surrealdb_types::Value) -> crate::val::Value {
	match value {
		surrealdb_types::Value::None => crate::val::Value::None,
		surrealdb_types::Value::Null => crate::val::Value::Null,
		surrealdb_types::Value::Bool(b) => crate::val::Value::Bool(b),
		surrealdb_types::Value::Number(n) => match n {
			surrealdb_types::Number::Int(i) => {
				crate::val::Value::Number(crate::val::Number::Int(i))
			}
			surrealdb_types::Number::Float(f) => {
				crate::val::Value::Number(crate::val::Number::Float(f))
			}
			surrealdb_types::Number::Decimal(d) => {
				crate::val::Value::Number(crate::val::Number::Decimal(d))
			}
		},
		surrealdb_types::Value::String(s) => crate::val::Value::String(s.into()),
		surrealdb_types::Value::Duration(d) => {
			crate::val::Value::Duration(crate::val::Duration(d.into_inner()))
		}
		surrealdb_types::Value::Datetime(dt) => {
			crate::val::Value::Datetime(crate::val::Datetime(dt.into_inner()))
		}
		surrealdb_types::Value::Uuid(u) => {
			crate::val::Value::Uuid(crate::val::Uuid(u.into_inner()))
		}
		surrealdb_types::Value::Array(a) => crate::val::Value::Array(crate::val::Array::from(
			a.into_iter().map(convert_public_value_to_internal).collect::<Vec<_>>(),
		)),
		surrealdb_types::Value::Set(s) => crate::val::Value::Set(crate::val::Set::from(
			s.into_iter()
				.map(convert_public_value_to_internal)
				.collect::<std::collections::BTreeSet<_>>(),
		)),
		surrealdb_types::Value::Object(o) => crate::val::Value::Object(crate::val::Object::from(
			o.into_iter()
				.map(|(k, v)| (k.into(), convert_public_value_to_internal(v)))
				.collect::<std::collections::BTreeMap<surrealdb_strand::Strand, crate::val::Value>>(),
		)),
		surrealdb_types::Value::Geometry(g) => {
			crate::val::Value::Geometry(convert_public_geometry_to_internal(g))
		}
		surrealdb_types::Value::Bytes(b) => {
			crate::val::Value::Bytes(crate::val::Bytes(b.into_inner()))
		}
		surrealdb_types::Value::Table(t) => crate::val::Value::Table(t.into_table_name()),
		surrealdb_types::Value::RecordId(PublicRecordId {
			table,
			key,
		}) => {
			let key = convert_public_record_id_key_to_internal(key);
			crate::val::Value::RecordId(crate::val::RecordId {
				table: table.into_table_name(),
				key,
			})
		}
		surrealdb_types::Value::File(f) => crate::val::Value::File(crate::val::File {
			bucket: f.bucket,
			key: f.key,
		}),
		surrealdb_types::Value::Range(r) => crate::val::Value::Range(Box::new(crate::val::Range {
			start: match r.start {
				Bound::Included(v) => Bound::Included(convert_public_value_to_internal(v)),
				Bound::Excluded(v) => Bound::Excluded(convert_public_value_to_internal(v)),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match r.end {
				Bound::Included(v) => Bound::Included(convert_public_value_to_internal(v)),
				Bound::Excluded(v) => Bound::Excluded(convert_public_value_to_internal(v)),
				Bound::Unbounded => Bound::Unbounded,
			},
		})),
		surrealdb_types::Value::Regex(r) => {
			crate::val::Value::Regex(crate::val::Regex(r.into_inner()))
		}
	}
}

fn convert_public_record_id_key_to_internal(
	key: surrealdb_types::RecordIdKey,
) -> crate::val::RecordIdKey {
	match key {
		surrealdb_types::RecordIdKey::Number(n) => crate::val::RecordIdKey::Number(n),
		surrealdb_types::RecordIdKey::String(s) => crate::val::RecordIdKey::String(s.into()),
		surrealdb_types::RecordIdKey::Uuid(u) => {
			crate::val::RecordIdKey::Uuid(crate::val::Uuid(u.into_inner()))
		}
		surrealdb_types::RecordIdKey::Array(a) => crate::val::RecordIdKey::Array(
			crate::val::Array(a.into_iter().map(convert_public_value_to_internal).collect()),
		),
		surrealdb_types::RecordIdKey::Object(o) => {
			crate::val::RecordIdKey::Object(crate::val::Object::from(
				o.into_iter()
					.map(|(k, v)| (k.into(), convert_public_value_to_internal(v)))
					.collect::<std::collections::BTreeMap<surrealdb_strand::Strand, crate::val::Value>>(
				),
			))
		}
		surrealdb_types::RecordIdKey::Range(r) => {
			crate::val::RecordIdKey::Range(Box::new(crate::val::RecordIdKeyRange {
				start: match r.start {
					Bound::Included(k) => {
						Bound::Included(convert_public_record_id_key_to_internal(k))
					}
					Bound::Excluded(k) => {
						Bound::Excluded(convert_public_record_id_key_to_internal(k))
					}
					Bound::Unbounded => Bound::Unbounded,
				},
				end: match r.end {
					Bound::Included(k) => {
						Bound::Included(convert_public_record_id_key_to_internal(k))
					}
					Bound::Excluded(k) => {
						Bound::Excluded(convert_public_record_id_key_to_internal(k))
					}
					Bound::Unbounded => Bound::Unbounded,
				},
			}))
		}
	}
}
