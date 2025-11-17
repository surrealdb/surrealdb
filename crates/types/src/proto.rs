//! Conversions between surrealdb-types and protobuf types
//!
//! This module provides From trait implementations for converting between
//! surrealdb-types Value, Variables, and other types and their protobuf
//! equivalents from the surrealdb-protocol crate.
//!
//! For complex nested types like Geometry and Range, we use serde as an
//! intermediate format since direct conversion would require exposing or
//! reconstructing internal representations from external crates (geo).

use std::collections::BTreeMap;

use surrealdb_protocol::proto::v1 as proto;

use crate::{Array, Bytes, Datetime, Duration, File, Number, Object, Set, Uuid, Value, Variables};

// =============================================================================
// Variables conversions
// =============================================================================

impl From<proto::Variables> for Variables {
	fn from(proto_vars: proto::Variables) -> Self {
		let map: BTreeMap<String, Value> =
			proto_vars.variables.into_iter().map(|(k, v)| (k, Value::from(v))).collect();
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

impl From<proto::Value> for Value {
	fn from(proto_val: proto::Value) -> Self {
		match proto_val.value {
			None => Value::None,
			Some(proto::value::Value::Null(_)) => Value::Null,
			Some(proto::value::Value::Bool(b)) => Value::Bool(b),
			Some(proto::value::Value::Int64(i)) => Value::Number(Number::Int(i)),
			Some(proto::value::Value::Uint64(u)) => {
				// Convert u64 to i64 if possible, otherwise to f64
				if let Ok(i) = i64::try_from(u) {
					Value::Number(Number::Int(i))
				} else {
					Value::Number(Number::Float(u as f64))
				}
			}
			Some(proto::value::Value::Float64(f)) => Value::Number(Number::Float(f)),
			Some(proto::value::Value::Decimal(d)) => {
				// Parse decimal string
				d.value.parse().map(Number::Decimal).map(Value::Number).unwrap_or(Value::None)
			}
			Some(proto::value::Value::String(s)) => Value::String(s),
			Some(proto::value::Value::Bytes(b)) => Value::Bytes(Bytes::from(b.to_vec())),
			Some(proto::value::Value::Duration(d)) => {
				// Convert prost Duration to std Duration
				let secs = d.seconds as u64;
				let nanos = d.nanos as u32;
				Value::Duration(Duration(std::time::Duration::new(secs, nanos)))
			}
			Some(proto::value::Value::Datetime(ts)) => {
				// Convert prost Timestamp to chrono DateTime
				chrono::DateTime::from_timestamp(ts.seconds, ts.nanos as u32)
					.map(|dt| Value::Datetime(Datetime(dt)))
					.unwrap_or(Value::None)
			}
			Some(proto::value::Value::Uuid(u)) => {
				// Parse UUID string
				u.value.parse().map(Uuid).map(Value::Uuid).unwrap_or(Value::None)
			}
			Some(proto::value::Value::Geometry(g)) => {
				// Use serde for geometry conversion
				serde_json::to_value(&g)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.unwrap_or(Value::None)
			}
			Some(proto::value::Value::RecordId(r)) => {
				// Use serde for RecordId conversion
				serde_json::to_value(&r)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.unwrap_or(Value::None)
			}
			Some(proto::value::Value::File(f)) => Value::File(File {
				bucket: f.bucket,
				key: f.key,
			}),
			Some(proto::value::Value::Range(r)) => {
				// Use serde for Range conversion
				serde_json::to_value(&*r)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.map(|r| Value::Range(Box::new(r)))
					.unwrap_or(Value::None)
			}
			Some(proto::value::Value::Object(o)) => {
				let map: BTreeMap<String, Value> =
					o.items.into_iter().map(|(k, v)| (k, Value::from(v))).collect();
				Value::Object(Object(map))
			}
			Some(proto::value::Value::Array(a)) => {
				let values: Vec<Value> = a.values.into_iter().map(Value::from).collect();
				Value::Array(Array(values))
			}
			Some(proto::value::Value::Set(s)) => {
				#[allow(clippy::mutable_key_type)]
				let values: std::collections::BTreeSet<Value> = s.values.into_iter().map(Value::from).collect();
				Value::Set(Set(values))
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
				// Use serde for geometry conversion
				serde_json::to_value(&g)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.map(proto::value::Value::Geometry)
			}
			Value::Table(t) => {
				// Tables are represented as strings in protobuf
				Some(proto::value::Value::String(t.to_string()))
			}
			Value::RecordId(r) => {
				// Use serde for RecordId conversion
				serde_json::to_value(&r)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.map(proto::value::Value::RecordId)
			}
			Value::File(f) => Some(proto::value::Value::File(proto::File {
				bucket: f.bucket,
				key: f.key,
			})),
			Value::Range(r) => {
				// Use serde for Range conversion
				serde_json::to_value(&*r)
					.ok()
					.and_then(|json| serde_json::from_value(json).ok())
					.map(|r| proto::value::Value::Range(Box::new(r)))
			}
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
