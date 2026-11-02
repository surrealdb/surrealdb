use chrono::SecondsFormat;
use geo_types::{LineString, Point, Polygon};
use serde_json::{Map, Number as JsonNumber, Value as JsonValue, json};

use crate::sql::ToSql;
use crate::{Geometry, Number, Value};

impl Value {
	/// Converts the value into its JSON representation.
	///
	/// This is a total conversion: every value variant maps to a `JsonValue`, so
	/// it never fails. Types without a native JSON counterpart are encoded on a
	/// best-effort basis (for example record ids, ranges, durations and datetimes
	/// become strings, and a non-finite float becomes `null`).
	// TODO: Remove the JsonValue intermediate and implement a json formatter for
	// Value.
	pub fn into_json_value(self) -> JsonValue {
		// This function goes through some extra length to manually implement the
		// encoding into json value. This is done to ensure clarity and stability in
		// regards to how the value variants are converted.

		match self {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			Value::None | Value::Null => JsonValue::Null,
			Value::Bool(boolean) => JsonValue::Bool(boolean),
			Value::Number(number) => match number {
				Number::Int(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::I8(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::U8(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::I16(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::U16(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::I32(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::U32(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::U64(int) => JsonValue::Number(JsonNumber::from(int)),
				Number::BigDecimal(decimal) => JsonValue::String(decimal.to_string()),
				Number::Float(float) => {
					// This is replicating serde_json::to_string default behavior.
					// f64 will convert to null if it is either NaN of infinity.
					if let Some(number) = JsonNumber::from_f64(float) {
						JsonValue::Number(number)
					} else {
						JsonValue::Null
					}
				}
				Number::F16(float) => {
					if let Some(number) = JsonNumber::from_f64(float.to_f64()) {
						JsonValue::Number(number)
					} else {
						JsonValue::Null
					}
				}
				Number::F32(float) => {
					if let Some(number) = float
						.is_finite()
						.then(|| float.to_string().parse::<f64>().ok())
						.flatten()
						.and_then(JsonNumber::from_f64)
					{
						JsonValue::Number(number)
					} else {
						JsonValue::Null
					}
				}
				Number::Decimal(decimal) => JsonValue::String(decimal.to_string()),
			},
			Value::String(strand) => JsonValue::String(strand),
			Value::Duration(duration) => JsonValue::String(duration.to_string()),
			Value::Datetime(datetime) => {
				JsonValue::String(datetime.0.to_rfc3339_opts(SecondsFormat::AutoSi, true))
			}
			Value::Date(v) => JsonValue::String(v.to_string()),
			Value::Time(v) => JsonValue::String(v.to_string()),
			Value::Timestamp(v) => JsonValue::String(v.to_string()),
			Value::Interval(v) => JsonValue::String(v.to_string()),
			Value::Uuid(uuid) => {
				// This buffer is the exact size needed to be able to encode the uuid.
				let mut buffer = [0u8; uuid::fmt::Hyphenated::LENGTH];
				let string = (*uuid.0.hyphenated().encode_lower(&mut buffer)).to_string();
				JsonValue::String(string)
			}
			Value::Array(array) => JsonValue::Array(
				array.0.into_iter().map(Value::into_json_value).collect::<Vec<JsonValue>>(),
			),
			Value::Set(set) => JsonValue::Array(
				set.0.into_iter().map(Value::into_json_value).collect::<Vec<JsonValue>>(),
			),
			Value::Object(object) => {
				let mut map = Map::with_capacity(object.len());
				for (k, v) in object.0 {
					map.insert(k, v.into_json_value());
				}
				JsonValue::Object(map)
			}
			Value::Geometry(geo) => geometry_into_json_value(geo),
			Value::Bytes(bytes) => {
				JsonValue::Array(bytes.0.into_iter().map(|x| JsonValue::Number(x.into())).collect())
			}
			Value::Table(table) => JsonValue::String(table.to_string()),
			Value::RecordId(thing) => JsonValue::String(thing.to_sql()),
			// TODO: Maybe remove
			Value::Regex(regex) => JsonValue::String(regex.to_sql()),
			Value::File(file) => JsonValue::String(file.to_sql()),
			// This kind of breaks the behaviour
			// TODO: look at the serialization here.
			Value::Range(range) => JsonValue::String(range.to_sql()),
		}
	}
}

impl Value {
	/// Appends the JSON encoding of this value to `out`: exactly what
	/// serialising [`Value::into_json_value`] would produce, without building
	/// the intermediate tree (one allocation per node) or consuming the value.
	pub fn write_json(&self, out: &mut Vec<u8>) {
		self.write_json_as(out, Decimals::Strings);
	}

	/// Appends the JSON encoding of this value as a SQL `json` or `jsonb`
	/// column returns it: [`Value::write_json`], except that a `decimal` and a
	/// `bigdecimal` are JSON numbers spelled with their exact digits, since a
	/// SQL JSON number has arbitrary precision.
	pub fn write_sql_json(&self, out: &mut Vec<u8>) {
		self.write_json_as(out, Decimals::Numbers);
	}

	fn write_json_as(&self, out: &mut Vec<u8>, decimals: Decimals) {
		// Strings and numbers go through serde_json's own formatting, so the
		// escaping and float formatting are the ones `into_json_value` gets.
		fn put<T: serde::Serialize + ?Sized>(out: &mut Vec<u8>, v: &T) {
			serde_json::to_writer(&mut *out, v).expect("writing json to a Vec cannot fail");
		}
		match self {
			Value::None | Value::Null => out.extend_from_slice(b"null"),
			Value::Bool(true) => out.extend_from_slice(b"true"),
			Value::Bool(false) => out.extend_from_slice(b"false"),
			Value::Number(Number::Int(int)) => put(out, int),
			Value::Number(Number::I8(int)) => put(out, int),
			Value::Number(Number::U8(int)) => put(out, int),
			Value::Number(Number::I16(int)) => put(out, int),
			Value::Number(Number::U16(int)) => put(out, int),
			Value::Number(Number::I32(int)) => put(out, int),
			Value::Number(Number::U32(int)) => put(out, int),
			Value::Number(Number::U64(int)) => put(out, int),
			Value::Number(Number::Float(float)) => {
				if float.is_finite() {
					put(out, float)
				} else {
					out.extend_from_slice(b"null")
				}
			}
			Value::Number(Number::F16(float)) => {
				if float.is_finite() {
					put(out, &float.to_f64())
				} else {
					out.extend_from_slice(b"null")
				}
			}
			Value::Number(Number::F32(float)) => {
				if float.is_finite() {
					put(out, float)
				} else {
					out.extend_from_slice(b"null")
				}
			}
			// Both spell their value in positional notation, which is a JSON number.
			Value::Number(Number::Decimal(decimal)) => match decimals {
				Decimals::Strings => put(out, &decimal.to_string()),
				Decimals::Numbers => out.extend_from_slice(decimal.to_string().as_bytes()),
			},
			Value::Number(Number::BigDecimal(decimal)) => match decimals {
				Decimals::Strings => put(out, &decimal.to_string()),
				Decimals::Numbers => out.extend_from_slice(decimal.to_string().as_bytes()),
			},
			Value::String(strand) => put(out, strand.as_str()),
			Value::Uuid(uuid) => {
				let mut buffer = [0u8; uuid::fmt::Hyphenated::LENGTH];
				put(out, &*uuid.0.hyphenated().encode_lower(&mut buffer))
			}
			Value::Array(array) => write_json_seq(out, array.0.iter(), decimals),
			Value::Set(set) => write_json_seq(out, set.0.iter(), decimals),
			Value::Object(object) => {
				out.push(b'{');
				for (i, (k, v)) in object.0.iter().enumerate() {
					if i > 0 {
						out.push(b',');
					}
					put(out, k.as_str());
					out.push(b':');
					v.write_json_as(out, decimals);
				}
				out.push(b'}');
			}
			Value::Bytes(bytes) => {
				out.push(b'[');
				for (i, b) in bytes.0.iter().enumerate() {
					if i > 0 {
						out.push(b',');
					}
					put(out, b);
				}
				out.push(b']');
			}
			// Rare or string-shaped: the conversion itself, so they cannot drift.
			other @ (Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Geometry(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Range(_)
			| Value::Regex(_)) => put(out, &other.clone().into_json_value()),
		}
	}
}

/// How a `decimal` or `bigdecimal` is written to JSON.
#[derive(Clone, Copy)]
enum Decimals {
	/// As a string, so a reader that parses JSON numbers as `f64` keeps every digit.
	Strings,
	/// As a number.
	Numbers,
}

fn write_json_seq<'a>(
	out: &mut Vec<u8>,
	items: impl Iterator<Item = &'a Value>,
	decimals: Decimals,
) {
	out.push(b'[');
	for (i, v) in items.enumerate() {
		if i > 0 {
			out.push(b',');
		}
		v.write_json_as(out, decimals);
	}
	out.push(b']');
}

fn geometry_into_json_value(geo: Geometry) -> JsonValue {
	match geo {
		Geometry::Point(point) => json!({
			"type": "Point",
			"coordinates": point_into_json_value(point)
		}),
		Geometry::Line(line_string) => {
			json!({
				"type": "LineString",
				"coordinates": line_into_json_value(&line_string)
			})
		}
		Geometry::Polygon(polygon) => {
			json!({
				"type": "Polygon",
				"coordinates": polygon_into_json_value(&polygon)
			})
		}
		Geometry::MultiPoint(multi_point) => {
			json!({
				"type": "MultiPoint",
				"coordinates": multi_point.into_iter().map(point_into_json_value).collect::<Vec<_>>(),
			})
		}
		Geometry::MultiLine(multi_line_string) => {
			json!({
				"type": "MultiLineString",
				"coordinates": multi_line_string.into_iter().map(|line| line_into_json_value(&line)).collect::<Vec<_>>(),
			})
		}
		Geometry::MultiPolygon(multi_polygon) => {
			json!({
				"type": "MultiPolygon",
				"coordinates": multi_polygon.into_iter().map(|polygon| polygon_into_json_value(&polygon)).collect::<Vec<_>>(),
			})
		}
		Geometry::Collection(items) => {
			json!({
				"type": "GeometryCollection",
				"geometries": items.into_iter().map(geometry_into_json_value).collect::<Vec<_>>(),
			})
		}
	}
}

fn point_into_json_value(point: Point) -> JsonValue {
	vec![JsonValue::from(point.x()), JsonValue::from(point.y())].into()
}

fn line_into_json_value(line_string: &LineString) -> JsonValue {
	line_string.points().map(point_into_json_value).collect::<Vec<_>>().into()
}

fn polygon_into_json_value(polygon: &Polygon) -> JsonValue {
	let mut coords =
		vec![polygon.exterior().points().map(point_into_json_value).collect::<Vec<_>>()];

	for int in polygon.interiors() {
		let int = int.points().map(point_into_json_value).collect::<Vec<_>>();
		coords.push(int);
	}
	coords.into()
}

#[cfg(test)]
mod write_json_tests {
	use std::collections::BTreeMap;

	use crate::{Array, Number, Object, Value};

	#[test]
	fn sql_json_writes_decimals_as_numbers() {
		let big: crate::BigDecimal = "-12345678901234567890123456789012345.50".parse().unwrap();
		let decimal: rust_decimal::Decimal = "1.50".parse().unwrap();
		let value = Value::Array(Array::from(vec![
			Value::Number(Number::BigDecimal(Box::new(big))),
			Value::Number(Number::Decimal(decimal)),
			Value::Object(Object::from(BTreeMap::from([(
				"n".to_owned(),
				Value::Number(Number::Decimal(decimal)),
			)]))),
		]));
		let mut sql = Vec::new();
		value.write_sql_json(&mut sql);
		let sql = String::from_utf8(sql).unwrap();
		assert_eq!(sql, r#"[-12345678901234567890123456789012345.50,1.50,{"n":1.50}]"#);
		assert!(serde_json::from_str::<serde_json::Value>(&sql).is_ok());
		let mut json = Vec::new();
		value.write_json(&mut json);
		assert_eq!(
			String::from_utf8(json).unwrap(),
			r#"["-12345678901234567890123456789012345.50","1.50",{"n":"1.50"}]"#
		);
	}

	#[test]
	fn matches_into_json_value() {
		let mut obj = BTreeMap::new();
		obj.insert("b\"q\u{1}".to_owned(), Value::Number(Number::Float(1.5)));
		obj.insert("a".to_owned(), Value::Number(Number::Int(-7)));
		obj.insert("i8".to_owned(), Value::Number(Number::I8(-8)));
		obj.insert("u64".to_owned(), Value::Number(Number::U64(u64::MAX)));
		obj.insert("f16".to_owned(), Value::Number(Number::F16(half::f16::from_f32(1.5))));
		obj.insert("f32".to_owned(), Value::Number(Number::F32(0.1)));
		obj.insert("n".to_owned(), Value::Number(Number::Float(f64::NAN)));
		obj.insert("f".to_owned(), Value::Number(Number::Float(1e300)));
		obj.insert("z".to_owned(), Value::Number(Number::Float(-0.0)));
		obj.insert("s".to_owned(), Value::String("héllo \n \u{2028} 😀".to_owned()));
		obj.insert("none".to_owned(), Value::None);
		obj.insert("t".to_owned(), Value::Bool(true));
		obj.insert(
			"arr".to_owned(),
			Value::Array(Array::from(vec![
				Value::Null,
				Value::Bool(false),
				Value::Number(Number::Int(i64::MIN)),
			])),
		);
		obj.insert("bytes".to_owned(), Value::Bytes(crate::Bytes::from(vec![0u8, 255, 7])));
		obj.insert("uuid".to_owned(), Value::Uuid(crate::Uuid::new_v4()));
		obj.insert("dt".to_owned(), Value::Datetime(crate::Datetime::now()));
		obj.insert("dur".to_owned(), Value::Duration(crate::Duration::from_secs(90)));
		obj.insert("empty".to_owned(), Value::Object(Object::default()));
		let v = Value::Array(Array::from(vec![
			Value::Object(Object::from(obj)),
			Value::Array(Array::new()),
		]));
		let mut out = Vec::new();
		v.write_json(&mut out);
		assert_eq!(
			String::from_utf8(out).unwrap(),
			serde_json::to_string(&v.into_json_value()).unwrap()
		);
		assert_eq!(
			serde_json::to_string(&Value::Number(Number::F32(0.1)).into_json_value()).unwrap(),
			"0.1"
		);
	}
}
