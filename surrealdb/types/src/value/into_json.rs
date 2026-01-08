use chrono::SecondsFormat;
use geo::{LineString, Point, Polygon};
use serde_json::{Map, Number as JsonNumber, Value as JsonValue, json};

use crate::sql::ToSql;
use crate::{Geometry, Number, Value};

impl Value {
	/// Converts the value into a json representation of the value.
	/// Returns None if there are non serializable values present in the value.
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
				Number::Float(float) => {
					// This is replicating serde_json::to_string default behavior.
					// f64 will convert to null if it is either NaN of infinity.
					if let Some(number) = JsonNumber::from_f64(float) {
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

fn geometry_into_json_value(geo: Geometry) -> JsonValue {
	match geo {
		Geometry::Point(point) => json!({
			"type": "Point",
			"coordinates": point_into_json_value(point)
		}),
		Geometry::Line(line_string) => {
			json!({
				"type": "LineString",
				"coordinates": line_into_json_value(line_string)
			})
		}
		Geometry::Polygon(polygon) => {
			json!({
				"type": "Polygon",
				"coordinates": polygon_into_json_value(polygon)
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
				"coordinates": multi_line_string.into_iter().map(line_into_json_value).collect::<Vec<_>>(),
			})
		}
		Geometry::MultiPolygon(multi_polygon) => {
			json!({
				"type": "MultiPolygon",
				"coordinates": multi_polygon.into_iter().map(polygon_into_json_value).collect::<Vec<_>>(),
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

fn line_into_json_value(line_string: LineString) -> JsonValue {
	line_string.points().map(point_into_json_value).collect::<Vec<_>>().into()
}

fn polygon_into_json_value(polygon: Polygon) -> JsonValue {
	let mut coords =
		vec![polygon.exterior().points().map(point_into_json_value).collect::<Vec<_>>()];

	for int in polygon.interiors() {
		let int = int.points().map(point_into_json_value).collect::<Vec<_>>();
		coords.push(int);
	}
	coords.into()
}
