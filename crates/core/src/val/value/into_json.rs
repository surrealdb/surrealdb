use chrono::SecondsFormat;
use geo::{LineString, Point, Polygon};
use serde_json::{Map, Number as JsonNumber, Value as JsonValue, json};

use crate::val::{Geometry, Number, Value};

impl Value {
	/// Converts the value into a json representation of the value.
	/// Returns None if there are non serializable values present in the value.
	// TODO: Remove the JsonValue intermediate and implement a json formatter for
	// Value.
	pub fn into_json_value(self) -> Option<JsonValue> {
		// This function goes through some extra length to manually implement the
		// encoding into json value. This is done to ensure clarity and stability in
		// regards to how the value varients are converted.

		let res = match self {
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
			Value::Strand(strand) => JsonValue::String(strand.into_string()),
			Value::Duration(duration) => JsonValue::String(duration.to_raw()),
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
				array
					.0
					.into_iter()
					.map(Value::into_json_value)
					.collect::<Option<Vec<JsonValue>>>()?,
			),
			Value::Object(object) => {
				let mut map = Map::with_capacity(object.len());
				for (k, v) in object.0 {
					map.insert(k, v.into_json_value()?);
				}
				JsonValue::Object(map)
			}
			Value::Geometry(geo) => geometry_into_json_value(geo),
			Value::Bytes(bytes) => {
				JsonValue::Array(bytes.0.into_iter().map(|x| JsonValue::Number(x.into())).collect())
			}
			Value::RecordId(thing) => JsonValue::String(thing.to_string()),
			// TODO: Maybe remove
			Value::Regex(regex) => JsonValue::String(regex.0.to_string()),
			Value::File(file) => JsonValue::String(file.to_string()),
			// This kind of breaks the behaviour
			// TODO: look at the serialization here.
			Value::Range(range) => JsonValue::String(range.to_string()),
			// These Value types are un-computed values
			// and are not used in query responses sent
			// to the client.
			Value::Table(_) => return None,
			Value::Closure(_) => return None,
		};
		Some(res)
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

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;
	use std::time::Duration;

	use chrono::{DateTime, Utc};
	use geo::{MultiLineString, MultiPoint, MultiPolygon, line_string, point, polygon};
	use rstest::rstest;
	use rust_decimal::Decimal;
	use serde_json::{Value as Json, json};
	use uuid::Uuid;

	use crate::val::{self, RecordId, RecordIdKey, Value};

	#[rstest]
	#[case::none(Value::None, json!(null), Value::Null)]
	#[case::null(Value::Null, json!(null), Value::Null)]
	#[case::bool(Value::Bool(true), json!(true), Value::Bool(true))]
	#[case::bool(Value::Bool(false), json!(false), Value::Bool(false))]
	#[case::number(
		Value::Number(val::Number::Int(i64::MIN)),
		json!(i64::MIN),
		Value::Number(val::Number::Int(i64::MIN)),
	)]
	#[case::number(
		Value::Number(val::Number::Int(i64::MAX)),
		json!(i64::MAX),
		Value::Number(val::Number::Int(i64::MAX)),
	)]
	#[case::number(
		Value::Number(val::Number::Float(1.23)),
		json!(1.23),
		Value::Number(val::Number::Float(1.23)),
	)]
	#[case::number(
		Value::Number(val::Number::Float(f64::NEG_INFINITY)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(val::Number::Float(f64::MIN)),
		json!(-1.7976931348623157e308),
		Value::Number(val::Number::Float(f64::MIN)),
	)]
	#[case::number(
		Value::Number(val::Number::Float(0.0)),
		json!(0.0),
		Value::Number(val::Number::Float(0.0)),
	)]
	#[case::number(
		Value::Number(val::Number::Float(f64::MAX)),
		json!(1.7976931348623157e308),
		Value::Number(val::Number::Float(f64::MAX)),
	)]
	#[case::number(
		Value::Number(val::Number::Float(f64::INFINITY)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(val::Number::Float(f64::NAN)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(val::Number::Decimal(Decimal::new(123, 2))),
		json!("1.23"),
		Value::Strand("1.23".into()),
	)]
	#[case::strand(
		Value::Strand("".into()),
		json!(""),
		Value::Strand("".into()),
	)]
	#[case::strand(
		Value::Strand("foo".into()),
		json!("foo"),
		Value::Strand("foo".into()),
	)]
	#[case::duration(
		Value::Duration(val::Duration(Duration::ZERO)),
		json!("0ns"),
		Value::Strand("0ns".into()),
	)]
	#[case::duration(
		Value::Duration(val::Duration(Duration::MAX)),
		json!("584942417355y3w5d7h15s999ms999µs999ns"),
		Value::Strand("584942417355y3w5d7h15s999ms999µs999ns".into()),
	)]
	#[case::datetime(
		Value::Datetime(val::Datetime(DateTime::<Utc>::MIN_UTC)),
		json!("-262143-01-01T00:00:00Z"),
		Value::Strand("-262143-01-01T00:00:00Z".into()),
	)]
	#[case::datetime(
		Value::Datetime(val::Datetime(DateTime::<Utc>::MAX_UTC)),
		json!("+262142-12-31T23:59:59.999999999Z"),
		Value::Strand("+262142-12-31T23:59:59.999999999Z".into()),
	)]
	#[case::uuid(
		Value::Uuid(val::Uuid(Uuid::nil())),
		json!("00000000-0000-0000-0000-000000000000"),
		Value::Strand("00000000-0000-0000-0000-000000000000".into()),
	)]
	#[case::uuid(
		Value::Uuid(val::Uuid(Uuid::max())),
		json!("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		Value::Strand("ffffffff-ffff-ffff-ffff-ffffffffffff".into()),
	)]
	#[case::bytes(
		Value::Bytes(val::Bytes(vec![])),
		json!([]),
		Value::Array(val::Array(vec![])),
	)]
	#[case::bytes(
		Value::Bytes(val::Bytes(b"foo".to_vec())),
		json!([102, 111, 111]),
		Value::Array(val::Array(vec![
			Value::Number(val::Number::Int(102)),
			Value::Number(val::Number::Int(111)),
			Value::Number(val::Number::Int(111)),
		])),
	)]
	#[case::thing(
		Value::RecordId(RecordId{ table: "foo".to_string(), key: RecordIdKey::String("bar".into())}) ,
		json!("foo:bar"),
		Value::RecordId(RecordId{ table: "foo".to_string(), key: RecordIdKey::String("bar".into())}) ,
	)]
	#[case::array(
		Value::Array(val::Array(vec![])),
		json!([]),
		Value::Array(val::Array(vec![])),
	)]
	#[case::array(
		Value::Array(val::Array(vec![Value::Bool(true), Value::Bool(false)])),
		json!([true, false]),
		Value::Array(val::Array(vec![Value::Bool(true), Value::Bool(false)])),
	)]
	#[case::object(
		Value::Object(val::Object(BTreeMap::new())),
		json!({}),
		Value::Object(val::Object(BTreeMap::new())),
	)]
	#[case::object(
		Value::Object(val::Object(BTreeMap::from([("done".to_owned(), Value::Bool(true))]))),
		json!({"done": true}),
		Value::Object(val::Object(BTreeMap::from([("done".to_owned(), Value::Bool(true))]))),
	)]
	#[case::geometry_point(
		Value::Geometry(val::Geometry::Point(point! { x: 10., y: 20. })),
		json!({ "type": "Point", "coordinates": [10., 20.]}),
		Value::Geometry(val::Geometry::Point(point! { x: 10., y: 20. })),
	)]
	#[case::geometry_line(
		Value::Geometry(val::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
		json!({ "type": "LineString", "coordinates": [[0., 0.], [10., 0.]]}),
		Value::Geometry(val::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
	)]
	#[case::geometry_polygon(
		Value::Geometry(val::Geometry::Polygon(polygon![
			(x: -111., y: 45.),
			(x: -111., y: 41.),
			(x: -104., y: 41.),
			(x: -104., y: 45.),
		])),
		json!({ "type": "Polygon", "coordinates": [[
			[-111., 45.],
			[-111., 41.],
			[-104., 41.],
			[-104., 45.],
			[-111., 45.],
		]]}),
		Value::Geometry(val::Geometry::Polygon(polygon![
			(x: -111., y: 45.),
			(x: -111., y: 41.),
			(x: -104., y: 41.),
			(x: -104., y: 45.),
		])),
	)]
	#[case::geometry_multi_point(
		Value::Geometry(val::Geometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
		json!({ "type": "MultiPoint", "coordinates": [[0., 0.], [1., 2.]]}),
		Value::Geometry(val::Geometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
	)]
	#[case::geometry_multi_line(
		Value::Geometry(
			val::Geometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
		json!({ "type": "MultiLineString", "coordinates": [[[0., 0.], [1., 2.]]]}),
		Value::Geometry(
			val::Geometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
	)]
	#[case::geometry_multi_polygon(
		Value::Geometry(val::Geometry::MultiPolygon(MultiPolygon::new(vec![
			polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			],
		]))),
		json!({ "type": "MultiPolygon", "coordinates": [[[
			[-111., 45.],
			[-111., 41.],
			[-104., 41.],
			[-104., 45.],
			[-111., 45.],
		]]]})
	,	Value::Geometry(val::Geometry::MultiPolygon(MultiPolygon::new(vec![
			polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			],
		]))),
	)]
	#[case::geometry_collection(
		Value::Geometry(val::Geometry::Collection(vec![])),
		json!({
			"type": "GeometryCollection",
			"geometries": [],
		}),
		Value::Geometry(val::Geometry::Collection(vec![])),
	)]
	#[case::geometry_collection_with_point(
		Value::Geometry(val::Geometry::Collection(vec![val::Geometry::Point(point! { x: 10., y: 20. })])),
		json!({
		"type": "GeometryCollection",
		"geometries": [ { "type": "Point", "coordinates": [10., 20.] } ],
	}),
		Value::Geometry(val::Geometry::Collection(vec![val::Geometry::Point(point! { x: 10., y: 20. })])),
	)]
	#[case::geometry_collection_with_line(
		Value::Geometry(val::Geometry::Collection(vec![val::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
		json!({
			"type": "GeometryCollection",
			"geometries": [ { "type": "LineString", "coordinates": [[0., 0.], [10., 0.]] } ],
		}),
		Value::Geometry(val::Geometry::Collection(vec![val::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
	)]

	fn test_json(
		#[case] value: Value,
		#[case] expected: Json,
		#[case] expected_deserialized: Value,
	) {
		let json_value = value.into_json_value().unwrap();
		assert_eq!(json_value, expected);

		let json_str = serde_json::to_string(&json_value).expect("Failed to serialize to JSON");
		let deserialized_sql_value = crate::syn::value_legacy_strand(&json_str).unwrap();
		let deserialized: Value = deserialized_sql_value;
		assert_eq!(deserialized, expected_deserialized);
	}
}
