use crate::expr;
use crate::expr::Number;
use crate::expr::Value;
use crate::expr::constant::ConstantValue;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value as JsonValue;
use serde_json::json;

impl From<Value> for serde_json::Value {
	fn from(value: Value) -> Self {
		match value {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			Value::None | Value::Null => JsonValue::Null,
			Value::Bool(boolean) => boolean.into(),
			Value::Number(number) => match number {
				Number::Int(int) => int.into(),
				Number::Float(float) => float.into(),
				Number::Decimal(decimal) => json!(decimal),
			},
			Value::Strand(strand) => strand.0.into(),
			Value::Duration(duration) => duration.to_raw().into(),
			Value::Datetime(datetime) => json!(datetime.0),
			Value::Uuid(uuid) => json!(uuid.0),
			Value::Array(array) => JsonValue::Array(Array::from(array).0),
			Value::Object(object) => JsonValue::Object(Object::from(object).0),
			Value::Geometry(geo) => Geometry::from(geo).0,
			Value::Bytes(bytes) => json!(bytes.0),
			Value::Thing(thing) => thing.to_string().into(),
			// These Value types are un-computed values
			// and are not used in query responses sent
			// to the client.
			Value::Param(param) => json!(param),
			Value::Idiom(idiom) => json!(idiom),
			Value::Table(table) => json!(table),
			Value::Mock(mock) => json!(mock),
			Value::Regex(regex) => json!(regex),
			Value::Block(block) => json!(block),
			Value::Range(range) => json!(range),
			Value::Edges(edges) => json!(edges),
			Value::Future(future) => json!(future),
			Value::Constant(constant) => match constant.value() {
				ConstantValue::Datetime(datetime) => json!(datetime.0),
				ConstantValue::Float(float) => float.into(),
				ConstantValue::Duration(duration) => duration.to_string().into(),
			},
			Value::Cast(cast) => json!(cast),
			Value::Function(function) => json!(function),
			Value::Model(model) => json!(model),
			Value::Query(query) => json!(query),
			Value::Subquery(subquery) => json!(subquery),
			Value::Expression(expression) => json!(expression),
			Value::Closure(closure) => json!(closure),
			Value::Refs(_) => json!(expr::Array::new()),
			Value::File(file) => file.to_string().into(),
		}
	}
}

#[derive(Serialize)]
struct Array(Vec<JsonValue>);

impl From<expr::Array> for Array {
	fn from(arr: expr::Array) -> Self {
		let mut vec = Vec::with_capacity(arr.len());
		for value in arr {
			vec.push(value.into());
		}
		Self(vec)
	}
}

#[derive(Serialize)]
struct Object(Map<String, JsonValue>);

impl From<expr::Object> for Object {
	fn from(obj: expr::Object) -> Self {
		let mut map = Map::with_capacity(obj.len());
		for (key, value) in obj {
			map.insert(key.clone(), value.into());
		}
		Self(map)
	}
}

#[derive(Serialize)]
enum CoordinatesType {
	Point,
	LineString,
	Polygon,
	MultiPoint,
	MultiLineString,
	MultiPolygon,
}

#[derive(Serialize)]
struct Coordinates {
	#[serde(rename = "type")]
	typ: CoordinatesType,
	coordinates: JsonValue,
}

struct GeometryCollection;

impl Serialize for GeometryCollection {
	fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		s.serialize_str("GeometryCollection")
	}
}

#[derive(Serialize)]
struct Geometries {
	#[serde(rename = "type")]
	typ: GeometryCollection,
	geometries: Vec<JsonValue>,
}

#[derive(Serialize)]
struct Geometry(JsonValue);

impl From<expr::Geometry> for Geometry {
	fn from(geo: expr::Geometry) -> Self {
		Self(match geo {
			expr::Geometry::Point(v) => json!(Coordinates {
				typ: CoordinatesType::Point,
				coordinates: vec![json!(v.x()), json!(v.y())].into(),
			}),
			expr::Geometry::Line(v) => json!(Coordinates {
				typ: CoordinatesType::LineString,
				coordinates: v
					.points()
					.map(|p| vec![json!(p.x()), json!(p.y())].into())
					.collect::<Vec<JsonValue>>()
					.into(),
			}),
			expr::Geometry::Polygon(v) => json!(Coordinates {
				typ: CoordinatesType::Polygon,
				coordinates: vec![
					v.exterior()
						.points()
						.map(|p| vec![json!(p.x()), json!(p.y())].into())
						.collect::<Vec<JsonValue>>()
				]
				.into_iter()
				.chain(
					v.interiors()
						.iter()
						.map(|i| {
							i.points()
								.map(|p| vec![json!(p.x()), json!(p.y())].into())
								.collect::<Vec<JsonValue>>()
						})
						.collect::<Vec<Vec<JsonValue>>>(),
				)
				.collect::<Vec<Vec<JsonValue>>>()
				.into(),
			}),
			expr::Geometry::MultiPoint(v) => json!(Coordinates {
				typ: CoordinatesType::MultiPoint,
				coordinates: v
					.0
					.iter()
					.map(|v| vec![json!(v.x()), json!(v.y())].into())
					.collect::<Vec<JsonValue>>()
					.into()
			}),
			expr::Geometry::MultiLine(v) => json!(Coordinates {
				typ: CoordinatesType::MultiLineString,
				coordinates: v
					.0
					.iter()
					.map(|v| {
						v.points()
							.map(|v| vec![json!(v.x()), json!(v.y())].into())
							.collect::<Vec<JsonValue>>()
					})
					.collect::<Vec<Vec<JsonValue>>>()
					.into()
			}),
			expr::Geometry::MultiPolygon(v) => json!(Coordinates {
				typ: CoordinatesType::MultiPolygon,
				coordinates: v
					.0
					.iter()
					.map(|v| {
						vec![
							v.exterior()
								.points()
								.map(|p| vec![json!(p.x()), json!(p.y())].into())
								.collect::<Vec<JsonValue>>(),
						]
						.into_iter()
						.chain(
							v.interiors()
								.iter()
								.map(|i| {
									i.points()
										.map(|p| vec![json!(p.x()), json!(p.y())].into())
										.collect::<Vec<JsonValue>>()
								})
								.collect::<Vec<Vec<JsonValue>>>(),
						)
						.collect::<Vec<Vec<JsonValue>>>()
					})
					.collect::<Vec<Vec<Vec<JsonValue>>>>()
					.into(),
			}),
			expr::Geometry::Collection(v) => json!(Geometries {
				typ: GeometryCollection,
				geometries: v.into_iter().map(Geometry::from).map(|x| x.0).collect(),
			}),
		})
	}
}

#[cfg(test)]
mod tests {
	use crate::expr;
	use crate::expr::Thing;
	use crate::expr::Value;
	use chrono::DateTime;
	use chrono::Utc;
	use geo::MultiLineString;
	use geo::MultiPoint;
	use geo::MultiPolygon;
	use geo::line_string;
	use geo::point;
	use geo::polygon;
	use rust_decimal::Decimal;
	use serde_json::Value as Json;
	use serde_json::json;
	use std::collections::BTreeMap;
	use std::time::Duration;
	use uuid::Uuid;

	use rstest::rstest;

	#[rstest]
	#[case::none(Value::None, json!(null), Value::Null)]
	#[case::null(Value::Null, json!(null), Value::Null)]
	#[case::bool(Value::Bool(true), json!(true), Value::Bool(true))]
	#[case::bool(Value::Bool(false), json!(false), Value::Bool(false))]
	#[case::number(
		Value::Number(expr::Number::Int(i64::MIN)),
		json!(i64::MIN),
		Value::Number(expr::Number::Int(i64::MIN)),
	)]
	#[case::number(
		Value::Number(expr::Number::Int(i64::MAX)),
		json!(i64::MAX),
		Value::Number(expr::Number::Int(i64::MAX)),
	)]
	#[case::number(
		Value::Number(expr::Number::Float(1.23)),
		json!(1.23),
		Value::Number(expr::Number::Float(1.23)),
	)]
	#[case::number(
		Value::Number(expr::Number::Float(f64::NEG_INFINITY)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(expr::Number::Float(f64::MIN)),
		json!(-1.7976931348623157e308),
		Value::Number(expr::Number::Float(f64::MIN)),
	)]
	#[case::number(
		Value::Number(expr::Number::Float(0.0)),
		json!(0.0),
		Value::Number(expr::Number::Float(0.0)),
	)]
	#[case::number(
		Value::Number(expr::Number::Float(f64::MAX)),
		json!(1.7976931348623157e308),
		Value::Number(expr::Number::Float(f64::MAX)),
	)]
	#[case::number(
		Value::Number(expr::Number::Float(f64::INFINITY)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(expr::Number::Float(f64::NAN)),
		json!(null),
		Value::Null,
	)]
	#[case::number(
		Value::Number(expr::Number::Decimal(Decimal::new(123, 2))),
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
		Value::Duration(expr::Duration(Duration::ZERO)),
		json!("0ns"),
		Value::Strand("0ns".into()),
	)]
	#[case::duration(
		Value::Duration(expr::Duration(Duration::MAX)),
		json!("584942417355y3w5d7h15s999ms999µs999ns"),
		Value::Strand("584942417355y3w5d7h15s999ms999µs999ns".into()),
	)]
	#[case::datetime(
		Value::Datetime(expr::Datetime(DateTime::<Utc>::MIN_UTC)),
		json!("-262143-01-01T00:00:00Z"),
		Value::Strand("-262143-01-01T00:00:00Z".into()),
	)]
	#[case::datetime(
		Value::Datetime(expr::Datetime(DateTime::<Utc>::MAX_UTC)),
		json!("+262142-12-31T23:59:59.999999999Z"),
		Value::Strand("+262142-12-31T23:59:59.999999999Z".into()),
	)]
	#[case::uuid(
		Value::Uuid(expr::Uuid(Uuid::nil())),
		json!("00000000-0000-0000-0000-000000000000"),
		Value::Strand("00000000-0000-0000-0000-000000000000".into()),
	)]
	#[case::uuid(
		Value::Uuid(expr::Uuid(Uuid::max())),
		json!("ffffffff-ffff-ffff-ffff-ffffffffffff"),
		Value::Strand("ffffffff-ffff-ffff-ffff-ffffffffffff".into()),
	)]
	#[case::bytes(
		Value::Bytes(expr::Bytes(vec![])),
		json!([]),
		Value::Array(expr::Array(vec![])),
	)]
	#[case::bytes(
		Value::Bytes(expr::Bytes(b"foo".to_vec())),
		json!([102, 111, 111]),
		Value::Array(expr::Array(vec![
			Value::Number(expr::Number::Int(102)),
			Value::Number(expr::Number::Int(111)),
			Value::Number(expr::Number::Int(111)),
		])),
	)]
	#[case::thing(
		Value::Thing(Thing { tb: "foo".to_string(), id: "bar".into()}) ,
		json!("foo:bar"),
		Value::Thing(Thing { tb: "foo".to_string(), id: "bar".into()}) ,
	)]
	#[case::array(
		Value::Array(expr::Array(vec![])),
		json!([]),
		Value::Array(expr::Array(vec![])),
	)]
	#[case::array(
		Value::Array(expr::Array(vec![Value::Bool(true), Value::Bool(false)])),
		json!([true, false]),
		Value::Array(expr::Array(vec![Value::Bool(true), Value::Bool(false)])),
	)]
	#[case::object(
		Value::Object(expr::Object(BTreeMap::new())),
		json!({}),
		Value::Object(expr::Object(BTreeMap::new())),
	)]
	#[case::object(
		Value::Object(expr::Object(BTreeMap::from([("done".to_owned(), Value::Bool(true))]))),
		json!({"done": true}),
		Value::Object(expr::Object(BTreeMap::from([("done".to_owned(), Value::Bool(true))]))),
	)]
	#[case::geometry_point(
		Value::Geometry(expr::Geometry::Point(point! { x: 10., y: 20. })),
		json!({ "type": "Point", "coordinates": [10., 20.]}),
		Value::Geometry(expr::Geometry::Point(point! { x: 10., y: 20. })),
	)]
	#[case::geometry_line(
		Value::Geometry(expr::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
		json!({ "type": "LineString", "coordinates": [[0., 0.], [10., 0.]]}),
		Value::Geometry(expr::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])),
	)]
	#[case::geometry_polygon(
		Value::Geometry(expr::Geometry::Polygon(polygon![
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
		Value::Geometry(expr::Geometry::Polygon(polygon![
			(x: -111., y: 45.),
			(x: -111., y: 41.),
			(x: -104., y: 41.),
			(x: -104., y: 45.),
		])),
	)]
	#[case::geometry_multi_point(
		Value::Geometry(expr::Geometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
		json!({ "type": "MultiPoint", "coordinates": [[0., 0.], [1., 2.]]}),
		Value::Geometry(expr::Geometry::MultiPoint(MultiPoint::new(vec![
			point! { x: 0., y: 0. },
			point! { x: 1., y: 2. },
		]))),
	)]
	#[case::geometry_multi_line(
		Value::Geometry(
			expr::Geometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
		json!({ "type": "MultiLineString", "coordinates": [[[0., 0.], [1., 2.]]]}),
		Value::Geometry(
			expr::Geometry::MultiLine(
				MultiLineString::new(vec![
					line_string![( x: 0., y: 0. ), ( x: 1., y: 2. )],
				])
			)
		),
	)]
	#[case::geometry_multi_polygon(
		Value::Geometry(expr::Geometry::MultiPolygon(MultiPolygon::new(vec![
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
	,	Value::Geometry(expr::Geometry::MultiPolygon(MultiPolygon::new(vec![
			polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			],
		]))),
	)]
	#[case::geometry_collection(
		Value::Geometry(expr::Geometry::Collection(vec![])),
		json!({
			"type": "GeometryCollection",
			"geometries": [],
		}),
		Value::Geometry(expr::Geometry::Collection(vec![])),
	)]
	#[case::geometry_collection_with_point(
		Value::Geometry(expr::Geometry::Collection(vec![expr::Geometry::Point(point! { x: 10., y: 20. })])),
		json!({
		"type": "GeometryCollection",
		"geometries": [ { "type": "Point", "coordinates": [10., 20.] } ],
	}),
		Value::Geometry(expr::Geometry::Collection(vec![expr::Geometry::Point(point! { x: 10., y: 20. })])),
	)]
	#[case::geometry_collection_with_line(
		Value::Geometry(expr::Geometry::Collection(vec![expr::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
		json!({
			"type": "GeometryCollection",
			"geometries": [ { "type": "LineString", "coordinates": [[0., 0.], [10., 0.]] } ],
		}),
		Value::Geometry(expr::Geometry::Collection(vec![expr::Geometry::Line(line_string![
			( x: 0., y: 0. ),
			( x: 10., y: 0. ),
		])])),
	)]

	fn test_json(
		#[case] value: Value,
		#[case] expected: Json,
		#[case] expected_deserialized: Value,
	) {
		let json_value = Json::from(value.clone());
		assert_eq!(json_value, expected);

		let json_str = serde_json::to_string(&json_value).expect("Failed to serialize to JSON");
		let deserialized_sql_value = crate::syn::value_legacy_strand(&json_str).unwrap();
		let deserialized: Value = deserialized_sql_value.into();
		assert_eq!(deserialized, expected_deserialized);
	}
}
