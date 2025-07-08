use crate::rpc::protocol::v1::types::{V1Array, V1Geometry, V1Number, V1Object, V1Value};
use serde::Serialize;
use serde_json::Map;
use serde_json::Value as JsonValue;
use serde_json::json;

impl From<V1Value> for serde_json::Value {
	fn from(value: V1Value) -> Self {
		match value {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			V1Value::None | V1Value::Null => JsonValue::Null,
			V1Value::Bool(boolean) => boolean.into(),
			V1Value::Number(number) => match number {
				V1Number::Int(int) => int.into(),
				V1Number::Float(float) => float.into(),
				V1Number::Decimal(decimal) => json!(decimal),
			},
			V1Value::Strand(strand) => strand.0.into(),
			V1Value::Duration(duration) => duration.to_raw().into(),
			V1Value::Datetime(datetime) => json!(datetime.0),
			V1Value::Uuid(uuid) => json!(uuid.0),
			V1Value::Array(array) => JsonValue::Array(Array::from(array).0),
			V1Value::Object(object) => JsonValue::Object(Object::from(object).0),
			V1Value::Geometry(geo) => Geometry::from(geo).0,
			V1Value::Bytes(bytes) => json!(bytes.0),
			V1Value::RecordId(record_id) => record_id.to_string().into(),
			V1Value::Table(table) => json!(table),
			V1Value::Model(model) => json!(model),
			V1Value::File(file) => file.to_string().into(),
		}
	}
}

#[derive(Serialize)]
struct Array(Vec<JsonValue>);

impl From<V1Array> for Array {
	fn from(arr: V1Array) -> Self {
		let mut vec = Vec::with_capacity(arr.len());
		for value in arr {
			vec.push(value.into());
		}
		Self(vec)
	}
}

#[derive(Serialize)]
struct Object(Map<String, JsonValue>);

impl From<V1Object> for Object {
	fn from(obj: V1Object) -> Self {
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

impl From<V1Geometry> for Geometry {
	fn from(geo: V1Geometry) -> Self {
		Self(match geo {
			V1Geometry::Point(v) => json!(Coordinates {
				typ: CoordinatesType::Point,
				coordinates: vec![json!(v.x()), json!(v.y())].into(),
			}),
			V1Geometry::Line(v) => json!(Coordinates {
				typ: CoordinatesType::LineString,
				coordinates: v
					.points()
					.map(|p| vec![json!(p.x()), json!(p.y())].into())
					.collect::<Vec<JsonValue>>()
					.into(),
			}),
			V1Geometry::Polygon(v) => json!(Coordinates {
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
			V1Geometry::MultiPoint(v) => json!(Coordinates {
				typ: CoordinatesType::MultiPoint,
				coordinates: v
					.0
					.iter()
					.map(|v| vec![json!(v.x()), json!(v.y())].into())
					.collect::<Vec<JsonValue>>()
					.into()
			}),
			V1Geometry::MultiLine(v) => json!(Coordinates {
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
			V1Geometry::MultiPolygon(v) => json!(Coordinates {
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
			V1Geometry::Collection(v) => json!(Geometries {
				typ: GeometryCollection,
				geometries: v.into_iter().map(Geometry::from).map(|x| x.0).collect(),
			}),
		})
	}
}

#[cfg(test)]
mod tests {
	mod into_json {
		use crate::rpc::protocol::v1::types::V1Value;
		use crate::sql;
		use crate::sql::from_value;
		use chrono::DateTime;
		use chrono::Utc;
		use geo::LineString;
		use geo::MultiLineString;
		use geo::MultiPoint;
		use geo::MultiPolygon;
		use geo::Point;
		use geo::Polygon;
		use geo::line_string;
		use geo::point;
		use geo::polygon;
		use rust_decimal::Decimal;
		use serde_json::Value as Json;
		use serde_json::json;
		use std::collections::BTreeMap;
		use std::time::Duration;
		use uuid::Uuid;

		#[test]
		fn none_or_null() {
			for value in [V1Value::None, V1Value::Null] {
				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(null));

				let response: Option<String> = from_value(value).unwrap();
				assert_eq!(response, None);
			}
		}

		#[test]
		fn bool() {
			for boolean in [true, false] {
				let value = V1Value::Bool(boolean);

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(boolean));

				let response: bool = from_value(value).unwrap();
				assert_eq!(response, boolean);
			}
		}

		#[test]
		fn number_int() {
			for num in [i64::MIN, 0, i64::MAX] {
				let value = V1Value::V1Number(sql::V1Number::Int(num));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(num));

				let response: i64 = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn number_float() {
			for num in [f64::NEG_INFINITY, f64::MIN, 0.0, f64::MAX, f64::INFINITY, f64::NAN] {
				let value = V1Value::V1Number(sql::V1Number::Float(num));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(num));

				let response: f64 = from_value(value).unwrap();
				if response.is_finite() {
					// Infinity numbers are not comparable
					assert_eq!(response, num);
				}
			}
		}

		#[test]
		fn number_decimal() {
			for num in [i64::MIN, 0, i64::MAX] {
				let num = Decimal::new(num, 0);
				let value = V1Value::V1Number(sql::V1Number::Decimal(num));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(num.to_string()));

				let response: Decimal = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn strand() {
			for str in ["", "foo"] {
				let value = V1Value::Strand(str.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(str));

				let response: String = from_value(value).unwrap();
				assert_eq!(response, str);
			}
		}

		#[test]
		fn duration() {
			for duration in [Duration::ZERO, Duration::MAX] {
				let value = V1Value::Duration(duration.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(sql::Duration(duration).to_raw()));

				let response: Duration = from_value(value).unwrap();
				assert_eq!(response, duration);
			}
		}

		#[test]
		fn datetime() {
			for datetime in [DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC] {
				let value = V1Value::Datetime(datetime.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(datetime));

				let response: DateTime<Utc> = from_value(value).unwrap();
				assert_eq!(response, datetime);
			}
		}

		#[test]
		fn uuid() {
			for uuid in [Uuid::nil(), Uuid::max()] {
				let value = V1Value::Uuid(uuid.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(uuid));

				let response: Uuid = from_value(value).unwrap();
				assert_eq!(response, uuid);
			}
		}

		#[test]
		fn array() {
			for vec in [vec![], vec![true, false]] {
				let value =
					V1Value::Array(sql::Array(vec.iter().copied().map(V1Value::from).collect()));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(vec));

				let response: Vec<bool> = from_value(value).unwrap();
				assert_eq!(response, vec);
			}
		}

		#[test]
		fn object() {
			for map in [BTreeMap::new(), map!("done".to_owned() => true)] {
				let value = V1Value::Object(sql::Object(
					map.iter().map(|(key, value)| (key.clone(), V1Value::from(*value))).collect(),
				));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(map));

				let response: BTreeMap<String, bool> = from_value(value).unwrap();
				assert_eq!(response, map);
			}
		}

		#[test]
		fn geometry_point() {
			let point = point! { x: 10., y: 20. };
			let value = V1Value::Geometry(sql::Geometry::Point(point));

			let simple_json = Json::from(value.clone());
			assert_eq!(simple_json, json!({ "type": "Point", "coordinates": [10., 20.]}));

			let response: Point = from_value(value).unwrap();
			assert_eq!(response, point);
		}

		#[test]
		fn geometry_line() {
			let line_string = line_string![
				( x: 0., y: 0. ),
				( x: 10., y: 0. ),
			];
			let value = V1Value::Geometry(sql::Geometry::Line(line_string.clone()));

			let simple_json = Json::from(value.clone());
			assert_eq!(
				simple_json,
				json!({ "type": "LineString", "coordinates": [[0., 0.], [10., 0.]]})
			);

			let response: LineString = from_value(value).unwrap();
			assert_eq!(response, line_string);
		}

		#[test]
		fn geometry_polygon() {
			let polygon = polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			];
			let value = V1Value::Geometry(sql::Geometry::Polygon(polygon.clone()));

			let simple_json = Json::from(value.clone());
			assert_eq!(
				simple_json,
				json!({ "type": "Polygon", "coordinates": [[
					[-111., 45.],
					[-111., 41.],
					[-104., 41.],
					[-104., 45.],
					[-111., 45.],
				]]})
			);

			let response: Polygon = from_value(value).unwrap();
			assert_eq!(response, polygon);
		}

		#[test]
		fn geometry_multi_point() {
			let multi_point: MultiPoint =
				vec![point! { x: 0., y: 0. }, point! { x: 1., y: 2. }].into();
			let value = V1Value::Geometry(sql::Geometry::MultiPoint(multi_point.clone()));

			let simple_json = Json::from(value.clone());
			assert_eq!(
				simple_json,
				json!({ "type": "MultiPoint", "coordinates": [[0., 0.], [1., 2.]]})
			);

			let response: MultiPoint = from_value(value).unwrap();
			assert_eq!(response, multi_point);
		}

		#[test]
		fn geometry_multi_line() {
			let multi_line = MultiLineString::new(vec![line_string![
					( x: 0., y: 0. ),
					( x: 1., y: 2. ),
			]]);
			let value = V1Value::Geometry(sql::Geometry::MultiLine(multi_line.clone()));

			let simple_json = Json::from(value.clone());
			assert_eq!(
				simple_json,
				json!({ "type": "MultiLineString", "coordinates": [[[0., 0.], [1., 2.]]]})
			);

			let response: MultiLineString = from_value(value).unwrap();
			assert_eq!(response, multi_line);
		}

		#[test]
		fn geometry_multi_polygon() {
			let multi_polygon: MultiPolygon = vec![polygon![
				(x: -111., y: 45.),
				(x: -111., y: 41.),
				(x: -104., y: 41.),
				(x: -104., y: 45.),
			]]
			.into();
			let value = V1Value::Geometry(sql::Geometry::MultiPolygon(multi_polygon.clone()));

			let simple_json = Json::from(value.clone());
			assert_eq!(
				simple_json,
				json!({ "type": "MultiPolygon", "coordinates": [[[
					[-111., 45.],
					[-111., 41.],
					[-104., 41.],
					[-104., 45.],
					[-111., 45.],
				]]]})
			);

			let response: MultiPolygon = from_value(value).unwrap();
			assert_eq!(response, multi_polygon);
		}

		#[test]
		fn geometry_collection() {
			for geometries in [vec![], vec![sql::Geometry::Point(point! { x: 10., y: 20. })]] {
				let value = V1Value::Geometry(geometries.clone().into());

				let simple_json = Json::from(value.clone());
				assert_eq!(
					simple_json,
					json!({
						"type": "GeometryCollection",
						"geometries": geometries.clone().into_iter().map(|geo| Json::from(V1Value::from(geo))).collect::<Vec<_>>(),
					})
				);

				let response: Vec<sql::Geometry> = from_value(value).unwrap();
				assert_eq!(response, geometries);
			}
		}

		#[test]
		fn bytes() {
			for bytes in [vec![], b"foo".to_vec()] {
				let value = V1Value::Bytes(sql::Bytes(bytes.clone()));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(bytes));

				let sql::Bytes(response) = from_value(value).unwrap();
				assert_eq!(response, bytes);
			}
		}

		#[test]
		fn thing() {
			let record_id = "foo:bar";
			let thing = sql::thing(record_id).unwrap();
			let value = V1Value::Thing(thing.clone());

			let simple_json = Json::from(value.clone());
			assert_eq!(simple_json, json!(record_id));

			let response: sql::Thing = from_value(value).unwrap();
			assert_eq!(response, thing);
		}
	}
}
