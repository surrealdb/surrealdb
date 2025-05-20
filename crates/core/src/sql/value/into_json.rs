use crate::sql;
use crate::sql::Number;
use crate::sql::SqlValue;
use crate::sql::constant::ConstantValue;
use serde::Serialize;
use serde_json::Map;
use serde_json::Value as JsonValue;
use serde_json::json;

impl From<SqlValue> for serde_json::Value {
	fn from(value: SqlValue) -> Self {
		match value {
			// These value types are simple values which
			// can be used in query responses sent to
			// the client.
			SqlValue::None | SqlValue::Null => JsonValue::Null,
			SqlValue::Bool(boolean) => boolean.into(),
			SqlValue::Number(number) => match number {
				Number::Int(int) => int.into(),
				Number::Float(float) => float.into(),
				Number::Decimal(decimal) => json!(decimal),
			},
			SqlValue::Strand(strand) => strand.0.into(),
			SqlValue::Duration(duration) => duration.to_raw().into(),
			SqlValue::Datetime(datetime) => json!(datetime.0),
			SqlValue::Uuid(uuid) => json!(uuid.0),
			SqlValue::Array(array) => JsonValue::Array(Array::from(array).0),
			SqlValue::Object(object) => JsonValue::Object(Object::from(object).0),
			SqlValue::Geometry(geo) => Geometry::from(geo).0,
			SqlValue::Bytes(bytes) => json!(bytes.0),
			SqlValue::Thing(thing) => thing.to_string().into(),
			// These Value types are un-computed values
			// and are not used in query responses sent
			// to the client.
			SqlValue::Param(param) => json!(param),
			SqlValue::Idiom(idiom) => json!(idiom),
			SqlValue::Table(table) => json!(table),
			SqlValue::Mock(mock) => json!(mock),
			SqlValue::Regex(regex) => json!(regex),
			SqlValue::Block(block) => json!(block),
			SqlValue::Range(range) => json!(range),
			SqlValue::Edges(edges) => json!(edges),
			SqlValue::Future(future) => json!(future),
			SqlValue::Constant(constant) => match constant.value() {
				ConstantValue::Datetime(datetime) => json!(datetime.0),
				ConstantValue::Float(float) => float.into(),
				ConstantValue::Duration(duration) => duration.to_string().into(),
			},
			SqlValue::Cast(cast) => json!(cast),
			SqlValue::Function(function) => json!(function),
			SqlValue::Model(model) => json!(model),
			SqlValue::Query(query) => json!(query),
			SqlValue::Subquery(subquery) => json!(subquery),
			SqlValue::Expression(expression) => json!(expression),
			SqlValue::Closure(closure) => json!(closure),
			SqlValue::Refs(_) => json!(sql::Array::new()),
			SqlValue::File(file) => file.to_string().into(),
		}
	}
}

#[derive(Serialize)]
struct Array(Vec<JsonValue>);

impl From<sql::Array> for Array {
	fn from(arr: sql::Array) -> Self {
		let mut vec = Vec::with_capacity(arr.len());
		for value in arr {
			vec.push(value.into());
		}
		Self(vec)
	}
}

#[derive(Serialize)]
struct Object(Map<String, JsonValue>);

impl From<sql::Object> for Object {
	fn from(obj: sql::Object) -> Self {
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

impl From<sql::Geometry> for Geometry {
	fn from(geo: sql::Geometry) -> Self {
		Self(match geo {
			sql::Geometry::Point(v) => json!(Coordinates {
				typ: CoordinatesType::Point,
				coordinates: vec![json!(v.x()), json!(v.y())].into(),
			}),
			sql::Geometry::Line(v) => json!(Coordinates {
				typ: CoordinatesType::LineString,
				coordinates: v
					.points()
					.map(|p| vec![json!(p.x()), json!(p.y())].into())
					.collect::<Vec<JsonValue>>()
					.into(),
			}),
			sql::Geometry::Polygon(v) => json!(Coordinates {
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
			sql::Geometry::MultiPoint(v) => json!(Coordinates {
				typ: CoordinatesType::MultiPoint,
				coordinates: v
					.0
					.iter()
					.map(|v| vec![json!(v.x()), json!(v.y())].into())
					.collect::<Vec<JsonValue>>()
					.into()
			}),
			sql::Geometry::MultiLine(v) => json!(Coordinates {
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
			sql::Geometry::MultiPolygon(v) => json!(Coordinates {
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
			sql::Geometry::Collection(v) => json!(Geometries {
				typ: GeometryCollection,
				geometries: v.into_iter().map(Geometry::from).map(|x| x.0).collect(),
			}),
		})
	}
}

#[cfg(test)]
mod tests {
	mod into_json {
		use crate::sql;
		use crate::sql::SqlValue;
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
			for value in [SqlValue::None, SqlValue::Null] {
				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(null));

				let response: Option<String> = from_value(value).unwrap();
				assert_eq!(response, None);
			}
		}

		#[test]
		fn bool() {
			for boolean in [true, false] {
				let value = SqlValue::Bool(boolean);

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(boolean));

				let response: bool = from_value(value).unwrap();
				assert_eq!(response, boolean);
			}
		}

		#[test]
		fn number_int() {
			for num in [i64::MIN, 0, i64::MAX] {
				let value = SqlValue::Number(sql::Number::Int(num));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(num));

				let response: i64 = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn number_float() {
			for num in [f64::NEG_INFINITY, f64::MIN, 0.0, f64::MAX, f64::INFINITY, f64::NAN] {
				let value = SqlValue::Number(sql::Number::Float(num));

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
				let value = SqlValue::Number(sql::Number::Decimal(num));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(num.to_string()));

				let response: Decimal = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn strand() {
			for str in ["", "foo"] {
				let value = SqlValue::Strand(str.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(str));

				let response: String = from_value(value).unwrap();
				assert_eq!(response, str);
			}
		}

		#[test]
		fn duration() {
			for duration in [Duration::ZERO, Duration::MAX] {
				let value = SqlValue::Duration(duration.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(sql::Duration(duration).to_raw()));

				let response: Duration = from_value(value).unwrap();
				assert_eq!(response, duration);
			}
		}

		#[test]
		fn datetime() {
			for datetime in [DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC] {
				let value = SqlValue::Datetime(datetime.into());

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(datetime));

				let response: DateTime<Utc> = from_value(value).unwrap();
				assert_eq!(response, datetime);
			}
		}

		#[test]
		fn uuid() {
			for uuid in [Uuid::nil(), Uuid::max()] {
				let value = SqlValue::Uuid(uuid.into());

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
					SqlValue::Array(sql::Array(vec.iter().copied().map(SqlValue::from).collect()));

				let simple_json = Json::from(value.clone());
				assert_eq!(simple_json, json!(vec));

				let response: Vec<bool> = from_value(value).unwrap();
				assert_eq!(response, vec);
			}
		}

		#[test]
		fn object() {
			for map in [BTreeMap::new(), map!("done".to_owned() => true)] {
				let value = SqlValue::Object(sql::Object(
					map.iter().map(|(key, value)| (key.clone(), SqlValue::from(*value))).collect(),
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
			let value = SqlValue::Geometry(sql::Geometry::Point(point));

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
			let value = SqlValue::Geometry(sql::Geometry::Line(line_string.clone()));

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
			let value = SqlValue::Geometry(sql::Geometry::Polygon(polygon.clone()));

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
			let value = SqlValue::Geometry(sql::Geometry::MultiPoint(multi_point.clone()));

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
			let value = SqlValue::Geometry(sql::Geometry::MultiLine(multi_line.clone()));

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
			let value = SqlValue::Geometry(sql::Geometry::MultiPolygon(multi_polygon.clone()));

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
				let value = SqlValue::Geometry(geometries.clone().into());

				let simple_json = Json::from(value.clone());
				assert_eq!(
					simple_json,
					json!({
						"type": "GeometryCollection",
						"geometries": geometries.clone().into_iter().map(|geo| Json::from(SqlValue::from(geo))).collect::<Vec<_>>(),
					})
				);

				let response: Vec<sql::Geometry> = from_value(value).unwrap();
				assert_eq!(response, geometries);
			}
		}

		#[test]
		fn bytes() {
			for bytes in [vec![], b"foo".to_vec()] {
				let value = SqlValue::Bytes(sql::Bytes(bytes.clone()));

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
			let value = SqlValue::Thing(thing.clone());

			let simple_json = Json::from(value.clone());
			assert_eq!(simple_json, json!(record_id));

			let response: sql::Thing = from_value(value).unwrap();
			assert_eq!(response, thing);
		}
	}
}
