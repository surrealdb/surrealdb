use crate::sql::constant::ConstantValue;
use crate::sql::id::Gen;
use crate::sql::Value;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use serde_json::Map;
use serde_json::Value as JsonValue;

impl From<Value> for serde_json::Value {
	fn from(value: Value) -> Self {
		into_json(value, true)
	}
}

fn into_json(value: Value, simplify: bool) -> JsonValue {
	use crate::sql;
	use crate::sql::Number;

	#[derive(Serialize)]
	struct Array(Vec<JsonValue>);

	impl From<(sql::Array, bool)> for Array {
		fn from((arr, simplify): (sql::Array, bool)) -> Self {
			let mut vec = Vec::with_capacity(arr.0.len());
			for value in arr.0 {
				vec.push(into_json(value, simplify));
			}
			Self(vec)
		}
	}

	#[derive(Serialize)]
	struct Object(Map<String, JsonValue>);

	impl From<(sql::Object, bool)> for Object {
		fn from((obj, simplify): (sql::Object, bool)) -> Self {
			let mut map = Map::with_capacity(obj.0.len());
			for (key, value) in obj.0 {
				map.insert(key.to_owned(), into_json(value, simplify));
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
					coordinates: vec![v
						.exterior()
						.points()
						.map(|p| vec![json!(p.x()), json!(p.y())].into())
						.collect::<Vec<JsonValue>>()]
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
							vec![v
								.exterior()
								.points()
								.map(|p| vec![json!(p.x()), json!(p.y())].into())
								.collect::<Vec<JsonValue>>()]
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

	#[derive(Serialize)]
	enum Id {
		Number(i64),
		String(String),
		Array(Array),
		Object(Object),
	}

	impl From<(sql::Id, bool)> for Id {
		fn from((id, simplify): (sql::Id, bool)) -> Self {
			match id {
				sql::Id::Number(n) => Id::Number(n),
				sql::Id::String(s) => Id::String(s),
				sql::Id::Array(arr) => Id::Array((arr, simplify).into()),
				sql::Id::Object(obj) => Id::Object((obj, simplify).into()),
				sql::Id::Generate(v) => match v {
					Gen::Rand => Id::from((sql::Id::rand(), simplify)),
					Gen::Ulid => Id::from((sql::Id::ulid(), simplify)),
					Gen::Uuid => Id::from((sql::Id::uuid(), simplify)),
				},
			}
		}
	}

	#[derive(Serialize)]
	struct Thing {
		tb: String,
		id: Id,
	}

	impl From<(sql::Thing, bool)> for Thing {
		fn from((thing, simplify): (sql::Thing, bool)) -> Self {
			Self {
				tb: thing.tb,
				id: (thing.id, simplify).into(),
			}
		}
	}

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
		Value::Strand(strand) => strand.into(),
		Value::Duration(duration) => match simplify {
			true => duration.to_raw().into(),
			false => json!(duration.0),
		},
		Value::Datetime(datetime) => json!(datetime.0),
		Value::Uuid(uuid) => json!(uuid.0),
		Value::Array(array) => JsonValue::Array(Array::from((array, simplify)).0),
		Value::Object(object) => JsonValue::Object(Object::from((object, simplify)).0),
		Value::Geometry(geo) => match simplify {
			true => Geometry::from(geo).0,
			false => match geo {
				sql::Geometry::Point(geo) => json!(geo),
				sql::Geometry::Line(geo) => json!(geo),
				sql::Geometry::Polygon(geo) => json!(geo),
				sql::Geometry::MultiPoint(geo) => json!(geo),
				sql::Geometry::MultiLine(geo) => json!(geo),
				sql::Geometry::MultiPolygon(geo) => json!(geo),
				sql::Geometry::Collection(geo) => json!(geo),
			},
		},
		Value::Bytes(bytes) => json!(bytes.0),
		Value::Thing(thing) => match simplify {
			true => thing.to_string().into(),
			false => json!(thing),
		},
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
		Value::Constant(constant) => match simplify {
			true => match constant.value() {
				ConstantValue::Datetime(datetime) => json!(datetime.0),
				ConstantValue::Float(float) => float.into(),
			},
			false => json!(constant),
		},
		Value::Cast(cast) => json!(cast),
		Value::Function(function) => json!(function),
		Value::Model(model) => json!(model),
		Value::Query(query) => json!(query),
		Value::Subquery(subquery) => json!(subquery),
		Value::Expression(expression) => json!(expression),
	}
}

#[derive(Debug, Clone)]
#[doc(hidden)]
#[non_exhaustive]
pub struct FromValueError {
	pub value: Value,
	pub error: String,
}

/// Deserializes a value `T` from `SurrealDB` [`Value`]
#[doc(hidden)]
pub fn from_value<T>(value: Value) -> Result<T, FromValueError>
where
	T: DeserializeOwned,
{
	let json = into_json(value.clone(), false);
	serde_json::from_value(json).map_err(|error| FromValueError {
		value,
		error: error.to_string(),
	})
}

#[cfg(test)]
mod tests {
	mod into_json {
		use crate::sql;
		use crate::sql::value::serde::de::from_value;
		use crate::sql::value::serde::de::into_json;
		use crate::sql::Value;
		use chrono::DateTime;
		use chrono::Utc;
		use geo::line_string;
		use geo::point;
		use geo::polygon;
		use geo::LineString;
		use geo::MultiLineString;
		use geo::MultiPoint;
		use geo::MultiPolygon;
		use geo::Point;
		use geo::Polygon;
		use rust_decimal::Decimal;
		use serde_json::json;
		use std::collections::BTreeMap;
		use std::time::Duration;
		use uuid::Uuid;

		#[test]
		fn none_or_null() {
			for value in [Value::None, Value::Null] {
				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(null));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(null));

				let response: Option<String> = from_value(value).unwrap();
				assert_eq!(response, None);
			}
		}

		#[test]
		fn bool() {
			for boolean in [true, false] {
				let value = Value::Bool(boolean);

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(boolean));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(boolean));

				let response: bool = from_value(value).unwrap();
				assert_eq!(response, boolean);
			}
		}

		#[test]
		fn number_int() {
			for num in [i64::MIN, 0, i64::MAX] {
				let value = Value::Number(sql::Number::Int(num));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(num));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(num));

				let response: i64 = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn number_float() {
			for num in [f64::NEG_INFINITY, f64::MIN, 0.0, f64::MAX, f64::INFINITY, f64::NAN] {
				let value = Value::Number(sql::Number::Float(num));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(num));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(num));

				if num.is_finite() {
					let response: f64 = from_value(value).unwrap();
					assert_eq!(response, num);
				} else {
					let response: Option<f64> = from_value(value).unwrap();
					assert_eq!(response, None);
				}
			}
		}

		#[test]
		fn number_decimal() {
			for num in [i64::MIN, 0, i64::MAX] {
				let num = Decimal::new(num, 0);
				let value = Value::Number(sql::Number::Decimal(num));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(num.to_string()));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(num));

				let response: Decimal = from_value(value).unwrap();
				assert_eq!(response, num);
			}
		}

		#[test]
		fn strand() {
			for str in ["", "foo"] {
				let value = Value::Strand(str.into());

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(str));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(str));

				let response: String = from_value(value).unwrap();
				assert_eq!(response, str);
			}
		}

		#[test]
		fn duration() {
			for duration in [Duration::ZERO, Duration::MAX] {
				let value = Value::Duration(duration.into());

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(sql::Duration(duration).to_raw()));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(duration));

				let response: Duration = from_value(value).unwrap();
				assert_eq!(response, duration);
			}
		}

		#[test]
		fn datetime() {
			for datetime in [DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC] {
				let value = Value::Datetime(datetime.into());

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(datetime));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(datetime));

				let response: DateTime<Utc> = from_value(value).unwrap();
				assert_eq!(response, datetime);
			}
		}

		#[test]
		fn uuid() {
			for uuid in [Uuid::nil(), Uuid::max()] {
				let value = Value::Uuid(uuid.into());

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(uuid));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(uuid));

				let response: Uuid = from_value(value).unwrap();
				assert_eq!(response, uuid);
			}
		}

		#[test]
		fn array() {
			for vec in [vec![], vec![true, false]] {
				let value =
					Value::Array(sql::Array(vec.iter().copied().map(Value::from).collect()));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(vec));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(vec));

				let response: Vec<bool> = from_value(value).unwrap();
				assert_eq!(response, vec);
			}
		}

		#[test]
		fn object() {
			for map in [BTreeMap::new(), map!("done".to_owned() => true)] {
				let value = Value::Object(sql::Object(
					map.iter().map(|(key, value)| (key.clone(), Value::from(*value))).collect(),
				));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(map));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(map));

				let response: BTreeMap<String, bool> = from_value(value).unwrap();
				assert_eq!(response, map);
			}
		}

		#[test]
		fn geometry_point() {
			let point = point! { x: 10., y: 20. };
			let value = Value::Geometry(sql::Geometry::Point(point));

			let simple_json = into_json(value.clone(), true);
			assert_eq!(simple_json, json!({ "type": "Point", "coordinates": [10., 20.]}));

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(point));

			let response: Point = from_value(value).unwrap();
			assert_eq!(response, point);
		}

		#[test]
		fn geometry_line() {
			let line_string = line_string![
				( x: 0., y: 0. ),
				( x: 10., y: 0. ),
			];
			let value = Value::Geometry(sql::Geometry::Line(line_string.clone()));

			let simple_json = into_json(value.clone(), true);
			assert_eq!(
				simple_json,
				json!({ "type": "LineString", "coordinates": [[0., 0.], [10., 0.]]})
			);

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(line_string));

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
			let value = Value::Geometry(sql::Geometry::Polygon(polygon.clone()));

			let simple_json = into_json(value.clone(), true);
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

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(polygon));

			let response: Polygon = from_value(value).unwrap();
			assert_eq!(response, polygon);
		}

		#[test]
		fn geometry_multi_point() {
			let multi_point: MultiPoint =
				vec![point! { x: 0., y: 0. }, point! { x: 1., y: 2. }].into();
			let value = Value::Geometry(sql::Geometry::MultiPoint(multi_point.clone()));

			let simple_json = into_json(value.clone(), true);
			assert_eq!(
				simple_json,
				json!({ "type": "MultiPoint", "coordinates": [[0., 0.], [1., 2.]]})
			);

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(multi_point));

			let response: MultiPoint = from_value(value).unwrap();
			assert_eq!(response, multi_point);
		}

		#[test]
		fn geometry_multi_line() {
			let multi_line = MultiLineString::new(vec![line_string![
					( x: 0., y: 0. ),
					( x: 1., y: 2. ),
			]]);
			let value = Value::Geometry(sql::Geometry::MultiLine(multi_line.clone()));

			let simple_json = into_json(value.clone(), true);
			assert_eq!(
				simple_json,
				json!({ "type": "MultiLineString", "coordinates": [[[0., 0.], [1., 2.]]]})
			);

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(multi_line));

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
			let value = Value::Geometry(sql::Geometry::MultiPolygon(multi_polygon.clone()));

			let simple_json = into_json(value.clone(), true);
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

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(multi_polygon));

			let response: MultiPolygon = from_value(value).unwrap();
			assert_eq!(response, multi_polygon);
		}

		#[test]
		fn geometry_collection() {
			for geometries in [vec![], vec![sql::Geometry::Point(point! { x: 10., y: 20. })]] {
				let value = Value::Geometry(geometries.clone().into());

				let simple_json = into_json(value.clone(), true);
				assert_eq!(
					simple_json,
					json!({
						"type": "GeometryCollection",
						"geometries": geometries.clone().into_iter().map(|geo| into_json(Value::from(geo), true)).collect::<Vec<_>>(),
					})
				);

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(geometries));

				let response: Vec<sql::Geometry> = from_value(value).unwrap();
				assert_eq!(response, geometries);
			}
		}

		#[test]
		fn bytes() {
			for bytes in [vec![], b"foo".to_vec()] {
				let value = Value::Bytes(sql::Bytes(bytes.clone()));

				let simple_json = into_json(value.clone(), true);
				assert_eq!(simple_json, json!(bytes));

				let json = into_json(value.clone(), false);
				assert_eq!(json, json!(bytes));

				let response: Vec<u8> = from_value(value).unwrap();
				assert_eq!(response, bytes);
			}
		}

		#[test]
		fn thing() {
			let record_id = "foo:bar";
			let thing = sql::thing(record_id).unwrap();
			let value = Value::Thing(thing.clone());

			let simple_json = into_json(value.clone(), true);
			assert_eq!(simple_json, json!(record_id));

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(thing));

			let response: sql::Thing = from_value(value).unwrap();
			assert_eq!(response, thing);
		}
	}
}
