pub(super) mod map;
pub(super) mod opt;
pub(super) mod vec;

use crate::array::Array;
use crate::constant::ConstantValue;
use crate::err::Error;
use crate::id::Gen;
use crate::object::Object;
use crate::value::serde::ser;
use crate::value::Value;
use crate::Block;
use crate::Bytes;
use crate::Datetime;
use crate::Duration;
use crate::Future;
use crate::Ident;
use crate::Idiom;
use crate::Param;
use crate::Query;
use crate::Statements;
use crate::Strand;
use crate::Table;
use crate::Uuid;
use map::SerializeValueMap;
use rust_decimal::Decimal;
use ser::cast::SerializeCast;
use ser::edges::SerializeEdges;
use ser::expression::SerializeExpression;
use ser::function::SerializeFunction;
use ser::mock::SerializeMock;
use ser::range::SerializeRange;
use ser::thing::SerializeThing;
use ser::Serializer as _;
use serde::de::DeserializeOwned;
use serde::ser::Error as _;
use serde::ser::SerializeMap as _;
use serde::ser::SerializeSeq as _;
use serde::Serialize;
use serde_json::json;
use serde_json::Map;
use serde_json::Value as JsonValue;
use std::fmt::Display;
use storekey::encode::Error as EncodeError;
use vec::SerializeValueVec;

/// Convert a `T` into `surrealdb::crate::Value` which is an enum that can represent any valid SQL data.
pub fn to_value<T>(value: T) -> Result<Value, Error>
where
	T: Serialize,
{
	value.serialize(Serializer.wrap())
}

impl serde::ser::Error for Error {
	fn custom<T>(msg: T) -> Self
	where
		T: Display,
	{
		Self::Encode(EncodeError::Message(msg.to_string()))
	}
}

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Value;
	type Error = Error;

	type SerializeSeq = SerializeArray;
	type SerializeTuple = SerializeArray;
	type SerializeTupleStruct = SerializeTupleStruct;
	type SerializeTupleVariant = SerializeTupleVariant;
	type SerializeMap = SerializeMap;
	type SerializeStruct = SerializeStruct;
	type SerializeStructVariant = SerializeStructVariant;

	const EXPECTED: &'static str = "an enum `Value`";

	#[inline]
	fn serialize_bool(self, value: bool) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i8(self, value: i8) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i16(self, value: i16) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i32(self, value: i32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	fn serialize_i128(self, value: i128) -> Result<Self::Ok, Error> {
		// TODO: Replace with native 128-bit integer support.
		// #[allow(clippy::unnecessary_fallible_conversions)] // `Decimal::from` can panic
		// `clippy::unnecessary_fallible_conversions` not available on Rust < v1.75
		#[allow(warnings)]
		match Decimal::try_from(value) {
			Ok(decimal) => Ok(decimal.into()),
			_ => Err(Error::TryFrom(value.to_string(), "Decimal")),
		}
	}

	#[inline]
	fn serialize_u8(self, value: u8) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u16(self, value: u16) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u32(self, value: u32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_u64(self, value: u64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	fn serialize_u128(self, value: u128) -> Result<Self::Ok, Error> {
		// TODO: replace with native 128-bit integer support.
		// #[allow(clippy::unnecessary_fallible_conversions)] // `Decimal::from` can panic
		// `clippy::unnecessary_fallible_conversions` not available on Rust < v1.75
		#[allow(warnings)]
		match Decimal::try_from(value) {
			Ok(decimal) => Ok(decimal.into()),
			_ => Err(Error::TryFrom(value.to_string(), "Decimal")),
		}
	}

	#[inline]
	fn serialize_f32(self, value: f32) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_f64(self, value: f64) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	#[inline]
	fn serialize_char(self, value: char) -> Result<Self::Ok, Error> {
		Ok({
			let mut s = String::new();
			s.push(value);
			s
		}
		.into())
	}

	#[inline]
	fn serialize_str(self, value: &str) -> Result<Self::Ok, Error> {
		Ok(value.into())
	}

	fn serialize_bytes(self, value: &[u8]) -> Result<Self::Ok, Error> {
		Ok(Value::Bytes(Bytes(value.to_owned())))
	}

	#[inline]
	fn serialize_unit(self) -> Result<Self::Ok, Error> {
		Ok(Value::None)
	}

	#[inline]
	fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Error> {
		self.serialize_unit()
	}

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match name {
			crate::constant::TOKEN => ser::constant::Serializer
				.serialize_unit_variant(name, variant_index, variant)
				.map(Value::Constant),
			crate::value::TOKEN => match variant {
				"None" => Ok(Value::None),
				"Null" => Ok(Value::Null),
				variant => Err(Error::custom(format!("unknown unit variant `Value::{variant}`"))),
			},
			_ => self.serialize_str(variant),
		}
	}

	#[inline]
	fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match name {
			crate::strand::TOKEN => {
				Ok(Value::Strand(Strand(value.serialize(ser::string::Serializer.wrap())?)))
			}
			crate::block::TOKEN => Ok(Value::Block(Box::new(Block(
				value.serialize(ser::block::entry::vec::Serializer.wrap())?,
			)))),
			crate::duration::TOKEN => {
				Ok(Value::Duration(Duration(value.serialize(ser::duration::Serializer.wrap())?)))
			}
			crate::future::TOKEN => Ok(Value::Future(Box::new(Future(Block(
				value.serialize(ser::block::entry::vec::Serializer.wrap())?,
			))))),
			crate::regex::TOKEN => {
				Ok(Value::Regex(value.serialize(ser::string::Serializer.wrap())?.parse().unwrap()))
			}
			crate::table::TOKEN => {
				Ok(Value::Table(Table(value.serialize(ser::string::Serializer.wrap())?)))
			}
			crate::idiom::TOKEN => {
				Ok(Value::Idiom(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?)))
			}
			crate::param::TOKEN => {
				Ok(Value::Param(Param(Ident(value.serialize(ser::string::Serializer.wrap())?))))
			}
			crate::query::TOKEN => Ok(Value::Query(Query(Statements(
				value.serialize(ser::statement::vec::Serializer.wrap())?,
			)))),
			crate::array::TOKEN => {
				Ok(Value::Array(Array(value.serialize(vec::Serializer.wrap())?)))
			}
			crate::object::TOKEN => {
				Ok(Value::Object(Object(value.serialize(map::Serializer.wrap())?)))
			}
			crate::uuid::TOKEN => {
				Ok(Value::Uuid(Uuid(value.serialize(ser::uuid::Serializer.wrap())?)))
			}
			crate::datetime::TOKEN => {
				Ok(Value::Datetime(Datetime(value.serialize(ser::datetime::Serializer.wrap())?)))
			}
			_ => value.serialize(self.wrap()),
		}
	}

	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match name {
			crate::number::TOKEN => {
				Ok(Value::Number(ser::number::Serializer.serialize_newtype_variant(
					name,
					variant_index,
					variant,
					value,
				)?))
			}
			crate::subquery::TOKEN => {
				Ok(Value::Subquery(Box::new(ser::subquery::Serializer.serialize_newtype_variant(
					name,
					variant_index,
					variant,
					value,
				)?)))
			}
			crate::geometry::TOKEN => {
				Ok(Value::Geometry(ser::geometry::Serializer.serialize_newtype_variant(
					name,
					variant_index,
					variant,
					value,
				)?))
			}
			crate::value::TOKEN => value.serialize(Serializer.wrap()),
			_ => Ok(map! {
				String::from(variant) => value.serialize(Serializer.wrap())?,
			}
			.into()),
		}
	}

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Error> {
		self.serialize_unit()
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}

	fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Error> {
		let inner = vec::SerializeValueVec(Vec::with_capacity(len.unwrap_or_default()));
		Ok(SerializeArray(inner))
	}

	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Error> {
		self.serialize_seq(Some(len))
	}

	fn serialize_tuple_struct(
		self,
		name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Error> {
		match name {
			crate::cast::TOKEN => Ok(SerializeTupleStruct::Cast(Default::default())),
			_ => Ok(SerializeTupleStruct::Array(Default::default())),
		}
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		variant_index: u32,
		variant: &'static str,
		len: usize,
	) -> Result<Self::SerializeTupleVariant, Error> {
		Ok(match name {
			crate::mock::TOKEN => SerializeTupleVariant::Model(
				ser::mock::Serializer.serialize_tuple_variant(name, variant_index, variant, len)?,
			),
			crate::function::TOKEN => {
				SerializeTupleVariant::Function(ser::function::Serializer.serialize_tuple_variant(
					name,
					variant_index,
					variant,
					len,
				)?)
			}
			_ => SerializeTupleVariant::Unknown {
				variant,
				fields: SerializeValueVec(Vec::with_capacity(len)),
			},
		})
	}

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Error> {
		Ok(SerializeMap(Default::default()))
	}

	fn serialize_struct(
		self,
		name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(match name {
			crate::thing::TOKEN => SerializeStruct::Thing(Default::default()),
			crate::edges::TOKEN => SerializeStruct::Edges(Default::default()),
			crate::range::TOKEN => SerializeStruct::Range(Default::default()),
			_ => SerializeStruct::Unknown(Default::default()),
		})
	}

	fn serialize_struct_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Error> {
		Ok(if name == crate::expression::TOKEN {
			SerializeStructVariant::Expression(match variant {
				"Unary" => SerializeExpression::Unary(Default::default()),
				"Binary" => SerializeExpression::Binary(Default::default()),
				_ => return Err(Error::custom(format!("unexpected `Expression::{name}`"))),
			})
		} else {
			SerializeStructVariant::Object {
				name: String::from(variant),
				map: Object::default(),
			}
		})
	}
}

#[derive(Default)]
pub(super) struct SerializeArray(vec::SerializeValueVec);

impl serde::ser::SerializeSeq for SerializeArray {
	type Ok = Value;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_element(value)
	}

	fn end(self) -> Result<Value, Error> {
		Ok(Value::Array(Array(self.0.end()?)))
	}
}

impl serde::ser::SerializeTuple for SerializeArray {
	type Ok = Value;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		serde::ser::SerializeSeq::serialize_element(self, value)
	}

	fn end(self) -> Result<Value, Error> {
		serde::ser::SerializeSeq::end(self)
	}
}

impl serde::ser::SerializeTupleStruct for SerializeArray {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		serde::ser::SerializeSeq::serialize_element(self, value)
	}

	fn end(self) -> Result<Value, Error> {
		serde::ser::SerializeSeq::end(self)
	}
}

pub(super) struct SerializeMap(map::SerializeValueMap);

impl serde::ser::SerializeMap for SerializeMap {
	type Ok = Value;
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_key(key)
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		self.0.serialize_value(value)
	}

	fn end(self) -> Result<Value, Error> {
		Ok(Value::Object(Object(self.0.end()?)))
	}
}

pub(super) enum SerializeTupleVariant {
	Model(SerializeMock),
	Function(SerializeFunction),
	Unknown {
		variant: &'static str,
		fields: SerializeValueVec,
	},
}

pub(super) enum SerializeTupleStruct {
	Cast(SerializeCast),
	Array(SerializeArray),
}

impl serde::ser::SerializeTupleStruct for SerializeTupleStruct {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Cast(cast) => cast.serialize_field(value),
			Self::Array(array) => array.serialize_field(value),
		}
	}

	fn end(self) -> Result<Value, Error> {
		match self {
			Self::Cast(cast) => Ok(Value::Cast(Box::new(cast.end()?))),
			Self::Array(array) => Ok(serde::ser::SerializeTupleStruct::end(array)?),
		}
	}
}

impl serde::ser::SerializeTupleVariant for SerializeTupleVariant {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Model(model) => model.serialize_field(value),
			Self::Function(function) => function.serialize_field(value),
			Self::Unknown {
				ref mut fields,
				..
			} => fields.serialize_element(value),
		}
	}

	fn end(self) -> Result<Value, Error> {
		match self {
			Self::Model(model) => Ok(Value::Mock(model.end()?)),
			Self::Function(function) => Ok(Value::Function(Box::new(function.end()?))),
			Self::Unknown {
				variant,
				fields,
			} => Ok(map! {
				variant.to_owned() => Value::Array(Array(fields.end()?)),
			}
			.into()),
		}
	}
}

pub(super) enum SerializeStruct {
	Thing(SerializeThing),
	Edges(SerializeEdges),
	Range(SerializeRange),
	Unknown(SerializeValueMap),
}

impl serde::ser::SerializeStruct for SerializeStruct {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Thing(thing) => thing.serialize_field(key, value),
			Self::Edges(edges) => edges.serialize_field(key, value),
			Self::Range(range) => range.serialize_field(key, value),
			Self::Unknown(map) => map.serialize_entry(key, value),
		}
	}

	fn end(self) -> Result<Value, Error> {
		match self {
			Self::Thing(thing) => Ok(Value::Thing(thing.end()?)),
			Self::Edges(edges) => Ok(Value::Edges(Box::new(edges.end()?))),
			Self::Range(range) => Ok(Value::Range(Box::new(range.end()?))),
			Self::Unknown(map) => Ok(Value::Object(Object(map.end()?))),
		}
	}
}

pub(super) enum SerializeStructVariant {
	Expression(SerializeExpression),
	Object {
		name: String,
		map: Object,
	},
}

impl serde::ser::SerializeStructVariant for SerializeStructVariant {
	type Ok = Value;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Expression(expression) => expression.serialize_field(key, value),
			Self::Object {
				map,
				..
			} => {
				map.0.insert(String::from(key), value.serialize(Serializer.wrap())?);
				Ok(())
			}
		}
	}

	fn end(self) -> Result<Value, Error> {
		match self {
			Self::Expression(expression) => Ok(Value::from(expression.end()?)),
			Self::Object {
				name,
				map,
			} => {
				let mut object = Object::default();

				object.insert(name, Value::Object(map));

				Ok(Value::Object(object))
			}
		}
	}
}

impl From<Value> for serde_json::Value {
	fn from(value: Value) -> Self {
		into_json(value, true)
	}
}

fn into_json(value: Value, simplify: bool) -> JsonValue {
	use crate::Number;

	#[derive(Serialize)]
	struct Array(Vec<JsonValue>);

	impl From<(crate::Array, bool)> for Array {
		fn from((arr, simplify): (crate::Array, bool)) -> Self {
			let mut vec = Vec::with_capacity(arr.0.len());
			for value in arr.0 {
				vec.push(into_json(value, simplify));
			}
			Self(vec)
		}
	}

	#[derive(Serialize)]
	struct Object(Map<String, JsonValue>);

	impl From<(crate::Object, bool)> for Object {
		fn from((obj, simplify): (crate::Object, bool)) -> Self {
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

	impl From<crate::Geometry> for Geometry {
		fn from(geo: crate::Geometry) -> Self {
			Self(match geo {
				crate::Geometry::Point(v) => json!(Coordinates {
					typ: CoordinatesType::Point,
					coordinates: vec![json!(v.x()), json!(v.y())].into(),
				}),
				crate::Geometry::Line(v) => json!(Coordinates {
					typ: CoordinatesType::LineString,
					coordinates: v
						.points()
						.map(|p| vec![json!(p.x()), json!(p.y())].into())
						.collect::<Vec<JsonValue>>()
						.into(),
				}),
				crate::Geometry::Polygon(v) => json!(Coordinates {
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
				crate::Geometry::MultiPoint(v) => json!(Coordinates {
					typ: CoordinatesType::MultiPoint,
					coordinates: v
						.0
						.iter()
						.map(|v| vec![json!(v.x()), json!(v.y())].into())
						.collect::<Vec<JsonValue>>()
						.into()
				}),
				crate::Geometry::MultiLine(v) => json!(Coordinates {
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
				crate::Geometry::MultiPolygon(v) => json!(Coordinates {
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
				crate::Geometry::Collection(v) => json!(Geometries {
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

	impl From<(crate::Id, bool)> for Id {
		fn from((id, simplify): (crate::Id, bool)) -> Self {
			match id {
				crate::Id::Number(n) => Id::Number(n),
				crate::Id::String(s) => Id::String(s),
				crate::Id::Array(arr) => Id::Array((arr, simplify).into()),
				crate::Id::Object(obj) => Id::Object((obj, simplify).into()),
				crate::Id::Generate(v) => match v {
					Gen::Rand => Id::from((crate::Id::rand(), simplify)),
					Gen::Ulid => Id::from((crate::Id::ulid(), simplify)),
					Gen::Uuid => Id::from((crate::Id::uuid(), simplify)),
				},
			}
		}
	}

	#[derive(Serialize)]
	struct Thing {
		tb: String,
		id: Id,
	}

	impl From<(crate::Thing, bool)> for Thing {
		fn from((thing, simplify): (crate::Thing, bool)) -> Self {
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
		Value::Strand(strand) => strand.0.into(),
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
				crate::Geometry::Point(geo) => json!(geo),
				crate::Geometry::Line(geo) => json!(geo),
				crate::Geometry::Polygon(geo) => json!(geo),
				crate::Geometry::MultiPoint(geo) => json!(geo),
				crate::Geometry::MultiLine(geo) => json!(geo),
				crate::Geometry::MultiPolygon(geo) => json!(geo),
				crate::Geometry::Collection(geo) => json!(geo),
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
	use super::*;
	use crate::block::Entry;
	use crate::constant::Constant;
	use crate::statements::CreateStatement;
	use crate::*;
	use ::serde::Serialize;
	use std::ops::Bound;

	#[test]
	fn none() {
		let expected = Value::None;
		assert_eq!(expected, to_value(None::<u32>).unwrap());
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn null() {
		let expected = Value::Null;
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn r#false() {
		let expected = Value::Bool(false);
		assert_eq!(expected, to_value(false).unwrap());
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn r#true() {
		let expected = Value::Bool(true);
		assert_eq!(expected, to_value(true).unwrap());
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn number() {
		let number = Number::Int(Default::default());
		let value = to_value(&number).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());

		let number = Number::Float(Default::default());
		let value = to_value(&number).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());

		let number = Number::Decimal(Default::default());
		let value = to_value(&number).unwrap();
		let expected = Value::Number(number);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn strand() {
		let strand = Strand("foobar".to_owned());
		let value = to_value(&strand).unwrap();
		let expected = Value::Strand(strand);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());

		let strand = "foobar".to_owned();
		let value = to_value(&strand).unwrap();
		let expected = Value::Strand(Strand(strand));
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());

		let strand = "foobar";
		let value = to_value(strand).unwrap();
		let expected = Value::Strand(Strand(strand.to_owned()));
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn duration() {
		let duration = Duration::default();
		let value = to_value(&duration).unwrap();
		let expected = Value::Duration(duration);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn datetime() {
		let datetime = Datetime::default();
		let value = to_value(&datetime).unwrap();
		let expected = Value::Datetime(datetime);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn uuid() {
		let uuid = Uuid::default();
		let value = to_value(uuid).unwrap();
		let expected = Value::Uuid(uuid);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn array() {
		let array = Array::default();
		let value = to_value(&array).unwrap();
		let expected = Value::Array(array);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn object() {
		let object = Object::default();
		let value = to_value(&object).unwrap();
		let expected = Value::Object(object);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn geometry() {
		let geometry = Geometry::Collection(Vec::new());
		let value = to_value(&geometry).unwrap();
		let expected = Value::Geometry(geometry);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn bytes() {
		let bytes = Bytes("foobar".as_bytes().to_owned());
		let value = to_value(&bytes).unwrap();
		let expected = Value::Bytes(bytes);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn param() {
		let param = Param::default();
		let value = to_value(&param).unwrap();
		let expected = Value::Param(param);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn idiom() {
		let idiom = Idiom::default();
		let value = to_value(&idiom).unwrap();
		let expected = Value::Idiom(idiom);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn table() {
		let table = Table("foo".to_owned());
		let value = to_value(&table).unwrap();
		let expected = Value::Table(table);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn thing() {
		let record_id = crate::thing("foo:bar").unwrap();
		let value = to_value(&record_id).unwrap();
		let expected = Value::Thing(record_id);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn model() {
		let model = Mock::Count("foo".to_owned(), Default::default());
		let value = to_value(&model).unwrap();
		let expected = Value::Mock(model);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn regex() {
		let regex = "abc".parse().unwrap();
		let value = to_value(&regex).unwrap();
		let expected = Value::Regex(regex);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn block() {
		let block = Box::default();
		let value = to_value(&block).unwrap();
		let expected = Value::Block(block);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn range() {
		let range = Box::new(Range {
			tb: "foo".to_owned(),
			beg: Bound::Included("foo".into()),
			end: Bound::Unbounded,
		});
		let value = to_value(&range).unwrap();
		let expected = Value::Range(range);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn edges() {
		let edges = Box::new(Edges {
			dir: Dir::In,
			from: crate::thing("foo:bar").unwrap(),
			what: Tables(vec!["foo".into()]),
		});
		let value = to_value(&edges).unwrap();
		let expected = Value::Edges(edges);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn future() {
		let future = Box::new(Future(Value::default().into()));
		let value = to_value(&future).unwrap();
		let expected = Value::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());

		let future = Box::new(Future(Block(vec![Entry::Create(CreateStatement::default())])));
		let value = to_value(&future).unwrap();
		let expected = Value::Future(future);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn constant() {
		let constant = Constant::MathPi;
		let value = to_value(&constant).unwrap();
		let expected = Value::Constant(constant);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn function() {
		let function = Box::new(Function::Normal(Default::default(), Default::default()));
		let value = to_value(&function).unwrap();
		let expected = Value::Function(function);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn query() {
		let query = crate::parse("SELECT * FROM foo").unwrap();
		let value = to_value(&query).unwrap();
		let expected = Value::Query(query);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn subquery() {
		let subquery = Box::new(Subquery::Value(Value::None));
		let value = to_value(&subquery).unwrap();
		let expected = Value::Subquery(subquery);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn expression() {
		let expression = Box::new(Expression::Binary {
			l: "foo".into(),
			o: Operator::Equal,
			r: "Bar".into(),
		});
		let value = to_value(&expression).unwrap();
		let expected = Value::Expression(expression);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	#[test]
	fn unknown_struct() {
		#[derive(Debug, Serialize)]
		struct FooBar {
			foo: String,
			bar: i32,
		}

		let foo = "Foo";
		let bar = Default::default();
		let foo_bar = FooBar {
			bar,
			foo: foo.to_owned(),
		};
		let value = to_value(foo_bar).unwrap();
		let expected = Value::Object(
			map! {
				"foo".to_owned() => foo.into(),
				"bar".to_owned() => bar.into(),
			}
			.into(),
		);
		assert_eq!(value, expected);
		assert_eq!(expected, to_value(&expected).unwrap());
	}

	mod into_json {
		use crate::value::serde::ser::value::from_value;
		use crate::value::serde::ser::value::into_json;
		use crate::Value;
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
				let value = Value::Number(crate::Number::Int(num));

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
				let value = Value::Number(crate::Number::Float(num));

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
				let value = Value::Number(crate::Number::Decimal(num));

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
				assert_eq!(simple_json, json!(crate::Duration(duration).to_raw()));

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
					Value::Array(crate::Array(vec.iter().copied().map(Value::from).collect()));

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
				let value = Value::Object(crate::Object(
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
			let value = Value::Geometry(crate::Geometry::Point(point));

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
			let value = Value::Geometry(crate::Geometry::Line(line_string.clone()));

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
			let value = Value::Geometry(crate::Geometry::Polygon(polygon.clone()));

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
			let value = Value::Geometry(crate::Geometry::MultiPoint(multi_point.clone()));

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
			let value = Value::Geometry(crate::Geometry::MultiLine(multi_line.clone()));

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
			let value = Value::Geometry(crate::Geometry::MultiPolygon(multi_polygon.clone()));

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
			for geometries in [vec![], vec![crate::Geometry::Point(point! { x: 10., y: 20. })]] {
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

				let response: Vec<crate::Geometry> = from_value(value).unwrap();
				assert_eq!(response, geometries);
			}
		}

		#[test]
		fn bytes() {
			for bytes in [vec![], b"foo".to_vec()] {
				let value = Value::Bytes(crate::Bytes(bytes.clone()));

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
			let thing = crate::thing(record_id).unwrap();
			let value = Value::Thing(thing.clone());

			let simple_json = into_json(value.clone(), true);
			assert_eq!(simple_json, json!(record_id));

			let json = into_json(value.clone(), false);
			assert_eq!(json, json!(thing));

			let response: crate::Thing = from_value(value).unwrap();
			assert_eq!(response, thing);
		}
	}
}
