use crate::sql::Datetime;
use crate::sql::Duration;
use crate::sql::Number;
use crate::sql::Thing;
use crate::sql::Uuid;
use crate::sql::Value;
use rmpv::Value as Data;

const TAG_NONE: i8 = 1;
const TAG_UUID: i8 = 2;
const TAG_DECIMAL: i8 = 3;
const TAG_DURATION: i8 = 4;
const TAG_DATETIME: i8 = 5;
const TAG_RECORDID: i8 = 6;

#[derive(Debug)]
pub struct Pack(pub Data);

impl TryFrom<Pack> for Value {
	type Error = &'static str;
	fn try_from(val: Pack) -> Result<Self, &'static str> {
		match val.0 {
			Data::Nil => Ok(Value::Null),
			Data::Boolean(v) => Ok(Value::from(v)),
			Data::Integer(v) if v.is_i64() => match v.as_i64() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::Integer(v) if v.is_u64() => match v.as_u64() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::F32(v) => Ok(Value::from(v)),
			Data::F64(v) => Ok(Value::from(v)),
			Data::String(v) => match v.into_str() {
				Some(v) => Ok(Value::from(v)),
				None => Ok(Value::Null),
			},
			Data::Binary(v) => Ok(Value::Bytes(v.into())),
			Data::Array(v) => {
				v.into_iter().map(|v| Value::try_from(Pack(v))).collect::<Result<Value, &str>>()
			}
			Data::Map(v) => v
				.into_iter()
				.map(|(k, v)| {
					let k = Value::try_from(Pack(k)).map(|k| k.as_raw_string());
					let v = Value::try_from(Pack(v));
					Ok((k?, v?))
				})
				.collect::<Result<Value, &str>>(),
			Data::Ext(t, v) => {
				match t {
					// A literal uuid
					TAG_NONE => Ok(Value::None),
					// A literal uuid
					TAG_UUID => match std::str::from_utf8(&v) {
						Ok(v) => match Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid UUID value"),
						},
						_ => Err("Expected a valid UTF-8 string"),
					},
					// A literal decimal
					TAG_DECIMAL => match std::str::from_utf8(&v) {
						Ok(v) => match Number::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Decimal value"),
						},
						_ => Err("Expected a valid UTF-8 string"),
					},
					// A literal duration
					TAG_DURATION => match std::str::from_utf8(&v) {
						Ok(v) => match Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Duration value"),
						},
						_ => Err("Expected a valid UTF-8 string"),
					},
					// A literal datetime
					TAG_DATETIME => match std::str::from_utf8(&v) {
						Ok(v) => match Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Datetime value"),
						},
						_ => Err("Expected a valid UTF-8 string"),
					},
					// A literal recordid
					TAG_RECORDID => match std::str::from_utf8(&v) {
						Ok(v) => match Thing::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid RecordID value"),
						},
						_ => Err("Expected a valid UTF-8 string"),
					},
					// An unknown tag
					_ => Err("Encountered an unknown MessagePack tag"),
				}
			}
			_ => Err("Encountered an unknown MessagePack data type"),
		}
	}
}

impl TryFrom<Value> for Pack {
	type Error = &'static str;
	fn try_from(val: Value) -> Result<Self, &'static str> {
		match val {
			Value::None => Ok(Pack(Data::Ext(TAG_NONE, vec![]))),
			Value::Null => Ok(Pack(Data::Nil)),
			Value::Bool(v) => Ok(Pack(Data::Boolean(v))),
			Value::Number(v) => match v {
				Number::Int(v) => Ok(Pack(Data::Integer(v.into()))),
				Number::Float(v) => Ok(Pack(Data::F64(v))),
				Number::Decimal(v) => {
					Ok(Pack(Data::Ext(TAG_DECIMAL, v.to_string().as_bytes().to_vec())))
				}
			},
			Value::Strand(v) => Ok(Pack(Data::String(v.0.into()))),
			Value::Duration(v) => Ok(Pack(Data::Ext(TAG_DURATION, v.to_raw().as_bytes().to_vec()))),
			Value::Datetime(v) => Ok(Pack(Data::Ext(TAG_DATETIME, v.to_raw().as_bytes().to_vec()))),
			Value::Uuid(v) => Ok(Pack(Data::Ext(TAG_UUID, v.to_raw().as_bytes().to_vec()))),
			Value::Array(v) => Ok(Pack(Data::Array(
				v.into_iter()
					.map(|v| {
						let v = Pack::try_from(v)?.0;
						Ok(v)
					})
					.collect::<Result<Vec<Data>, &str>>()?,
			))),
			Value::Object(v) => Ok(Pack(Data::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = Data::String(k.into());
						let v = Pack::try_from(v)?.0;
						Ok((k, v))
					})
					.collect::<Result<Vec<(Data, Data)>, &str>>()?,
			))),
			Value::Bytes(v) => Ok(Pack(Data::Binary(v.into_inner()))),
			Value::Thing(v) => Ok(Pack(Data::Ext(TAG_RECORDID, v.to_raw().as_bytes().to_vec()))),
			// We shouldn't reach here
			_ => Err("Found unsupported SurrealQL value being encoded into a msgpack value"),
		}
	}
}
