use ciborium::Value as Data;
use std::collections::BTreeMap;
use surrealdb::sql::Datetime;
use surrealdb::sql::Duration;
use surrealdb::sql::Id;
use surrealdb::sql::Number;
use surrealdb::sql::Thing;
use surrealdb::sql::Uuid;
use surrealdb::sql::Value;

const TAG_NONE: u64 = 78_773_250;
const TAG_UUID: u64 = 78_773_251;
const TAG_DECIMAL: u64 = 78_773_252;
const TAG_DURATION: u64 = 78_773_253;
const TAG_DATETIME: u64 = 78_773_254;
const TAG_RECORDID: u64 = 78_773_255;

#[derive(Debug)]
pub struct Cbor(pub Data);

impl TryFrom<Cbor> for Value {
	type Error = &'static str;
	fn try_from(val: Cbor) -> Result<Self, &'static str> {
		match val.0 {
			Data::Null => Ok(Value::Null),
			Data::Bool(v) => Ok(Value::from(v)),
			Data::Integer(v) => Ok(Value::from(i128::from(v))),
			Data::Float(v) => Ok(Value::from(v)),
			Data::Bytes(v) => Ok(Value::Bytes(v.into())),
			Data::Text(v) => Ok(Value::from(v)),
			Data::Array(v) => {
				v.into_iter().map(|v| Value::try_from(Cbor(v))).collect::<Result<Value, &str>>()
			}
			Data::Map(v) => v
				.into_iter()
				.map(|(k, v)| {
					let k = Value::try_from(Cbor(k)).map(|k| k.as_raw_string());
					let v = Value::try_from(Cbor(v));
					Ok((k?, v?))
				})
				.collect::<Result<Value, &str>>(),
			Data::Tag(t, v) => {
				match t {
					// A literal NONE
					TAG_NONE => Ok(Value::None),
					// A literal uuid
					TAG_UUID => match *v {
						Data::Text(v) => match Uuid::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid UUID value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal decimal
					TAG_DECIMAL => match *v {
						Data::Text(v) => match Number::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Decimal value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal duration
					TAG_DURATION => match *v {
						Data::Text(v) => match Duration::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Duration value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal datetime
					TAG_DATETIME => match *v {
						Data::Text(v) => match Datetime::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid Datetime value"),
						},
						_ => Err("Expected a CBOR text data type"),
					},
					// A literal recordid
					TAG_RECORDID => match *v {
						Data::Text(v) => match Thing::try_from(v) {
							Ok(v) => Ok(v.into()),
							_ => Err("Expected a valid RecordID value"),
						},
						Data::Array(mut v) if v.len() == 2 => match (v.remove(0), v.remove(0)) {
							(Data::Text(tb), Data::Text(id)) => {
								Ok(Value::from(Thing::from((tb, id))))
							}
							(Data::Text(tb), Data::Integer(id)) => {
								Ok(Value::from(Thing::from((tb, Id::from(i128::from(id) as i64)))))
							}
							(Data::Text(tb), Data::Array(id)) => Ok(Value::from(Thing::from((
								tb,
								Id::from(
									id.into_iter()
										.map(|v| Value::try_from(Cbor(v)))
										.collect::<Result<Vec<Value>, &str>>()?,
								),
							)))),
							(Data::Text(tb), Data::Map(id)) => Ok(Value::from(Thing::from((
								tb,
								Id::from(
									id.into_iter()
										.map(|(k, v)| {
											let k =
												Value::try_from(Cbor(k)).map(|k| k.as_raw_string());
											let v = Value::try_from(Cbor(v));
											Ok((k?, v?))
										})
										.collect::<Result<BTreeMap<String, Value>, &str>>()?,
								),
							)))),
							_ => Err("Expected a CBOR array with 2 elements, a text data type, and a valid ID type"),
						},
						_ => Err("Expected a CBOR text data type, or a CBOR array with 2 elements"),
					},
					// An unknown tag
					_ => Err("Encountered an unknown CBOR tag"),
				}
			}
			_ => Err("Encountered an unknown CBOR data type"),
		}
	}
}

impl TryFrom<Value> for Cbor {
	type Error = &'static str;
	fn try_from(val: Value) -> Result<Self, &'static str> {
		match val {
			Value::None => Ok(Cbor(Data::Tag(TAG_NONE, Box::new(Data::Null)))),
			Value::Null => Ok(Cbor(Data::Null)),
			Value::Bool(v) => Ok(Cbor(Data::Bool(v))),
			Value::Number(v) => match v {
				Number::Int(v) => Ok(Cbor(Data::Integer(v.into()))),
				Number::Float(v) => Ok(Cbor(Data::Float(v))),
				Number::Decimal(v) => {
					Ok(Cbor(Data::Tag(TAG_DECIMAL, Box::new(Data::Text(v.to_string())))))
				}
			},
			Value::Strand(v) => Ok(Cbor(Data::Text(v.0))),
			Value::Duration(v) => {
				Ok(Cbor(Data::Tag(TAG_DURATION, Box::new(Data::Text(v.to_raw())))))
			}
			Value::Datetime(v) => {
				Ok(Cbor(Data::Tag(TAG_DATETIME, Box::new(Data::Text(v.to_raw())))))
			}
			Value::Uuid(v) => Ok(Cbor(Data::Tag(TAG_UUID, Box::new(Data::Text(v.to_raw()))))),
			Value::Array(v) => Ok(Cbor(Data::Array(
				v.into_iter()
					.map(|v| {
						let v = Cbor::try_from(v)?.0;
						Ok(v)
					})
					.collect::<Result<Vec<Data>, &str>>()?,
			))),
			Value::Object(v) => Ok(Cbor(Data::Map(
				v.into_iter()
					.map(|(k, v)| {
						let k = Data::Text(k);
						let v = Cbor::try_from(v)?.0;
						Ok((k, v))
					})
					.collect::<Result<Vec<(Data, Data)>, &str>>()?,
			))),
			Value::Bytes(v) => Ok(Cbor(Data::Bytes(v.into_inner()))),
			Value::Thing(v) => Ok(Cbor(Data::Tag(
				TAG_RECORDID,
				Box::new(Data::Array(vec![
					Data::Text(v.tb),
					match v.id {
						Id::Number(v) => Data::Integer(v.into()),
						Id::String(v) => Data::Text(v),
						Id::Array(v) => Cbor::try_from(Value::from(v))?.0,
						Id::Object(v) => Cbor::try_from(Value::from(v))?.0,
						Id::Generate(_) => unreachable!(),
					},
				])),
			))),
			// We shouldn't reach here
			_ => unreachable!(),
		}
	}
}
