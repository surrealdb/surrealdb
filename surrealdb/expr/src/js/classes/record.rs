#![deny(clippy::wildcard_enum_match_arm)]

use js::JsLifetime;
use js::class::Trace;
use surrealdb_types::ToSql;

use crate::val::{RecordId, TableName, Value};

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub(crate) struct Record {
	#[qjs(skip_trace)]
	pub(crate) value: RecordId,
}

#[js::methods]
impl Record {
	#[qjs(constructor)]
	pub fn new(table: String, key: Value) -> Self {
		Self {
			value: RecordId {
				table: TableName::new(table),
				key: match key {
					Value::Array(v) => v.into(),
					Value::Object(v) => v.into(),
					Value::Number(v) => v.to_int().into(),
					Value::Uuid(v) => v.into(),
					v @ (Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::String(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Set(_)
					| Value::Geometry(_)
					| Value::Bytes(_)
					| Value::Table(_)
					| Value::RecordId(_)
					| Value::File(_)
					| Value::Regex(_)
					| Value::Range(_)
					| Value::Closure(_)) => v.into_raw_string().into(),
				},
			},
		}
	}

	#[qjs(get)]
	pub fn tb(&self) -> String {
		self.value.table.as_str().to_string()
	}

	#[qjs(get)]
	pub fn id(&self) -> Value {
		self.value.key.clone().into_value()
	}
	// Compare two Record instances
	pub fn is(a: &Record, b: &Record) -> bool {
		a.value == b.value
	}
	/// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		self.value.to_sql()
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		self.value.to_sql()
	}
}
