use js::JsLifetime;
use js::class::Trace;

use crate::val::{RecordId, Strand, Value};

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub struct Record {
	#[qjs(skip_trace)]
	pub(crate) value: RecordId,
}

#[js::methods]
impl Record {
	#[qjs(constructor)]
	pub fn new(table: String, key: Value) -> Self {
		Self {
			value: RecordId {
				table,
				key: match key {
					Value::Array(v) => v.into(),
					Value::Object(v) => v.into(),
					Value::Number(v) => v.to_int().into(),
					Value::Uuid(v) => v.into(),
					// TODO: Null byte validity
					v => Strand::new(v.as_raw_string()).unwrap().into(),
				},
			},
		}
	}

	#[qjs(get)]
	pub fn tb(&self) -> String {
		self.value.table.clone()
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
		self.value.to_string()
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		self.value.to_string()
	}
}
