use crate::sql::thing;
use crate::sql::value::Value;
use js::class::Trace;

#[derive(Clone, Trace)]
#[js::class]
#[non_exhaustive]
pub struct Record {
	#[qjs(skip_trace)]
	pub(crate) value: thing::Thing,
}

#[js::methods]
impl Record {
	#[qjs(constructor)]
	pub fn new(tb: String, id: Value) -> Self {
		Self {
			value: thing::Thing {
				tb,
				id: match id {
					Value::Array(v) => v.into(),
					Value::Object(v) => v.into(),
					Value::Number(v) => v.into(),
					v => v.as_string().into(),
				},
			},
		}
	}

	#[qjs(get)]
	pub fn tb(&self) -> String {
		self.value.tb.clone()
	}

	#[qjs(get)]
	pub fn id(&self) -> String {
		self.value.id.to_raw()
	}
	// Compare two Record instances
	pub fn is(a: &Record, b: &Record) -> bool {
		a.value == b.value
	}
	/// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		self.value.to_raw()
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		self.value.to_raw()
	}
}
