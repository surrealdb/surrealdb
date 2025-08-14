use js::JsLifetime;
use js::class::Trace;

use crate::val::Uuid as SqlUuid;

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub struct Uuid {
	#[qjs(skip_trace)]
	pub(crate) value: Option<SqlUuid>,
}

#[js::methods]
impl Uuid {
	#[qjs(constructor)]
	pub fn new(value: String) -> Self {
		Self {
			value: SqlUuid::try_from(value).ok(),
		}
	}
	#[qjs(get)]
	pub fn value(&self) -> String {
		match &self.value {
			Some(v) => v.to_raw(),
			None => String::from("Invalid Uuid"),
		}
	}
	// Compare two Uuid instances
	pub fn is(a: &Uuid, b: &Uuid) -> bool {
		a.value.is_some() && b.value.is_some() && a.value == b.value
	}
	/// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		match &self.value {
			Some(v) => v.to_raw(),
			None => String::from("Invalid Uuid"),
		}
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		match &self.value {
			Some(v) => v.to_raw(),
			None => String::from("Invalid Uuid"),
		}
	}
}
