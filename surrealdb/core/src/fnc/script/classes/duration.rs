use js::JsLifetime;
use js::class::Trace;

use crate::val;

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
pub struct Duration {
	#[qjs(skip_trace)]
	pub(crate) value: Option<val::Duration>,
}

#[js::methods]
impl Duration {
	#[qjs(constructor)]
	pub fn new(value: String) -> Self {
		Self {
			value: value.parse().ok(),
		}
	}

	#[qjs(get)]
	pub fn value(&self) -> String {
		match &self.value {
			Some(v) => v.to_string(),
			None => String::from("Invalid Duration"),
		}
	}
	// Compare two Duration instances
	pub fn is(a: &Duration, b: &Duration) -> bool {
		a.value.is_some() && b.value.is_some() && a.value == b.value
	}
	/// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		match &self.value {
			Some(v) => v.to_string(),
			None => String::from("Invalid Duration"),
		}
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		match &self.value {
			Some(v) => v.to_string(),
			None => String::from("Invalid Duration"),
		}
	}
}
