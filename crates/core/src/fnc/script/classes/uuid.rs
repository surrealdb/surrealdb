use crate::sql::uuid;
use js::{class::Trace, JsLifetime};

#[derive(Clone, Trace, JsLifetime)]
#[js::class]
#[non_exhaustive]
pub struct Uuid {
	#[qjs(skip_trace)]
	pub(crate) value: Option<uuid::Uuid>,
}

#[js::methods]
impl Uuid {
	#[qjs(constructor)]
	pub fn new(value: String) -> Self {
		Self {
			value: uuid::Uuid::try_from(value).ok(),
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
