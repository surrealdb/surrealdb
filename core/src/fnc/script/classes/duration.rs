use js::class::Trace;

use crate::sql::duration;

#[derive(Clone, Trace)]
#[js::class]
#[non_exhaustive]
pub struct Duration {
	#[qjs(skip_trace)]
	pub(crate) value: Option<duration::Duration>,
}

#[js::methods]
impl Duration {
	#[qjs(constructor)]
	pub fn new(value: String) -> Self {
		Self {
			value: duration::Duration::try_from(value).ok(),
		}
	}

	#[qjs(get)]
	pub fn value(&self) -> String {
		match &self.value {
			Some(v) => v.to_raw(),
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
			Some(v) => v.to_raw(),
			None => String::from("Invalid Duration"),
		}
	}
	/// Convert the object to JSON
	#[qjs(rename = "toJSON")]
	pub fn to_json(&self) -> String {
		match &self.value {
			Some(v) => v.to_raw(),
			None => String::from("Invalid Duration"),
		}
	}
}
