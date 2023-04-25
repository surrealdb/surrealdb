#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod duration {

	use crate::sql::duration;
	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Duration {
		#[quickjs(hide)]
		pub(crate) value: Option<duration::Duration>,
	}

	impl Duration {
		#[quickjs(constructor)]
		pub fn new(value: String, args: Rest<Value>) -> Self {
			Self {
				value: duration::Duration::try_from(value).ok(),
			}
		}
		#[quickjs(get)]
		pub fn value(&self) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Duration"),
			}
		}
		// Compare two Duration instances
		pub fn is(a: &Duration, b: &Duration, args: Rest<Value>) -> bool {
			a.value.is_some() && b.value.is_some() && a.value == b.value
		}
		/// Convert the object to a string
		pub fn toString(&self, args: Rest<Value>) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Duration"),
			}
		}
		/// Convert the object to JSON
		pub fn toJSON(&self, args: Rest<Value>) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Duration"),
			}
		}
	}
}
