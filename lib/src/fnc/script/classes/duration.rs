#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod duration {

	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Duration {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Duration {
		#[quickjs(constructor)]
		pub fn new(value: String, args: Rest<Value>) -> Self {
			Self {
				value,
			}
		}
		#[quickjs(get)]
		pub fn value(&self) -> &str {
			&self.value
		}
		// Compare two Duration instances
		pub fn is(a: &Duration, b: &Duration, args: Rest<Value>) -> bool {
			a.value == b.value
		}
		/// Convert the object to a string
		pub fn toString(&self, args: Rest<Value>) -> String {
			self.value.to_owned()
		}
		/// Convert the object to JSON
		pub fn toJSON(&self, args: Rest<Value>) -> String {
			self.value.to_owned()
		}
	}
}
