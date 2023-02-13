#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod uuid {

	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Uuid {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Uuid {
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
		// Compare two Uuid instances
		pub fn is(a: &Uuid, b: &Uuid, args: Rest<Value>) -> bool {
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
