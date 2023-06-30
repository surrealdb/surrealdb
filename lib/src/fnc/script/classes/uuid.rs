#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod uuid {

	use crate::sql::uuid;
	use crate::sql::value::Value;
	use js::{class::Ref, function::Rest};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	pub struct Uuid {
		pub(crate) value: Option<uuid::Uuid>,
	}

	impl Uuid {
		#[quickjs(constructor)]
		pub fn new(value: String, args: Rest<Value>) -> Self {
			Self {
				value: uuid::Uuid::try_from(value).ok(),
			}
		}
		#[quickjs(get)]
		pub fn value(&self) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Uuid"),
			}
		}
		// Compare two Uuid instances
		pub fn is<'js>(a: Ref<'js, Uuid>, b: Ref<'js, Uuid>, _args: Rest<()>) -> bool {
			a.value.is_some() && b.value.is_some() && a.value == b.value
		}
		/// Convert the object to a string
		pub fn toString(&self, _args: Rest<()>) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Uuid"),
			}
		}
		/// Convert the object to JSON
		pub fn toJSON(&self, _args: Rest<()>) -> String {
			match &self.value {
				Some(v) => v.to_raw(),
				None => String::from("Invalid Uuid"),
			}
		}
	}
}
