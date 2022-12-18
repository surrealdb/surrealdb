#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(clippy::module_inception)]
pub mod uuid {

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Uuid {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Uuid {
		#[quickjs(constructor)]
		pub fn new(value: String) -> Self {
			Self {
				value,
			}
		}
		#[quickjs(get)]
		pub fn value(&self) -> &str {
			&self.value
		}
		/// Convert the object to a string
		pub fn toString(&self) -> String {
			self.value.to_owned()
		}
	}
}
