#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(clippy::module_inception)]
pub mod duration {

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Duration {
		#[quickjs(hide)]
		pub(crate) value: String,
	}

	impl Duration {
		#[quickjs(constructor)]
		pub fn new(value: String) -> Self {
			Self {
				value,
			}
		}
		#[quickjs(rename = "toString")]
		pub fn output(&self) -> String {
			self.value.to_owned()
		}
		#[quickjs(get)]
		pub fn value(&self) -> &str {
			&self.value
		}
	}
}
