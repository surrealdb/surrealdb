#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod blob {

	use js::Rest;
	use js::Value;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	pub struct Blob {
		#[quickjs(hide)]
		pub(crate) mime: String,
		#[quickjs(hide)]
		pub(crate) data: Vec<u8>,
	}

	impl Blob {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			Self {
				data: vec![],
				mime: String::new(),
			}
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		#[quickjs(get)]
		pub fn size(&self) -> usize {
			self.data.len()
		}

		#[quickjs(get)]
		pub fn r#type(&self) -> &str {
			&self.mime
		}

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Blob]")
		}
	}
}
