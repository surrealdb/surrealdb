#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod blob {

	use js::function::Rest;
	use js::Value;

	#[derive(Clone)]
	#[quickjs(cloneable)]
	pub struct Blob {
		pub(crate) mime: Option<String>,
		// TODO: make bytes?
		pub(crate) data: Vec<u8>,
	}

	impl Blob {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			todo!()
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		#[quickjs(get)]
		pub fn size(&self) -> usize {
			self.data.len()
		}

		#[quickjs(get)]
		pub fn r#type(&self) -> Option<&str> {
			self.mime.as_deref()
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
