#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(unused_variables)]
#[allow(non_snake_case)]
pub mod blob {

	use js::Rest;
	use js::Value;

	#[derive(Clone)]
	#[quickjs(class)]
	pub struct Blob {
		#[quickjs(hide)]
		pub(crate) mime: String,
		#[quickjs(hide)]
		pub(crate) data: Vec<u8>,
	}

	impl Blob {
		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			Self {
				data: vec![],
				mime: String::new(),
			}
		}
		#[quickjs(get)]
		pub fn size(&self) -> usize {
			self.data.len()
		}
		#[quickjs(get)]
		pub fn r#type(&self) -> &str {
			&self.mime
		}
		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Blob]")
		}
	}
}
