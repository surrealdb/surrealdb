#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod request {

	use super::super::blob::blob::Blob;
	use crate::sql::value::Value;
	use js::Rest;

	#[derive(Clone)]
	#[quickjs(class)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Request {
		#[quickjs(hide)]
		pub(crate) url: Option<String>,
		pub(crate) credentials: Option<String>,
		pub(crate) headers: Option<String>,
		pub(crate) method: Option<String>,
		pub(crate) mode: Option<String>,
		pub(crate) referrer: Option<String>,
	}

	impl Request {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(args: Rest<Value>) -> Self {
			Self {
				url: None,
				credentials: None,
				headers: None,
				method: None,
				mode: None,
				referrer: None,
			}
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		// TODO

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Request]")
		}

		// Creates a copy of the request object
		#[quickjs(rename = "clone")]
		pub fn copy(&self, args: Rest<Value>) -> Request {
			self.clone()
		}

		// Returns a promise with the request body as a Blob
		pub async fn blob(self, args: Rest<Value>) -> js::Result<Blob> {
			Err(throw!("Not yet implemented"))
		}

		// Returns a promise with the request body as FormData
		pub async fn formData(self, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!("Not yet implemented"))
		}

		// Returns a promise with the request body as JSON
		pub async fn json(self, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!("Not yet implemented"))
		}

		// Returns a promise with the request body as text
		pub async fn text(self, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!("Not yet implemented"))
		}
	}
}
