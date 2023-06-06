#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod response {

	use super::super::blob::blob::Blob;
	use crate::sql::value::Value;
	use js::function::Rest;

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Response {
		pub(crate) url: Option<String>,
		pub(crate) credentials: Option<String>,
		pub(crate) headers: Option<String>,
		pub(crate) method: Option<String>,
		pub(crate) mode: Option<String>,
		pub(crate) referrer: Option<String>,
	}

	impl Response {
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
			String::from("[object Response]")
		}

		// Creates a copy of the request object
		#[quickjs(rename = "clone")]
		pub fn copy(&self, args: Rest<Value>) -> Response {
			self.clone()
		}

		// Returns a promise with the response body as a Blob
		pub async fn blob(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Blob> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the response body as FormData
		pub async fn formData(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the response body as JSON
		pub async fn json(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the response body as text
		pub async fn text(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// ------------------------------
		// Static methods
		// ------------------------------

		// Returns a new response representing a network error
		pub fn error(ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Response> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Creates a new response with a different URL
		pub fn redirect(ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Response> {
			Err(throw!(ctx, "Not yet implemented"))
		}
	}
}
