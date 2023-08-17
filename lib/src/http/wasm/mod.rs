use super::{ClientBuilder, Error, Request};
use js_sys::Promise;
use std::error::Error as StdError;
use std::sync::Arc;
use wasm_bindgen::prelude::*;

mod body;
mod response;
mod stream;
pub use response::Response;

pub type BoxError = Box<dyn StdError + Send + Sync>;

impl Error {
	fn wasm(v: wasm_bindgen::JsValue) -> Self {
		Self::Backend(format!("{v:?}").into())
	}
}

impl From<wasm_bindgen::JsValue> for Error {
	fn from(v: wasm_bindgen::JsValue) -> Self {
		Error::wasm(v)
	}
}

pub type BackendError = BoxError;
pub type BackendBody = body::Body;

#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(js_name = fetch)]
	fn fetch_with_request(input: &web_sys::Request) -> Promise;
}

fn js_fetch(req: &web_sys::Request) -> Promise {
	let global = js_sys::global();

	if let Ok(true) = js_sys::Reflect::has(&global, &JsValue::from_str("ServiceWorkerGlobalScope"))
	{
		global.unchecked_into::<web_sys::ServiceWorkerGlobalScope>().fetch_with_request(req)
	} else {
		// browser
		fetch_with_request(req)
	}
}

pub struct Backend {
	config: Arc<ClientBuilder>,
}

impl Backend {
	pub fn new() -> Self {
		Self {
			config: Arc::new(ClientBuilder::new()),
		}
	}

	pub fn build(builder: ClientBuilder) -> Self {
		Backend {
			config: Arc::new(builder),
		}
	}

	pub async fn execute(&self, request: Request) -> Result<Response, Error> {
		todo!()
	}
}
