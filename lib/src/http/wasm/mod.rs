use super::{ClientBuilder, Error, Request};
use js_sys::Promise;
use std::error::Error as StdError;
use std::sync::Arc;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

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

pub struct AbortDropper(Option<web_sys::AbortController>);

impl AbortDropper {
	pub fn new() -> Result<Self, Error> {
		Ok(AbortDropper(Some(web_sys::AbortController::new()?)))
	}

	pub fn signal(&self) -> web_sys::AbortSignal {
		self.0.as_ref().unwrap().signal()
	}

	pub fn done(mut self) {
		self.0.take();
	}
}

impl Drop for AbortDropper {
	fn drop(&mut self) {
		if let Some(x) = self.0.take() {
			x.abort();
		}
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
		let headers = web_sys::Headers::new()?;
		let abort = AbortDropper::new()?;
		if let Some(default_headers) = self.config.default_headers.as_ref() {
			for (k, v) in default_headers.iter() {
				headers.append(k.as_str(), v.to_str().map_err(BoxError::from)?)?;
			}
		}
		for (k, v) in request.headers.iter() {
			headers.append(k.as_str(), v.to_str().map_err(BoxError::from)?)?;
		}

		let mut init = web_sys::RequestInit::new();
		init.headers(headers.as_ref());
		init.body(Some(&request.body.into_js_value()));
		init.signal(Some(&abort.signal()));

		let request = web_sys::Request::new_with_str_and_init(request.url.as_str(), &init)?;
		let response = JsFuture::from(js_fetch(&request)).await?;
		let response = response.unchecked_into::<web_sys::Response>();
		let response = Response::from_js(response)?;
		abort.done();
		Ok(response)
	}
}
