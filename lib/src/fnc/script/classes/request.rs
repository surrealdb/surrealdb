#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(clippy::module_inception)]
pub mod request {
	use super::RequestInput;
	use super::RequestOptions;
	use crate::fnc::script::classes::blob::blob::Blob;
	use crate::fnc::script::classes::headers::headers::Headers;
	use crate::fnc::script::classes::response::JsArrayBuffer;
	use crate::sql::json;
	use crate::sql::Value;
	use futures::lock::Mutex;
	use js::Rest;
	use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Request {
		#[quickjs(hide)]
		pub(crate) url: Option<surf::Url>,
		pub(crate) headers: Headers,
		pub(crate) method: surf::http::Method,
		#[quickjs(skip)]
		pub(crate) body: Option<Arc<Mutex<surf::Body>>>,
		#[quickjs(hide)]
		bodyUsed: Arc<AtomicBool>,
		#[quickjs(skip)]
		pub(crate) keepalive: bool,
		pub(crate) mode: Option<String>,
		pub(crate) credentials: Option<String>,
		pub(crate) cache: Option<String>,
		pub(crate) redirect: Option<String>,
		pub(crate) referrer: Option<String>,
		pub(crate) referrerPolicy: Option<String>,
		pub(crate) integrity: Option<String>,
	}

	impl Request {
		#[quickjs(constructor)]
		pub fn new(input: RequestInput, mut args: Rest<RequestOptions>) -> Self {
			let mut request = Self {
				url: None,
				body: None,
				credentials: None,
				headers: Headers::new(),
				method: surf::http::Method::Get,
				mode: None,
				referrer: None,
				bodyUsed: Arc::new(AtomicBool::new(false)),
				keepalive: true,
				cache: None,
				redirect: None,
				referrerPolicy: None,
				integrity: None,
			};
			match input {
				RequestInput::URL(url) => {
					let url =
						surf::http::Url::parse(&url).map_err(|e| throw_js_exception!(e)).unwrap();
					request.url = Some(url);
				}
				RequestInput::Request(req) => {
					request = req.clone();
				}
			}

			if let Some(options) = args.pop() {
				if let Some(method) = options.method {
					request.method = method;
				}
				if let Some(headers) = options.headers {
					request.headers = headers;
				}
				if let Some(body) = options.body {
					request.body = Some(Arc::new(Mutex::new(body)));
				}
				if let Some(keepalive) = options.keepalive {
					request.keepalive = keepalive;
				}
			}
			request
		}

		#[quickjs(get)]
		pub fn headers(self) -> Headers {
			self.headers
		}

		#[quickjs(get)]
		pub fn bodyUsed(&self) -> bool {
			self.bodyUsed.load(Ordering::SeqCst)
		}

		#[quickjs(get)]
		pub fn method(&self) -> String {
			self.method.to_string()
		}

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Request]")
		}

		// mark body had been used
		#[quickjs(skip)]
		fn set_body_used(&mut self) -> js::Result<()> {
			// https://fetch.spec.whatwg.org/#body-mixin
			// The bodyUsed getter steps are to return true if this’s body is non-null and this’s body’s stream is disturbed;
			// otherwise false.
			if self.body.is_none() {
				return Ok(());
			}
			if self.bodyUsed.fetch_or(true, Ordering::SeqCst) {
				return Err(throw_js_exception!("TypeError: body stream already read"));
			}
			Ok(())
		}

		#[quickjs(skip)]
		pub(crate) async fn take_body(self) -> Option<surf::Body> {
			if let Some(body) = self.body {
				let mut ptr = body.lock().await;
				return Some(std::mem::replace(&mut *ptr, surf::Body::empty()));
			}
			None
		}

		// Returns a promise with the request body as a Blob
		pub async fn blob(mut self) -> js::Result<Blob> {
			self.set_body_used()?;
			if let Some(body) = self.take_body().await {
				let mime = body.mime().to_string();
				let data = body.into_bytes().await.map_err(|e| throw_js_exception!(e))?;
				return Ok(Blob {
					mime,
					data,
				});
			}
			Ok(Blob {
				mime: "".to_owned(),
				data: vec![],
			})
		}

		// Returns a promise with the request body as text
		pub async fn text(mut self) -> js::Result<String> {
			self.set_body_used()?;
			if let Some(body) = self.take_body().await {
				let text = body.into_string().await.map_err(|e| throw_js_exception!(e))?;
				return Ok(text);
			}
			Ok("".to_owned())
		}

		// Returns a promise with the request body as json
		pub async fn json(mut self) -> js::Result<Value> {
			self.set_body_used()?;
			if let Some(body) = self.take_body().await {
				let text = body.into_string().await.map_err(|e| throw_js_exception!(e))?;
				return Ok(json(&text).map_err(|e| throw_js_exception!(e))?);
			}
			Err(throw_js_exception!("SyntaxError: Unexpected end of input"))
		}

		// Returns a promise with the request body as arrayBuffer
		pub async fn arrayBuffer(mut self) -> js::Result<JsArrayBuffer> {
			self.set_body_used()?;
			if let Some(body) = self.take_body().await {
				let data = body.into_bytes().await.map_err(|e| throw_js_exception!(e))?;
				return Ok(JsArrayBuffer(data));
			}
			Ok(JsArrayBuffer(vec![]))
		}

		// Creates a copy of the request object
		#[quickjs(rename = "clone")]
		pub(crate) fn safe_clone(&self) -> js::Result<Request> {
			if self.bodyUsed() {
				return Err(throw_js_exception!("TypeError: Request body is already used"));
			}
			Ok(Request::clone(&self))
		}
	}
}

use crate::fnc::script::classes::headers::headers::Headers;
use crate::fnc::script::util::{take_http_body, take_http_headers};
use std::str::FromStr;
use surf::http;

pub enum RequestInput {
	URL(String),
	Request(request::Request),
}
impl<'js> js::FromJs<'js> for RequestInput {
	fn from_js(_ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		if value.is_string() {
			let url = value.as_string().map_or("".to_owned(), |s| s.to_string().unwrap());
			return Ok(RequestInput::URL(url));
		}

		if value.is_object() {
			let object = value.into_object().unwrap();
			// Check to see if this object is a Headers
			if (object).instance_of::<request::Request>() {
				let request = object.into_instance::<request::Request>().unwrap();
				let request: &request::Request = request.as_ref();
				return Ok(RequestInput::Request(request.safe_clone()?));
			}
		}
		Err(throw_js_exception!("TypeError: Unexpected fetch input"))
	}
}

#[allow(dead_code)]
#[derive(Default)]
pub struct RequestOptions {
	method: Option<http::Method>,
	headers: Option<Headers>,
	body: Option<http::Body>,
	keepalive: Option<bool>,
	mode: Option<String>,
	credentials: Option<String>,
	cache: Option<String>,
	redirect: Option<String>,
	referrer: Option<String>,
	referrer_policy: Option<String>,
	integrity: Option<String>,
	// TODO:
	// signal: Option<AbortSignal>
}

impl<'js> js::FromJs<'js> for RequestOptions {
	fn from_js(ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		match value {
			val if val.is_object() => {
				let mut options = RequestOptions::default();
				let val = val.into_object().unwrap();
				if let Ok(method) = val.get::<_, String>("method") {
					options.method =
						Some(http::Method::from_str(&method).map_err(|e| throw_js_exception!(e))?);
				}
				if let Ok(headers) = val.get::<_, js::Value<'js>>("headers") {
					options.headers = take_http_headers(ctx, headers)?;
				}
				if let Ok(body) = val.get::<_, js::Value<'js>>("body") {
					options.body = take_http_body(ctx, body)?;
				}
				if let Ok(keepalive) = val.get::<_, js::Value<'js>>("keepalive") {
					if keepalive.is_bool() {
						options.keepalive = keepalive.as_bool();
					}
				}
				Ok(options)
			}
			_ => Ok(Self::default()),
		}
	}
}

impl Into<surf::Request> for request::Request {
	fn into(self) -> surf::Request {
		let rb = surf::RequestBuilder::new(self.method, self.url.unwrap());
		let mut req = rb.build();
		for (name, values) in self.headers {
			for value in values {
				req.append_header(name.clone(), value)
			}
		}
		req
	}
}
