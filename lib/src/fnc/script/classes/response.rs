#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(clippy::module_inception)]
pub mod response {
	use crate::fnc::script::classes::headers::headers::Headers;
	use crate::sql::{json, Value};
	use crate::throw_js_exception;
	use futures::lock::Mutex;
	use js::Result;
	use std::sync::{atomic::AtomicBool, atomic::Ordering, Arc};

	use super::JsArrayBuffer;

	#[quickjs(cloneable)]
	#[derive(Clone)]
	pub struct Response {
		#[quickjs(skip)]
		pub(crate) inner: Arc<Mutex<surf::Response>>,
		headers: Headers,
		#[quickjs(hide)]
		statusCode: surf::http::StatusCode,
		#[quickjs(hide)]
		bodyUsed: Arc<AtomicBool>,
	}

	impl Response {
		#[quickjs(get)]
		pub fn ok(&self) -> bool {
			self.statusCode.is_success()
		}

		#[quickjs(get)]
		pub fn bodyUsed(&self) -> bool {
			self.bodyUsed.load(Ordering::SeqCst)
		}

		#[quickjs(get)]
		pub fn status(&self) -> u16 {
			self.statusCode.into()
		}

		#[quickjs(get)]
		pub fn statusText(&self) -> &'static str {
			self.statusCode.canonical_reason()
		}

		#[quickjs(get)]
		pub fn headers(self) -> Headers {
			self.headers
		}

		#[quickjs(skip)]
		fn set_body_used(&mut self) -> Result<()> {
			if self.bodyUsed.fetch_or(true, Ordering::SeqCst) {
				return Err(throw_js_exception!("TypeError: body stream already read"));
			}
			Ok(())
		}

		pub async fn text(mut self) -> Result<String> {
			self.set_body_used()?;
			let text =
				self.inner.lock().await.body_string().await.map_err(|e| throw_js_exception!(e))?;
			Ok(text)
		}

		pub async fn json(mut self) -> Result<Value> {
			self.set_body_used()?;
			let text =
				self.inner.lock().await.body_string().await.map_err(|e| throw_js_exception!(e))?;
			json(&text).map_err(|e| throw_js_exception!(e))
		}

		pub async fn arrayBuffer(mut self) -> Result<JsArrayBuffer> {
			self.set_body_used()?;
			let buf =
				self.inner.lock().await.body_bytes().await.map_err(|e| throw_js_exception!(e))?;
			Ok(JsArrayBuffer(buf))
		}

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Response]")
		}

		#[quickjs(skip)]
		pub(crate) fn from_surf(resp: surf::Response) -> Self {
			let headers: &surf::http::Headers = resp.as_ref();
			let headers = Headers::from(headers);
			let statusCode = resp.status().clone();

			Response {
				inner: Arc::new(Mutex::new(resp)),
				headers,
				statusCode,
				bodyUsed: Arc::new(AtomicBool::new(false)),
			}
		}
	}
}

use js::{ArrayBuffer, Ctx, Error, IntoJs};

pub struct JsArrayBuffer(pub(crate) Vec<u8>);

impl<'js> IntoJs<'js> for JsArrayBuffer {
	fn into_js(self, ctx: Ctx<'js>) -> Result<js::Value<'js>, Error> {
		ArrayBuffer::new(ctx, self.0).map(|ab| ab.into_value())
	}
}

// TODO: just convert once
// pub struct JSONStr(String);

// impl<'js> IntoJs<'js> for JSONStr {
// 	fn into_js(self, ctx: Ctx<'js>) -> Result<js::Value<'js>, Error> {
// 		let json = ctx.globals().get::<_, js::Object>("JSON").unwrap();
// 		let json_parse = json.get::<_, js::Function>("parse").unwrap();
// 		json_parse.call::<_, js::Value>((This(json), self.0))
// 	}
// }
