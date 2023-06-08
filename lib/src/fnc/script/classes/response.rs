use std::{cell::RefCell, rc::Rc};

use js::{
	class::{HasRefs, RefsMarker},
	prelude::Coerced,
	ArrayBuffer, Class, Ctx, Error, Exception, Object, Persistent, Result, TypedArray, Value,
};
use reqwest::{StatusCode, Url};

use super::{blob::blob::Blob, headers::headers::Headers};

#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod response {

	use crate::fnc::script::classes::{blob::blob::Blob, headers::headers::Headers};

	use super::{BodyInit, ResponseInit};
	use js::{
		function::{Opt, Rest},
		ArrayBuffer, Class, Ctx, Result, Value,
	};
	use reqwest::{header::HeaderName, Url};

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Response {
		#[quickjs(has_refs)]
		pub(crate) body: Option<BodyInit>,
		#[quickjs(has_refs)]
		pub(crate) init: ResponseInit,
		pub(crate) url: Option<Url>,
	}

	impl Response {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(
			ctx: Ctx<'_>,
			body: Opt<Option<BodyInit>>,
			init: Opt<ResponseInit>,
			args: Rest<()>,
		) -> Result<Self> {
			let init = match init.into_inner() {
				Some(x) => x,
				None => ResponseInit::default(ctx)?,
			};

			Ok(Self::new_inner(None, body.into_inner().and_then(|x| x), init))
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		#[quickjs(get)]
		pub fn bodyUsed(&self) -> bool {
			match self.body {
				Some(BodyInit::Stream(ref stream)) => stream.borrow().is_none(),
				_ => true,
			}
		}

		#[quickjs(get)]
		pub fn status(&self) -> i32 {
			self.init.status.as_u16() as i32
		}

		#[quickjs(get)]
		pub fn ok(&self) -> bool {
			self.init.status.is_success()
		}

		#[quickjs(get)]
		pub fn statusText(&self) -> &str {
			self.init.status.canonical_reason().unwrap_or("")
		}

		#[quickjs(get)]
		pub fn headers<'js>(&self, ctx: Ctx<'js>) -> Class<'js, Headers> {
			self.init.headers.clone().restore(ctx).unwrap()
		}

		#[quickjs(get)]
		pub fn url(&self) -> Option<String> {
			self.url.as_ref().map(|x| x.to_string())
		}

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Response]")
		}

		// Creates a copy of the request object
		#[quickjs(rename = "clone")]
		pub fn copy(&self, args: Rest<()>) -> Response {
			self.clone()
		}

		// Returns a promise with the response body as a Blob
		pub async fn blob<'js>(self, ctx: Ctx<'_>, args: Rest<()>) -> Result<Class<Blob>> {
			let stream = self.take_stream(ctx)?;
			let headers = self.init.headers.restore(ctx).unwrap();
			let mime = {
				let headers = headers.as_class_def().inner.borrow();
				let key = HeaderName::from_static("content-type");
				let types = headers.get_all(key);
				// TODO: This is not according to spec.
				types.iter().next().map(|x| x.to_str().unwrap_or("text/html").to_owned())
			};

			let data = stream.bytes().await.map_err(|e| throw!(ctx, e))?.to_vec();

			Class::instance(
				ctx,
				Blob {
					mime,
					data,
				},
			)
		}

		// Returns a promise with the response body as FormData
		pub async fn formData(self, ctx: Ctx<'_>, args: Rest<()>) -> Result<Value<'_>> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the response body as JSON
		pub async fn json(self, ctx: Ctx<'_>, args: Rest<()>) -> Result<Value<'_>> {
			let stream = self.take_stream(ctx)?;
			let text = stream.text().await.map_err(|e| throw!(ctx, e))?;
			ctx.json_parse(text)
		}

		// Returns a promise with the response body as text
		pub async fn text(self, ctx: Ctx<'_>, args: Rest<()>) -> Result<String> {
			let stream = self.take_stream(ctx)?;
			let text = stream.text().await.map_err(|e| throw!(ctx, e))?;
			Ok(text)
		}

		// Returns a promise with the response body as text
		pub async fn arrayBuffer(self, ctx: Ctx<'_>, args: Rest<()>) -> Result<ArrayBuffer<'_>> {
			let stream = self.take_stream(ctx)?;
			let bytes = stream.bytes().await.map_err(|e| throw!(ctx, e))?;
			ArrayBuffer::new(ctx, bytes)
		}

		// ------------------------------
		// Static methods
		// ------------------------------

		// Returns a new response representing a network error
		pub fn error(ctx: Ctx<'_>, args: Rest<()>) -> Result<Response> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Creates a new response with a different URL
		pub fn redirect(ctx: Ctx<'_>, args: Rest<()>) -> Result<Response> {
			Err(throw!(ctx, "Not yet implemented"))
		}
	}
}
use response::Response as ResponseClass;

impl ResponseClass {
	pub fn new_inner(url: Option<Url>, body: Option<BodyInit>, init: ResponseInit) -> Self {
		ResponseClass {
			body,
			init,
			url,
		}
	}

	fn take_stream(&self, ctx: Ctx<'_>) -> Result<reqwest::Response> {
		if let Some(BodyInit::Stream(ref stream)) = self.body {
			if let Some(x) = stream.borrow_mut().take() {
				return Ok(x);
			}
		}
		Err(Exception::throw_type(ctx, "Body is unusable"))
	}
}

#[derive(Clone)]
pub enum BodyInit {
	Blob(Persistent<js::Class<'static, Blob>>),
	ArrayI8(Persistent<TypedArray<'static, i8>>),
	ArrayU8(Persistent<TypedArray<'static, u8>>),
	ArrayI16(Persistent<TypedArray<'static, i16>>),
	ArrayU16(Persistent<TypedArray<'static, u16>>),
	ArrayI32(Persistent<TypedArray<'static, i32>>),
	ArrayU32(Persistent<TypedArray<'static, u32>>),
	ArrayI64(Persistent<TypedArray<'static, i64>>),
	ArrayU64(Persistent<TypedArray<'static, u64>>),
	ArrayBuffer(Persistent<ArrayBuffer<'static>>),
	FormData(()),
	URLSearchParams(()),
	String(Persistent<js::String<'static>>),
	Stream(Rc<RefCell<Option<reqwest::Response>>>),
}

impl HasRefs for BodyInit {
	fn mark_refs(&self, marker: &RefsMarker) {
		match *self {
			BodyInit::Blob(ref x) => x.mark_refs(marker),
			BodyInit::ArrayI8(ref x) => x.mark_refs(marker),
			BodyInit::ArrayU8(ref x) => x.mark_refs(marker),
			BodyInit::ArrayI16(ref x) => x.mark_refs(marker),
			BodyInit::ArrayU16(ref x) => x.mark_refs(marker),
			BodyInit::ArrayI32(ref x) => x.mark_refs(marker),
			BodyInit::ArrayU32(ref x) => x.mark_refs(marker),
			BodyInit::ArrayI64(ref x) => x.mark_refs(marker),
			BodyInit::ArrayU64(ref x) => x.mark_refs(marker),
			BodyInit::ArrayBuffer(ref x) => x.mark_refs(marker),
			BodyInit::String(ref x) => x.mark_refs(marker),
			BodyInit::FormData(_) | BodyInit::URLSearchParams(_) | BodyInit::Stream(_) => {}
		}
	}
}

impl<'js> js::FromJs<'js> for BodyInit {
	fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = match value.type_of() {
			js::Type::String => {
				let s = Persistent::save(ctx, value.into_string().unwrap());
				return Ok(BodyInit::String(s));
			}
			js::Type::Object => value.as_object().unwrap(),
			x => {
				return Err(Error::FromJs {
					from: x.as_str(),
					to: "Blob, TypedArray, FormData, URLSearchParams, or String",
					message: None,
				})
			}
		};
		if let Ok(x) = js::Class::<Blob>::from_object(object.clone()) {
			return Ok(BodyInit::Blob(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<i8>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayI8(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<u8>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayU8(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<i16>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayI16(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<u16>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayU16(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<i32>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayI32(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<u32>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayU32(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<i64>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayI64(Persistent::save(ctx, x)));
		}
		if let Ok(x) = TypedArray::<u64>::from_object(object.clone()) {
			return Ok(BodyInit::ArrayU64(Persistent::save(ctx, x)));
		}
		if let Ok(x) = ArrayBuffer::from_object(object.clone()) {
			return Ok(BodyInit::ArrayBuffer(Persistent::save(ctx, x)));
		}

		Err(Error::FromJs {
			from: "object",
			to: "Blob, TypedArray, FormData, URLSearchParams, or String",
			message: None,
		})
	}
}

#[derive(Clone)]
pub struct ResponseInit {
	pub status: StatusCode,
	pub status_text: String,
	pub headers: Persistent<Class<'static, Headers>>,
}

impl HasRefs for ResponseInit {
	fn mark_refs(&self, marker: &RefsMarker) {
		self.headers.mark_refs(marker);
	}
}

impl ResponseInit {
	/// Returns a ResponseInit object with all values as the default value.
	pub fn default(ctx: Ctx<'_>) -> Result<ResponseInit> {
		let headers = Class::instance(ctx, Headers::new_empty())?;
		let headers = Persistent::save(ctx, headers);
		Ok(ResponseInit {
			status: StatusCode::OK,
			status_text: String::new(),
			headers,
		})
	}
}

/// Test whether a string matches the reason phrase http spec production.
fn is_reason_phrase(text: &str) -> bool {
	// Cannot be empty
	!text.is_empty()
		// all characters match VCHAR (0x21..=0x7E), obs-text (0x80..=0xFF), HTAB, or SP
		&& text.as_bytes().iter().all(|b| matches!(b,0x21..=0x7E | 0x80..=0xFF | b'\t' | b' '))
}

impl<'js> js::FromJs<'js> for ResponseInit {
	fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = Object::from_js(ctx, value)?;

		let status =
			if let Some(Coerced(status)) = object.get::<_, Option<Coerced<i32>>>("status")? {
				if !(200..=599).contains(&status) {
					return Err(Exception::throw_range(ctx, "response status code outside range"));
				}
				StatusCode::from_u16(status as u16).unwrap()
			} else {
				StatusCode::OK
			};

		let status_text = if let Some(Coerced(string)) =
			object.get::<_, Option<Coerced<String>>>("statusText")?
		{
			if !is_reason_phrase(string.as_str()) {
				return Err(Exception::throw_type(ctx, "statusText was not a valid reason phrase"));
			}
			string
		} else {
			String::new()
		};

		let headers = if let Some(headers) = object.get::<_, Option<Value>>("headers")? {
			let headers = Headers::new_inner(ctx, headers)?;
			Class::instance(ctx, headers)?
		} else {
			Class::instance(ctx, Headers::new_empty())?
		};
		let headers = Persistent::save(ctx, headers);

		Ok(ResponseInit {
			status,
			status_text,
			headers,
		})
	}
}
