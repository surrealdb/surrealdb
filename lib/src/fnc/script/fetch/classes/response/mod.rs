//! Response class implementation

use super::{Blob, Headers};
use crate::fnc::script::fetch::{
	body::{Body, BodyAndKind},
	util,
};
use crate::http::header::{self, HeaderValue};
use bytes::Bytes;
use js::{
	class::Trace,
	prelude::{Opt, This},
	ArrayBuffer, Class, Ctx, Exception, Result, Value,
};
use std::mem;
use url::Url;

mod init;
pub use init::ResponseInit;

#[allow(dead_code)]
#[derive(Clone, Copy)]
pub enum ResponseType {
	Basic,
	Cors,
	Default,
	Error,
	Opaque,
	OpaqueRedirect,
}

#[allow(dead_code)]
#[derive(Trace)]
#[js::class]
#[qjs(rename_all = "camelCase")]
pub struct Response<'js> {
	#[qjs(skip_trace)]
	pub(crate) body: Body,
	#[qjs(skip_trace)]
	pub(crate) url: Option<Url>,
	#[qjs(skip_trace)]
	pub(crate) r#type: ResponseType,
	pub(crate) was_redirected: bool,
	#[qjs(get)]
	pub(crate) status: u16,
	#[qjs(get)]
	pub(crate) status_text: String,
	#[qjs(get)]
	pub(crate) headers: Class<'js, Headers>,
}

#[js::methods]
impl<'js> Response<'js> {
	// ------------------------------
	// Constructor
	// ------------------------------

	#[qjs(constructor)]
	pub fn new(
		ctx: Ctx<'js>,
		body: Opt<Option<BodyAndKind>>,
		init: Opt<ResponseInit<'js>>,
	) -> Result<Self> {
		let ResponseInit {
			status,
			status_text,
			headers,
		} = match init.into_inner() {
			Some(x) => x,
			None => ResponseInit::default(ctx.clone())?,
		};
		let body = body.into_inner().and_then(|x| x);
		let body = if let Some(body) = body {
			if util::is_null_body_status(status) {
				// Null body statuses are not allowed to have a body.
				return Err(Exception::throw_type(
					&ctx,
					&format!("Response with status `{}` is not allowed to have a body", status),
				));
			}

			let mut headers = headers.try_borrow_mut()?;
			body.apply_to_headers(&mut headers.inner)
		} else {
			Body::empty()
		};

		Ok(Response {
			body,
			url: None,
			r#type: ResponseType::Default,
			was_redirected: false,
			status,
			headers,
			status_text,
		})
	}

	// ------------------------------
	// Instance properties
	// ------------------------------

	#[qjs(get, rename = "bodyUsed")]
	pub fn body_used(&self) -> bool {
		self.body.is_used()
	}

	#[qjs(get)]
	pub fn ok(&self) -> bool {
		util::is_ok_status(self.status)
	}

	#[qjs(get)]
	pub fn redirected(&self) -> bool {
		self.was_redirected
	}

	#[qjs(get, rename = "type")]
	pub fn r#type(&self) -> &'static str {
		match self.r#type {
			ResponseType::Basic => "basic",
			ResponseType::Cors => "cors",
			ResponseType::Default => "default",
			ResponseType::Error => "error",
			ResponseType::Opaque => "opaque",
			ResponseType::OpaqueRedirect => "opaqueredirect",
		}
	}

	#[qjs(get)]
	pub fn url(&self) -> Option<String> {
		self.url.as_ref().map(|x| {
			if x.fragment().is_some() {
				let mut res = x.clone();
				res.set_fragment(None);
				res.to_string()
			} else {
				x.to_string()
			}
		})
	}

	// ------------------------------
	// Instance methods
	// ------------------------------

	// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		String::from("[object Response]")
	}

	// Creates a copy of the request object
	#[qjs(rename = "clone")]
	pub fn clone_js(&mut self, ctx: Ctx<'js>) -> Self {
		Response {
			headers: self.headers.clone(),
			body: self.body.clone_js(&ctx),
			url: self.url.clone(),
			r#type: self.r#type,
			was_redirected: self.was_redirected,
			status_text: self.status_text.clone(),
			status: self.status,
		}
	}

	#[qjs(skip)]
	async fn take_buffer(this: This<Class<'js, Self>>, ctx: &Ctx<'js>) -> Result<Bytes> {
		// Manually borrowing the class to ensure we don't hold the guard across awaits.
		let body = {
			let mut borrow = this.try_borrow_mut()?;
			mem::replace(&mut borrow.body, Body::used())
		};

		if body.is_used() {
			return Err(Exception::throw_type(ctx, "Body unusable"));
		}

		match body.into_buffer().await {
			Ok(Some(x)) => Ok(x),
			Ok(None) => Ok(Bytes::new()),
			Err(e) => Err(Exception::throw_type(ctx, &format!("stream failed: {e}"))),
		}
	}

	// Returns a promise with the response body as a Blob
	pub async fn blob(this: This<Class<'js, Self>>, ctx: Ctx<'js>) -> Result<Blob> {
		let mime = {
			// Don't hold the borrow across take_buffer
			let borrow = this.try_borrow()?;
			let headers = borrow.headers.clone();
			let headers = headers.try_borrow()?;
			let headers = &headers.inner;
			let types = headers.get_all(crate::http::header::CONTENT_TYPE);
			// TODO: This is not according to spec.
			types
				.iter()
				.next()
				.map(|x| x.to_str().unwrap_or("text/html"))
				.unwrap_or("text/html")
				.to_owned()
		};

		let data = Self::take_buffer(this, &ctx).await?;
		Ok(Blob {
			mime,
			data,
		})
	}

	// Returns a promise with the response body as FormData
	#[qjs(rename = "formData")]
	pub async fn form_data(
		_this: This<Class<'js, Response<'js>>>,
		ctx: Ctx<'js>,
	) -> Result<Value<'js>> {
		Err(Exception::throw_internal(&ctx, "Not yet implemented"))
	}

	// Returns a promise with the response body as JSON
	pub async fn json(this: This<Class<'js, Self>>, ctx: Ctx<'js>) -> Result<Value<'js>> {
		let text = Self::text(this, ctx.clone()).await?;
		ctx.json_parse(text)
	}

	// Returns a promise with the response body as text
	pub async fn text(this: This<Class<'js, Self>>, ctx: Ctx<'js>) -> Result<String> {
		let data = Self::take_buffer(this, &ctx).await?;

		// Skip UTF-BOM
		if data.starts_with(&[0xEF, 0xBB, 0xBF]) {
			Ok(String::from_utf8_lossy(&data[3..]).into_owned())
		} else {
			Ok(String::from_utf8_lossy(&data).into_owned())
		}
	}

	// Returns a promise with the response body as text
	#[qjs(rename = "arrayBuffer")]
	pub async fn array_buffer(
		this: This<Class<'js, Self>>,
		ctx: Ctx<'js>,
	) -> Result<ArrayBuffer<'js>> {
		let data = Self::take_buffer(this, &ctx).await?;
		ArrayBuffer::new(ctx, data)
	}

	// ------------------------------
	// Static methods
	// ------------------------------

	#[qjs(static, rename = "json")]
	pub fn static_json(
		ctx: Ctx<'js>,
		data: Value<'js>,
		init: Opt<ResponseInit<'js>>,
	) -> Result<Self> {
		let json = ctx.json_stringify(data)?;
		let json =
			json.ok_or_else(|| Exception::throw_type(&ctx, "Value is not JSON serializable"))?;
		let json = json.to_string()?;

		let ResponseInit {
			status,
			status_text,
			headers,
		} = if let Some(init) = init.into_inner() {
			init
		} else {
			ResponseInit::default(ctx)?
		};

		headers.try_borrow_mut()?.inner.entry(header::CONTENT_TYPE).or_insert_with(|| {
			let mime = mime::APPLICATION_JSON;
			// Should always be a correct header value;
			HeaderValue::try_from(mime.to_string()).unwrap()
		});

		Ok(Response {
			url: None,
			body: Body::from(json),
			status,
			status_text,
			headers,
			r#type: ResponseType::Default,
			was_redirected: false,
		})
	}

	// Returns a new response representing a network error
	#[qjs(static)]
	pub fn error(ctx: Ctx<'js>) -> Result<Self> {
		let headers = Class::instance(ctx, Headers::new_empty())?;
		Ok(Response {
			url: None,
			body: Body::empty(),
			status: 0,
			status_text: String::new(),
			headers,
			r#type: ResponseType::Error,
			was_redirected: false,
		})
	}

	// Creates a new response with a different URL
	#[qjs(static)]
	pub fn redirect(ctx: Ctx<'_>, url: String, status: Opt<u32>) -> Result<Response> {
		let url = url
			.parse::<Url>()
			.map_err(|e| Exception::throw_type(&ctx, &format!("Invalid url: {e}")))?;

		let status = status.into_inner().unwrap_or(302) as u16;
		if !util::is_redirect_status(status) {
			return Err(Exception::throw_range(&ctx, "Status code is not a redirect status"));
		}

		let mut headers = Headers::new_empty();
		// A valid url should be a valid header value.
		headers.inner.insert(header::LOCATION, HeaderValue::try_from(url.to_string()).unwrap());
		let headers = Class::instance(ctx, headers)?;

		Ok(Response {
			url: Some(url),
			body: Body::empty(),
			status,
			status_text: String::new(),
			headers,
			r#type: ResponseType::Default,
			was_redirected: false,
		})
	}
}

#[cfg(test)]
mod test {
	use crate::fnc::script::fetch::test::create_test_context;
	use js::{promise::Promise, CatchResultExt};

	#[tokio::test]
	async fn basic_response_use() {
		create_test_context!(ctx => {
			ctx.eval::<Promise<()>,_>(r#"
				(async () => {
					let resp = new Response();
					assert(!resp.bodyUsed);
					assert.seq(resp.status,200);
					assert.seq(resp.ok,true);
					assert.seq(resp.statusText,'');
					assert.seq(await resp.text(),'');
					assert(resp.bodyUsed);

					// invalid status
					assert.mustThrow(() => {
						new Response(undefined,{ status: 9001})
					})

					// statusText not a reason phrase
					assert.mustThrow(() => {
						new Response(undefined,{ statusText: " \r"})
					})

					resp = Response.json({ a: 1, b: "2", c: { d: 3 }},{ headers: { "SomeHeader": "Some-Value" }});
					assert.seq(resp.status,200);
					assert.seq(resp.ok,true);
					assert.seq(resp.statusText,'');
					assert.seq(resp.headers.get("SomeHeader"),"Some-Value");
					assert.seq(resp.headers.get("content-type"),"application/json");
					let obj = await resp.json();
					assert.seq(typeof obj, "object");
					assert.seq(obj.a, 1);
					assert.seq(obj.b, "2");
					assert.seq(typeof obj.c, "object");
					assert.seq(obj.c.d, 3);

					resp = Response.error();
					assert.seq(resp.status,0);
					assert.seq(resp.ok,false);
					assert.seq(resp.statusText,'');

					resp = Response.redirect("http://a");
					assert.seq(resp.status,302);
					assert.seq(resp.ok,false);
					assert.seq(resp.headers.get("location"),"http://a/");

					// not a redirect status
					assert.mustThrow(() => {
						Response.redirect("http://a",200);
					})

					// url required
					assert.mustThrow(() => {
						Response.redirect();
					})

					// invalid url
					assert.mustThrow(() => {
						Response.redirect("invalid url");
					})

					resp = new Response("some text");
					let resp_2 = resp.clone();
					assert.seq(resp.headers.get("content-type"),"text/plain; charset=utf-8");
					assert.seq(await resp.text(),"some text");
					assert.seq(await resp_2.text(),"some text");


				})()
			"#).catch(&ctx).unwrap().await.catch(&ctx).unwrap();
		})
		.await;
	}
}
