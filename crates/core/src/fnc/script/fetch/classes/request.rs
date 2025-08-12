//! Request class implementation

use bytes::Bytes;
use js::class::Trace;
use js::function::Opt;
use js::prelude::Coerced;
use js::{Class, Ctx, Exception, FromJs, JsLifetime, Object, Result, Value};
use reqwest::header::HeaderName;
use reqwest::{Method, Url};

use super::{Blob, Headers};
use crate::fnc::script::fetch::RequestError;
use crate::fnc::script::fetch::body::Body;

#[derive(Clone, Copy, Eq, PartialEq, JsLifetime)]
pub enum RequestMode {
	Navigate,
	SameOrigin,
	NoCors,
	Cors,
}

impl<'js> FromJs<'js> for RequestMode {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let res = if let Some(Coerced(x)) = <Option<Coerced<String>>>::from_js(ctx, value)? {
			match x.as_str() {
				"navigate" => RequestMode::Navigate,
				"same-origin" => RequestMode::SameOrigin,
				"no-cors" => RequestMode::NoCors,
				"cors" => RequestMode::Cors,
				x => {
					return Err(Exception::throw_type(
						ctx,
						&format!(
							"unexpected request mode `{}`, expected one of \
							`navigate`,\
							`same-origin`,\
							`no-cors`,\
							or `cors`",
							x
						),
					));
				}
			}
		} else {
			RequestMode::Cors
		};
		Ok(res)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, JsLifetime)]
pub enum RequestCredentials {
	Omit,
	SameOrigin,
	Include,
}

impl<'js> FromJs<'js> for RequestCredentials {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let res = if let Some(Coerced(x)) = <Option<Coerced<String>>>::from_js(ctx, value)? {
			match x.as_str() {
				"omit" => RequestCredentials::Omit,
				"same-origin" => RequestCredentials::SameOrigin,
				"include" => RequestCredentials::Include,
				x => {
					return Err(Exception::throw_type(
						ctx,
						&format!(
							"unexpected request credentials `{}`, expected one of \
								`omit`\
								, `same-oring`\
								, or `include`",
							x
						),
					));
				}
			}
		} else {
			RequestCredentials::Omit
		};
		Ok(res)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, JsLifetime)]
pub enum RequestCache {
	Default,
	NoStore,
	Reload,
	NoCache,
	ForceCache,
	OnlyIfCached,
}

impl<'js> FromJs<'js> for RequestCache {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let res = if let Some(Coerced(x)) = <Option<Coerced<String>>>::from_js(ctx, value)? {
			match x.as_str() {
				"default" => RequestCache::Default,
				"no-store" => RequestCache::NoStore,
				"reload" => RequestCache::Reload,
				"no-cache" => RequestCache::NoCache,
				"force-cache" => RequestCache::ForceCache,
				"only-if-cached" => RequestCache::OnlyIfCached,
				x => {
					return Err(Exception::throw_type(
						ctx,
						&format!(
							"unexpected request cache `{}`, expected one of \
								`default`\
								, `no-store`\
								, `reload`\
								, `no-cache`\
								, `force-cache`\
								, or `only-if-cached`",
							x
						),
					));
				}
			}
		} else {
			RequestCache::Default
		};
		Ok(res)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, JsLifetime)]
pub enum RequestRedirect {
	Follow,
	Error,
	Manual,
}

impl<'js> FromJs<'js> for RequestRedirect {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let res = if let Some(Coerced(x)) = <Option<Coerced<String>>>::from_js(ctx, value)? {
			match x.as_str() {
				"follow" => RequestRedirect::Follow,
				"error" => RequestRedirect::Error,
				"manual" => RequestRedirect::Manual,
				x => {
					return Err(Exception::throw_type(
						ctx,
						&format!(
							"unexpected request redirect `{}`, expected one of \
							`follow`,\
							`error`,\
							or `manual`",
							x
						),
					));
				}
			}
		} else {
			RequestRedirect::Follow
		};
		Ok(res)
	}
}

#[derive(Clone, Copy, Eq, PartialEq, JsLifetime)]
pub enum ReferrerPolicy {
	Empty,
	NoReferrer,
	NoReferrerWhenDowngrade,
	SameOrigin,
	Origin,
	StrictOrigin,
	OriginWhenCrossOrigin,
	StrictOriginWhenCrossOrigin,
	UnsafeUrl,
}

impl<'js> FromJs<'js> for ReferrerPolicy {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let res = if let Some(Coerced(x)) = <Option<Coerced<String>>>::from_js(ctx, value)? {
			match x.as_str() {
				"" => ReferrerPolicy::Empty,
				"no-referrer" => ReferrerPolicy::NoReferrer,
				"no-referrer-when-downgrade" => ReferrerPolicy::NoReferrerWhenDowngrade,
				"same-origin" => ReferrerPolicy::SameOrigin,
				"origin" => ReferrerPolicy::Origin,
				"strict-origin" => ReferrerPolicy::StrictOrigin,
				"origin-when-cross-origin" => ReferrerPolicy::OriginWhenCrossOrigin,
				"strict-origin-when-cross-origin" => ReferrerPolicy::StrictOriginWhenCrossOrigin,
				"unsafe-url" => ReferrerPolicy::UnsafeUrl,
				x => {
					return Err(Exception::throw_type(
						ctx,
						&format!(
							"unexpected referrer policy `{}`, expected one of \
							, ``\
							, `no-referrer`\
							, `no-referrer-when-downgrade`\
							, `same-origin`\
							, `strict-origin`\
							, `origin-when-cross-origin`\
							, `strict-origin-when-cross-origin`\
							, or `unsafe-url1`",
							x
						),
					));
				}
			}
		} else {
			ReferrerPolicy::Empty
		};
		Ok(res)
	}
}

#[derive(JsLifetime)]
pub struct RequestInit<'js> {
	pub method: Method,
	pub headers: Class<'js, Headers>,
	pub body: Option<Body>,
	pub referrer: String,
	pub referrer_policy: ReferrerPolicy,
	pub request_mode: RequestMode,
	pub request_credentials: RequestCredentials,
	pub request_cache: RequestCache,
	pub request_redirect: RequestRedirect,
	pub integrity: String,
	pub keep_alive: bool,
}

impl<'js> Trace<'js> for RequestInit<'js> {
	fn trace<'a>(&self, tracer: js::class::Tracer<'a, 'js>) {
		self.headers.trace(tracer);
	}
}

impl<'js> RequestInit<'js> {
	pub fn default(ctx: Ctx<'js>) -> Result<Self> {
		let headers = Class::instance(ctx, Headers::new_empty())?;
		Ok(RequestInit {
			method: Method::GET,
			headers,
			body: None,
			referrer: "client".to_string(),
			referrer_policy: ReferrerPolicy::Empty,
			request_mode: RequestMode::Cors,
			request_credentials: RequestCredentials::SameOrigin,
			request_cache: RequestCache::Default,
			request_redirect: RequestRedirect::Follow,
			integrity: String::new(),
			keep_alive: false,
		})
	}

	pub fn clone_js(&self, ctx: Ctx<'js>) -> Result<Self> {
		let headers = self.headers.clone();
		let headers = Class::instance(ctx.clone(), headers.borrow().clone())?;

		let body = self.body.as_ref().map(|x| x.clone_js(ctx));

		Ok(RequestInit {
			method: self.method.clone(),
			headers,
			body,
			referrer: self.referrer.clone(),
			referrer_policy: self.referrer_policy,
			request_mode: self.request_mode,
			request_credentials: self.request_credentials,
			request_cache: self.request_cache,
			request_redirect: self.request_redirect,
			integrity: self.integrity.clone(),
			keep_alive: self.keep_alive,
		})
	}
}

// Normalize method string according to spec.
fn normalize_method(ctx: &Ctx<'_>, m: String) -> Result<Method> {
	if m.as_bytes().eq_ignore_ascii_case(b"CONNECT")
		|| m.as_bytes().eq_ignore_ascii_case(b"TRACE")
		|| m.as_bytes().eq_ignore_ascii_case(b"TRACK")
	{
		//methods that are not allowed [`https://fetch.spec.whatwg.org/#methods`]
		return Err(Exception::throw_type(ctx, &format!("method {m} is forbidden")));
	}

	// The following methods must be uppercased to the default case insensitive
	// equivalent.
	if m.as_bytes().eq_ignore_ascii_case(b"DELETE") {
		return Ok(Method::DELETE);
	}
	if m.as_bytes().eq_ignore_ascii_case(b"GET") {
		return Ok(Method::GET);
	}
	if m.as_bytes().eq_ignore_ascii_case(b"HEAD") {
		return Ok(Method::HEAD);
	}
	if m.as_bytes().eq_ignore_ascii_case(b"OPTIONS") {
		return Ok(Method::OPTIONS);
	}
	if m.as_bytes().eq_ignore_ascii_case(b"POST") {
		return Ok(Method::POST);
	}
	if m.as_bytes().eq_ignore_ascii_case(b"PUT") {
		return Ok(Method::PUT);
	}

	match Method::from_bytes(m.as_bytes()) {
		Ok(x) => Ok(x),
		Err(e) => Err(Exception::throw_type(ctx, &format!("invalid method: {e}"))),
	}
}

impl<'js> FromJs<'js> for RequestInit<'js> {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = Object::from_js(ctx, value)?;

		let referrer = object
			.get::<_, Option<Coerced<String>>>("referrer")?
			.map(|x| x.0)
			.unwrap_or_else(|| "client".to_owned());
		let method = object
			.get::<_, Option<String>>("method")?
			.map(|m| normalize_method(ctx, m))
			.transpose()?
			.unwrap_or(Method::GET);

		let referrer_policy = object.get("referrerPolicy")?;
		let request_mode = object.get("mode")?;
		let request_redirect = object.get("redirect")?;
		let request_cache = object.get("cache")?;
		let request_credentials = object.get("credentials")?;
		let integrity = object
			.get::<_, Option<Coerced<String>>>("integrity")?
			.map(|x| x.0)
			.unwrap_or_else(String::new);
		let keep_alive =
			object.get::<_, Option<Coerced<bool>>>("keep_alive")?.map(|x| x.0).unwrap_or_default();

		// duplex can only be `half`
		if let Some(Coerced(x)) = object.get::<_, Option<Coerced<String>>>("duplex")? {
			if x != "half" {
				return Err(Exception::throw_type(
					ctx,
					&format!("unexpected request duplex `{}` expected `half`", x),
				));
			}
		}

		let headers = if let Some(hdrs) = object.get::<_, Option<Object>>("headers")? {
			if let Some(cls) = Class::<Headers>::from_object(&hdrs) {
				cls
			} else {
				Class::instance(ctx.clone(), Headers::new_inner(ctx, hdrs.into_value())?)?
			}
		} else {
			Class::instance(ctx.clone(), Headers::new_empty())?
		};

		let body = object.get::<_, Option<Body>>("body")?;

		Ok(Self {
			method,
			headers,
			body,
			referrer,
			referrer_policy,
			request_mode,
			request_credentials,
			request_cache,
			request_redirect,
			integrity,
			keep_alive,
		})
	}
}

#[js::class]
#[derive(Trace, JsLifetime)]
pub struct Request<'js> {
	#[qjs(skip_trace)]
	pub(crate) url: Url,
	pub(crate) init: RequestInit<'js>,
}

#[js::methods]
impl<'js> Request<'js> {
	// ------------------------------
	// Constructor
	// ------------------------------

	#[qjs(constructor)]
	pub fn new(ctx: Ctx<'js>, input: Value<'js>, init: Opt<RequestInit<'js>>) -> Result<Self> {
		if let Some(url) = input.as_string() {
			// url string
			let url_str = url.to_string()?;
			let url = Url::parse(&url_str)
				.map_err(|e| Exception::throw_type(&ctx, &format!("failed to parse url: {e}")))?;

			if !url.username().is_empty() || !url.password().map(str::is_empty).unwrap_or(true) {
				// url cannot contain non empty username and passwords
				return Err(Exception::throw_type(&ctx, "Url contained credentials."));
			}
			let init = init.into_inner().map_or_else(|| RequestInit::default(ctx.clone()), Ok)?;
			// HEAD and GET methods can't have a body
			if init.body.is_some() && init.method == Method::GET || init.method == Method::HEAD {
				return Err(Exception::throw_type(
					&ctx,
					&format!("Request with method `{}` cannot have a body", init.method),
				));
			}

			Ok(Self {
				url,
				init,
			})
		} else if let Some(request) = input.as_object().and_then(Class::<Self>::from_object) {
			// existing request object, just return it
			request.try_borrow()?.clone_js(ctx.clone())
		} else {
			Err(Exception::throw_type(
				&ctx,
				"request `init` parameter must either be a request object or a string",
			))
		}
	}

	/// Clone the response, teeing any possible underlying streams.
	#[qjs(rename = "clone")]
	pub fn clone_js(&self, ctx: Ctx<'js>) -> Result<Self> {
		Ok(Self {
			url: self.url.clone(),
			init: self.init.clone_js(ctx)?,
		})
	}

	// ------------------------------
	// Instance properties
	// ------------------------------
	#[qjs(get, rename = "body_used")]
	pub fn body_used(&self) -> bool {
		self.init.body.as_ref().map(Body::used).unwrap_or(true)
	}

	#[qjs(get)]
	pub fn method(&self) -> String {
		self.init.method.to_string()
	}

	#[qjs(get)]
	pub fn url(&self) -> String {
		self.url.to_string()
	}

	#[qjs(get)]
	pub fn headers(&self) -> Class<'js, Headers> {
		self.init.headers.clone()
	}

	#[qjs(get)]
	pub fn referrer(&self) -> String {
		self.init.referrer.clone()
	}
	// TODO

	// ------------------------------
	// Instance methods
	// ------------------------------

	// Convert the object to a string
	#[qjs(rename = "toString")]
	pub fn js_to_string(&self) -> String {
		String::from("[object Request]")
	}

	/// Takes the buffer from the body leaving it used.
	#[qjs(skip)]
	async fn take_buffer(&self, ctx: &Ctx<'js>) -> Result<Bytes> {
		let Some(body) = self.init.body.as_ref() else {
			return Ok(Bytes::new());
		};
		match body.to_buffer().await {
			Ok(Some(x)) => Ok(x),
			Ok(None) => Err(Exception::throw_type(ctx, "Body unusable")),
			Err(e) => match e {
				RequestError::Reqwest(e) => {
					Err(Exception::throw_type(ctx, &format!("stream failed: {e}")))
				}
			},
		}
	}

	// Returns a promise with the request body as a Blob
	pub async fn blob(&self, ctx: Ctx<'js>) -> Result<Blob> {
		let headers = self.init.headers.clone();
		let mime = {
			let headers = headers.borrow();
			let headers = &headers.inner;
			let key = HeaderName::from_static("content-type");
			let types = headers.get_all(key);
			// TODO: This is not according to spec.
			types
				.iter()
				.next()
				.map(|x| x.to_str().unwrap_or("text/html"))
				.unwrap_or("text/html")
				.to_owned()
		};

		let data = self.take_buffer(&ctx).await?;
		Ok(Blob {
			mime,
			data,
		})
	}

	// Returns a promise with the request body as FormData
	#[qjs(rename = "formData")]
	pub async fn form_data(&self, ctx: Ctx<'js>) -> Result<Value<'js>> {
		Err(Exception::throw_internal(&ctx, "Not yet implemented"))
	}

	// Returns a promise with the request body as JSON
	pub async fn json(&self, ctx: Ctx<'js>) -> Result<Value<'js>> {
		let text = self.text(ctx.clone()).await?;
		ctx.json_parse(text)
	}

	// Returns a promise with the request body as text
	pub async fn text(&self, ctx: Ctx<'js>) -> Result<String> {
		let data = self.take_buffer(&ctx).await?;

		// Skip UTF-BOM
		if data.starts_with(&[0xEF, 0xBB, 0xBF]) {
			Ok(String::from_utf8_lossy(&data[3..]).into_owned())
		} else {
			Ok(String::from_utf8_lossy(&data).into_owned())
		}
	}
}

#[cfg(test)]
mod test {
	use js::CatchResultExt;
	use js::promise::Promise;

	use crate::fnc::script::fetch::test::create_test_context;

	#[tokio::test]
	async fn basic_request_use() {
		create_test_context!(ctx => {
			ctx.eval::<Promise,_>(r#"
				(async () => {
					assert.mustThrow(() => {
						new Request("invalid url")
					});
					assert.mustThrow(() => {
						new Request("http://invalid url")
					});
					// no credentials
					assert.mustThrow(() => {
						new Request("http://username:password@some_url.com")
					});
					// invalid option value
					assert.mustThrow(() => {
						new Request("http://a",{ referrerPolicy: "invalid value"})
					});
					assert.mustThrow(() => {
						new Request("http://a",{ mode: "invalid value"})
					});
					assert.mustThrow(() => {
						new Request("http://a",{ redirect: "invalid value"})
					});
					assert.mustThrow(() => {
						new Request("http://a",{ cache: "invalid value"})
					});
					assert.mustThrow(() => {
						new Request("http://a",{ credentials: "invalid value"})
					});
					assert.mustThrow(() => {
						new Request("http://a",{ duplex: "invalid value"})
					});

					let req = new Request("http://a",{ method: "PUT", body: "some text" });
					assert.seq(await req.text(),"some text");

					req = new Request("http://a",{ method: "PUT", body: JSON.stringify({ a: 1, b: [2], c: { d: 3} })});
					let res = await req.json();
					assert.seq(res.a,1);
					assert(Array.isArray(res.b));
					assert.seq(res.b[0],2);
					assert.seq(typeof res.c,"object");
					assert.seq(res.c.d,3);

					// some methods must be uppercased.
					req = new Request("http://a",{ method: "gEt" });
					assert.seq(req.method,"GET");

					// get requests can't have a body.
					assert.mustThrow(() => {
						new Request("http://a",{ body: "a"})
					})
					// head requests can't have a body.
					assert.mustThrow(() => {
						new Request("http://a",{ method: "HEAD", body: "a"})
					})

					// use body twice
					await assert.mustThrowAsync(async () => {
						let req = new Request("http://a",{ method: "PUT",body: "some text" });
						await req.text();
						await req.text();
					});

					// clone request
					req = new Request("http://a",{ method: "PUT", body: "some text" });
					let req_2 = req.clone()
					assert.seq(await req.text(),"some text");
					assert.seq(await req_2.text(),"some text");

				})()
			"#).catch(&ctx).unwrap().into_future::<()>().await.catch(&ctx).unwrap();
		})
		.await;
	}
}
