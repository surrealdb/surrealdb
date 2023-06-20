//! Request class implementation
//!
use js::{
	bind,
	class::{HasRefs, RefsMarker},
	Class, Ctx, Exception, FromJs, Object, Persistent, Result, Value,
};
use reqwest::Method;

use crate::fnc::script::fetch::{
	body::Body,
	classes::{BlobClass, HeadersClass},
	util::ascii_equal_ignore_case,
	RequestError,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestMode {
	Navigate,
	SameOrigin,
	NoCors,
	Cors,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestCredentials {
	Omit,
	SameOrigin,
	Include,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestCache {
	Default,
	NoStore,
	Reload,
	NoCache,
	ForceCache,
	OnlyIfCached,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestRedirect {
	Follow,
	Error,
	Manual,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RequestDuplex {
	Half,
}

#[derive(Clone, Copy, Eq, PartialEq)]
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

pub struct RequestInit {
	pub method: Method,
	pub headers: Persistent<Class<'static, HeadersClass>>,
	pub body: Option<Body>,
	pub referrer: String,
	pub referer_policy: ReferrerPolicy,
	pub request_mode: RequestMode,
	pub request_credentails: RequestCredentials,
	pub request_cache: RequestCache,
	pub request_redirect: RequestRedirect,
	pub integrity: String,
	pub keep_alive: bool,
	pub duplex: RequestDuplex,
}

impl HasRefs for RequestInit {
	fn mark_refs(&self, marker: &RefsMarker) {
		self.headers.mark_refs(marker);
	}
}

impl RequestInit {
	pub fn default(ctx: Ctx<'_>) -> Result<Self> {
		let headers = Persistent::save(ctx, Class::instance(ctx, HeadersClass::new_empty())?);
		Ok(RequestInit {
			method: Method::GET,
			headers,
			body: None,
			referrer: "client".to_string(),
			referer_policy: ReferrerPolicy::Empty,
			request_mode: RequestMode::Cors,
			request_credentails: RequestCredentials::SameOrigin,
			request_cache: RequestCache::Default,
			request_redirect: RequestRedirect::Follow,
			integrity: String::new(),
			keep_alive: false,
			duplex: RequestDuplex::Half,
		})
	}

	pub fn clone_js(&self, ctx: Ctx<'_>) -> Result<Self> {
		let headers = self.headers.clone().restore(ctx).unwrap();
		let headers = Persistent::save(ctx, Class::instance(ctx, headers.borrow().clone())?);

		let body = self.body.as_ref().map(|x| x.clone_js(ctx));

		Ok(RequestInit {
			method: self.method.clone(),
			headers,
			body,
			referrer: self.referrer.clone(),
			referer_policy: self.referer_policy,
			request_mode: self.request_mode,
			request_credentails: self.request_credentails,
			request_cache: self.request_cache,
			request_redirect: self.request_redirect,
			integrity: self.integrity.clone(),
			keep_alive: self.keep_alive,
			duplex: self.duplex,
		})
	}
}

// Normalize method string according to spec.
fn normalize_method(ctx: Ctx<'_>, m: String) -> Result<Method> {
	if ascii_equal_ignore_case(m.as_bytes(), b"CONNECT")
		|| ascii_equal_ignore_case(m.as_bytes(), b"TRACE")
		|| ascii_equal_ignore_case(m.as_bytes(), b"TRACK")
	{
		//methods that are not allowed [`https://fetch.spec.whatwg.org/#methods`]
		return Err(Exception::throw_type(ctx, &format!("method {m} is forbidden")));
	}

	// The following methods must be uppercased to the default case insensitive equivalent.
	if ascii_equal_ignore_case(m.as_bytes(), b"DELETE") {
		return Ok(Method::DELETE);
	}
	if ascii_equal_ignore_case(m.as_bytes(), b"GET") {
		return Ok(Method::GET);
	}
	if ascii_equal_ignore_case(m.as_bytes(), b"HEAD") {
		return Ok(Method::HEAD);
	}
	if ascii_equal_ignore_case(m.as_bytes(), b"OPTIONS") {
		return Ok(Method::OPTIONS);
	}
	if ascii_equal_ignore_case(m.as_bytes(), b"POST") {
		return Ok(Method::POST);
	}
	if ascii_equal_ignore_case(m.as_bytes(), b"PUT") {
		return Ok(Method::PUT);
	}

	match Method::from_bytes(m.as_bytes()) {
		Ok(x) => Ok(x),
		Err(e) => Err(Exception::throw_type(ctx, &format!("invalid method: {e}"))),
	}
}

impl<'js> FromJs<'js> for RequestInit {
	fn from_js(ctx: Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = Object::from_js(ctx, value)?;

		let referrer =
			object.get::<_, Option<String>>("referrer")?.unwrap_or_else(|| "client".to_owned());
		let method = object
			.get::<_, Option<String>>("method")?
			.map(|m| normalize_method(ctx, m))
			.transpose()?
			.unwrap_or(Method::GET);

		let headers = if let Some(hdrs) = object.get::<_, Option<Object>>("headers")? {
			if let Ok(cls) = Class::<HeadersClass>::from_object(hdrs.clone()) {
				cls
			} else {
				Class::instance(ctx, HeadersClass::new_inner(ctx, hdrs.into_value())?)?
			}
		} else {
			Class::instance(ctx, HeadersClass::new_empty())?
		};
		let headers = Persistent::save(ctx, headers);

		let body = object.get::<_, Option<Body>>("body")?;

		Ok(Self {
			method,
			headers,
			body,
			referrer,
			referer_policy: ReferrerPolicy::Empty,
			request_mode: RequestMode::Cors,
			request_credentails: RequestCredentials::SameOrigin,
			request_cache: RequestCache::Default,
			request_redirect: RequestRedirect::Follow,
			integrity: String::new(),
			keep_alive: false,
			duplex: RequestDuplex::Half,
		})
	}
}

pub use request::Request as RequestClass;

#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
mod request {

	pub use super::*;

	use bytes::Bytes;
	use js::{
		function::{Opt, Rest},
		Class, Ctx, Exception, HasRefs, Result, Value,
	};
	// TODO: change implementation based on features.
	use reqwest::{header::HeaderName, Url};

	#[allow(dead_code)]
	#[derive(HasRefs)]
	#[quickjs(has_refs)]
	pub struct Request {
		pub(crate) url: Url,
		#[quickjs(has_refs)]
		pub(crate) init: RequestInit,
	}

	impl Request {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new<'js>(
			ctx: Ctx<'js>,
			input: Value<'js>,
			init: Opt<RequestInit>,
			args: Rest<()>,
		) -> Result<Self> {
			if let Some(url) = input.as_string() {
				// url string
				let url_str = url.to_string()?;
				let url = Url::parse(&url_str).map_err(|e| {
					Exception::throw_type(ctx, &format!("failed to parse url: {e}"))
				})?;
				if !url.username().is_empty() || !url.password().map(str::is_empty).unwrap_or(true)
				{
					// url cannot contain non empty username and passwords
					return Err(Exception::throw_type(ctx, "Url contained credentials."));
				}
				let init = init.into_inner().map_or_else(|| RequestInit::default(ctx), Ok)?;
				// HEAD and GET methods can't have a body
				if init.body.is_some() && init.method == Method::GET || init.method == Method::HEAD
				{
					return Err(Exception::throw_type(
						ctx,
						&format!("Request with method `{}` cannot have a body", init.method),
					));
				}

				Ok(Self {
					url,
					init,
				})
			} else if let Some(request) = input
				.into_object()
				.and_then(|obj| Class::<Self>::from_object(obj).ok().map(|x| x.borrow()))
			{
				// existing request object, just return it
				request.clone_js(ctx, Default::default())
			} else {
				Err(Exception::throw_type(
					ctx,
					"request `init` paramater must either be a request object or a string",
				))
			}
		}

		/// Clone the response, teeing any possible underlying streams.
		#[quickjs(rename = "clone")]
		pub fn clone_js(&self, ctx: Ctx<'_>, _rest: Rest<()>) -> Result<Self> {
			Ok(Self {
				url: self.url.clone(),
				init: self.init.clone_js(ctx)?,
			})
		}

		// ------------------------------
		// Instance properties
		// ------------------------------
		#[quickjs(get)]
		pub fn bodyUsed(&self) -> bool {
			self.init.body.as_ref().map(Body::used).unwrap_or(true)
		}

		#[quickjs(get)]
		pub fn method(&self) -> String {
			self.init.method.to_string()
		}

		#[quickjs(get)]
		pub fn url(&self) -> String {
			self.url.to_string()
		}

		#[quickjs(get)]
		pub fn headers<'js>(&self, ctx: Ctx<'js>) -> Class<'js, HeadersClass> {
			self.init.headers.clone().restore(ctx).unwrap()
		}

		#[quickjs(get)]
		pub fn referrer(&self, ctx: Ctx<'_>) -> String {
			self.init.referrer.clone()
		}
		// TODO

		// ------------------------------
		// Instance methods
		// ------------------------------

		// Convert the object to a string
		pub fn toString(&self) -> String {
			String::from("[object Request]")
		}

		/// Takes the buffer from the body leaving it used.
		#[quickjs(skip)]
		async fn take_buffer<'js>(&self, ctx: Ctx<'js>) -> Result<Bytes> {
			let Some(body) = self.init.body.as_ref() else {
				return Ok(Bytes::new())
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
		pub async fn blob(&self, ctx: Ctx<'_>, args: Rest<()>) -> Result<BlobClass> {
			let headers = self.init.headers.clone().restore(ctx).unwrap();
			let mime = {
				let headers = headers.borrow();
				let headers = headers.inner.borrow();
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

			let data = self.take_buffer(ctx).await?;
			Ok(BlobClass {
				mime,
				data,
			})
		}

		// Returns a promise with the request body as FormData
		pub async fn formData<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<Value<'js>> {
			Err(Exception::throw_internal(ctx, "Not yet implemented"))
		}

		// Returns a promise with the request body as JSON
		pub async fn json<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<Value<'js>> {
			let text = self.text(ctx, args).await?;
			ctx.json_parse(text)
		}

		// Returns a promise with the request body as text
		pub async fn text<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<String> {
			let data = self.take_buffer(ctx).await?;

			Ok(String::from_utf8(data.to_vec())?)
		}
	}
}

#[cfg(test)]
mod test {
	fn method() {}
}
