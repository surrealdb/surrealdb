use js::{class::RefsMarker, Class, Persistent};
use reqwest::Method;

use crate::fnc::script::classes::headers::headers::Headers;

#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod request {

	use super::super::blob::blob::Blob;
	use super::RequestInit;
	use crate::sql::value::Value;
	use js::function::{Opt, Rest};
	// TODO: change implementation based on features.
	use reqwest::Url;

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Request {
		pub(crate) url: Url,
		pub(crate) init: RequestInit,
	}

	impl Request {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new<'js>(
			ctx: js::Ctx<'js>,
			input: js::Value<'js>,
			init: Opt<RequestInit>,
			args: Rest<()>,
		) -> js::Result<Self> {
			if let Some(url) = input.as_string() {
				// url string
				let url_str = url.to_string()?;
				let url = Url::parse(&url_str).map_err(|e| {
					// TODO: make type error.
					js::Exception::from_message(ctx, &format!("failed to parse url: {e}"))
						.ok()
						.map(|x| x.throw())
						.unwrap_or(js::Error::Exception)
				})?;
				if !url.username().is_empty() || !url.password().map(str::is_empty).unwrap_or(true)
				{
					// url cannot contain non empty username and passwords
					// TODO: make type error.
					return Err(js::Exception::from_message(ctx, "Url contained credentials.")
						.ok()
						.map(|x| x.throw())
						.unwrap_or(js::Error::Exception));
				}
				Ok(Self {
					url,
					init: init.into_inner().unwrap_or_default(),
				})
			} else if let Some(request) =
				input.as_object().and_then(|obj| js::Class::<Self>::try_ref(ctx, obj).ok())
			{
				// existing request object, just return it
				Ok(request.clone())
			} else {
				Err(js::Exception::from_message(
					ctx,
					"request `init` paramater must either be a request object or a string",
				)?
				.throw())
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
		pub async fn blob(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Blob> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the request body as FormData
		pub async fn formData(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the request body as JSON
		pub async fn json(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}

		// Returns a promise with the request body as text
		pub async fn text(self, ctx: js::Ctx<'_>, args: Rest<Value>) -> js::Result<Value> {
			Err(throw!(ctx, "Not yet implemented"))
		}
	}
}

#[derive(Clone)]
pub enum RequestDestination {
	Empty,
	Audio,
	Audioworklet,
	Document,
	Embed,
	Font,
	Frame,
	Iframe,
	Image,
	Manifest,
	Object,
	Paintworklet,
	Report,
	Script,
	Sharedworker,
	Style,
	Track,
	Video,
	Worker,
	Xslt,
}

#[derive(Clone)]
pub enum RequestMode {
	Navigate,
	SameOrigin,
	NoCors,
	Cors,
}

#[derive(Clone)]
pub enum RequestCredentials {
	Omit,
	SameOrigin,
	Include,
}

#[derive(Clone)]
pub enum RequestCache {
	Default,
	NoStore,
	Reload,
	NoCache,
	ForceCache,
	OnlyIfCached,
}

#[derive(Clone)]
pub enum RequestRedirect {
	Follow,
	Error,
	Manual,
}

#[derive(Clone)]
pub enum RequestDuplex {
	Half,
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct RequestInit {
	pub method: Method,
	pub headers: js::Persistent<Class<'static, Headers>>,
	pub body: (),
	pub referrer: String,
	pub refferer_policy: ReferrerPolicy,
	pub request_mode: RequestMode,
	pub request_credentails: RequestCredentials,
	pub request_cache: RequestCache,
	pub request_redirect: RequestRedirect,
	pub integrity: String,
	pub keep_alive: bool,
	signal: (),
	pub duplex: RequestDuplex,
}

impl<'js> js::class::HasRefs for RequestInit {
	fn mark_refs(&self, marker: &RefsMarker) {
		self.headers.mark_refs(marker);
	}
}

fn ascii_equal_ignore_case(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	a.into_iter().zip(b).all(|(a, b)| a.to_ascii_lowercase() == b.to_ascii_lowercase())
}

impl Default for RequestInit {
	fn default() -> Self {
		todo!()
	}
}

impl<'js> js::FromJs<'js> for RequestInit {
	fn from_js(ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		let object = js::Object::from_js(ctx, value)?;

		let referrer = object.get::<_, Option<String>>("referrer")?.unwrap_or_default();
		let method = object
			.get::<_, Option<String>>("method")?
			.map(|m| -> js::Result<Method> {
				if ascii_equal_ignore_case(m.as_bytes(), b"CONNECT")
					|| ascii_equal_ignore_case(m.as_bytes(), b"TRACE")
					|| ascii_equal_ignore_case(m.as_bytes(), b"TRACK")
				{
					//methods that are not allowed [`https://fetch.spec.whatwg.org/#methods`]

					return Err(js::Exception::from_message(
						ctx,
						&format!("method {m} is forbidden"),
					)?
					.throw());
				}

				if ascii_equal_ignore_case(m.as_bytes(), b"DELETE")
					|| ascii_equal_ignore_case(m.as_bytes(), b"GET")
					|| ascii_equal_ignore_case(m.as_bytes(), b"HEAD")
					|| ascii_equal_ignore_case(m.as_bytes(), b"OPTIONS")
					|| ascii_equal_ignore_case(m.as_bytes(), b"POST")
					|| ascii_equal_ignore_case(m.as_bytes(), b"PUT")
				{
					// these methods must be normalized to uppercase.
					m.to_ascii_uppercase();
				}

				match Method::from_bytes(m.as_bytes()) {
					Ok(x) => Ok(x),
					Err(e) => Err(js::Exception::from_message(ctx, "invalid method: {e}")?.throw()),
				}
			})
			.transpose()?
			.unwrap_or(Method::GET);

		let headers = if let Some(hdrs) = object.get::<_, Option<js::Object>>("headers")? {
			if let Ok(cls) = js::Class::<Headers>::from_object(hdrs.clone()) {
				cls
			} else {
				Class::instance(ctx, Headers::new_inner(ctx, hdrs.into_value())?)?
			}
		} else {
			Class::instance(ctx, Headers::new_empty())?
		};
		let headers = Persistent::save(ctx, headers);

		Ok(Self {
			method,
			headers,
			body: (),
			referrer,
			refferer_policy: ReferrerPolicy::Empty,
			request_mode: RequestMode::Cors,
			request_credentails: RequestCredentials::Include,
			request_cache: RequestCache::Default,
			request_redirect: RequestRedirect::Follow,
			integrity: String::new(),
			keep_alive: false,
			signal: (),
			duplex: RequestDuplex::Half,
		})
	}
}
