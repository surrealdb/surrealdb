use reqwest::Method;

#[js::bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod request {

	use super::super::blob::blob::Blob;
	use crate::sql::value::Value;
	use js::function::Rest;
	// TODO: change implementation based on features.
	use reqwest::Url;

	#[derive(Clone)]
	#[quickjs(cloneable)]
	#[allow(dead_code)]
	pub struct Request {
		pub(crate) url: Url,
	}

	impl Request {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new<'js>(
			ctx: js::Ctx<'js>,
			input: js::Value<'js>,
			init: Option<js::Object<'js>>,
			args: Rest<()>,
		) -> js::Result<Self> {
			let url = if let Some(url) = input.as_string() {
				// url string
				let url_str = url.to_string()?;
				let url = Url::parse(&url_str).map_err(|e| {
					// TODO: make type error.
					js::Exception::from_message(ctx, &format!("failed to parse url: {e}"))
						.ok()
						.map(|x| x.throw())
						.unwrap_or(js::Error::Exception)
				})?;
				if !url.username().is_empty() || !url.password().map(str::is_empty).unwrap_or(false)
				{
					// url cannot contain non empty username and passwords
					// TODO: make type error.
					return Err(js::Exception::from_message(ctx, "Url contained credentials.")
						.ok()
						.map(|x| x.throw())
						.unwrap_or(js::Error::Exception));
				}
				url
			} else if let Some(request) =
				input.as_object().and_then(|obj| js::Class::<Self>::try_ref(ctx, obj).ok())
			{
				// existing request object, just return it
				return Ok(request.clone());
			} else {
				return Err(js::Exception::from_message(
					ctx,
					"request `init` paramater must either be a request object or a string",
				)?
				.throw());
			};
			Ok(Self {
				url,
				credentials: None,
				headers: None,
				method: "GET".to_string(),
				mode: None,
				referrer: None,
			})
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

pub enum RequestMode {
	Navigate,
	SameOrigin,
	NoCors,
	Cors,
}

pub enum RequestCredentials {
	Omit,
	SameOrigin,
	Include,
}

pub enum RequestCache {
	Default,
	NoStore,
	Reload,
	NoCache,
	ForceCache,
	OnlyIfCached,
}

pub enum RequestRedirect {
	Follow,
	Error,
	Manual,
}

pub enum RequestDuplex {
	Half,
}

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
	method: Method,
	headers: (),
	body: (),
	referrer: String,
	refferer_policy: ReferrerPolicy,
	request_mode: RequestMode,
	request_credentails: RequestCredentials,
	request_cache: RequestCache,
	request_redirect: RequestRedirect,
	integrity: String,
	keep_alive: bool,
	signal: (),
	duplex: RequestDuplex,
}

fn ascii_equal_ignore_case(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	a.into_iter().zip(b).all(|(a, b)| a.to_ascii_lowercase() == b.to_ascii_lowercase())
}

impl<'js> js::FromJs<'js> for RequestInit {
	fn from_js(ctx: js::Ctx<'js>, value: js::Value<'js>) -> js::Result<Self> {
		let object = js::Object::from_js(ctx, value)?;

		let referrer = object.get::<Option<String>>("referrer").unwrap_or_else(String::new);
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

		todo!()
	}
}
