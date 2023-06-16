use js::bind;

mod init;
pub use init::ResponseInit;

pub use response::Response as ResponseClass;

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

#[bind(object, public)]
#[quickjs(bare)]
#[allow(non_snake_case)]
#[allow(unused_variables)]
#[allow(clippy::module_inception)]
pub mod response {

	use super::super::{
		super::{
			util::{self, is_redirect_status},
			Body, BodyKind, RequestError,
		},
		BlobClass, HeadersClass,
	};

	use super::{ResponseInit, ResponseType};
	use bytes::Bytes;
	use js::{
		function::{Opt, Rest},
		ArrayBuffer, Class, Ctx, Exception, HasRefs, Persistent, Result, Value,
	};
	use reqwest::{header::HeaderName, Url};

	#[derive(HasRefs)]
	#[allow(dead_code)]
	#[quickjs(has_refs)]
	pub struct Response {
		#[quickjs(has_refs)]
		pub(crate) body: Body,
		#[quickjs(has_refs)]
		pub(crate) init: ResponseInit,
		pub(crate) url: Option<Url>,
		pub(crate) r#type: ResponseType,
		pub(crate) was_redirected: bool,
	}

	impl Response {
		// ------------------------------
		// Constructor
		// ------------------------------

		#[quickjs(constructor)]
		pub fn new(
			ctx: Ctx<'_>,
			body: Opt<Option<Body>>,
			init: Opt<ResponseInit>,
			args: Rest<()>,
		) -> Result<Self> {
			let init = match init.into_inner() {
				Some(x) => x,
				None => ResponseInit::default(ctx)?,
			};
			let body = body.into_inner().and_then(|x| x);
			if body.is_some() && util::is_null_body_status(init.status) {
				// Null body statuses are not allowed to have a body.
				return Err(Exception::throw_type(
					ctx,
					&format!(
						"Response with status `{}` is not allowed to have a body",
						init.status
					),
				));
			}
			let body = body.unwrap_or_default();

			Ok(Response {
				body,
				init,
				url: None,
				r#type: ResponseType::Default,
				was_redirected: false,
			})
		}

		// ------------------------------
		// Instance properties
		// ------------------------------

		#[quickjs(get)]
		pub fn bodyUsed(&self) -> bool {
			self.body.used()
		}

		#[quickjs(get)]
		pub fn status(&self) -> u16 {
			self.init.status
		}

		#[quickjs(get)]
		pub fn ok(&self) -> bool {
			util::is_ok_status(self.init.status)
		}

		#[quickjs(get)]
		pub fn redirected(&self) -> bool {
			self.was_redirected
		}

		#[quickjs(get)]
		pub fn statusText(&self) -> String {
			self.init.status_text.clone()
		}

		#[quickjs(get)]
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

		#[quickjs(get)]
		pub fn headers<'js>(&self, ctx: Ctx<'js>) -> Class<'js, HeadersClass> {
			self.init.headers.clone().restore(ctx).unwrap()
		}

		#[quickjs(get)]
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
		pub fn toString(&self) -> String {
			String::from("[object Response]")
		}

		// Creates a copy of the request object
		#[quickjs(rename = "clone")]
		pub fn clone_js(&self, ctx: Ctx<'_>, args: Rest<()>) -> Response {
			Response {
				body: self.body.clone_js(ctx),
				init: self.init.clone(),
				url: self.url.clone(),
				r#type: self.r#type,
				was_redirected: self.was_redirected,
			}
		}

		#[quickjs(skip)]
		async fn take_buffer<'js>(&self, ctx: Ctx<'js>) -> Result<Bytes> {
			match self.body.to_buffer().await {
				Ok(Some(x)) => Ok(x),
				Ok(None) => Err(Exception::throw_type(ctx, "Body unusable")),
				Err(e) => match &*e {
					RequestError::Reqwest(e) => {
						Err(Exception::throw_type(ctx, &format!("stream failed: {e}")))
					}
					RequestError::Io(e) => {
						Err(Exception::throw_message(ctx, &format!("Io Error: {e}")))
					}
				},
			}
		}

		// Returns a promise with the response body as a Blob
		pub async fn blob<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<BlobClass> {
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

		// Returns a promise with the response body as FormData
		pub async fn formData<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<Value<'js>> {
			Err(Exception::throw_internal(ctx, "Not yet implemented"))
		}

		// Returns a promise with the response body as JSON
		pub async fn json<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<Value<'js>> {
			let text = self.text(ctx, args).await?;
			ctx.json_parse(text)
		}

		// Returns a promise with the response body as text
		pub async fn text<'js>(&self, ctx: Ctx<'js>, args: Rest<()>) -> Result<String> {
			let data = self.take_buffer(ctx).await?;

			Ok(String::from_utf8(data.to_vec())?)
		}

		// Returns a promise with the response body as text
		pub async fn arrayBuffer<'js>(
			&self,
			ctx: Ctx<'js>,
			args: Rest<()>,
		) -> Result<ArrayBuffer<'js>> {
			let data = self.take_buffer(ctx).await?;
			ArrayBuffer::new(ctx, data)
		}

		// ------------------------------
		// Static methods
		// ------------------------------

		#[quickjs(rename = "json")]
		pub fn static_json<'js>(
			ctx: Ctx<'js>,
			data: Value<'js>,
			init: Opt<ResponseInit>,
			args: Rest<()>,
		) -> Result<Self> {
			let json = ctx.json_stringify(data)?;
			let json =
				json.ok_or_else(|| Exception::throw_type(ctx, "Value is not JSON serializable"))?;
			let json = json.to_string()?;

			let init = if let Some(init) = init.into_inner() {
				init
			} else {
				ResponseInit::default(ctx)?
			};

			Ok(Response {
				url: None,
				body: Body::buffer(BodyKind::Buffer, json),
				init,
				r#type: ResponseType::Default,
				was_redirected: false,
			})
		}

		// Returns a new response representing a network error
		pub fn error(ctx: Ctx<'_>, args: Rest<()>) -> Result<Self> {
			let headers = Persistent::save(ctx, Class::instance(ctx, HeadersClass::new_empty())?);
			Ok(Response {
				url: None,
				body: Body::new(),
				init: ResponseInit {
					status: 0,
					status_text: String::new(),
					headers,
				},
				r#type: ResponseType::Error,
				was_redirected: false,
			})
		}

		// Creates a new response with a different URL
		pub fn redirect(ctx: Ctx<'_>, url: String, status: Opt<u32>) -> Result<Response> {
			let url = url
				.parse::<Url>()
				.map_err(|e| Exception::throw_type(ctx, &format!("Invalid url: {e}")))?;

			let status = status.into_inner().unwrap_or(302) as u16;
			if !is_redirect_status(status) {
				return Err(Exception::throw_range(ctx, "Status code is not a redirect status"));
			}

			let headers = Persistent::save(ctx, Class::instance(ctx, HeadersClass::new_empty())?);

			Ok(Response {
				url: Some(url),
				body: Body::new(),
				init: ResponseInit {
					status,
					status_text: String::new(),
					headers,
				},
				r#type: ResponseType::Default,
				was_redirected: false,
			})
		}
	}
}
