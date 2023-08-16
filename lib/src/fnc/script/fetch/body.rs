use crate::fnc::script::fetch::{stream::ReadableStream, RequestError};
use crate::http::Body as BackendBody;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use js::{Class, Ctx, Error, FromJs, Result, Type, Value};
use lib_http::{header, HeaderMap};
use mime::Mime;
use std::error::Error as StdError;
use std::result::Result as StdResult;
use std::sync::Arc;

use super::{classes::Blob, util};

/// A struct representing the body mixin.
///
/// Implements [`FromJs`] for conversion from `Blob`, `ArrayBuffer`, any `TypedBuffer` and `String`.
pub struct Body(BackendBody);

/// The type from which a body was created.
pub enum BodyKind {
	String,
	Bytes,
	Blob(Mime),
	FormData,
}

pub struct BodyAndKind {
	pub body: Body,
	pub kind: BodyKind,
}

impl BodyAndKind {
	/// Applies the content type to the headers and returns the body.
	pub fn apply_to_headers(self, headers: &mut HeaderMap) -> Body {
		match self.kind {
			BodyKind::String => {
				headers.entry(header::CONTENT_TYPE).or_insert_with(|| {
					let mime = mime::TEXT_PLAIN_UTF_8.to_string();
					// TEXT_PLAIN should be a valid header value.
					mime.parse().unwrap()
				});
			}
			BodyKind::Bytes => {}
			BodyKind::Blob(mime) => {
				// Mime's should always a valid header value.
				headers
					.entry(header::CONTENT_TYPE)
					.or_insert_with(|| mime.to_string().parse().unwrap());
			}
			BodyKind::FormData => todo!(),
		}
		self.body
	}
}

impl Default for BodyAndKind {
	fn default() -> Self {
		Self {
			body: Body::empty(),
			kind: BodyKind::Bytes,
		}
	}
}

impl Default for Body {
	fn default() -> Self {
		Body::empty()
	}
}

impl Body {
	/// Create a new used body.
	pub fn used() -> Self {
		Body(BackendBody::used())
	}

	/// Create a new used body.
	pub fn empty() -> Self {
		Body(BackendBody::empty())
	}

	/// Returns wther the body is alread used.
	pub fn is_used(&self) -> bool {
		self.0.is_used()
	}

	/// Create a body from a stream.
	pub fn stream<S, O, E>(stream: S) -> Self
	where
		S: Stream<Item = StdResult<O, E>> + Send + Sync + 'static,
		Bytes: From<O>,
		E: StdError + Send + Sync + 'static,
	{
		Body(BackendBody::wrap_stream(stream))
	}

	/// Returns the data from the body as a buffer.
	///
	/// if the body is a stream this future only returns when the full body is consumed.
	pub async fn to_buffer(self) -> StdResult<Option<Bytes>, String> {
		self.0.to_buffer().await.transpose().map_err(|e| e.to_string())
	}

	/// Clones the body teeing any possible underlying streems
	pub fn clone_js(&mut self, ctx: &Ctx<'_>) -> Self {
		let (res, future) = self.0.tee();
		if let Some(future) = future {
			ctx.spawn(future);
		}
		Body(res)
	}

	pub fn into_backend_body(self) -> BackendBody {
		self.0
	}
}

impl<B> From<B> for Body
where
	BackendBody: From<B>,
{
	fn from(value: B) -> Self {
		Body(BackendBody::from(value))
	}
}

impl<'js> FromJs<'js> for BodyAndKind {
	fn from_js(ctx: &Ctx<'js>, value: Value<'js>) -> Result<Self> {
		let object = match value.type_of() {
			Type::String => {
				let string = value.as_string().unwrap().to_string()?;
				let body = Body::from(string);
				return Ok(BodyAndKind {
					body,
					kind: BodyKind::String,
				});
			}
			Type::Object => value.as_object().unwrap(),
			x => {
				return Err(Error::FromJs {
					from: x.as_str(),
					to: "Blob, TypedArray, FormData, URLSearchParams, or String",
					message: None,
				})
			}
		};
		if let Some(x) = Class::<Blob>::from_object(object.clone()) {
			let borrow = x.borrow();
			let body = Body::from(borrow.data.clone());
			return Ok(BodyAndKind {
				body,
				// for now
				kind: BodyKind::Blob(mime::STAR_STAR),
			});
		}

		if let Some(bytes) = util::buffer_source_to_bytes(&object)? {
			let bytes = Bytes::copy_from_slice(bytes);
			let body = Body::from(bytes);
			return Ok(BodyAndKind {
				body,
				kind: BodyKind::Bytes,
			});
		}

		Err(Error::FromJs {
			from: "object",
			to: "Blob, TypedArray, FormData, URLSearchParams, or String",
			message: None,
		})
	}
}
