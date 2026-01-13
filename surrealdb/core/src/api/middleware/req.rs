use anyhow::Result;
use headers::{ContentType, HeaderMapExt};
use mime::{APPLICATION_JSON, APPLICATION_OCTET_STREAM, Mime, TEXT_PLAIN};
use surrealdb_types::Value;

use super::common::{APPLICATION_CBOR, APPLICATION_SDB_FB};
use crate::api::err::ApiError;
use crate::api::middleware::common::{APPLICATION_SDB_NATIVE, BodyStrategy};
use crate::api::request::ApiRequest;
use crate::kvs::IntoBytes;
use crate::rpc::format;

pub async fn body(req: &mut ApiRequest, strategy: BodyStrategy) -> Result<()> {
	let mut parser = BodyParser::from((req, strategy));
	parser.process().await
}

pub struct BodyParser<'a> {
	mime: Option<Mime>,
	req: &'a mut ApiRequest,
	strategy: BodyStrategy,
}

impl<'a> From<(&'a mut ApiRequest, BodyStrategy)> for BodyParser<'a> {
	fn from((req, strategy): (&'a mut ApiRequest, BodyStrategy)) -> Self {
		let mime = req.headers.typed_get::<ContentType>().map(Mime::from);
		Self {
			mime,
			req,
			strategy,
		}
	}
}

impl<'a> BodyParser<'a> {
	pub async fn process(&mut self) -> Result<()> {
		match self.strategy {
			BodyStrategy::Json => self.json(true),
			BodyStrategy::Cbor => self.cbor(true),
			BodyStrategy::Flatbuffers => self.flatbuffers(true),
			BodyStrategy::Plain => self.plain(true),
			BodyStrategy::Bytes => self.bytes(true),
			BodyStrategy::Native => self.native(true),
			BodyStrategy::Auto => {
				let Some(mime) = &self.mime else {
					return Err(ApiError::MissingContentType.into());
				};

				if mime == &APPLICATION_JSON {
					return self.json(false);
				}

				if mime == &*APPLICATION_CBOR {
					return self.cbor(false);
				}

				if mime == &*APPLICATION_SDB_FB {
					return self.flatbuffers(false);
				}

				if mime == &TEXT_PLAIN {
					return self.plain(false);
				}

				if mime == &APPLICATION_OCTET_STREAM {
					return self.bytes(false);
				}

				if mime == &*APPLICATION_SDB_NATIVE {
					return self.native(false);
				}

				Err(ApiError::UnsupportedContentType(mime.to_string()).into())
			}
		}
	}

	fn is_mime(&self, mime: &Mime) -> bool {
		self.mime.as_ref().map(|x| x == mime).unwrap_or(false)
	}

	fn assert_mime(&self, mime: &Mime) -> Result<()> {
		if !self.is_mime(mime) {
			Err(ApiError::InvalidContentType(mime.to_string()).into())
		} else {
			Ok(())
		}
	}

	fn json(&mut self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&APPLICATION_JSON)?;
		}

		let Value::Bytes(ref bytes) = self.req.body else {
			return Err(ApiError::RequestBodyNotBinary.into());
		};

		self.req.body =
			format::json::decode(bytes.as_slice()).map_err(|_| ApiError::BodyDecodeFailure)?;

		Ok(())
	}

	fn cbor(&mut self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&APPLICATION_CBOR)?;
		}

		let Value::Bytes(ref bytes) = self.req.body else {
			return Err(ApiError::RequestBodyNotBinary.into());
		};

		self.req.body =
			format::cbor::decode(bytes.as_slice()).map_err(|_| ApiError::BodyDecodeFailure)?;

		Ok(())
	}

	fn flatbuffers(&mut self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&APPLICATION_SDB_FB)?;
		}

		let Value::Bytes(ref bytes) = self.req.body else {
			return Err(ApiError::RequestBodyNotBinary.into());
		};

		self.req.body = format::flatbuffers::decode(bytes.as_slice())
			.map_err(|_| ApiError::BodyDecodeFailure)?;

		Ok(())
	}

	fn plain(&mut self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&TEXT_PLAIN)?;
		}

		let Value::Bytes(ref bytes) = self.req.body else {
			return Err(ApiError::RequestBodyNotBinary.into());
		};

		self.req.body = Value::String(String::from_utf8_lossy(bytes.as_slice()).to_string());

		Ok(())
	}

	fn bytes(&mut self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&APPLICATION_OCTET_STREAM)?;
		}

		if !self.req.body.is_bytes() {
			return Err(ApiError::RequestBodyNotBinary.into());
		}

		Ok(())
	}

	fn native(&self, validate: bool) -> Result<()> {
		if validate {
			self.assert_mime(&APPLICATION_SDB_NATIVE)?;
		}

		Ok(())
	}
}
