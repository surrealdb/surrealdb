#![allow(dead_code)]

use std::{convert::Infallible, str::Utf8Error, sync::Arc};

use lib_http::{
	header::{InvalidHeaderName, InvalidHeaderValue},
	method::InvalidMethod,
	Method, StatusCode,
};
use thiserror::Error;

#[cfg(not(target_arch = "wasm32"))]
mod hyper;
#[cfg(not(target_arch = "wasm32"))]
use hyper::{Client as NativeClient, Error as ClientError};
#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
use wasm::{Client as NativeClient, Error as ClientError};

mod url;
pub use url::{IntoUrl, Url};
mod builder;
pub use builder::{ClientBuilder, RedirectAction, RedirectPolicy};
mod request;
pub use request::Request;
mod response;
pub use response::Response;
mod body;
pub use body::Body;

#[derive(Error, Debug)]
pub enum Error {
	#[error("{0}")]
	Url(#[from] url::UrlParseError),
	#[error("{0}")]
	Client(#[from] ClientError),
	#[error("Failed to parse bytes to string: {0}")]
	Utf8(#[from] Utf8Error),
	#[error(
		"Invalid authorization token, authorization token could not be used as a header value"
	)]
	InvalidToken,
	#[error("Request returned error statuscode: {0}")]
	StatusCode(StatusCode),
	#[error("{0}")]
	InvalidHeaderName(#[from] InvalidHeaderName),
	#[error("{0}")]
	InvalidHeaderValue(#[from] InvalidHeaderValue),
	#[error("{0}")]
	InvalidMethod(#[from] InvalidMethod),
}

impl From<Infallible> for Error {
	fn from(value: Infallible) -> Self {
		panic!("Infallible error was created")
	}
}

#[derive(Clone)]
pub struct Client {
	inner: Arc<NativeClient>,
}

impl Client {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(NativeClient::new()),
		}
	}

	pub fn builder() -> ClientBuilder {
		ClientBuilder::new()
	}

	pub fn get<U: IntoUrl>(&self, url: U) -> Result<Request, Error> {
		let url = url.into_url()?;
		Request::new(Method::GET, url, self.clone())
	}

	pub fn post<U: IntoUrl>(&self, url: U) -> Result<Request, Error> {
		let url = url.into_url()?;
		Request::new(Method::POST, url, self.clone())
	}

	pub fn head<U: IntoUrl>(&self, url: U) -> Result<Request, Error> {
		let url = url.into_url()?;
		Request::new(Method::HEAD, url, self.clone())
	}

	pub fn put<U: IntoUrl>(&self, url: U) -> Result<Request, Error> {
		let url = url.into_url()?;
		Request::new(Method::PUT, url, self.clone())
	}
}
