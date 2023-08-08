#![allow(dead_code)]

#[cfg(not(target_arch = "wasm32"))]
mod hyper;
use http::HeaderMap;
#[cfg(not(target_arch = "wasm32"))]
use hyper::Client as NativeClient;
#[cfg(target_arch = "wasm32")]
mod wasm;
#[cfg(target_arch = "wasm32")]
use wasm::Client as NativeClient;

mod url;
pub use url::{IntoUrl, Url};

use crate::opt::Tls;

pub struct Request {}

pub struct Client {
	inner: NativeClient,
}

impl Client {
	pub fn new() -> Self {
		Self {
			inner: NativeClient::new(),
		}
	}

	pub fn build() -> ClientBuilder {
		ClientBuilder::new()
	}

	pub fn get<U: IntoUrl>(&self, url: U) -> Request {
		todo!()
	}
}

pub struct ClientBuilder {
	default_headers: Option<HeaderMap>,
	tls_config: Option<Tls>,
}

impl Default for ClientBuilder {
	fn default() -> Self {
		Self::new()
	}
}

impl ClientBuilder {
	pub fn new() -> Self {
		ClientBuilder {
			default_headers: None,
			tls_config: None,
		}
	}

	pub fn with_tls(mut self, tls: Tls) -> Self {
		self.tls_config = Some(tls);
		self
	}

	pub fn default_headers(mut self, tls: Tls) -> Self {
		self.tls_config = Some(tls);
		self
	}
}
