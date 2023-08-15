use crate::sql::Bytes;

use super::{ClientBuilder, Error, Request};
use futures::Stream;
use hyper::client::HttpConnector;
use lib_http::{HeaderMap, Uri};
use std::error::Error as StdError;

mod connect;
use connect::Connector;
mod body;
mod response;
pub use response::Response;

pub type BoxError = Box<dyn StdError + Send + Sync>;
pub type BoxStream = Box<dyn Stream<Item = Result<Bytes, BoxError>>>;

pub type BackendError = hyper::Error;
pub type BackendBody = body::Body;

pub struct Backend {
	client: hyper::Client<Connector, BackendBody>,
	default_headers: Option<HeaderMap>,
}

impl Backend {
	pub fn new() -> Self {
		let client = hyper::Client::builder().build(Connector::Http(HttpConnector::new()));

		Self {
			client,
			default_headers: None,
		}
	}

	pub fn build(builder: ClientBuilder) -> Self {
		let connector = Connector::from_tls(builder.tls_config);
		let client = hyper::Client::builder().build(connector);
		Self {
			client,
			default_headers: builder.default_headers,
		}
	}

	pub async fn execute(&self, request: Request) -> Result<Response, Error> {
		let uri = Uri::try_from(request.url.as_ref()).expect("A url should always be a valid uri");
		let mut request_builder = hyper::Request::builder().uri(uri).method(request.method);

		// These unwraps should not ever panic since request already ensures that everything should
		// be valid for the request.
		*request_builder.headers_mut().unwrap() = request.headers;
		let req = request_builder.body(request.body.into_client()).unwrap();

		let response = if let Some(timeout) = request.timeout {
			tokio::time::timeout(timeout, self.client.request(req)).await??
		} else {
			self.client.request(req).await?
		};

		Ok(Response::from_hyper(response))
	}
}
