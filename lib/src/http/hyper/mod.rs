use super::{ClientBuilder, Error, Request};
use bytes::Bytes;
use futures::Stream;
use hyper::client::HttpConnector;
use lib_http::{HeaderMap, Uri};
use std::{error::Error as StdError, pin::Pin, sync::Arc};

mod connect;
use connect::Connector;
mod body;
mod once_option;
mod response;
pub use response::Response;

pub type BoxStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxError>> + Send + Sync>>;
pub type BoxError = Arc<dyn StdError + Send + Sync>;

pub type BackendError = hyper::Error;
pub type BackendBody = body::Body;

#[derive(Clone, Debug)]
pub struct Backend(Arc<InnerBackend>);

#[derive(Debug)]
struct InnerBackend {
	client: hyper::Client<Connector, BackendBody>,
	default_headers: Option<HeaderMap>,
}

impl Backend {
	/// Create a new backend with default options.
	pub fn new() -> Self {
		let client = hyper::Client::builder().build(Connector::Http(HttpConnector::new()));

		Self(Arc::new(InnerBackend {
			client,
			default_headers: None,
		}))
	}

	/// Create a new backend for a client builder.
	pub fn build(builder: ClientBuilder) -> Self {
		let connector = Connector::from_tls(builder.tls_config);
		let client = hyper::Client::builder().build(connector);
		Self(Arc::new(InnerBackend {
			client,
			default_headers: builder.default_headers,
		}))
	}

	/// Execute a request.
	pub async fn execute(&self, request: Request) -> Result<Response, Error> {
		let uri = Uri::try_from(request.url.as_ref()).expect("A url should always be a valid uri");
		let mut request_builder = hyper::Request::builder().uri(uri).method(request.method);

		// These unwraps should not ever panic since request already ensures that everything should
		// be valid for the request.
		*request_builder.headers_mut().unwrap() = request.headers;
		let req = request_builder.body(request.body).unwrap();

		let response = if let Some(timeout) = request.timeout {
			tokio::time::timeout(timeout, self.0.client.request(req)).await??
		} else {
			self.0.client.request(req).await?
		};

		Ok(Response::from_hyper(response))
	}
}
