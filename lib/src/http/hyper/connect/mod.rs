use hyper::{
	client::{connect::Connection, HttpConnector},
	service::Service,
};
use lib_http::Uri;
use std::{
	error::Error as StdError,
	task::{ready, Poll},
};
use tokio::net::TcpStream;

mod future;
use future::ConnectorFuture;
mod stream;
use stream::Stream;

use crate::opt::Tls;

#[cfg(feature = "rustls")]
pub type RustlsStream = tokio_rustls::client::TlsStream<TcpStream>;
#[cfg(feature = "native-tls")]
pub type NativeStream = hyper_tls::TlsStream<TcpStream>;

#[cfg(feature = "rustls")]
pub type RustlsConnector = hyper_rustls::HttpsConnector<HttpConnector>;
#[cfg(feature = "native-tls")]
pub type NativeConnector = hyper_tls::HttpsConnector<HttpConnector>;

#[derive(Clone)]
pub enum Connector {
	#[cfg(feature = "rustls")]
	Rustls(RustlsConnector),
	#[cfg(feature = "native-tls")]
	Native(NativeConnector),
	Http(hyper::client::HttpConnector),
}

impl Connector {
	pub fn from_tls(tls: Option<Tls>) -> Self {
		match tls {
			#[cfg(feature = "native-tls")]
			Some(Tls::Native(connector)) => {
				let http_connector = HttpConnector::new();
				NativeStream::from((http_connector, connector))
			}
			#[cfg(feature = "rustls")]
			Some(Tls::Rust(config)) => {
				let connector = hyper_rustls::HttpsConnectorBuilder::new()
					.with_tls_config(config)
					.https_or_http()
					.enable_all_versions()
					.build();
				Connector::Rustls(connector)
			}
			None => Connector::Http(HttpConnector::new()),
		}
	}
}

impl Service<Uri> for Connector {
	type Response = Stream;

	type Error = Box<dyn StdError + Send + Sync>;

	type Future = ConnectorFuture;

	fn poll_ready(
		&mut self,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<Result<(), Self::Error>> {
		match self {
			#[cfg(feature = "rustls")]
			Connector::Rustls(conn) => conn.poll_ready(cx),
			#[cfg(feature = "native-tls")]
			Connector::Native(conn) => conn.poll_ready(cx),
			Connector::Http(conn) => conn.poll_ready(cx).map(|e| e.map_err(|e| e.into())),
		}
	}

	fn call(&mut self, req: Uri) -> Self::Future {
		match self {
			#[cfg(feature = "rustls")]
			Connector::Rustls(conn) => ConnectorFuture::Rustls {
				inner: conn.call(req),
			},
			#[cfg(feature = "native-tls")]
			Connector::Native(conn) => ConnectorFuture::Native {
				inner: conn.call(req),
			},
			Connector::Http(conn) => ConnectorFuture::Http {
				inner: conn.call(req),
			},
		}
	}
}
