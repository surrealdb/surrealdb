//! TLS for `grpcs://` with a caller-supplied `rustls` configuration.
//!
//! tonic builds its own client configuration from a `ClientTlsConfig` and has
//! no way to adopt a `rustls::ClientConfig`, so a private certificate
//! authority, a client certificate or a custom verifier the caller configured
//! cannot be handed to it. Instead the channel is given a connector that opens
//! the TCP connection and performs the TLS handshake with the caller's
//! configuration itself, and tonic speaks HTTP/2 over the stream it returns.
//! The configuration is used as given, except that ALPN is set to `h2`, which
//! gRPC requires.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tonic::codegen::Service;
use tonic::codegen::http::Uri;

/// Connects to the host and port of the URI it is called with, and performs
/// the TLS handshake for `server_name` with the configuration it holds.
#[derive(Clone)]
pub(super) struct RustlsConnector {
	connector: TlsConnector,
	server_name: ServerName<'static>,
}

impl RustlsConnector {
	/// A connector presenting `server_name` for SNI and certificate
	/// verification.
	pub(super) fn new(mut config: rustls::ClientConfig, server_name: ServerName<'static>) -> Self {
		config.alpn_protocols = vec![b"h2".to_vec()];
		Self {
			connector: TlsConnector::from(Arc::new(config)),
			server_name,
		}
	}
}

impl Service<Uri> for RustlsConnector {
	type Response = TokioIo<TlsStream<TcpStream>>;
	type Error = std::io::Error;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, uri: Uri) -> Self::Future {
		let connector = self.connector.clone();
		let server_name = self.server_name.clone();
		Box::pin(async move {
			let host = uri.host().ok_or_else(|| {
				std::io::Error::new(std::io::ErrorKind::InvalidInput, "the URI has no host")
			})?;
			// A bracketed IPv6 literal is how a URI writes one; a socket
			// address takes it bare.
			let host = host.trim_start_matches('[').trim_end_matches(']');
			let port = uri.port_u16().unwrap_or(443);
			let tcp = TcpStream::connect((host, port)).await?;
			tcp.set_nodelay(true)?;
			let tls = connector.connect(server_name, tcp).await?;
			Ok(TokioIo::new(tls))
		})
	}
}
