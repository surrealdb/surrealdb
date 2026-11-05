//! Outbound `wasi:http` for a guest, held to the network policy it was granted.
//!
//! The request sender that ships with `wasmtime-wasi-http` opens its own
//! sockets and resolves names itself, so it would reach past both the
//! `allow_net` grant and the operator's `deny_net` rules that every other way
//! out of the sandbox answers to. This sender asks the same [`NetPolicy`] the
//! DNS shim and the guest's own sockets ask, in the same order: the name must
//! be allowed, what it resolves to is filtered and recorded against the rules
//! that vended it, and the connection goes only to an address the policy then
//! admits. It connects to that address rather than to the name again, so the
//! answer that was vetted is the answer that is used.
//!
//! A module that was granted no network reaches nothing through here: the name
//! check refuses every host.

use std::net::{IpAddr, SocketAddr};
use std::pin::{Pin, pin};
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll, ready};
use std::time::Duration;

use bytes::Bytes;
use http::uri::Scheme;
use http::{Request, Response};
use http_body::Body;
use http_body_util::BodyExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use wasmtime_wasi_http::{Error, RequestOptions, WasiBody, WasiHttpHooks};

use crate::net_allow::NetPolicy;

/// How long a guest waits when it names no timeout of its own.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The longest wait a guest may ask for, per phase.
const MAX_TIMEOUT: Duration = Duration::from_secs(600);

/// The `wasi:http` hooks of one invocation: its network policy.
pub(crate) struct GuestHttp {
	net: Arc<NetPolicy>,
}

impl GuestHttp {
	pub(crate) fn new(net: Arc<NetPolicy>) -> Self {
		Self {
			net,
		}
	}
}

type IoFuture = Box<dyn Future<Output = Result<(), Error>> + Send>;

impl WasiHttpHooks for GuestHttp {
	fn send_request(
		&mut self,
		request: Request<WasiBody>,
		options: Option<RequestOptions>,
		fut: IoFuture,
	) -> Box<dyn Future<Output = Result<(Response<WasiBody>, IoFuture), Error>> + Send> {
		// The request body's own completion is reported through the body.
		drop(fut);
		let net = Arc::clone(&self.net);
		Box::new(async move {
			let (response, io) = send(net, request, options).await?;
			Ok((response.map(BodyExt::boxed_unsync), Box::new(io) as IoFuture))
		})
	}
}

trait Stream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}
impl<T> Stream for T where T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static {}

fn timeout(requested: Option<Duration>) -> Duration {
	requested.map_or(DEFAULT_TIMEOUT, |t| t.min(MAX_TIMEOUT))
}

/// The addresses a connection to `host:port` may use, in resolver order.
///
/// Empty when the policy refuses the name or every address it resolved to.
async fn admitted_addrs(net: &NetPolicy, host: &str, port: u16) -> Result<Vec<SocketAddr>, Error> {
	if !net.allows_name(host) {
		tracing::debug!(target: "surrealism::http", host, "refused: not in allow_net");
		return Err(Error::HttpRequestDenied);
	}
	let resolved: Vec<IpAddr> = match host.parse::<IpAddr>() {
		Ok(ip) => vec![ip],
		Err(_) => tokio::net::lookup_host((host, port))
			.await
			.map_err(|_| Error::DnsError {
				rcode: Some("name-error".to_string()),
				info_code: None,
			})?
			.map(|a| a.ip().to_canonical())
			.collect(),
	};
	if resolved.is_empty() {
		return Err(Error::DestinationNotFound);
	}
	let admitted: Vec<SocketAddr> = net
		.accept_resolved(host, resolved)
		.into_iter()
		.map(|ip| SocketAddr::new(ip, port))
		.filter(|addr| net.allows_socket_addr(addr))
		.collect();
	if admitted.is_empty() {
		tracing::debug!(
			target: "surrealism::http",
			host,
			"refused: every resolved address is special-use or denied"
		);
		return Err(Error::HttpRequestDenied);
	}
	Ok(admitted)
}

async fn connect(addrs: &[SocketAddr]) -> Result<TcpStream, Error> {
	let mut last = None;
	for addr in addrs {
		match TcpStream::connect(addr).await {
			Ok(stream) => return Ok(stream),
			Err(e) => last = Some(e),
		}
	}
	Err(last.map_or(Error::DestinationUnavailable, Error::Connect))
}

fn tls_config() -> Arc<rustls::ClientConfig> {
	static CONFIG: LazyLock<Arc<rustls::ClientConfig>> = LazyLock::new(|| {
		let roots = rustls::RootCertStore {
			roots: webpki_roots::TLS_SERVER_ROOTS.into(),
		};
		let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
		let config = rustls::ClientConfig::builder_with_provider(provider)
			.with_safe_default_protocol_versions()
			.expect("the provider supports the default protocol versions")
			.with_root_certificates(roots)
			.with_no_client_auth();
		Arc::new(config)
	});
	Arc::clone(&CONFIG)
}

/// The response body, failing when the server goes quiet between frames.
struct Incoming {
	body: hyper::body::Incoming,
	quiet: tokio::time::Interval,
}

impl Body for Incoming {
	type Data = Bytes;
	type Error = Error;

	fn poll_frame(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<http_body::Frame<Bytes>, Error>>> {
		match Pin::new(&mut self.body).poll_frame(cx) {
			Poll::Ready(None) => Poll::Ready(None),
			Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(Error::from(e)))),
			Poll::Ready(Some(Ok(frame))) => {
				self.quiet.reset();
				Poll::Ready(Some(Ok(frame)))
			}
			Poll::Pending => {
				ready!(self.quiet.poll_tick(cx));
				Poll::Ready(Some(Err(Error::ConnectionReadTimeout)))
			}
		}
	}

	fn is_end_stream(&self) -> bool {
		self.body.is_end_stream()
	}

	fn size_hint(&self) -> http_body::SizeHint {
		self.body.size_hint()
	}
}

async fn send(
	net: Arc<NetPolicy>,
	mut req: Request<WasiBody>,
	options: Option<RequestOptions>,
) -> Result<(Response<Incoming>, impl Future<Output = Result<(), Error>> + Send), Error> {
	let uri = req.uri();
	let authority = uri.authority().ok_or(Error::HttpRequestUriInvalid)?;
	let tls = uri.scheme() == Some(&Scheme::HTTPS);
	let host = authority.host().trim_start_matches('[').trim_end_matches(']').to_string();
	let port = authority.port_u16().unwrap_or(if tls {
		443
	} else {
		80
	});

	let options = options.unwrap_or_default();
	let connect_timeout = timeout(options.connect_timeout);
	let first_byte_timeout = timeout(options.first_byte_timeout);
	let between_bytes_timeout = timeout(options.between_bytes_timeout);

	let stream = tokio::time::timeout(connect_timeout, async {
		let addrs = admitted_addrs(&net, &host, port).await?;
		connect(&addrs).await
	})
	.await
	.map_err(|_| Error::ConnectionTimeout)??;

	let stream: Box<dyn Stream> = if tls {
		let name = rustls::pki_types::ServerName::try_from(host.clone())
			.map_err(|_| Error::HttpRequestUriInvalid)?;
		let stream = tokio::time::timeout(
			connect_timeout,
			tokio_rustls::TlsConnector::from(tls_config()).connect(name, stream),
		)
		.await
		.map_err(|_| Error::ConnectionTimeout)?
		.map_err(Error::Tls)?;
		Box::new(stream)
	} else {
		Box::new(stream)
	};

	let (mut sender, conn) = tokio::time::timeout(
		connect_timeout,
		hyper::client::conn::http1::Builder::new()
			.handshake(wasmtime_wasi_http::io::TokioIo::new(stream)),
	)
	.await
	.map_err(|_| Error::ConnectionTimeout)??;

	// The request line carries only the path unless the peer is a proxy.
	*req.uri_mut() = http::Uri::builder()
		.path_and_query(req.uri().path_and_query().map_or("/", |p| p.as_str()))
		.build()
		.map_err(|_| Error::HttpRequestUriInvalid)?;

	let exchange = async move {
		let response = tokio::time::timeout(first_byte_timeout, sender.send_request(req))
			.await
			.map_err(|_| Error::ConnectionReadTimeout)?
			.map_err(Error::from)?;
		// An interval of zero length panics, so a zero wait is the shortest one.
		let mut quiet = tokio::time::interval(between_bytes_timeout.max(Duration::from_nanos(1)));
		quiet.reset();
		Ok::<_, Error>(response.map(|body| Incoming {
			body,
			quiet,
		}))
	};
	let mut exchange = pin!(exchange);
	let mut conn = Some(conn);
	// Drive the connection's I/O while waiting for the response.
	let response = std::future::poll_fn(|cx| match exchange.as_mut().poll(cx) {
		Poll::Ready(r) => Poll::Ready(r),
		Poll::Pending => {
			let Some(driver) = conn.as_mut() else {
				return Poll::Pending;
			};
			let done = ready!(Pin::new(driver).poll(cx));
			conn = None;
			match done {
				Ok(()) => exchange.as_mut().poll(cx),
				Err(e) => Poll::Ready(Err(Error::from(e))),
			}
		}
	})
	.await?;

	Ok((response, async move {
		let Some(conn) = conn.take() else {
			return Ok(());
		};
		conn.await.map_err(Error::from)
	}))
}

#[cfg(test)]
mod tests {
	use http_body_util::Empty;
	use tokio::io::{AsyncReadExt, AsyncWriteExt};
	use tokio::net::TcpListener;
	use wasmtime::component::{Component, Linker};
	use wasmtime::{Config, Engine};

	use super::*;
	use crate::net_allow::NetDeny;
	use crate::store::StoreData;

	fn policy(entries: &[&str]) -> Arc<NetPolicy> {
		let entries: Vec<String> = entries.iter().map(|e| (*e).to_string()).collect();
		NetPolicy::parse(&entries, NetDeny::Nothing).unwrap()
	}

	fn get(url: &str) -> Request<WasiBody> {
		Request::builder()
			.uri(url)
			.body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed_unsync())
			.unwrap()
	}

	/// A server that answers one request with a fixed body.
	async fn serve_once() -> u16 {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		tokio::spawn(async move {
			let (mut conn, _) = listener.accept().await.unwrap();
			let mut buf = [0u8; 1024];
			let _ = conn.read(&mut buf).await;
			conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
				.await
				.unwrap();
		});
		port
	}

	async fn body_of(net: &Arc<NetPolicy>, url: &str) -> Result<Bytes, Error> {
		let (response, io) = send(Arc::clone(net), get(url), None).await?;
		let body = response.into_body().collect().await?.to_bytes();
		io.await?;
		Ok(body)
	}

	/// A module granted the address it is calling reaches it.
	#[tokio::test]
	async fn a_granted_address_is_reachable() {
		let port = serve_once().await;
		let net = policy(&["127.0.0.1"]);
		let body = body_of(&net, &format!("http://127.0.0.1:{port}/")).await.unwrap();
		assert_eq!(&body[..], b"ok");
	}

	/// A request body reaches the server: the body is driven by the connection,
	/// not by the completion future `send_request` is handed.
	#[tokio::test]
	async fn a_request_body_is_sent() {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let port = listener.local_addr().unwrap().port();
		let received = tokio::spawn(async move {
			let (mut conn, _) = listener.accept().await.unwrap();
			let mut seen = Vec::new();
			let mut buf = [0u8; 1024];
			while !seen.ends_with(b"payload") {
				let n = conn.read(&mut buf).await.unwrap();
				assert_ne!(n, 0, "the connection closed before the body arrived");
				seen.extend_from_slice(&buf[..n]);
			}
			conn.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
				.await
				.unwrap();
			String::from_utf8(seen).unwrap()
		});
		let request = Request::builder()
			.method("POST")
			.uri(format!("http://127.0.0.1:{port}/"))
			.header("content-length", "7")
			.body(
				http_body_util::Full::new(Bytes::from_static(b"payload"))
					.map_err(|never| match never {})
					.boxed_unsync(),
			)
			.unwrap();
		let (response, io) = send(policy(&["127.0.0.1"]), request, None).await.unwrap();
		response.into_body().collect().await.unwrap();
		io.await.unwrap();
		assert!(received.await.unwrap().starts_with("POST / HTTP/1.1"));
	}

	/// A module granted no network reaches nothing, loopback included.
	#[tokio::test]
	async fn no_grant_refuses_every_host() {
		let port = serve_once().await;
		let err = body_of(&NetPolicy::deny_all(), &format!("http://127.0.0.1:{port}/"))
			.await
			.unwrap_err();
		assert!(matches!(err, Error::HttpRequestDenied), "got {err:?}");
	}

	/// A name that resolves to loopback is refused unless an IP rule names
	/// loopback too, as it is for the guest's own sockets.
	#[tokio::test]
	async fn a_name_resolving_to_loopback_needs_an_ip_rule() {
		let port = serve_once().await;
		let url = format!("http://localhost:{port}/");
		let err = body_of(&policy(&["localhost"]), &url).await.unwrap_err();
		assert!(matches!(err, Error::HttpRequestDenied), "got {err:?}");

		let body = body_of(&policy(&["localhost", "127.0.0.1"]), &url).await.unwrap();
		assert_eq!(&body[..], b"ok");
	}

	/// The operator's deny rule outranks the grant.
	#[tokio::test]
	async fn an_operator_deny_rule_refuses_a_granted_address() {
		let port = serve_once().await;
		let deny = NetDeny::parse(&[format!("127.0.0.1:{port}")]).unwrap();
		let net = NetPolicy::parse(&["127.0.0.1".to_string()], deny).unwrap();
		let err = body_of(&net, &format!("http://127.0.0.1:{port}/")).await.unwrap_err();
		assert!(matches!(err, Error::HttpRequestDenied), "got {err:?}");
	}

	/// A component importing `wasi:http/types` at a newer patch version than
	/// the host links against still pre-instantiates.
	#[test]
	fn a_component_importing_wasi_http_pre_instantiates() {
		let mut config = Config::new();
		config.wasm_component_model(true);
		let engine = Engine::new(&config).unwrap();
		let component = Component::new(
			&engine,
			r#"(component
				(import "wasi:http/types@0.2.9" (instance
					(export "fields" (type (sub resource)))
				))
			)"#,
		)
		.unwrap();
		let mut linker: Linker<StoreData> = Linker::new(&engine);
		wasmtime_wasi::p2::add_to_linker_async(&mut linker).unwrap();
		wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker).unwrap();
		linker.instantiate_pre(&component).unwrap();
	}
}
