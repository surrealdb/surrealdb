//! `serve` over an arbitrary byte stream: a connection handed over after its `SSLRequest` was
//! answered, with credentials judged by an external authenticator.

#![recursion_limit = "256"]

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
use std::time::Duration;

use rcgen::CertifiedKey;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_iam::{Actor, Auth, Level, Role};
use surrealdb_pgwire::{
	AuthFuture, AuthRequest, Authenticator, Dialect, Io, ServeOptions, Shared, Start, serve,
};
use tokio::io::{
	AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex,
};
use tokio::time::timeout;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;

const PROTOCOL_3_0: i32 = 196608;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// A duplex stream as a connection's byte stream.
struct Duplex(DuplexStream);

impl AsyncRead for Duplex {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.0).poll_read(cx, buf)
	}
}

impl AsyncWrite for Duplex {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.0).poll_write(cx, buf)
	}
	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.0).poll_flush(cx)
	}
	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.0).poll_shutdown(cx)
	}
}

impl Io for Duplex {}

/// Accepts the user `alice` with the password `secret` as a root owner.
struct Fixed;

impl Authenticator for Fixed {
	fn authenticate<'a>(&'a self, base: &'a Session, request: AuthRequest<'a>) -> AuthFuture<'a> {
		Box::pin(async move {
			if request.user == "alice" && request.password == "secret" {
				let mut session = base.clone();
				session.au = Arc::new(Auth::new(Actor::new(
					request.user.to_string(),
					vec![Role::Owner],
					Level::Root,
				)));
				Some(session)
			} else {
				None
			}
		})
	}
}

struct Tls {
	cert: CertificateDer<'static>,
	acceptor: Arc<TlsAcceptor>,
}

fn tls() -> Tls {
	let _ = aws_lc_rs::default_provider().install_default();
	let CertifiedKey {
		cert,
		signing_key,
	} = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("certificate");
	let key = tokio_rustls::rustls::pki_types::PrivateKeyDer::try_from(signing_key.serialize_der())
		.expect("key");
	let config = ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(vec![cert.der().clone()], key)
		.expect("server config");
	Tls {
		cert: cert.der().clone(),
		acceptor: Arc::new(TlsAcceptor::from(Arc::new(config))),
	}
}

async fn start(tls: &Tls, authenticator: Option<Arc<dyn Authenticator>>) -> DuplexStream {
	let (client, server) = duplex(1 << 16);
	let ds = Datastore::new("memory").await.expect("datastore");
	let peer: SocketAddr = "203.0.113.7:5432".parse().expect("address");
	tokio::spawn(serve(
		Duplex(server),
		ds,
		ServeOptions {
			peer,
			tls: Some(Arc::clone(&tls.acceptor)),
			start: Start::Tls,
			ready: Arc::new(AtomicBool::new(true)),
			shutdown: CancellationToken::new(),
			authenticator,
			shared: Arc::new(Shared::default()),
			dialect: Dialect::PgSql,
		},
	));
	client
}

async fn connect(tls: &Tls, client: DuplexStream) -> TlsStream<DuplexStream> {
	let mut roots = RootCertStore::empty();
	roots.add(tls.cert.clone()).expect("trust the certificate");
	let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
	let name = ServerName::try_from("localhost").expect("server name");
	TlsConnector::from(Arc::new(config)).connect(name, client).await.expect("the TLS handshake")
}

async fn send_startup<S: AsyncWrite + Unpin>(stream: &mut S, user: &str) {
	let mut body = PROTOCOL_3_0.to_be_bytes().to_vec();
	for (key, value) in [("user", user), ("database", "test/test")] {
		body.extend_from_slice(key.as_bytes());
		body.push(0);
		body.extend_from_slice(value.as_bytes());
		body.push(0);
	}
	body.push(0);
	let mut packet = ((body.len() + 4) as i32).to_be_bytes().to_vec();
	packet.extend_from_slice(&body);
	stream.write_all(&packet).await.expect("send the startup message");
}

async fn read_message<S: AsyncRead + Unpin>(stream: &mut S) -> (u8, Vec<u8>) {
	let kind = stream.read_u8().await.expect("a message type");
	let len = stream.read_i32().await.expect("a message length");
	let body_len = usize::try_from(len - 4).expect("a length");
	assert!(body_len <= 1 << 20, "a message over 1 MiB");
	let mut body = vec![0u8; body_len];
	stream.read_exact(&mut body).await.expect("a message body");
	(kind, body)
}

async fn send_password<S: AsyncWrite + Unpin>(stream: &mut S, password: &str) {
	let mut packet = vec![b'p'];
	packet.extend_from_slice(&((password.len() + 5) as i32).to_be_bytes());
	packet.extend_from_slice(password.as_bytes());
	packet.push(0);
	stream.write_all(&packet).await.expect("send the password");
}

#[tokio::test]
async fn an_external_authenticator_admits_a_user_over_a_stream_that_starts_with_tls() {
	let tls = tls();
	let client = start(&tls, Some(Arc::new(Fixed))).await;
	timeout(IO_TIMEOUT, async {
		let mut stream = connect(&tls, client).await;
		send_startup(&mut stream, "alice").await;
		let (kind, body) = read_message(&mut stream).await;
		assert_eq!((kind, &body[..]), (b'R', &3i32.to_be_bytes()[..]), "a cleartext request");
		send_password(&mut stream, "secret").await;
		loop {
			let (kind, body) = read_message(&mut stream).await;
			match kind {
				b'Z' => return,
				b'E' => panic!("refused: {}", String::from_utf8_lossy(&body)),
				_ => {}
			}
		}
	})
	.await
	.expect("the exchange timed out");
}

#[tokio::test]
async fn an_external_authenticator_refuses_a_wrong_password() {
	let tls = tls();
	let client = start(&tls, Some(Arc::new(Fixed))).await;
	timeout(IO_TIMEOUT, async {
		let mut stream = connect(&tls, client).await;
		send_startup(&mut stream, "alice").await;
		let _ = read_message(&mut stream).await;
		send_password(&mut stream, "wrong").await;
		let (kind, body) = read_message(&mut stream).await;
		assert_eq!(kind, b'E');
		assert!(String::from_utf8_lossy(&body).contains("28P01"), "an authentication failure");
	})
	.await
	.expect("the exchange timed out");
}

#[tokio::test]
async fn a_stream_that_starts_with_tls_needs_an_acceptor() {
	let (mut client, server) = duplex(1 << 12);
	let ds = Datastore::new("memory").await.expect("datastore");
	let handle = tokio::spawn(serve(
		Duplex(server),
		ds,
		ServeOptions {
			peer: "203.0.113.7:5432".parse().expect("address"),
			tls: None,
			start: Start::Tls,
			ready: Arc::new(AtomicBool::new(true)),
			shutdown: CancellationToken::new(),
			authenticator: None,
			shared: Arc::new(Shared::default()),
			dialect: Dialect::PgSql,
		},
	));
	timeout(IO_TIMEOUT, handle).await.expect("the connection ends").expect("no panic");
	let mut rest = Vec::new();
	client.read_to_end(&mut rest).await.expect("closed");
	assert!(rest.is_empty());
}

#[tokio::test]
async fn a_served_connection_starts_in_the_dialect_it_is_given() {
	let tls = tls();
	let client = start(&tls, Some(Arc::new(Fixed))).await;
	timeout(IO_TIMEOUT, async {
		let mut stream = connect(&tls, client).await;
		send_startup(&mut stream, "alice").await;
		let _ = read_message(&mut stream).await;
		send_password(&mut stream, "secret").await;
		loop {
			if read_message(&mut stream).await.0 == b'Z' {
				break;
			}
		}
		// `SELECT 1 AS one` is a PostgreSQL statement; SurrealQL refuses it for want of a `FROM`.
		let query = b"select 1 as one\0";
		let mut packet = vec![b'Q'];
		packet.extend_from_slice(&((query.len() + 4) as i32).to_be_bytes());
		packet.extend_from_slice(query);
		stream.write_all(&packet).await.expect("send the query");
		let mut tags = Vec::new();
		loop {
			let (kind, body) = read_message(&mut stream).await;
			match kind {
				b'C' => {
					tags.push(String::from_utf8_lossy(&body).trim_end_matches('\0').to_string())
				}
				b'E' => panic!("refused: {}", String::from_utf8_lossy(&body)),
				b'Z' => break,
				_ => {}
			}
		}
		assert_eq!(tags, ["SELECT 1"]);
	})
	.await
	.expect("the exchange timed out");
}

/// A connection whose TLS the host ended: the startup message arrives in clear and the password is
/// still asked for, because the connection counts as encrypted.
#[tokio::test]
async fn a_secured_connection_takes_a_startup_message_in_clear_and_asks_for_a_password() {
	let (mut client, server) = duplex(1 << 16);
	let ds = Datastore::new("memory").await.expect("datastore");
	tokio::spawn(serve(
		Duplex(server),
		ds,
		ServeOptions {
			peer: "203.0.113.7:5432".parse().expect("address"),
			tls: None,
			start: Start::Secured,
			ready: Arc::new(AtomicBool::new(true)),
			shutdown: CancellationToken::new(),
			authenticator: Some(Arc::new(Fixed)),
			shared: Arc::new(Shared::default()),
			dialect: Dialect::PgSql,
		},
	));
	timeout(IO_TIMEOUT, async {
		send_startup(&mut client, "alice").await;
		let (kind, body) = read_message(&mut client).await;
		assert_eq!((kind, &body[..]), (b'R', &3i32.to_be_bytes()[..]), "a cleartext request");
		send_password(&mut client, "secret").await;
		loop {
			let (kind, body) = read_message(&mut client).await;
			match kind {
				b'Z' => return,
				b'E' => panic!("refused: {}", String::from_utf8_lossy(&body)),
				_ => {}
			}
		}
	})
	.await
	.expect("the exchange timed out");
}
