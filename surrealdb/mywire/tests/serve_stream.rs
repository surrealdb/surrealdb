//! `serve` over an arbitrary byte stream: a connection handed over after its greeting was sent,
//! with credentials judged by an external authenticator.

#![recursion_limit = "256"]

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::task::{Context, Poll};
use std::time::Duration;

use rcgen::CertifiedKey;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_iam::{Actor, Auth, Level, Role};
use surrealdb_mywire::auth::AUTH_PLUGIN_CACHING_SHA2_PASSWORD;
use surrealdb_mywire::constants::*;
use surrealdb_mywire::{
	AuthFuture, AuthRequest, Authenticator, Io, ServeOptions, Start, greeting_packet, serve,
};
use tokio::io::{
	AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf, duplex,
};
use tokio::time::timeout;
use tokio_rustls::rustls::crypto::aws_lc_rs;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::sync::CancellationToken;

const IO_TIMEOUT: Duration = Duration::from_secs(10);

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
	let key = PrivateKeyDer::try_from(signing_key.serialize_der()).expect("key");
	let config = ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(vec![cert.der().clone()], key)
		.expect("server config");
	Tls {
		cert: cert.der().clone(),
		acceptor: Arc::new(TlsAcceptor::from(Arc::new(config))),
	}
}

async fn packet<S: AsyncWrite + Unpin>(stream: &mut S, seq: u8, payload: &[u8]) {
	let mut buf = (payload.len() as u32).to_le_bytes()[..3].to_vec();
	buf.push(seq);
	buf.extend_from_slice(payload);
	stream.write_all(&buf).await.expect("write a packet");
}

async fn read_packet<S: AsyncRead + Unpin>(stream: &mut S) -> (u8, Vec<u8>) {
	let mut header = [0u8; 4];
	stream.read_exact(&mut header).await.expect("a packet header");
	let len = (header[0] as usize) | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
	assert!(len <= 1 << 20, "a packet over 1 MiB");
	let mut payload = vec![0u8; len];
	stream.read_exact(&mut payload).await.expect("a packet payload");
	(header[3], payload)
}

const CLIENT_CAPS: u32 = CLIENT_PROTOCOL_41
	| CLIENT_SECURE_CONNECTION
	| CLIENT_PLUGIN_AUTH
	| CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
	| CLIENT_DEPRECATE_EOF
	| CLIENT_SSL;

fn ssl_request() -> Vec<u8> {
	let mut payload = CLIENT_CAPS.to_le_bytes().to_vec();
	payload.extend_from_slice(&(1u32 << 24).to_le_bytes());
	payload.push(CHARSET_UTF8MB4_GENERAL_CI as u8);
	payload.extend_from_slice(&[0u8; 23]);
	payload
}

fn handshake_response(user: &str) -> Vec<u8> {
	let mut payload = ssl_request();
	payload.extend_from_slice(user.as_bytes());
	payload.push(0);
	// An empty lenenc auth response, then the plugin name.
	payload.push(0);
	payload.extend_from_slice(AUTH_PLUGIN_CACHING_SHA2_PASSWORD.as_bytes());
	payload.push(0);
	payload
}

/// Runs the handed-over exchange for `user` and `password`, returning the packet that answers the
/// password.
async fn exchange(user: &str, password: &str) -> Vec<u8> {
	let tls = tls();
	let (client, server) = duplex(1 << 16);
	let ds = Datastore::new("memory").await.expect("datastore");
	tokio::spawn(serve(
		Duplex(server),
		ds,
		ServeOptions {
			peer: "203.0.113.7:3306".parse().expect("address"),
			tls: Some(Arc::clone(&tls.acceptor)),
			start: Start::Greeted {
				connection_id: 7,
			},
			ready: Arc::new(AtomicBool::new(true)),
			shutdown: CancellationToken::new(),
			authenticator: Some(Arc::new(Fixed)),
		},
	));
	timeout(IO_TIMEOUT, async move {
		let mut client = client;
		// The front end's greeting and the client's SSLRequest, replayed into the connection.
		let greeting = greeting_packet(7);
		assert_eq!(greeting[3], 0, "the greeting is sequence 0");
		packet(&mut client, 1, &ssl_request()).await;
		let mut roots = RootCertStore::empty();
		roots.add(tls.cert.clone()).expect("trust the certificate");
		let config = ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
		let name = ServerName::try_from("localhost").expect("server name");
		let mut stream =
			TlsConnector::from(Arc::new(config)).connect(name, client).await.expect("TLS");
		packet(&mut stream, 2, &handshake_response(user)).await;
		let (seq, payload) = read_packet(&mut stream).await;
		assert_eq!(seq, 3);
		assert_eq!(payload, vec![0x01, 0x04], "full authentication is requested");
		packet(&mut stream, 4, format!("{password}\0").as_bytes()).await;
		let (seq, payload) = read_packet(&mut stream).await;
		assert_eq!(seq, 5);
		payload
	})
	.await
	.expect("the exchange timed out")
}

#[tokio::test]
async fn an_external_authenticator_admits_a_user_after_a_greeting_sent_elsewhere() {
	let answer = exchange("alice", "secret").await;
	assert_eq!(answer[0], 0x00, "an OK packet");
}

#[tokio::test]
async fn an_external_authenticator_refuses_a_wrong_password() {
	let answer = exchange("alice", "wrong").await;
	assert_eq!(answer[0], 0xff, "an ERR packet");
	assert_eq!(u16::from_le_bytes([answer[1], answer[2]]), 1045);
}

/// A connection whose TLS the host ended: the handshake response arrives in clear and full
/// authentication is still asked for.
#[tokio::test]
async fn a_secured_connection_takes_its_handshake_response_in_clear() {
	let (mut client, server) = duplex(1 << 16);
	let ds = Datastore::new("memory").await.expect("datastore");
	tokio::spawn(serve(
		Duplex(server),
		ds,
		ServeOptions {
			peer: "203.0.113.7:3306".parse().expect("address"),
			tls: None,
			start: Start::Secured {
				connection_id: 7,
			},
			ready: Arc::new(AtomicBool::new(true)),
			shutdown: CancellationToken::new(),
			authenticator: Some(Arc::new(Fixed)),
		},
	));
	timeout(IO_TIMEOUT, async {
		packet(&mut client, 2, &handshake_response("alice")).await;
		let (seq, payload) = read_packet(&mut client).await;
		assert_eq!(seq, 3);
		assert_eq!(payload, vec![0x01, 0x04], "full authentication is requested");
		packet(&mut client, 4, b"secret\0").await;
		let (seq, payload) = read_packet(&mut client).await;
		assert_eq!(seq, 5);
		assert_eq!(payload[0], 0x00, "an OK packet");
	})
	.await
	.expect("the exchange timed out");
}
