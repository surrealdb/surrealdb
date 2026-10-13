//! Resolution of `silo::` module executables against the Silo package host.
//!
//! A silo executable names an organisation, a package and an exact semantic
//! version. Those coordinates map onto a single immutable object:
//!
//! ```text
//! {endpoint}/{organisation}/{package}/{major}.{minor}.{patch}.surli
//! ```
//!
//! The endpoint is `surrealism_silo_endpoint`, so an operator can point the
//! whole resolution at a mirror without touching any `DEFINE MODULE`
//! statement. Packages are public and immutable per version, so the fetch
//! carries no credentials and the result is cached under
//! [`SurrealismCacheLookup::Silo`](crate::surrealism::cache::SurrealismCacheLookup).

use anyhow::{Context as _, Result, bail};
use bytes::{Bytes, BytesMut};
use http::Method;
use url::Url;

use crate::http::HttpClient;

/// The maximum size of a package body accepted from the Silo host.
///
/// A `.surli` is a compressed archive whose uncompressed filesystem is bounded
/// separately by `surrealism_max_fs_bytes`; this bounds the download itself so
/// a hostile or misconfigured endpoint cannot exhaust memory before unpacking
/// ever begins.
const MAX_PACKAGE_BYTES: u64 = 256 * 1024 * 1024;

/// Reject any organisation or package name that cannot appear verbatim in a
/// URL path segment.
///
/// Both names reach here straight from the parser, where a backtick-quoted
/// identifier can hold arbitrary text including `/` and `..`. Restricting them
/// to the character set Silo itself allows keeps a definition from reaching
/// outside the package namespace it names.
fn validate_segment(kind: &str, value: &str) -> Result<()> {
	if value.is_empty() {
		bail!("A silo module {kind} must not be empty");
	}
	if !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
		bail!(
			"A silo module {kind} may only contain ASCII letters, digits, `-` and `_`, but found `{value}`"
		);
	}
	Ok(())
}

/// Build the URL a package version resolves to.
pub(crate) fn package_url(
	endpoint: &str,
	organisation: &str,
	package: &str,
	major: u32,
	minor: u32,
	patch: u32,
) -> Result<Url> {
	validate_segment("organisation", organisation)?;
	validate_segment("package", package)?;

	let endpoint = endpoint.trim_end_matches('/');
	let url = format!("{endpoint}/{organisation}/{package}/{major}.{minor}.{patch}.surli");

	Url::parse(&url).with_context(|| format!("Invalid silo module endpoint `{endpoint}`"))
}

/// Download the `.surli` package at `url`, which [`package_url`] built.
///
/// The caller supplies a client already scoped to the endpoint's host — which
/// is why it needs the URL first — so the server's deny-list still applies even
/// though a silo fetch needs no `allow_net` grant of its own. The coordinates
/// are passed alongside so a miss names the module rather than the object.
pub(crate) async fn fetch_package(
	client: &HttpClient,
	url: &Url,
	organisation: &str,
	package: &str,
	major: u32,
	minor: u32,
	patch: u32,
) -> Result<Bytes> {
	fetch_package_capped(client, url, organisation, package, major, minor, patch, MAX_PACKAGE_BYTES)
		.await
}

/// [`fetch_package`] with an explicit ceiling, so the streaming bound can be
/// exercised without moving [`MAX_PACKAGE_BYTES`] worth of bytes.
#[expect(clippy::too_many_arguments, reason = "package coordinates are five of these")]
async fn fetch_package_capped(
	client: &HttpClient,
	url: &Url,
	organisation: &str,
	package: &str,
	major: u32,
	minor: u32,
	patch: u32,
	max_bytes: u64,
) -> Result<Bytes> {
	let mut response = client
		.request(Method::GET, url.clone())
		.send()
		.await
		.with_context(|| format!("Failed to fetch silo module package from `{url}`"))?;

	let status = response.status();
	if status == http::StatusCode::NOT_FOUND {
		bail!(
			"The silo module `silo::{organisation}::{package}::<{major}.{minor}.{patch}>` was not found"
		);
	}
	if !status.is_success() {
		bail!("Failed to fetch silo module package from `{url}`: HTTP status {status}");
	}

	// Refuse a package that announces itself as oversized before reading a byte
	// of it.
	if let Some(len) = response.content_length()
		&& len > max_bytes
	{
		bail!("The silo module package at `{url}` is {len} bytes, over the {max_bytes} byte limit");
	}

	// `content-length` is advisory — a chunked response omits it, and a hostile
	// one may understate it — so the body is accumulated a chunk at a time and
	// the cap is enforced as it grows, rather than after the whole body is
	// already held in memory.
	let mut body =
		BytesMut::with_capacity(response.content_length().unwrap_or(0).min(max_bytes) as usize);
	while let Some(chunk) = response
		.chunk()
		.await
		.with_context(|| format!("Failed to read silo module package from `{url}`"))?
	{
		if body.len() as u64 + chunk.len() as u64 > max_bytes {
			bail!("The silo module package at `{url}` is over the {max_bytes} byte limit");
		}
		body.extend_from_slice(&chunk);
	}

	Ok(body.freeze())
}

#[cfg(test)]
mod tests {
	use std::io::{Read as _, Write as _};
	use std::net::{Ipv4Addr, TcpListener};

	use super::*;
	use crate::dbs::capabilities::Targets;
	use crate::http::config::HttpConfig;

	/// Serve one canned response on a loopback port and return its base URL.
	///
	/// The listener accepts exactly one connection, so each test gets a fresh
	/// server and there is nothing to tear down.
	fn serve_once(response: Vec<u8>) -> String {
		let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
		let addr = listener.local_addr().unwrap();

		std::thread::spawn(move || {
			let Ok((mut stream, _)) = listener.accept() else {
				return;
			};
			// Drain the request head before replying, so closing the socket
			// does not reset a connection with bytes still inbound.
			let mut buf = Vec::new();
			let mut chunk = [0u8; 1024];
			while let Ok(n) = stream.read(&mut chunk) {
				if n == 0 {
					break;
				}
				buf.extend_from_slice(&chunk[..n]);
				if buf.windows(4).any(|w| w == b"\r\n\r\n") {
					break;
				}
			}
			let _ = stream.write_all(&response);
			let _ = stream.flush();
		});

		format!("http://{addr}")
	}

	fn client_for(url: &Url) -> HttpClient {
		let host = url.host().expect("test endpoint has a host");
		let allow = Targets::Some(
			[crate::dbs::capabilities::NetTarget::Host(
				host.to_owned(),
				url.port_or_known_default(),
			)]
			.into_iter()
			.collect(),
		);
		HttpClient::new(allow, Targets::None, &HttpConfig::default()).unwrap()
	}

	fn ok_response(body: &[u8]) -> Vec<u8> {
		let mut out = format!(
			"HTTP/1.1 200 OK\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
			body.len()
		)
		.into_bytes();
		out.extend_from_slice(body);
		out
	}

	#[tokio::test]
	async fn fetches_a_package_body() {
		let body = b"not really a surli, but bytes are bytes";
		let endpoint = serve_once(ok_response(body));
		let url = package_url(&endpoint, "surrealdb", "base58", 1, 0, 0).unwrap();

		let fetched =
			fetch_package(&client_for(&url), &url, "surrealdb", "base58", 1, 0, 0).await.unwrap();

		assert_eq!(fetched.as_ref(), body);
	}

	#[tokio::test]
	async fn reports_a_missing_version_by_name() {
		let endpoint = serve_once(
			b"HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_vec(),
		);
		let url = package_url(&endpoint, "surrealdb", "base58", 9, 9, 9).unwrap();

		let err = fetch_package(&client_for(&url), &url, "surrealdb", "base58", 9, 9, 9)
			.await
			.unwrap_err()
			.to_string();

		assert!(err.contains("silo::surrealdb::base58::<9.9.9>"), "{err}");
		assert!(err.contains("was not found"), "{err}");
	}

	#[tokio::test]
	async fn surfaces_a_server_error_with_its_status() {
		let endpoint = serve_once(
			b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
				.to_vec(),
		);
		let url = package_url(&endpoint, "surrealdb", "base58", 1, 0, 0).unwrap();

		let err = fetch_package(&client_for(&url), &url, "surrealdb", "base58", 1, 0, 0)
			.await
			.unwrap_err()
			.to_string();

		assert!(err.contains("503"), "{err}");
	}

	/// A declared length over the cap is refused before the body is read, so a
	/// hostile endpoint cannot spend the memory it announced.
	#[tokio::test]
	async fn refuses_an_oversized_declared_length() {
		let endpoint = serve_once(
			format!(
				"HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
				MAX_PACKAGE_BYTES + 1
			)
			.into_bytes(),
		);
		let url = package_url(&endpoint, "surrealdb", "base58", 1, 0, 0).unwrap();

		let err = fetch_package(&client_for(&url), &url, "surrealdb", "base58", 1, 0, 0)
			.await
			.unwrap_err()
			.to_string();

		assert!(err.contains("over the"), "{err}");
	}

	/// A body that exceeds the cap is refused even when `content-length` never
	/// said so, which is the case the pre-read check cannot cover.
	#[tokio::test]
	async fn refuses_an_undeclared_oversized_body() {
		// No content-length: the client reads until EOF, so only the running
		// bound can stop it.
		let mut response = b"HTTP/1.1 200 OK\r\nconnection: close\r\n\r\n".to_vec();
		response.extend_from_slice(&vec![0u8; 4096]);
		let endpoint = serve_once(response);
		let url = package_url(&endpoint, "surrealdb", "base58", 1, 0, 0).unwrap();

		let err =
			fetch_package_capped(&client_for(&url), &url, "surrealdb", "base58", 1, 0, 0, 1024)
				.await
				.unwrap_err()
				.to_string();

		assert!(err.contains("over the 1024 byte limit"), "{err}");
	}

	#[test]
	fn builds_a_package_url() {
		let url =
			package_url("https://silo.surrealdb.com", "surrealdb", "base58", 1, 0, 0).unwrap();
		assert_eq!(url.as_str(), "https://silo.surrealdb.com/surrealdb/base58/1.0.0.surli");
	}

	#[test]
	fn strips_a_trailing_slash_from_the_endpoint() {
		let url = package_url("https://mirror.example.com/", "acme", "pkg", 2, 11, 3).unwrap();
		assert_eq!(url.as_str(), "https://mirror.example.com/acme/pkg/2.11.3.surli");
	}

	#[test]
	fn rejects_a_traversing_segment() {
		let err = package_url("https://silo.surrealdb.com", "../../etc", "pkg", 1, 0, 0)
			.unwrap_err()
			.to_string();
		assert!(err.contains("organisation"), "{err}");
	}

	#[test]
	fn rejects_an_empty_segment() {
		let err =
			package_url("https://silo.surrealdb.com", "acme", "", 1, 0, 0).unwrap_err().to_string();
		assert!(err.contains("package"), "{err}");
	}
}
