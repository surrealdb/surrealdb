//! Dynamic support for any engine
//!
//! SurrealDB supports various ways of storing and accessing your data. For
//! storing data we support a number of key value stores. These are SurrealKV
//! and an in-memory store. We call these local engines. SurrealKV is a
//! file-based, single node key value store. The
//! in-memory store does not persist your data, it only stores it in memory. All
//! these can be embedded in your application, so you don't need to spin up a
//! SurrealDB server first in order to use them. We also support spinning up a
//! server externally and then access your database via WebSockets or HTTP. We
//! call these remote engines.
//!
//! The Rust SDK abstracts away the implementation details of the engines to
//! make them work in a unified way. All these engines, whether they are local
//! or remote, work exactly the same way using the same API. The only difference
//! in the API is the endpoint you use to access the engine. Normally you
//! provide the scheme of the engine you want to use as a type parameter to
//! `Surreal::new`. This allows you detect, at compile, whether the engine
//! you are trying to use is enabled. If not, your code won't compile. This is
//! awesome but it strongly couples your application to the engine you are
//! using. In order to change an engine you would need to update your code to
//! the new scheme and endpoint you need to use and recompile it. This is where
//! the `any` engine comes in. We will call it `Surreal<Any>` (the type it
//! creates) to avoid confusion with the word any.
//!
//! `Surreal<Any>` allows you to use any engine as long as it was enabled when
//! compiling. Unlike with the typed scheme, the choice of the engine is made at
//! runtime depending on the endpoint that you provide as a string. If you use
//! an environment variable to provide this endpoint string, you won't need to
//! change your code  in order to switch engines. The downside to this is that
//! you will get a runtime error if you forget to enable the engine you
//! want to use when compiling your code. On the other hand, this totally
//! decouples your application from the engine you are using and makes it
//! possible to use whichever engine SurrealDB supports by simply changing the
//! Cargo features you enable when compiling. This enables some cool workflows.
//!
//! One of the common use cases we see is using SurrealDB as an embedded
//! database using SurrealKV as the local engine. This is a nice way to boost
//! the performance of your application when all you need is a single node.
//! SurrealKV is written in pure Rust so it needs no external dependencies, but
//! `Surreal<Any>` still lets you develop using an in-memory engine and deploy
//! using SurrealKV without changing your code.
//!
//! # Getting Started
//!
//! You can start by declaring your `surrealdb` dependency like this in
//! Cargo.toml
//!
//! ```toml
//! surrealdb = {
//!     version = "1",
//!
//!     # Disables the default features, which are `protocol-ws` and `rustls`.
//!     # Not necessary but can reduce your compile times if you don't need those features.
//!     default-features = false,
//!
//!     # Unconditionally enables the in-memory store.
//!     # Also not necessary but this will make `cargo run` just work.
//!     # Without it, you would need `cargo run --features surrealdb/kv-mem` during development. If you use a build
//!     # tool like `make` or `cargo make`, however, you can put that in your build step and avoid typing it manually.
//!     features = ["kv-mem"],
//!
//!     # Also not necessary but this makes it easy to switch between `stable`, `beta` and `nightly` crates, if need be.
//!     # See https://surrealdb.com/blog/introducing-nightly-and-beta-rust-crates for more information on those crates.
//!     package = "surrealdb"
//! }
//! ```
//!
//! You then simply need to instantiate `Surreal<Any>` instead of `Surreal<Db>`
//! or `Surreal<Client>`.
//!
//! # Examples
//!
//! ```no_run
//! use std::env;
//! use surrealdb::engine::any;
//! use surrealdb::engine::any::Any;
//! use surrealdb::opt::Resource;
//! use surrealdb::Surreal;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Use the endpoint specified in the environment variable or default to `memory`.
//!     // This makes it possible to use the memory engine during development but switch it
//!     // to any other engine for deployment.
//!     let endpoint = env::var("SURREALDB_ENDPOINT").unwrap_or_else(|_| "memory".to_owned());
//!
//!     // Create the Surreal instance. This will create `Surreal<Any>`.
//!     let db = any::connect(endpoint).await?;
//!
//!     // Specify the namespace and database to use
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Use the database like you normally would.
//!     delete_user(&db, "jane").await?;
//!
//!     Ok(())
//! }
//!
//! // Deletes a user from the user table in the database
//! async fn delete_user(db: &Surreal<Any>, username: &str) -> surrealdb::Result<()> {
//!     db.delete(Resource::from(("user", username))).await?;
//!     Ok(())
//! }
//! ```
//!
//! By doing something like this, you can use an in-memory database on your
//! development machine and you can just run `cargo run` without having to
//! specify the environment variable first or spinning up an external server
//! remotely. For the production binary you simply need to build it using
//! something like
//!
//! ```bash
//! cargo build --features surrealdb/kv-surrealkv --release
//! ```
//!
//! and export the `SURREALDB_ENDPOINT` environment variable when starting it.
//!
//! ```bash
//! export SURREALDB_ENDPOINT="surrealkv:/path/to/database/folder"
//! /path/to/binary
//! ```
//!
//! The example above shows one benefit of using `Surreal<Any>`: the engine is
//! chosen at runtime. For example, you can use the in-memory engine for
//! development and deploy using a remote engine like the WebSocket engine. This way you avoid
//! having to spin up a SurrealDB server first when developing and testing your application.
//!
//! For some applications where you allow users to determine the engine they
//! want to use, you can enable multiple engines for them when building, or even
//! enable them all. To do this you simply need to comma separate the Cargo
//! features.
//!
//! ```bash
//! cargo build --features surrealdb/protocol-ws,surrealdb/kv-surrealkv --release
//! ```
//!
//! In this case, the binary you build will have support for accessing an
//! external server via WebSockets or embedding the database using SurrealKV.

use std::sync::Arc;
#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(target_family = "wasm")]
mod wasm;

use std::marker::PhantomData;

use url::Url;

use crate::opt::{Config, Endpoint, path_to_string};
use crate::{Connect, Error, Result, Surreal};

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint: into_endpoint::Sealed {}

mod into_endpoint {
	pub trait Sealed {
		/// Converts an input into a server address object
		fn into_endpoint(self) -> super::Result<super::Endpoint>;
	}
}

#[doc(hidden)]
/// Internal API
pub fn __into_endpoint(path: impl IntoEndpoint) -> Result<Endpoint> {
	into_endpoint::Sealed::into_endpoint(path)
}

fn split_url(url: &str) -> (&str, &str) {
	match url.split_once("://") {
		Some(parts) => parts,
		None => match url.split_once(':') {
			Some(parts) => parts,
			None => (url, ""),
		},
	}
}

/// Percent-decodes the path of a local endpoint URL.
///
/// Every `%XX` escape with two hex digits is replaced by its byte; a `%` that
/// does not start such an escape is kept literally. Fails if the decoded bytes
/// are not UTF-8, if they contain a `?`, which the datastore would read as the
/// start of the query string, or if they contain a NUL byte, which storage
/// engines and filesystem APIs cannot accept in a path.
fn percent_decode_path(path: &str) -> Result<String> {
	let bytes = path.as_bytes();
	let mut decoded = Vec::with_capacity(bytes.len());
	let mut i = 0;
	while i < bytes.len() {
		if bytes[i] == b'%'
			&& let Some(byte) = bytes
				.get(i + 1..i + 3)
				.and_then(|hex| std::str::from_utf8(hex).ok())
				.and_then(|hex| u8::from_str_radix(hex, 16).ok())
		{
			decoded.push(byte);
			i += 3;
		} else {
			decoded.push(bytes[i]);
			i += 1;
		}
	}
	let decoded = String::from_utf8(decoded).map_err(|_| {
		Error::configuration(format!("Endpoint path `{path}` decodes to invalid UTF-8"), None)
	})?;
	if decoded.contains('?') {
		return Err(Error::configuration(
			format!("Endpoint path `{path}` decodes to a `?`, which is not supported"),
			None,
		));
	}
	if decoded.contains('\0') {
		return Err(Error::configuration(
			format!("Endpoint path `{path}` decodes to a NUL byte, which is not supported"),
			None,
		));
	}
	Ok(decoded)
}

impl IntoEndpoint for &str {}
impl into_endpoint::Sealed for &str {
	fn into_endpoint(self) -> Result<Endpoint> {
		let (url, path) = match self {
			// Handle bare "memory" and "mem://" (no query params) — path format: mem://
			"memory" | "mem://" => {
				(Url::parse("mem://").expect("valid memory url"), "mem://".to_owned())
			}
			// Handle "memory?..." with query parameters
			url if url.starts_with("memory?") => {
				let query = &url["memory?".len()..];
				(Url::parse("mem://").expect("valid memory url"), format!("mem://?{query}"))
			}
			// Handle "mem://?..." with query parameters
			url if url.starts_with("mem://?") => {
				let query = &url["mem://?".len()..];
				(Url::parse("mem://").expect("valid memory url"), format!("mem://?{query}"))
			}
			url if url.starts_with("ws")
				| url.starts_with("http")
				| url.starts_with("grpc")
				| url.starts_with("tikv") =>
			{
				(
					Url::parse(url)
						.map_err(|_| Error::internal(format!("Invalid URL: {}", self)))?,
					String::new(),
				)
			}

			_ => {
				let (scheme, rest) = split_url(self);
				let protocol = format!("{scheme}://");
				// Only the path is decoded; the query string is passed through
				// as written.
				let path = match rest.split_once('?') {
					Some((path, query)) => format!("{}?{query}", percent_decode_path(path)?),
					None => percent_decode_path(rest)?,
				};
				(
					Url::parse(&protocol)
						.map_err(|_| Error::internal(format!("Invalid URL: {}", self)))?,
					path_to_string(&protocol, path),
				)
			}
		};
		let mut endpoint = Endpoint::new(url);
		endpoint.path = path;
		Ok(endpoint)
	}
}

impl IntoEndpoint for &String {}
impl into_endpoint::Sealed for &String {
	fn into_endpoint(self) -> Result<Endpoint> {
		self.as_str().into_endpoint()
	}
}

impl IntoEndpoint for String {}
impl into_endpoint::Sealed for String {
	fn into_endpoint(self) -> Result<Endpoint> {
		self.as_str().into_endpoint()
	}
}

impl<T> IntoEndpoint for (T, Config) where T: Into<String> {}
impl<T> into_endpoint::Sealed for (T, Config)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = into_endpoint::Sealed::into_endpoint(self.0.into())?;
		endpoint.config = self.1;
		Ok(endpoint)
	}
}

/// A dynamic connection that supports any engine and allows you to pick at
/// runtime
#[derive(Debug, Clone)]
pub struct Any(());

impl Surreal<Any> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use std::sync::LazyLock;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::any::Any;
	///
	/// static DB: LazyLock<Surreal<Any>> = LazyLock::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect("ws://localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect(&self, address: impl IntoEndpoint) -> Connect<Any, ()> {
		Connect {
			surreal: Arc::clone(&self.inner).into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

/// Connects to a local, remote or embedded database
///
/// # Examples
///
/// ```no_run
/// use surrealdb::engine::any::connect;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// // Connect to a local endpoint
/// let db = connect("ws://localhost:8000").await?;
///
/// // Connect to a remote endpoint
/// let db = connect("wss://cloud.surrealdb.com").await?;
///
/// // Connect using HTTP
/// let db = connect("http://localhost:8000").await?;
///
/// // Connect using HTTPS
/// let db = connect("https://cloud.surrealdb.com").await?;
///
/// // Instantiate an in-memory instance (no persistence)
/// let db = connect("mem://").await?;
/// // Or use the "memory" alias: connect("memory").await?;
///
/// // In-memory with persistence to disk (e.g. mem:///tmp/data?versioned=true)
/// let db = connect("mem:///tmp/data").await?;
///
/// // Instantiate a SurrealKV-backed instance
/// let db = connect("surrealkv://path/to/database-folder").await?;
///
/// # Ok(())
/// # }
/// ```
pub fn connect(address: impl IntoEndpoint) -> Connect<Any, Surreal<Any>> {
	Connect {
		surreal: Surreal::init(),
		address: address.into_endpoint(),
		capacity: 0,
		response_type: PhantomData,
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod endpoint_tests {

	use super::*;

	/// A remote scheme keeps its authority in the URL and leaves the path
	/// empty; a local scheme keeps only its scheme in the URL and carries the
	/// rest in the path, which is where a file-backed engine reads it from.
	#[test]
	fn remote_schemes_keep_their_authority() {
		for url in ["ws://127.0.0.1:8000", "http://127.0.0.1:8000", "grpc://127.0.0.1:8000"] {
			let endpoint = __into_endpoint(url).unwrap();
			assert_eq!(endpoint.url.host_str(), Some("127.0.0.1"), "{url} lost its host");
			assert_eq!(endpoint.url.port(), Some(8000), "{url} lost its port");
			assert!(endpoint.path.is_empty(), "{url} was treated as a local engine");
		}
	}

	/// The schemes of earlier storage engines are deprecated spellings of
	/// `surrealkv` and open the same path with it.
	#[test]
	fn deprecated_schemes_open_with_surrealkv() {
		for url in ["rocksdb://some/folder", "indxdb://name"] {
			let endpoint = __into_endpoint(url).unwrap();
			assert!(
				matches!(
					crate::opt::endpoint::EndpointKind::from(endpoint.url.scheme()),
					crate::opt::endpoint::EndpointKind::SurrealKv
				),
				"{url} is not opened with surrealkv"
			);
			assert_eq!(endpoint.path, url);
		}
	}

	#[test]
	fn local_schemes_carry_a_path() {
		let endpoint = __into_endpoint("surrealkv://some/folder").unwrap();
		assert_eq!(endpoint.url.scheme(), "surrealkv");
		assert_eq!(endpoint.url.host_str(), None);
		assert_eq!(endpoint.path, "surrealkv://some/folder");
	}

	/// A local endpoint string is a URL, so its path is percent-decoded before
	/// it reaches the filesystem. A `%` that does not start a valid escape is
	/// kept literally, and the query string is passed through untouched.
	#[test]
	fn local_scheme_path_is_percent_decoded() {
		let cases = [
			(
				"surrealkv:///tmp/surrealdb%20path%20test/db",
				"surrealkv:///tmp/surrealdb path test/db",
			),
			("rocksdb://some%2Ffolder/db", "rocksdb://some/folder/db"),
			("surrealkv:///tmp/100%/db", "surrealkv:///tmp/100%/db"),
			("surrealkv:///tmp/50%zz/db", "surrealkv:///tmp/50%zz/db"),
			("surrealkv:///tmp/a%2/db", "surrealkv:///tmp/a%2/db"),
			("surrealkv:///tmp/caf%C3%A9", "surrealkv:///tmp/café"),
			("mem:///tmp/my%20data?versioned=true", "mem:///tmp/my data?versioned=true"),
			("surrealkv:///tmp/db?sync=a%20b", "surrealkv:///tmp/db?sync=a%20b"),
		];
		for (input, expected) in cases {
			let endpoint = __into_endpoint(input).unwrap();
			assert_eq!(endpoint.path, expected, "failed to decode `{input}`");
		}
	}

	#[test]
	fn local_scheme_path_rejects_undecodable_escapes() {
		for input in
			["surrealkv:///tmp/%FF/db", "surrealkv:///tmp/a%3Fb/db", "surrealkv:///tmp/a%00b/db"]
		{
			assert!(__into_endpoint(input).is_err(), "`{input}` should be rejected");
		}
	}
}

#[cfg(all(test, feature = "kv-mem"))]
#[allow(clippy::unwrap_used)]
mod tests {

	use super::*;
	use crate::opt::auth::Root;
	use crate::opt::capabilities::Capabilities;
	use crate::types::{Object, Value};

	#[tokio::test]
	async fn local_engine_without_auth() {
		// Instantiate an in-memory instance without root credentials
		let db = connect("memory").await.unwrap();
		db.use_ns("N").use_db("D").await.unwrap();
		// The client has access to everything
		assert!(
			db.query("INFO FOR ROOT").await.unwrap().check().is_ok(),
			"client should have access to ROOT"
		);
		assert!(
			db.query("INFO FOR NS").await.unwrap().check().is_ok(),
			"client should have access to NS"
		);
		assert!(
			db.query("INFO FOR DB").await.unwrap().check().is_ok(),
			"client should have access to DB"
		);

		// There are no users in the datastore
		let mut res = db.query("INFO FOR ROOT").await.unwrap();
		let users: Value = res.take("users").unwrap();

		assert_eq!(
			users,
			Value::Object(Object::default()),
			"there should be no users in the system"
		);
	}

	#[tokio::test]
	async fn local_engine_with_auth() {
		// Instantiate an in-memory instance with root credentials
		let creds = Root {
			username: "root".to_string(),
			password: "root".to_string(),
		};
		let db = connect((
			"memory",
			Config::new().user(creds.clone()).capabilities(Capabilities::all()),
		))
		.await
		.unwrap();
		// `use_ns`/`use_db` set the session context but no longer
		// implicitly create the namespace/database for an unauthenticated
		// session — that auto-creation now requires the same authorization
		// as `DEFINE NAMESPACE` / `DEFINE DATABASE` (SECURITY_GUIDE §3).
		db.use_ns("N").use_db("D").await.unwrap();

		// The client needs to sign in before it can access anything
		db.query("INFO FOR ROOT")
			.await
			.unwrap()
			.check()
			.expect_err("client should not have access to KV");
		db.query("INFO FOR NS")
			.await
			.unwrap()
			.check()
			.expect_err("client should not have access to NS");
		db.query("INFO FOR DB")
			.await
			.unwrap()
			.check()
			.expect_err("client should not have access to DB");

		// It can sign in
		db.signin(creds).await.expect("client should be able to sign in");

		// Re-issue `use_ns`/`use_db` now that the session has the
		// authority to create the namespace/database it targets.
		db.use_ns("N").use_db("D").await.unwrap();

		// After the sign in and re-issued `use`, the client has access to
		// everything.
		db.query("INFO FOR ROOT").await.unwrap().check().expect("client should have access to KV");
		db.query("INFO FOR NS").await.unwrap().check().expect("client should have access to NS");
		db.query("INFO FOR DB").await.unwrap().check().expect("client should have access to DB");
	}
}
