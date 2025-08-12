//! Dynamic support for any engine
//!
//! SurrealDB supports various ways of storing and accessing your data. For
//! storing data we support a number of key value stores. These are SurrealKV,
//! RocksDB, TiKV, FoundationDB and an in-memory store. We call these
//! local engines. SurrealKV and RocksDB are file-based, single node key value
//! stores. TiKV and FoundationDB are are distributed stores that can scale
//! horizontally across multiple nodes. The in-memory store does not persist
//! your data, it only stores it in memory. All these can be embedded in your
//! application, so you don't need to spin up a SurrealDB server first in order
//! to use them. We also support spinning up a server externally and then access
//! your database via WebSockets or HTTP. We call these remote engines.
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
//! database using RocksDB as the local engine. This is a nice way to boost the
//! performance of your application when all you need is a single node. The
//! downside of this approach is that RocksDB is not written in Rust so you will
//! need to install some external dependencies on your development machine in
//! order to successfully compile it. Some of our users have reported that
//! this is not exactly straight-forward on Windows. Another issue is that
//! RocksDB is very resource intensive to compile and it takes a long time. Both
//! of these issues can be easily avoided by using `Surreal<Any>`. You can
//! develop using an in-memory engine but deploy using RocksDB. If you develop
//! on Windows but deploy to Linux then you completely avoid having to build
//! RocksDB on Windows at all.
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
//! ```rust
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
//! remotely to avoid RocksDB's compilation cost. You also don't need to install
//! any `C` or `C++` dependencies on your Windows machine. For the production
//! binary you simply need to build it using something like
//!
//! ```bash
//! cargo build --features surrealdb/kv-rocksdb --release
//! ```
//!
//! and export the `SURREALDB_ENDPOINT` environment variable when starting it.
//!
//! ```bash
//! export SURREALDB_ENDPOINT="rocksdb:/path/to/database/folder"
//! /path/to/binary
//! ```
//!
//! The example above shows how you can avoid compiling RocksDB on your
//! development machine, thereby avoiding dependency hell and paying the
//! compilation cost during development. This is not the only benefit you can
//! derive from using `Surreal<Any>` though. It's still useful even when your
//! engine isn't expensive to compile. For example, the remote engines use pure
//! Rust dependencies but you can still benefit from using `Surreal<Any>` by
//! using the in-memory engine for development and deploy using a remote engine
//! like the WebSocket engine. This way you avoid having to spin up a SurrealDB
//! server first when developing and testing your application.
//!
//! For some applications where you allow users to determine the engine they
//! want to use, you can enable multiple engines for them when building, or even
//! enable them all. To do this you simply need to comma separate the Cargo
//! features.
//!
//! ```bash
//! cargo build --features surrealdb/protocol-ws,surrealdb/kv-rocksdb,surrealdb/kv-tikv --release
//! ```
//!
//! In this case, the binary you build will have support for accessing an
//! external server via WebSockets, embedding the database using RocksDB or
//! using a distributed TiKV cluster.

#[cfg(not(target_family = "wasm"))]
mod native;
#[cfg(target_family = "wasm")]
mod wasm;

use std::marker::PhantomData;

use url::Url;

use crate::api::err::Error;
use crate::api::opt::{Config, Endpoint};
use crate::api::{Connect, Result, Surreal};
use crate::opt::path_to_string;

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

impl IntoEndpoint for &str {}
impl into_endpoint::Sealed for &str {
	fn into_endpoint(self) -> Result<Endpoint> {
		let (url, path) = match self {
			"memory" | "mem://" => (Url::parse("mem://").unwrap(), "memory".to_owned()),
			url if url.starts_with("ws") | url.starts_with("http") | url.starts_with("tikv") => {
				(Url::parse(url).map_err(|_| Error::InvalidUrl(self.to_owned()))?, String::new())
			}

			_ => {
				let (scheme, path) = split_url(self);
				let protocol = format!("{scheme}://");
				(
					Url::parse(&protocol).map_err(|_| Error::InvalidUrl(self.to_owned()))?,
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
			surreal: self.inner.clone().into(),
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
/// // Instantiate an in-memory instance
/// let db = connect("mem://").await?;
///
/// // Instantiate a file-backed instance (currently uses RocksDB)
/// let db = connect("file://path/to/database-folder").await?;
///
/// // Instantiate a RocksDB-backed instance
/// let db = connect("rocksdb://path/to/database-folder").await?;
///
/// // Instantiate a SurrealKV-backed instance
/// let db = connect("surrealkv://path/to/database-folder").await?;
///
/// // Instantiate an IndxDB-backed instance
/// let db = connect("indxdb://DatabaseName").await?;
///
/// // Instantiate a TiKV-backed instance
/// let db = connect("tikv://localhost:2379").await?;
///
/// // Instantiate a FoundationDB-backed instance
/// let db = connect("fdb://path/to/fdb.cluster").await?;
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

#[cfg(all(test, feature = "kv-mem"))]
mod tests {

	use super::*;
	use crate::Value;
	use crate::core::val;
	use crate::opt::auth::Root;
	use crate::opt::capabilities::Capabilities;

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
			users.into_inner(),
			val::Value::from(val::Object::default()),
			"there should be no users in the system"
		);
	}

	#[tokio::test]
	async fn local_engine_with_auth() {
		// Instantiate an in-memory instance with root credentials
		let creds = Root {
			username: "root",
			password: "root",
		};
		let db = connect(("memory", Config::new().user(creds).capabilities(Capabilities::all())))
			.await
			.unwrap();
		db.use_ns("N").use_db("D").await.unwrap();

		// The client needs to sign in before it can access anything
		assert!(
			db.query("INFO FOR ROOT").await.unwrap().check().is_err(),
			"client should not have access to KV"
		);
		assert!(
			db.query("INFO FOR NS").await.unwrap().check().is_err(),
			"client should not have access to NS"
		);
		assert!(
			db.query("INFO FOR DB").await.unwrap().check().is_err(),
			"client should not have access to DB"
		);

		// It can sign in
		assert!(db.signin(creds).await.is_ok(), "client should be able to sign in");

		// After the sign in, the client has access to everything
		assert!(
			db.query("INFO FOR ROOT").await.unwrap().check().is_ok(),
			"client should have access to KV"
		);
		assert!(
			db.query("INFO FOR NS").await.unwrap().check().is_ok(),
			"client should have access to NS"
		);
		assert!(
			db.query("INFO FOR DB").await.unwrap().check().is_ok(),
			"client should have access to DB"
		);
	}
}
