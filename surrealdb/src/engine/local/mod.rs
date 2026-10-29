//! Embedded database instance
//!
//! `SurrealDB` itself can be embedded in this library, allowing you to query it
//! using the same crate and API that you would use when connecting to it
//! remotely via WebSockets or HTTP. All storage engines are supported but you
//! have to activate their feature flags first.
//!
//! **NB**: Some storage engines depend on non-Rust
//! libraries so you need to install those libraries before you can build this
//! crate when you activate their feature flags. Please refer to [these instructions](https://github.com/surrealdb/surrealdb/blob/main/doc/BUILDING.md)
//! for more details on how to install them. If you are on Linux and you use
//! [the Nix package manager](https://github.com/surrealdb/surrealdb/tree/main/pkg/nix#installing-nix)
//! you can just run
//!
//! ```bash
//! nix develop github:surrealdb/surrealdb
//! ```
//!
//! which will drop you into a shell with all the dependencies available. One
//! tip you may find useful is to only enable the in-memory engine (`kv-mem`)
//! during development. Besides letting you not worry about those dependencies
//! on your dev machine, it allows you to keep compile times low
//! during development while allowing you to test your code fully.
//!
//! When running SurrealDB as an embedded database within Rust, using the
//! correct release profile and memory allocator can greatly improve the
//! performance of the database core engine. In addition using an optimised
//! asynchronous runtime configuration can help speed up concurrent queries and
//! increase database throughput.
//!
//! In your project’s Cargo.toml file, ensure that the release profile uses the
//! following configuration:
//!
//! ```toml
//! [profile.release]
//! lto = true
//! strip = true
//! opt-level = 3
//! panic = 'abort'
//! codegen-units = 1
//! ```
//!
//! In your project’s Cargo.toml file, ensure that the allocator feature is
//! among those enabled on the surrealdb dependency:
//!
//! ```toml
//! [dependencies]
//! surrealdb = { version = "3", features = ["allocator", "kv-surrealkv"] }
//! ```
//!
//! When running SurrealDB within your Rust code, ensure that the asynchronous
//! runtime is configured correctly, making use of multiple threads, an
//! increased stack size, and an optimised number of threads:
//!
//! ```toml
//! [dependencies]
//! tokio = { version = "1", features = ["sync", "rt-multi-thread"] }
//! ```
//!
//! ```no_run
//! tokio::runtime::Builder::new_multi_thread()
//!     .enable_all()
//!     .thread_stack_size(10 * 1024 * 1024) // 10MiB
//!     .build()
//!     .unwrap()
//!     .block_on(async {
//!         // Your application code
//!     })
//! ```
//!
//! # Example
//!
//! ```no_compile
//! use std::borrow::Cow;
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use surrealdb::{Error, Surreal};
//! use surrealdb::opt::auth::Root;
//! use surrealdb::engine::local::SurrealKv;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: String,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! // Pro tip: Replace String with Cow<'static, str> to
//! // avoid unnecessary heap allocations when inserting
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let db = Surreal::new::<SurrealKv>("path/to/database/folder").await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Option<Person> = db.create("person")
//!         .content(Person {
//!             title: "Founder & CEO".into(),
//!             name: Name {
//!                 first: "Tobie".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: true,
//!         })
//!         .await?;
//!
//!     // Create a new person with a specific ID
//!     let created: Option<Person> = db.create(("person", "jaime"))
//!         .content(Person {
//!             title: "Founder & COO".into(),
//!             name: Name {
//!                 first: "Jaime".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: false,
//!         })
//!         .await?;
//!
//!     // Update a person record with a specific ID
//!     let updated: Option<Person> = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let query = r#"
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     "#;
//!
//!     let groups = db.query(query)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#[cfg(not(target_family = "wasm"))]
pub(crate) mod native;
#[cfg(target_family = "wasm")]
pub(crate) mod wasm;

use std::marker::PhantomData;
use std::sync::Arc;

use surrealdb_engine_local::{LocalConfig, Root};

use crate::opt::{Endpoint, IntoEndpoint};
use crate::{Connect, Surreal};

/// In-memory database
///
/// # Examples
///
/// Instantiating a global instance
///
/// ```
/// use std::sync::LazyLock;
/// use surrealdb::{Result, Surreal};
/// use surrealdb::engine::local::Db;
/// use surrealdb::engine::local::Mem;
///
/// static DB: LazyLock<Surreal<Db>> = LazyLock::new(Surreal::init);
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     DB.connect::<Mem>(()).await?;
///
///     Ok(())
/// }
/// ```
///
/// Instantiating an in-memory instance
///
/// ```
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::Mem;
///
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// let db = Surreal::new::<Mem>(()).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-mem")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-mem")))]
#[derive(Debug)]
pub struct Mem;

/// SurrealKV database
///
/// # Examples
///
/// Instantiating a SurrealKV-backed instance
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealKv;
///
/// let db = Surreal::new::<SurrealKv>("path/to/database-folder").await?;
/// # Ok(())
/// # }
/// ```
///
/// Capping the engine's memory, either on the connection path or with
/// [`Config::datastore_option`](crate::opt::Config::datastore_option). The
/// path wins when both set a value.
///
/// ```no_run
/// # #[tokio::main]
/// # async fn main() -> surrealdb::Result<()> {
/// use surrealdb::Surreal;
/// use surrealdb::engine::local::SurrealKv;
/// use surrealdb::opt::Config;
///
/// let db = Surreal::new::<SurrealKv>(
///     "path/to/database-folder?block_cache_capacity=268435456&max_memtable_size=67108864",
/// )
/// .await?;
///
/// let config = Config::new()
///     .datastore_option("surrealkv_block_cache_capacity", 256 * 1024 * 1024)
///     .datastore_option("surrealkv_max_memtable_size", 64 * 1024 * 1024);
/// let db = Surreal::new::<SurrealKv>(("path/to/other-folder", config)).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "kv-surrealkv")]
#[cfg_attr(docsrs, doc(cfg(feature = "kv-surrealkv")))]
#[derive(Debug)]
pub struct SurrealKv;

/// An embedded database
#[derive(Debug, Clone)]
pub struct Db(());

impl Surreal<Db> {
	/// Connects to a specific database endpoint, saving the connection on the
	/// static client
	pub fn connect<P>(&self, address: impl IntoEndpoint<P, Client = Db>) -> Connect<Db, ()> {
		Connect {
			surreal: Arc::clone(&self.inner).into(),
			address: address.into_endpoint(),
			capacity: 0,
			response_type: PhantomData,
		}
	}
}

/// Derive the embedded engine's bootstrap parameters from the endpoint the
/// caller connected to.
///
/// The path is the one the scheme prefix already carries.
pub(crate) fn local_config(address: Endpoint) -> LocalConfig {
	let path = address.path;
	let config = address.config;
	// `Level::No` means no credentials were configured, and the datastore then
	// starts with authentication disabled.
	let root = if let surrealdb_iam::Level::Root = config.auth {
		Some(Root {
			username: config.username,
			password: config.password,
		})
	} else {
		None
	};
	#[cfg(storage)]
	let temporary_directory = config.temporary_directory;
	#[cfg(not(storage))]
	let temporary_directory = None;
	LocalConfig {
		path,
		root,
		query_timeout: config.query_timeout,
		transaction_timeout: config.transaction_timeout,
		capabilities: config.capabilities,
		bucket_folder_allowlist: config.bucket_folder_allowlist,
		temporary_directory,
		node_membership_refresh_interval: config.node_membership_refresh_interval,
		node_membership_check_interval: config.node_membership_check_interval,
		node_membership_cleanup_interval: config.node_membership_cleanup_interval,
		changefeed_gc_interval: config.changefeed_gc_interval,
		datastore_options: config.datastore_options,
	}
}
