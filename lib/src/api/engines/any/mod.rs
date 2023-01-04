//! Dynamic support for any engine
//!
//! # Examples
//!
//! ```no_run
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use std::borrow::Cow;
//! use surrealdb::sql;
//! use surrealdb::engines::any::connect;
//! use surrealdb::opt::auth::Root;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Name {
//!     first: Cow<'static, str>,
//!     last: Cow<'static, str>,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Person {
//!     title: Cow<'static, str>,
//!     name: Name,
//!     marketing: bool,
//! }
//!
//! #[tokio::main]
//! async fn main() -> surrealdb::Result<()> {
//!     let db = connect("ws://localhost:8000").await?;
//!
//!     // Signin as a namespace, database, or root user
//!     db.signin(Root {
//!         username: "root",
//!         password: "root",
//!     }).await?;
//!
//!     // Select a specific namespace / database
//!     db.use_ns("namespace").use_db("database").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Person = db.create("person")
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
//!     let created: Person = db.create(("person", "jaime"))
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
//!     let updated: Person = db.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = db.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let sql = sql! {
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     };
//!
//!     let groups = db.query(sql)
//!         .bind(("table", "person"))
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(target_arch = "wasm32")]
mod wasm;

use crate::api::conn::Method;
use crate::api::err::Error;
use crate::api::opt::Endpoint;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
use crate::api::opt::Strict;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use std::marker::PhantomData;
use url::Url;

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint {
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
}

impl IntoEndpoint for &str {
	fn into_endpoint(self) -> Result<Endpoint> {
		let url = match self {
			"memory" => "mem://",
			_ => self,
		};
		Ok(Endpoint {
			endpoint: Url::parse(url).map_err(|_| Error::InvalidUrl(self.to_owned()))?,
			strict: false,
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
		})
	}
}

impl IntoEndpoint for &String {
	fn into_endpoint(self) -> Result<Endpoint> {
		self.as_str().into_endpoint()
	}
}

impl IntoEndpoint for String {
	fn into_endpoint(self) -> Result<Endpoint> {
		self.as_str().into_endpoint()
	}
}

#[cfg(feature = "rustls")]
#[cfg_attr(docsrs, doc(cfg(feature = "rustls")))]
impl<T> IntoEndpoint for (T, rustls::ClientConfig)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config) = self;
		let mut address = address.into().into_endpoint()?;
		address.tls_config = Some(Tls::Rust(config));
		Ok(address)
	}
}

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	)))
)]
impl<T> IntoEndpoint for (T, Strict)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let mut address = IntoEndpoint::into_endpoint(self.0.into())?;
		address.strict = true;
		Ok(address)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	),
	feature = "rustls",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(all(
		any(
			feature = "kv-mem",
			feature = "kv-tikv",
			feature = "kv-rocksdb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "rustls",
	)))
)]
impl<T> IntoEndpoint for (T, rustls::ClientConfig, Strict)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config, _) = self;
		let mut address = address.into().into_endpoint()?;
		address.tls_config = Some(Tls::Rust(config));
		address.strict = true;
		Ok(address)
	}
}

/// A dynamic connection that supports any engine and allows you to pick at runtime
#[derive(Debug)]
pub struct Any {
	id: i64,
	method: Method,
}

impl Surreal<Any> {
	/// Connects to a specific database endpoint, saving the connection on the static client
	///
	/// # Examples
	///
	/// ```no_run
	/// use surrealdb::Surreal;
	/// use surrealdb::engines::any::Any;
	///
	/// static DB: Surreal<Any> = Surreal::init();
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect("ws://localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect(&'static self, address: impl IntoEndpoint) -> Connect<Any, ()> {
		Connect {
			router: Some(&self.router),
			address: address.into_endpoint(),
			capacity: 0,
			client: PhantomData,
			response_type: PhantomData,
		}
	}
}

/// Connects to a local, remote or embedded database
///
/// # Examples
///
/// ```no_run
/// use surrealdb::engines::any::connect;
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
/// // Instantiate an file-backed instance
/// let db = connect("file://temp.db").await?;
///
/// // Instantiate an IndxDB-backed instance
/// let db = connect("indxdb://MyDatabase").await?;
///
/// // Instantiate a TiKV-backed instance
/// let db = connect("tikv://localhost:2379").await?;
///
/// // Instantiate a FoundationDB-backed instance
/// let db = connect("fdb://fdb.cluster").await?;
/// # Ok(())
/// # }
/// ```
pub fn connect(address: impl IntoEndpoint) -> Connect<'static, Any, Surreal<Any>> {
	Connect {
		router: None,
		address: address.into_endpoint(),
		capacity: 0,
		client: PhantomData,
		response_type: PhantomData,
	}
}
