//! Dynamic support for any engine
//!
//! # Examples
//!
//! ```no_run
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use std::borrow::Cow;
//! use surrealdb::sql;
//! use surrealdb::engine::any::connect;
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
//!     let created: Vec<Person> = db.create("person")
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
//!     let sql = r#"
//!         SELECT marketing, count()
//!         FROM type::table($table)
//!         GROUP BY marketing
//!     "#;
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
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
use crate::api::opt::auth::Root;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
use crate::api::opt::Strict;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use crate::api::opt::Tls;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::dbs::Level;
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
			config: Default::default(),
			#[cfg(any(feature = "native-tls", feature = "rustls"))]
			tls_config: None,
			auth: Level::No,
			username: String::new(),
			password: String::new(),
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

#[cfg(feature = "native-tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "native-tls")))]
impl<T> IntoEndpoint for (T, native_tls::TlsConnector)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config) = self;
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Native(config));
		Ok(endpoint)
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
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Rust(config));
		Ok(endpoint)
	}
}

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	)))
)]
impl<T> IntoEndpoint for (T, Strict)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, _) = self;
		let mut endpoint = IntoEndpoint::into_endpoint(address.into())?;
		endpoint.config.strict = true;
		Ok(endpoint)
	}
}

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	)))
)]
impl<T> IntoEndpoint for (T, Config)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config) = self;
		let mut endpoint = IntoEndpoint::into_endpoint(address.into())?;
		endpoint.config = config;
		Ok(endpoint)
	}
}

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	)))
)]
impl<T> IntoEndpoint for (T, Root<'_>)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, root) = self;
		let mut endpoint = IntoEndpoint::into_endpoint(address.into())?;
		endpoint.auth = Level::Kv;
		endpoint.username = root.username.to_owned();
		endpoint.password = root.password.to_owned();
		Ok(endpoint)
	}
}

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-tikv",
	feature = "kv-rocksdb",
	feature = "kv-speedb",
	feature = "kv-fdb",
	feature = "kv-indxdb",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	)))
)]
impl<T> IntoEndpoint for (T, Strict, Root<'_>)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, _, root) = self;
		let mut endpoint = IntoEndpoint::into_endpoint((address, root))?;
		endpoint.config.strict = true;
		Ok(endpoint)
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
impl<T> IntoEndpoint for (T, Config, Root<'_>)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config, root) = self;
		let mut endpoint = IntoEndpoint::into_endpoint((address, root))?;
		endpoint.config = config;
		Ok(endpoint)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-fdb",
		feature = "kv-speedb",
		feature = "kv-indxdb",
	),
	feature = "native-tls",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(all(
		any(
			feature = "kv-mem",
			feature = "kv-tikv",
			feature = "kv-rocksdb",
			feature = "kv-speedb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "native-tls",
	)))
)]
impl<T> IntoEndpoint for (T, Strict, native_tls::TlsConnector)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, _, config) = self;
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Native(config));
		endpoint.config.strict = true;
		Ok(endpoint)
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
	feature = "native-tls",
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
		feature = "native-tls",
	)))
)]
impl<T> IntoEndpoint for (T, Config, native_tls::TlsConnector)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, opt_config, config) = self;
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Native(config));
		endpoint.config = opt_config;
		Ok(endpoint)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
		feature = "kv-fdb",
		feature = "kv-indxdb",
	),
	feature = "native-tls",
))]
#[cfg_attr(
	docsrs,
	doc(cfg(all(
		any(
			feature = "kv-mem",
			feature = "kv-tikv",
			feature = "kv-rocksdb",
			feature = "kv-speedb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "native-tls",
	)))
)]
impl<T> IntoEndpoint for (T, native_tls::TlsConnector, Root<'_>)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config, root) = self;
		let mut endpoint = (address, root).into_endpoint()?;
		endpoint.tls_config = Some(Tls::Native(config));
		Ok(endpoint)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
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
			feature = "kv-speedb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "rustls",
	)))
)]
impl<T> IntoEndpoint for (T, Strict, rustls::ClientConfig)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, _, config) = self;
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Rust(config));
		endpoint.config.strict = true;
		Ok(endpoint)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
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
			feature = "kv-speedb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "rustls",
	)))
)]
impl<T> IntoEndpoint for (T, Config, rustls::ClientConfig)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, opt_config, config) = self;
		let mut endpoint = address.into().into_endpoint()?;
		endpoint.tls_config = Some(Tls::Rust(config));
		endpoint.config = opt_config;
		Ok(endpoint)
	}
}

#[cfg(all(
	any(
		feature = "kv-mem",
		feature = "kv-tikv",
		feature = "kv-rocksdb",
		feature = "kv-speedb",
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
			feature = "kv-speedb",
			feature = "kv-fdb",
			feature = "kv-indxdb",
		),
		feature = "rustls",
	)))
)]
impl<T> IntoEndpoint for (T, rustls::ClientConfig, Root<'_>)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let (address, config, root) = self;
		let mut endpoint = (address, root).into_endpoint()?;
		endpoint.tls_config = Some(Tls::Rust(config));
		Ok(endpoint)
	}
}

/// A dynamic connection that supports any engine and allows you to pick at runtime
#[derive(Debug, Clone)]
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
	/// use surrealdb::engine::any::Any;
	///
	/// static DB: Surreal<Any> = Surreal::init();
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect("ws://localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect(&self, address: impl IntoEndpoint) -> Connect<Any, ()> {
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

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::{test::Parse, value::Value};

	#[tokio::test]
	async fn local_engine_without_auth() {
		// Instantiate an in-memory instance without root credentials
		let db = connect("memory").await.unwrap();
		db.use_ns("N").use_db("D").await.unwrap();
		// The client has access to everything
		assert!(
			db.query("INFO FOR KV").await.unwrap().check().is_ok(),
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

		// There are no users in the datastore
		let mut res = db.query("INFO FOR KV").await.unwrap();
		let users: Value = res.take("users").unwrap();

		assert_eq!(users, Value::parse("[{}]"), "there should be no users in the system");
	}

	#[tokio::test]
	async fn local_engine_with_auth() {
		// Instantiate an in-memory instance with root credentials
		let creds = Root {
			username: "root",
			password: "root",
		};
		let db = connect(("memory", creds)).await.unwrap();
		db.use_ns("N").use_db("D").await.unwrap();

		// The client needs to sign in before it can access anything
		assert!(
			db.query("INFO FOR KV").await.unwrap().check().is_err(),
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
		assert!(db.signin(creds).await.is_ok(), "client should not be able to sign in");

		// After the sign in, the client has access to everything
		assert!(
			db.query("INFO FOR KV").await.unwrap().check().is_ok(),
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
