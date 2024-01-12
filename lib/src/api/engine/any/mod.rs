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
use crate::api::opt::Config;
use crate::api::opt::Endpoint;
use crate::api::Connect;
use crate::api::Result;
use crate::api::Surreal;
use crate::opt::path_to_string;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::OnceLock;
use url::Url;

/// A trait for converting inputs to a server address object
pub trait IntoEndpoint {
	/// Converts an input into a server address object
	fn into_endpoint(self) -> Result<Endpoint>;
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

impl IntoEndpoint for &str {
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
		Ok(Endpoint {
			url,
			path,
			config: Default::default(),
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

impl<T> IntoEndpoint for (T, Config)
where
	T: Into<String>,
{
	fn into_endpoint(self) -> Result<Endpoint> {
		let mut endpoint = IntoEndpoint::into_endpoint(self.0.into())?;
		endpoint.config = self.1;
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
	/// use once_cell::sync::Lazy;
	/// use surrealdb::Surreal;
	/// use surrealdb::engine::any::Any;
	///
	/// static DB: Lazy<Surreal<Any>> = Lazy::new(Surreal::init);
	///
	/// # #[tokio::main]
	/// # async fn main() -> surrealdb::Result<()> {
	/// DB.connect("ws://localhost:8000").await?;
	/// # Ok(())
	/// # }
	/// ```
	pub fn connect(&self, address: impl IntoEndpoint) -> Connect<Any, ()> {
		Connect {
			router: self.router.clone(),
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
pub fn connect(address: impl IntoEndpoint) -> Connect<Any, Surreal<Any>> {
	Connect {
		router: Arc::new(OnceLock::new()),
		address: address.into_endpoint(),
		capacity: 0,
		client: PhantomData,
		response_type: PhantomData,
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::dbs::Capabilities;
	use crate::opt::auth::Root;
	use crate::sql::Value;
	use crate::syn::Parse;

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

		assert_eq!(users, Value::parse("{}"), "there should be no users in the system");
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
