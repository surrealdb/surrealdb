#![deny(missing_docs)]
#![forbid(unsafe_code)]
#![deny(missing_debug_implementations)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(test, deny(warnings))]

//! This SurrealDB library enables simple and advanced querying of a remote database from
//! server-side code. All connections to SurrealDB are made over WebSockets by default (HTTP is
//! also supported), and automatically reconnect when the connection is terminated.
//!
//! # Examples
//!
//! ```no_run
//! use serde::{Serialize, Deserialize};
//! use serde_json::json;
//! use std::borrow::Cow;
//! use surrealdb_rs::{Result, Surreal};
//! use surrealdb_rs::param::Root;
//! use surrealdb_rs::protocol::Ws;
//! use ulid::Ulid;
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
//!     identifier: Ulid,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let client = Surreal::connect::<Ws>("127.0.0.1:8000").await?;
//!
//!     // Signin as a namespace, database, or root user
//!     client.signin(Root {
//!         username: "root",
//!         password: "root",
//!     }).await?;
//!
//!     // Select a specific namespace / database
//!     client.use_ns("test").use_db("test").await?;
//!
//!     // Create a new person with a random ID
//!     let created: Person = client.create("person")
//!         .content(Person {
//!             title: "Founder & CEO".into(),
//!             name: Name {
//!                 first: "Tobie".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: true,
//!             identifier: Ulid::new(),
//!         })
//!         .await?;
//!
//!     // Create a new person with a specific ID
//!     let created: Person = client.create(("person", "jaime"))
//!         .content(Person {
//!             title: "Founder & COO".into(),
//!             name: Name {
//!                 first: "Jaime".into(),
//!                 last: "Morgan Hitchcock".into(),
//!             },
//!             marketing: false,
//!             identifier: Ulid::new(),
//!         })
//!         .await?;
//!
//!     // Update a person record with a specific ID
//!     let updated: Person = client.update(("person", "jaime"))
//!         .merge(json!({"marketing": true}))
//!         .await?;
//!
//!     // Select all people records
//!     let people: Vec<Person> = client.select("person").await?;
//!
//!     // Perform a custom advanced query
//!     let groups = client
//!         .query("SELECT marketing, count() FROM type::table($tb) GROUP BY marketing")
//!         .bind("tb", "person")
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#[cfg(not(any(feature = "http", feature = "ws")))]
compile_error!("Either feature \"http\" or \"ws\" must be enabled for this crate.");

mod err;

pub mod method;

#[cfg(any(feature = "http", feature = "ws"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "http", feature = "ws"))))]
pub mod net;
pub mod param;
#[cfg(any(feature = "http", feature = "ws"))]
#[cfg_attr(docsrs, doc(cfg(any(feature = "http", feature = "ws"))))]
pub mod protocol;

pub use err::Error;
pub use err::ErrorKind;

use crate::param::ServerAddrs;
use crate::param::ToServerAddrs;
use async_trait::async_trait;
use flume::Receiver;
use flume::Sender;
use futures::future::BoxFuture;
use method::Method;
use once_cell::sync::OnceCell;
use semver::VersionReq;
use serde::de::DeserializeOwned;
use std::fmt::Debug;
use std::future::IntoFuture;
use std::marker::PhantomData;
#[cfg(feature = "ws")]
use std::sync::atomic::AtomicI64;
#[cfg(feature = "ws")]
use std::sync::atomic::Ordering;
use std::sync::Arc;
use surrealdb::sql::Value;

/// Result type returned by the client
pub type Result<T> = std::result::Result<T, Error>;

const SUPPORTED_VERSIONS: &str = ">=1.0.0-beta.8+20221030.c12a1cc, <2.0.0";

/// Connection trait implemented by supported protocols
#[async_trait]
pub trait Connection: Sized + Send + Sync + 'static {
	/// The payload the caller sends to the router
	type Request: Send + Debug;
	/// The payload the router sends back to the caller
	type Response: Send + Debug;

	/// Constructs a new client without connecting to the server
	fn new(method: Method) -> Self;

	/// Connect to the server
	async fn connect(address: ServerAddrs, capacity: usize) -> Result<Surreal<Self>>;

	/// Send a query to the server
	async fn send(
		&mut self,
		router: &Router<Self>,
		param: param::Param,
	) -> Result<Receiver<Self::Response>>;

	/// Receive responses for all methods except `query`
	async fn recv<R>(&mut self, receiver: Receiver<Self::Response>) -> Result<R>
	where
		R: DeserializeOwned;

	/// Receive the response of the `query` method
	async fn recv_query(
		&mut self,
		receiver: Receiver<Self::Response>,
	) -> Result<Vec<Result<Vec<Value>>>>;

	/// Execute all methods except `query`
	async fn execute<R>(&mut self, router: &Router<Self>, param: param::Param) -> Result<R>
	where
		R: DeserializeOwned,
	{
		let rx = self.send(router, param).await?;
		self.recv(rx).await
	}

	/// Execute the `query` method
	async fn execute_query(
		&mut self,
		router: &Router<Self>,
		param: param::Param,
	) -> Result<Vec<Result<Vec<Value>>>> {
		let rx = self.send(router, param).await?;
		self.recv_query(rx).await
	}
}

/// Connect future created by `Surreal::connect`
#[derive(Debug)]
pub struct Connect<'r, C: Connection, Response> {
	router: Option<&'r OnceCell<Arc<Router<C>>>>,
	address: Result<ServerAddrs>,
	capacity: usize,
	client: PhantomData<C>,
	response_type: PhantomData<Response>,
}

impl<C, R> Connect<'_, C, R>
where
	C: Connection,
{
	/// Sets the maximum capacity of the connection
	///
	/// This is used to set bounds of the channels used internally
	/// as well set the capacity of the `HashMap` used for routing
	/// responses in case of the WebSocket client.
	///
	/// Setting this capacity to `0` (the default) means that
	/// unbounded channels will be used. If your queries per second
	/// are so high that the client is running out of memory,
	/// it might be helpful to set this to a number that works best
	/// for you.
	///
	/// # Examples
	///
	/// ```no_run
	/// # #[tokio::spawn]
	/// # async fn main() -> surrealdb_rs::Result<()> {
	/// use surrealdb_rs::Surreal;
	///
	/// let client = Surreal::connect::<Ws>("localhost:8000")
	///     .with_capacity(100_000)
	///     .await?;
	/// # Ok(())
	/// # }
	/// ```
	#[must_use]
	pub const fn with_capacity(mut self, capacity: usize) -> Self {
		self.capacity = capacity;
		self
	}
}

impl<'r, Client> IntoFuture for Connect<'r, Client, Surreal<Client>>
where
	Client: Connection,
{
	type Output = Result<Surreal<Client>>;
	type IntoFuture = BoxFuture<'r, Result<Surreal<Client>>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			let client = Client::connect(self.address?, self.capacity).await?;
			client.check_server_version();
			Ok(client)
		})
	}
}

impl<'r, Client> IntoFuture for Connect<'r, Client, ()>
where
	Client: Connection,
{
	type Output = Result<()>;
	type IntoFuture = BoxFuture<'r, Result<()>>;

	fn into_future(self) -> Self::IntoFuture {
		Box::pin(async move {
			match self.router {
				Some(router) => {
					let option =
						Client::connect(self.address?, self.capacity).await?.router.into_inner();
					match option {
						Some(client) => {
							let _res = router.set(client);
						}
						None => unreachable!(),
					}
				}
				None => unreachable!(),
			}
			Ok(())
		})
	}
}

#[derive(Debug)]
struct Route<A, R> {
	request: A,
	response: Sender<R>,
}

/// Message router
#[derive(Debug)]
pub struct Router<C: Connection> {
	conn: PhantomData<C>,
	sender: Sender<Option<Route<C::Request, C::Response>>>,
	#[cfg(feature = "ws")]
	last_id: AtomicI64,
}

impl<C> Router<C>
where
	C: Connection,
{
	#[cfg(feature = "ws")]
	fn next_id(&self) -> i64 {
		self.last_id.fetch_add(1, Ordering::SeqCst)
	}
}

impl<C> Drop for Router<C>
where
	C: Connection,
{
	fn drop(&mut self) {
		let _res = self.sender.send(None);
	}
}

/// `SurrealDB` client
#[derive(Debug)]
pub struct Surreal<C: Connection> {
	router: OnceCell<Arc<Router<C>>>,
}

impl<C> Surreal<C>
where
	C: Connection,
{
	fn check_server_version(&self) {
		let conn = self.clone();
		tokio::spawn(async move {
			// invalid version requirements should be caught during development
			let req = VersionReq::parse(SUPPORTED_VERSIONS).expect("valid version");
			match conn.version().await {
				Ok(version) => {
					if !req.matches(&version) {
						tracing::warn!("server version `{version}` does not match the range supported by the client `{SUPPORTED_VERSIONS}`");
					}
				}
				Err(error) => {
					tracing::trace!("failed to lookup the server version; {error:?}");
				}
			}
		});
	}
}

impl<C> Clone for Surreal<C>
where
	C: Connection,
{
	fn clone(&self) -> Self {
		Self {
			router: self.router.clone(),
		}
	}
}

/// Exposes a `connect` method for use with `Surreal::new`
pub trait StaticClient<C>
where
	C: Connection,
{
	/// Connects to a specific database endpoint, saving the connection on the static client
	fn connect<P>(&self, address: impl ToServerAddrs<P, Client = C>) -> Connect<C, ()>;
}

trait ExtractRouter<C>
where
	C: Connection,
{
	fn extract(&self) -> Result<&Router<C>>;
}

impl<C> ExtractRouter<C> for OnceCell<Arc<Router<C>>>
where
	C: Connection,
{
	fn extract(&self) -> Result<&Router<C>> {
		let router = self.get().ok_or_else(connection_uninitialised)?;
		Ok(router)
	}
}

fn connection_uninitialised() -> Error {
	ErrorKind::ConnectionUninitialized.with_message("connection uninitialized")
}
